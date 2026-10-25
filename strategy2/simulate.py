"""One position per coin simulator and reproducible ISO-week block bootstrap."""

from datetime import datetime, timezone

import numpy as np
import polars as pl

from strategy2.signal import expected_cost, round_trip_cost
from strategy2.train import timestamp_ms

DAY_MS = 86_400_000


def week_blocks(ts_ms):
    return [datetime.fromtimestamp(int(t) / 1000, timezone.utc).strftime("%G-W%V")
            for t in ts_ms]


def weekly_bootstrap(values, ts_ms, draws=1000, seed=17):
    """Resample whole ISO weeks, preserving row weights and paired differences."""
    values = np.asarray(values, dtype=float)
    if draws < 1:
        raise ValueError("Bootstrap draws must be positive")
    if len(values) != len(ts_ms) or not np.isfinite(values).all():
        raise ValueError("Bootstrap needs aligned finite observations")
    if not len(values):
        return {"mean": None, "ci_low": None, "ci_high": None, "weeks": 0}
    # Epoch day zero is Thursday: adding three groups Monday-through-Sunday.
    _, inverse = np.unique((np.asarray(ts_ms, dtype=np.int64) // DAY_MS + 3) // 7, return_inverse=True)
    sums = np.bincount(inverse, weights=values)
    counts = np.bincount(inverse)
    rng = np.random.default_rng(seed)
    samples = np.empty(draws)
    for i in range(draws):
        blocks = rng.integers(0, len(sums), size=len(sums))
        samples[i] = sums[blocks].sum() / counts[blocks].sum()
    low, high = np.quantile(samples, [0.025, 0.975])
    return {"mean": float(values.mean()), "ci_low": float(low),
            "ci_high": float(high), "weeks": len(sums)}


def funding_crossings(entry_ms, exit_ms, next_funding_ms, funding_interval_ms):
    if next_funding_ms is None or funding_interval_ms is None:
        return 0
    interval = int(funding_interval_ms)
    if interval <= 0:
        raise ValueError("funding_interval_ms must be positive")
    first = int(next_funding_ms)
    if first <= entry_ms:
        first += ((entry_ms - first) // interval + 1) * interval
    return max(0, (exit_ms - first) // interval + 1)


def simulate(rows, config, target="a", ev_threshold=None):
    """Vectorise probability/EV gating; schedule only gated rows, one coin at a time."""
    frame = rows if isinstance(rows, pl.DataFrame) else pl.DataFrame(rows)
    if frame.is_empty():
        return []
    p = frame.select("p_up", "p_down", "p_none").to_numpy()
    if (not np.isfinite(p).all() or (p < 0).any() or (p > 1).any()
            or not np.allclose(p.sum(axis=1), 1, rtol=1e-9, atol=1e-6)):
        raise ValueError("Invalid probability distributions")
    move = frame["p_move"].to_numpy() if "p_move" in frame.columns else 1 - p[:, 2]
    side = frame["p_side"].to_numpy() if "p_side" in frame.columns else np.divide(
        p[:, 0], p[:, 0] + p[:, 1], out=np.full(len(frame), 0.5), where=p[:, 0] + p[:, 1] > 0)
    if (not np.isfinite(move).all() or not np.isfinite(side).all()
            or (move < 0).any() or (move > 1).any() or (side < 0).any() or (side > 1).any()):
        raise ValueError("Invalid two-stage probabilities")
    direction = np.sign(side - 0.5).astype(np.int8)
    def setting(key):
        default = config.get(key, 0.0)
        return frame[key].fill_null(default).to_numpy() if key in frame.columns else default
    timeout = np.where(direction > 0, setting("timeout_mean_long"), setting("timeout_mean_short"))
    cost = round_trip_cost(config)
    tp_cost = round_trip_cost(config, outcome=1)
    two_stage = "p_move" in frame.columns or "p_side" in frame.columns
    edge = move * np.abs(2 * side - 1) if two_stage else np.abs(p[:, 0] - p[:, 1])
    none = 1 - move if two_stage else p[:, 2]
    p_tp = move * np.maximum(side, 1 - side) if two_stage else np.maximum(p[:, 0], p[:, 1])
    ev = edge * config["targets"][target]["barrier"] + none * timeout - expected_cost(config, p_tp)
    threshold = setting("ev_threshold") if ev_threshold is None else ev_threshold
    gate = (direction != 0) & (ev > threshold)
    gated = frame.with_columns(pl.Series("_direction", direction), pl.Series("_ev", ev)).filter(pl.Series(gate))
    prices = frame["entry_price"].to_numpy()
    if not np.isfinite(prices).all() or (prices <= 0).any():
        raise ValueError("entry_price must be positive")
    if gated.is_empty():
        return []
    trades = []
    horizon = config["targets"][target]["horizon_minutes"]
    for group in gated.sort(["coin", "ts_ms"]).partition_by("coin", maintain_order=True):
        times = group["ts_ms"].to_numpy()
        entries = group["entry_ms"].to_numpy()
        labels = group["label"].to_numpy()
        minutes = np.where(labels != 0, group["minutes_to_touch"].to_numpy(), horizon)
        selected = []
        index = 0
        while index < len(group):
            if labels[index] not in (-1, 0, 1):
                raise ValueError("Invalid outcome label")
            if not np.isfinite(minutes[index]) or minutes[index] <= 0:
                raise ValueError("Touched rows need a positive minutes_to_touch")
            exit_ms = int(entries[index]) + int(minutes[index] * 60_000)
            selected.append(index)
            # Decision timestamp, rather than future fill, must follow the prior exit.
            index = max(index + 1, int(np.searchsorted(times, exit_ms, side="left")))
        for row in group[selected].iter_rows(named=True):
            entry, direction = int(row["entry_ms"]), int(row["_direction"])
            label = int(row["label"])
            minutes = row["minutes_to_touch"] if label else horizon
            exit_ms = entry + int(minutes * 60_000)
            crossings = funding_crossings(entry, exit_ms, row.get("next_funding_ms"), row.get("funding_interval_ms"))
            rate = row.get("last_funding_rate")
            if crossings and (rate is None or not np.isfinite(rate)):
                raise ValueError("Funding crossing has no finite last_funding_rate")
            funding = direction * rate * crossings if crossings else 0.0
            gross = direction * row["exit_return"]
            realised_cost = tp_cost if label * direction == 1 else cost
            trades.append({"ts_ms": row["ts_ms"], "coin": row["coin"], "entry_ms": entry,
                           "exit_ms": exit_ms, "direction": direction, "outcome": label * direction,
                           "gross_return": gross, "cost": realised_cost, "funding": funding,
                           "funding_crossings": crossings, "net_return": gross - realised_cost - funding,
                           "ev": row["_ev"]})
    return sorted(trades, key=lambda trade: (trade["ts_ms"], trade["coin"]))



def trade_metrics(trades, config, target="a", start_ms=None, end_ms=None):
    barrier = config["targets"][target]["barrier"]
    result = {"trades": len(trades), "break_even_p": 0.5 + round_trip_cost(config) / (2 * barrier)}
    if "tp_exit_fee" in config.get("costs", {}):
        market, tp = round_trip_cost(config), round_trip_cost(config, outcome=1)
        result["break_even_p"] = (barrier + market) / (2 * barrier + market - tp)
    if not trades:
        return dict(result, trades_per_month=0.0, win_rate=None, loss_rate=None,
                    timeout_rate=None, touched_win_rate=None, mean_net_return=None,
                    mean_net_return_ci=None, total_return=0.0, daily_sharpe=None,
                    max_drawdown=0.0)
    outcomes = np.array([t["outcome"] for t in trades])
    returns = np.array([t["net_return"] for t in trades])
    times = [t["exit_ms"] for t in trades]
    first = min(times) if start_ms is None else start_ms
    last = max(times) if end_ms is None else end_ms
    first_day, last_day = first // DAY_MS, max(last, max(times)) // DAY_MS
    daily = np.zeros(last_day - first_day + 1)
    for trade in trades:
        daily[trade["exit_ms"] // DAY_MS - first_day] += trade["net_return"]
    # Fixed one-unit notional per trade/coin, cash P&L, no reinvestment or leverage assumptions.
    equity = np.r_[1.0, 1.0 + np.cumsum(daily)]
    peaks = np.maximum.accumulate(equity)
    touched = outcomes != 0
    ci = weekly_bootstrap(returns, times, config.get("bootstrap_draws", 1000),
                          config.get("bootstrap_seed", 17))
    result.update(trades_per_month=len(trades) / max((last - first) / DAY_MS / (365.25 / 12), 1 / (365.25 / 12)),
                  win_rate=float(np.mean(outcomes == 1)), loss_rate=float(np.mean(outcomes == -1)),
                  timeout_rate=float(np.mean(outcomes == 0)),
                  touched_win_rate=float(np.mean(outcomes[touched] == 1)) if touched.any() else None,
                  mean_net_return=float(returns.mean()), mean_net_return_ci=ci,
                  total_return=float(returns.sum()),
                  daily_sharpe=float(daily.mean() / daily.std(ddof=1) * np.sqrt(365))
                  if len(daily) > 1 and daily.std(ddof=1) > 0 else None,
                  max_drawdown=float(np.max((peaks - equity) / peaks)))
    return result


def period_frames(frame, config):
    """Periods are assigned by decision/test time, with an exclusive tuning cutoff."""
    cutoff = timestamp_ms(config.get("tuning_until", "2025-04-01"))
    return {"all": frame, "tuning": frame.filter(pl.col("ts_ms") < cutoff),
            "confirmation": frame.filter(pl.col("ts_ms") >= cutoff)}


def choose_ev_threshold(tuning_sweep, minimum_trades=100):
    eligible = [row for row in tuning_sweep if row["trades"] >= minimum_trades
                and row["mean_net_return"] is not None]
    if not eligible:
        return None
    # Deterministic tie-break: prefer the lower threshold.
    return min(eligible, key=lambda row: (-row["mean_net_return"], row["ev_threshold"]))["ev_threshold"]


def simulation_report(rows, config, target="a"):
    frame = rows if isinstance(rows, pl.DataFrame) else pl.DataFrame(rows)
    periods = period_frames(frame, config)
    cutoff = timestamp_ms(config.get("tuning_until", "2025-04-01"))
    bounds = {period: (int(part["ts_ms"].min()), int(part["ts_ms"].max()) + 300_000)
              if len(part) else (None, None) for period, part in periods.items()}
    def split_trades(trades, period):
        if period == "all":
            return trades
        return [t for t in trades if (t["ts_ms"] < cutoff) == (period == "tuning")]
    def metrics(trades, cfg, period):
        return trade_metrics(split_trades(trades, period), cfg, target, *bounds[period])
    scenarios = config.get("cost_scenarios", {"headline": config.get("costs", {})})
    if "headline" not in scenarios:
        raise ValueError("cost_scenarios requires a headline scenario")
    ordered = ["headline", *[name for name in scenarios if name != "headline"]]
    result = {"scenarios": {}}
    for name in ordered:
        cfg = dict(config, costs=scenarios[name])
        default_trades = simulate(frame, cfg, target)
        by_period = {}
        for period, part in periods.items():
            selected = split_trades(default_trades, period)
            report = metrics(default_trades, cfg, period)
            report["per_coin"] = {coin: trade_metrics([t for t in selected if t["coin"] == coin],
                cfg, target, *bounds[period]) for coin in sorted(part["coin"].unique())}
            report["per_year"] = {}
            years = part.select(pl.from_epoch("ts_ms", time_unit="ms").dt.year()).to_series().unique().sort()
            for year in years:
                lo, hi = timestamp_ms(f"{year}-01-01"), timestamp_ms(f"{year + 1}-01-01")
                start, end = bounds[period]
                report["per_year"][str(year)] = trade_metrics([t for t in selected if lo <= t["exit_ms"] < hi],
                    cfg, target, max(start, lo), min(end, hi))
            report["ev_threshold_sweep"] = []
            by_period[period] = report
        sweep_trades = {}
        for threshold in config.get("ev_threshold_sweep", [config.get("ev_threshold", 0.0)]):
            trades = simulate(frame, cfg, target, threshold)
            sweep_trades[threshold] = trades
            for period in periods:
                by_period[period]["ev_threshold_sweep"].append(dict(
                    ev_threshold=threshold, **metrics(trades, cfg, period)))
        chosen = choose_ev_threshold(by_period["tuning"]["ev_threshold_sweep"])
        selection = {"threshold": chosen, "selection_period": "tuning", "minimum_tuning_trades": 100,
                     "reason": None if chosen is not None else "No threshold reaches 100 tuning trades.",
                     "periods": {period: metrics(sweep_trades[chosen], cfg, period) for period in periods}
                     if chosen is not None else {}}
        scenario = {"label": "Headline: taker entry / taker exit" if name == "headline" else
                    f"Sensitivity: {name} (maker fills are not guaranteed)",
                    "round_trip_cost": round_trip_cost(cfg), "periods": by_period, "ev_selection": selection}
        if "tp_exit_fee" in cfg["costs"]:
            scenario["tp_round_trip_cost"] = round_trip_cost(cfg, outcome=1)
        result["scenarios"][name] = scenario
    # Preserve v1's public report fields, including the per-fold EV mechanism.
    headline = result["scenarios"]["headline"]
    result.update(headline["periods"]["all"])
    result["periods"] = headline["periods"]
    result["ev_selection"] = headline["ev_selection"]
    result["accounting"] = ("Fixed unit notional per coin; summed cash returns; daily realised P&L including zero-trade days. "
        "Funding holds the last known rate constant. Periods use decision timestamps; schedules run continuously across the cutoff.")
    return result
