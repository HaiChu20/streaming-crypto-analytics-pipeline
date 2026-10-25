"""One position per coin simulator and reproducible ISO-week block bootstrap."""

from datetime import datetime, timezone

import numpy as np
import polars as pl

from strategy2.signal import make_signal, round_trip_cost

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
    _, inverse = np.unique(week_blocks(ts_ms), return_inverse=True)
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
    """Return trade dictionaries. Rows use the target-independent OOF schema."""
    frame = rows if isinstance(rows, pl.DataFrame) else pl.DataFrame(rows)
    if frame.is_empty():
        return []
    trades = []
    available = {}
    for row in frame.sort(["ts_ms", "coin"]).iter_rows(named=True):
        coin, entry = row["coin"], int(row["entry_ms"])
        # The decision, not its future fill, must occur after the previous exit.
        if row["ts_ms"] < available.get(coin, -1):
            continue
        cfg = dict(config, entry_ms=entry)
        for key in ("timeout_mean_long", "timeout_mean_short", "ev_threshold"):
            if row.get(key) is not None:
                cfg[key] = row[key]
        if ev_threshold is not None:
            cfg["ev_threshold"] = ev_threshold
        signal = make_signal(row["ts_ms"], coin, target, row["p_up"], row["p_down"],
                             row["p_none"], row["entry_price"], cfg)
        if not signal["trade"]:
            continue
        label = int(row["label"])
        if label not in (-1, 0, 1):
            raise ValueError("Invalid outcome label")
        minutes = row["minutes_to_touch"] if label else config["targets"][target]["horizon_minutes"]
        if minutes is None or minutes <= 0:
            raise ValueError("Touched rows need a positive minutes_to_touch")
        exit_ms = entry + int(minutes * 60_000)
        crossings = funding_crossings(entry, exit_ms, row.get("next_funding_ms"),
                                      row.get("funding_interval_ms"))
        rate = row.get("last_funding_rate")
        if crossings and (rate is None or not np.isfinite(rate)):
            raise ValueError("Funding crossing has no finite last_funding_rate")
        funding = signal["direction"] * rate * crossings if crossings else 0.0
        gross = signal["direction"] * row["exit_return"]
        cost = round_trip_cost(cfg)
        trades.append({"ts_ms": row["ts_ms"], "coin": coin, "entry_ms": entry,
                       "exit_ms": exit_ms, "direction": signal["direction"],
                       "outcome": label * signal["direction"], "gross_return": gross,
                       "cost": cost, "funding": funding, "funding_crossings": crossings,
                       "net_return": gross - cost - funding, "ev": signal["ev"]})
        available[coin] = exit_ms
    return trades


def trade_metrics(trades, config, target="a", start_ms=None, end_ms=None):
    barrier = config["targets"][target]["barrier"]
    result = {"trades": len(trades), "break_even_p": 0.5 + round_trip_cost(config) / (2 * barrier)}
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


def simulation_report(rows, config, target="a"):
    frame = rows if isinstance(rows, pl.DataFrame) else pl.DataFrame(rows)
    start, end = int(frame["ts_ms"].min()), int(frame["ts_ms"].max()) + 300_000
    trades = simulate(frame, config, target)
    report = trade_metrics(trades, config, target, start, end)
    report["accounting"] = "Fixed unit notional per coin; summed cash returns; daily realised P&L including zero-trade days. Funding holds the last known rate constant."
    report["per_coin"] = {coin: trade_metrics([t for t in trades if t["coin"] == coin], config, target, start, end)
                          for coin in sorted(frame["coin"].unique())}
    years = sorted({datetime.fromtimestamp(t / 1000, timezone.utc).year for t in frame["ts_ms"]})
    report["per_year"] = {}
    for year in years:
        lo = int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        hi = int(datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        report["per_year"][str(year)] = trade_metrics(
            [t for t in trades if lo <= t["exit_ms"] < hi], config, target, max(start, lo), min(end, hi))
    report["ev_threshold_sweep"] = [dict(ev_threshold=threshold, **trade_metrics(
        simulate(frame, config, target, threshold), config, target, start, end))
        for threshold in config.get("ev_threshold_sweep", [config.get("ev_threshold", 0.0)])]
    return report
