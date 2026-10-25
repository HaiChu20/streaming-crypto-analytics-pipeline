"""Pure Strategy 2 signal records; all returns and costs are fractions."""

import math


def round_trip_cost(config, outcome=0):
    """Market-exit cost by default; outcome +1 may use a resting TP limit."""
    costs = config.get("costs", {})
    values = [costs.get("taker_fee", 0.0005), costs.get("slippage_entry", 0.0001),
              costs.get("slippage_exit", 0.0002)]
    if any(not math.isfinite(v) or v < 0 for v in values):
        raise ValueError("Costs must be finite and nonnegative")
    entry_fee, exit_fee = costs.get("entry_fee", values[0]), costs.get("exit_fee", values[0])
    if any(not math.isfinite(v) or v < 0 for v in (entry_fee, exit_fee)):
        raise ValueError("Costs must be finite and nonnegative")
    if "tp_exit_fee" in costs:
        tp_fee, tp_slippage = costs["tp_exit_fee"], costs.get("tp_slippage_exit", 0.0)
        if any(not math.isfinite(v) or v < 0 for v in (tp_fee, tp_slippage)):
            raise ValueError("Costs must be finite and nonnegative")
        if outcome == 1:
            return entry_fee + values[1] + tp_fee + tp_slippage
    return entry_fee + exit_fee + values[1] + values[2]


def expected_cost(config, p_tp):
    """Probability-weighted exit costs; accepts scalar or vector TP probability."""
    market = round_trip_cost(config)
    if "tp_exit_fee" not in config.get("costs", {}):
        return market
    return market + p_tp * (round_trip_cost(config, outcome=1) - market)


def make_signal(ts_ms, coin, target, p_up, p_down, p_none, entry_price, config, p_move=None, p_side=None):
    """Build a signal. Optional entry_ms and timeout_mean_{long,short} are causal inputs."""
    probs = (p_up, p_down, p_none)
    if any(not math.isfinite(p) or p < 0 or p > 1 for p in probs):
        raise ValueError("Invalid probabilities")
    if not math.isclose(sum(probs), 1.0, abs_tol=1e-6):
        raise ValueError("Probabilities must sum to one")
    if p_move is not None or p_side is not None:
        move = 1 - p_none if p_move is None else p_move
        side_probability = p_up / (p_up + p_down) if p_side is None and p_up + p_down else 0.5
        if p_side is not None:
            side_probability = p_side
        if any(not math.isfinite(p) or not 0 <= p <= 1 for p in (move, side_probability)):
            raise ValueError("Invalid two-stage probabilities")
        p_up, p_down, p_none = move * side_probability, move * (1 - side_probability), 1 - move
    else:
        move = 1 - p_none
        side_probability = p_up / (p_up + p_down) if p_up + p_down else 0.5
    if not math.isfinite(entry_price) or entry_price <= 0:
        raise ValueError("entry_price must be positive")
    settings = config["targets"][target]
    barrier = settings["barrier"]
    direction = int(side_probability > 0.5) - int(side_probability < 0.5)
    side = "long" if direction > 0 else "short"
    timeout_mean = config.get(f"timeout_mean_{side}", 0.0)
    ev = abs(p_up - p_down) * barrier + p_none * timeout_mean - expected_cost(config, max(p_up, p_down))
    entry_ms = int(config.get("entry_ms", ts_ms + 60_000))
    return {
        "ts_ms": int(ts_ms), "coin": coin, "target": target,
        "p_up_first": p_up, "p_down_first": p_down, "p_none": p_none,
        "direction": direction, "p_win": max(side_probability, 1 - side_probability),
        "p_move": move, "entry_ms": entry_ms, "entry_price": entry_price,
        "tp_price": entry_price * (1 + direction * barrier),
        "sl_price": entry_price * (1 - direction * barrier),
        "time_stop_ms": entry_ms + int(settings["horizon_minutes"] * 60_000),
        "ev": ev, "trade": direction != 0 and ev > config.get("ev_threshold", 0.0),
    }
