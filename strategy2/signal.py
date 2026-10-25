"""Pure Strategy 2 signal records; all returns and costs are fractions."""

import math


def round_trip_cost(config):
    costs = config.get("costs", {})
    values = [costs.get("taker_fee", 0.0005), costs.get("slippage_entry", 0.0001),
              costs.get("slippage_exit", 0.0002)]
    if any(not math.isfinite(v) or v < 0 for v in values):
        raise ValueError("Costs must be finite and nonnegative")
    return 2 * values[0] + values[1] + values[2]


def make_signal(ts_ms, coin, target, p_up, p_down, p_none, entry_price, config):
    """Build a signal. Optional entry_ms and timeout_mean_{long,short} are causal inputs."""
    probs = (p_up, p_down, p_none)
    if any(not math.isfinite(p) or p < 0 or p > 1 for p in probs):
        raise ValueError("Invalid probabilities")
    if not math.isclose(sum(probs), 1.0, abs_tol=1e-6):
        raise ValueError("Probabilities must sum to one")
    if not math.isfinite(entry_price) or entry_price <= 0:
        raise ValueError("entry_price must be positive")
    settings = config["targets"][target]
    barrier = settings["barrier"]
    direction = int(p_up > p_down) - int(p_up < p_down)
    side = "long" if direction > 0 else "short"
    timeout_mean = config.get(f"timeout_mean_{side}", 0.0)
    ev = abs(p_up - p_down) * barrier + p_none * timeout_mean - round_trip_cost(config)
    entry_ms = int(config.get("entry_ms", ts_ms + 60_000))
    return {
        "ts_ms": int(ts_ms), "coin": coin, "target": target,
        "p_up_first": p_up, "p_down_first": p_down, "p_none": p_none,
        "direction": direction, "p_win": max(p_up, p_down) / (p_up + p_down)
        if p_up + p_down > 0 else 0.5,
        "p_move": 1 - p_none, "entry_ms": entry_ms, "entry_price": entry_price,
        "tp_price": entry_price * (1 + direction * barrier),
        "sl_price": entry_price * (1 - direction * barrier),
        "time_stop_ms": entry_ms + int(settings["horizon_minutes"] * 60_000),
        "ev": ev, "trade": direction != 0 and ev > config.get("ev_threshold", 0.0),
    }
