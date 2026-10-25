"""Vectorized first-touch labels on UTC minute bars."""

import numpy as np
import polars as pl

from .bars import MINUTE_MS


def _timestamps(bars):
    ts = bars['ts_ms'].to_numpy()
    if bars['ts_ms'].null_count() or np.any(ts % MINUTE_MS) or np.any(np.diff(ts) <= 0):
        raise ValueError('bars must have sorted, unique UTC minute timestamps')
    return ts


def first_touch_labels(bars, barrier_pct, horizon_min, step_min=5, latency_min=1):
    """Return labels at UTC-aligned decisions from the first to last input bar.

    Percentages are fractions: 0.005 means 0.5%. Excursions are positive
    magnitudes, measured through the entire exit bar (intrabar order is unknown).
    ``minutes_to_touch`` is 1 for an entry-bar touch and null for a time stop.
    ``label_end_ms`` is the exclusive full-horizon end, even on early exit:
    all horizon bars must exist. Ties have null label/return but retain excursions.
    Invalid OHLC values also make a window incomplete. Memory usage is O(n),
    with one numpy pass per horizon offset, rather than an n-by-horizon matrix.
    """
    if not np.isfinite(barrier_pct) or not 0 < barrier_pct < 1:
        raise ValueError('barrier_pct must be a finite fraction between 0 and 1')
    for name, value, minimum in (('horizon_min', horizon_min, 1), ('step_min', step_min, 1),
                                 ('latency_min', latency_min, 0)):
        if not isinstance(value, (int, np.integer)) or value < minimum:
            raise ValueError(f'{name} must be an integer >= {minimum}')
    ts = _timestamps(bars)
    stride = step_min * MINUTE_MS
    decision = np.arange(((ts[0] + stride - 1) // stride) * stride, ts[-1] + 1, stride,
                         dtype=np.int64) if len(ts) else np.empty(0, dtype=np.int64)
    entry_ms = decision + latency_min * MINUTE_MS
    end_ms = entry_ms + horizon_min * MINUTE_MS
    size = len(decision)
    entry_idx = np.searchsorted(ts, entry_ms)
    complete = entry_idx + horizon_min <= len(ts)
    candidates = np.flatnonzero(complete)
    o, high, low, close = [bars[name].to_numpy() for name in ('open', 'high', 'low', 'close')]
    bad = (~np.isfinite(o) | ~np.isfinite(high) | ~np.isfinite(low) | ~np.isfinite(close)
           | (o <= 0) | (low <= 0) | (high < np.maximum(o, close))
           | (low > np.minimum(o, close)))
    bad_prefix = np.concatenate(([0], np.cumsum(bad)))
    i = entry_idx[candidates]
    complete[candidates] &= ((ts[i] == entry_ms[candidates])
                             & (ts[i + horizon_min - 1] == end_ms[candidates] - MINUTE_MS)
                             & (bad_prefix[i + horizon_min] == bad_prefix[i]))
    valid = np.flatnonzero(complete)
    entry = np.full(size, np.nan)
    entry[valid] = o[entry_idx[valid]]
    direction = np.zeros(size, dtype=np.int8)
    touch = np.zeros(size, dtype=np.int32)
    tie = np.zeros(size, dtype=bool)
    max_up, max_down, returns = [np.full(size, np.nan) for _ in range(3)]
    max_up[valid] = max_down[valid] = 0.
    active = valid
    for offset in range(horizon_min):
        if not len(active):
            break
        idx = entry_idx[active] + offset
        prices = entry[active]
        max_up[active] = np.maximum(max_up[active], high[idx] / prices - 1)
        max_down[active] = np.maximum(max_down[active], 1 - low[idx] / prices)
        up = high[idx] >= prices * (1 + barrier_pct)
        down = low[idx] <= prices * (1 - barrier_pct)
        hit = up | down
        touched = active[hit]
        direction[touched] = up[hit].astype(np.int8) - down[hit].astype(np.int8)
        touch[touched] = offset + 1
        tie[touched] = up[hit] & down[hit]
        returns[touched] = direction[touched] * barrier_pct
        active = active[~hit]
    returns[active] = close[entry_idx[active] + horizon_min - 1] / entry[active] - 1
    usable = complete & ~tie

    def nullable(name, values, mask):
        return pl.Series(name, values).set(pl.Series(~mask), None)

    return pl.DataFrame([
        pl.Series('ts_ms', decision), pl.Series('entry_ms', entry_ms),
        nullable('entry_price', entry, complete), nullable('label', direction, usable),
        nullable('minutes_to_touch', touch, complete & (touch > 0)),
        nullable('max_up_pct', max_up, complete), nullable('max_down_pct', max_down, complete),
        nullable('exit_return_pct', returns, usable), pl.Series('label_end_ms', end_ms),
        pl.Series('tie', tie), pl.Series('incomplete', ~complete),
    ])
