"""Small causal baselines; probabilities use up-first, down-first, none order."""

import numpy as np
import polars as pl

from .bars import MINUTE_MS
from .labels import _timestamps

PROB_COLUMNS = ('p_up_first', 'p_down_first', 'p_none')


def vol_only(bars, barrier_pct, horizon_min, lookback_min=60):
    """Zero-drift arithmetic Brownian exit from (-a, a), a=barrier_pct.

    One row per DECISION timestamp ts_ms. Join on ts_ms, never entry_ms:
    the latency bar is unavailable at the decision. Sigma is the RMS of the
    last lookback_min close-to-close log returns, strictly before ts_ms
    (61 completed bars for 60 returns). Gaps yield null probabilities.
    The survival series is 4/pi * sum((-1)^k/(2k+1) *
    exp(-(2k+1)^2*pi^2*sigma^2*horizon/(8*a^2))).
    """
    ts = _timestamps(bars)
    if not np.isfinite(barrier_pct) or not 0 < barrier_pct < 1:
        raise ValueError('barrier_pct must be a finite fraction between 0 and 1')
    if any(not isinstance(x, (int, np.integer)) or x < 1 for x in (horizon_min, lookback_min)):
        raise ValueError('horizon_min and lookback_min must be positive integers')
    variance = bars.select(pl.col('close').log().diff().pow(2)
                           .rolling_mean(lookback_min).shift(1)).to_series().to_numpy()
    contiguous = bars.select((pl.col('ts_ms') - pl.col('ts_ms').shift(lookback_min + 1)
                              == (lookback_min + 1) * MINUTE_MS)).to_series().fill_null(False).to_numpy()
    valid = contiguous & np.isfinite(variance) & (variance >= 0)
    scaled = variance[valid] * horizon_min / barrier_pct ** 2
    survival = np.ones(len(scaled))
    # Below this cutoff touch probability is < 4e-23; avoid cancellation.
    moving = scaled >= .01
    x = scaled[moving] * np.pi ** 2 / 8
    total = np.zeros(len(x))
    for k in range(64):
        odd = 2 * k + 1
        total += (-1.) ** k / odd * np.exp(-odd * odd * x)
    survival[moving] = np.clip(4 / np.pi * total, 0, 1)
    touch = 1 - survival
    probabilities = np.full((len(ts), 3), np.nan)
    probabilities[valid] = np.column_stack((touch / 2, touch / 2, survival))
    return pl.DataFrame({'ts_ms': ts, **{
        name: pl.Series(name, probabilities[:, i], nan_to_null=True)
        for i, name in enumerate(PROB_COLUMNS)
    }})


def prior(training_labels):
    """Class frequencies from an explicitly supplied training window; omit nulls."""
    labels = training_labels['label'] if isinstance(training_labels, pl.DataFrame) else training_labels
    labels = labels.drop_nulls()
    if not len(labels) or not labels.is_in([-1, 0, 1]).all():
        raise ValueError('training labels must contain at least one valid class')
    return dict(zip(PROB_COLUMNS, (float((labels == value).mean()) for value in (1, -1, 0))))


def momentum(bars, horizon_min):
    """Direction at decision ts_ms from closes strictly before ts_ms.

    Join on decision ts_ms, never entry_ms, to exclude the latency bar.
    """
    _timestamps(bars)
    if not isinstance(horizon_min, (int, np.integer)) or horizon_min < 1:
        raise ValueError('horizon_min must be a positive integer')
    return bars.select('ts_ms', pl.when(
        pl.col('ts_ms') - pl.col('ts_ms').shift(horizon_min + 1)
        == (horizon_min + 1) * MINUTE_MS
    ).then((pl.col('close').shift(1) / pl.col('close').shift(horizon_min + 1) - 1)
           .sign().cast(pl.Int8)).alias('direction'))


def reversal(bars, horizon_min):
    """Opposite of the momentum direction; for accuracy comparisons only."""
    return momentum(bars, horizon_min).with_columns(-pl.col('direction'))


def join_baselines(decisions, bars, target, barrier_pct, horizon_min):
    """Attach forecasts on DECISION ts_ms, regardless of entry_ms/latency.

    Every forecast uses only bars whose close is available by the decision.
    """
    probabilities = vol_only(bars, barrier_pct, horizon_min).rename(dict(zip(
        PROB_COLUMNS, (f'vol_p_up_{target}', f'vol_p_down_{target}', f'vol_p_none_{target}'))))
    directions = momentum(bars, horizon_min).rename({'direction': f'mom_dir_{target}'})
    return decisions.join(probabilities, on='ts_ms', how='left').join(
        directions, on='ts_ms', how='left').with_columns(
            (-pl.col(f'mom_dir_{target}')).alias(f'rev_dir_{target}'))
