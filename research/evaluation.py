"""Date isolation, bounded matrices, and diagnostics shared by offline evaluation."""
from datetime import date, datetime, timezone
import math
import os
import re

import numpy as np
import polars as pl

from research.threads import resolve_threads

DAY_NS = 86_400_000_000_000


def utc_day(tick):
    return datetime.fromtimestamp(int(tick) // 1_000_000_000, timezone.utc).date().isoformat()


def date_split(ticks, lockbox_pct=20, lockbox_from=None, embargo_days=1, max_horizon_s=1800,
               input_days=None):
    """Use input feature dates, independent of target selection and labels."""
    if not 0 < lockbox_pct < 100:
        raise ValueError('lockbox_pct must be between 0 and 100')
    if embargo_days < math.ceil(max_horizon_s / 86400):
        raise ValueError('embargo_days must be >= maximum horizon rounded up to days')
    days = (np.unique(np.asarray([(date.fromisoformat(day) - date(1970, 1, 1)).days
                                  for day in input_days], dtype=np.int64)) if input_days is not None
            else np.unique(np.asarray(ticks, dtype=np.int64) // DAY_NS))
    if not len(days):
        raise ValueError('No feature days')
    if lockbox_from:
        parsed = date.fromisoformat(lockbox_from)
        if parsed.isoformat() != lockbox_from:
            raise ValueError('lockbox_from must be YYYY-MM-DD')
        boundary = (parsed - date(1970, 1, 1)).days
    else:
        boundary = int(days[-max(1, math.ceil(len(days) * lockbox_pct / 100))])
    dev_end = boundary - embargo_days

    def span(selected):
        return {'first_day': utc_day(selected[0] * DAY_NS),
                'last_day': utc_day(selected[-1] * DAY_NS), 'days': len(selected)} if len(selected) else None

    return {'development': span(days[days < dev_end]),
            'embargo': {'first_day': utc_day(dev_end * DAY_NS),
                        'last_day': utc_day((boundary - 1) * DAY_NS), 'days': embargo_days},
            'lockbox': span(days[days >= boundary]),
            'lockbox_start_ns': boundary * DAY_NS, 'development_end_ns': dev_end * DAY_NS,
            'input_first_day': utc_day(days[0] * DAY_NS), 'input_last_day': utc_day(days[-1] * DAY_NS),
            'lockbox_pct': lockbox_pct, 'embargo_days': embargo_days}


def feature_array(matrix, features):
    """Identical Float64 conversion for training and live, including missing/inf."""
    x = matrix.select([(pl.col(c) if c in matrix.columns else pl.lit(None)).cast(pl.Float64).alias(c)
                       for c in features]).to_numpy().copy()
    x[~np.isfinite(x)] = np.nan
    return x


def memory_estimate(rows, features, max_memory_gb=None):
    estimated = int(rows) * int(features) * np.dtype(np.float64).itemsize
    if max_memory_gb is None:
        max_memory_gb = .7 * os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE') / 1024**3
    if not math.isfinite(max_memory_gb) or max_memory_gb <= 0:
        raise ValueError('max_memory_gb must be positive and finite')
    print(f'X memory estimate: {rows:,} rows × {features:,} features × 8 bytes = '
          f'{estimated / 1024**3:.3f} GiB (float64); limit {max_memory_gb:.3f} GiB', flush=True)
    if estimated > max_memory_gb * 1024**3:
        raise ValueError('X exceeds --max-memory-gb; increase --stride-s or use fewer days. '
                         'Estimate covers X only; input frames and LightGBM need additional RAM.')
    return {'rows': rows, 'features': features, 'bytes': estimated, 'dtype': 'float64',
            'max_memory_gb': max_memory_gb}


def day_block_permutation(ticks, seed=17):
    """Move whole UTC-day label blocks; keep each source day's internal order.

    Unequal/missing-day row counts are allowed: shuffled blocks are concatenated
    then assigned to the chronological training rows. Labels/directions stay paired.
    """
    days = np.asarray(ticks) // DAY_NS
    blocks = [np.flatnonzero(days == day) for day in np.unique(days)]
    if len(blocks) < 2:
        raise ValueError('Shuffle test needs at least two training UTC days')
    rng = np.random.default_rng(seed)
    # Fixed points retain label==feature leaks. Reject those permutations; a
    # rotation is a guaranteed nonidentity fallback even with unequal blocks.
    for _ in range(100):
        order = rng.permutation(len(blocks))
        permutation = np.concatenate([blocks[i] for i in order])
        if not np.any(permutation == np.arange(len(ticks))):
            return permutation
    return np.concatenate(blocks[1:] + blocks[:1])


def leak_check_metrics(result):
    # Daily effective sample count is deliberately conservative for overlapping
    # horizons and intraday autocorrelation; raw-row iid SE is also disclosed.
    n = result.get('effective_days', result['n'])
    se = 1 / math.sqrt(n - 1) if n > 1 else None
    ic = result.get('spearman_ic')
    return dict(result, ic_zero_se=se, se_formula='1/sqrt(N_days - 1); UTC test days as effective N '
                '(conservative block approximation, not an independence guarantee)',
                flag='LEAK SUSPECTED' if se is not None and ic is not None and abs(ic) > 3 * se
                else 'No leak flag' if se is not None and ic is not None else 'Insufficient data')


def feature_groups(name):
    group = re.sub(r'_\d+(?:s)?$', '', name)
    source = next((prefix for prefix in ('cal_', 'ctx_', 'cross_') if name.startswith(prefix)), None)
    if source is None:
        source = '_'.join(name.split('_')[:2]) if name.startswith(('binance_', 'okx_', 'kraken_', 'deribit_')) else 'other'
    return group, source


def model_importance(model, x, features, sample_size=50_000, num_threads=None):
    num_threads = resolve_threads(num_threads)
    # Chunk contribution tensors (multiclass has three copies) to bound scratch RAM.
    indices = np.sort(np.random.default_rng(17).choice(len(x), min(len(x), sample_size, 50_000), replace=False))
    total = np.zeros(len(features))
    for start in range(0, len(indices), 1024):
        values = np.asarray(model.predict(x[indices[start:start + 1024]], pred_contrib=True, num_threads=num_threads))
        values = values.reshape(len(values), -1, len(features) + 1)
        total += np.abs(values[:, :, :-1]).mean(axis=1).sum(axis=0)
    shap = total / max(1, len(indices))
    gain = model.feature_importance(importance_type='gain')
    records = [{'feature': f, 'gain': float(g), 'mean_abs_shap': float(s)} for f, g, s in zip(features, gain, shap)]
    groups, sources = {}, {}
    for item in records:
        group, source = feature_groups(item['feature'])
        for mapping, key in ((groups, group), (sources, source)):
            sums = mapping.setdefault(key, {'gain': 0., 'mean_abs_shap': 0.})
            for metric in sums:
                sums[metric] += item[metric]
    return {'sample_rows': len(indices), 'top_gain': sorted(records, key=lambda r: -r['gain'])[:30],
            'top_shap': sorted(records, key=lambda r: -r['mean_abs_shap'])[:30],
            'by_group': groups, 'by_source': sources,
            'notes': 'Mean absolute TreeSHAP; classifier averages absolute contributions across classes; bias excluded.'}
