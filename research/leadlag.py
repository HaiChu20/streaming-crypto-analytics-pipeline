"""Receive-clock lead/lag on wide, fixed-grid mid Parquet files (offline only)."""

import argparse
from datetime import datetime, timezone
from itertools import combinations
import json
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq

from research.evaluation import moving_day_blocks

NS = 1_000_000_000
DAY_NS = 86400 * NS
HORIZONS_S = (0, .1, .5, 1, 2, 5, 10)


def _mean_curves(curves):
    values = np.asarray(curves, dtype=float)
    counts = np.isfinite(values).sum(axis=0)
    return np.divide(np.nansum(values, axis=0), counts,
                     out=np.full(values.shape[1:], np.nan), where=counts > 0)


def lag_stats(x, y, steps, source_start=0, source_end=None):
    """Overlap Pearson sufficient statistics; positive k compares x[t] to y[t+k].

    Null intervals on either instrument mask both returns at that grid position.
    Positions are never compressed. Restrict the first return to a source hour;
    the follower can lie in an adjacent hour, but never an adjacent day.
    FFTs make the cost independent of the requested number of lags.
    """
    source_end = len(x) if source_end is None else source_end
    valid = np.isfinite(x) & np.isfinite(y)
    mx = valid.copy()
    mx[:source_start] = False
    mx[source_end:] = False
    width = 2 * steps + 1
    if not mx.any():
        return np.zeros((6, width))
    # Bps scaling keeps variances away from floating point cancellation noise.
    a = np.where(mx, x * 10000, 0.)
    b = np.where(valid, y * 10000, 0.)
    size = 1 << (2 * len(x) - 1).bit_length()
    fa, fb, fm, fn, faa, fbb = (
        np.fft.rfft(v, n=size) for v in (a, b, mx, valid, a * a, b * b))
    indices = np.arange(-steps, steps + 1) % size
    def cross(first, second):
        return np.fft.irfft(first.conj() * second, n=size)[indices]
    result = np.stack((cross(fm, fn), cross(fa, fn), cross(fm, fb),
                       cross(faa, fn), cross(fm, fbb), cross(fa, fb)))
    result[0] = np.rint(result[0]).clip(0)
    # FFT indices wrap when max lag exceeds the segment length.
    result[:, np.abs(np.arange(-steps, steps + 1)) >= len(x)] = 0
    return result


def correlation(stats):
    n, sx, sy, sxx, syy, sxy = stats
    divisor = np.maximum(n, 1)
    vx = np.maximum(sxx - sx * sx / divisor, 0)
    vy = np.maximum(syy - sy * sy / divisor, 0)
    denom = np.sqrt(vx * vy)
    # Constant series and roundoff-sized variances have undefined correlations.
    valid = (n >= 3) & (vx > 1e-12) & (vy > 1e-12)
    return np.divide(sxy - sx * sy / divisor, denom,
                     out=np.full(n.shape, np.nan), where=valid).clip(-1, 1)


def llr(curve):
    curve = np.asarray(curve, dtype=float)
    middle = len(curve) // 2
    positive, negative = curve[middle + 1:], curve[:middle]
    if not np.isfinite(positive).any() or not np.isfinite(negative).any():
        return np.nan
    denominator = np.nansum(negative ** 2)
    return np.nansum(positive ** 2) / denominator if denominator > 0 else np.nan


def summarize(curve, lags_ms):
    finite = np.flatnonzero(np.isfinite(curve))
    # Strongest absolute association; ties favor the smallest absolute lag.
    peak = min(finite, key=lambda i: (-abs(curve[i]), abs(lags_ms[i]), lags_ms[i])) if len(finite) else None
    return {'c': curve, 'peak_lag_ms': None if peak is None else int(lags_ms[peak]),
            'c_peak': np.nan if peak is None else curve[peak],
            'c0': curve[len(curve) // 2], 'llr': llr(curve)}


def response_stats(leader, follower, grid_ns, threshold_bps=2.):
    """Signed cumulative follower log bps from t-1 to t+h after a shock at t.

    Each horizon requires a complete follower path; no crossing nulls or days.
    The 0s fraction uses the same events as the final (10s) horizon.
    Nonintegral horizons use the first grid point at or after the horizon.
    """
    change = np.diff(leader, prepend=np.nan) * 10000
    events = np.flatnonzero(np.isfinite(change) & (np.abs(change) >= threshold_bps))
    events = events[events > 0]
    signs = np.sign(change[events])
    missing = np.r_[0, np.cumsum(~np.isfinite(follower))]
    sums, counts = [], []
    simultaneous_final = 0.
    for horizon in HORIZONS_S:
        offset = int(np.ceil(horizon * NS / grid_ns))
        end = events + offset
        fits = end < len(follower)
        starts, ends, directions = events[fits] - 1, end[fits], signs[fits]
        valid = missing[ends + 1] == missing[starts]
        starts, ends, directions = starts[valid], ends[valid], directions[valid]
        moves = (follower[ends] - follower[starts]) * directions * 10000
        sums.append(float(moves.sum()))
        counts.append(len(moves))
        if horizon == HORIZONS_S[-1]:
            simultaneous_final = float(((follower[starts + 1] - follower[starts]) * directions * 10000).sum())
    return np.array(sums), np.array(counts), simultaneous_final


def response_summary(stats, grid_ns):
    sums, counts, simultaneous = stats
    means = np.divide(sums, counts, out=np.full(len(counts), np.nan), where=counts > 0)
    return {'horizons_s': list(HORIZONS_S),
            'effective_horizons_ms': [int(np.ceil(h * NS / grid_ns)) * grid_ns / 1e6 for h in HORIZONS_S],
            'mean_signed_bps': means, 'events': counts,
            'fraction_already_done_0s': simultaneous / sums[-1] if counts[-1] and sums[-1] != 0 else np.nan}


def _bootstrap(curves, peak):
    curves = np.asarray(curves)
    sampled = moving_day_blocks(len(curves), 1, draws=1000, seed=17)
    weights = np.zeros((len(sampled), len(curves)))
    np.add.at(weights, (np.arange(len(sampled))[:, None], sampled), 1)
    counts = weights @ np.isfinite(curves)
    means = np.divide(weights @ np.nan_to_num(curves), counts,
                      out=np.full(counts.shape, np.nan), where=counts > 0)
    ratios = np.array([llr(c) for c in means])
    def interval(values):
        values = values[np.isfinite(values)]
        return {'percentiles': [2.5, 50, 97.5],
                'values': np.percentile(values, [2.5, 50, 97.5]) if len(values) else [None] * 3,
                'valid_draws': len(values)}
    return {'draws': 1000, 'seed': 17, 'block_days': 1,
            'llr': interval(ratios),
            'c_peak': interval(means[:, peak] if peak is not None else np.array([]))}


def discover(inputs):
    paths = set()
    for value in inputs:
        path = Path(value)
        if path.is_file():
            paths.add(path.resolve())
        elif path.is_dir():
            found = list(path.glob('date=*/mids.parquet')) + list(path.glob('mids/date=*/mids.parquet'))
            if (path / 'mids.parquet').is_file():
                found.append(path / 'mids.parquet')
            paths.update(p.resolve() for p in found)
        else:
            raise ValueError(f'Mids input does not exist: {path}')
    if not paths:
        raise ValueError('No mids.parquet files found')
    return sorted(paths)


def analyze(inputs, max_lag_ms=10000, pairs='auto', threshold_bps=2.):
    if max_lag_ms < 1 or not np.isfinite(threshold_bps) or threshold_bps <= 0:
        raise ValueError('Need positive max lag and shock threshold')
    paths = discover(inputs)
    by_day, columns, grids, modes = {}, set(), set(), set()
    for path in paths:
        scan = pl.scan_parquet(path)
        schema = scan.collect_schema()
        if schema.get('tick_ns') != pl.Int64:
            raise ValueError(f'{path}: tick_ns must be Int64')
        mids = {c for c in schema if c.endswith('_mid_e9')}
        if not mids or any(schema[c] != pl.Int64 for c in mids):
            raise ValueError(f'{path}: expected Int64 mid_e9 columns')
        columns.update(mids)
        metadata = pq.read_metadata(path).metadata or {}
        if b'mid_grid_ms' in metadata:
            grids.add(int(metadata[b'mid_grid_ms']) * 1_000_000)
        modes.add(metadata.get(b'mode', b'ideal').decode())
        days = scan.select((pl.col('tick_ns') // DAY_NS).unique()).collect().to_series().to_list()
        for day in days:
            by_day.setdefault(day, []).append(path)
    if not by_day:
        raise ValueError('No grid rows found')
    if None in by_day:
        raise ValueError('tick_ns must not contain nulls')
    if len(modes) > 1 or len(grids) > 1:
        raise ValueError('Cannot mix replay modes or grid spacings')
    names = sorted(c.removesuffix('_mid_e9') for c in columns)
    if pairs == 'auto' or pairs == ['auto']:
        selected = list(combinations(names, 2))
    else:
        selected = [tuple(p.split(':')) for p in pairs]
        if any(len(p) != 2 or p[0] == p[1] or any(n not in names for n in p) for p in selected):
            raise ValueError('--pairs expects FIRST:SECOND using column names without _mid_e9')
        selected = list(dict.fromkeys(selected))
    if not selected:
        raise ValueError('Need at least two instruments')
    needed = sorted({n for pair in selected for n in pair})
    output = {pair: {'first': pair[0], 'second': pair[1], 'days': [], '_curves': [],
                     '_hourly': None, '_responses': None} for pair in selected}
    grid_ns = next(iter(grids), None)
    for day, files in sorted(by_day.items()):
        frames = []
        for path in files:
            scan = pl.scan_parquet(path)
            schema = scan.collect_schema()
            frames.append(scan.filter((pl.col('tick_ns') // DAY_NS) == day).select(
                'tick_ns', *[pl.col(n + '_mid_e9') if n + '_mid_e9' in schema
                            else pl.lit(None, dtype=pl.Int64).alias(n + '_mid_e9') for n in needed]).collect())
        frame = pl.concat(frames).sort('tick_ns')
        ticks = frame['tick_ns'].to_numpy()
        differences = np.diff(ticks)
        if len(differences):
            step = int(differences[0])
            if step <= 0 or np.any(differences != step) or (grid_ns is not None and step != grid_ns):
                raise ValueError(f'{files}: duplicate, missing or inconsistent grid ticks')
            grid_ns = step
        if grid_ns is None or grid_ns <= 0 or np.any(ticks % grid_ns):
            raise ValueError('Need at least two aligned grid ticks or mid_grid_ms metadata')
        steps = int(max_lag_ms * 1_000_000 // grid_ns)
        if steps < 1:
            raise ValueError('--max-lag-ms must cover at least one grid step')
        lags_ms = np.arange(-steps, steps + 1) * grid_ns / 1e6
        logs = {}
        for name in needed:
            values = frame[name + '_mid_e9'].to_numpy().astype(float)
            values[values <= 0] = np.nan
            logs[name] = np.log(values)
        del frame, frames
        returns = {n: np.diff(v, prepend=np.nan) for n, v in logs.items()}
        hours = (ticks % DAY_NS) // (3600 * NS)
        date = datetime.fromtimestamp(day * 86400, timezone.utc).date().isoformat()
        for pair, result in output.items():
            x, y = (returns[n] for n in pair)
            hourly = np.zeros((24, 6, 2 * steps + 1))
            for hour in range(24):
                a, b = np.searchsorted(hours, [hour, hour + 1])
                if a == b:
                    continue
                lo, hi = max(0, a - steps), min(len(x), b + steps)
                hourly[hour] = lag_stats(x[lo:hi], y[lo:hi], steps, a - lo, b - lo)
            curve = correlation(hourly.sum(axis=0))
            responses = [response_stats(logs[a], logs[b], grid_ns, threshold_bps)
                         for a, b in (pair, pair[::-1])]
            result['days'].append({'date': date, **summarize(curve, lags_ms),
                                   'n': hourly.sum(axis=0)[0].astype(np.int64),
                                   'response_first_leads': response_summary(responses[0], grid_ns),
                                   'response_second_leads': response_summary(responses[1], grid_ns)})
            result['_curves'].append(curve)
            if result['_hourly'] is None:
                result['_hourly'] = hourly
                result['_responses'] = responses
            else:
                result['_hourly'] += hourly
                result['_responses'] = [tuple(a + b for a, b in zip(old, new))
                                        for old, new in zip(result['_responses'], responses)]
    for result in output.values():
        curves = result.pop('_curves')
        pooled = _mean_curves(curves)
        result['pooled'] = summarize(pooled, lags_ms)
        lag = result['pooled']['peak_lag_ms']
        peak = None if lag is None else int(np.argmin(abs(lags_ms - lag)))
        result['pooled']['bootstrap'] = _bootstrap(curves, peak)
        result['pooled']['hourly'] = [{'hour_utc': h, **summarize(correlation(stats), lags_ms)}
                                    for h, stats in enumerate(result.pop('_hourly'))]
        responses = result.pop('_responses')
        result['pooled']['response_first_leads'] = response_summary(responses[0], grid_ns)
        result['pooled']['response_second_leads'] = response_summary(responses[1], grid_ns)
        result['pooled']['leader'] = None if lag is None or lag == 0 else result['first' if lag > 0 else 'second']
    return clean_json({'grid_ms': grid_ns / 1e6, 'lags_ms': lags_ms,
                       'replay_mode': next(iter(modes)), 'max_lag_ms': max_lag_ms,
                       'positive_lag': 'first leads second', 'peak_rule': 'maximum absolute correlation',
                       'threshold_bps': threshold_bps, 'files': [str(p) for p in paths],
                       'pairs': list(output.values())})


def clean_json(value):
    if isinstance(value, np.ndarray):
        return clean_json(value.tolist())
    if isinstance(value, dict):
        return {k: clean_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean_json(v) for v in value]
    if isinstance(value, (float, np.floating)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, np.integer):
        return int(value)
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inputs', nargs='+')
    parser.add_argument('--max-lag-ms', type=int, default=10000)
    parser.add_argument('--pairs', nargs='+', default=['auto'], help='auto or FIRST:SECOND (omit _mid_e9)')
    parser.add_argument('--threshold-bps', type=float, default=2.)
    parser.add_argument('--json-out', type=Path)
    args = parser.parse_args(argv)
    try:
        report = analyze(args.inputs, args.max_lag_ms, args.pairs, args.threshold_bps)
    except (ValueError, OSError, pl.exceptions.PolarsError) as error:
        parser.error(str(error))
    print(f"{'first → second':70} {'days':>4} {'peak ms':>8} {'c(0)':>8} {'c(peak)':>8} {'LLR':>9}")
    def fmt(value):
        return 'n/a' if value is None else f'{value:.4g}'
    for pair in report['pairs']:
        stats = pair['pooled']
        print(f"{pair['first'] + ' → ' + pair['second']:70} {len(pair['days']):4} "
              f"{fmt(stats['peak_lag_ms']):>8} {fmt(stats['c0']):>8} {fmt(stats['c_peak']):>8} {fmt(stats['llr']):>9}")
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    return report


if __name__ == '__main__':
    main()
