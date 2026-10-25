"""Build the causal s2-features-v1 Parquet contract, processing one coin at a time."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import resource
import subprocess
import sys
from time import perf_counter

import polars as pl

from .bars import MINUTE_MS, _boundary_ms, load_bars
from .baselines import join_baselines
from .features import COINS, FEATURE_GROUPS, build_features
from .labels import first_touch_labels
from .sources import coverage, load_sources

VERSION = 's2-features-v1'
TARGETS = {'a': (.005, 60), 'b': (.01, 240)}


def _peak_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 ** 2 if sys.platform == 'darwin' else 1024)


def build_coin(root, coin, start, end, *, step_min=5, latency_min=1,
               btc_bars=None, eth_bars=None):
    """Return (dataset, coverage). Load 31 preceding days for rolling warmup.

    Features and baselines are joined on decision ts_ms; entry_ms is reserved
    for labels and the simulator. The requested last UTC day is inclusive.
    Labels whose full future window is unavailable remain null.
    """
    start_ms, end_ms = _boundary_ms(start), _boundary_ms(end, end=True)
    warmup_ms = start_ms - 31 * 1440 * MINUTE_MS
    sources = load_sources(root, coin, warmup_ms, end_ms)
    bars = sources['perp']
    if not bars.height:
        raise ValueError(f'{coin}: no perpetual bars in requested range')
    if btc_bars is None:
        btc_bars = load_bars(root, 'perp', 'BTC', warmup_ms, end_ms)
    if eth_bars is None:
        eth_bars = load_bars(root, 'perp', 'ETH', warmup_ms, end_ms)
    frame = build_features(coin, sources, start=warmup_ms, end=end_ms,
                           step_min=step_min, btc_bars=btc_bars, eth_bars=eth_bars)
    frame = frame.filter(pl.col('ts_ms').is_between(start_ms, end_ms, closed='left'))
    frame = frame.with_columns((pl.col('ts_ms') + latency_min * MINUTE_MS).alias('entry_ms'))
    for target, (barrier, horizon) in TARGETS.items():
        labels = first_touch_labels(bars, barrier, horizon, step_min, latency_min)
        names = {'label': f'label_{target}', 'exit_return_pct': f'exit_return_{target}',
                 'minutes_to_touch': f'minutes_to_touch_{target}', 'max_up_pct': f'max_up_{target}',
                 'max_down_pct': f'max_down_{target}', 'label_end_ms': f'label_end_ms_{target}',
                 'entry_price': f'entry_price_{target}'}
        labels = labels.select('ts_ms', *names).rename(names)
        frame = frame.join(labels, on='ts_ms', how='left')
        frame = frame.with_columns((pl.col('entry_ms') + horizon * MINUTE_MS).alias(f'label_end_ms_{target}'))
        frame = join_baselines(frame, bars, target, barrier, horizon)
    return frame.sort('ts_ms'), coverage(sources)


def build_dataset(root, out, start, end, coins=COINS, *, step_min=5, latency_min=1):
    """Write one zstd Parquet per coin and a reproducibility/coverage report."""
    if not coins or set(coins) - set(COINS):
        raise ValueError(f'coins must be selected from {COINS}')
    if end is None or _boundary_ms(end, end=True) <= _boundary_ms(start):
        raise ValueError('end must be after start')
    started = perf_counter()
    folder = Path(out) / VERSION
    folder.mkdir(parents=True, exist_ok=True)
    metadata = {'version': VERSION, 'features': [{'name': name, 'group': group}
                for name, group in FEATURE_GROUPS.items()], 'feature_columns': list(FEATURE_GROUPS),
                'git_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                'git_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], text=True).strip()),
                'created_utc': datetime.now(timezone.utc).isoformat(),
                'parameters': {'from': str(start), 'to': str(end), 'coins': list(coins),
                               'step_min': step_min, 'latency_min': latency_min, 'warmup_days': 31,
                               'minimum_window_coverage': .9,
                               'staleness_clock': 'availability', 'returns': 'log; label returns are simple fractions'},
                'coins': {}}
    warmup_ms = _boundary_ms(start) - 31 * 1440 * MINUTE_MS
    end_ms = _boundary_ms(end, end=True)
    # Reuse only two compact OHLCV context frames, never full coin feature tables.
    btc = load_bars(root, 'perp', 'BTC', warmup_ms, end_ms)
    eth = load_bars(root, 'perp', 'ETH', warmup_ms, end_ms)
    for coin in dict.fromkeys(coins):
        coin_started = perf_counter()
        frame, sources = build_coin(root, coin, start, end, step_min=step_min,
                                    latency_min=latency_min, btc_bars=btc, eth_bars=eth)
        path = folder / f'{coin}.parquet'
        frame.write_parquet(path, compression='zstd')
        nulls = {name: frame[name].null_count() / frame.height if frame.height else 1.
                 for name in FEATURE_GROUPS}
        group_nulls = {group: sum(value for name, value in nulls.items() if FEATURE_GROUPS[name] == group)
                      / sum(g == group for g in FEATURE_GROUPS.values()) for group in 'ABCDEFGH'}
        shares = {}
        for target in TARGETS:
            labels = frame[f'label_{target}'].drop_nulls()
            shares[target] = {str(label): float((labels == label).mean()) if len(labels) else None
                              for label in (-1, 0, 1)}
        stats = {'rows': frame.height, 'first_ts_ms': frame['ts_ms'].min(), 'last_ts_ms': frame['ts_ms'].max(),
                 'sources': sources, 'null_share': nulls, 'null_share_by_group': group_nulls,
                 'class_shares': shares, 'label_null_share': {t: frame[f'label_{t}'].null_count() / frame.height
                                                           for t in TARGETS},
                 'build_seconds': perf_counter() - coin_started}
        metadata['coins'][coin] = stats
        print(f'{coin}: {frame.height} rows, {stats["build_seconds"]:.3f}s, '
              f'group null shares={json.dumps(group_nulls)}, labels={json.dumps(shares)}', flush=True)
        del frame
    metadata['runtime_seconds'] = perf_counter() - started
    metadata['peak_memory_mb'] = _peak_mb()
    (folder / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    print(f'Runtime: {metadata["runtime_seconds"]:.3f}s; peak RSS: {metadata["peak_memory_mb"]:.1f} MiB', flush=True)
    return metadata


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--from', dest='start', required=True)
    parser.add_argument('--to', dest='end', required=True)
    parser.add_argument('--coins', nargs='+', default=list(COINS))
    parser.add_argument('--step-min', type=int, default=5)
    parser.add_argument('--latency-min', type=int, default=1)
    args = parser.parse_args(argv)
    coins = tuple(coin.upper() for part in args.coins for coin in part.split(','))
    build_dataset(args.root, args.out, args.start, args.end, coins,
                  step_min=args.step_min, latency_min=args.latency_min)


if __name__ == '__main__':
    main()
