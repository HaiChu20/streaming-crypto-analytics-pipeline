"""In-memory v3 feature benchmark; no market data, labels or disk output.

Run with PYTHONPATH=. python tests/strategy2/bench_features.py --years 3
Times exclude synthetic fixture construction and include all K/L features.
"""

import argparse
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import numpy as np
import polars as pl

from strategy2.bars import MINUTE_MS
from strategy2.dataset import _peak_mb
from strategy2.features import ALL_COINS, COINS, build_features
from strategy2.sources import load_sources
from test_features import START, synthetic_aggflow, synthetic_bars


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--years', type=float, default=3)
    parser.add_argument('--coins', nargs='+', choices=ALL_COINS, default=ALL_COINS)
    parser.add_argument('--step-min', type=int, default=5)
    args = parser.parse_args()
    n = int(args.years * 365 * 1440)
    if n < 1 or args.step_min < 1:
        parser.error('years and step-min must be positive')
    end = START + n * MINUTE_MS
    # A fresh, nonexistent worktree-local path supplies auxiliary schemas.
    # Populate all of them synthetically, with their real availability delays.
    root = Path(__file__).parent / '.tmp' / f'feature-bench-{uuid4().hex}'
    sources = load_sources(root, 'BTC', START, end)
    for name, stride, delay, scale in (('premium', 1, 1, .0001), ('metrics', 5, 5, 1000.),
            ('depth', 5, .5, 1000.), ('dvol', 60, 60, 40.)):
        x = np.arange(0, n, stride)
        stamps = START + x * MINUTE_MS
        values = {'ts_ms': stamps, 'available_ms': stamps + int(delay * MINUTE_MS)}
        values.update({column: scale * (1 + .1 * np.sin(x / 137. + i))
                       for i, column in enumerate(sources[name].columns)
                       if column not in values})
        sources[name] = pl.DataFrame(values, schema=sources[name].schema)
    stamps = START + np.arange(0, n, 480) * MINUTE_MS
    sources['funding'] = pl.DataFrame({'ts_ms': stamps, 'available_ms': stamps,
        'last_funding_rate': .0001 + .00002 * np.sin(np.arange(len(stamps))),
        'funding_interval_ms': np.full(len(stamps), 480 * MINUTE_MS),
        'next_funding_ms': stamps + 480 * MINUTE_MS})
    bars = synthetic_bars(n, .0000001)
    sources.update(perp=bars, spot=bars, aggflow=synthetic_aggflow(n))
    sources['coinbase'] = bars.select('ts_ms', (pl.col('ts_ms') + MINUTE_MS).alias('available_ms'),
        (pl.col('close') * pl.Series(np.exp(.0001 + .00001 * np.sin(np.arange(n) / 137.)))).alias('close'),
        (pl.col('volume') * .2).alias('volume'))
    market = {coin: bars.select('ts_ms', 'close') for coin in COINS}
    coverage = pl.DataFrame({'start_ms': [START], 'end_ms': [end]})
    for coin in args.coins:
        started = perf_counter()
        frame = build_features(coin, sources, start=START, end=end, step_min=args.step_min,
            btc_bars=bars, eth_bars=bars, market_bars=market, version='v3', aggflow_coverage=coverage)
        print(f'{coin}: {perf_counter() - started:.3f}s, bars={n}, decisions={frame.height}, '
              f'features={sum(name.startswith("f_") for name in frame.columns)}, '
              f'peak_rss_mb={_peak_mb():.1f}', flush=True)
        del frame


if __name__ == '__main__':
    main()
