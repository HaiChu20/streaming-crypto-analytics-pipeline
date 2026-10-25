"""Reproduce Strategy 2 barrier base rates from downloaded Binance minute bars."""

import argparse
from time import perf_counter

import polars as pl

from .bars import load_bars
from .labels import first_touch_labels


def median_hour_range(bars):
    """Median trailing 60-minute (high-low)/first-open, excluding gaps."""
    ranges = bars.select(pl.when(pl.col('ts_ms') - pl.col('ts_ms').shift(59) == 59 * 60_000)
                         .then((pl.col('high').rolling_max(60) - pl.col('low').rolling_min(60))
                               / pl.col('open').shift(59)).alias('range'))
    return ranges['range'].median()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--from', dest='start', required=True, help='first UTC day, inclusive')
    parser.add_argument('--to', dest='end', required=True, help='last UTC day, inclusive')
    parser.add_argument('--coins', nargs='+', default=['BTC', 'ETH', 'SOL', 'XRP', 'DOGE'])
    parser.add_argument('--market', choices=['perp', 'spot'], default='perp')
    parser.add_argument('--step-min', type=int, default=5)
    parser.add_argument('--latency-min', type=int, default=1)
    args = parser.parse_args(argv)
    started = perf_counter()
    print(f'UTC {args.start} through {args.end}; step={args.step_min}m latency={args.latency_min}m')
    print('Rates use complete non-tie labels; tie % uses all complete windows. Up share = up/touched.')
    print('coin target       touched%     up%   down%    tie% up_share% median_1h_range% valid incomplete')
    for coin in args.coins:
        bars = load_bars(args.root, args.market, coin, args.start, args.end)
        if not bars.height:
            raise ValueError(f'{coin}: no bars in requested range')
        hour_range = median_hour_range(bars)
        for barrier, horizon in ((.005, 60), (.01, 240)):
            labels = first_touch_labels(bars, barrier, horizon, args.step_min, args.latency_min)
            valid = labels['label'].drop_nulls()
            up = (valid == 1).sum()
            down = (valid == -1).sum()
            count = len(valid)
            complete = labels.height - labels['incomplete'].sum()
            pct = lambda number, denominator: 100 * number / denominator if denominator else float('nan')
            print(f'{coin:5} {100 * barrier:g}%/{horizon:<4}m '
                  f'{pct(up + down, count):9.3f} {pct(up, count):7.3f} {pct(down, count):7.3f} '
                  f'{pct(labels["tie"].sum(), complete):7.3f} {pct(up, up + down):9.3f} '
                  f'{100 * hour_range if hour_range is not None else float("nan"):16.3f} '
                  f'{count:6} {labels["incomplete"].sum():10}')
    print(f'Runtime: {perf_counter() - started:.3f}s')


if __name__ == '__main__':
    main()
