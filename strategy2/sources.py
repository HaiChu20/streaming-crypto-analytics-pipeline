"""Typed public-history loaders. Every observation carries its availability."""

from pathlib import Path
from zipfile import ZipFile

import polars as pl

from .bars import MINUTE_MS, _boundary_ms, _read_zip, load_bars

HOUR_MS = 60 * MINUTE_MS
METRICS = ('sum_open_interest', 'sum_open_interest_value',
           'count_toptrader_long_short_ratio', 'sum_toptrader_long_short_ratio',
           'count_long_short_ratio', 'sum_taker_long_short_vol_ratio')
BANDS = (.2, 1, 2, 5)
DEPTH_COLUMNS = tuple(f'{side}_{str(band).replace(".", "_")}'
                      for band in BANDS for side in ('bid', 'ask'))


def _symbol(coin):
    symbol = coin.upper()
    if not symbol.isalnum():
        raise ValueError('invalid coin')
    return symbol if symbol.endswith('USDT') else symbol + 'USDT'


def _files(root, kind, coin, start, end, *, minute=False):
    symbol = _symbol(coin)
    first = pl.Series([_boundary_ms(start)]).cast(pl.Datetime('ms', 'UTC')).dt.strftime('%Y-%m-%d')[0]
    last = pl.Series([_boundary_ms(end, end=True) - 1]).cast(pl.Datetime('ms', 'UTC')).dt.strftime('%Y-%m-%d')[0]
    base = Path(root) / 'binance/futures/um'
    for cadence in ('monthly', 'daily'):
        folder = base / cadence / kind / symbol
        if minute:
            folder /= '1m'
        for path in sorted(folder.glob('*.zip')):
            prefix = f'{symbol}-1m-' if minute else f'{symbol}-{kind}-'
            stamp = path.stem.removeprefix(prefix)
            left, right = (first[:7], last[:7]) if cadence == 'monthly' else (first, last)
            if left <= stamp <= right:
                yield path


def _csv(path):
    with ZipFile(path) as archive:
        names = [name for name in archive.namelist() if name.endswith('.csv')]
        if len(names) != 1:
            raise ValueError(f'{path}: expected one CSV')
        with archive.open(names[0]) as source:
            try:
                return pl.read_csv(source, infer_schema_length=10000)
            except pl.exceptions.NoDataError:
                return pl.DataFrame()


def _finish(frames, columns, start, end, delay=0):
    schema = {'ts_ms': pl.Int64, **{name: pl.Float64 for name in columns}}
    frame = pl.concat(frames, how='vertical_relaxed') if frames else pl.DataFrame(schema=schema)
    frame = frame.select(*[pl.col(name).cast(dtype) for name, dtype in schema.items()])
    frame = frame.filter(pl.col('ts_ms').is_between(_boundary_ms(start), _boundary_ms(end, end=True), closed='left')).unique()
    if frame['ts_ms'].is_duplicated().any():
        raise ValueError('conflicting duplicate source timestamp')
    return frame.sort('ts_ms').with_columns((pl.col('ts_ms') + delay).alias('available_ms'))


def load_premium(root, coin, start, end):
    frames = [_read_zip(path).select('ts_ms', pl.col('close').alias('premium'))
              for path in _files(root, 'premiumIndexKlines', coin, start, end, minute=True)]
    return _finish(frames, ['premium'], start, end, MINUTE_MS)


def load_metrics(root, coin, start, end):
    frames = []
    for path in _files(root, 'metrics', coin, start, end):
        frame = _csv(path)
        if frame.height:
            frames.append(frame.select(pl.col('create_time').str.to_datetime('%Y-%m-%d %H:%M:%S', time_zone='UTC')
                                       .dt.epoch('ms').alias('ts_ms'), *METRICS))
    return _finish(frames, METRICS, start, end, 5 * MINUTE_MS)


def load_depth(root, coin, start, end):
    frames = []
    for path in _files(root, 'bookDepth', coin, start, end):
        frame = _csv(path)
        if not frame.height:
            continue
        frame = frame.with_columns(pl.col('percentage').cast(pl.Float64)).filter(
            pl.col('percentage').abs().is_in(BANDS))
        # Aggregate each daily file before concatenation; full depth history is large.
        frame = frame.select(pl.col('timestamp').str.to_datetime('%Y-%m-%d %H:%M:%S', time_zone='UTC')
                             .dt.epoch('ms').alias('ts_ms'), 'percentage',
                             pl.col('notional').cast(pl.Float64)).unique()
        if frame.select(pl.struct('ts_ms', 'percentage').is_duplicated().any()).item():
            raise ValueError(f'{path}: conflicting duplicate depth band')
        frames.append(frame.group_by('ts_ms').agg(*[
            pl.col('notional').filter(pl.col('percentage') == sign * band).first().alias(
                f'{side}_{str(band).replace(".", "_")}')
            for band in BANDS for side, sign in (('bid', -1), ('ask', 1))]))
    return _finish(frames, DEPTH_COLUMNS, start, end, 30_000)


def load_funding(root, coin, start, end):
    frames = []
    for path in _files(root, 'fundingRate', coin, start, end):
        frame = _csv(path)
        if frame.height:
            frames.append(frame.select(pl.col('calc_time').cast(pl.Int64).alias('ts_ms'),
                                       pl.col('last_funding_rate').cast(pl.Float64),
                                       (pl.col('funding_interval_hours') * HOUR_MS).cast(pl.Int64).alias('funding_interval_ms')))
    rest = Path(root) / 'binance/futures/um/rest/fundingRate' / _symbol(coin)
    for path in sorted(rest.glob('*.csv')):
        frame = pl.read_csv(path)
        if frame.height:
            frames.append(frame.select(pl.col('funding_time_ms').cast(pl.Int64).alias('ts_ms'),
                                       pl.col('funding_rate').cast(pl.Float64).alias('last_funding_rate'),
                                       pl.lit(None, dtype=pl.Int64).alias('funding_interval_ms')))
    schema = {'ts_ms': pl.Int64, 'last_funding_rate': pl.Float64, 'funding_interval_ms': pl.Int64}
    frame = pl.concat(frames) if frames else pl.DataFrame(schema=schema)
    frame = frame.with_columns(((pl.col('ts_ms') + 500) // 1000 * 1000).alias('ts_ms'))
    if frame.group_by('ts_ms').agg(pl.col('last_funding_rate').n_unique().alias('n')).filter(pl.col('n') > 1).height:
        raise ValueError('conflicting duplicate funding rate')
    if frame.group_by('ts_ms').agg(pl.col('funding_interval_ms').drop_nulls().n_unique().alias('n')).filter(pl.col('n') > 1).height:
        raise ValueError('conflicting duplicate funding interval')
    frame = frame.group_by('ts_ms').agg(pl.col('last_funding_rate').first(),
                                       pl.col('funding_interval_ms').drop_nulls().first()).sort('ts_ms')
    # A REST row has no interval: carry only a previously known interval, or 8h.
    frame = frame.with_columns(pl.col('funding_interval_ms').forward_fill().fill_null(8 * HOUR_MS))
    return frame.filter(pl.col('ts_ms').is_between(_boundary_ms(start), _boundary_ms(end, end=True), closed='left')).with_columns(
        pl.col('ts_ms').alias('available_ms'), (pl.col('ts_ms') + pl.col('funding_interval_ms')).alias('next_funding_ms'))


def load_dvol(root, currency, start, end):
    frames = []
    first = pl.Series([_boundary_ms(start)]).cast(pl.Datetime('ms', 'UTC')).dt.strftime('%Y-%m')[0]
    last = pl.Series([_boundary_ms(end, end=True) - 1]).cast(pl.Datetime('ms', 'UTC')).dt.strftime('%Y-%m')[0]
    for path in sorted((Path(root) / 'deribit/dvol' / currency / '1h').glob('*.csv')):
        if first <= path.stem <= last:
            frame = pl.read_csv(path)
            if frame.height:
                frames.append(frame.select('ts_ms', pl.col('close').alias('dvol')))
    return _finish(frames, ['dvol'], start, end, HOUR_MS)


def load_sources(root, coin, start, end):
    """Load one coin at a time. BTC DVOL supplies non-ETH market context."""
    return {
        'perp': load_bars(root, 'perp', coin, start, end).with_columns(
            (pl.col('ts_ms') + MINUTE_MS).alias('available_ms')),
        'spot': load_bars(root, 'spot', coin, start, end).with_columns(
            (pl.col('ts_ms') + MINUTE_MS).alias('available_ms')),
        'premium': load_premium(root, coin, start, end),
        'funding': load_funding(root, coin, start, end),
        'metrics': load_metrics(root, coin, start, end),
        'depth': load_depth(root, coin, start, end),
        'dvol': load_dvol(root, 'ETH' if coin == 'ETH' else 'BTC', start, end),
    }


def coverage(sources):
    return {name: {'rows': frame.height, 'first_ts_ms': frame['ts_ms'].min(),
                   'last_ts_ms': frame['ts_ms'].max()} for name, frame in sources.items()}
