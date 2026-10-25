"""Read Binance minute klines without filling gaps or rounding prices."""

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zipfile import ZipFile

import polars as pl

MINUTE_MS = 60_000
CSV_COLUMNS = [
    'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
    'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore',
]
BAR_SCHEMA = {
    'ts_ms': pl.Int64, 'open': pl.Float64, 'high': pl.Float64, 'low': pl.Float64,
    'close': pl.Float64, 'volume': pl.Float64, 'quote_volume': pl.Float64,
    'trades': pl.Int64, 'taker_buy_base': pl.Float64, 'taker_buy_quote': pl.Float64,
}


def _boundary_ms(value, *, end=False):
    """Dates are inclusive UTC days; datetimes/epoch-ms are exact boundaries."""
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = date.fromisoformat(value) if len(value) == 10 else datetime.fromisoformat(value)
    if isinstance(value, datetime):
        value = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    elif isinstance(value, date):
        value = datetime.combine(value + timedelta(days=int(end)), time(), timezone.utc)
    else:
        raise TypeError('expected an ISO date, datetime or epoch milliseconds')
    return int(value.timestamp() * 1000)


def _read_zip(path):
    with ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith('.csv')]
        if len(members) != 1:
            raise ValueError(f'{path}: expected exactly one CSV')
        with archive.open(members[0]) as source:
            header = source.readline().decode('utf-8-sig').split(',')[0].strip() == 'open_time'
        with archive.open(members[0]) as source:
            frame = pl.read_csv(source, has_header=header, new_columns=CSV_COLUMNS,
                                schema_overrides={name: pl.String for name in CSV_COLUMNS})
    # Modern spot archives use microseconds; the cutoff is beyond any plausible ms date.
    stamp = pl.col('open_time').cast(pl.Int64)
    close_stamp = pl.col('close_time').cast(pl.Int64)
    return frame.select(
        pl.when(stamp >= 100_000_000_000_000).then(stamp // 1000)
        .otherwise(stamp).alias('ts_ms'),
        *[pl.col(name).cast(pl.Float64) for name in
          ('open', 'high', 'low', 'close', 'volume', 'quote_volume')],
        pl.col('count').cast(pl.Int64).alias('trades'),
        pl.col('taker_buy_volume').cast(pl.Float64).alias('taker_buy_base'),
        pl.col('taker_buy_quote_volume').cast(pl.Float64).alias('taker_buy_quote'),
        pl.when(close_stamp >= 100_000_000_000_000).then(close_stamp // 1000)
        .otherwise(close_stamp).alias('_close_time'),
        pl.col('ignore').cast(pl.Float64).alias('_ignore'),
    )


def load_bars(root, market, coin, start, end) -> pl.DataFrame:
    """Load [start, end), with an inclusive end day when dates are supplied.

    ``coin`` accepts BTC or BTCUSDT. Overlapping daily/monthly rows must agree
    on every CSV field (after timestamp normalization). Missing files and
    minutes remain gaps.
    """
    if market not in ('perp', 'spot'):
        raise ValueError("market must be 'perp' or 'spot'")
    start_ms, end_ms = _boundary_ms(start), _boundary_ms(end, end=True)
    if end_ms <= start_ms:
        raise ValueError('end must be after start')
    symbol = coin.upper()
    symbol = symbol if symbol.endswith('USDT') else symbol + 'USDT'
    if not symbol.isalnum():
        raise ValueError('invalid coin')
    base = Path(root) / 'binance' / ('futures/um' if market == 'perp' else 'spot')
    first = datetime.fromtimestamp(start_ms / 1000, timezone.utc).date()
    last = datetime.fromtimestamp((end_ms - 1) / 1000, timezone.utc).date()
    frames = []
    for cadence in ('monthly', 'daily'):
        for path in sorted((base / cadence / 'klines' / symbol / '1m').glob(f'{symbol}-1m-*.zip')):
            stamp = path.stem.removeprefix(f'{symbol}-1m-')
            if cadence == 'monthly':
                left, right = first.isoformat()[:7], last.isoformat()[:7]
            else:
                left, right = first.isoformat(), last.isoformat()
            if left <= stamp <= right:
                frames.append(_read_zip(path).filter(pl.col('ts_ms').is_between(start_ms, end_ms, closed='left')))
    if not frames:
        return pl.DataFrame(schema=BAR_SCHEMA)
    bars = pl.concat(frames).unique()
    if bars['ts_ms'].is_duplicated().any():
        raise ValueError(f'{symbol}: conflicting duplicate open_time')
    if bars.null_count().row(0) != (0,) * bars.width or (bars['ts_ms'] % MINUTE_MS != 0).any():
        raise ValueError(f'{symbol}: null values or timestamps off the minute grid')
    return bars.select(list(BAR_SCHEMA)).sort('ts_ms')
