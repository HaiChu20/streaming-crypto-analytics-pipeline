"""Bounded-batch aggregation of Binance USDT-M aggTrades archives in RAM."""

from zipfile import ZipFile

import polars as pl
import pyarrow as pa
import pyarrow.csv as pacsv

COLUMNS = ('agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id',
           'transact_time', 'is_buyer_maker')
SCHEMA = {
    'minute_ms': pl.Int64, 'n': pl.Int64,
    **{name: pl.Float64 for name in (
        'buy_qty', 'sell_qty', 'buy_notional', 'sell_notional',
        'max_buy_notional', 'max_sell_notional',
        *(f'{side}_b{i}' for side in ('buy', 'sell') for i in range(5)))},
}
BUCKET_EDGES = (0., 1e3, 1e4, 1e5, 1e6, float('inf'))


def aggregate_zip(source, *, block_size=8 * 1024 * 1024) -> pl.DataFrame:
    """Read one CSV in bounded Arrow batches; retain only minute aggregates.

    ``source`` is a seekable, RAM-backed compressed archive. Neither the ZIP nor
    the uncompressed CSV is extracted to disk. Input order need not be sorted.
    """
    result = pl.DataFrame(schema=SCHEMA)
    with ZipFile(source) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith('.csv')]
        if len(names) != 1:
            raise ValueError('aggTrades archive must contain exactly one CSV')
        with archive.open(names[0]) as member:
            first = member.readline()
            if not first:
                return result
            header = first.decode('utf-8-sig').split(',')[0].strip() == COLUMNS[0]
            if header and not member.read(1):
                return result
        types = {'price': pa.float64(), 'quantity': pa.float64(),
                 'transact_time': pa.int64(), 'is_buyer_maker': pa.bool_()}
        with archive.open(names[0]) as member:
            reader = pacsv.open_csv(
                member,
                read_options=pacsv.ReadOptions(column_names=list(COLUMNS), skip_rows=int(header),
                                               block_size=block_size, use_threads=False),
                convert_options=pacsv.ConvertOptions(column_types=types, include_columns=list(types)))
            for batch in reader:
                frame = pl.from_arrow(batch)
                if frame.select(pl.any_horizontal(pl.all().is_null()).any()).item():
                    raise ValueError('null aggTrade value')
                if frame.filter(~pl.col('price').is_finite() | ~pl.col('quantity').is_finite()
                                | (pl.col('price') < 0) | (pl.col('quantity') < 0)).height:
                    raise ValueError('invalid aggTrade price or quantity')
                frame = frame.with_columns(
                    (pl.col('transact_time') // 60_000 * 60_000).alias('minute_ms'),
                    (pl.col('price') * pl.col('quantity')).alias('notional'))
                if not frame['notional'].is_finite().all():
                    raise ValueError('non-finite aggTrade notional')
                expressions = [pl.len().cast(pl.Int64).alias('n')]
                for side, maker in (('buy', False), ('sell', True)):
                    selected = pl.col('is_buyer_maker') == maker
                    qty = pl.when(selected).then(pl.col('quantity')).otherwise(0.)
                    notional = pl.when(selected).then(pl.col('notional')).otherwise(0.)
                    expressions.extend((qty.sum().alias(f'{side}_qty'),
                                        notional.sum().alias(f'{side}_notional'),
                                        notional.max().alias(f'max_{side}_notional')))
                    for i, (lower, upper) in enumerate(zip(BUCKET_EDGES, BUCKET_EDGES[1:])):
                        expressions.append(pl.when(selected & pl.col('notional').is_between(
                            lower, upper, closed='left')).then(pl.col('notional')).otherwise(0.)
                                           .sum().alias(f'{side}_b{i}'))
                minute = frame.group_by('minute_ms').agg(expressions).select(*SCHEMA)
                # Merge across batch boundaries without retaining batches of trades.
                result = pl.concat([result, minute]).group_by('minute_ms').agg([
                    (pl.col(name).max() if name.startswith('max_') else pl.col(name).sum())
                    for name in SCHEMA if name != 'minute_ms'])
    return result.sort('minute_ms')
