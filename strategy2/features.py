"""Versioned causal minute-grid features, sampled at decision time.

Bars/premium become available after 60s, metrics after 5m, depth after 30s,
and DVOL after 1h. All joins use availability <= decision time. Missing bar
minutes stay null. Rolling statistics require at least 90% valid samples.
Returns and positive-level changes are logarithmic; premium differences are
arithmetic. Volatility is in fractional price units. V2 adds groups I and J.
"""

from math import ceil
from pathlib import Path

import numpy as np
import polars as pl

from .bars import MINUTE_MS, _boundary_ms
from .sources import BANDS, HOUR_MS, METRICS

DAY = 1440
COINS = ('BTC', 'ETH', 'SOL', 'XRP', 'DOGE')
ROUND = dict(zip(COINS, (1000., 100., 10., .1, .01)))
GROUP_FEATURES = {
    'A': [*[f'rv_{w}' for w in (15, 60, 240, 1440, 10080)], 'parkinson_60', 'parkinson_240',
          'down_share_60', 'up_share_60', 'jump_share_60', 'vol_of_vol', 'barrier_sigma_a', 'barrier_sigma_b'],
    'B': [*[f'return_{w}' for w in (15, 60, 240, 1440)], 'abnormal_day',
          'high_distance_1440', 'low_distance_1440', 'high_distance_10080', 'low_distance_10080',
          'vwap_deviation_60', 'vwap_deviation_240', 'vwap_deviation_1440', 'round_distance'],
    'C': ['imbalance_15', 'imbalance_60', 'imbalance_240', 'quarter_imbalance_4',
          'quarter_imbalance_16', 'relative_volume', 'trade_count_z', 'spot_perp_imbalance_60'],
    'D': ['oi_change_60', 'oi_change_240', 'oi_change_1440', 'oi_price_sign_240', 'oi_volume',
          'funding_rate', 'funding_z', 'predicted_funding', 'minutes_to_funding',
          *[f'{name}{suffix}' for name in METRICS[2:] for suffix in ('', '_change_240')]],
    'E': [*[f'{kind}_{str(band).replace(".", "_")}' for band in BANDS
            for kind in ('log_bid', 'log_ask', 'depth_asymmetry')], 'depth_change_60', 'depth_z'],
    'F': ['dvol', 'dvol_change_60', 'dvol_change_1440', 'dvol_rv_ratio'],
    'G': ['hour_sin', 'hour_cos', 'weekday_sin', 'weekday_cos', 'minute', 'us_cash',
          *[f'minutes_{direction}_{event}' for event in ('cpi', 'nfp', 'fomc') for direction in ('to', 'since')],
          'minutes_to_expiry', 'month_end_expiry', 'quarter_end_expiry'],
    'H': ['btc_return_15', 'btc_return_60', 'btc_return_240', 'residual_60', 'residual_240',
          'btc_rv_60', 'eth_btc_trend_240'],
}
FEATURE_GROUPS_V1 = {f'f_{name}': group for group, names in GROUP_FEATURES.items() for name in names}
FEATURE_GROUPS_V1['f_coin_id'] = 'H'
GROUP_FEATURES.update({
    'I': ['mkt_return_15', 'mkt_return_60', 'mkt_return_240',
          'rel_strength_60', 'rel_strength_240', 'return_z_60', 'return_z_240',
          'return_z_1440', 'imbalance_z_60', 'premium_change_60', 'funding_x_oi_240',
          'breakout_up_1440', 'breakout_down_1440',
          *[f'{name}_24h_{target}' for target in ('a', 'b')
            for name in ('touch_up_share', 'touch_rate')]],
    'J': ['cb_premium', 'cb_premium_change_60', 'cb_premium_change_240',
          'cb_premium_z', 'cb_volume_share_60'],
})
FEATURE_GROUPS_V2 = {**FEATURE_GROUPS_V1,
                     **{f'f_{name}': group for group in ('I', 'J') for name in GROUP_FEATURES[group]}}
FEATURE_GROUPS = FEATURE_GROUPS_V1


def feature_groups(version):
    if version not in ('v1', 'v2'):
        raise ValueError('version must be v1 or v2')
    return FEATURE_GROUPS_V1 if version == 'v1' else FEATURE_GROUPS_V2


def _div(a, b):
    return pl.when(b != 0).then(a / b)


def _roll(expr, window, kind='sum'):
    return getattr(expr, f'rolling_{kind}')(window, min_samples=ceil(.9 * window))


def _change(expr, window):
    valid = expr.is_not_null().cast(pl.Int32).rolling_sum(window) >= ceil(.9 * window)
    return pl.when(valid).then(expr.log() - expr.shift(window).log())


def _asof(grid, source, tolerance=None):
    """Backward availability join. Tolerance measures age since availability."""
    source = source.drop('ts_ms') if 'ts_ms' in source.columns else source
    return grid.join_asof(source.sort('available_ms'), left_on='ts_ms', right_on='available_ms',
                          strategy='backward', tolerance=tolerance).drop('available_ms')


def _bar_grid(grid, bars, prefix=''):
    # An exact availability join, not a carry: a missing minute remains null.
    fields = ['open', 'high', 'low', 'close', 'volume', 'quote_volume',
              'trades', 'taker_buy_base', 'taker_buy_quote']
    source = bars.select((pl.col('ts_ms') + MINUTE_MS).alias('available_ms'),
                         *[pl.col(name).alias(prefix + name) for name in fields])
    return _asof(grid, source, 0)


def build_features(coin, sources, *, start=None, end=None, step_min=5,
                   btc_bars=None, eth_bars=None, events=None, version='v1',
                   market_bars=None, labels=None):
    """Build one coin using only information available at each decision ts_ms.

    Source frames come from sources.load_sources. start/end accept the same
    inclusive-date/half-open-ms convention as load_bars. Supply warmup history
    in the source frames and trim output separately to preserve rolling context.
    Every feature has the same schema for every coin. BTC residuals are null.
    Funding z is time-weighted over the last 30 days of settled-rate context;
    trade-count z uses completed hourly observations from the previous 30 days.
    """
    groups = feature_groups(version)
    if coin not in COINS or not isinstance(step_min, int) or step_min < 1:
        raise ValueError('unsupported coin or invalid step_min')
    bars = sources['perp']
    if start is None:
        start = bars['ts_ms'].min()
    if end is None:
        end = bars['ts_ms'].max() + MINUTE_MS
    lo, hi = _boundary_ms(start), _boundary_ms(end, end=True)
    grid = pl.DataFrame({'ts_ms': np.arange(lo, hi, MINUTE_MS, dtype=np.int64)})
    frame = _bar_grid(grid, bars)
    frame = _bar_grid(frame, sources['spot'], 'spot_')
    frame = _asof(frame, sources['premium'].select('available_ms', 'premium'), 0)
    frame = _asof(frame, sources['metrics'].select('available_ms', *METRICS), 15 * MINUTE_MS)
    frame = _asof(frame, sources['depth'].drop('ts_ms'), 5 * MINUTE_MS)
    frame = _asof(frame, sources['dvol'].select('available_ms', 'dvol'), 3 * HOUR_MS)
    funding = sources['funding'].rename({'ts_ms': '_funding_time'})
    frame = _asof(frame, funding)
    frame = frame.with_columns(
        pl.col('close').log().diff().alias('_r'),
        (pl.col('high') / pl.col('low')).log().pow(2).alias('_hl2'),
        (pl.col('ts_ms') - pl.col('_funding_time')).alias('_since_funding'),
    ).with_columns(pl.col('_r').pow(2).alias('_r2'))
    frame = frame.with_columns((_roll(pl.col('_r2'), 60) ** .5).alias('_rv60'),
                                _roll(pl.col('quote_volume'), 60).alias('_q60'),
                                _roll(pl.col('trades'), 60).alias('_trades60'))
    # Keep only the sampled feature arrays, rather than 100+ full minute columns.
    sample = (frame['ts_ms'] % (step_min * MINUTE_MS) == 0)
    result = {'ts_ms': frame['ts_ms'].filter(sample),
              'coin': pl.Series('coin', [coin] * int(sample.sum())),
              'f_coin_id': pl.Series('f_coin_id', np.full(int(sample.sum()), COINS.index(coin), dtype=np.int8))}

    def add(name, expression):
        result['f_' + name] = frame.select('ts_ms', expression.cast(pl.Float64).alias('f_' + name)).to_series(1).filter(sample)

    r, r2, close, rv = (pl.col(name) for name in ('_r', '_r2', 'close', '_rv60'))
    for window in (15, 60, 240, 1440, 10080):
        add(f'rv_{window}', (_roll(r2, window) * 60 / window).sqrt())
    for window in (60, 240):
        add(f'parkinson_{window}', (_roll(pl.col('_hl2'), window) * 60 / window / (4 * np.log(2))).sqrt())
    add('down_share_60', _div(_roll(pl.when(r < 0).then(r2).otherwise(pl.when(r.is_not_null()).then(0.)), 60), _roll(r2, 60)))
    add('up_share_60', _div(_roll(pl.when(r > 0).then(r2).otherwise(pl.when(r.is_not_null()).then(0.)), 60), _roll(r2, 60)))
    add('jump_share_60', _div(_roll(r.abs(), 60, 'max'), rv))
    hourly = frame.filter(pl.col('ts_ms') % HOUR_MS == 0).select('ts_ms',
        _roll(pl.col('_rv60'), 24, 'std').alias('_volvol'),
        _roll(pl.col('_trades60').shift(1), 30 * 24, 'mean').alias('_trades_mean'),
        _roll(pl.col('_trades60').shift(1), 30 * 24, 'std').alias('_trades_std'))
    frame = _asof(frame, hourly.rename({'ts_ms': 'available_ms'}), HOUR_MS - 1)
    add('vol_of_vol', pl.col('_volvol'))
    add('barrier_sigma_a', _div(pl.lit(.005), rv))
    add('barrier_sigma_b', _div(pl.lit(.01), rv * 2))
    for window in (15, 60, 240, 1440):
        add(f'return_{window}', _change(close, window))
    daily = frame.filter(pl.col('ts_ms') % (DAY * MINUTE_MS) == 0).select(
        'ts_ms', _roll(pl.col('close').log().diff().shift(1), 30, 'std').alias('_daily_std'))
    frame = _asof(frame, daily.rename({'ts_ms': 'available_ms'}), DAY * MINUTE_MS - 1)
    add('abnormal_day', pl.when(pl.col('_daily_std').is_not_null() & _change(close, 1440).is_not_null())
        .then(_change(close, 1440).abs() > 2 * pl.col('_daily_std')))
    for window in (1440, 10080):
        add(f'high_distance_{window}', _div((_roll(pl.col('high'), window, 'max') / close).log(), rv))
        add(f'low_distance_{window}', _div((close / _roll(pl.col('low'), window, 'min')).log(), rv))
    for window in (60, 240, 1440):
        vwap = _div(_roll(pl.col('quote_volume'), window), _roll(pl.col('volume'), window))
        add(f'vwap_deviation_{window}', (close / vwap).log())
    add('round_distance', ((close / ROUND[coin]).round() * ROUND[coin] - close).abs() / close)

    def imbalance(prefix, window):
        return _div(2 * _roll(pl.col(prefix + 'taker_buy_quote'), window)
                    - _roll(pl.col(prefix + 'quote_volume'), window), _roll(pl.col(prefix + 'quote_volume'), window))

    for window in (15, 60, 240):
        add(f'imbalance_{window}', imbalance('', window))
    quarter = frame.filter(pl.col('ts_ms') % (15 * MINUTE_MS) == MINUTE_MS).select('ts_ms',
        *[_roll(_div(2 * pl.col('taker_buy_quote') - pl.col('quote_volume'), pl.col('quote_volume')), count, 'mean')
          .alias(f'_quarter_{count}') for count in (4, 16)])
    frame = _asof(frame, quarter.rename({'ts_ms': 'available_ms'}), 15 * MINUTE_MS - 1)
    for count in (4, 16):
        add(f'quarter_imbalance_{count}', pl.col(f'_quarter_{count}'))
    # Same clock minute/hour on the seven preceding UTC days, never today's value.
    same_hour = [pl.col('_q60').shift(day * DAY) for day in range(1, 8)]
    add('relative_volume', pl.when(pl.sum_horizontal([x.is_not_null().cast(pl.Int8) for x in same_hour]) >= 7)
        .then(_div(pl.col('_q60'), pl.mean_horizontal(same_hour))))
    add('trade_count_z', _div(pl.col('_trades60') - pl.col('_trades_mean'), pl.col('_trades_std')))
    add('spot_perp_imbalance_60', imbalance('spot_', 60) - imbalance('', 60))

    oi = pl.col('sum_open_interest')
    for window in (60, 240, 1440):
        add(f'oi_change_{window}', _change(oi, window))
    add('oi_price_sign_240', _change(oi, 240) * _change(close, 240).sign())
    add('oi_volume', _div(pl.col('sum_open_interest_value'), _roll(pl.col('quote_volume'), DAY)))
    rate = pl.col('last_funding_rate')
    add('funding_rate', rate)
    add('funding_z', _div(rate - _roll(rate, 30 * DAY, 'mean'), _roll(rate, 30 * DAY, 'std')))
    premium = pl.when((pl.col('ts_ms') - MINUTE_MS) >= pl.col('_funding_time')).then(pl.col('premium'))
    frame = frame.with_columns(premium.fill_null(0).cum_sum().over('_funding_time').alias('_premium_sum'),
        premium.is_not_null().cast(pl.Int64).cum_sum().over('_funding_time').alias('_premium_count'))
    add('predicted_funding', pl.when((pl.col('_since_funding') > 0)
        & (pl.col('_premium_count') >= .9 * pl.col('_since_funding') / MINUTE_MS))
        .then(_div(pl.col('_premium_sum'), pl.col('_premium_count'))))
    interval = pl.col('funding_interval_ms')
    frame = frame.with_columns((pl.col('_funding_time') + interval *
        ((pl.col('_since_funding') + interval - 1) // interval).clip(lower_bound=1)).alias('next_funding_ms'))
    add('minutes_to_funding', (pl.col('next_funding_ms') - pl.col('ts_ms')) / MINUTE_MS)
    for name in METRICS[2:]:
        add(name, pl.col(name))
        add(name + '_change_240', _change(pl.col(name), 240))

    for band in BANDS:
        name = str(band).replace('.', '_')
        bid, ask = pl.col('bid_' + name), pl.col('ask_' + name)
        add('log_bid_' + name, pl.when(bid > 0).then(bid.log()))
        add('log_ask_' + name, pl.when(ask > 0).then(ask.log()))
        add('depth_asymmetry_' + name, _div(bid - ask, bid + ask))
    total = pl.col('bid_1') + pl.col('ask_1')
    add('depth_change_60', _change(total, 60))
    add('depth_z', _div(total - _roll(total, 7 * DAY, 'mean'), _roll(total, 7 * DAY, 'std')))
    dvol = pl.col('dvol')
    add('dvol', dvol)
    add('dvol_change_60', _change(dvol, 60))
    add('dvol_change_1440', _change(dvol, DAY))
    rv_day = (_roll(r2, DAY) * 60 / DAY).sqrt()
    add('dvol_rv_ratio', _div(dvol / 100, rv_day * np.sqrt(365 * 24)))

    utc = pl.col('ts_ms').cast(pl.Datetime('ms', 'UTC'))
    hour = utc.dt.hour() + utc.dt.minute() / 60
    weekday = utc.dt.weekday() - 1
    add('hour_sin', (hour * 2 * np.pi / 24).sin())
    add('hour_cos', (hour * 2 * np.pi / 24).cos())
    add('weekday_sin', (weekday * 2 * np.pi / 7).sin())
    add('weekday_cos', (weekday * 2 * np.pi / 7).cos())
    add('minute', utc.dt.minute())
    ny = utc.dt.convert_time_zone('America/New_York')
    clock = ny.dt.hour().cast(pl.Int32) * 60 + ny.dt.minute()
    add('us_cash', (ny.dt.weekday() <= 5) & (clock >= 570) & (clock < 960))
    if events is None:
        events = pl.read_csv(Path(__file__).parent / 'config/events.csv', comment_prefix='#')
    for event in ('cpi', 'nfp', 'fomc'):
        times = events.filter(pl.col('event').str.to_lowercase() == event).select(
            pl.col('release_utc').str.to_datetime(time_zone='UTC').dt.epoch('ms').alias('_event')).unique().sort('_event')
        for direction, strategy in (('to', 'forward'), ('since', 'backward')):
            joined = grid.join_asof(times, left_on='ts_ms', right_on='_event', strategy=strategy)
            values = joined.select(((pl.col('_event') - pl.col('ts_ms')).abs() / MINUTE_MS)
                                   .clip(upper_bound=7 * DAY).fill_null(7 * DAY)).to_series()
            result[f'f_minutes_{direction}_{event}'] = values.filter(sample)
    stamps = grid['ts_ms'].to_numpy()
    days = stamps // (DAY * MINUTE_MS)
    expiry = (days + (4 - (days + 3) % 7) % 7) * DAY * MINUTE_MS + 8 * HOUR_MS
    expiry += (expiry < stamps) * 7 * DAY * MINUTE_MS
    expiry_dt = pl.Series(expiry).cast(pl.Datetime('ms', 'UTC'))
    following = pl.Series(expiry + 7 * DAY * MINUTE_MS).cast(pl.Datetime('ms', 'UTC'))
    last_friday = expiry_dt.dt.month() != following.dt.month()
    result['f_minutes_to_expiry'] = pl.Series((expiry - stamps) / MINUTE_MS).filter(sample)
    result['f_month_end_expiry'] = last_friday.cast(pl.Float64).filter(sample)
    result['f_quarter_end_expiry'] = (last_friday & (expiry_dt.dt.month() % 3 == 0)).cast(pl.Float64).filter(sample)

    btc_bars = bars if coin == 'BTC' else (btc_bars if btc_bars is not None else bars.head(0))
    eth_bars = bars if coin == 'ETH' else (eth_bars if eth_bars is not None else bars.head(0))
    if version == 'v1':
        frame = _bar_grid(frame, btc_bars, 'btc_')
        frame = _bar_grid(frame, eth_bars, 'eth_')
    else:
        frame = _close_grid(frame, btc_bars, 'btc_close')
        frame = _close_grid(frame, eth_bars, 'eth_close')
    frame = frame.with_columns(pl.col('btc_close').log().diff().alias('_btc_r'))
    btc_r = pl.col('_btc_r')
    paired_btc = pl.when(r.is_not_null()).then(btc_r)
    beta = _div(pl.rolling_cov(r, btc_r, window_size=7 * DAY, min_samples=ceil(.9 * 7 * DAY)),
                _roll(paired_btc, 7 * DAY, 'var'))
    for window in (15, 60, 240):
        add(f'btc_return_{window}', _change(pl.col('btc_close'), window))
    for window in (60, 240):
        add(f'residual_{window}', pl.lit(None) if coin == 'BTC' else
            _change(close, window) - beta * _change(pl.col('btc_close'), window))
    add('btc_rv_60', _roll(btc_r.pow(2), 60).sqrt())
    add('eth_btc_trend_240', _change(pl.col('eth_close'), 240) - _change(pl.col('btc_close'), 240))
    if version == 'v2':
        frame = _v2_frame(frame, coin, sources, market_bars or {}, btc_bars, eth_bars)
        for group in ('I', 'J'):
            for name in GROUP_FEATURES[group]:
                if not name.startswith(('touch_up_share_', 'touch_rate_')):
                    add(name, pl.col('_' + name))
    for name in ('last_funding_rate', 'next_funding_ms', 'funding_interval_ms'):
        result[name] = frame[name].filter(sample)
    output = pl.DataFrame(result)
    if version == 'v2':
        output = add_touch_history(output, labels)
        output = _v2_columns(output)
    return output.with_columns(*[pl.when(pl.col(name).is_finite()).then(pl.col(name)).otherwise(None).alias(name)
                                 for name in groups if name != 'f_coin_id'])


def _v2_columns(frame):
    """Keep the original column order, then append I and J in contract order."""
    extra = [name for name in FEATURE_GROUPS_V2 if name not in FEATURE_GROUPS_V1]
    return frame.select(*[name for name in frame.columns if name not in extra], *extra)


def _close_grid(frame, bars, name):
    return _asof(frame, bars.select((pl.col('ts_ms') + MINUTE_MS).alias('available_ms'),
                                   pl.col('close').alias(name)), 0)


def _v2_frame(frame, coin, sources, market_bars, btc_bars, eth_bars):
    """Minute-grid market, flow and Coinbase context; no decision-grid rolling."""
    for other in COINS:
        bars = sources['perp'] if other == coin else market_bars.get(other)
        if bars is None:
            bars = {'BTC': btc_bars, 'ETH': eth_bars}.get(other)
        if bars is None:
            bars = sources['perp'].head(0)
        frame = _close_grid(frame, bars, '_mkt_' + other)
    for horizon in (15, 60, 240):
        returns = [_change(pl.col('_mkt_' + other), horizon) for other in COINS]
        frame = frame.with_columns(pl.when(pl.sum_horizontal(
            [r.is_not_null().cast(pl.Int8) for r in returns]) >= 4)
            .then(pl.mean_horizontal(returns)).alias(f'_mkt_return_{horizon}'))
    close, rv = pl.col('close'), pl.col('_rv60')
    q, buy = pl.col('quote_volume'), pl.col('taker_buy_quote')
    frame = frame.with_columns(_div(2 * _roll(buy, 60) - _roll(q, 60), _roll(q, 60)).alias('_imb60'))
    imbalance = pl.col('_imb60')
    frame = frame.with_columns(
        *[(_change(close, h) - pl.col(f'_mkt_return_{h}')).alias(f'_rel_strength_{h}')
          for h in (60, 240)],
        *[_div(_change(close, h), rv * np.sqrt(h / 60)).alias(f'_return_z_{h}')
          for h in (60, 240, 1440)],
        _div(imbalance - _roll(imbalance, 7 * DAY, 'mean'),
             _roll(imbalance, 7 * DAY, 'std')).alias('_imbalance_z_60'),
        (pl.col('premium') - pl.col('premium').shift(60)).alias('_premium_change_60'),
        (pl.col('last_funding_rate') * _change(pl.col('sum_open_interest'), 240)).alias('_funding_x_oi_240'),
        (_roll(pl.col('high'), 15, 'max') >= _roll(pl.col('high').shift(15), DAY, 'max'))
            .cast(pl.Float64).alias('_breakout_up_1440'),
        (_roll(pl.col('low'), 15, 'min') <= _roll(pl.col('low').shift(15), DAY, 'min'))
            .cast(pl.Float64).alias('_breakout_down_1440'))

    cb = sources.get('coinbase')
    if cb is None:
        cb = pl.DataFrame(schema={'ts_ms': pl.Int64, 'available_ms': pl.Int64,
                                  'close': pl.Float64, 'volume': pl.Float64})
    # Match Binance to the Coinbase candle's actual minute before carrying.
    spot = sources['spot'].select('ts_ms', pl.col('close').alias('_cb_spot_close'))
    matched = cb.join(spot, on='ts_ms', how='left').select('available_ms',
        (pl.col('close') / pl.col('_cb_spot_close')).log().alias('_cb_premium'))
    frame = _asof(frame, matched, 5 * MINUTE_MS)
    # Do not forward-fill volume: an absent Coinbase minute has zero trades.
    # Beyond five minutes of price staleness, volume-share coverage is missing.
    volume = cb.select('available_ms',
                       (pl.col('volume') * pl.col('close')).alias('_cb_quote'))
    frame = _asof(frame, volume, 0)
    premium = pl.col('_cb_premium')
    cb_quote = pl.when(premium.is_not_null()).then(pl.col('_cb_quote').fill_null(0))
    cb60, spot60 = _roll(cb_quote, 60), _roll(pl.col('spot_quote_volume'), 60)
    return frame.with_columns(
        (premium - premium.shift(60)).alias('_cb_premium_change_60'),
        (premium - premium.shift(240)).alias('_cb_premium_change_240'),
        _div(premium - _roll(premium, 7 * DAY, 'mean'),
             _roll(premium, 7 * DAY, 'std')).alias('_cb_premium_z'),
        _div(cb60, cb60 + spot60).alias('_cb_volume_share_60'))


def add_touch_history(decisions, labels=None):
    """Trailing (T-24h, T] completed-label history, by full label_end_ms.

    Up share excludes none outcomes; touch rate includes them. Null/tied or
    incomplete labels contribute to neither denominator. A label is visible
    only once label_end_ms <= T, even if it touched a barrier earlier.
    ``labels`` can include warmup decisions outside the requested output range.
    """
    labels = decisions if labels is None else labels
    if 'coin' in decisions.columns and 'coin' in labels.columns:
        coins = decisions['coin'].unique().to_list()
        if len(coins) > 1:
            raise ValueError('touch history decisions must contain one coin')
        labels = labels.filter(pl.col('coin').is_in(coins))
    times = decisions['ts_ms'].to_numpy()
    result = decisions
    for target in ('a', 'b'):
        label, end = f'label_{target}', f'label_end_ms_{target}'
        if label not in labels.columns or end not in labels.columns:
            result = result.with_columns(*[pl.lit(None, dtype=pl.Float64).alias(
                f'f_{name}_24h_{target}') for name in ('touch_up_share', 'touch_rate')])
            continue
        known = labels.filter(pl.col(label).is_in([-1, 0, 1]) & pl.col(end).is_not_null()).sort(end)
        ends, values = known[end].to_numpy(), known[label].to_numpy()
        left = np.searchsorted(ends, times - DAY * MINUTE_MS, side='right')
        right = np.searchsorted(ends, times, side='right')
        def count(mask):
            prefix = np.concatenate(([0], np.cumsum(mask, dtype=np.int64)))
            return prefix[right] - prefix[left]
        touched, up = count(values != 0), count(values == 1)
        for name, numerator, denominator in (
                ('touch_up_share', up, touched), ('touch_rate', touched, right - left)):
            array = np.full(len(times), np.nan)
            np.divide(numerator, denominator, out=array, where=denominator != 0)
            result = result.with_columns(pl.Series(f'f_{name}_24h_{target}', array, nan_to_null=True))
    return result
