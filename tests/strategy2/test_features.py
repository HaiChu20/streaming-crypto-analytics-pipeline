"""Causal features tested with synthetic history in the public mirror layout."""

import hashlib
import json
from io import BytesIO
from zipfile import ZipFile

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal
import pytest

from strategy2.aggflow import SCHEMA as AGGFLOW_SCHEMA
from strategy2.bars import MINUTE_MS, load_bars
from strategy2.baselines import join_baselines
from strategy2.dataset import TARGETS, _aggflow_coverage, build_coin, build_dataset
from strategy2.features import (ALL_COINS, COINS, EXTRA_COINS, ROUND, FEATURE_GROUPS, FEATURE_GROUPS_V1, FEATURE_GROUPS_V2, FEATURE_GROUPS_V3,
                                _flow_features, add_touch_history, build_features, feature_groups)
from strategy2.labels import first_touch_labels
from strategy2.sources import BANDS, HOUR_MS, METRICS, load_depth, load_funding, load_metrics, load_sources, load_coinbase, load_perp_closes

START = 1767225600000  # 2026-01-01 UTC
END = START + 2 * 1440 * MINUTE_MS


def zip_csv(path, frame, header=True):
    path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(path, 'w') as archive:
        archive.writestr('rows.csv', frame.write_csv(include_header=header))


def synthetic_bars(n, drift=.001, price=100.):
    close = price * np.exp(np.arange(n) * drift)
    return pl.DataFrame({'ts_ms': START + np.arange(n) * MINUTE_MS, 'open': close,
        'high': close * 1.002, 'low': close * .998, 'close': close,
        'volume': np.full(n, 10.), 'quote_volume': close * 10,
        'trades': np.full(n, 20, dtype=np.int64), 'taker_buy_base': np.full(n, 6.),
        'taker_buy_quote': close * 6})


def synthetic_aggflow(n):
    x = np.arange(n)
    values = {'minute_ms': START + x * MINUTE_MS, 'n': 20 + x % 17}
    for side, phase in (('buy', 0.), ('sell', 1.7)):
        for i, base in enumerate((300., 3000., 30000., 200000., 1200000.)):
            values[f'{side}_b{i}'] = base * (1.2 + np.sin(x / (43 + i) + phase))
        values[f'{side}_notional'] = sum(values[f'{side}_b{i}'] for i in range(5))
        values[f'{side}_qty'] = values[f'{side}_notional'] / 100.
        values[f'max_{side}_notional'] = values[f'{side}_b4']
    return pl.DataFrame(values, schema=AGGFLOW_SCHEMA).with_columns(
        (pl.col('minute_ms') + MINUTE_MS).alias('available_ms'))


def klines(frame, microseconds=False):
    scale = 1000 if microseconds else 1
    return frame.select((pl.col('ts_ms') * scale).alias('open_time'),
        'open', 'high', 'low', 'close', 'volume',
        ((pl.col('ts_ms') + MINUTE_MS - 1) * scale).alias('close_time'),
        'quote_volume', pl.col('trades').alias('count'),
        pl.col('taker_buy_base').alias('taker_buy_volume'),
        pl.col('taker_buy_quote').alias('taker_buy_quote_volume'), pl.lit(0).alias('ignore'))


def mirror(root, n=2000):
    for coin, drift, price in (('BTC', .001, 100.), ('ETH', .002, 200.)):
        sym = coin + 'USDT'
        bars = synthetic_bars(n, drift, price)
        for venue in ('futures/um', 'spot'):
            path = root / f'binance/{venue}/monthly/klines/{sym}/1m/{sym}-1m-2026-01.zip'
            zip_csv(path, klines(bars, venue == 'spot'), header=venue != 'spot')
        premium = bars.with_columns(*[pl.lit(.0002).alias(name) for name in ('open', 'high', 'low', 'close')])
        zip_csv(root / f'binance/futures/um/monthly/premiumIndexKlines/{sym}/1m/{sym}-1m-2026-01.zip', klines(premium))
        stamps = START + np.arange(0, n, 480) * MINUTE_MS
        funding = pl.DataFrame({'calc_time': stamps + 3, 'funding_interval_hours': np.full(len(stamps), 8),
                                 'last_funding_rate': np.full(len(stamps), .0001)})
        zip_csv(root / f'binance/futures/um/monthly/fundingRate/{sym}/{sym}-fundingRate-2026-01.zip', funding)
        idx = np.arange(0, n, 5)
        metrics = pl.DataFrame({'create_time': pl.Series(START + idx * MINUTE_MS).cast(pl.Datetime('ms', 'UTC'))
            .dt.strftime('%Y-%m-%d %H:%M:%S'), 'symbol': [sym] * len(idx),
            'sum_open_interest': 1000 * np.exp(.0005 * idx),
            'sum_open_interest_value': 100000 * np.exp(.0005 * idx),
            **{name: 1 + idx / 10000 for name in METRICS[2:]}}).reverse()
        zip_csv(root / f'binance/futures/um/daily/metrics/{sym}/{sym}-metrics-2026-01-01.zip', metrics)
        depth = pl.DataFrame({'timestamp': pl.Series(START + np.repeat(np.arange(n), 2 * len(BANDS)) * MINUTE_MS)
            .cast(pl.Datetime('ms', 'UTC')).dt.strftime('%Y-%m-%d %H:%M:%S'),
            'percentage': np.tile([sign * band for band in BANDS for sign in (-1, 1)], n),
            'depth': np.ones(n * 2 * len(BANDS)),
            'notional': np.tile([value * band for band in BANDS for value in (1000, 500)], n)})
        zip_csv(root / f'binance/futures/um/daily/bookDepth/{sym}/{sym}-bookDepth-2026-01-01.zip', depth)
        path = root / f'deribit/dvol/{coin}/1h/2026-01.csv'
        path.parent.mkdir(parents=True, exist_ok=True)
        hours = np.arange((n + 59) // 60)
        pl.DataFrame({'ts_ms': START + hours * HOUR_MS,
            **{name: 40. + hours for name in ('open', 'high', 'low', 'close')}}).write_csv(path)


@pytest.fixture
def history(tmp_path):
    mirror(tmp_path)
    return tmp_path


def features(root, coin='ETH', *, sources=None, events=None):
    return build_features(coin, sources or load_sources(root, coin, START, END), start=START, end=END,
        step_min=1, btc_bars=load_bars(root, 'perp', 'BTC', START, END),
        eth_bars=load_bars(root, 'perp', 'ETH', START, END), events=events)


def test_loaders_availability_and_quirks(history):
    loaded = load_sources(history, 'BTC', START, END)
    for name, delay in (('perp', MINUTE_MS), ('spot', MINUTE_MS), ('premium', MINUTE_MS),
        ('metrics', 5 * MINUTE_MS), ('depth', 30_000), ('dvol', HOUR_MS), ('funding', 0)):
        source = loaded[name]
        assert source.height
        assert source['ts_ms'].is_sorted()
        assert (source['available_ms'] - source['ts_ms'] == delay).all()
    assert loaded['funding']['ts_ms'][0] == START
    assert loaded['metrics']['sum_open_interest'][0] == 1000
    assert loaded['depth']['bid_1'][0] == 1000
    assert loaded['depth']['ask_1'][0] == 500
    assert loaded['premium']['premium'][0] == .0002
    assert loaded['dvol']['dvol'][0] == 40
    assert load_sources(history, 'SOL', START, END)['dvol']['dvol'][0] == 40


def test_funding_rest_union_and_conflict(history):
    path = history / 'binance/futures/um/rest/fundingRate/BTCUSDT/partial.csv'
    path.parent.mkdir(parents=True)
    def write(rate):
        pl.DataFrame({'funding_time_ms': [START + 7, START + 2400 * MINUTE_MS],
            'funding_rate': [rate, .0003], 'mark_price': [100., 110.]}).write_csv(path)
    write(.0001)
    funding = load_funding(history, 'BTC', START, END)
    assert funding.height == 6
    assert funding['funding_interval_ms'][-1] == 8 * HOUR_MS
    assert funding['last_funding_rate'][-1] == .0003
    write(.01)
    with pytest.raises(ValueError, match='conflicting duplicate funding'):
        load_funding(history, 'BTC', START, END)


def test_metrics_quoted_empty_values_are_null(tmp_path):
    path = tmp_path / 'binance/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-01-01.zip'
    path.parent.mkdir(parents=True)
    with ZipFile(path, 'w') as archive:
        archive.writestr('rows.csv', 'create_time,symbol,' + ','.join(METRICS) + '\n'
                         '2026-01-01 00:05:00,BTCUSDT,1,2,"","",3,4\n')
    result = load_metrics(tmp_path, 'BTC', START, END)
    assert result['sum_open_interest'][0] == 1
    assert result['count_toptrader_long_short_ratio'][0] is None


def test_partial_and_empty_depth(tmp_path):
    path = tmp_path / 'binance/futures/um/daily/bookDepth/BTCUSDT/BTCUSDT-bookDepth-2026-01-01.zip'
    frame = pl.DataFrame({'timestamp': ['2026-01-01 00:00:00'], 'percentage': [-1.], 'depth': [2.], 'notional': [100.]})
    zip_csv(path, frame)
    result = load_depth(tmp_path, 'BTC', START, END)
    assert result['bid_1'][0] == 100
    assert result['ask_1'][0] is None
    zip_csv(path, frame.head(0))
    assert load_depth(tmp_path, 'BTC', START, END).height == 0


@pytest.mark.parametrize('version', ['v1', 'v2', 'v3'])
def test_generic_no_lookahead_all_sources(tmp_path, version):
    # More than 30 days exercises long-window features as well as short ones.
    mirror(tmp_path, n=47_000)
    cutoff = START + 45_000 * MINUTE_MS
    end = START + 47_000 * MINUTE_MS
    sources = load_sources(tmp_path, 'ETH', START, end)
    btc = load_bars(tmp_path, 'perp', 'BTC', START, end)
    eth = load_bars(tmp_path, 'perp', 'ETH', START, end)
    flow_coverage = None
    if version == 'v3':
        sources['aggflow'] = synthetic_aggflow(47_000).rename({'minute_ms': 'ts_ms'})
        flow_coverage = pl.DataFrame({'start_ms': [START], 'end_ms': [end]})
    sources['coinbase'] = eth.select('ts_ms', 'close', 'volume').with_columns(
        (pl.col('ts_ms') + MINUTE_MS).alias('available_ms'))
    market = {coin: synthetic_bars(47_000, .0001 * (i + 1)).select('ts_ms', 'close')
              for i, coin in enumerate(COINS)}
    labels = None
    for target, (barrier, horizon) in TARGETS.items():
        part = first_touch_labels(eth, barrier, horizon).select('ts_ms',
            pl.col('label').alias(f'label_{target}'), pl.col('label_end_ms').alias(f'label_end_ms_{target}'))
        labels = part if labels is None else labels.join(part, on='ts_ms')
    def build(s, b, e, m, l):
        return build_features('ETH', s, start=START, end=end, btc_bars=b, eth_bars=e,
            version=version, market_bars=m, labels=l, aggflow_coverage=flow_coverage).filter(
            pl.col('ts_ms') <= cutoff).select('ts_ms', *feature_groups(version))
    original = build(sources, btc, eth, market, labels)
    def perturb(frame, availability):
        return frame.with_columns(*[pl.when(availability > cutoff).then(pl.col(name) * 1.7 + .003)
            .otherwise(pl.col(name)).cast(dtype).alias(name)
            for name, dtype in frame.schema.items() if name not in
            ('ts_ms', 'available_ms', 'next_funding_ms') and dtype.is_numeric()])
    changed = {name: perturb(frame, pl.col('available_ms')) for name, frame in sources.items()}
    future_labels = labels.with_columns(*[pl.when(pl.col(f'label_end_ms_{target}') > cutoff)
        .then(-pl.col(f'label_{target}') + 1).otherwise(pl.col(f'label_{target}'))
        .cast(pl.Int8).alias(f'label_{target}') for target in TARGETS])
    actual = build(changed, perturb(btc, pl.col('ts_ms') + MINUTE_MS),
                   perturb(eth, pl.col('ts_ms') + MINUTE_MS),
                   {coin: perturb(bars, pl.col('ts_ms') + MINUTE_MS) for coin, bars in market.items()},
                   future_labels)
    assert_frame_equal(original, actual)


def test_staleness_limits(history):
    sources = load_sources(history, 'ETH', START, END)
    for name in ('metrics', 'depth', 'dvol'):
        sources[name] = sources[name].head(1)
    result = features(history, sources=sources)
    assert result['f_count_long_short_ratio'][20] is not None
    assert result['f_count_long_short_ratio'][21] is None
    assert result['f_log_bid_1'][5] is not None
    assert result['f_log_bid_1'][6] is None
    assert result['f_dvol'][240] == 40
    assert result['f_dvol'][241] is None


@pytest.mark.parametrize('missing,valid', [(6, True), (7, False)])
def test_ninety_percent_window_and_no_fill(history, missing, valid):
    sources = load_sources(history, 'ETH', START, END)
    sources['perp'] = sources['perp'].filter(~pl.col('ts_ms').is_between(
        START + 840 * MINUTE_MS, START + (839 + missing) * MINUTE_MS))
    result = features(history, sources=sources)
    assert (result['f_imbalance_60'][900] is not None) == valid
    sources['perp'] = sources['perp'].filter(pl.col('ts_ms') != START + 899 * MINUTE_MS)
    assert features(history, sources=sources)['f_return_60'][900] is None


def test_hand_computed_formulas_all_groups(history):
    events = pl.DataFrame({'event': ['cpi', 'nfp', 'fomc'],
        'release_utc': ['2026-01-01T16:00:00Z', '2026-01-01T14:00:00Z', '2026-01-01T19:00:00Z']})
    row = features(history, events=events).row(900, named=True)
    # A: constant .002 1m log return, scaled per hour.
    assert row['f_rv_60'] == pytest.approx(.002 * np.sqrt(60))
    assert row['f_up_share_60'] == pytest.approx(1.)
    assert row['f_barrier_sigma_b'] == pytest.approx(.01 / (.002 * np.sqrt(60) * 2))
    # B: log return and quote/base VWAP.
    assert row['f_return_60'] == pytest.approx(.12)
    prices = 200 * np.exp(np.arange(2000) * .002)
    assert row['f_vwap_deviation_60'] == pytest.approx(np.log(prices[899] / prices[840:900].mean()))
    # C: 60% taker-buy quote; quarter openings are exactly .2 imbalance too.
    assert row['f_imbalance_60'] == pytest.approx(.2)
    assert row['f_quarter_imbalance_4'] == pytest.approx(.2)
    assert row['f_spot_perp_imbalance_60'] == pytest.approx(0.)
    # D: metrics published five minutes late; funding settled at 08:00.
    assert row['f_oi_change_60'] == pytest.approx(.03)
    assert row['f_minutes_to_funding'] == 60
    assert row['f_predicted_funding'] == pytest.approx(.0002)
    # E: bands are cumulative, negative is bid, positive is ask.
    assert row['f_log_bid_1'] == pytest.approx(np.log(1000))
    assert row['f_depth_asymmetry_1'] == pytest.approx(1 / 3)
    # F: 14:00 hourly close is first known at 15:00.
    assert row['f_dvol'] == 54
    assert row['f_dvol_change_60'] == pytest.approx(np.log(54 / 53))
    # G: 15:00 UTC is 10:00 NY; next Friday is Jan 2 08:00 UTC.
    assert row['f_us_cash'] == 1
    assert row['f_hour_sin'] == pytest.approx(-np.sqrt(.5))
    assert row['f_minutes_to_cpi'] == 60
    assert row['f_minutes_since_nfp'] == 60
    assert row['f_minutes_to_expiry'] == 17 * 60
    # H: ETH .002 minus BTC .001 per minute over 240m.
    assert row['f_btc_return_60'] == pytest.approx(.06)
    assert row['f_eth_btc_trend_240'] == pytest.approx(.24)


def test_baselines_join_decision_not_latency_entry(history):
    bars = load_bars(history, 'perp', 'ETH', START, END)
    decision = START + 900 * MINUTE_MS
    decisions = pl.DataFrame({'ts_ms': [decision], 'entry_ms': [decision + MINUTE_MS]})
    original = join_baselines(decisions, bars, 'a', .005, 60)
    changed = bars.with_columns(pl.when(pl.col('ts_ms') >= decision).then(pl.col('close') * .1)
        .otherwise(pl.col('close')).alias('close'))
    assert_frame_equal(original, join_baselines(decisions, changed, 'a', .005, 60))
    shifted = decisions.with_columns(pl.col('entry_ms').alias('ts_ms'))
    assert join_baselines(shifted, changed, 'a', .005, 60)['mom_dir_a'][0] == -1
    assert original['mom_dir_a'][0] == 1


def test_seven_day_beta_and_relative_volume(history):
    n = 12_000
    stamps = START + np.arange(n) * MINUTE_MS
    returns = .0001 + .0003 * np.sin(np.arange(n) / 13)
    btc = synthetic_bars(n).with_columns(pl.Series('close', 100 * np.exp(np.cumsum(returns))))
    own = synthetic_bars(n).with_columns(pl.Series('close', 200 * np.exp(2 * np.cumsum(returns))))
    sources = load_sources(history, 'ETH', START, END)
    sources['perp'] = own
    sources['spot'] = own
    result = build_features('ETH', sources, start=START, end=int(stamps[-1] + MINUTE_MS),
                            step_min=1, btc_bars=btc, eth_bars=own)
    assert result['f_residual_60'][11_000] == pytest.approx(0, abs=1e-9)
    assert result['f_residual_240'][11_000] == pytest.approx(0, abs=1e-9)
    # Quote volume in synthetic_bars has a constant .001 logarithmic trend.
    expected = 7 / sum(np.exp(-.001 * 1440 * day) for day in range(1, 8))
    assert result['f_relative_volume'][11_000] == pytest.approx(expected)


def test_last_friday_expiry_calendar(history):
    sources = load_sources(history, 'ETH', START, END)
    start = 1774598100000  # 2026-03-27 07:55 UTC, last Friday of the quarter
    result = build_features('ETH', sources, start=start, end=start + 10 * MINUTE_MS, step_min=1)
    assert result['f_minutes_to_expiry'][0] == 5
    assert result['f_month_end_expiry'][0] == 1
    assert result['f_quarter_end_expiry'][0] == 1
    assert result['f_month_end_expiry'][6] == 0


def test_dataset_contract_and_metadata(history, tmp_path):
    out = tmp_path / 'out'
    metadata = build_dataset(history, out, '2026-01-01', '2026-01-02', ['BTC', 'ETH'])
    for coin in ('BTC', 'ETH'):
        frame = pl.read_parquet(out / f's2-features-v1/{coin}.parquet')
        assert frame.height == 576
        expected = {'ts_ms', 'coin', 'entry_ms', 'last_funding_rate', 'next_funding_ms', 'funding_interval_ms', *FEATURE_GROUPS}
        for target in TARGETS:
            expected.update(f'{name}_{target}' for name in ('label', 'exit_return', 'minutes_to_touch',
                'max_up', 'max_down', 'label_end_ms', 'entry_price', 'vol_p_up', 'vol_p_down', 'vol_p_none', 'mom_dir', 'rev_dir'))
            assert frame.schema[f'label_{target}'] == pl.Int8
            assert frame.schema[f'entry_price_{target}'] == pl.Float64
            assert frame.schema[f'label_end_ms_{target}'] == pl.Int64
        assert set(frame.columns) == expected
        assert frame.schema['coin'] == pl.String
        assert frame.schema['ts_ms'] == pl.Int64
        assert frame.schema['funding_interval_ms'] == pl.Int64
        assert frame.schema['next_funding_ms'] == pl.Int64
        assert (frame['entry_ms'] == frame['ts_ms'] + MINUTE_MS).all()
        assert (frame['label_end_ms_b'] == frame['entry_ms'] + 240 * MINUTE_MS).all()
        assert all(frame.schema[name] == (pl.Int8 if name == 'f_coin_id' else pl.Float64) for name in FEATURE_GROUPS)
        assert metadata['coins'][coin]['sources']['perp']['rows'] == 2000
    saved = json.loads((out / 's2-features-v1/metadata.json').read_text())
    assert saved['targets'] == {
        'a': {'barrier': .005, 'horizon_minutes': 60},
        'b': {'barrier': .01, 'horizon_minutes': 240},
        'c': {'barrier': .02, 'horizon_minutes': 480}}
    assert (frame['label_end_ms_c'] == frame['entry_ms'] + 480 * MINUTE_MS).all()
    assert len(saved['features']) == len(FEATURE_GROUPS)
    assert set(saved['coins']['BTC']['null_share_by_group']) == set('ABCDEFGH')



@pytest.mark.parametrize('coin,expected', [
    ('BTC', 'e72d57bd113473f99ccf71b71d21271318f5a082c8efe75e82f973eac48a690e'),
    ('ETH', 'e0c57723dc1022418696ae5b631d21e6b6c999b6ce7ab4ab13436108055ea33d'),
])
def test_v1_parquet_bytes_match_a0a415d_synthetic_dataset(history, coin, expected):
    # Captured from the unmodified a0a415d build_coin on this mirror fixture,
    # before adding v2. Includes all columns, dtypes, order and zstd bytes.
    baseline, _ = build_coin(history, coin, '2026-01-01', '2026-01-02')
    explicit, _ = build_coin(history, coin, '2026-01-01', '2026-01-02', version='v1')
    assert_frame_equal(baseline, explicit)
    output = BytesIO()
    # The historical hash covers a/b only; C adds label/baseline columns.
    explicit.select(name for name in explicit.columns if not name.endswith('_c')).write_parquet(output, compression='zstd')
    assert hashlib.sha256(output.getvalue()).hexdigest() == expected
    assert sum(name.startswith('f_') for name in explicit.columns) == 89
    assert FEATURE_GROUPS is FEATURE_GROUPS_V1 is feature_groups('v1')


@pytest.mark.parametrize('version,count,columns_hash', [
    ('v1', 89, '6b7c2ca8e404e649241862cc141a47fcbab549f3fc79745185e60cd44237e61d'),
    ('v2', 111, '1a9f310b20a8c146db2ae86cb1773c311740a08be9d721989abd3ada5dab2da1'),
])
def test_target_c_preserves_feature_columns_and_bytes(history, monkeypatch, version, count, columns_hash):
    columns = list(feature_groups(version))
    assert len(columns) == count
    # Frozen before adding C: column order and names are part of the contract.
    assert hashlib.sha256(json.dumps(columns).encode()).hexdigest() == columns_hash
    assert not any(name.endswith('_c') for name in columns)
    with_c, _ = build_coin(history, 'ETH', '2026-01-01', '2026-01-02', version=version)
    monkeypatch.setattr('strategy2.dataset.TARGETS', {'a': (.005, 60), 'b': (.01, 240)})
    without_c, _ = build_coin(history, 'ETH', '2026-01-01', '2026-01-02', version=version)
    physical_columns = [name for name in with_c.columns if name.startswith('f_')]
    assert set(physical_columns) == set(columns)
    assert physical_columns == [name for name in without_c.columns if name.startswith('f_')]
    assert_frame_equal(with_c.select(columns), without_c.select(columns), check_exact=True)
    encoded = []
    for frame in (with_c, without_c):
        buffer = BytesIO()
        frame.select(columns).write_parquet(buffer, compression='zstd')
        encoded.append(buffer.getvalue())
    assert encoded[0] == encoded[1]


def v2_inputs(root, n=12_000):
    sources = load_sources(root, 'ETH', START, START + n * MINUTE_MS)
    market = {coin: synthetic_bars(n, .0001 * (i + 1)).select('ts_ms', 'close')
              for i, coin in enumerate(COINS)}
    own = synthetic_bars(n, .0002)
    sources['perp'] = own
    sources['spot'] = own
    premium = .001 + np.arange(n) * .0000001 + .0002 * np.sin(np.arange(n) / 31)
    sources['coinbase'] = own.select('ts_ms', (pl.col('close') * pl.Series(np.exp(premium))).alias('close'),
        (pl.col('volume') * .2).alias('volume')).with_columns(
        (pl.col('ts_ms') + MINUTE_MS).alias('available_ms'))
    sources['premium'] = own.select('ts_ms', (pl.col('ts_ms') + MINUTE_MS).alias('available_ms'),
                                    pl.Series('premium', np.arange(n) * 1e-7))
    return sources, market, premium


def build_v2(sources, market, *, step=1, labels=None):
    return build_features('ETH', sources, start=START,
        end=int(sources['perp']['ts_ms'].max() + MINUTE_MS), step_min=step,
        btc_bars=market['BTC'], eth_bars=market['ETH'], market_bars=market,
        version='v2', labels=labels)


def test_v2_hand_computed_market_persistence_and_coinbase(history):
    sources, market, premium = v2_inputs(history)
    result = build_v2(sources, market)
    row = result.row(1600, named=True)
    for h in (15, 60, 240):
        assert row[f'f_mkt_return_{h}'] == pytest.approx(.0003 * h)
    for h in (60, 240):
        assert row[f'f_rel_strength_{h}'] == pytest.approx(-.0001 * h)
    for h in (60, 240, 1440):
        assert row[f'f_return_z_{h}'] == pytest.approx(np.sqrt(h))
    assert row['f_premium_change_60'] == pytest.approx(60e-7)
    assert row['f_funding_x_oi_240'] == pytest.approx(.0001 * .0005 * 240)
    assert row['f_breakout_up_1440'] == 1
    assert row['f_breakout_down_1440'] == 0
    assert row['f_cb_premium'] == pytest.approx(premium[1599])
    for h in (60, 240):
        assert row[f'f_cb_premium_change_{h}'] == pytest.approx(premium[1599] - premium[1599-h])
    cb = sources['coinbase'].slice(1540, 60)
    spot = sources['spot'].slice(1540, 60)
    cbq = (cb['close'] * cb['volume']).sum()
    assert row['f_cb_volume_share_60'] == pytest.approx(cbq / (cbq + spot['quote_volume'].sum()))
    i = 11_000
    window = premium[i - 10080:i]
    assert result['f_cb_premium_z'][i] == pytest.approx((premium[i-1]-window.mean())/window.std(ddof=1))
    sampled = build_v2(sources, market, step=5)
    assert_frame_equal(sampled, result.filter(pl.col('ts_ms') % (5 * MINUTE_MS) == 0))
    assert len(FEATURE_GROUPS_V2) == 111
    assert_frame_equal(result.select('ts_ms', *FEATURE_GROUPS_V1),
        build_features('ETH', sources, start=START, end=START + 12_000 * MINUTE_MS,
            step_min=1, btc_bars=synthetic_bars(12_000, .0001),
            eth_bars=sources['perp']).select('ts_ms', *FEATURE_GROUPS_V1))


@pytest.mark.parametrize('missing,valid', [(1, True), (2, False)])
def test_market_requires_four_coins(history, missing, valid):
    sources, market, _ = v2_inputs(history, 1000)
    for coin in ('SOL', 'XRP')[:missing]:
        market[coin] = market[coin].head(0)
    row = build_v2(sources, market).row(900, named=True)
    assert (row['f_mkt_return_60'] is not None) == valid
    if valid:
        assert row['f_mkt_return_60'] == pytest.approx(.0003 * 60)  # SOL is the .0003 coin.


def test_coinbase_carry_matches_original_spot_minute_and_never_carries_volume(history):
    sources, market, premium = v2_inputs(history, 1000)
    sources['coinbase'] = sources['coinbase'].filter(~pl.col('ts_ms').is_between(
        START + 900 * MINUTE_MS, START + 909 * MINUTE_MS))
    result = build_v2(sources, market)
    assert result['f_cb_premium'][900] == pytest.approx(premium[899])
    assert result['f_cb_premium'][905] == pytest.approx(premium[899])
    assert result['f_cb_premium'][906] is None
    assert result['f_cb_premium'][911] == pytest.approx(premium[910])
    cb = sources['coinbase'].filter(pl.col('ts_ms').is_between(
        START + 845 * MINUTE_MS, START + 904 * MINUTE_MS))
    cbq = (cb['close'] * cb['volume']).sum()
    spotq = sources['spot'].slice(845,60)['quote_volume'].sum()
    assert result['f_cb_volume_share_60'][905] == pytest.approx(cbq / (cbq + spotq))


@pytest.mark.parametrize('missing,valid', [(1008, True), (1009, False)])
def test_coinbase_z_coverage_on_minute_grid(history, missing, valid):
    sources, market, _ = v2_inputs(history)
    # A gap longer than the permitted carry: precisely `missing` invalid values.
    sources['coinbase'] = sources['coinbase'].filter(~pl.col('ts_ms').is_between(
        START + 1500 * MINUTE_MS, START + (1504 + missing) * MINUTE_MS))
    result = build_v2(sources, market)
    assert (result['f_cb_premium_z'][11_000] is not None) == valid


def test_imbalance_z_is_distribution_of_hourly_imbalance_on_minute_grid(history):
    sources, market, _ = v2_inputs(history)
    sources['perp'] = sources['perp'].with_columns(
        (pl.col('quote_volume') * pl.Series(.5 + .1 * np.sin(np.arange(12_000)/100))).alias('taker_buy_quote'))
    result = build_v2(sources, market)
    values = result['f_imbalance_60'].slice(11_000-10080+1, 10080).to_numpy()
    expected = (values[-1]-values.mean())/values.std(ddof=1)
    assert result['f_imbalance_z_60'][11_000] == pytest.approx(expected)


def test_touch_history_uses_only_completed_labels_at_T():
    t = START + 2 * 1440 * MINUTE_MS
    decisions = pl.DataFrame({'ts_ms': [t-1, t, t+1]})
    labels = pl.DataFrame({'ts_ms': [t - i * 5 * MINUTE_MS for i in range(6)],
        **{f'label_{target}': [1, -1, 0, 1, None, -1] for target in TARGETS},
        **{f'label_end_ms_{target}': [t-1, t, t-60_000, t+1, t-1, t-1440*MINUTE_MS]
           for target in TARGETS}})
    result = add_touch_history(decisions, labels)
    assert not any(name.startswith('f_') and name.endswith('_c') for name in result.columns)
    for target in ('a', 'b'):
        assert result[f'f_touch_up_share_24h_{target}'][1] == .5
        assert result[f'f_touch_rate_24h_{target}'][1] == pytest.approx(2/3)
        changed = labels.with_columns(pl.when(pl.col(f'label_end_ms_{target}') > t)
            .then(-1).otherwise(pl.col(f'label_{target}')).alias(f'label_{target}'))
        assert_frame_equal(result.head(2), add_touch_history(decisions, changed).head(2))
    # The label ending exactly at T becomes visible at T, never at T-1.
    assert result['f_touch_up_share_24h_a'][0] == .5
    assert result['f_touch_rate_24h_a'][0] == pytest.approx(2/3)


def test_coinbase_loader_empty_days_and_compact_market_context(history):
    path = history / 'coinbase/candles_1m/ETH-USD/2026-01-01.csv'
    path.parent.mkdir(parents=True)
    synthetic_bars(10).select('ts_ms', 'open', 'high', 'low', 'close', 'volume').reverse().write_csv(path)
    empty = path.with_name('2026-01-02.csv')
    empty.write_text('ts_ms,open,high,low,close,volume\n')
    result = load_coinbase(history, 'ETH', START, END)
    assert result.height == 10 and result['ts_ms'].is_sorted()
    assert (result['available_ms'] == result['ts_ms'] + MINUTE_MS).all()
    assert load_coinbase(history, 'XRP', START, END).height == 0
    assert_frame_equal(load_perp_closes(history, 'ETH', START, END),
                       load_bars(history, 'perp', 'ETH', START, END).select('ts_ms', 'close'))


def test_v2_dataset_metadata_labels_warmup_and_v1_columns(history, tmp_path):
    out = tmp_path / 'v2-out'
    metadata = build_dataset(history, out, '2026-01-01', '2026-01-02', ['ETH'], version='v2')
    frame = pl.read_parquet(out / 's2-features-v2/ETH.parquet')
    v1, _ = build_coin(history, 'ETH', '2026-01-01', '2026-01-02')
    assert frame.columns[:v1.width] == v1.columns
    assert_frame_equal(frame.select(v1.columns), v1)
    assert metadata['version'] == 's2-features-v2'
    assert metadata['targets']['c'] == {'barrier': .02, 'horizon_minutes': 480}
    assert metadata['feature_columns'] == list(FEATURE_GROUPS_V2)
    assert metadata['features'] == [{'name': name, 'group': group} for name, group in FEATURE_GROUPS_V2.items()]
    assert set(metadata['coins']['ETH']['null_share_by_group']) == set('ABCDEFGHIJ')
    # The first available history is after the full label horizon has matured.
    assert frame['f_touch_rate_24h_a'][12] is None
    assert frame['f_touch_rate_24h_a'][13] == 1
    assert frame['f_cb_premium'].null_count() == frame.height
    second, _ = build_coin(history, 'ETH', START + 1440*MINUTE_MS,
        START + 2000*MINUTE_MS, version='v2')
    assert second['f_touch_rate_24h_a'][0] == 1  # Includes labels from the preceding day.
    with pytest.raises(ValueError, match='version'):
        build_features('ETH', {}, version='v4')


def test_touch_history_same_coin_and_exact_availability_boundary():
    decisions = pl.DataFrame({'ts_ms': [START-1, START], 'coin': ['ETH', 'ETH']})
    labels = pl.DataFrame({'coin': ['ETH', 'BTC'], 'label_a': [1, -1],
                           'label_end_ms_a': [START, START]})
    result = add_touch_history(decisions, labels)
    assert result['f_touch_up_share_24h_a'].to_list() == [None, 1.]
    assert result['f_touch_rate_24h_a'].to_list() == [None, 1.]
    assert result['f_touch_rate_24h_b'].to_list() == [None, None]


@pytest.mark.parametrize('direction', ['up', 'down'])
def test_breakout_compares_last_fifteen_minutes_with_prior_day(history, direction):
    sources, market, _ = v2_inputs(history, 2000)
    field, extreme, ordinary = ('high', 1000., 101.) if direction == 'up' else ('low', 1., 99.)
    # Flat market, an old extreme, then an equal extreme in the last 15 minutes.
    sources['perp'] = sources['perp'].with_columns(
        pl.lit(100.).alias('close'), pl.lit(101.).alias('high'), pl.lit(99.).alias('low'))
    sources['perp'] = sources['perp'].with_columns(
        pl.when(pl.col('ts_ms').is_in([START + 100 * MINUTE_MS, START + 1500 * MINUTE_MS]))
        .then(extreme).otherwise(ordinary).alias(field))
    result = build_v2(sources, market)
    name = f'f_breakout_{direction}_1440'
    assert result[name][1490] == 0.
    assert result[name][1501] == 1.
    assert result[name][1516] == 0.


@pytest.mark.parametrize('version,hashes', [
    ('v1', (
        'aed3778873a89f3b0f216078726fcb6997c41c80357a402cb19c1d570180226c',
        '268f7491597d3dee8c9e07c02f08a6532bc472d5a8e9e92ea02862652dd73715',
        'd9e022653bcdf943dddab7b6439f8a2da5dc482846594ece7418a78e4f1fef86',
        '7a3feb32869069a24852425ef52071362ca68bb31815baa44d5334e5f2ea80b0',
        'e05a3a01de6d4407eb9673351e02508d47fc2bc628e12e538f88eae35ca8b812')),
    ('v2', (
        '1d6916785cea76083e5fd9ecc252385a0c3e27b93715e7d12196403983377929',
        '49dc4718f1b2f6df58a1cbea1f1345127fa76cbe95da173824ff7d40a63e46cc',
        '262322c425901b0542869ed8ac445d5dbd6c8c7be51bd0255e948e8a9dc96f98',
        'be594d1dea616b7bc6c647168753f885853cc68a004c6c3c904a460f41ccdd2c',
        'efc20e81723ebd823e681781a5458946e4d4b5cdd3952443e4280f1b8388c7a1')),
])
def test_core_frozen_bytes_before_v3(history, version, hashes):
    # Captured from unmodified dfe4dc7. >32 days exercises long-window values.
    sources, market, _ = v2_inputs(history, 47_000)
    for coin, expected in zip(COINS, hashes):
        frame = build_features(coin, sources, start=START, end=START + 47_000 * MINUTE_MS,
            btc_bars=synthetic_bars(47_000, .0001), eth_bars=synthetic_bars(47_000, .0002),
            market_bars=market, version=version)
        buffer = BytesIO()
        frame.write_parquet(buffer, compression='zstd')
        assert hashlib.sha256(buffer.getvalue()).hexdigest() == expected, coin
        if version == 'v2':
            extended = build_features(coin, sources, start=START, end=START + 47_000 * MINUTE_MS,
                btc_bars=synthetic_bars(47_000, .0001), eth_bars=synthetic_bars(47_000, .0002),
                market_bars=market, version='v3').select(frame.columns)
            buffer = BytesIO()
            extended.write_parquet(buffer, compression='zstd')
            assert hashlib.sha256(buffer.getvalue()).hexdigest() == expected, coin


@pytest.mark.parametrize('coin,level', tuple(zip(EXTRA_COINS, (10., .01, .1, .1, 1., .001, .1, 10., .1, .01))))
def test_extra_coins_same_schema_btc_context_and_missing_sources(history, coin, level):
    sources = load_sources(history, coin, START, END)
    assert sources['dvol']['dvol'][0] == 40  # All extras use BTC DVOL.
    sources['perp'] = synthetic_bars(1000, .0001, 1.234 * level)
    if coin in ('BNB', 'TRX'):
        # Unsupported products stay null even if a caller supplies stray bars.
        sources['coinbase'] = sources['perp'].select('ts_ms', 'close', 'volume').with_columns(
            (pl.col('ts_ms') + MINUTE_MS).alias('available_ms'))
    result = build_features(coin, sources, start=START, end=START + 1000 * MINUTE_MS,
        step_min=1, btc_bars=synthetic_bars(1000, .0002), version='v3')
    assert ROUND[coin] == level
    assert result['f_coin_id'].unique().to_list() == [ALL_COINS.index(coin)]
    assert result['f_btc_return_60'][900] == pytest.approx(.012)
    close = sources['perp']['close'][899]
    assert result['f_round_distance'][900] == pytest.approx(abs(round(close / level) * level - close) / close)
    assert result['f_dvol'][120] == 41
    for name, group in FEATURE_GROUPS_V3.items():
        if group in ('D', 'E', 'J') and name != 'f_minutes_to_funding':
            assert result[name].null_count() == result.height
    if coin in ('BNB', 'TRX'):
        assert load_coinbase(history, coin, START, END).is_empty()
    assert set(n for n in result.columns if n.startswith('f_')) == set(FEATURE_GROUPS_V3)


def test_v3_trend_formulas_full_windows_and_sampling(history):
    n = 48_001
    sources, market, _ = v2_inputs(history, n)
    log_price = .000001 * np.arange(n) + .12 * np.sin(np.arange(n) / (3 * 1440))
    close = 100 * np.exp(log_price)
    own = synthetic_bars(n, 0.).with_columns(pl.Series('close', close),
        pl.Series('high', close * 1.002), pl.Series('low', close * .998))
    sources['perp'] = own
    def build(step):
        return build_features('ETH', sources, start=START, end=START + n * MINUTE_MS,
            step_min=step, btc_bars=market['BTC'], market_bars=market, version='v3')
    result = build(1)
    i = 47_600
    row = result.row(i, named=True)
    midnight = i // 1440 * 1440
    daily_returns = np.diff(log_price[np.arange(midnight - 30 * 1440, midnight + 1, 1440) - 1])
    for days in (3, 7, 14, 30):
        w = days * 1440
        ret = log_price[i - 1] - log_price[i - w - 1]
        assert row[f'f_return_{w}'] == pytest.approx(ret)
        assert row[f'f_return_daily_z_{w}'] == pytest.approx(ret / (daily_returns.std(ddof=1) * np.sqrt(days)))
        assert result[f'f_return_{w}'][w] is None
        assert result[f'f_return_{w}'][w + 1] is not None
        assert result[f'f_return_daily_z_{w}'][30 * 1440 - 1] is None
    for days in (7, 30):
        w = days * 1440
        assert row[f'f_sma_deviation_{w}'] == pytest.approx(close[i - 1] / close[i - w:i].mean() - 1)
        assert result[f'f_sma_deviation_{w}'][w - 1] is None
        assert row[f'f_btc_return_{w}'] == pytest.approx(.0001 * w)
        # Own ETH replaces the synthetic ETH context; the other four core coins stay fixed.
        ret = log_price[i - 1] - log_price[i - w - 1]
        market_return = (ret + (.0001 + .0003 + .0004 + .0005) * w) / 5
        assert row[f'f_rel_strength_{w}'] == pytest.approx(ret - market_return)
    assert row['f_high_distance_frac_28800'] == pytest.approx(close[i - 1] / (close[i - 28800:i].max() * 1.002) - 1)
    assert row['f_low_distance_frac_28800'] == pytest.approx(close[i - 1] / (close[i - 28800:i].min() * .998) - 1)
    assert result['f_high_distance_frac_28800'][28800 - 1] is None
    assert row['f_positive_day_share_14'] == pytest.approx((daily_returns[-14:] > 0).mean())
    assert result['f_positive_day_share_14'][14 * 1440 - 1] is None
    assert_frame_equal(build(5), result.filter(pl.col('ts_ms') % (5 * MINUTE_MS) == 0), check_exact=True)


def test_v3_dataset_contract_preserves_v2(history, tmp_path):
    metadata = build_dataset(history, tmp_path / 'out', '2026-01-01', '2026-01-02', ['ETH'], version='v3')
    frame = pl.read_parquet(tmp_path / 'out/s2-features-v3/ETH.parquet')
    v2, _ = build_coin(history, 'ETH', '2026-01-01', '2026-01-02', version='v2')
    assert frame.columns[:v2.width] == v2.columns
    assert_frame_equal(frame.select(v2.columns), v2, check_exact=True)
    assert metadata['feature_columns'] == list(FEATURE_GROUPS_V3)
    assert metadata['features'] == [{'name': n, 'group': g} for n, g in FEATURE_GROUPS_V3.items()]
    assert set(metadata['targets']) == {'a', 'b', 'c'}
    assert metadata['parameters']['warmup_days'] == 31
    assert set(metadata['coins']['ETH']['null_share_by_group']) == set('ABCDEFGHIJKL')


@pytest.mark.parametrize('version,option,expected', [
    ('v1', [], COINS), ('v2', [], COINS), ('v3', [], ALL_COINS),
    ('v1', ['--coins', 'all'], ALL_COINS), ('v3', ['--coins', 'bnb,ada'], ('BNB', 'ADA')),
])
def test_dataset_cli_coin_defaults(monkeypatch, version, option, expected):
    import strategy2.dataset as module
    captured = {}
    def fake(root, out, start, end, coins, **kwargs):
        captured['coins'] = coins if coins is not None else (ALL_COINS if kwargs['version'] == 'v3' else COINS)
    monkeypatch.setattr(module, 'build_dataset', fake)
    module.main(['--root', 'synthetic', '--out', 'synthetic', '--from', '2026-01-01',
                 '--to', '2026-01-02', '--version', version, *option])
    assert captured['coins'] == expected


@pytest.mark.parametrize('version,expected', [('v1', COINS), ('v2', COINS), ('v3', ALL_COINS)])
def test_build_dataset_default_universe(history, tmp_path, monkeypatch, version, expected):
    import strategy2.dataset as module
    frame, _ = build_coin(history, 'ETH', '2026-01-01', '2026-01-02', version=version)
    def fake(root, coin, start, end, **kwargs):
        assert tuple(kwargs['market_bars']) == COINS if version != 'v1' else kwargs['market_bars'] is None
        return frame.with_columns(pl.lit(coin).alias('coin'),
            pl.lit(ALL_COINS.index(coin), dtype=pl.Int8).alias('f_coin_id')), {}
    monkeypatch.setattr(module, 'build_coin', fake)
    metadata = build_dataset(history, tmp_path / 'out', '2026-01-01', '2026-01-02', version=version)
    assert tuple(metadata['coins']) == expected
    assert metadata['parameters']['coins'] == list(expected)


def flow_features(source, n, coverage=None):
    grid = pl.DataFrame({'ts_ms': START + np.arange(n) * MINUTE_MS})
    if coverage is None:
        coverage = pl.DataFrame({'start_ms': [START], 'end_ms': [START + n * MINUTE_MS]})
    return _flow_features(grid, source, coverage)


def test_flow_hand_computed_all_features_and_completed_hour_references():
    n = 12_001
    source = synthetic_aggflow(n)
    result = flow_features(source, n)
    i = 11_371
    row = result.row(i, named=True)
    for window in (15, 60, 240, 1440):
        part = source.slice(i - window, window)
        total = (part['buy_notional'] + part['sell_notional']).sum()
        large = sum((part[f'buy_b{b}'] - part[f'sell_b{b}']).sum() for b in (3, 4)) / total
        mid = (part['buy_b2'] - part['sell_b2']).sum() / total
        small = sum((part[f'buy_b{b}'] - part[f'sell_b{b}']).sum() for b in (0, 1)) / total
        for size, expected in (('large', large), ('mid', mid), ('small', small)):
            assert row[f'_flow_{size}_imbalance_{window}'] == pytest.approx(expected)
        if window in (60, 240):
            assert row[f'_flow_large_small_divergence_{window}'] == pytest.approx(large - small)
            toxicity = ((part['buy_notional'] - part['sell_notional']).abs()
                        / (part['buy_notional'] + part['sell_notional'])).mean()
            assert row[f'_flow_toxicity_{window}'] == pytest.approx(toxicity)
        if window in (60, 1440):
            large_total = sum((part[f'buy_b{b}'] + part[f'sell_b{b}']).sum() for b in (3, 4))
            assert row[f'_flow_large_share_{window}'] == pytest.approx(large_total / total)
    last_hour = i // 60 * 60
    hourly_sizes, hourly_maxima = [], []
    for end in range(last_hour - 167 * 60, last_hour + 1, 60):
        part = source.slice(end - 60, 60)
        hourly_sizes.append((part['buy_notional'] + part['sell_notional']).sum() / part['n'].sum())
        hourly_maxima.append(max(part['max_buy_notional'].max(), part['max_sell_notional'].max()))
    part = source.slice(i - 60, 60)
    mean_size = (part['buy_notional'] + part['sell_notional']).sum() / part['n'].sum()
    assert row['_flow_mean_size_z_60'] == pytest.approx(
        (mean_size - np.mean(hourly_sizes)) / np.std(hourly_sizes, ddof=1))
    maximum = max(part['max_buy_notional'].max(), part['max_sell_notional'].max())
    assert row['_flow_max_trade_ratio_60'] == pytest.approx(maximum / np.median(hourly_maxima))
    assert row['_flow_max_trade_side_60'] == np.sign(part['max_buy_notional'].max() - part['max_sell_notional'].max())
    assert len([g for g in FEATURE_GROUPS_V3.values() if g == 'L']) == 21


@pytest.mark.parametrize('missing,valid', [(6, True), (7, False)])
def test_flow_coverage_uses_files_and_no_trade_minutes_are_zero(missing, valid):
    n = 1000
    source = synthetic_aggflow(n)
    # Six missing source minutes are allowed in a 60m window; seven are not.
    coverage = pl.DataFrame({'start_ms': [START, START + (840 + missing) * MINUTE_MS],
        'end_ms': [START + 840 * MINUTE_MS, START + n * MINUTE_MS]})
    row = flow_features(source, n, coverage).row(900, named=True)
    for name in ('_flow_large_imbalance_60', '_flow_max_trade_side_60', '_flow_toxicity_60'):
        assert (row[name] is not None) == valid
    # The same absent trade rows inside a present file are known zeros. Even
    # >10% no-trade minutes must not count as missing file coverage.
    source = source.filter(~pl.col('minute_ms').is_between(START + 840 * MINUTE_MS, START + 859 * MINUTE_MS))
    row = flow_features(source, n).row(900, named=True)
    assert row['_flow_large_imbalance_60'] is not None
    observed = source.filter(pl.col('minute_ms').is_between(START + 840 * MINUTE_MS, START + 899 * MINUTE_MS))
    tox = ((observed['buy_notional'] - observed['sell_notional']).abs()
           / (observed['buy_notional'] + observed['sell_notional'])).sum() / 60
    assert row['_flow_toxicity_60'] == pytest.approx(tox)


def test_flow_zero_denominators_missing_sources_and_max_side_ties():
    empty = pl.DataFrame(schema={**AGGFLOW_SCHEMA, 'available_ms': pl.Int64})
    covered = flow_features(empty, 1000).row(900, named=True)
    for name, value in covered.items():
        if name.startswith('_flow_'):
            assert value == (0. if name in ('_flow_toxicity_60', '_flow_toxicity_240', '_flow_max_trade_side_60') else None)
    absent = _flow_features(pl.DataFrame({'ts_ms': START + np.arange(1000) * MINUTE_MS}), empty, None)
    assert all(absent[name].null_count() == absent.height for name in absent.columns if name != 'ts_ms')
    source = synthetic_aggflow(1000).with_columns(pl.col('max_buy_notional').alias('max_sell_notional'))
    assert flow_features(source, 1000)['_flow_max_trade_side_60'][900] == 0.


def test_flow_weekly_references_require_minute_file_coverage():
    n = 12_001
    source = synthetic_aggflow(n)
    # Each hour retains 54/60 covered minutes, so all hourly mean sizes and
    # maxima are individually valid. The weekly minute rule is also required.
    covered = []
    for hour in range(n // 60):
        start = START + hour * 60 * MINUTE_MS
        covered.append((start, start + 54 * MINUTE_MS))
    intervals = pl.DataFrame(covered, schema={'start_ms': pl.Int64, 'end_ms': pl.Int64}, orient='row')
    valid = flow_features(source, n, intervals)
    assert valid['_flow_max_trade_ratio_60'][11_400] is not None
    # 152 valid hourly values meet the hourly 90% rule, but 16 absent hours
    # plus the six-minute gaps in the remaining hours miss the minute rule.
    sparse = intervals.with_row_index('hour').filter(~pl.col('hour').is_between(30, 45)).drop('hour')
    invalid = flow_features(source, n, sparse)
    hourly = invalid.filter((pl.col('ts_ms') % HOUR_MS == 0) & pl.col('ts_ms').is_between(
        START + (11_400 - 167 * 60) * MINUTE_MS, START + 11_400 * MINUTE_MS))
    assert hourly['_flow_large_imbalance_60'].is_not_null().sum() == 152
    assert invalid['_flow_max_trade_ratio_60'][11_400] is None
    assert invalid['_flow_mean_size_z_60'][11_400] is None


@pytest.mark.parametrize('window', [15, 60, 240, 1440])
@pytest.mark.parametrize('one_extra_missing', [False, True])
def test_flow_minimum_coverage_all_windows(window, one_extra_missing):
    n = 2000
    i = 1800
    missing = window - int(np.ceil(.9 * window)) + int(one_extra_missing)
    start = i - window
    coverage = pl.DataFrame({'start_ms': [START, START + (start + missing) * MINUTE_MS],
        'end_ms': [START + start * MINUTE_MS, START + n * MINUTE_MS]})
    result = flow_features(synthetic_aggflow(n), n, coverage)
    for size in ('large', 'mid', 'small'):
        assert (result[f'_flow_{size}_imbalance_{window}'][i] is None) == one_extra_missing


def test_flow_coverage_never_inferred_from_trade_rows():
    n = 12_000
    source = synthetic_aggflow(n)
    result = _flow_features(pl.DataFrame({'ts_ms': START + np.arange(n) * MINUTE_MS}), source, None)
    assert all(result[name].null_count() == result.height for name in result.columns if name != 'ts_ms')


def test_flow_file_presence_union_empty_files_and_missing_month(tmp_path):
    folder = tmp_path / 'binance/aggflow/ETH'
    folder.mkdir(parents=True)
    source = synthetic_aggflow(1000).drop('available_ms')
    source.write_parquet(folder / '2026-01.parquet')
    source.write_parquet(folder / '2026-01-01.parquet')  # Overlapping day.
    empty = pl.DataFrame(schema=AGGFLOW_SCHEMA)
    empty.write_parquet(folder / '2026-03-01.parquet')
    end = START + 61 * 1440 * MINUTE_MS  # Through Mar 2 (exclusive).
    intervals = _aggflow_coverage(tmp_path, 'ETH', START, end)
    assert intervals.rows() == [(START, START + 31 * 1440 * MINUTE_MS),
        (START + 59 * 1440 * MINUTE_MS, START + 60 * 1440 * MINUTE_MS)]
    result = flow_features(source.with_columns((pl.col('minute_ms') + MINUTE_MS).alias('available_ms')),
                           61 * 1440, intervals)
    # No-trade Jan minutes and a present empty Mar file are covered zero flow.
    assert result['_flow_toxicity_60'][2 * 1440] == 0.
    for name in result.columns:
        if 'imbalance' in name or 'divergence' in name or 'share' in name:
            assert result[name][2 * 1440] is None
    assert result['_flow_toxicity_60'][59 * 1440 + 60] == 0.
    # Missing February must never be filled from adjacent months.
    assert result['_flow_toxicity_60'][40 * 1440] is None
    assert result['_flow_max_trade_side_60'][40 * 1440] is None
    # Availability at Mar 1 00:00 still excludes the 00:00 minute itself.
    assert result['_flow_toxicity_60'][59 * 1440] is None


def test_v3_dataset_loads_flow_and_records_coverage(history, tmp_path):
    folder = history / 'binance/aggflow/ETH'
    folder.mkdir(parents=True)
    synthetic_aggflow(2000).drop('available_ms').write_parquet(folder / '2026-01.parquet')
    metadata = build_dataset(history, tmp_path / 'out', '2026-01-01', '2026-01-02', ['ETH'], version='v3')
    frame = pl.read_parquet(tmp_path / 'out/s2-features-v3/ETH.parquet')
    assert metadata['coins']['ETH']['sources']['aggflow']['rows'] == 2000
    row = frame.filter(pl.col('ts_ms') == START + 900 * MINUTE_MS).row(0, named=True)
    expected = flow_features(synthetic_aggflow(2000), 2000).row(900, named=True)
    for name, group in FEATURE_GROUPS_V3.items():
        if group == 'L':
            assert row[name] == expected['_' + name.removeprefix('f_')]
    # The file covers the full month even beyond its last trading minute.
    assert frame['f_flow_toxicity_60'][-1] == 0.
