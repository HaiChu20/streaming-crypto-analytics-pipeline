"""Causal features tested with synthetic history in the public mirror layout."""

import json
from zipfile import ZipFile

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal
import pytest

from strategy2.bars import MINUTE_MS, load_bars
from strategy2.baselines import join_baselines
from strategy2.dataset import TARGETS, build_dataset
from strategy2.features import FEATURE_GROUPS, build_features
from strategy2.sources import BANDS, HOUR_MS, METRICS, load_depth, load_funding, load_metrics, load_sources

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


def test_generic_no_lookahead_all_sources(tmp_path):
    # More than 30 days exercises long-window features as well as short ones.
    mirror(tmp_path, n=47_000)
    cutoff = START + 45_000 * MINUTE_MS
    end = START + 47_000 * MINUTE_MS
    sources = load_sources(tmp_path, 'ETH', START, end)
    btc = load_bars(tmp_path, 'perp', 'BTC', START, end)
    eth = load_bars(tmp_path, 'perp', 'ETH', START, end)
    def build(s, b, e):
        return build_features('ETH', s, start=START, end=end, btc_bars=b, eth_bars=e).filter(
            pl.col('ts_ms') <= cutoff).select('ts_ms', *FEATURE_GROUPS)
    original = build(sources, btc, eth)
    def perturb(frame, availability):
        return frame.with_columns(*[pl.when(availability > cutoff).then(pl.col(name) * 1.7 + .003)
            .otherwise(pl.col(name)).cast(dtype).alias(name)
            for name, dtype in frame.schema.items() if name not in
            ('ts_ms', 'available_ms', 'next_funding_ms') and dtype.is_numeric()])
    changed = {name: perturb(frame, pl.col('available_ms')) for name, frame in sources.items()}
    actual = build(changed, perturb(btc, pl.col('ts_ms') + MINUTE_MS),
                   perturb(eth, pl.col('ts_ms') + MINUTE_MS))
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
    assert len(saved['features']) == len(FEATURE_GROUPS)
    assert set(saved['coins']['BTC']['null_share_by_group']) == set('ABCDEFGH')
