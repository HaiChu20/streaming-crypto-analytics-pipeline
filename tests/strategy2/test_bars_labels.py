"""Synthetic minute-bar parsing, causal labels and baseline checks."""

from datetime import datetime, timezone
from time import perf_counter
from zipfile import ZipFile

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal
import pytest

from strategy2.bars import BAR_SCHEMA, CSV_COLUMNS, MINUTE_MS, load_bars
from strategy2.base_rates import main, median_hour_range
from strategy2.baselines import PROB_COLUMNS, momentum, prior, reversal, vol_only
from strategy2.labels import first_touch_labels


def bars(n=8, *, start=0, price=100.):
    return pl.DataFrame({'ts_ms': np.arange(start, start + n, dtype=np.int64) * MINUTE_MS,
                         **{name: np.full(n, price) for name in ('open', 'high', 'low', 'close')}})


def replace(frame, index, **values):
    return frame.with_columns(*[
        pl.when(pl.col('ts_ms') == index * MINUTE_MS).then(pl.lit(value))
        .otherwise(pl.col(name)).alias(name) for name, value in values.items()
    ])


def write_zip(root, cadence='monthly', *, header=True, microseconds=False,
              close='100.25', stamp='2026-01', market='perp', close_time_delta=0, ignore=0):
    base = root / 'binance' / ('futures/um' if market == 'perp' else 'spot')
    path = base / cadence / 'klines/BTCUSDT/1m' / f'BTCUSDT-1m-{stamp}.zip'
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    scale = 1000 if microseconds else 1
    rows = [','.join(map(str, [(ts + offset * MINUTE_MS) * scale, 100, 101, 99, close,
                              12, (ts + (offset + 1) * MINUTE_MS - 1 + close_time_delta) * scale,
                              1200, 42, 7, 700, ignore])) for offset in (0, 2)]
    with ZipFile(path, 'w') as archive:
        archive.writestr('klines.csv', (','.join(CSV_COLUMNS) + '\n' if header else '')
                         + '\n'.join(rows) + '\n')


@pytest.mark.parametrize('header', [False, True])
@pytest.mark.parametrize('microseconds,market', [(False, 'perp'), (True, 'spot')])
def test_zip_header_timestamp_and_missing_minutes(tmp_path, header, microseconds, market):
    write_zip(tmp_path, header=header, microseconds=microseconds, market=market)
    result = load_bars(tmp_path, market, 'BTC', '2026-01-01', '2026-01-01')
    assert result.schema == BAR_SCHEMA
    assert result.height == 2
    assert result['ts_ms'].diff()[1] == 2 * MINUTE_MS
    assert result['ts_ms'][0] == 1767225600000
    assert result['close'].to_list() == [100.25, 100.25]
    assert result['trades'].to_list() == [42, 42]
    assert result['taker_buy_base'].to_list() == [7., 7.]
    # Datetime boundaries are half open; date end boundaries are inclusive.
    assert load_bars(tmp_path, market, 'BTCUSDT', '2026-01-01T00:00:00+00:00',
                     '2026-01-01T00:02:00+00:00').height == 1


@pytest.mark.parametrize('conflict', [{'close': '100.26'}, {'close_time_delta': 1}, {'ignore': 1}])
def test_overlap_deduplicates_and_conflicts_raise(tmp_path, conflict):
    write_zip(tmp_path)
    write_zip(tmp_path, 'daily', header=False, stamp='2026-01-01')
    assert load_bars(tmp_path, 'perp', 'BTC', '2026-01-01', '2026-01-31').height == 2
    write_zip(tmp_path, 'daily', stamp='2026-01-01', **conflict)
    with pytest.raises(ValueError, match='conflicting duplicate'):
        load_bars(tmp_path, 'perp', 'BTC', '2026-01-01', '2026-01-31')


def test_empty_load_and_validation(tmp_path):
    assert load_bars(tmp_path, 'perp', 'BTC', '2026-01-01', '2026-01-31').schema == BAR_SCHEMA
    with pytest.raises(ValueError, match='market'):
        load_bars(tmp_path, 'future', 'BTC', '2026-01-01', '2026-01-31')
    with pytest.raises(ValueError, match='end'):
        load_bars(tmp_path, 'perp', 'BTC', '2026-02-01', '2026-01-31')


@pytest.mark.parametrize('high,low,label', [(101., 100., 1), (100., 99., -1),
                                          (100.4, 99.6, 0), (101., 99., None)])
def test_first_touch_outcomes_and_equality(high, low, label):
    frame = replace(bars(), 1, high=high, low=low)
    # A later bar hits the opposite barrier. First touch must win.
    if label in (-1, 1):
        frame = replace(frame, 2, high=103., low=97.)
    result = first_touch_labels(frame, .01, 3, 1, 0).row(0, named=True)
    assert result['label'] == label
    assert result['tie'] == (label is None)
    assert result['incomplete'] is False
    assert result['minutes_to_touch'] == (None if label == 0 else 2)
    assert result['label_end_ms'] == 3 * MINUTE_MS
    if label in (-1, 1):
        assert result['exit_return_pct'] == label * .01
        assert result['max_up_pct'] < .02
        assert result['max_down_pct'] < .02
    elif label is None:
        assert result['exit_return_pct'] is None
    else:
        assert result['exit_return_pct'] == 0


def test_time_stop_excursions_and_last_window_bar():
    frame = replace(bars(), 1, high=100.6, low=99.7)
    frame = replace(frame, 2, high=100.7, low=99.2, close=100.3)
    row = first_touch_labels(frame, .01, 3, 1, 0).row(0, named=True)
    assert row['max_up_pct'] == pytest.approx(.007)
    assert row['max_down_pct'] == pytest.approx(.008)
    assert row['exit_return_pct'] == pytest.approx(.003)
    frame = replace(frame, 2, high=101.)
    assert first_touch_labels(frame, .01, 3, 1, 0)['minutes_to_touch'][0] == 3


@pytest.mark.parametrize('missing', [0, 1, 2])
def test_missing_entry_or_any_window_bar_is_null_even_after_touch(missing):
    frame = replace(bars(), 0, high=102.)
    # Keep the decision present, put entry at minute 1.
    frame = replace(frame, 1, high=102.).filter(pl.col('ts_ms') != (missing + 1) * MINUTE_MS)
    row = first_touch_labels(frame, .01, 3, 1, 1).row(0, named=True)
    assert row['label'] is None
    assert row['incomplete'] is True
    assert row['exit_return_pct'] is None
    assert row['max_up_pct'] is None


def test_latency_and_utc_sampling_and_missing_decision():
    frame = replace(bars(14), 1, high=102.)
    immediate = first_touch_labels(frame, .01, 2, 1, 0)
    delayed = first_touch_labels(frame, .01, 2, 1, 2)
    assert immediate['label'][0] == 1
    assert delayed['label'][0] == 0
    assert delayed['entry_ms'][0] == 2 * MINUTE_MS
    sampled = first_touch_labels(frame.slice(2), .01, 2)
    assert sampled['ts_ms'].to_list() == [5 * MINUTE_MS, 10 * MINUTE_MS]
    gap = frame.filter(pl.col('ts_ms') != 5 * MINUTE_MS)
    assert first_touch_labels(gap, .01, 2)['label'][1] == 0


def test_no_lookahead_or_pre_entry_dependence():
    frame = replace(bars(20), 5, high=102.)
    original = first_touch_labels(frame, .01, 5, 1, 1).filter(pl.col('ts_ms') == 3 * MINUTE_MS)
    altered = replace(frame, 2, open=400., high=401., low=399., close=400.)
    altered = replace(altered, 9, open=200., high=250., low=150., close=200.)
    actual = first_touch_labels(altered, .01, 5, 1, 1).filter(pl.col('ts_ms') == 3 * MINUTE_MS)
    assert_frame_equal(original, actual)


def test_random_labels_match_simple_reference():
    rng = np.random.default_rng(19)
    price = 100 * np.exp(np.cumsum(rng.normal(0, .002, 120)))
    frame = pl.DataFrame({'ts_ms': np.arange(120) * MINUTE_MS, 'open': price,
                          'high': price * 1.002, 'low': price * .998, 'close': price})
    for horizon in (1, 7, 30):
        result = first_touch_labels(frame, .005, horizon, 1, 0)
        for i in range(120 - horizon + 1):
            label, touch, exit_offset = 0, None, horizon - 1
            for offset in range(horizon):
                up = frame['high'][i + offset] >= price[i] * 1.005
                down = frame['low'][i + offset] <= price[i] * .995
                if up or down:
                    label = None if up and down else (1 if up else -1)
                    touch, exit_offset = offset + 1, offset
                    break
            assert result['label'][i] == label
            assert result['minutes_to_touch'][i] == touch
            assert result['max_up_pct'][i] == pytest.approx(max(0, frame['high'][i:i + exit_offset + 1].max() / price[i] - 1))
            assert result['max_down_pct'][i] == pytest.approx(max(0, 1 - frame['low'][i:i + exit_offset + 1].min() / price[i]))


@pytest.mark.parametrize('kwargs', [{'barrier_pct': 0}, {'barrier_pct': float('nan')},
                                  {'horizon_min': 0}, {'step_min': 0}, {'latency_min': -1}])
def test_invalid_label_parameters(kwargs):
    with pytest.raises(ValueError):
        first_touch_labels(bars(), **dict({'barrier_pct': .01, 'horizon_min': 3}, **kwargs))


def test_empty_tail_and_bad_timestamps():
    assert first_touch_labels(bars(0), .01, 3).height == 0
    assert first_touch_labels(bars(2), .01, 3, 1, 0)['label'].null_count() == 2
    with pytest.raises(ValueError, match='sorted, unique'):
        first_touch_labels(bars().reverse(), .01, 3)
    with pytest.raises(ValueError, match='sorted, unique'):
        first_touch_labels(bars().with_columns(pl.col('ts_ms') + 1), .01, 3)


def volatility_bars(sigma):
    frame = bars(140)
    # Alternating equal-magnitude log returns give known realized variance.
    return frame.with_columns(pl.Series('close', 100 * np.exp(sigma * (np.arange(140) % 2))))


def test_vol_probabilities_series_value_and_sigma_monotonicity():
    probabilities = []
    for sigma in (0., .0002, .001, .003, .1):
        result = vol_only(volatility_bars(sigma), .005, 60)
        assert result['p_none'][:61].null_count() == 61
        values = result.select(PROB_COLUMNS).drop_nulls().to_numpy()
        assert np.all((values >= 0) & (values <= 1))
        np.testing.assert_allclose(values.sum(axis=1), 1)
        np.testing.assert_allclose(values[:, 0], values[:, 1])
        probabilities.append(1 - values[0, 2])
    assert np.all(np.diff(probabilities) > 0)
    # At sigma*sqrt(T)/a = 1 the standard survival series is ~0.37077743.
    known = vol_only(volatility_bars(.005 / np.sqrt(60)), .005, 60)
    assert known['p_none'][70] == pytest.approx(.3707774298)


def test_vol_and_direction_only_use_strictly_pre_entry_history():
    frame = volatility_bars(.001)
    changed = replace(frame, 90, close=180.)
    # Changing the entry bar or any later bar cannot change its forecast.
    assert_frame_equal(vol_only(frame, .005, 60).slice(90, 1), vol_only(changed, .005, 60).slice(90, 1))
    assert_frame_equal(momentum(frame, 60).slice(90, 1), momentum(changed, 60).slice(90, 1))
    changed = replace(frame, 89, close=180.)
    assert vol_only(frame, .005, 60)['p_none'][90] != vol_only(changed, .005, 60)['p_none'][90]
    gap = frame.filter(pl.col('ts_ms') != 80 * MINUTE_MS)
    assert vol_only(gap, .005, 60).filter(pl.col('ts_ms') == 90 * MINUTE_MS)['p_none'][0] is None
    assert momentum(gap, 60).filter(pl.col('ts_ms') == 90 * MINUTE_MS)['direction'][0] is None


def test_prior_momentum_and_reversal():
    assert prior(pl.Series([1, 1, -1, 0, None])) == dict(zip(PROB_COLUMNS, [.5, .25, .25]))
    with pytest.raises(ValueError):
        prior(pl.Series('label', [], dtype=pl.Int8))
    frame = bars(10).with_columns(pl.Series('close', np.arange(100., 110.)))
    assert momentum(frame, 3)['direction'].to_list() == [None] * 4 + [1] * 6
    assert reversal(frame, 3)['direction'].to_list() == [None] * 4 + [-1] * 6
    assert momentum(bars(10), 3)['direction'][5] == 0


def test_cli_and_median_hour_range(tmp_path, capsys):
    write_zip(tmp_path)
    main(['--root', str(tmp_path), '--from', '2026-01-01', '--to', '2026-01-01', '--coins', 'BTC'])
    assert 'step=5m latency=1m' in capsys.readouterr().out
    frame = bars(65).with_columns(pl.lit(101.).alias('high'), pl.lit(99.).alias('low'))
    assert median_hour_range(frame) == .02
    assert median_hour_range(frame.filter(pl.col('ts_ms') != 30 * MINUTE_MS)) is None


def test_two_million_rows_performance():
    # Worst case: flat bars never exit early, so all 240 offsets are visited.
    frame = bars(2_000_000)
    started = perf_counter()
    result = first_touch_labels(frame, .01, 240, 1, 0)
    elapsed = perf_counter() - started
    assert result.height == 2_000_000
    assert result['label'].null_count() == 239
    assert result['label'].drop_nulls().sum() == 0
    print(f'2M rows x 240 minutes: {elapsed:.3f}s')
    # A generous guard for shared/slow CI; normal hardware completes in seconds.
    assert elapsed < 60
