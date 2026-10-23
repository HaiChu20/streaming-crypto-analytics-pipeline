"""Small, offline lead/lag contracts including exact lag and response direction."""
import json

import numpy as np
import polars as pl
import pytest

from research.leadlag import (analyze, correlation, lag_stats, llr, main,
                              response_stats, response_summary)


def test_copied_returns_have_positive_peak_and_reversed_llr():
    rng = np.random.default_rng(17)
    x = rng.normal(0, .001, 8000)
    y = np.r_[np.full(4, np.nan), x[:-4]]
    curve = correlation(lag_stats(x, y, 15))
    assert np.nanargmax(curve) - 15 == 4
    assert curve[19] == pytest.approx(1.)
    assert llr(curve) > 100
    reverse = correlation(lag_stats(y, x, 15))
    assert np.nanargmax(reverse) - 15 == -4
    assert llr(reverse) == pytest.approx(1 / llr(curve))


def test_fft_null_handling_matches_uncompressed_overlap_pearson():
    rng = np.random.default_rng(7)
    x, y = rng.normal(size=(2, 60))
    x[5:14] = np.nan
    y[23:28] = np.nan
    actual = correlation(lag_stats(x, y, 8, 10, 45))
    valid = np.isfinite(x) & np.isfinite(y)
    for k in range(-8, 9):
        first = np.arange(10, 45)
        first = first[(first + k >= 0) & (first + k < len(x))]
        first = first[valid[first] & valid[first + k]]
        expected = np.corrcoef(x[first], y[first + k])[0, 1]
        assert actual[k + 8] == pytest.approx(expected, abs=1e-12)
    assert np.isnan(correlation(lag_stats(np.ones(20), np.ones(20), 3))).all()
    assert np.isnan(correlation(lag_stats(np.full(20, np.nan), np.ones(20), 3))).all()


def test_response_both_signs_zero_fraction_and_missing_path():
    # Two isolated shocks; 25% simultaneous and 75% one step later.
    leader = np.zeros(400)
    leader[20:200] = .001
    follower = .25 * leader + .75 * np.r_[0, leader[:-1]]
    result = response_summary(response_stats(leader, follower, 100_000_000), 100_000_000)
    assert result['events'].tolist() == [2] * 7
    assert result['mean_signed_bps'][0] == pytest.approx(2.5)
    assert result['mean_signed_bps'][1:] == pytest.approx([10.] * 6)
    assert result['fraction_already_done_0s'] == pytest.approx(.25)
    follower[22] = np.nan
    missing = response_summary(response_stats(leader, follower, 100_000_000), 100_000_000)
    assert missing['events'][0] == 2
    assert missing['events'][-1] == 1
    assert missing['fraction_already_done_0s'] == pytest.approx(.25)


def write_day(path, day, lag=3):
    x = np.random.default_rng(day).normal(0, .0005, 1600)
    y = np.r_[np.zeros(lag), x[:-lag]]
    a, b = np.exp(np.cumsum(x)) * 100e9, np.exp(np.cumsum(y)) * 100e9
    pl.DataFrame({'tick_ns': day * 86400 * 10**9 + np.arange(len(x)) * 100_000_000,
                  'binance_spot_BTCUSDT_mid_e9': a.astype(np.int64),
                  'kraken_perp_PF_XBTUSD_mid_e9': b.astype(np.int64)}).write_parquet(path)


def test_cli_json_daily_pooled_hourly_bootstrap_and_pairs(tmp_path, capsys):
    for day in (20000, 20001):
        folder = tmp_path / f'date={day}'
        folder.mkdir()
        write_day(folder / 'mids.parquet', day)
    output = tmp_path / 'summary.json'
    report = main([str(tmp_path), '--json-out', str(output), '--max-lag-ms', '1000'])
    assert json.loads(output.read_text()) == report
    assert 'binance_spot_BTCUSDT' in capsys.readouterr().out
    assert report['grid_ms'] == 100
    pair, = report['pairs']
    assert len(pair['days']) == 2
    assert [d['peak_lag_ms'] for d in pair['days']] == [300, 300]
    pooled = pair['pooled']
    assert pooled['peak_lag_ms'] == 300 and pooled['llr'] > 10
    assert pooled['leader'] == pair['first']
    assert pooled['bootstrap']['draws'] == 1000
    assert pooled['bootstrap']['c_peak']['valid_draws'] == 1000
    assert len(pooled['hourly']) == 24
    assert pooled['hourly'][0]['llr'] > 10
    assert pooled['hourly'][1]['llr'] is None
    assert len(pooled['response_first_leads']['mean_signed_bps']) == 7
    reverse = analyze([tmp_path], 1000, [pair['second'] + ':' + pair['first']])
    assert reverse['pairs'][0]['pooled']['peak_lag_ms'] == -300


def test_grid_and_pair_validation_and_no_day_bridging(tmp_path):
    path = tmp_path / 'mids.parquet'
    write_day(path, 20000)
    with pytest.raises(ValueError, match='pairs'):
        analyze([path], pairs=['unknown:other'])
    frame = pl.read_parquet(path)
    frame.filter(pl.col('tick_ns') != frame['tick_ns'][10]).write_parquet(tmp_path / 'missing.parquet')
    with pytest.raises(ValueError, match='grid ticks'):
        analyze([tmp_path / 'missing.parquet'])
    # Each day is independently differenced: an overnight jump is not a shock.
    flat = pl.DataFrame({'tick_ns': np.r_[np.arange(200), 864000 + np.arange(200)] * 100_000_000,
                         'a_mid_e9': [100_000_000_000] * 200 + [200_000_000_000] * 200,
                         'b_mid_e9': [100_000_000_000] * 200 + [200_000_000_000] * 200})
    flat.write_parquet(tmp_path / 'flat.parquet')
    pair, = analyze([tmp_path / 'flat.parquet'])['pairs']
    assert pair['pooled']['peak_lag_ms'] is None
    assert pair['pooled']['response_first_leads']['events'] == [0] * 7
