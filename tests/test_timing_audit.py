"""Delayed entry labels retain integer precision, isolation and feature-time controls."""

from fractions import Fraction
import json
from pathlib import Path

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal
import pytest

from research.dataset import read_features
from research.evaluation import DAY_NS
from research.fitting import inner_split
from research.labels import NS, load_spec, make_labels
from research.train import (delayed_labels, main, open_lockbox, regime_assignments,
                            train_target, walk_forward_folds)
from test_evaluation import sparse_evaluation
from test_research import row, synthetic_path, write_parquet


@pytest.mark.parametrize('delay', [1, 5, 30])
@pytest.mark.parametrize('horizon', [300, 900, 1800])
def test_delayed_return_and_excursions_use_exact_start_and_unchanged_holding_period(tmp_path, delay, horizon):
    base = 2**53 + 1
    mids = [base + t for t in range(delay + horizon + 2)]
    if delay > 1:
        mids[1] = 1  # A pre-entry drop must not enter the excursion window.
    mids[-1] = 2**63 - 1  # Nor may the second after the endpoint.
    path = write_parquet(tmp_path, [row(t, mid) for t, mid in enumerate(mids)])
    rows = read_features([path], ['BTC_spot'], stride_s=10)
    actual = delayed_labels(rows, 'BTC_spot', horizon, entry_delay_s=delay).filter(pl.col('tick_ns') == 0)
    assert actual['return_pct'][0] == float(Fraction(100 * horizon, mids[delay]))
    assert actual['direction'][0] == 1
    assert actual['label_end_ns'][0] == (delay + horizon) * NS
    assert actual['label_available_ns'][0] == (delay + horizon) * NS + 250_000_000
    assert actual['adverse_long_bps'][0] == 0.
    assert actual['adverse_short_bps'][0] == pytest.approx(float(Fraction(10_000 * horizon, mids[delay])), rel=3e-16)
    expected = make_labels(rows, 'BTC_spot', horizon).filter(pl.col('tick_ns') == delay * NS)
    assert_frame_equal(actual, expected.with_columns(pl.lit(0, dtype=pl.Int64).alias('tick_ns')), check_exact=True)


@pytest.mark.parametrize('missing', ['start', 'end', 'path', 'pre_entry'])
def test_delayed_window_missing_seconds_are_not_interpolated(tmp_path, missing):
    delay, horizon = 5, 300
    absent = {'start': delay, 'end': delay + horizon, 'path': delay + 1, 'pre_entry': 1}[missing]
    rows = read_features([write_parquet(tmp_path, [row(t, 2**53 + 1 + t) for t in range(307) if t != absent])])
    labels = delayed_labels(rows, 'BTC_spot', horizon, entry_delay_s=delay)
    actual = pl.DataFrame({'tick_ns': [0]}).join(labels, on='tick_ns', how='left')
    if missing in {'start', 'end'}:
        assert actual['return_pct'][0] is None and actual['direction'][0] is None
    else:
        assert actual['return_pct'][0] is not None
    if missing != 'pre_entry':
        assert actual['adverse_long_bps'][0] is None and actual['adverse_short_bps'][0] is None
    else:
        assert actual['adverse_long_bps'][0] == 0.


@pytest.mark.parametrize('delay', [-1, 1.5, True, '1'])
def test_delay_requires_a_nonnegative_integer(delay, tmp_path):
    with pytest.raises(ValueError, match='non-negative integer'):
        delayed_labels(pl.DataFrame(), 'BTC_spot', 300, entry_delay_s=delay)
    with pytest.raises(ValueError, match='non-negative integer'):
        train_target(pl.DataFrame(), 'BTC_spot', 300, tmp_path, 'invalid', entry_delay_s=delay)


@pytest.mark.parametrize('delay', ['-1', '1.5', 'true'])
def test_cli_rejects_invalid_delay(tmp_path, delay, capsys):
    with pytest.raises(SystemExit) as exc:
        main([str(tmp_path), '--entry-delay-s', delay])
    assert exc.value.code == 2 and '--entry-delay-s' in capsys.readouterr().err


def test_zero_delay_preserves_labels_models_and_all_predictions(tmp_path, synthetic_path, compact_folds):
    rows = read_features([synthetic_path])
    assert_frame_equal(delayed_labels(rows, 'BTC_spot', 300), make_labels(rows, 'BTC_spot', 300), check_exact=True)
    spec = dict(load_spec(), calendar=None)
    options = dict(spec=spec, rounds=2, num_threads=2, lockbox_from='1970-01-03')
    default = train_target(rows, 'BTC_spot', 300, tmp_path, 'default', **options)
    zero = train_target(rows, 'BTC_spot', 300, tmp_path, 'zero', entry_delay_s=0, **options)
    assert {k: v for k, v in default.items() if k not in {'version', 'model_dir'}} == {
        k: v for k, v in zero.items() if k not in {'version', 'model_dir'}}
    left, right = Path(default['model_dir']), Path(zero['model_dir'])
    assert_frame_equal(pl.read_parquet(left / 'oof.parquet'), pl.read_parquet(right / 'oof.parquet'), check_exact=True)
    for model in left.glob('*.txt'):
        assert model.read_bytes() == (right / model.name).read_bytes()


def test_delayed_end_cannot_cross_development_and_momentum_stays_at_feature_tick(tmp_path, compact_folds):
    boundary = 86400
    rows = read_features([write_parquet(tmp_path, [row(t, 2**53 + 1 + t) for t in range(boundary - 3600, boundary + 401)])])
    spec = dict(load_spec(), calendar=None)
    options = dict(spec=spec, rounds=1, num_threads=2, lockbox_from='1970-01-03')
    default = train_target(rows, 'BTC_spot', 300, tmp_path, 'instant', **options)
    delayed = train_target(rows, 'BTC_spot', 300, tmp_path, 'delayed', entry_delay_s=30, **options)
    probe = (boundary - 310) * NS  # t+h is in development; t+D+h is outside it.
    assert delayed_labels(rows, 'BTC_spot', 300, spec, 30).filter(pl.col('tick_ns') == probe)['return_pct'][0] is not None
    original_rows = pl.read_parquet(Path(default['model_dir']) / 'training_rows.parquet')
    actual_rows = pl.read_parquet(Path(delayed['model_dir']) / 'training_rows.parquet')
    assert probe in original_rows['tick_ns'] and probe not in actual_rows['tick_ns']
    assert actual_rows['label_end_ns'].max() < delayed['date_split']['development_end_ns'] == boundary * NS
    assert actual_rows['label_available_ns'].max() < boundary * NS
    left = pl.read_parquet(Path(default['model_dir']) / 'oof.parquet')
    right = pl.read_parquet(Path(delayed['model_dir']) / 'oof.parquet')
    common = left.select('tick_ns', 'momentum', 'reversal').join(
        right.select('tick_ns', 'momentum', 'reversal'), on='tick_ns', suffix='_delayed')
    assert common.height > 0 and default['features'] == delayed['features']
    for name in ('momentum', 'reversal'):
        np.testing.assert_array_equal(common[name].to_numpy(), common[name + '_delayed'].to_numpy())
    np.testing.assert_array_equal(right['label_start_ns'].to_numpy(), right['tick_ns'].to_numpy() + 30 * NS)
    np.testing.assert_array_equal(right['label_end_ns'].to_numpy(), right['label_start_ns'].to_numpy() + 300 * NS)
    assert right['entry_delay_s'].unique().to_list() == [30]


def test_outer_and_inner_purge_use_delayed_endpoints_without_moving_test_ticks():
    ticks = np.arange(4 * 24 * 120, dtype=np.int64) * 30 * NS
    horizon, delay = 300, 30
    end = ticks + (horizon + delay) * NS
    emitted = end + 250_000_000
    base = walk_forward_folds(ticks, horizon, label_available_ns=ticks + horizon * NS + 250_000_000,
                              fold_days=1, min_train_days=1)
    actual = walk_forward_folds(ticks, horizon, label_available_ns=emitted,
                                fold_days=1, min_train_days=1)
    for (base_train, base_test), (train, test) in zip(base, actual):
        np.testing.assert_array_equal(base_test, test)
        assert set(train) < set(base_train)
        assert np.all(end[train] + horizon * NS < ticks[test[0]])
        assert np.all(emitted[train] + horizon * NS < ticks[test[0]])
    train, valid = inner_split(ticks, end, emitted, horizon, valid_days=1)
    assert np.all(end[train] + horizon * NS < ticks[valid[0]])
    assert np.all(emitted[train] + horizon * NS < ticks[valid[0]])


def test_day_phase_uses_feature_utc_time_and_is_in_pooled_and_lockbox_summaries(tmp_path, sparse_evaluation):
    ticks = np.array([0, 3600 * NS - 1, 3600 * NS, DAY_NS - 1, DAY_NS, DAY_NS + 3599 * NS, -NS])
    groups = regime_assignments(ticks, np.ones(len(ticks)), [0., 2.])
    np.testing.assert_array_equal(groups['day_phase'], np.array([0, 0, 1, 1, 0, 0, 1], dtype=np.int8))
    rows, spec, _ = sparse_evaluation
    report = train_target(rows, 'BTC_spot', 300, tmp_path, 'phase', spec=spec, rounds=1, num_threads=2)
    frame = pl.read_parquet(Path(report['model_dir']) / 'oof.parquet')
    assert frame.schema['day_phase'] == pl.Int8
    np.testing.assert_array_equal(frame['day_phase'].to_numpy(), (frame['tick_ns'].to_numpy() % DAY_NS >= 3600 * NS).astype(np.int8))
    phase = report['regimes']['day_phase']
    assert list(phase) == ['first_hour', 'rest']
    assert 0 < phase['first_hour']['regressor']['n'] < report['metrics']['regressor']['n']
    held = open_lockbox(rows, tmp_path, 'phase', num_threads=2)['results'][0]['lockbox_results']
    assert held['regimes']['day_phase']['first_hour']['regressor']['n'] > 0
    assert sum(g['regressor']['n'] for g in held['regimes']['day_phase'].values()) == held['metrics']['regressor']['n']


def test_delay_cli_metadata_experiment_and_html(tmp_path, synthetic_path, compact_folds):
    output = tmp_path / 'output'
    result = main([str(synthetic_path), '--output-root', str(output), '--target', 'BTC_spot',
        '--horizon-s', '300', '--rounds', '1', '--threads', '2', '--version', 'timing',
        '--lockbox-from', '1970-01-03', '--entry-delay-s', '5'])
    report = result['results'][0]
    assert report['entry_delay_s'] == 5
    directory = Path(report['model_dir'])
    assert json.loads((directory / 'metadata.json').read_text())['entry_delay_s'] == 5
    assert pl.read_parquet(directory / 'oof.parquet')['entry_delay_s'].unique().to_list() == [5]
    entry = json.loads((output / 'experiments.jsonl').read_text())
    assert entry['entry_delay_s'] == entry['results'][0]['entry_delay_s'] == 5
    html = (output / 'report_timing.html').read_text()
    assert 'entry delay 5 s' in html and 'day_phase' in html
