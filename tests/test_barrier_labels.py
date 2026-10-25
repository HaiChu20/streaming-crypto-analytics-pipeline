"""First-touch barriers preserve evaluation contracts and exact integer thresholds."""

from fractions import Fraction
from io import BytesIO
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from research.labels import (NS, _exact_exit_returns, exact_return_pct, load_spec,
                             make_excursion_labels, make_labels, momentum_900s)
from research.train import delayed_labels, open_lockbox, train_target
from test_risk import frame
from test_training_options import training_rows


def spec_for(horizon, pct):
    return dict(load_spec(), barriers_pct={horizon: pct})


def test_barrier_spec_preserves_kraken_references_and_calendar():
    old = load_spec('research/targets_kraken_short.yaml')
    assert load_spec('research/targets_kraken_barrier.yaml') == dict(
        old, version='v1-kraken-spot-barrier', horizons_s=[3600, 14400],
        barriers_pct={3600: .5, 14400: 1.})


@pytest.mark.parametrize('mids,direction,exit_mid', [
    ([10000, 10051, 9940, 10010], 1, 10051),
    ([10000, 9949, 10060, 10010], -1, 9949),
    ([10000, 10049, 9951, 10010], 0, 10010),
    ([10000, 10050, 9940, 10010], 1, 10050),
    ([10000, 9950, 10060, 10010], -1, 9950),
    ([10000, 10000, 10000, 10050], 1, 10050),
])
def test_first_touch_order_neither_equality_and_endpoint(mids, direction, exit_mid):
    rows = frame(mids)
    labels = make_labels(rows, 'BTC_spot', 3, spec_for(3, .5))
    assert labels['direction'][0] == direction
    assert labels['return_pct'][0] == exact_return_pct(mids[0], exit_mid)
    assert labels['label_end_ns'][0] == 3 * NS
    assert labels['label_available_ns'][0] == 3 * NS + 250_000_000
    assert_frame_equal(labels.select('tick_ns', pl.col('^.*_bps$')),
                       make_excursion_labels(rows, 'BTC_spot', 3))
    assert labels['direction'][-1] is None
    assert_frame_equal(labels, make_labels(rows, 'BTC_spot', 3, json.loads(json.dumps(spec_for(3, .5)))))


def test_same_second_tie_is_null_and_full_path_excursions_remain():
    # One mid cannot touch both positive-width barriers in a single second.
    # Zero width exercises the explicitly required ambiguity rule.
    labels = make_labels(frame([100, 100, 102]), 'BTC_spot', 2, spec_for(2, 0))
    assert labels['return_pct'][0] is labels['direction'][0] is None
    assert labels['label_end_ns'][0] == 2 * NS
    assert labels['label_available_ns'][0] == 2 * NS + 250_000_000
    assert labels['favorable_long_bps'][0] == 200


@pytest.mark.parametrize('defect', ['missing', 'invalid', 'warm', 'null', 'zero', 'offgrid'])
@pytest.mark.parametrize('second', [0, 2, 3])
def test_whole_window_required_even_after_an_early_touch(defect, second):
    rows = frame([10000, 10100, 9900, 10000])
    if defect == 'missing':
        rows = rows.filter(pl.col('tick_ns') != second * NS)
        if second == 0:
            return
    else:
        column, value = {'invalid': ('book_valid', False), 'warm': ('warmup', True),
                         'null': ('mid_e9', None), 'zero': ('mid_e9', 0),
                         'offgrid': ('tick_ns', second * NS + 1)}[defect]
        rows = rows.with_columns(pl.when(pl.col('tick_ns') == second * NS).then(pl.lit(value))
                                .otherwise(pl.col(column)).alias(column))
    labels = make_labels(rows, 'BTC_spot', 3, spec_for(3, .5))
    assert labels['direction'][0] is labels['return_pct'][0] is None
    assert labels['adverse_long_bps'][0] is None


@pytest.mark.parametrize('start,pct,delta,direction', [
    (2**53 + 1, '0.00000000000001', 1, 1),
    (2**53 + 1, '0.00000000000001', -1, -1),
    (2**63 - 2, '0.00000000000000001', 1, 1),
    (2**63 - 2, .5, 1, 0),  # upper threshold exceeds Int64; no overflow or false touch
    (200 * (2**53 + 1), .5, 2**53 + 1, 1),  # exact equality above 2**53
    (200 * (2**53 + 1), .5, -(2**53 + 1), -1),
])
def test_large_integer_thresholds_and_returns(start, pct, delta, direction):
    # An unrelated null must not coerce the entire dense price array to Float64.
    rows = frame([start, start + delta, start, None, start])
    labels = make_labels(rows, 'BTC_spot', 2, spec_for(2, pct))
    assert labels['direction'][0] == direction
    end = start + delta if direction else start
    expected = float(Fraction(100 * (end - start), start))
    assert labels['return_pct'][0] == expected
    if direction:
        assert labels['return_pct'][0] != 0


@pytest.mark.parametrize('pct', [-.5, float('nan'), float('inf'), 'bad'])
def test_invalid_barrier_pct_rejected(pct):
    with pytest.raises(ValueError, match='finite and non-negative'):
        make_labels(frame([100, 101]), 'BTC_spot', 1, spec_for(1, pct))


def test_empty_and_offgrid_inputs():
    for rows in (frame([100, 101]).head(0),
                 frame([100, 101]).with_columns(pl.col('tick_ns') + 1)):
        labels = make_labels(rows, 'BTC_spot', 1, spec_for(1, .5))
        assert labels.height == rows.height
        assert labels['direction'].null_count() == rows.height
        assert_frame_equal(labels.select('label_end_ns'), rows.select(pl.col('tick_ns') + NS)
                           .rename({'tick_ns': 'label_end_ns'}))


@pytest.mark.parametrize('horizon', [1, 2, 3, 4, 7, 8, 15, 32, 61])
def test_vectorized_search_matches_exact_brute_force(horizon):
    rng = np.random.default_rng(718)
    mids = rng.integers(9900, 10101, size=150).tolist()
    pct = Fraction('0.5')
    labels = make_labels(frame(mids), 'BTC_spot', horizon, spec_for(horizon, .5))
    for i, start in enumerate(mids[:-horizon]):
        expected_direction, end = 0, mids[i + horizon]
        for candidate in mids[i + 1:i + horizon + 1]:
            up = candidate * 100 * pct.denominator >= start * (100 * pct.denominator + pct.numerator)
            down = candidate * 100 * pct.denominator <= start * (100 * pct.denominator - pct.numerator)
            if up or down:
                expected_direction, end = int(up) - int(down), candidate
                break
        assert labels['direction'][i] == expected_direction
        assert labels['return_pct'][i] == exact_return_pct(start, end)


def test_entry_delay_shifts_start_without_shortening_conservative_endpoints():
    rows = frame([10000, 10000, 10100, 9900, 10000, 10010])
    spec = spec_for(3, .5)
    original = make_labels(rows, 'BTC_spot', 3, spec)
    delayed = delayed_labels(rows, 'BTC_spot', 3, spec, entry_delay_s=1)
    assert_frame_equal(delayed, original.with_columns(pl.col('tick_ns') - NS))
    first = delayed.filter(pl.col('tick_ns') == 0)
    assert first['direction'][0] == 1
    assert first['return_pct'][0] == 1.
    assert first['label_end_ns'][0] == 4 * NS
    assert first['label_available_ns'][0] == 4 * NS + 250_000_000


@pytest.mark.parametrize('horizon', [1, 2, 3, 60, 300, 900, 1800])
def test_nonbarrier_horizons_have_identical_serialized_output(horizon):
    rows = frame(10000 + np.arange(2 * horizon + 3) % 7 * 10).with_columns(
        pl.when(pl.col('tick_ns') == NS).then(False).otherwise(pl.col('book_valid')).alias('book_valid'))
    original = make_labels(rows, 'BTC_spot', horizon, load_spec())
    mixed = make_labels(rows, 'BTC_spot', horizon, spec_for(14400, 1.))
    a, b = BytesIO(), BytesIO()
    original.write_ipc(a)
    mixed.write_ipc(b)
    assert a.getvalue() == b.getvalue()


@pytest.mark.parametrize('horizon', [3600, 14400])
def test_barrier_training_oof_metadata_baselines_and_lockbox(tmp_path, horizon):
    rows = training_rows(days=9, seconds=horizon + 1801).filter(pl.col('asset') == 'BTC').with_columns(
        pl.lit('kraken').alias('exchange'), pl.lit('BTC/USD').alias('symbol'),
        (100 * NS + (np.sin(pl.col('tick_ns') / NS / 170)
            * pl.when(pl.col('tick_ns') // (86400 * NS) % 2 == 0)
                .then(1_000_000_000).otherwise(100_000_000)).cast(pl.Int64)).alias('mid_e9'))
    spec = load_spec('research/targets_kraken_barrier.yaml')
    report = train_target(rows, 'BTC_spot', horizon, tmp_path, 'barrier', spec=spec,
        rounds=2, num_threads=2, stride_s=30, entry_delay_s=1,
        lockbox_from='1970-01-08', fold_days=1, min_train_days=2, max_folds=2,
        early_stopping=True, inner_valid_days=1, early_stopping_rounds=2, shuffle_test=True)
    assert report['status'] == 'trained'
    assert report['barrier_pct'] == spec['barriers_pct'][horizon]
    assert 'accuracy excludes "no touch" (flat) rows' in report['metric_notes']
    root = Path(report['model_dir'])
    metadata = json.loads((root / 'metadata.json').read_text())
    assert metadata['barrier_pct'] == report['barrier_pct']
    assert metadata['label_spec'] == json.loads(json.dumps(spec))
    html = (root.parent / 'report_barrier.html').read_text()
    assert f'{horizon} seconds · barrier ±{report["barrier_pct"]}%' in html
    oof = pl.read_parquet(root / 'oof.parquet')
    assert oof.height > 0
    assert oof.filter((pl.col('direction') == 0) & (pl.col('return_pct') != 0)).height > 0
    touched = oof.filter(pl.col('direction') != 0).height
    assert report['metrics']['classifier']['directional_n'] == touched
    assert report['metrics']['prior']['directional_n'] == touched
    assert sum(group['classifier']['directional_n'] for group in report['regimes']['week_part'].values()) == touched
    assert report['shuffle_test']['classifier']['directional_n'] == touched
    trailing = momentum_900s(rows, 'BTC_spot', spec)
    joined = oof.join(trailing, on='tick_ns')
    np.testing.assert_array_equal(joined['momentum'], joined['momentum_return_900s'].to_numpy() * (horizon / 900))
    np.testing.assert_array_equal(oof['reversal'], -oof['momentum'].to_numpy())
    assert oof['direction'].is_in([-1, 0, 1]).all()
    labels = delayed_labels(rows, 'BTC_spot', horizon, spec, 1).drop('favorable_long_bps', 'favorable_short_bps')
    expected = oof.select('tick_ns').join(labels, on='tick_ns')
    assert_frame_equal(oof.select(expected.columns), expected)
    assert report['metrics']['prior']['n'] == oof.height
    from research.compare import compare_runs
    from research.evaluation import class_prior
    comparison = compare_runs(root, root, draws=10)
    assert comparison['metrics']['classifier_directional_accuracy']['rows_a'] == touched
    assert comparison['metrics']['classifier_log_loss']['a'] == report['metrics']['classifier']['log_loss']
    for fold in report['folds']:
        training = labels.filter(
            (pl.col('tick_ns') >= fold['train_start_ns'])
            & (pl.col('tick_ns') <= fold['train_end_ns'])
            & (pl.col('tick_ns') % (30 * NS) == 0)).drop_nulls('direction')
        assert training.height == fold['train_rows']
        expected_prior = class_prior(training['direction'].to_numpy())
        fold_oof = oof.filter(pl.col('fold') == fold['fold'])
        prior = fold_oof.select('prior_p_down', 'prior_p_flat', 'prior_p_up').to_numpy()
        np.testing.assert_array_equal(prior, np.tile(expected_prior, (fold_oof.height, 1)))
    opened = open_lockbox(rows, tmp_path, 'barrier', num_threads=2,
                          lockbox_ledger=tmp_path / 'ledger.jsonl')
    assert opened['results'][0]['barrier_pct'] == report['barrier_pct']
    held = pl.read_parquet(root / 'lockbox.parquet')
    assert held.height > 0
    held_metrics = opened['results'][0]['lockbox_results']
    assert held_metrics['metrics']['classifier']['directional_n'] == held.filter(pl.col('direction') != 0).height
    assert sum(group['classifier']['directional_n'] for group in held_metrics['regimes']['week_part'].values()) == held_metrics['metrics']['classifier']['directional_n']
    assert_frame_equal(held.select(expected.columns), held.select('tick_ns').join(labels, on='tick_ns'))


def test_vectorized_exit_returns_match_python_integer_division_bit_for_bit():
    rng = np.random.default_rng(1718)
    starts = rng.integers(1, 2**63 - 1, size=1024)
    exits = rng.integers(1, 2**63 - 1, size=1024)
    # Adjacent integers and binary exponent boundaries exercise cancellation,
    # scaling, and ties-to-even rounding at both ends of the Int64 price range.
    starts = np.r_[starts, [1, 2**53 + 1, 2**63 - 2, 2**63 - 1, 100, 100]]
    exits = np.r_[exits, [2**63 - 1, 2**53 + 2, 2**63 - 1, 1, 100, 101]]
    expected = np.array([exact_return_pct(a, b) for a, b in zip(starts, exits)])
    np.testing.assert_array_equal(_exact_exit_returns(pl.Series(starts), exits), expected)


def test_direction_metrics_score_barrier_class_but_keep_actual_exit_returns():
    from research.evaluation import probability_scores
    from research.train import forecast_metrics
    actual = np.array([.1, -.2, .5, -.6])
    directions = np.array([0, 0, 1, -1])
    probs = np.array([[.1, .8, .1], [.1, .8, .1], [.1, .1, .8], [.8, .1, .1]])
    prediction = np.array([0., 0., .5, -.6])
    result = forecast_metrics(actual, {'classifier': prediction}, {'classifier': probs}, directions)['classifier']
    assert result['directional_n'] == 2 and result['directional_accuracy'] == 1.
    assert result['mae'] == pytest.approx(.075)
    assert {k: result[k] for k in probability_scores(directions, probs)} == probability_scores(directions, probs)


def test_barrier_selective_and_compare_scores_use_class_not_return_sign():
    from research.compare import _row_scores
    from research.evaluation import probability_losses, selective_columns, selective_metrics
    y = np.array([.1, -.2, .5, -.6])
    directions = np.array([0, 0, 1, -1])
    probs = np.array([[.05, .7, .25], [.25, .7, .05], [.05, .1, .85], [.85, .1, .05]])
    frame = pl.DataFrame({'tick_ns': np.arange(4) * NS, 'return_pct': y, 'direction': directions,
        'regressor': np.zeros(4), 'ridge': np.zeros(4), 'classifier_expected': probs @ [-.5, .01, .5],
        'p_up_minus_down': probs[:, 2] - probs[:, 0], 'reversal': [1., -1., -1., -1.],
        **{f'p_{name}': probs[:, i] for i, name in enumerate(('down', 'flat', 'up'))},
        **selective_columns(probs, {str(q): 0 for q in (50, 20, 10, 5)})})
    result = selective_metrics(frame, 'available')
    for level in result['levels'].values():
        assert level['rows'] == 4 and level['accuracy_rows'] == 2
        assert level['model_accuracy'] == 1. and level['reversal_accuracy'] == .5
        assert level['bootstrap']['blocks']['1']['model_vs_reversal_accuracy']['paired_rows'] == 2
    scores = _row_scores(frame, [], False)
    np.testing.assert_array_equal(scores['classifier_directional_accuracy'], [np.nan, np.nan, 1., 1.])
    losses = probability_losses(directions, probs)
    np.testing.assert_array_equal(scores['classifier_log_loss'], losses[0])
    np.testing.assert_array_equal(scores['classifier_brier_score'], losses[1])
    np.testing.assert_array_equal(scores['regressor_mse'], y ** 2)


def test_fixed_horizon_oof_report_and_compare_preserve_legacy_scoring(tmp_path, monkeypatch):
    import research.compare as comparison
    import research.train as module
    from research.evaluation import selective_metrics
    rows = training_rows()
    spec = dict(load_spec(), calendar=None, context_assets=['USDT'])
    labels = make_labels(rows, 'BTC_spot', 300, spec).drop_nulls('direction')
    np.testing.assert_array_equal(labels['direction'], np.sign(labels['return_pct']))
    kwargs = dict(spec=spec, rounds=2, num_threads=2, lockbox_from='1970-01-08',
        fold_days=1, min_train_days=2, max_folds=2, early_stopping=True,
        inner_valid_days=1, early_stopping_rounds=2)
    current = train_target(rows, 'BTC_spot', 300, tmp_path / 'current', 'legacy', **kwargs)
    current_forecast = module.forecast_metrics
    current_scores = comparison._row_scores
    expected_compare = comparison.compare_runs(current['model_dir'], current['model_dir'], draws=10)

    def legacy_forecast(actual, predictions, probabilities, directions=None):
        # Before barriers every class score inferred its label from the return.
        return current_forecast(actual, predictions, probabilities)

    def legacy_scores(frame, stops, shrunk):
        return current_scores(frame.with_columns(pl.col('return_pct').sign().alias('direction')), stops, shrunk)

    monkeypatch.setattr(module, 'forecast_metrics', legacy_forecast)
    monkeypatch.setattr(module, 'selective_metrics',
                        lambda frame, status: selective_metrics(frame.drop('direction'), status))
    monkeypatch.setattr(comparison, '_row_scores', legacy_scores)
    legacy = train_target(rows, 'BTC_spot', 300, tmp_path / 'old', 'legacy', **kwargs)
    a, b = Path(current['model_dir']), Path(legacy['model_dir'])
    assert (a / 'oof.parquet').read_bytes() == (b / 'oof.parquet').read_bytes()
    assert (a.parent / 'report_legacy.html').read_bytes() == (b.parent / 'report_legacy.html').read_bytes()
    assert {k: v for k, v in current.items() if k != 'model_dir'} == {
        k: v for k, v in legacy.items() if k != 'model_dir'}
    old_compare = comparison.compare_runs(current['model_dir'], current['model_dir'], draws=10)
    assert old_compare == expected_compare


@pytest.mark.parametrize('horizon', [3600, 14400])
def test_cli_accepts_spec_defined_barrier_horizons(tmp_path, horizon):
    from research.train import main
    from test_research import row, write_parquet
    path = write_parquet(tmp_path, [row(t, exchange='kraken', symbol='BTC/USD') for t in range(20)])
    result = main([str(path), '--output-root', str(tmp_path / 'out'), '--version', 'cli-barrier',
        '--target', 'BTC_spot', '--horizon-s', str(horizon), '--threads', '2',
        '--label-spec', 'research/targets_kraken_barrier.yaml', '--lockbox-from', '1970-01-03'])
    report = result['results'][0]
    assert report['horizon_s'] == horizon
    assert report['barrier_pct'] == load_spec('research/targets_kraken_barrier.yaml')['barriers_pct'][horizon]
    assert report['status'] == 'skipped' and report['reason'] == 'not enough labeled rows'
