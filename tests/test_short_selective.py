"""Short-horizon contracts and inner-calibrated selective direction evaluation."""

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from research.evaluation import (CONFIDENCE_LEVELS, DAY_NS, confidence_thresholds,
                                 paired_day_block_bootstrap, selective_columns, selective_metrics)
from research.fitting import inner_split
from research.labels import NS, load_spec, make_labels, momentum_return
from research.train import main, open_lockbox, train_target, walk_forward_folds
from test_evaluation import sparse_evaluation
from test_research import row, synthetic_path


@pytest.mark.parametrize('source,short,version', [
    ('targets.yaml', 'targets_short.yaml', 'v1-short'),
    ('targets_kraken.yaml', 'targets_kraken_short.yaml', 'v1-kraken-spot-short')])
def test_short_specs_preserve_original_settings(source, short, version):
    original = load_spec(Path('research') / source)
    expected = dict(original, version=version, horizons_s=[60, 120, 300, 900, 1800])
    assert load_spec(Path('research') / short) == expected
    assert original['horizons_s'] == [300, 900, 1800]


@pytest.mark.parametrize('horizon', [60, 120])
def test_short_exact_labels_excursions_momentum_and_missing_path(horizon):
    spec = load_spec('research/targets_short.yaml')
    rows = pl.DataFrame([row(t, mid=100 * NS + t * t * 1000) for t in range(2 * horizon + 1)])
    labels = make_labels(rows, 'BTC_spot', horizon, spec)
    start, end = rows['mid_e9'][0], rows['mid_e9'][horizon]
    assert labels['return_pct'][0] == 100 * (end - start) / start
    assert labels['direction'][0] == 1
    assert labels['label_end_ns'][0] == horizon * NS
    assert labels['adverse_long_bps'][0] == 0
    assert labels['adverse_short_bps'][0] == 10000 * (end - start) / start
    momentum = momentum_return(rows, 'BTC_spot', horizon, spec)
    assert momentum[f'momentum_return_{horizon}s'][horizon] == labels['return_pct'][0]
    missing = rows.filter(pl.col('tick_ns') != NS)
    broken = make_labels(missing, 'BTC_spot', horizon, spec)
    assert broken['return_pct'][0] == labels['return_pct'][0]
    assert broken['adverse_short_bps'][0] is None
    missing_endpoint = rows.filter(pl.col('tick_ns') != horizon * NS)
    assert make_labels(missing_endpoint, 'BTC_spot', horizon, spec)['return_pct'][0] is None
    past = momentum_return(missing_endpoint, 'BTC_spot', horizon, spec)
    assert past.filter(pl.col('tick_ns') == 2 * horizon * NS)[f'momentum_return_{horizon}s'][0] is None


@pytest.mark.parametrize('horizon', [60, 120])
def test_short_outer_and_inner_purge_availability(horizon):
    ticks = np.arange(24 * 40, dtype=np.int64) * 3600 * NS
    ends = ticks + horizon * NS
    emitted = ends + NS
    emitted[0] = ticks[-1] + DAY_NS
    for train, test in walk_forward_folds(ticks, horizon, label_available_ns=emitted):
        start = ticks[test[0]]
        assert np.all(ticks[train] + 2 * horizon * NS < start)
        assert np.all(emitted[train] + horizon * NS < start)
        inner_train, valid = inner_split(ticks[train], ends[train], emitted[train], horizon, 3)
        assert np.all(emitted[train][inner_train] + horizon * NS < ticks[train][valid[0]])
    with pytest.raises(ValueError, match='purge_s'):
        walk_forward_folds(ticks, horizon, purge_s=horizon - 1)


@pytest.mark.parametrize('horizon', [60, 120])
def test_short_cli_oof_baseline_and_saved_serving_model(tmp_path, synthetic_path, compact_folds, horizon):
    from inference.service import load_models
    from research.dataset import read_features
    result = main([str(synthetic_path), '--output-root', str(tmp_path / 'output'),
        '--target', 'BTC_spot', '--horizon-s', str(horizon), '--rounds', '2', '--threads', '2',
        '--version', 'short', '--label-spec', 'research/targets_short.yaml', '--lockbox-from', '1970-01-03'])
    report = result['results'][0]
    assert report['status'] == 'trained'
    directory = Path(report['model_dir'])
    oof = pl.read_parquet(directory / 'oof.parquet')
    exact = momentum_return(read_features([synthetic_path]), 'BTC_spot', horizon, report['label_spec'])
    joined = oof.join(exact, on='tick_ns')
    np.testing.assert_array_equal(joined['momentum'], joined[f'momentum_return_{horizon}s'])
    np.testing.assert_array_equal(oof['reversal'], -oof['momentum'].to_numpy())
    assert f'momentum_return_{horizon}s' not in report['features']
    assert report['stop_labeled_rows']['long'] == report['labeled_rows']
    assert report['selective']['status'] == 'unavailable'
    assert report['confidence_thresholds'] is None
    model = load_models(tmp_path / 'output', 'short', horizons=[horizon])[0]
    assert model.metadata['horizon_s'] == horizon and len(model.stops) == 4


def test_selective_quantiles_ties_common_mask_and_empty_days():
    inner = np.array([[.1, .2, .7], [.4, .2, .4], [.6, .2, .2], [.05, .05, .9]])
    thresholds = confidence_thresholds(inner)
    for q in CONFIDENCE_LEVELS:
        assert thresholds[str(q)] == pytest.approx(np.quantile([.6, 0., .4, .85], 1 - q / 100))
    probs = np.array([[.05, .05, .9], [.4, .2, .4], [.9, .05, .05], [.05, .05, .9], [.8, .1, .1]])
    ticks = np.array([0, 1, 2, 3, 4]) * DAY_NS
    columns = selective_columns(probs, thresholds)
    assert not columns['selected_q50'][1]  # p_up == p_down never predicts.
    frame = pl.DataFrame({'tick_ns': ticks, 'return_pct': [1., -1., -1., 0., -1.],
        'p_up_minus_down': probs[:, 2] - probs[:, 0], 'reversal': [-1., -1., -1., -1., np.nan], **columns})
    report = selective_metrics(frame, 'available')
    level = report['levels']['50']
    assert level['rows'] == 4 and level['days'] == 4 and level['coverage'] == .8
    assert level['accuracy_rows'] == 2 and level['model_accuracy'] == 1. and level['reversal_accuracy'] == .5
    differences = np.array([1., np.nan, 0., np.nan, np.nan])
    assert level['bootstrap'] == paired_day_block_bootstrap(ticks, {'model_vs_reversal_accuracy': differences})
    assert level['bootstrap']['test_days'] == 5
    empty = frame.with_columns(pl.lit(False).alias(f'selected_q{q}') for q in CONFIDENCE_LEVELS)
    for level in selective_metrics(empty, 'available')['levels'].values():
        assert level['rows'] == 0 and level['model_accuracy'] is None
        assert all(b['model_vs_reversal_accuracy']['valid_draws'] == 0 for b in level['bootstrap']['blocks'].values())
    all_ties = selective_columns(np.full((2, 3), 1 / 3), {str(q): 0. for q in CONFIDENCE_LEVELS})
    assert not all_ties['selected_q50'].any()


@pytest.mark.parametrize('horizon', [60, 120])
@pytest.mark.parametrize('early_stopping,valid_days,status', [(False, 4, 'unavailable'),
    (True, 100, 'unavailable'), (True, 4, 'available')])
def test_inner_thresholds_oof_and_frozen_lockbox(tmp_path, sparse_evaluation, monkeypatch,
                                               early_stopping, valid_days, status, horizon):
    import research.train as module
    rows, spec, _ = sparse_evaluation
    # Supply exact trailing endpoints while retaining a small multi-day fixture.
    copies = [rows.with_columns((pl.col('tick_ns') - seconds * NS).alias('tick_ns'),
                               (pl.col('available_ns') - seconds * NS).alias('available_ns'),
                               (pl.col('mid_e9') - seconds * 1000).alias('mid_e9'))
              for seconds in (0, 60, 120, 300)]
    rows = pl.concat(copies).sort('tick_ns')
    spec = dict(spec, horizons_s=[60, 120, 300, 900, 1800])
    captured = []
    original = module.fit_booster

    def capture(*args, **kwargs):
        result = original(*args, **kwargs)
        if args[3]['objective'] == 'multiclass':
            captured.append(confidence_thresholds(result[2][1]) if result[2] is not None else None)
        return result

    monkeypatch.setattr(module, 'fit_booster', capture)
    report = train_target(rows, 'BTC_spot', horizon, tmp_path, 'selective', spec=spec, rounds=4,
        num_threads=2, early_stopping=early_stopping, inner_valid_days=valid_days, early_stopping_rounds=2)
    assert [fold['confidence_thresholds'] for fold in report['folds']] == captured[:-1]
    assert report['confidence_thresholds'] == captured[-1]
    assert report['selective']['status'] == status
    directory = Path(report['model_dir'])
    oof = pl.read_parquet(directory / 'oof.parquet')
    expected = pl.concat([part.with_columns(*[
        pl.Series(name, value) for name, value in selective_columns(
            part.select('p_down', 'p_flat', 'p_up').to_numpy(), fold['confidence_thresholds']).items()])
        for fold, part in zip(report['folds'], oof.partition_by('fold', maintain_order=True))])
    assert_frame_equal(oof, expected)

    def forbidden(*args, **kwargs):
        pytest.fail('lockbox must use frozen models and thresholds')

    monkeypatch.setattr(module, 'fit_models', forbidden)
    monkeypatch.setattr(module, 'fit_booster', forbidden)
    opened = open_lockbox(rows, tmp_path, 'selective', num_threads=2)['results'][0]
    lockbox = pl.read_parquet(directory / 'lockbox.parquet')
    assert opened['confidence_thresholds'] == report['confidence_thresholds']
    assert opened['lockbox_results']['selective']['status'] == status
    assert opened['lockbox_results']['selective'] == selective_metrics(lockbox, status)
    columns = selective_columns(lockbox.select('p_down', 'p_flat', 'p_up').to_numpy(), captured[-1])
    for name, values in columns.items():
        np.testing.assert_array_equal(lockbox[name], values)
    exact = momentum_return(rows.filter(pl.col('tick_ns') >= report['date_split']['lockbox_start_ns']),
                            'BTC_spot', horizon, spec)
    joined = lockbox.join(exact, on='tick_ns')
    np.testing.assert_array_equal(joined['momentum'], joined[f'momentum_return_{horizon}s'])
    np.testing.assert_array_equal(joined['reversal'], -joined['momentum'].to_numpy())
    html = (directory.parent / 'report_selective.html').read_text()
    assert html.count('Selective direction evaluation') == 2
    assert ('Model − reversal' in html) == (status == 'available')
    json.loads((directory / 'metadata.json').read_text())  # Finite JSON, including unavailable paths.


def test_outer_labels_and_predictions_cannot_change_fold_thresholds(tmp_path, sparse_evaluation, monkeypatch):
    import research.train as module
    rows, spec, _ = sparse_evaluation
    boundary = int(rows['tick_ns'][0]) + 20 * DAY_NS
    original_labels, original_fit = module.make_labels, module.fit_models
    changed = False

    def labels(*args):
        frame = original_labels(*args)
        if changed:
            frame = frame.with_columns(
                pl.when(pl.col('tick_ns') >= boundary).then(-pl.col(name)).otherwise(pl.col(name)).alias(name)
                for name in ('return_pct', 'direction'))
        return frame

    def fit(*args, **kwargs):
        result = original_fit(*args, **kwargs)
        if changed:
            classifier = result[1]
            native = classifier.predict

            def predict(x, **options):
                if options.get('pred_contrib'):
                    return native(x, **options)
                return np.tile([.1, .1, .8], (len(x), 1))

            classifier.predict = predict
        return result

    def outer(ticks, *args, **kwargs):
        return [(np.flatnonzero(ticks < boundary), np.flatnonzero(ticks >= boundary))]

    monkeypatch.setattr(module, 'make_labels', labels)
    monkeypatch.setattr(module, 'fit_models', fit)
    monkeypatch.setattr(module, 'walk_forward_folds', outer)
    options = dict(spec=spec, rounds=4, num_threads=2, early_stopping=True,
                   inner_valid_days=4, early_stopping_rounds=2)
    before = train_target(rows, 'BTC_spot', 300, tmp_path, 'before', **options)
    changed = True
    after = train_target(rows, 'BTC_spot', 300, tmp_path, 'after', **options)
    assert before['folds'][0]['confidence_thresholds'] is not None
    assert before['folds'][0]['confidence_thresholds'] == after['folds'][0]['confidence_thresholds']
    left, right = [pl.read_parquet(Path(r['model_dir']) / 'oof.parquet') for r in (before, after)]
    assert not np.array_equal(left['confidence'], right['confidence'])
    np.testing.assert_array_equal(left['return_pct'], -right['return_pct'].to_numpy())


@pytest.mark.parametrize('horizon', [300, 900, 1800])
def test_long_horizons_byte_identical_with_short_spec(tmp_path, synthetic_path, compact_folds, horizon):
    from research.dataset import read_features
    rows = read_features([synthetic_path])
    original = train_target(rows, 'BTC_spot', horizon, tmp_path, 'original',
                            spec=load_spec(), rounds=2, num_threads=2, lockbox_from='1970-01-03')
    extended = train_target(rows, 'BTC_spot', horizon, tmp_path, 'extended',
                            spec=load_spec('research/targets_short.yaml'), rounds=2,
                            num_threads=2, lockbox_from='1970-01-03')
    for name in ('metrics', 'bootstrap', 'stop_metrics', 'features'):
        assert original[name] == extended[name]
    for name in ('regressor.txt', 'classifier.txt', 'oof.parquet'):
        assert (Path(original['model_dir']) / name).read_bytes() == (Path(extended['model_dir']) / name).read_bytes()
