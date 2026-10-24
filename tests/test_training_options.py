"""Development-only feature pruning and dense training with paired evaluation ticks."""
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from research.compare import compare_runs
from research.dataset import build_matrix
from research.evaluation import DAY_NS, feature_array
from research.labels import NS, load_spec
from research.train import (feature_schema, main, open_lockbox, resolve_eval_stride,
                            train_target, walk_forward_folds)
from test_research import synthetic_path


def training_rows(days=9, seconds=1801):
    ticks_s = np.concatenate([day * 86400 + np.arange(seconds) for day in range(days)])
    n = len(ticks_s)
    spot = pl.DataFrame({
        'tick_ns': ticks_s * NS, 'available_ns': ticks_s * NS + 250_000_000,
        'asset': ['BTC'] * n, 'exchange': ['binance'] * n, 'market': ['spot'] * n,
        'symbol': ['BTCUSDT'] * n, 'engine_version': ['fixture'] * n, 'replay_mode': ['ideal'] * n,
        'book_valid': np.ones(n, dtype=bool), 'warmup': np.zeros(n, dtype=bool),
        'mid_e9': 100 * NS + (np.sin(ticks_s / 170) * 100_000_000).astype(np.int64),
        'spread_bps': 2 + np.sin(ticks_s / 31), 'ret_60s': np.full(n, np.nan),
        'rv_300s': np.full(n, .5), 'bid_depth10': np.full(n, .1),
        'funding_rate': [None if t % 20 else .001 for t in ticks_s],
        # Changes only in the lockbox: it must still be removed during training.
        'basis_bps_binance': np.where(ticks_s >= 7 * 86400, ticks_s % 30, 0.).astype(float),
        'lead_kp_ks_gap_dev_60s': pl.Series([None] * n, dtype=pl.Float64),
        # Variation on unlabeled development tails still prevents pruning.
        'ask_depth10': np.where(ticks_s % 86400 >= seconds - 10, 2., 1.),
    })
    context = spot.filter(pl.col('tick_ns') % (5 * NS) == 0).with_columns(
        pl.lit('USDT').alias('asset'), pl.lit('USDTUSD').alias('symbol'),
        (pl.lit(NS) + pl.col('tick_ns') // NS % 100 * 1000).alias('mid_e9'))
    cross = context.with_columns(pl.lit('BTC').alias('asset'), pl.lit('BTC').alias('symbol'),
        pl.lit('cross').alias('exchange'), pl.lit('cross').alias('market'),
        (pl.col('spread_bps') * 2).alias('lead_kp_ks_gap_dev_60s'))
    return pl.concat([spot, context, cross]).sort('tick_ns', 'exchange', 'symbol')


@pytest.fixture
def options_rows():
    return training_rows(), dict(load_spec(), calendar=None, context_assets=['USDT'])


def train(rows, spec, root, version, **kwargs):
    return train_target(rows, 'BTC_spot', 300, root, version, spec=spec, rounds=6, num_threads=2,
                        lockbox_from='1970-01-08', fold_days=1, min_train_days=2, max_folds=3,
                        early_stopping=True, inner_valid_days=1, early_stopping_rounds=2, **kwargs)


@pytest.mark.parametrize('stride,evaluation', [(0, None), (5, 0), (5, -5), (10, 5), (5, 7),
                                              (5, 2.5), (5, True)])
def test_eval_stride_must_be_positive_integer_multiple(stride, evaluation, tmp_path):
    with pytest.raises(ValueError, match='positive multiple'):
        train_target(pl.DataFrame(), 'BTC_spot', 300, tmp_path, 'bad', stride_s=stride,
                     eval_stride_s=evaluation)


def test_eval_stride_default_and_cli_validation(tmp_path):
    assert resolve_eval_stride(5) == 5
    assert resolve_eval_stride(5, 10) == 10
    with pytest.raises(ValueError, match='positive multiple'):
        main([str(tmp_path), '--stride-s', '5', '--eval-stride-s', '7'])
    assert not (tmp_path / 'experiments.jsonl').exists()


@pytest.mark.parametrize('params', [{}, {'feature_fraction': .8, 'feature_pre_filter': True}])
@pytest.mark.parametrize('excluded', [False, True])
def test_constants_pruned_before_array_preserve_predictions_and_baselines(tmp_path, options_rows,
                                                                         monkeypatch, excluded, params):
    import research.train as module
    rows, spec = options_rows
    options = dict(exclude_groups=['returns'], exclude_features=['_spread_bps$']) if excluded else {}
    full = train(rows, spec, tmp_path, 'full', keep_constant_features=True,
                 params=params, **options)
    seen = []

    def array(frame, features):
        seen.append(frame.columns)
        assert 'binance_spot_bid_depth10' not in frame.columns
        assert 'binance_spot_rv_300s' not in frame.columns
        assert 'ctx_usdt_cross_cross_bid_depth10' not in frame.columns
        return feature_array(frame, features)

    monkeypatch.setattr(module, 'feature_array', array)
    lean = train(rows, spec, tmp_path, 'lean', params=params, **options)
    assert seen and lean['status'] == full['status'] == 'trained'
    constant = lean['exclusions']['constant']
    assert constant == sorted(constant) and lean['exclusions']['constant_count'] == len(constant)
    for name in ('binance_spot_bid_depth10', 'binance_spot_rv_300s', 'binance_spot_basis_bps_binance',
                 'binance_spot_lead_kp_ks_gap_dev_60s', 'ctx_usdt_cross_cross_bid_depth10'):
        assert name in constant and name not in lean['features']
    if not excluded:
        assert 'binance_spot_ret_60s' in constant  # all NaN
    assert 'binance_spot_funding_rate' in lean['features']  # one value plus nulls
    assert 'binance_spot_ask_depth10' in lean['features']  # includes unlabeled matrix rows
    assert full['exclusions']['constant'] == []
    assert lean['features'] == [name for name in full['features'] if name not in constant]
    assert lean['memory']['bytes'] < full['memory']['bytes']
    assert feature_schema(lean['features']) == {k: lean[k] for k in feature_schema(lean['features'])}
    a, b = [pl.read_parquet(Path(r['model_dir']) / 'oof.parquet') for r in (full, lean)]
    for name in ('regressor', 'p_down', 'p_flat', 'p_up', 'classifier_expected', 'momentum',
                 'stop_long_q80_volatility_scaled', 'volatility_tercile'):
        np.testing.assert_array_equal(a[name].to_numpy(), b[name].to_numpy())
    assert np.isfinite(b['ridge'].to_numpy()).all()
    np.testing.assert_allclose(a['ridge'].to_numpy(), b['ridge'].to_numpy(), rtol=1e-12, atol=1e-12)
    assert lean['risk_calibration'] == full['risk_calibration']
    assert lean['volatility_edges'] == full['volatility_edges'] == [.5, .5]
    metadata = json.loads((Path(lean['model_dir']) / 'metadata.json').read_text())
    assert metadata['features'] == lean['features'] and metadata['exclusions'] == lean['exclusions']


def test_dense_training_matches_comparison_rows_and_keeps_selection_dense(tmp_path, options_rows):
    rows, spec = options_rows
    a = train(rows, spec, tmp_path, 'stride10')
    dense = train(rows, spec, tmp_path, 'stride5', stride_s=5)
    b = train(rows, spec, tmp_path, 'eval10', stride_s=5, eval_stride_s=10)
    result = compare_runs(a['model_dir'], b['model_dir'], draws=10)
    assert result['n_rows'] == a['oof_rows'] == b['oof_rows'] < dense['oof_rows']
    assert a['date_split'] == b['date_split'] == dense['date_split']
    assert a['eval_stride_s'] == b['eval_stride_s'] == 10 and b['stride_s'] == 5
    assert b['chosen_rounds'] == dense['chosen_rounds']
    assert b['confidence_thresholds'] == dense['confidence_thresholds']
    assert b['final_train_rows'] == dense['final_train_rows'] > a['final_train_rows']
    keys = ['fold', 'train_rows', 'train_start_ns', 'train_end_ns', 'train_label_end_ns',
            'train_label_available_ns', 'train_feature_available_ns', 'chosen_rounds', 'confidence_thresholds']
    for fa, fb, fd in zip(a['folds'], b['folds'], dense['folds']):
        assert {k: fb[k] for k in keys} == {k: fd[k] for k in keys}
        assert fa['fold'] == fb['fold']
        assert fa['test_start_ns'] == fb['test_start_ns']
        assert fa['test_end_ns'] == fb['test_end_ns']
        assert fb['train_end_ns'] + 600 * NS < fb['test_start_ns']
        assert fb['metrics']['regressor']['n'] == fb['test_rows']
    oof = pl.read_parquet(Path(b['model_dir']) / 'oof.parquet')
    expected = pl.read_parquet(Path(dense['model_dir']) / 'oof.parquet').filter(pl.col('tick_ns') % (10 * NS) == 0)
    assert_frame_equal(oof, expected)
    assert b['metrics']['regressor']['n'] == oof.height
    assert b['bootstrap']['test_days'] == (oof['tick_ns'] // DAY_NS).n_unique()
    for report in (a, b):
        root = tmp_path / report['version']
        open_lockbox(rows, tmp_path, report['version'], num_threads=2,
                     lockbox_ledger=root / 'ledger.jsonl')
    # Comparing held-out files also exercises the saved reduced features and ridge.
    lock_a, lock_b = [Path(r['model_dir']) / 'lockbox.parquet' for r in (a, b)]
    compare_runs(lock_a, lock_b, draws=10)
    assert (pl.read_parquet(lock_b)['tick_ns'] % (10 * NS)).sum() == 0
    from inference.service import load_models, vector_for
    model = load_models(tmp_path, 'eval10', horizons=[300])[0]
    matrix, _ = build_matrix(rows, 'BTC_spot', stride_s=5, spec=spec)
    tick = 900 * NS
    current = rows.filter(pl.col('tick_ns') == tick).to_dicts()
    history = rows.filter((pl.col('tick_ns') < tick) & (pl.col('symbol') == 'BTCUSDT')).to_dicts()
    np.testing.assert_array_equal(vector_for(current, history, model.metadata),
                                  feature_array(matrix.filter(pl.col('tick_ns') == tick), b['features']))


def test_cli_options_in_experiment_and_html(tmp_path, synthetic_path, compact_folds):
    result = main([str(synthetic_path), '--output-root', str(tmp_path / 'out'), '--version', 'options',
                   '--target', 'BTC_spot', '--horizon-s', '300', '--rounds', '1', '--threads', '2',
                   '--lockbox-from', '1970-01-03', '--stride-s', '5', '--eval-stride-s', '10',
                   '--keep-constant-features'])
    report = result['results'][0]
    assert report['status'] == 'trained' and report['exclusions']['constant'] == []
    entry = json.loads((tmp_path / 'out/experiments.jsonl').read_text())
    assert entry['stride_s'] == entry['results'][0]['stride_s'] == 5
    assert entry['eval_stride_s'] == entry['results'][0]['eval_stride_s'] == report['eval_stride_s'] == 10
    html = (tmp_path / 'out/report_options.html').read_text()
    assert 'stride_s: 5; eval_stride_s: 10 seconds' in html


def test_subsampling_without_prefilter_can_change_predictions(tmp_path, options_rows):
    rows, spec = options_rows
    full = train(rows, spec, tmp_path, 'sample-full', keep_constant_features=True,
                 params={'feature_fraction': .8})
    lean = train(rows, spec, tmp_path, 'sample-lean', params={'feature_fraction': .8})
    a, b = [pl.read_parquet(Path(r['model_dir']) / 'oof.parquet') for r in (full, lean)]
    differences = {name: float(np.max(np.abs(a[name].to_numpy() - b[name].to_numpy())))
                   for name in ('regressor', 'p_down', 'p_flat', 'p_up', 'ridge')}
    print('feature_fraction=.8, feature_pre_filter=False max_abs:', differences)
    assert differences['regressor'] > 0
    assert max(differences[name] for name in ('p_down', 'p_flat', 'p_up')) > 0
    assert differences['ridge'] < 1e-12
    assert all(np.isfinite(b[name].to_numpy()).all() for name in differences)
