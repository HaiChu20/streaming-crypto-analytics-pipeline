"""Inner-only model selection, chunked ridge and explicit feature ablations."""

import json
import re

import lightgbm as lgb
import numpy as np
import polars as pl
import pytest

from research.dataset import FEATURE_GROUPS, select_features
from research.evaluation import DAY_NS
from research.fitting import fit_booster, inner_split, parse_params, shrinkage_coefficient
from research.labels import NS
from research.linear import fit_ridge, ridge_predict
from research.risk import PARAMS as STOP_PARAMS, STOP_NAMES, fit_stop_models
from research.train import PARAMS, fit_models, main, metrics, train_target
from test_evaluation import sparse_evaluation
from test_research import row, synthetic_path, write_parquet


def test_params_inline_file_and_dictionary(tmp_path):
    values = {'num_leaves': 7, 'learning_rate': .02, 'deterministic': True, 'monotone_constraints': [0, 1]}
    path = tmp_path / 'params.json'
    path.write_text(json.dumps(values))
    assert parse_params(str(path)) == parse_params(json.dumps(values)) == parse_params(values) == values
    assert parse_params(None) == {}


@pytest.mark.parametrize('value', ['[]', 'null', 'true', '12', '"text"', '{',
                                   '{"num_leaves":null}', '{"x":{"nested":1}}',
                                   '{"learning_rate":NaN}', '{"num_iterations":1}', '{"objective":"binary"}'])
def test_params_reject_invalid_input(value):
    with pytest.raises(ValueError, match='JSON object|Unsupported|controlled'):
        parse_params(value)


def test_missing_params_file_is_clear(tmp_path):
    with pytest.raises(ValueError, match='readable .json file'):
        parse_params(str(tmp_path / 'missing.json'))


def test_default_fits_exactly_match_pre_selection_lightgbm_calls():
    rng = np.random.default_rng(12)
    x = rng.normal(size=(150, 3)).astype(np.float64)
    y = np.sin(x[:, 0])
    y[::9] = 0.
    directions = np.sign(y).astype(np.int32)
    features = ['a', 'b', 'c']
    reg, cls, _ = fit_models(x, y, directions, features, 4, num_threads=2)
    for actual, objective, label in [(reg, {'objective': 'regression'}, y),
                                    (cls, {'objective': 'multiclass', 'num_class': 3}, directions + 1)]:
        expected = lgb.train(dict(PARAMS, num_threads=2, **objective),
            lgb.Dataset(x, label=label, feature_name=features), num_boost_round=4)
        np.testing.assert_array_equal(actual.predict(x, num_threads=2), expected.predict(x, num_threads=2))
    adverse = {'long': np.abs(y), 'short': 2 * np.abs(y)}
    adverse['long'][::10] = np.nan
    stops = fit_stop_models(x, adverse, features, 4, num_threads=2)
    for name in STOP_NAMES:
        q, side = int(name[-2:]) / 100, name.split('_')[1]
        keep = np.isfinite(adverse[side])
        expected = lgb.train(dict(STOP_PARAMS, num_threads=2, objective='quantile', alpha=q),
            lgb.Dataset(x[keep], label=adverse[side][keep], feature_name=features), num_boost_round=4)
        np.testing.assert_array_equal(stops[name].predict(x, num_threads=2), expected.predict(x, num_threads=2))


def test_inner_split_uses_whole_utc_days_and_strict_endpoint_emission_purge():
    ticks = np.arange(10 * 24, dtype=np.int64) * 3600 * NS
    ends = ticks + 900 * NS
    available = ends + 123_456_789
    emitted = ticks + NS
    start = 7 * DAY_NS
    available[2] = start
    emitted[3] = start
    train, valid = inner_split(ticks, ends, available, 900, 3, feature_available_ns=emitted)
    expected = np.flatnonzero((ticks + 1800 * NS < start)
                               & (np.maximum.reduce([ends, available, emitted]) + 900 * NS < start))
    np.testing.assert_array_equal(train, expected)
    np.testing.assert_array_equal(valid, np.flatnonzero(ticks >= start))
    assert 2 not in train and 3 not in train
    assert np.all(ends[train] < start) and np.all(available[train] < start)
    assert ticks[valid[0]] - ticks[train[-1]] > 900 * NS
    assert inner_split(ticks[:10], ends[:10], available[:10], 900) is None


def test_inner_split_requires_enough_observed_days_despite_calendar_gaps():
    ticks = np.array([0, 1, 10]) * DAY_NS
    assert inner_split(ticks, ticks + 900 * NS, ticks + 901 * NS, 900, valid_days=3) is None
    train, valid = inner_split(ticks, ticks + 900 * NS, ticks + 901 * NS, 900, valid_days=2)
    np.testing.assert_array_equal(train, [0])
    np.testing.assert_array_equal(valid, [1, 2])


@pytest.mark.parametrize('objective,metric', [('regression', 'l2'), ('multiclass', 'multi_logloss'), ('quantile', 'quantile')])
def test_inner_best_iteration_refits_on_full_outer_training_only(monkeypatch, objective, metric):
    import research.fitting as module
    rng = np.random.default_rng(92)
    x = rng.normal(size=(120, 2)).astype(np.float64)
    y = x[:, 0] + rng.normal(size=120)
    options = dict(PARAMS, num_threads=2, objective=objective)
    if objective == 'multiclass':
        options['num_class'] = 3
        y = np.sign(y).astype(np.int32) + 1
    elif objective == 'quantile':
        options['alpha'] = .8
        y = np.abs(y)
    calls = []
    original = module.lgb.train

    def record(params, dataset, **kwargs):
        recorded = {k: v for k, v in kwargs.items() if k != 'valid_sets'}
        if 'valid_sets' in kwargs:
            recorded['valid_rows'] = len(kwargs['valid_sets'][0].data)
        calls.append((dict(params), len(dataset.data), recorded))
        return original(params, dataset, **kwargs)

    monkeypatch.setattr(module.lgb, 'train', record)
    model, selected, validation = fit_booster(x, y, ['a', 'b'], options, 9,
        inner=(np.arange(60), np.arange(80, 120)), early_stopping=True, patience=2, metric=metric)
    assert len(calls) == 2
    assert calls[0][0]['metric'] == metric and calls[0][1] == 60
    assert calls[0][2]['valid_rows'] == 40
    assert calls[1][1] == 120 and 'valid_rows' not in calls[1][2]
    assert calls[1][2]['num_boost_round'] == selected['rounds']
    assert selected['status'] == 'selected' and 1 <= selected['rounds'] <= 9
    assert len(validation[0]) == 40


@pytest.mark.parametrize('actual,predicted,expected', [([1., 2.], [2., 4.], .5),
    ([-1., -2.], [1., 2.], 0.), ([2., 4.], [1., 2.], 1.), ([1.], [0.], 0.),
    ([1., np.nan], [2., 4.], .5)])
def test_regressor_shrinkage_formula_and_clipping(actual, predicted, expected):
    assert shrinkage_coefficient(actual, predicted) == expected


def test_ridge_matches_direct_augmented_lstsq_and_uses_train_only_preprocessing():
    x = np.array([[1., 2., 5., np.nan], [2., np.nan, 5., np.nan], [3., 6., 5., np.nan],
                  [4., 8., 5., np.nan], [5., 10., 5., np.nan], [6., 12., 5., np.nan],
                  [999., np.nan, 7., 1.], [np.nan, -999., 8., 2.]])
    y = np.array([1., 3., 2., 5., 4., 7., 100., -100.])
    train = np.arange(6)
    model, info = fit_ridge(x, y, train)
    medians = np.array([3.5, 8., 5., 0.])
    np.testing.assert_array_equal(model['medians'], medians)
    np.testing.assert_array_equal(model['missing_columns'], [1, 3])
    imputed = np.where(np.isfinite(x), x, medians)
    design = np.column_stack([imputed, ~np.isfinite(x[:, [1, 3]])])
    mean, scale = design[train].mean(axis=0), design[train].std(axis=0)
    active = scale > 0
    z = (design[:, active] - mean[active]) / scale[active]
    expected = np.linalg.lstsq(np.vstack([z[train], np.sqrt(info['alpha']) * np.eye(active.sum())]),
                             np.r_[y[train] - y[train].mean(), np.zeros(active.sum())], rcond=None)[0]
    np.testing.assert_allclose(model['coef'], expected, rtol=1e-13, atol=1e-13)
    np.testing.assert_allclose(ridge_predict(model, x), z @ expected + y[train].mean(), rtol=1e-13, atol=1e-13)
    changed = x.copy()
    changed[6:] = -1e10
    changed_model, _ = fit_ridge(changed, y, train)
    for key in model:
        np.testing.assert_array_equal(model[key], changed_model[key])
    constant, _ = fit_ridge(np.ones((8, 2)), y, train)
    np.testing.assert_array_equal(ridge_predict(constant, np.ones((8, 2))), np.full(8, y[train].mean()))


def test_ridge_transforms_only_row_chunks_and_selects_inner_alpha(monkeypatch):
    import research.linear as module
    rng = np.random.default_rng(8)
    x = rng.normal(size=(40, 4))
    x[::7, 1] = np.nan
    y = x[:, 0] + rng.normal(size=40)
    calls = []
    original = module.ridge_transform

    def bounded(model, data, indices):
        calls.append(len(indices))
        assert len(indices) <= 3
        return original(model, data, indices)

    monkeypatch.setattr(module, 'CHUNK_ROWS', 3)
    monkeypatch.setattr(module, 'ridge_transform', bounded)
    model, info = fit_ridge(x, y, np.arange(30), (np.arange(20), np.arange(20, 30)))
    assert info['status'] == 'selected' and info['factor'] in info['grid_factors']
    assert info['factor'] == info['grid_factors'][int(np.argmin(info['inner_mse']))]
    assert info['alpha'] == info['factor'] * 30 / 1000
    assert np.isfinite(ridge_predict(model, x)).all() and calls


NAMES = ['cal_hour_sin', 'binance_spot_liq_sell_base_60s', 'kraken_spot_l3_add_bid_10s',
         'binance_perp_flow_900s', 'binance_spot_ofi10_10s', 'kraken_spot_trades_60s',
         'binance_spot_vol_300s', 'binance_spot_volume_base', 'binance_spot_ret_1s',
         'momentum_return_900s', 'momentum_return_300s', 'binance_spot_rv_300s',
         'binance_perp_range_bps_60s', 'binance_spot_dvol', 'binance_spot_dvol_chg_900s',
         'binance_spot_spread_bps', 'kraken_spot_depth_ask_25bps', 'binance_perp_funding_rate',
         'binance_perp_open_interest', 'binance_perp_oi_chg_pct_300s', 'binance_perp_mark_index_bps',
         'cross_cross_lead_kp_ks_gap_dev_60s', 'cross_cross_basis_bps_binance',
         'binance_spot_book_valid', 'binance_spot_cov_1800s',
         'ctx_usdt_kraken_spot_premium_bps']


def test_feature_groups_match_families_and_exclude_unrelated_suffixes():
    for name, pattern in FEATURE_GROUPS.items():
        assert any(re.search(pattern, f) for f in NAMES), name
    assert re.search(FEATURE_GROUPS['returns'], 'momentum_return_900s')
    assert not re.search(FEATURE_GROUPS['flow'], 'binance_spot_dvol_chg_900s')
    assert not re.search(FEATURE_GROUPS['flow'], 'binance_spot_ret_60s')
    assert not re.search(FEATURE_GROUPS['quality'], 'binance_spot_cov_60s_extra')
    selected, info = select_features(NAMES, ['calendar', 'returns'], [r'_spread_bps$'])
    assert selected == [f for f in NAMES if f not in info['excluded']]
    assert 'momentum_return_900s' in info['excluded'] and info['groups'] == ['calendar', 'returns']


def test_lead_group_excludes_all_pairs_and_cross_group_still_matches():
    lead = [f'cross_cross_lead_{pair}_{suffix}'
            for pair in ('kp_ks', 'bs_ks', 'bp_ks', 'kp_bs', 'kp_bp')
            for suffix in ('gap_dev_60s', 'gap_dev_300s', 'gap_chg_1s', 'gap_chg_5s')]
    other = ['cross_cross_basis_bps_binance', 'binance_spot_ret_1s']
    selected, info = select_features(lead + other, ['lead'])
    assert selected == other and info['excluded'] == sorted(lead)
    assert all(re.search(FEATURE_GROUPS['cross'], name) for name in lead)
    selected, info = select_features(lead + other, ['cross'])
    assert selected == ['binance_spot_ret_1s']


@pytest.mark.parametrize('groups,patterns', [(['unknown'], []), ([], ['absent']), ([], ['[']), ([], ['.*'])])
def test_exclusion_errors(groups, patterns):
    with pytest.raises(ValueError, match='Unknown|nothing|Invalid|every'):
        select_features(NAMES, groups, patterns)


def test_early_stopping_and_shrinkage_choices_ignore_outer_test_labels(tmp_path, sparse_evaluation, monkeypatch):
    import research.train as module
    rows, spec, _ = sparse_evaluation
    boundary = int(rows['tick_ns'][0]) + 20 * DAY_NS
    original_labels = module.make_labels
    permute = False

    def labels(*args):
        result = original_labels(*args)
        if permute:
            test = np.flatnonzero(result['tick_ns'].to_numpy() >= boundary)
            order = np.random.default_rng(19).permutation(test)
            for name in ('return_pct', 'direction', 'adverse_long_bps', 'adverse_short_bps'):
                values = result[name].to_numpy().copy()
                values[test] = values[order]
                result = result.with_columns(pl.Series(name, values))
        return result

    def outer(ticks, *args, **kwargs):
        return [(np.flatnonzero(ticks < boundary), np.flatnonzero(ticks >= boundary))]

    monkeypatch.setattr(module, 'make_labels', labels)
    monkeypatch.setattr(module, 'walk_forward_folds', outer)
    options = dict(spec=spec, rounds=12, num_threads=2, early_stopping=True, inner_valid_days=4,
                   early_stopping_rounds=3, params={'num_leaves': 4, 'learning_rate': .1},
                   risk_params={'num_leaves': 3})
    before = train_target(rows, 'BTC_spot', 300, tmp_path, 'before', **options)
    permute = True
    after = train_target(rows, 'BTC_spot', 300, tmp_path, 'after', **options)
    left, right = before['folds'][0], after['folds'][0]
    assert left['chosen_rounds'] == right['chosen_rounds']
    assert left['regressor_shrinkage'] == right['regressor_shrinkage']
    assert left['ridge'] == right['ridge']
    assert before['model_params'] == after['model_params']
    assert all(1 <= selected['rounds'] <= 12 and selected['status'] == 'selected'
               for selected in left['chosen_rounds'].values())
    frames = [pl.read_parquet(tmp_path / 'models/BTC_spot/300' / version / 'oof.parquet') for version in ('before', 'after')]
    for column in ('regressor', 'classifier_expected', 'ridge', 'regressor_shrunk', *STOP_NAMES):
        np.testing.assert_array_equal(frames[0][column].to_numpy(), frames[1][column].to_numpy())
    np.testing.assert_array_equal(frames[0]['regressor_shrunk'].to_numpy(),
        frames[0]['regressor'].to_numpy() * left['regressor_shrinkage']['coefficient'])
    assert frames[0].schema['ridge'] == pl.Float64
    for block in before['bootstrap']['blocks'].values():
        assert 'ridge_vs_zero_mse' in block and 'regressor_shrunk_vs_zero_mse' in block
        y = frames[0]['return_pct'].to_numpy()
        for name in ('ridge', 'regressor_shrunk'):
            pred = frames[0][name].to_numpy()
            assert before['metrics'][name] == metrics(y, pred)
            assert block[name + '_vs_zero_mse']['mean_difference'] == pytest.approx(np.mean((y - pred) ** 2 - y**2))
    assert before['model_params']['regressor']['num_leaves'] == 4
    assert before['model_params']['stops']['stop_long_q80']['num_leaves'] == 3


def test_cli_params_ablation_fallback_metadata_and_ledger(tmp_path, synthetic_path, compact_folds):
    risk_path = tmp_path / 'risk.json'
    risk_path.write_text('{"learning_rate":0.03}')
    result = main([str(synthetic_path), '--output-root', str(tmp_path / 'output'), '--target', 'BTC_spot',
        '--horizon-s', '300', '--rounds', '3', '--threads', '2', '--version', 'config',
        '--lockbox-from', '1970-01-03', '--params', '{"num_leaves":4}', '--risk-params', str(risk_path),
        '--early-stopping', '--exclude-group', 'calendar', '--exclude-features', '^binance_spot_spread_bps$'])
    report = result['results'][0]
    assert report['status'] == 'trained'
    assert all(chosen['status'] == 'fallback' and chosen['rounds'] == 3 for chosen in report['chosen_rounds'].values())
    assert all(chosen['status'] == 'fallback' and chosen['rounds'] == 3
               for fold in report['folds'] for chosen in fold['chosen_rounds'].values())
    assert report['regressor_shrinkage']['coefficient'] == 1.
    assert not any(f.startswith('cal_') or f == 'binance_spot_spread_bps' for f in report['features'])
    assert report['feature_count'] == len(report['features'])
    assert report['model_params']['direction']['num_leaves'] == 4
    assert report['model_params']['risk']['learning_rate'] == .03
    directory = tmp_path / 'output/models/BTC_spot/300/config'
    assert json.loads((directory / 'metadata.json').read_text())['model_params'] == report['model_params']
    entry = json.loads((tmp_path / 'output/experiments.jsonl').read_text())
    assert entry['params'] == entry['results'][0]['params'] == report['model_params']
    assert entry['results'][0]['exclusions'] == report['exclusions']
    assert entry['results'][0]['feature_sha256'] == report['feature_sha256']
    html = (directory.parent / 'report_config.html').read_text()
    assert 'Chosen rounds' in html and 'ridge' in html and 'regressor_shrunk' in html and 'Excluded names' in html


def test_list_feature_groups_reads_only_one_day_without_fitting(tmp_path, monkeypatch, capsys):
    import research.train as module
    from datetime import datetime, timezone
    for day in range(1, 5):
        second = int(datetime(2026, 1, day, tzinfo=timezone.utc).timestamp())
        write_parquet(tmp_path / 'features' / f'date=2026-01-{day:02d}', [row(second), row(second + 10)])
    original = module.read_features
    reads = []

    def read(paths, *args, **kwargs):
        reads.append(paths)
        assert len(paths) == 1 and paths[0].parent.name == 'date=2026-01-01'
        return original(paths, *args, **kwargs)

    def forbidden(*args, **kwargs):
        pytest.fail('Feature listing must not fit any model')

    monkeypatch.setattr(module, 'read_features', read)
    monkeypatch.setattr(module, 'train_target', forbidden)
    result = main([str(tmp_path), '--target', 'BTC_spot', '--list-feature-groups', '--version', 'list'])
    assert result['status'] == 'listed' and reads
    assert result['feature_groups']['calendar'] > 0 and result['feature_groups']['book'] > 0
    output = capsys.readouterr().out
    assert all(f'{name}:' in output for name in FEATURE_GROUPS)
    assert 'Groups may overlap' in output and 'not by summing group results' in output
    assert not (tmp_path / 'models').exists()
