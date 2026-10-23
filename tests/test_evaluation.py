"""P23 safety boundaries and diagnostics; all inputs are generated fixtures."""
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest

from research.evaluation import (DAY_NS, date_split, day_block_permutation, feature_array,
                                 class_prior, date_intervals_overlap, feature_groups, leak_check_metrics,
                                 memory_estimate, model_importance, moving_day_blocks,
                                 paired_day_block_bootstrap, probability_losses, probability_scores)
from research.labels import NS, load_spec
from research.train import (fit_models, metrics, open_lockbox, train_target, walk_forward_folds,
                            InsufficientHistory, regime_assignments)
from scripts.archive_inventory import scan_archive, main as inventory_main


def stamp(day):
    return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()) * NS


def test_date_split_percent_rounds_up_global_days_and_utc():
    ticks = np.array([stamp('2026-01-01') + d * DAY_NS for d in range(11)])
    split = date_split(np.repeat(ticks, 3))
    assert split['lockbox'] == {'first_day': '2026-01-09', 'last_day': '2026-01-11', 'days': 3}
    assert split['development']['last_day'] == '2026-01-07'
    assert split['embargo']['first_day'] == split['embargo']['last_day'] == '2026-01-08'
    assert date_split([stamp('2026-01-01') - 1, stamp('2026-01-01')])['input_first_day'] == '2025-12-31'


def test_date_split_explicit_boundary_calendar_embargo_and_gaps():
    ticks = stamp('2026-01-01') + np.array([0, 1, 2, 4, 6, 7, 8, 10]) * DAY_NS
    split = date_split(ticks, lockbox_from='2026-01-08', embargo_days=2)
    assert split['development']['last_day'] == '2026-01-05'
    assert split['embargo']['first_day'] == '2026-01-06'
    assert split['embargo']['last_day'] == '2026-01-07'
    assert split['lockbox']['first_day'] == '2026-01-08'
    dev = ticks[ticks < split['development_end_ns']]
    box = ticks[ticks >= split['lockbox_start_ns']]
    assert not set(dev) & set(box)
    assert stamp('2026-01-07') not in dev and stamp('2026-01-07') not in box


@pytest.mark.parametrize('kwargs', [{'embargo_days': 0}, {'max_horizon_s': 86401},
                                   {'lockbox_pct': 0}, {'lockbox_pct': 100},
                                   {'lockbox_from': '20260108'}])
def test_invalid_date_split(kwargs):
    with pytest.raises(ValueError):
        date_split([0, DAY_NS], **kwargs)


def test_weekly_expanding_folds_default_embargo_availability_and_latest_cap():
    ticks = stamp('2026-01-01') + np.arange(120 * 24, dtype=np.int64) * 3600 * NS
    available = ticks + 300 * NS
    available[0] = ticks[-1] + DAY_NS
    folds = walk_forward_folds(ticks, 900, label_available_ns=available, max_folds=3)
    all_folds = walk_forward_folds(ticks, 900, label_available_ns=available, max_folds=100)
    assert len(folds) == 3 and len(all_folds) > 3
    for (train, test), (expected_train, expected_test) in zip(folds, all_folds[-3:]):
        np.testing.assert_array_equal(train, expected_train)
        np.testing.assert_array_equal(test, expected_test)
        assert 0 not in train
        assert np.all(ticks[train] + 1800 * NS < ticks[test[0]])
        assert np.all(available[train] < ticks[test[0]])
        assert ticks[test[-1]] - ticks[test[0]] < 7 * DAY_NS
    assert len(folds[-1][0]) > len(folds[0][0])
    assert all_folds[0][1][0] == 14 * 24


def test_weekly_blocks_keep_calendar_boundaries_with_missing_days_and_late_features():
    ticks = np.arange(40 * 24, dtype=np.int64) * 3600 * NS
    ticks = ticks[(ticks // DAY_NS != 14) & (ticks // DAY_NS != 21)]
    features = ticks + 1
    features[1] = 100 * DAY_NS
    folds = walk_forward_folds(ticks, 300, fold_embargo_s=3600, feature_available_ns=features)
    assert ticks[folds[0][1][0]] == 15 * DAY_NS
    assert ticks[folds[1][1][0]] == 22 * DAY_NS
    for train, test in folds:
        assert 1 not in train
        start_day = ticks[test[0]] // DAY_NS
        # Even when the first test day is absent, train stops before the block's true start.
        true_start = (start_day - 1) * DAY_NS if start_day in (15, 22) else start_day * DAY_NS
        assert np.all(ticks[train] + 3900 * NS < true_start)


@pytest.mark.parametrize('kwargs', [{'fold_days': 0}, {'min_train_days': 0}, {'max_folds': 0},
                                   {'fold_embargo_s': -1}, {'purge_s': 299}])
def test_invalid_weekly_settings(kwargs):
    with pytest.raises(ValueError):
        walk_forward_folds(np.arange(40) * DAY_NS, 300, **kwargs)


def test_short_history_never_falls_back_to_row_folds():
    with pytest.raises(InsufficientHistory):
        walk_forward_folds(np.arange(14) * DAY_NS, 300)


def test_day_permutation_preserves_whole_blocks_and_pairs():
    ticks = np.concatenate([d * DAY_NS + np.arange(size) * NS for d, size in enumerate([4, 7, 3, 9, 5])])
    order = day_block_permutation(ticks)
    np.testing.assert_array_equal(order, day_block_permutation(ticks))
    assert sorted(order) == list(range(len(ticks))) and not np.array_equal(order, np.arange(len(ticks)))
    for day in np.unique(ticks // DAY_NS):
        indices = order[ticks[order] // DAY_NS == day]
        assert np.all(np.diff(indices) == 1)
        locations = np.flatnonzero(ticks[order] // DAY_NS == day)
        assert np.all(np.diff(locations) == 1)


def test_shuffle_destroys_planted_label_feature_on_same_weekly_folds():
    rng = np.random.default_rng(42)
    ticks = np.arange(80 * 80, dtype=np.int64) * (1080 * NS)
    y = rng.normal(size=len(ticks))
    x = np.column_stack([y, rng.normal(size=(len(y), 12))]).astype(np.float64)  # planted leak plus ordinary noise
    features = ['leak'] + [f'noise_{i}' for i in range(12)]
    normal, shuffled, actual = [], [], []
    for i, (train, test) in enumerate(walk_forward_folds(ticks, 300, max_folds=12)):
        reg, _, _ = fit_models(x[train], y[train], np.sign(y[train]).astype(int), features, 30)
        perm = day_block_permutation(ticks[train], seed=17+i)
        random_reg, _, _ = fit_models(x[train], y[train][perm], np.sign(y[train][perm]).astype(int), features, 30)
        normal.extend(reg.predict(x[test], num_threads=2))
        shuffled.extend(random_reg.predict(x[test], num_threads=2))
        actual.extend(y[test])
    good, null = metrics(actual, normal), metrics(actual, shuffled)
    assert good['spearman_ic'] > .95
    assert abs(null['spearman_ic']) < .12
    assert .4 < null['directional_accuracy'] < .6


def test_leak_flag_synthetic_numbers_threshold_and_undefined():
    assert leak_check_metrics({'n': 1001, 'spearman_ic': .2})['flag'] == 'LEAK SUSPECTED'
    assert leak_check_metrics({'n': 1001, 'spearman_ic': -.2})['flag'] == 'LEAK SUSPECTED'
    result = leak_check_metrics({'n': 1001, 'spearman_ic': .02})
    assert result['flag'] == 'No leak flag' and result['ic_zero_se'] == pytest.approx(1/np.sqrt(1000))
    assert '1/sqrt' in result['se_formula']
    assert leak_check_metrics({'n': 1, 'spearman_ic': None})['flag'] == 'Insufficient data'


def test_memory_guard_before_allocation_and_physical_default(monkeypatch, capsys):
    monkeypatch.setattr('research.evaluation.os.sysconf', lambda key: {'SC_PHYS_PAGES': 1024**2, 'SC_PAGE_SIZE': 4096}[key])
    monkeypatch.setattr('research.evaluation.Path.read_text', lambda self: 'max')
    monkeypatch.delenv('SLURM_MEM_PER_NODE', raising=False)
    monkeypatch.delenv('SLURM_MEM_PER_CPU', raising=False)
    estimate = memory_estimate(100, 50)
    assert estimate['bytes'] == 100 * 50 * 8 and estimate['dtype'] == 'float64'
    assert estimate['max_memory_gb'] == pytest.approx(2.8)
    with pytest.raises(ValueError, match='stride-s.*fewer days'):
        memory_estimate(10**9, 100, .001)
    assert '8 bytes' in capsys.readouterr().out


@pytest.mark.parametrize('cgroup,node,cpu,expected_gb', [
    ('1073741824', None, None, .7),
    ('max', '2048', None, 1.4),
    ('9223372036854771712', None, '256', .7),
    ('2147483648', '1024', None, .7),
    ('0', None, None, 2.8),
    ('invalid', '-1', 'invalid', 2.8),
])
def test_memory_guard_respects_cgroup_and_slurm(monkeypatch, cgroup, node, cpu, expected_gb):
    monkeypatch.setattr('research.evaluation.os.sysconf',
                        lambda key: {'SC_PHYS_PAGES': 1024**2, 'SC_PAGE_SIZE': 4096}[key])
    monkeypatch.setattr('research.evaluation.Path.read_text', lambda self: cgroup)
    monkeypatch.setattr('research.evaluation.available_cpus', lambda: 4)
    monkeypatch.delenv('SLURM_CPUS_PER_TASK', raising=False)
    monkeypatch.delenv('SLURM_CPUS_ON_NODE', raising=False)
    for name, value in [('SLURM_MEM_PER_NODE', node), ('SLURM_MEM_PER_CPU', cpu)]:
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    assert memory_estimate(100, 10)['max_memory_gb'] == pytest.approx(expected_gb)


@pytest.mark.parametrize('cpu_variable', ['SLURM_CPUS_PER_TASK', 'SLURM_CPUS_ON_NODE'])
def test_memory_guard_uses_slurm_allocated_cpu_count(monkeypatch, cpu_variable):
    monkeypatch.setattr('research.evaluation.os.sysconf',
                        lambda key: {'SC_PHYS_PAGES': 1024**2, 'SC_PAGE_SIZE': 4096}[key])
    monkeypatch.setattr('research.evaluation.Path.read_text', lambda self: 'max')
    monkeypatch.setattr('research.evaluation.available_cpus', lambda: 64)
    for name in ('SLURM_MEM_PER_NODE', 'SLURM_CPUS_PER_TASK', 'SLURM_CPUS_ON_NODE'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv('SLURM_MEM_PER_CPU', '256')
    monkeypatch.setenv(cpu_variable, '2')
    assert memory_estimate(100, 10)['max_memory_gb'] == pytest.approx(.35)


@pytest.mark.parametrize('path', ['/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory.limit_in_bytes',
                                 '/sys/fs/cgroup/memory/memory.limit_in_bytes'])
def test_memory_guard_cgroup_paths_and_unavailable_files(monkeypatch, path):
    monkeypatch.setattr('research.evaluation.os.sysconf',
                        lambda key: {'SC_PHYS_PAGES': 1024**2, 'SC_PAGE_SIZE': 4096}[key])
    monkeypatch.delenv('SLURM_MEM_PER_NODE', raising=False)
    monkeypatch.delenv('SLURM_MEM_PER_CPU', raising=False)

    def read(self):
        if str(self) == path:
            return str(1024**3)
        raise OSError('unavailable')

    monkeypatch.setattr('research.evaluation.Path.read_text', read)
    assert memory_estimate(100, 10)['max_memory_gb'] == pytest.approx(.7)


def test_float64_live_training_vector_parity_and_predictions():
    from inference.service import _reindex
    rng = np.random.default_rng(3)
    frame = pl.DataFrame({'a': rng.normal(size=400), 'b': rng.normal(size=400)})
    features = ['b', 'a', 'missing']
    training = feature_array(frame, features)
    live = _reindex(frame, features)
    assert training.dtype == live.dtype == np.float64
    np.testing.assert_array_equal(training, live)
    y = frame['a'].to_numpy() * 2 + frame['b'].to_numpy()
    reg, clf, _ = fit_models(training, y, np.sign(y).astype(int), features, 10)
    raw = frame.select('b', 'a').with_columns(pl.lit(None, dtype=pl.Float64).alias('missing')).to_numpy()
    np.testing.assert_array_equal(training, raw)
    for model in (reg, clf):
        np.testing.assert_array_equal(model.predict(training, num_threads=2),
                                      model.predict(live, num_threads=2))
    special = feature_array(pl.DataFrame({'a': [float('inf'), None, 2.]}), ['a'])
    assert np.isnan(special[:2]).all()


def test_importance_multiclass_shapes_sample_limit_and_groups():
    rng = np.random.default_rng(7)
    features = ['binance_perp_rv_300s', 'binance_perp_rv_900s', 'cal_hour_sin', 'ctx_usdt_spread_60s', 'cross_cross_basis_bps_binance']
    x = rng.normal(size=(300, len(features))).astype(np.float64)
    y = x[:, 0] + x[:, 1]
    reg, clf, _ = fit_models(x, y, np.sign(y).astype(int), features, 3)
    for model in (reg, clf):
        result = model_importance(model, x, features, sample_size=51)
        assert result['sample_rows'] == 51 and len(result['top_shap']) == len(features)
        assert set(result['by_source']) == {'binance_perp', 'cal_', 'ctx_', 'cross_'}
        assert sum(v['gain'] for v in result['by_source'].values()) == pytest.approx(sum(model.feature_importance(importance_type='gain')))
        assert all(v['mean_abs_shap'] >= 0 for v in result['top_shap'])
    assert feature_groups('binance_perp_rv_300s') == ('binance_perp_rv', 'binance_perp')


@pytest.fixture
def sparse_evaluation(monkeypatch):
    """Sparse feature fixture with synthetic labels; sentinel guards every label call."""
    import research.train as module
    rng = np.random.default_rng(91)
    ticks = stamp('2026-01-01') + np.arange(40 * 24, dtype=np.int64) * 3600 * NS
    values = rng.normal(size=len(ticks))
    rows = pl.DataFrame({'tick_ns': ticks, 'available_ns': ticks + 250_000_000,
                         'exchange': ['binance'] * len(ticks), 'market': ['spot'] * len(ticks),
                         'symbol': ['BTCUSDT'] * len(ticks), 'mid_e9': [100 * NS] * len(ticks),
                         'book_valid': [True] * len(ticks), 'warmup': [False] * len(ticks),
                         'engine_version': ['fixture'] * len(ticks), 'replay_mode': ['ideal'] * len(ticks),
                         'oracle': values, 'rv_300s': np.abs(values) + .1})
    calls = []

    def labels(frame, target, horizon, spec):
        calls.append(frame['tick_ns'].to_numpy())
        y = frame['oracle'].to_numpy()
        return frame.select('tick_ns').with_columns(
            (pl.col('tick_ns') + horizon * NS).alias('label_end_ns'),
            (pl.col('tick_ns') + horizon * NS + 250_000_000).alias('label_available_ns'),
            pl.Series('return_pct', y), pl.Series('direction', np.sign(y).astype(np.int8)),
            pl.Series('adverse_long_bps', np.abs(y) * 100),
            pl.Series('adverse_short_bps', np.abs(y) * 90))
    monkeypatch.setattr(module, 'make_labels', labels)
    spec = dict(load_spec(), calendar=None, context_assets=[])
    return rows, spec, calls


def test_training_split_sentinel_report_and_all_final_importance(tmp_path, sparse_evaluation):
    rows, spec, calls = sparse_evaluation
    r = train_target(rows, 'BTC_spot', 300, tmp_path, 'v1', spec=spec, rounds=3, shuffle_test=True)
    assert r['status'] == 'trained'
    assert len(calls) == 1 and np.all(calls[0] < r['date_split']['development_end_ns'])
    assert not np.any(calls[0] >= r['date_split']['lockbox_start_ns'])
    assert 'lockbox_results' not in r and not (tmp_path / 'lockbox_ledger.jsonl').exists()
    directory = Path(r['model_dir'])
    audit = pl.read_parquet(directory / 'training_rows.parquet')
    assert audit['tick_ns'].max() < r['date_split']['development_end_ns']
    assert len(r['importance']) == 6 and r['shuffle_test']
    html_path = directory.parent / 'report_v1.html'
    html = html_path.read_text()
    for section in ('Data window', 'Walk-forward folds', 'Pooled out-of-fold', 'Regime breakdown', 'Stops:', 'Shuffle / leak check', 'Feature importance', 'by_group', 'by_source'):
        assert section in html
    assert 'http://' not in html and 'https://' not in html and '<svg' in html
    assert 'Opened lockbox' not in html
    from research.report import generate_report
    assert generate_report(tmp_path, 'v1').is_file()


def test_memory_guard_blocks_training_x_allocation(tmp_path, sparse_evaluation, monkeypatch):
    rows, spec, _ = sparse_evaluation
    def forbidden(*args, **kwargs):
        pytest.fail('X conversion must not run after failed memory guard')
    monkeypatch.setattr('research.train.feature_array', forbidden)
    with pytest.raises(ValueError, match='max-memory-gb'):
        train_target(rows, 'BTC_spot', 300, tmp_path, 'too-large', spec=spec, rounds=1, max_memory_gb=1e-12)
    assert not (tmp_path / 'models').exists()


def test_lockbox_frozen_models_ledger_and_cross_version_reuse_guard(tmp_path, sparse_evaluation, monkeypatch, capsys):
    rows, spec, calls = sparse_evaluation
    r = train_target(rows, 'BTC_spot', 300, tmp_path, 'v1', spec=spec, rounds=2)
    train_target(rows, 'BTC_spot', 300, tmp_path, 'v2', spec=spec, rounds=2)
    directory = Path(r['model_dir'])
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.glob('*.txt')}
    calls.clear()
    def forbidden(*args, **kwargs):
        pytest.fail('Lockbox opening cannot refit any model')
    monkeypatch.setattr('research.train.fit_models', forbidden)
    monkeypatch.setattr('research.train.fit_stop_models', forbidden)
    result = open_lockbox(rows, tmp_path, 'v1')
    assert 'lockbox_results' in result['results'][0]
    assert len(calls) == 1 and np.all(calls[0] >= r['date_split']['lockbox_start_ns'])
    assert hashes == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.glob('*.txt')}
    ledger = tmp_path / 'lockbox_ledger.jsonl'
    entries = [json.loads(line) for line in ledger.read_text().splitlines()]
    assert len(entries) == 1 and len(entries[0]['metrics_digest']) == 64 and entries[0]['version'] == 'v1'
    assert entries[0]['lockbox_range'] == r['date_split']['lockbox']
    assert 'Opened lockbox' in (tmp_path / 'report_v1.html').read_text()
    calls.clear()
    with pytest.raises(ValueError, match='i-understand-lockbox-reuse'):
        open_lockbox(rows, tmp_path, 'v2')
    assert not calls and 'no longer untouched' in capsys.readouterr().out
    open_lockbox(rows, tmp_path, 'v1', acknowledge_reuse=True)
    assert len(ledger.read_text().splitlines()) == 2


def test_open_lockbox_without_input_days_rejects_missing_frozen_day(tmp_path, sparse_evaluation):
    rows, spec, _ = sparse_evaluation
    trained = train_target(rows, 'BTC_spot', 300, tmp_path, 'v1', spec=spec, rounds=2)
    lockbox = trained['date_split']['lockbox']
    missing_day = stamp(lockbox['first_day']) // (24 * 3600 * NS)
    incomplete = rows.filter((pl.col('tick_ns') // (24 * 3600 * NS)) != missing_day)

    with pytest.raises(ValueError, match='Lockbox feature dates differ'):
        open_lockbox(incomplete, tmp_path, 'v1')


def test_regimes_utc_hours_weekend_and_training_edges():
    ticks = np.array([stamp('2026-01-02'), stamp('2026-01-03') + 20 * 3600 * NS])
    groups = regime_assignments(ticks, np.array([1., 8.]), [2., 5.])
    np.testing.assert_array_equal(groups['week_part'], [0, 1])
    np.testing.assert_array_equal(groups['utc_hour_4h'], [0, 5])
    np.testing.assert_array_equal(groups['volatility_tercile'], [0, 2])


def test_smoothed_prior_log_loss_brier_and_majority_direction_hand_example():
    from research.train import forecast_metrics
    prior = class_prior([-1, -1, 0, 1, 1, 1])
    np.testing.assert_array_equal(prior, np.array([3., 2., 4.]) / 9)
    probs = np.array([[.5, .25, .25], [.25, .5, .25], [.1, .2, .7]], dtype=np.float64)
    directions = np.array([-1, 0, 1])
    scores = probability_scores(directions, probs)
    assert scores['log_loss'] == pytest.approx(-np.log([.5, .5, .7]).mean())
    assert scores['brier_score'] == pytest.approx((.375 + .375 + .14) / 3)
    assert scores['probability_n'] == 3
    assert probability_scores([-1], [[0., 0., 1.]])['log_loss'] == -np.log(1e-15)
    assert probability_scores([], np.empty((0, 3)))['log_loss'] is None
    assert probability_scores([1], [[np.nan] * 3])['probability_n'] == 0
    y = np.array([-2., 0., 3.])
    prior_probs = np.tile(prior, (3, 1))
    measured = forecast_metrics(y, {'prior': np.ones(3)}, {'prior': prior_probs})['prior']
    assert measured['directional_accuracy'] == .5
    assert measured['directional_n'] == 2
    assert measured['mae'] == metrics(y, np.ones(3))['mae']
    with pytest.raises(ValueError, match='probability'):
        probability_losses(directions, np.ones((3, 2)))


@pytest.mark.parametrize('length', [1, 3, 7])
def test_bootstrap_resamples_whole_ordered_days_in_moving_blocks(length):
    # Unequal rows/day make row sampling or an unweighted mean of days incorrect.
    sizes = np.arange(1, 10)
    ticks = np.concatenate([day * DAY_NS + np.arange(size) for day, size in enumerate(sizes)])
    values = np.arange(len(ticks), dtype=np.float64) / 10
    samples = moving_day_blocks(len(sizes), length, draws=50, seed=43)
    np.testing.assert_array_equal(samples, moving_day_blocks(len(sizes), length, draws=50, seed=43))
    assert not np.array_equal(samples, moving_day_blocks(len(sizes), length, draws=50, seed=44))
    expected = []
    for sampled in samples:
        for start in range(0, len(sizes), length):
            assert np.all(np.diff(sampled[start:start + length]) == 1)
        indices = np.concatenate([np.flatnonzero(ticks // DAY_NS == day) for day in sampled])
        for day in np.unique(sampled):
            counts = np.bincount(indices, minlength=len(ticks))
            assert np.all(counts[ticks // DAY_NS == day] == np.sum(sampled == day))
        expected.append(values[indices].mean())
    result = paired_day_block_bootstrap(ticks, {'paired': values, 'identical': values - values},
                                        block_lengths=[length], draws=50, seed=43)
    assert result == paired_day_block_bootstrap(ticks, {'paired': values, 'identical': values - values},
                                                block_lengths=[length], draws=50, seed=43)
    score = result['blocks'][str(length)]['paired']
    np.testing.assert_allclose([score['p2_5'], score['p50'], score['p97_5']], np.quantile(expected, [.025, .5, .975]))
    assert score['mean_difference'] == pytest.approx(values.mean())
    assert result['test_days'] == 9 and score['paired_rows'] == sizes.sum()
    zero = result['blocks'][str(length)]['identical']
    assert [zero[k] for k in ('mean_difference', 'p2_5', 'p50', 'p97_5')] == [0.] * 4


def test_bootstrap_common_finite_mask_empty_scores_and_short_day_history():
    ticks = np.repeat(np.arange(2) * DAY_NS, 3)
    result = paired_day_block_bootstrap(ticks, {'missing': [1., np.nan, 3., np.nan, np.nan, np.nan],
                                               'empty': np.full(6, np.nan)})
    for scores in result['blocks'].values():
        assert scores['missing']['mean_difference'] == 2.
        assert scores['missing']['paired_rows'] == 2 and scores['missing']['paired_days'] == 1
        assert scores['empty']['p50'] is None and scores['empty']['valid_draws'] == 0
    assert paired_day_block_bootstrap([], {'empty': []})['test_days'] == 0
    with pytest.raises(ValueError, match='align'):
        paired_day_block_bootstrap(ticks, {'wrong': [0.]})


def test_oof_file_reproduces_direction_risk_regimes_bootstrap_and_shuffle(tmp_path, sparse_evaluation):
    from research.risk import STOP_NAMES, stop_metrics
    rows, spec, _ = sparse_evaluation
    # Include exact flats, true past endpoints for momentum and missing momentum rows.
    rows = rows.with_row_index('i').with_columns(
        pl.when(pl.col('i') % 11 == 0).then(0.).otherwise(pl.col('oracle')).alias('oracle'),
        (pl.col('mid_e9') + pl.col('i').cast(pl.Int64) * 100).alias('mid_e9')).drop('i')
    past = rows.with_columns((pl.col('tick_ns') - 900 * NS).alias('tick_ns'),
                             (pl.col('available_ns') - 900 * NS).alias('available_ns'),
                             (pl.col('mid_e9') - 1).alias('mid_e9'))
    rows = pl.concat([past, rows]).sort('tick_ns')
    report = train_target(rows, 'BTC_spot', 900, tmp_path, 'oof', spec=spec, rounds=2,
                           shuffle_test=True, num_threads=2)
    frame = pl.read_parquet(Path(report['model_dir']) / 'oof.parquet')
    required = {'tick_ns', 'available_ns', 'label_end_ns', 'label_available_ns', 'fold', 'return_pct',
                'direction', 'regressor', 'p_down', 'p_flat', 'p_up', 'classifier_expected', 'p_up_minus_down',
                'zero', 'momentum', 'reversal', 'prior_p_down', 'prior_p_flat', 'prior_p_up', 'prior_expected',
                'volatility_tercile', 'adverse_long_bps', 'adverse_short_bps',
                'shuffle_regressor', 'shuffle_classifier_expected', 'shuffle_p_down', 'shuffle_p_flat', 'shuffle_p_up',
                *STOP_NAMES}
    assert required <= set(frame.columns)
    assert frame.height == report['oof_rows'] == sum(f['test_rows'] for f in report['folds'])
    assert frame['tick_ns'].n_unique() == frame.height
    assert frame['tick_ns'].is_sorted()
    assert frame.schema['tick_ns'] == pl.Int64 and frame.schema['direction'] == pl.Int8
    for name in required - {'tick_ns', 'available_ns', 'label_end_ns', 'label_available_ns', 'fold', 'direction', 'volatility_tercile'}:
        assert frame.schema[name] == pl.Float64
    np.testing.assert_array_equal(frame['reversal'].to_numpy(), -frame['momentum'].to_numpy())
    np.testing.assert_array_equal(frame['p_up_minus_down'].to_numpy(), frame['p_up'].to_numpy() - frame['p_down'].to_numpy())

    def recompute(part, name, column, prefix=None):
        y, prediction = part['return_pct'].to_numpy(), part[column].to_numpy()
        probs = part.select([prefix + 'p_' + side for side in ('down', 'flat', 'up')]).to_numpy() if prefix is not None else None
        result = metrics(y, prediction, np.argmax(probs, axis=1) - 1 if probs is not None else None)
        if probs is not None:
            valid = np.isfinite(probs).all(axis=1)
            classes = part['direction'].to_numpy()[valid] + 1
            p = probs[valid]
            result.update(probability_n=int(valid.sum()),
                log_loss=float(-np.log(np.clip(p[np.arange(len(p)), classes], 1e-15, 1)).mean()) if len(p) else None,
                brier_score=float(((p - np.eye(3)[classes]) ** 2).sum(axis=1).mean()) if len(p) else None)
        return result

    def assert_direction(part, recorded):
        for name, column, prefix in [('regressor', 'regressor', None), ('classifier', 'classifier_expected', ''),
                                      ('prior', 'prior_expected', 'prior_'), ('zero', 'zero', None),
                                      ('momentum', 'momentum', None), ('reversal', 'reversal', None)]:
            assert recorded[name] == recompute(part, name, column, prefix)

    def assert_risk(part, recorded):
        for name in STOP_NAMES:
            q = int(name[-2:]) / 100
            actual = part[f'adverse_{name.split("_")[1]}_bps'].to_numpy()
            groups = part[name + '_volatility_tercile'].to_numpy()
            for method, column in [('model', name), ('unconditional', name + '_unconditional'),
                                    ('volatility_scaled', name + '_volatility_scaled')]:
                assert recorded[name][method] == stop_metrics(actual, part[column].to_numpy(), q, groups)

    assert_direction(frame, report['metrics'])
    assert_risk(frame, report['stop_metrics'])
    for fold in report['folds']:
        part = frame.filter(pl.col('fold') == fold['fold'])
        assert_direction(part, fold['metrics'])
        assert_risk(part, fold['stop_metrics'])
        assert fold['test_flat_labels'] == (part['direction'] == 0).sum()
        train_rows = rows.filter(pl.col('tick_ns') <= fold['train_end_ns'])
        assert fold['train_rows'] == train_rows.height
        assert fold['train_flat_labels'] == (train_rows['oracle'] == 0).sum()
        counts = np.bincount(np.sign(train_rows['oracle'].to_numpy()).astype(int) + 1, minlength=3)
        expected_prior = (counts + 1) / (counts.sum() + 3)
        np.testing.assert_array_equal(part.select('prior_p_down', 'prior_p_flat', 'prior_p_up').to_numpy(),
                                      np.tile(expected_prior, (part.height, 1)))
        training_y = train_rows['oracle'].to_numpy()
        class_means = np.array([training_y[np.sign(training_y) == d].mean() if np.any(np.sign(training_y) == d) else 0.
                                for d in (-1, 0, 1)])
        np.testing.assert_allclose(part['classifier_expected'].to_numpy(),
            part.select('p_down', 'p_flat', 'p_up').to_numpy() @ class_means, rtol=1e-15, atol=1e-17)
        np.testing.assert_allclose(part['prior_expected'].to_numpy(),
            part.select('prior_p_down', 'prior_p_flat', 'prior_p_up').to_numpy() @ class_means, rtol=1e-15, atol=1e-17)
    assert report['oof_flat_labels'] == (frame['direction'] == 0).sum() > 0
    trainable = rows.filter(pl.col('tick_ns') <= report['data_window']['end_ns'])
    assert report['flat_labels'] == (trainable['oracle'] == 0).sum() > 0
    for axis, labels in report['regimes'].items():
        for i, recorded in enumerate(labels.values()):
            assert_direction(frame.filter(pl.col(axis) == i), recorded)
    for name, column, prefix in [('regressor', 'shuffle_regressor', None), ('classifier', 'shuffle_classifier_expected', 'shuffle_')]:
        measured = recompute(frame, name, column, prefix)
        assert measured.items() <= report['shuffle_test'][name].items()
    assert set(report['bootstrap']['blocks']) == {'1', '3', '7'}
    assert report['bootstrap']['draws'] == 1000
    for block in report['bootstrap']['blocks'].values():
        assert len(block) == 9
        assert block['classifier_vs_reversal_directional_accuracy']['paired_rows'] > 0
        y = frame['return_pct'].to_numpy()
        directions = frame['direction'].to_numpy()
        cls_direction = np.argmax(frame.select('p_down', 'p_flat', 'p_up').to_numpy(), axis=1) - 1
        reversal = frame['reversal'].to_numpy()
        paired = (directions != 0) & np.isfinite(reversal)
        accuracy_difference = (cls_direction == directions).astype(float) - (np.sign(reversal) == directions)
        assert block['classifier_vs_reversal_directional_accuracy']['mean_difference'] == pytest.approx(accuracy_difference[paired].mean())
        assert block['regressor_vs_zero_mse']['mean_difference'] == pytest.approx(np.mean((y - frame['regressor'].to_numpy()) ** 2 - y**2))
        assert block['classifier_expected_vs_zero_mse']['mean_difference'] == pytest.approx(np.mean((y - frame['classifier_expected'].to_numpy()) ** 2 - y**2))
        cls_loss = -np.log(np.clip(frame.select('p_down', 'p_flat', 'p_up').to_numpy()[np.arange(len(y)), frame['direction'].to_numpy() + 1], 1e-15, 1))
        prior_loss = -np.log(frame.select('prior_p_down', 'prior_p_flat', 'prior_p_up').to_numpy()[np.arange(len(y)), frame['direction'].to_numpy() + 1])
        assert block['classifier_vs_prior_log_loss']['mean_difference'] == pytest.approx((cls_loss - prior_loss).mean())
        for name in STOP_NAMES:
            q = int(name[-2:]) / 100
            actual = frame[f'adverse_{name.split("_")[1]}_bps'].to_numpy()
            model_error = actual - frame[name].to_numpy()
            baseline_error = actual - frame[name + '_unconditional'].to_numpy()
            delta = np.maximum(q * model_error, (q - 1) * model_error) - np.maximum(q * baseline_error, (q - 1) * baseline_error)
            assert block[name + '_vs_unconditional_pinball']['mean_difference'] == pytest.approx(delta[np.isfinite(delta)].mean())
    html = (tmp_path / 'models/BTC_spot/900/report_oof.html').read_text()
    for text in ('reversal', 'prior', 'Log loss', 'Brier', 'Paired day-block bootstrap', 'Exact-flat labels'):
        assert text in html


@pytest.mark.parametrize('previous', [
    ('2026-02-02', '2026-02-09'),  # exact
    ('2026-01-30', '2026-02-04'),  # partial
    ('2026-02-04', '2026-02-06'),  # contained
    ('2026-01-01', '2026-03-01'),  # contains
    ('2026-01-30', '2026-02-02'),  # shared first endpoint
    ('2026-02-09', '2026-02-15'),  # shared last endpoint
])
def test_lockbox_refuses_any_inclusive_interval_overlap(tmp_path, monkeypatch, previous):
    requested = {'first_day': '2026-02-02', 'last_day': '2026-02-09', 'days': 8}
    prior = dict(zip(('first_day', 'last_day'), previous))
    assert date_intervals_overlap(requested, prior)
    assert date_intervals_overlap(prior, requested)
    ledger = tmp_path / 'custom.jsonl'
    ledger.write_text(json.dumps({'version': 'different', 'lockbox_range': prior}) + '\n')
    original = ledger.read_bytes()
    monkeypatch.setattr('research.train.lockbox_reports',
                        lambda *args: [{'date_split': {'lockbox': requested}}])
    with pytest.raises(ValueError, match='i-understand-lockbox-reuse'):
        open_lockbox(None, tmp_path, 'v1', lockbox_ledger=ledger)
    assert ledger.read_bytes() == original


def test_lockbox_disjoint_interval_allowed_and_acknowledged_reuse_marked(tmp_path, sparse_evaluation):
    rows, spec, _ = sparse_evaluation
    report = train_target(rows, 'BTC_spot', 300, tmp_path, 'v1', spec=spec, rounds=1, num_threads=2)
    ledger = tmp_path / 'ledger/custom.jsonl'
    ledger.parent.mkdir()
    ledger.write_text(json.dumps({'version': 'older', 'lockbox_range': {
        'first_day': '2025-01-01', 'last_day': '2025-12-31'}}) + '\n')
    first = open_lockbox(rows, tmp_path, 'v1', lockbox_ledger=ledger, num_threads=2)
    assert set(first['results'][0]['lockbox_results']['metrics']) == {'regressor', 'classifier', 'zero', 'momentum', 'reversal', 'prior', 'ridge'}
    entries = [json.loads(line) for line in ledger.read_text().splitlines()]
    assert entries[-1]['reused'] is False
    open_lockbox(rows, tmp_path, 'v1', acknowledge_reuse=True, lockbox_ledger=ledger, num_threads=2)
    entries = [json.loads(line) for line in ledger.read_text().splitlines()]
    assert len(entries) == 3 and entries[-1]['reused'] is True
    assert entries[-1]['lockbox_range'] == report['date_split']['lockbox']
    assert not (tmp_path / 'lockbox_ledger.jsonl').exists()


@pytest.mark.parametrize('custom', [False, True], ids=['features-root-default', 'explicit-path'])
def test_lockbox_cli_ledger_location_and_experiment_entry(tmp_path, sparse_evaluation, custom):
    import pyarrow.parquet as pq
    from research.train import main
    rows, spec, _ = sparse_evaluation
    root, output = tmp_path / 'features-root', tmp_path / 'different-output'
    report = train_target(rows, 'BTC_spot', 300, output, 'saved', spec=spec, rounds=1, num_threads=2)
    unopened = {'target': 'ETH_spot', 'horizon_s': 900, 'status': 'skipped',
                'reason': 'no reference rows', 'date_split': report['date_split']}
    (output / 'research_report_saved.json').write_text(json.dumps({'results': [report, unopened]}))
    for day in np.unique(rows['tick_ns'].to_numpy() // DAY_NS):
        path = root / 'features' / f'date={datetime.fromtimestamp(int(day) * 86400, timezone.utc).date().isoformat()}' / 'features.parquet'
        path.parent.mkdir(parents=True)
        frame = rows.filter(pl.col('tick_ns') // DAY_NS == day)
        pq.write_table(frame.to_arrow().replace_schema_metadata({b'mode': b'ideal', b'engine_version': b'fixture'}), path)
    ledger = tmp_path / 'custom/ledger.jsonl' if custom else root / 'lockbox_ledger.jsonl'
    argv = [str(root), '--output-root', str(output), '--open-lockbox', '--version', 'saved',
            '--target', 'BTC_spot', '--horizon-s', '300', '--threads', '2']
    if custom:
        argv += ['--lockbox-ledger', str(ledger)]
    result = main(argv)
    assert ledger.is_file() and not (output / 'lockbox_ledger.jsonl').exists()
    assert len(ledger.read_text().splitlines()) == 1
    measured = result['results'][0]['lockbox_results']['metrics']
    assert measured['classifier']['log_loss'] is not None and measured['prior']['brier_score'] is not None
    entry = json.loads((output / 'experiments.jsonl').read_text())
    assert entry['mode'] == 'open_lockbox' and entry['status'] == 'completed'
    assert len(entry['results']) == 1
    assert entry['targets'] == ['BTC_spot'] and entry['horizons_s'] == [300]
    assert entry['results'][0]['status'] == 'evaluated'
    assert entry['results'][0]['metrics'] == measured
    assert entry['results'][0]['rounds'] == 1
    assert entry['split'] == report['date_split']
    assert entry['engine_versions'] == ['fixture']


def test_inventory_stat_only_gaps_case_open_sources_and_targets(tmp_path, monkeypatch):
    archive = tmp_path / 'archive'
    files = ['bINANCEraw/sPOTv9/BTCUSDT/20260101_depth.jsonl',
             'bINANCEraw/sPOTv9/BTCUSDT/20260103_depth.jsonl.gz',
             'bINANCEraw/sPOTv9/BTCUSDT/20260104_depth.jsonl.open',
             'Binance/FuturesV1/ETHUSDT/20260103_markprice.jsonl',
             'Binance/FuturesV1/ETHUSDT/20260103_forceorder.jsonl',
             'Binance/FuturesV1/ETHUSDT/20260103_snapshot.jsonl',
             'Kraken/Futurev6/PF_XBTUSD/20260103_ticker.jsonl',
             'Kraken/Futurev6/PF_XBTUSD/20260103_trades.jsonl',
             'DeribitRaw/IndexV2/BTC/20260103_dvol.jsonl',
             'Other/Spot/BTC/20260102_book.jsonl',
             'Binance/Spot/BTCUSDT/20260102_unknown.jsonl',
             'Binance/Spot/BTCUSDT/20261301_depth.jsonl']
    for relative in files:
        path = archive / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'not even valid JSON')
    spec = load_spec()
    original_open = Path.open
    def guarded_open(self, *args, **kwargs):
        if self.is_relative_to(archive):
            pytest.fail('Inventory cannot open archive payloads')
        return original_open(self, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', guarded_open)
    result = scan_archive(archive, spec)
    entry = next(e for e in result['entries'] if e['channel'] == 'depth')
    assert entry['missing_days'] == ['2026-01-02']
    assert entry['number_of_days'] == 3 and len(entry['open_files']) == 1
    assert entry['total_bytes'] == 3 * len(b'not even valid JSON')
    assert result['targets']['BTC_spot']['trainable']
    assert result['targets']['ETH_perp']['trainable']
    assert not result['targets']['BTC_perp']['trainable']
    assert result['common_days']['BTC']['days'] == ['2026-01-03']
    assert result['common_days']['BTC']['exchanges'] == ['binance', 'deribit', 'kraken']
    assert result['feature_sources']['deribit_index_dvol'] == ['BTC']
    assert result['feature_sources']['binance_perp_forceorder'] == ['ETH']
    output = tmp_path / 'inventory.json'
    inventory_main([str(archive), '--json', str(output)])
    assert output.is_file()
    with pytest.raises(SystemExit):
        inventory_main([str(archive), '--json', str(archive / 'forbidden.json')])
    assert not (archive / 'forbidden.json').exists()


def test_train_from_changes_only_training_rows_and_preserves_oof_windows(tmp_path, sparse_evaluation):
    rows, spec, _ = sparse_evaluation
    baseline = train_target(rows, 'BTC_spot', 300, tmp_path, 'all-history', spec=spec, rounds=1, num_threads=2,
                            early_stopping=True, inner_valid_days=2)
    recent = train_target(rows, 'BTC_spot', 300, tmp_path, 'recent', spec=spec, rounds=1, num_threads=2,
                          train_from='2026-01-08', early_stopping=True, inner_valid_days=2)
    assert baseline['status'] == recent['status'] == 'trained'
    for original, trimmed in zip(baseline['folds'], recent['folds'], strict=True):
        for key in ('test_rows', 'test_start_ns', 'test_end_ns'):
            assert original[key] == trimmed[key]
        assert trimmed['train_rows'] < original['train_rows']
        assert trimmed['train_start_ns'] >= stamp('2026-01-08')
    columns = ['tick_ns', 'fold', 'label_end_ns', 'return_pct', 'direction']
    original_oof = pl.read_parquet(Path(baseline['model_dir']) / 'oof.parquet').select(columns)
    trimmed_oof = pl.read_parquet(Path(recent['model_dir']) / 'oof.parquet').select(columns)
    assert original_oof.equals(trimmed_oof)
    before = pl.read_parquet(Path(baseline['model_dir']) / 'training_rows.parquet')
    after = pl.read_parquet(Path(recent['model_dir']) / 'training_rows.parquet')
    assert after.equals(before.filter(pl.col('tick_ns') >= stamp('2026-01-08')))
    assert recent['final_train_rows'] == after.height < before.height


def test_lockbox_cli_uses_saved_kraken_spec_even_with_missing_label_spec(tmp_path, sparse_evaluation, monkeypatch):
    from research.train import main
    rows, _, _ = sparse_evaluation
    rows = rows.with_columns(pl.lit('kraken').alias('exchange'), pl.lit('BTC/USD').alias('symbol'),
                             pl.lit('ideal+exchange_clock_35ms').alias('replay_mode'))
    spec = dict(load_spec(Path(__file__).resolve().parents[1] / 'research/targets_kraken.yaml'), calendar=None)
    report = train_target(rows, 'BTC_spot', 300, tmp_path, 'kraken-saved', spec=spec, rounds=1, num_threads=2)
    assert report['label_spec'] == spec
    assert report['replay_modes'] == ['ideal+exchange_clock_35ms']
    path = tmp_path / 'features.parquet'
    pq.write_table(rows.to_arrow().replace_schema_metadata({b'mode': b'ideal+exchange_clock_35ms', b'engine_version': b'fixture'}), path)
    def default_forbidden(*args, **kwargs):
        pytest.fail('Lockbox must never load a current label spec')
    monkeypatch.setattr('research.train.load_spec', default_forbidden)
    result = main([str(path), '--version', 'kraken-saved', '--open-lockbox', '--threads', '2',
                   '--label-spec', str(tmp_path / 'missing.yaml')])
    opened = result['results'][0]
    assert opened['label_spec'] == spec
    assert opened['lockbox_results']['metrics']['zero']['n'] > 0


def test_train_from_without_training_rows_is_hard_error_naming_fold(tmp_path, sparse_evaluation):
    rows, spec, _ = sparse_evaluation
    with pytest.raises(ValueError, match=r'--train-from leaves selected fold 1 .*zero training rows'):
        train_target(rows, 'BTC_spot', 300, tmp_path, 'no-train', spec=spec, rounds=1,
                     num_threads=2, train_from='2026-02-01')
    assert not (tmp_path / 'models').exists()
