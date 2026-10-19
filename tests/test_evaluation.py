"""P23 safety boundaries and diagnostics; all inputs are generated fixtures."""
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import polars as pl
import pytest

from research.evaluation import (DAY_NS, date_split, day_block_permutation, feature_array,
                                 feature_groups, leak_check_metrics, memory_estimate, model_importance)
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
    x = np.column_stack([y, rng.normal(size=(len(y), 12))]).astype(np.float32)  # planted leak plus ordinary noise
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
    estimate = memory_estimate(100, 50)
    assert estimate['bytes'] == 100 * 50 * 4 and estimate['max_memory_gb'] == pytest.approx(2.8)
    with pytest.raises(ValueError, match='stride-s.*fewer days'):
        memory_estimate(10**9, 100, .001)
    assert '4 bytes' in capsys.readouterr().out


def test_float32_live_training_vector_parity_and_predictions():
    from inference.service import _reindex
    rng = np.random.default_rng(3)
    frame = pl.DataFrame({'a': rng.normal(size=400), 'b': rng.normal(size=400)})
    features = ['b', 'a', 'missing']
    training = feature_array(frame, features)
    live = _reindex(frame, features)
    assert training.dtype == live.dtype == np.float32
    np.testing.assert_array_equal(training, live)
    y = frame['a'].to_numpy() * 2 + frame['b'].to_numpy()
    reg, clf, _ = fit_models(training, y, np.sign(y).astype(int), features, 10)
    for model in (reg, clf):
        np.testing.assert_allclose(model.predict(training, num_threads=2),
                                   model.predict(training.astype(np.float64), num_threads=2), rtol=0, atol=0)
        raw = frame.select('b', 'a').with_columns(pl.lit(None, dtype=pl.Float64).alias('missing')).to_numpy()
        np.testing.assert_allclose(model.predict(training, num_threads=2), model.predict(raw, num_threads=2), rtol=1e-6, atol=1e-8)
    reg64, clf64, _ = fit_models(raw, y, np.sign(y).astype(int), features, 10)
    for model32, model64 in ((reg, reg64), (clf, clf64)):
        np.testing.assert_allclose(model32.predict(training, num_threads=2),
                                   model64.predict(raw, num_threads=2), rtol=1e-6, atol=1e-8)
    special = feature_array(pl.DataFrame({'a': [float('inf'), None, 2.]}), ['a'])
    assert np.isnan(special[:2]).all()


def test_importance_multiclass_shapes_sample_limit_and_groups():
    rng = np.random.default_rng(7)
    features = ['binance_perp_rv_300s', 'binance_perp_rv_900s', 'cal_hour_sin', 'ctx_usdt_spread_60s', 'cross_cross_basis_bps_binance']
    x = rng.normal(size=(300, len(features))).astype(np.float32)
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


def test_regimes_utc_hours_weekend_and_training_edges():
    ticks = np.array([stamp('2026-01-02'), stamp('2026-01-03') + 20 * 3600 * NS])
    groups = regime_assignments(ticks, np.array([1., 8.]), [2., 5.])
    np.testing.assert_array_equal(groups['week_part'], [0, 1])
    np.testing.assert_array_equal(groups['utc_hour_4h'], [0, 5])
    np.testing.assert_array_equal(groups['volatility_tercile'], [0, 2])


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
