"""Offline research contracts: exact labels, same-tick features, purged folds, native LGBM."""

from fractions import Fraction
from datetime import datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest
from polars.testing import assert_frame_equal

from research.dataset import build_matrix, feature_dates, feature_paths, read_features
from research.labels import (LABEL_INPUTS, NS, exact_return_pct, load_spec, make_labels,
                             momentum_900s, reference_for)
from research.train import average_ranks, main, metrics, train_target, upload_model, walk_forward_folds


def row(second, mid=100_000_000_005, **changes):
    result = dict(exchange="binance", market="spot", symbol="BTCUSDT",
                  tick_ns=second * NS, available_ns=second * NS + 250_000_000,
                  engine_version="test-v1", book_valid=True, warmup=False,
                  mid_e9=mid, mid=mid / NS, best_bid=mid / NS - .01,
                  best_ask=mid / NS + .01, microprice=mid / NS,
                  mark_price=mid / NS, spread_bps=2.0, ret_60s=0.001,
                  bid_depth10=5.0, funding_rate=0.0001, basis_bps_binance=None)
    result.update(changes)
    return result


def write_parquet(tmp_path, rows, mode="ideal", name="features.parquet"):
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pl.DataFrame(rows).to_arrow().replace_schema_metadata(
        {b"mode": mode.encode(), b"engine_version": b"test-v1"}
    )
    pq.write_table(table, path)
    return path


@pytest.fixture
def synthetic_path(tmp_path):
    # More than one horizon of training history before either test fold.
    rows = []
    for t in range(0, 10801):
        mid = 100_000_000_005 + int(100_000_000 * np.sin(t / 170)) + t * 1000
        rows.append(row(t, mid, warmup=t < 300,
                        ret_60s=float(np.sin(t / 170) - np.sin((t - 60) / 170)),
                        rv_300s=float(1 + abs(np.sin(t / 170)))))
    return write_parquet(tmp_path, rows)


def test_spec_freezes_all_four_references_and_horizons():
    spec = load_spec()
    assert spec["version"] == "v1"
    assert spec["horizons_s"] == [300, 900, 1800]
    for asset in ("BTC", "ETH"):
        for market in ("spot", "perp"):
            assert spec["references"][asset][market] == dict(
                exchange="binance", market=market, symbol=f"{asset}USDT"
            )


@pytest.mark.parametrize("start,end", [(100_000_000_005, 100_000_000_015),
                                       (2**53 + 1, 2**53 + 2),
                                       (2**63 - 1, 1), (1, 2**63 - 1)])
def test_exact_integer_label_math_including_half_atom_and_overflow(tmp_path, start, end):
    # ...005 represents a half atom midpoint when book prices have 8 decimals.
    rows = read_features([write_parquet(tmp_path, [row(0, start), row(900, end)])])
    labels = make_labels(rows, "BTC_spot", 900)
    assert labels["return_pct"][0] == float(Fraction(100 * (end - start), start))
    assert labels["direction"][0] == ((end > start) - (end < start))
    assert labels["label_end_ns"][0] == 900 * NS
    assert labels["label_available_ns"][0] == 900 * NS + 250_000_000


@pytest.mark.parametrize("endpoint", ["missing", "invalid", "null_mid", "nearby"])
def test_label_null_for_missing_or_invalid_exact_endpoint(tmp_path, endpoint):
    rows = [row(0)]
    if endpoint != "missing":
        changes = {"book_valid": False} if endpoint == "invalid" else {}
        if endpoint == "null_mid":
            changes["mid_e9"] = None
        rows.append(row(901 if endpoint == "nearby" else 900, **changes))
    frame = read_features([write_parquet(tmp_path, rows)])
    labels = make_labels(frame, "BTC_spot", 900)
    assert labels["return_pct"][0] is None
    assert labels["direction"][0] is None


@pytest.mark.parametrize("changes", [{"book_valid": False}, {"warmup": True}, {"mid_e9": 0}])
def test_label_requires_valid_nonwarm_start(tmp_path, changes):
    frame = read_features([write_parquet(tmp_path, [row(0, **changes), row(900)])])
    assert make_labels(frame, "BTC_spot", 900)["return_pct"][0] is None


def test_flat_direction_and_reference_isolation(tmp_path):
    data = [row(0), row(900), row(0, 10, market="perp"), row(900, 11, market="perp")]
    frame = read_features([write_parquet(tmp_path, data)])
    assert make_labels(frame, "BTC_spot", 900)["direction"][0] == 0
    assert make_labels(frame, "BTC_perp", 900)["return_pct"][0] == 10.0


def test_same_tick_wide_features_drop_prices_keep_cross_and_availability(tmp_path):
    data = [row(0), row(10), row(20, warmup=True), row(30, book_valid=False), row(11)]
    data += [row(0, exchange="okx", symbol="BTC-USDT", spread_bps=4.0,
                 available_ns=700_000_000),
             row(11, exchange="okx", symbol="BTC-USDT", spread_bps=9999.0),
             row(0, exchange="cross", market="cross", symbol="BTC", book_valid=False,
                 basis_bps_binance=3.0),
             row(0, symbol="ETHUSDT", spread_bps=8888.0)]
    frame = read_features([write_parquet(tmp_path, data)])
    matrix, features = build_matrix(frame, "BTC_spot")
    assert matrix["tick_ns"].to_list() == [0, 10 * NS]
    assert matrix["asset"].to_list() == ["BTC", "BTC"]
    assert matrix["available_ns"][0] == 700_000_000
    assert matrix["okx_spot_spread_bps"].to_list() == [4.0, None]
    assert matrix["cross_cross_basis_bps_binance"][0] == 3.0
    assert matrix["replay_mode"].to_list() == ["ideal", "ideal"]
    assert "binance_spot_ret_60s" in features
    for banned in ("mid", "mid_e9", "best_bid", "best_ask", "microprice", "mark_price",
                   "tick_ns", "available_ns", "engine_version", "replay_mode", "symbol"):
        assert not any(c == banned or c.endswith("_" + banned) for c in features)
    # Changing an arbitrarily large FUTURE feature cannot change the matrix at t=0/10.
    changed = frame.with_columns(pl.when(pl.col("tick_ns") > 10 * NS).then(1e20)
                                 .otherwise(pl.col("spread_bps")).alias("spread_bps"))
    other, _ = build_matrix(changed, "BTC_spot")
    assert matrix.equals(other)


def test_lazy_target_loading_keeps_context_cross_and_input_date_split(tmp_path):
    root = tmp_path / 'features'
    start = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
    for offset in range(10):
        day = (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=offset)).date().isoformat()
        rows = []
        for second in (0, 10, 300, 900):
            tick = start + offset * 86400 + second
            rows += [row(tick, symbol='BTCUSDT'),
                     row(tick, symbol='ETHUSDT', mid=200.0),
                     row(tick, symbol='USDTUSD', mid=1.0, asset='USDT'),
                     row(tick, exchange='cross', market='cross', symbol='BTC', asset='')]
        path = root / f'date={day}' / 'features.parquet'
        write_parquet(path.parent, rows)
    paths = feature_paths(tmp_path)
    spec = {k: v for k, v in load_spec().items() if k != 'calendar'}
    btc = read_features(paths, ['BTC_spot'], spec)
    both = read_features(paths, ['BTC_spot', 'ETH_spot'], spec)
    assert set(btc['symbol'].unique().to_list()) == {'BTCUSDT', 'USDTUSD', 'BTC'}
    assert 'ETHUSDT' in both['symbol'].unique().to_list()
    assert btc.filter(pl.col('exchange') == 'cross').height == 40
    btc_days = feature_dates(paths, btc['tick_ns'].to_numpy())
    both_days = feature_dates(paths, both['tick_ns'].to_numpy())
    assert btc_days == both_days == [f'2026-01-{day:02d}' for day in range(1, 11)]
    btc_split = __import__('research.evaluation', fromlist=['date_split']).date_split(
        btc['tick_ns'].to_numpy(), input_days=btc_days)
    both_split = __import__('research.evaluation', fromlist=['date_split']).date_split(
        both['tick_ns'].to_numpy(), input_days=both_days)
    assert btc_split == both_split
    assert_frame_equal = pytest.importorskip('polars.testing').assert_frame_equal
    btc_matrix, btc_features = build_matrix(btc, 'BTC_spot', spec=spec)
    both_matrix, both_features = build_matrix(both, 'BTC_spot', spec=spec)
    assert btc_features == both_features
    assert_frame_equal(btc_matrix, both_matrix)


def test_reject_mixed_modes_even_on_disjoint_days(tmp_path):
    paths = [write_parquet(tmp_path, [row(0)], "ideal", "a.parquet"),
             write_parquet(tmp_path, [row(10)], "as_live", "b.parquet")]
    with pytest.raises(ValueError, match="Cannot mix replay modes within one training run"):
        read_features(paths)


def test_reject_mixing_modes_within_a_feature_tick(tmp_path):
    paths = [write_parquet(tmp_path, [row(0)], "ideal", "a.parquet"),
             write_parquet(tmp_path, [row(0, market="perp")], "as_live", "b.parquet")]
    with pytest.raises(ValueError, match="Cannot mix"):
        build_matrix(read_features(paths), "BTC_spot")


def test_reject_duplicate_ticks_unknown_mode_offgrid_and_future_availability(tmp_path):
    path = write_parquet(tmp_path, [row(0), row(0)])
    with pytest.raises(ValueError, match="Duplicate"):
        read_features([path])
    path = write_parquet(tmp_path, [row(0)], "unknown", "unknown.parquet")
    with pytest.raises(ValueError, match="replay mode"):
        read_features([path])
    for name, changes in [("grid", {"tick_ns": 1}), ("early", {"available_ns": -1})]:
        path = write_parquet(tmp_path, [row(0, **changes)], name=f"{name}.parquet")
        with pytest.raises(ValueError, match="whole seconds"):
            read_features([path])


def test_trailing_momentum_uses_exact_past_and_never_future(tmp_path):
    data = [row(0, 100_000_000_005), row(900, 101_000_000_005), row(1800, 999_000_000_005)]
    rows = read_features([write_parquet(tmp_path, data)])
    trailing = momentum_900s(rows, "BTC_spot")
    assert trailing["momentum_return_900s"][0] is None
    assert trailing["momentum_return_900s"][1] == exact_return_pct(100_000_000_005, 101_000_000_005)


@pytest.mark.parametrize("horizon", [300, 900, 1800])
def test_walk_forward_order_expansion_and_purge_no_overlapping_labels(horizon):
    ticks = np.arange(40 * 24, dtype=np.int64) * 3600 * NS
    # Some labels are emitted even later than the normal endpoint + 250ms.
    available = ticks + horizon * NS + 125 * NS
    folds = walk_forward_folds(ticks, horizon, label_available_ns=available)
    previous_train, previous_test_end = set(), -1
    for train, test in folds:
        assert np.all(np.diff(ticks[train]) > 0) and np.all(np.diff(ticks[test]) > 0)
        assert previous_train < set(train)
        assert test[0] > previous_test_end
        assert ticks[train[-1]] + horizon * NS < ticks[test[0]]
        assert np.all(available[train] < ticks[test[0]])
        assert (ticks[test[0]] - ticks[train[-1]]) / NS >= horizon
        previous_train, previous_test_end = set(train), test[-1]


def test_reject_small_purge_unsorted_folds_and_insufficient_history():
    ticks = np.arange(400) * 10 * NS
    with pytest.raises(ValueError, match="purge_s"):
        walk_forward_folds(ticks, 900, purge_s=899)
    with pytest.raises(ValueError, match="time-ordered"):
        walk_forward_folds(ticks[::-1], 900)
    with pytest.raises(ValueError, match="Insufficient history"):
        walk_forward_folds(ticks[:10], 900)


def test_late_training_feature_availability_is_purged():
    ticks = np.arange(40 * 24, dtype=np.int64) * 3600 * NS
    available = ticks + 250_000_000
    available[0] = ticks[-1] + 86400 * NS
    folds = walk_forward_folds(ticks, 900, feature_available_ns=available)
    assert all(0 not in train for train, _ in folds)
    for train, test in folds:
        assert np.all(available[train] < ticks[test[0]])


def test_upload_uses_expected_model_keys_without_network(tmp_path, monkeypatch):
    import boto3
    from types import SimpleNamespace

    directory = tmp_path / "models" / "BTC_spot" / "900" / "test"
    directory.mkdir(parents=True)
    (directory / "regressor.txt").write_text("synthetic")
    calls = []
    monkeypatch.setattr(boto3, "client", lambda service: SimpleNamespace(
        upload_file=lambda *args: calls.append(args)))
    upload_model(directory, tmp_path, "synthetic-bucket")
    assert calls == [(str(directory / "regressor.txt"), "synthetic-bucket",
                      "models/BTC_spot/900/test/regressor.txt")]


def test_metrics_ties_flats_missing_predictions_and_undefined_ic():
    np.testing.assert_array_equal(average_ranks([3, 1, 1, 2]), [4, 1.5, 1.5, 3])
    result = metrics([1., -1., 0.], [2., -2., 0.])
    assert result["mae"] == 2 / 3
    assert result["rmse"] == pytest.approx(np.sqrt(2 / 3))
    assert result["directional_accuracy"] == 1 and result["directional_n"] == 2
    assert result["spearman_ic"] == 1
    assert metrics([1, -1], [0, 0])["spearman_ic"] is None
    assert metrics([1, -1], [np.nan, -1])["n"] == 1
    assert metrics([0], [0])["directional_accuracy"] is None
    assert metrics([1, -1], [1, -1], [-1, 1])["directional_accuracy"] == 0


@pytest.mark.parametrize("horizon", [300, 900, 1800])
def test_tiny_end_to_end_cli_models_metadata_and_row_modes(tmp_path, synthetic_path, horizon,
                                                         compact_folds, monkeypatch):
    monkeypatch.setattr('research.train.available_cpus', lambda: 4)
    output = tmp_path / "output"
    result = main([str(synthetic_path), "--output-root", str(output), "--target", "BTC_spot",
                   "--horizon-s", str(horizon), "--lockbox-from", "1970-01-03", "--rounds", "3", "--version", "test"])
    report = result["results"][0]
    assert report["labeled_rows"] > 0 and len(report["folds"]) == 2
    assert report["replay_modes"] == ["ideal"]
    directory = Path(report["model_dir"])
    metadata = json.loads((directory / "metadata.json").read_text())
    assert metadata["label_spec_version"] == "v1"
    assert metadata["engine_version"] == ["test-v1"]
    assert metadata['num_threads'] == 4
    assert metadata['memory']['dtype'] == 'float64'
    audit = pl.read_parquet(directory / "training_rows.parquet")
    assert audit.height == report["labeled_rows"]
    assert audit["replay_mode"].unique().to_list() == ["ideal"]
    oof = pl.read_parquet(directory / 'oof.parquet')
    assert oof.height == sum(f['test_rows'] for f in report['folds'])
    assert not any(c.startswith('shuffle_') for c in oof.columns)
    np.testing.assert_array_equal(oof['reversal'].to_numpy(), -oof['momentum'].to_numpy())
    matrix, features = build_matrix(read_features([synthetic_path]), "BTC_spot")
    x = matrix.select(features).head(2).to_numpy().astype(float)
    assert lgb.Booster(model_file=str(directory / "regressor.txt")).predict(x).shape == (2,)
    assert lgb.Booster(model_file=str(directory / "classifier.txt")).predict(x).shape == (2, 3)
    assert set(report["metrics"]) == {"regressor", "classifier", "zero", "momentum", "reversal", "prior", "ridge"}
    from research.risk import STOP_NAMES
    assert set(report["stop_models"]) == set(STOP_NAMES)
    assert set(report["stop_metrics"]) == set(STOP_NAMES)
    for name in STOP_NAMES:
        assert lgb.Booster(model_file=str(directory / f"{name}.txt")).predict(x, num_threads=2).shape == (2,)
        assert report["stop_metrics"][name]["model"]["n"] > 0
        assert report["stop_metrics"][name]["volatility_scaled"]["n"] > 0
    # The momentum baseline uses unscaled 300-second returns only at h=300.
    from research.labels import make_labels
    data = matrix.join(make_labels(read_features([synthetic_path]), "BTC_spot", horizon), on="tick_ns").filter(
        pl.col("return_pct").is_not_null() & (pl.col("available_ns") < pl.col("label_end_ns")))
    folds = compact_folds(data["tick_ns"].to_numpy(), horizon,
                               label_available_ns=data["label_available_ns"].to_numpy(),
                               feature_available_ns=data["available_ns"].to_numpy())
    test = np.concatenate([test for _, test in folds])
    momentum = (data["momentum_return_300s"].to_numpy() if horizon == 300 else
                data["momentum_return_900s"].to_numpy() * horizon / 900)
    assert report["metrics"]["momentum"] == metrics(data["return_pct"].to_numpy()[test], momentum[test])
    assert report["metrics"]["zero"]["directional_accuracy"] == 0
    assert (output / "research_report_test.json").is_file()
    experiment = json.loads((output / 'experiments.jsonl').read_text())
    assert experiment['status'] == 'completed'
    assert experiment['results'][0]['status'] == 'trained'
    assert experiment['results'][0]['metrics'] == report['metrics']
    assert experiment['results'][0]['feature_count'] == len(report['features'])
    assert experiment['results'][0]['feature_sha256'] == hashlib.sha256(
        json.dumps(report['features'], separators=(',', ':')).encode()).hexdigest()
    assert experiment['engine_versions'] == ['test-v1']
    for fold in report["folds"]:
        assert fold["train_label_available_ns"] < fold["test_start_ns"]
        assert fold["train_label_end_ns"] < fold["test_start_ns"]
        assert fold["train_feature_available_ns"] < fold["test_start_ns"]
    with pytest.raises(FileExistsError):
        train_target(read_features([synthetic_path]), "BTC_spot", horizon, output,
                     "test", rounds=1, lockbox_from="1970-01-03")


def test_matrix_uses_explicit_asset_for_generic_pairs(tmp_path):
    rows = [row(0, symbol='USDTUSD', asset='USDT'), row(10, symbol='USDTUSD', asset='USDT')]
    frame = read_features([write_parquet(tmp_path, rows)])
    spec = load_spec()
    spec['references']['USDT'] = {'spot': {'exchange':'binance','market':'spot','symbol':'USDTUSD'}}
    matrix, features = build_matrix(frame, 'USDT_spot', spec=spec)
    assert matrix['asset'].to_list() == ['USDT','USDT']
    assert matrix['binance_spot_spread_bps'].to_list() == [2.,2.]
    assert not any(f.endswith('_asset') for f in features)


def test_missing_references_are_skipped_while_present_target_trains(tmp_path, synthetic_path, compact_folds):
    result = main([str(synthetic_path), "--output-root", str(tmp_path / "skipping"),
                   "--horizon-s", "900", "--lockbox-from", "1970-01-03", "--rounds", "1", "--version", "skip"])
    assert result["trained"] == 1 and result["skipped"] == 3
    assert result["results"][0]["target"] == "BTC_spot"
    assert result["results"][0]["status"] == "trained"
    for report in result["results"][1:]:
        assert report["status"] == "skipped"
        assert report["reason"] == "reference instrument has no rows in input"
        assert "model_dir" not in report


def test_no_targets_or_too_few_labels_is_a_reported_success(tmp_path, capsys):
    path = write_parquet(tmp_path, [row(0), row(900)])
    output = tmp_path / "no-labels"
    result = main([str(path), "--output-root", str(output), "--version", "tiny", "--lockbox-from", "1970-01-03"])
    assert result["trained"] == 0 and result["skipped"] == 12
    assert all(report["status"] == "skipped" for report in result["results"])
    assert "not enough labeled rows" in result["results"][0]["reason"]
    assert not (output / "models").exists()
    assert (output / "research_report_tiny.json").is_file()
    experiment = json.loads((output / 'experiments.jsonl').read_text())
    assert experiment['status'] == 'completed' and len(experiment['results']) == 12
    assert all(r['status'] == 'skipped' and r['reason'] for r in experiment['results'])
    assert "No targets could train" in capsys.readouterr().out
    only_perp = read_features([write_parquet(tmp_path, [row(0, exchange="kraken", market="perp",
                                                             symbol="PF_XBTUSD")], name="perp.parquet")])
    report = train_target(only_perp, "BTC_spot", 900, output, "absent", lockbox_from="1970-01-03")
    assert report["reference"] == dict(exchange="binance", market="spot", symbol="BTCUSDT")
    assert report["reason"] == "reference instrument has no rows in input"
    with pytest.raises(ValueError, match="stride/folds"):
        train_target(only_perp, "BTC_spot", 900, output, "bad", max_folds=0)


def test_context_assets_same_tick_premium_missing_nulls_and_no_future_leakage(tmp_path):
    data = [row(0), row(10), row(20)]
    data += [row(0, 1_000_250_000, symbol="USDTUSD", asset="USDT", spread_bps=1.,
                 available_ns=700_000_000),
             row(0, 999_900_000, exchange="okx", symbol="USDT-USD", asset="USDT"),
             row(11, 1_100_000_000, symbol="USDTUSD", asset="USDT", spread_bps=9999.)]
    frame = read_features([write_parquet(tmp_path, data)])
    matrix, features = build_matrix(frame, "BTC_spot")
    assert matrix["ctx_usdt_binance_spot_premium_bps"].to_list() == [2.5, None, None]
    assert matrix["ctx_usdt_okx_spot_premium_bps"].to_list() == [-1., None, None]
    assert matrix["ctx_usdt_binance_spot_spread_bps"].to_list() == [1., None, None]
    assert matrix["available_ns"].to_list() == [700_000_000, 10 * NS + 250_000_000, 20 * NS + 250_000_000]
    assert "ctx_usdt_binance_spot_premium_bps" in features
    assert not any(c.endswith("_mid_e9") or c.endswith("_available_ns") for c in features)
    future = frame.with_columns(pl.when(pl.col("tick_ns") == 11 * NS).then(1e20)
                                .otherwise(pl.col("spread_bps")).alias("spread_bps"))
    assert matrix.equals(build_matrix(future, "BTC_spot")[0])
    disabled = dict(load_spec(), context_assets=[])
    no_context, columns = build_matrix(frame, "BTC_spot", spec=disabled)
    assert not any(c.startswith("ctx_") for c in columns)
    assert no_context["available_ns"][0] == 250_000_000
    absent, _ = build_matrix(frame.filter(pl.col("symbol") == "BTCUSDT"), "BTC_spot")
    assert not any(c.startswith('ctx_usdt_') for c in absent.columns)


def test_context_assets_apply_mode_version_and_availability_guards(tmp_path):
    frame = read_features([write_parquet(tmp_path, [row(0), row(900),
        row(0, 1_000_000_000, symbol="USDTUSD", available_ns=901 * NS)])])
    matrix, _ = build_matrix(frame, "BTC_spot")
    assert matrix["available_ns"][0] == 901 * NS
    report = train_target(frame, "BTC_spot", 900, tmp_path, "late-context", lockbox_from="1970-01-03")
    assert report["status"] == "skipped" and report["labeled_rows"] == 0
    for column, value in [("replay_mode", "as_live"), ("engine_version", "other-version")]:
        conflicting = frame.with_columns(pl.when(pl.col("symbol") == "USDTUSD").then(pl.lit(value))
                                         .otherwise(pl.col(column)).alias(column))
        with pytest.raises(ValueError, match="Cannot mix"):
            build_matrix(conflicting, "BTC_spot")
    # Warm/invalid context books cannot supply a stale price premium.
    invalid = frame.with_columns(pl.when(pl.col("symbol") == "USDTUSD").then(False)
                                 .otherwise(pl.col("book_valid")).alias("book_valid"))
    assert build_matrix(invalid, "BTC_spot")[0]["ctx_usdt_binance_spot_premium_bps"].null_count() == 2


@pytest.fixture(scope='module', params=[False, True], ids=['absent-context', 'present-context'])
def lean_paths(tmp_path_factory, request):
    root = tmp_path_factory.mktemp('lean')
    midnight = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp())
    daily = {}
    # A dense interval across midnight; prices exceed Float64's exact integer range.
    for offset in range(-1000, 2001):
        second = midnight + offset
        day = datetime.fromtimestamp(second, timezone.utc).date().isoformat()
        rows = daily.setdefault(day, [])
        for asset in ('BTC', 'ETH'):
            for market in ('spot', 'perp'):
                if (asset, market, offset) in {('BTC', 'spot', -23), ('ETH', 'perp', 101)}:
                    continue
                rows.append(row(second, 2**53 + 1 + (offset % 151), symbol=f'{asset}USDT',
                                market=market, warmup=offset == -998, book_valid=offset != 641,
                                ret_60s=None if offset == 10 else offset / 1000,
                                available_ns=(second + (19 if offset == -900 else 0)) * NS + 123_456_789))
        rows.append(row(second, exchange='okx', symbol='BTC-USDT', spread_bps=3.))
        rows.append(row(second, exchange='cross', market='cross', symbol='BTC', asset='',
                        available_ns=second * NS + 987_654_321))
        if request.param:
            rows.append(row(second, NS + 5, symbol='USDTUSD', asset='USDT'))
    return [write_parquet(root / 'features' / f'date={day}', rows)
            for day, rows in daily.items()]


@pytest.mark.parametrize('target', ['BTC_spot', 'BTC_perp', 'ETH_spot', 'ETH_perp'])
@pytest.mark.parametrize('horizon', [300, 900, 1800])
@pytest.mark.parametrize('multi_target', [False, True], ids=['single', 'multi'])
def test_lean_matrix_labels_momentum_and_provenance_equal_full(lean_paths, target, horizon, multi_target):
    spec = dict(load_spec())
    spec.pop('calendar')
    targets = ['BTC_spot', 'BTC_perp', 'ETH_spot', 'ETH_perp'] if multi_target else [target]
    full = read_features(lean_paths, targets, spec)
    lean = read_features(lean_paths, targets, spec, stride_s=10)
    # Every reference remains dense; only its non-stride feature payload is null.
    for requested in targets:
        ref = reference_for(requested, spec)
        keep = pl.all_horizontal([pl.col(k) == v for k, v in ref.items()])
        assert_frame_equal(full.filter(keep).select(LABEL_INPUTS).sort('tick_ns'),
                           lean.filter(keep).select(LABEL_INPUTS).sort('tick_ns'), check_exact=True)
    dense = lean.filter(pl.col('tick_ns') % (10 * NS) != 0)
    assert dense['ret_60s'].null_count() == dense.height
    assert lean.height < full.height
    expected, full_features = build_matrix(full, target, spec=spec)
    actual, lean_features = build_matrix(lean, target, spec=spec)
    assert lean_features == full_features == sorted(full_features)
    assert_frame_equal(actual, expected, check_exact=True)
    # Includes integer endpoint provenance, directions and all four excursions.
    expected_labels = make_labels(full, target, horizon, spec)
    actual_labels = make_labels(lean, target, horizon, spec)
    assert_frame_equal(actual_labels, expected_labels, check_exact=True)
    assert expected_labels['return_pct'].drop_nulls().len() > 0
    assert expected_labels['adverse_long_bps'].null_count() > 0
    from research.evaluation import feature_array
    x = feature_array(actual, lean_features)
    assert x.dtype == np.float64
    np.testing.assert_array_equal(x, feature_array(expected, full_features))


@pytest.mark.parametrize('kind', ['dense', 'stride', 'overlap'])
def test_lean_rejects_duplicate_keys(tmp_path, kind):
    duplicate = row(1) if kind != 'stride' else row(10, exchange='okx', symbol='BTC-USDT')
    paths = [write_parquet(tmp_path, [row(0), duplicate, *([] if kind == 'overlap' else [duplicate])])]
    if kind == 'overlap':
        paths.append(write_parquet(tmp_path, [duplicate], name='overlap.parquet'))
    with pytest.raises(ValueError, match='Duplicate'):
        read_features(paths, ['BTC_spot'], stride_s=10)


@pytest.mark.parametrize('changes,message', [
    ({'tick_ns': NS + 1}, 'whole seconds'),
    ({'available_ns': NS - 1}, 'whole seconds'),
    ({'engine_version': 'wrong'}, 'Engine version disagrees'),
])
def test_lean_validates_dense_reference_rows(tmp_path, changes, message):
    path = write_parquet(tmp_path, [row(0), row(1, **changes)])
    with pytest.raises(ValueError, match=message):
        read_features([path], ['BTC_spot'], stride_s=10)


def test_context_presence_fills_only_observed_assets_missing_venues(tmp_path):
    rows = [row(0), row(10), row(0, exchange='okx', symbol='BTC-USDT')]
    rows += [row(10, NS, symbol='USDTUSD')]
    full = read_features([write_parquet(tmp_path, rows)])
    for frame in (full, full.filter(pl.col('symbol') != 'USDTUSD')):
        matrix, features = build_matrix(frame, 'BTC_spot')
        present = frame.filter(pl.col('symbol') == 'USDTUSD').height > 0
        assert any(c.startswith('ctx_usdt_') for c in features) == present
        if present:
            assert matrix['ctx_usdt_okx_spot_premium_bps'].null_count() == matrix.height
            assert matrix['ctx_usdt_binance_spot_premium_bps'].to_list() == [None, 0.]
    # Presence is about input rows, even if none coincide with eligible matrix ticks.
    off_stride = full.with_columns(pl.when(pl.col('symbol') == 'USDTUSD')
                                  .then(pl.col('tick_ns') + NS).otherwise(pl.col('tick_ns')).alias('tick_ns'))
    matrix, _ = build_matrix(off_stride, 'BTC_spot')
    assert matrix['ctx_usdt_binance_spot_premium_bps'].null_count() == matrix.height


def test_cli_splits_paths_before_loading_and_trains_same_reports(tmp_path, synthetic_path,
                                                               compact_folds, monkeypatch, capsys):
    from research.evaluation import DAY_NS, date_split
    start = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()) * NS
    raw = pl.read_parquet(synthetic_path)
    for i in range(2):
        part = raw.slice(i * 5400, 5400).with_columns(
            (pl.col('tick_ns') - i * 5400 * NS + start + i * DAY_NS).alias('tick_ns'),
            (pl.col('available_ns') - i * 5400 * NS + start + i * DAY_NS).alias('available_ns'))
        write_parquet(tmp_path / 'features' / f'date=2026-01-{i+1:02d}', part.to_dicts())
    for i in (3, 4):
        write_parquet(tmp_path / 'features' / f'date=2026-01-{i:02d}',
                      [row(start // NS + (i - 1) * 86400)])
    paths = feature_paths(tmp_path)
    full = read_features(paths, ['BTC_spot'])
    expected_split = date_split(full['tick_ns'].to_numpy(), lockbox_from='2026-01-04',
                                input_days=feature_dates(paths, full['tick_ns'].to_numpy()))
    assert date_split([], lockbox_from='2026-01-04', input_days=feature_dates(paths)) == expected_split
    original = read_features
    seen = []

    def check_read(paths, *args, **kwargs):
        assert [p.parent.name for p in paths] == ['date=2026-01-01', 'date=2026-01-02']
        assert kwargs['before_ns'] == expected_split['development_end_ns']
        loaded = original(paths, *args, **kwargs)
        assert loaded['tick_ns'].max() < expected_split['development_end_ns']
        seen.append(loaded.height)
        return loaded

    monkeypatch.setattr('research.train.read_features', check_read)
    result = main([str(tmp_path), '--target', 'BTC_spot', '--horizon-s', '900', '--rounds', '2',
                   '--threads', '2', '--version', 'lean', '--lockbox-from', '2026-01-04'])
    report = result['results'][0]
    assert result['trained'] == 1 and result['input_rows'] == seen[0] < full.height
    assert report['date_split'] == expected_split
    baseline = train_target(full, 'BTC_spot', 900, tmp_path, 'full', rounds=2,
                            split=expected_split, num_threads=2)
    assert set(report) == set(baseline)
    for key in ('features', 'metrics', 'stop_metrics', 'matrix_rows', 'labeled_rows', 'folds'):
        assert report[key] == baseline[key]
    assert json.loads((tmp_path / 'research_report_lean.json').read_text()) == result
    assert set(result) == {'input_files', 'input_rows', 'results', 'trained', 'skipped'}
    output = capsys.readouterr().out
    for stage in ('loading', 'matrix/labels', 'fitting'):
        assert f'Peak RSS after {stage}' in output


def test_lockbox_cli_loads_union_of_saved_strides_and_dense_references(tmp_path, monkeypatch):
    for market, horizon, stride in [('spot', 900, 6), ('perp', 300, 10)]:
        directory = tmp_path / 'models' / f'BTC_{market}' / str(horizon) / 'saved'
        directory.mkdir(parents=True)
        (directory / 'metadata.json').write_text(json.dumps(dict(
            target=f'BTC_{market}', horizon_s=horizon, stride_s=stride, label_spec=load_spec())))
    for day in (1, 2):
        second = int(datetime(2026, 1, day, tzinfo=timezone.utc).timestamp())
        write_parquet(tmp_path / 'features' / f'date=2026-01-{day:02d}',
                      [row(second + t, market=market) for t in range(21) for market in ('spot', 'perp')])
    observed = []

    def evaluate(rows, output, version, targets, horizons, *args, **kwargs):
        assert output == tmp_path and version == 'saved'
        assert kwargs['input_days'] == ['2026-01-01', '2026-01-02']
        for market in ('spot', 'perp'):
            ref = rows.filter(pl.col('market') == market)
            assert ref.height == 42
            for stride in (6, 10):
                ticks = ref.filter(pl.col('tick_ns') % (stride * NS) == 0)
                assert ticks['ret_60s'].null_count() == 0
            dense = ref.filter(pl.col('tick_ns') % (2 * NS) != 0)
            assert dense['ret_60s'].null_count() == dense.height
        observed.append(rows.height)
        return {'evaluated': True}

    monkeypatch.setattr('research.train.open_lockbox', evaluate)
    assert main([str(tmp_path), '--version', 'saved', '--open-lockbox', '--stride-s', '7']) == {'evaluated': True}
    assert observed == [84]


@pytest.mark.parametrize('platform,peak', [('darwin', 5 * 1024**3 // 4), ('linux', 5 * 1024**2 // 4)])
def test_peak_rss_uses_platform_units(platform, peak, monkeypatch, capsys):
    from types import SimpleNamespace
    from research.train import print_peak_rss
    monkeypatch.setattr('research.train.sys.platform', platform)
    monkeypatch.setattr('research.train.resource.getrusage', lambda who: SimpleNamespace(ru_maxrss=peak))
    print_peak_rss('loading')
    assert capsys.readouterr().out == 'Peak RSS after loading: 1.250 GiB\n'


def test_experiment_ledger_records_failed_invocation_when_git_is_unavailable(tmp_path, monkeypatch):
    path = write_parquet(tmp_path, [row(0), row(900)])
    output = tmp_path / 'output'

    def fail_read(*args, **kwargs):
        raise ValueError('invalid fixture')

    def no_git(*args, **kwargs):
        raise FileNotFoundError('git missing')

    monkeypatch.setattr('research.train.read_features', fail_read)
    monkeypatch.setattr('research.train.subprocess.run', no_git)
    argv = [str(path), '--output-root', str(output), '--version', 'failed',
            '--target', 'BTC_spot', '--horizon-s', '900']
    for _ in range(2):
        with pytest.raises(ValueError, match='invalid fixture'):
            main(argv)
    entries = [json.loads(line) for line in (output / 'experiments.jsonl').read_text().splitlines()]
    assert len(entries) == 2
    for entry in entries:
        assert entry['git'] == {'sha': None, 'dirty': None}
        assert entry['status'] == 'failed' and entry['argv'] == argv
        assert entry['results'][0]['status'] == 'failed'
        assert entry['results'][0]['reason'] == 'ValueError: invalid fixture'


def test_kraken_label_spec_native_symbols_calendar_and_cli(tmp_path):
    spec_path = Path(__file__).resolve().parents[1] / 'research/targets_kraken.yaml'
    spec = load_spec(spec_path)
    assert spec['version'] == 'v1-kraken-spot'
    assert spec['calendar'] == load_spec()['calendar']
    assert spec['horizons_s'] == load_spec()['horizons_s']
    assert spec['context_assets'] == []
    assert spec['references'] == {
        asset: {'spot': {'exchange': 'kraken', 'market': 'spot', 'symbol': f'{asset}/USD'}}
        for asset in ('BTC', 'ETH')}
    path = write_parquet(tmp_path, [row(t, exchange='kraken', symbol='BTC/USD') for t in (0, 300)])
    rows = read_features([path], targets=['BTC_spot'], spec=spec, stride_s=10)
    assert make_labels(rows, 'BTC_spot', 300, spec)['return_pct'][0] == 0
    result = main([str(path), '--label-spec', str(spec_path), '--version', 'kraken', '--threads', '2',
                   '--train-from', '1970-01-01'])
    assert len(result['results']) == 6
    experiment = json.loads((tmp_path / 'experiments.jsonl').read_text())
    assert experiment['train_from'] == '1970-01-01'
    assert all(item['train_from'] == '1970-01-01' for item in experiment['results'])
    for report in result['results']:
        assert report['label_spec'] == spec
        assert report['reference']['exchange'] == 'kraken'
        assert report['target'].endswith('_spot')


@pytest.mark.parametrize('mode', ['ideal+exchange_clock_0ms', 'ideal+exchange_clock_35ms'])
def test_exchange_clock_provenance_is_accepted_and_preserved(tmp_path, mode):
    path = write_parquet(tmp_path, [row(0), row(10)], mode)
    rows = read_features([path])
    matrix, _ = build_matrix(rows, 'BTC_spot')
    assert matrix['replay_mode'].to_list() == [mode, mode]


def test_exchange_clock_modes_cannot_mix_across_days_or_hidden_lockbox_paths(tmp_path):
    from research.dataset import validate_replay_modes
    paths = [write_parquet(tmp_path, [row(0)], 'ideal+exchange_clock_35ms', 'a.parquet'),
             write_parquet(tmp_path, [row(86400)], 'ideal+exchange_clock_36ms', 'b.parquet')]
    with pytest.raises(ValueError, match='Cannot mix replay modes'):
        validate_replay_modes(paths)
    with pytest.raises(ValueError, match='Cannot mix replay modes'):
        read_features(paths, before_ns=86400 * NS)


def test_skipped_json_is_excluded_from_feature_paths_and_dates(tmp_path):
    skip = tmp_path / 'features/date=2025-11-11/skipped.json'
    skip.parent.mkdir(parents=True)
    skip.write_text(json.dumps({'reason': 'marker_only_inputs', 'files': ['level3.jsonl.gz']}))
    feature = write_parquet(tmp_path / 'features/date=2025-11-08', [row(0)])
    assert feature_paths(tmp_path) == [feature]
    assert feature_dates(feature_paths(tmp_path)) == ['2025-11-08']


def test_train_from_leaves_latest_twelve_fold_blocks_and_test_rows_unchanged():
    from research.evaluation import DAY_NS
    start = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp()) * NS
    ticks = start + np.arange(180 * 24, dtype=np.int64) * 3600 * NS
    original = walk_forward_folds(ticks, 300)
    trimmed = walk_forward_folds(ticks, 300, train_from='2025-02-01')
    assert len(original) == len(trimmed) == 12
    # Placement stays on the original start+14 day grid; select its last 12 blocks.
    assert ticks[original[0][1][0]] == start + 98 * DAY_NS
    cutoff = int(datetime(2025, 2, 1, tzinfo=timezone.utc).timestamp()) * NS
    for (train, test), (recent_train, recent_test) in zip(original, trimmed):
        np.testing.assert_array_equal(test, recent_test)
        np.testing.assert_array_equal(recent_train, train[ticks[train] >= cutoff])
        assert len(recent_train) < len(train)


@pytest.mark.parametrize('value', ['20250101', '2025-02-30', '2025-1-01'])
def test_invalid_train_from_cli_errors_before_loading(value):
    with pytest.raises(SystemExit, match='2'):
        main(['unused', '--train-from', value])
