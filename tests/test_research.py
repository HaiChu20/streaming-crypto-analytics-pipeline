"""Offline research contracts: exact labels, same-tick features, purged folds, native LGBM."""

from fractions import Fraction
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest

from research.dataset import build_matrix, read_features
from research.labels import NS, exact_return_pct, load_spec, make_labels, momentum_900s
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


def test_provenance_records_mode_per_row_across_files(tmp_path):
    paths = [write_parquet(tmp_path, [row(0)], "ideal", "a.parquet"),
             write_parquet(tmp_path, [row(10)], "as_live", "b.parquet")]
    matrix, _ = build_matrix(read_features(paths), "BTC_spot")
    assert matrix["replay_mode"].to_list() == ["ideal", "as_live"]


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
    ticks = np.arange(0, 30000, 10, dtype=np.int64) * NS
    # Some labels are emitted even later than the normal endpoint + 250ms.
    available = ticks + horizon * NS + 125 * NS
    folds = walk_forward_folds(ticks, horizon, 3, label_available_ns=available)
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
    ticks = np.arange(0, 30000, 10, dtype=np.int64) * NS
    available = ticks + 250_000_000
    available[0] = 40000 * NS
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
def test_tiny_end_to_end_cli_models_metadata_and_row_modes(tmp_path, synthetic_path, horizon):
    output = tmp_path / "output"
    result = main([str(synthetic_path), "--output-root", str(output), "--target", "BTC_spot",
                   "--horizon-s", str(horizon), "--folds", "2", "--rounds", "3", "--version", "test"])
    report = result["results"][0]
    assert report["labeled_rows"] > 0 and len(report["folds"]) == 2
    assert report["replay_modes"] == ["ideal"]
    directory = Path(report["model_dir"])
    metadata = json.loads((directory / "metadata.json").read_text())
    assert metadata["label_spec_version"] == "v1"
    assert metadata["engine_version"] == ["test-v1"]
    audit = pl.read_parquet(directory / "training_rows.parquet")
    assert audit.height == report["labeled_rows"]
    assert audit["replay_mode"].unique().to_list() == ["ideal"]
    matrix, features = build_matrix(read_features([synthetic_path]), "BTC_spot")
    x = matrix.select(features).head(2).to_numpy().astype(float)
    assert lgb.Booster(model_file=str(directory / "regressor.txt")).predict(x).shape == (2,)
    assert lgb.Booster(model_file=str(directory / "classifier.txt")).predict(x).shape == (2, 3)
    assert set(report["metrics"]) == {"regressor", "classifier", "zero", "momentum"}
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
    folds = walk_forward_folds(data["tick_ns"].to_numpy(), horizon, 2,
                               label_available_ns=data["label_available_ns"].to_numpy(),
                               feature_available_ns=data["available_ns"].to_numpy())
    test = np.concatenate([test for _, test in folds])
    momentum = (data["momentum_return_300s"].to_numpy() if horizon == 300 else
                data["momentum_return_900s"].to_numpy() * horizon / 900)
    assert report["metrics"]["momentum"] == metrics(data["return_pct"].to_numpy()[test], momentum[test])
    assert report["metrics"]["zero"]["directional_accuracy"] == 0
    assert (output / "research_report_test.json").is_file()
    for fold in report["folds"]:
        assert fold["train_label_available_ns"] < fold["test_start_ns"]
        assert fold["train_label_end_ns"] < fold["test_start_ns"]
        assert fold["train_feature_available_ns"] < fold["test_start_ns"]
    with pytest.raises(FileExistsError):
        train_target(read_features([synthetic_path]), "BTC_spot", horizon, output,
                     "test", n_folds=2, rounds=1)


def test_matrix_uses_explicit_asset_for_generic_pairs(tmp_path):
    rows = [row(0, symbol='USDTUSD', asset='USDT'), row(10, symbol='USDTUSD', asset='USDT')]
    frame = read_features([write_parquet(tmp_path, rows)])
    spec = load_spec()
    spec['references']['USDT'] = {'spot': {'exchange':'binance','market':'spot','symbol':'USDTUSD'}}
    matrix, features = build_matrix(frame, 'USDT_spot', spec=spec)
    assert matrix['asset'].to_list() == ['USDT','USDT']
    assert matrix['binance_spot_spread_bps'].to_list() == [2.,2.]
    assert not any(f.endswith('_asset') for f in features)


def test_missing_references_are_skipped_while_present_target_trains(tmp_path, synthetic_path):
    result = main([str(synthetic_path), "--output-root", str(tmp_path / "skipping"),
                   "--horizon-s", "900", "--folds", "2", "--rounds", "1", "--version", "skip"])
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
    result = main([str(path), "--output-root", str(output), "--version", "tiny"])
    assert result["trained"] == 0 and result["skipped"] == 12
    assert all(report["status"] == "skipped" for report in result["results"])
    assert "not enough labeled rows" in result["results"][0]["reason"]
    assert not (output / "models").exists()
    assert (output / "research_report_tiny.json").is_file()
    assert "No targets could train" in capsys.readouterr().out
    only_perp = read_features([write_parquet(tmp_path, [row(0, exchange="kraken", market="perp",
                                                             symbol="PF_XBTUSD")], name="perp.parquet")])
    report = train_target(only_perp, "BTC_spot", 900, output, "absent")
    assert report["reference"] == dict(exchange="binance", market="spot", symbol="BTCUSDT")
    assert report["reason"] == "reference instrument has no rows in input"
    with pytest.raises(ValueError, match="stride/folds"):
        train_target(only_perp, "BTC_spot", 900, output, "bad", n_folds=0)


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
    assert absent["ctx_usdt_binance_spot_premium_bps"].null_count() == 3


def test_context_assets_apply_mode_version_and_availability_guards(tmp_path):
    frame = read_features([write_parquet(tmp_path, [row(0), row(900),
        row(0, 1_000_000_000, symbol="USDTUSD", available_ns=901 * NS)])])
    matrix, _ = build_matrix(frame, "BTC_spot")
    assert matrix["available_ns"][0] == 901 * NS
    report = train_target(frame, "BTC_spot", 900, tmp_path, "late-context")
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
