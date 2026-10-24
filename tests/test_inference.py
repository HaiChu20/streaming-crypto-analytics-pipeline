"""Online/offline vector parity, anomaly isolation and native-model inference."""
import json
from pathlib import Path
import re
import subprocess
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest

from inference.service import Model, SCHEMA, Service, load_models, vector_for
from research.dataset import build_matrix
from research.labels import NS, load_spec
from research.train import fit_models, train_target


def row(second, **updates):
    result = dict.fromkeys(SCHEMA)
    result.update(exchange="binance", market="spot", symbol="BTCUSDT", asset="", engine_version="test",
                  replay_mode="as_live", tick_ns=second * NS, available_ns=second * NS + 250_000_000,
                  book_valid=True, warmup=False, mid_e9=100_000_000_005 + second * 100,
                  spread_bps=2., ret_60s=0.01)
    result.update(updates)
    return result


def metadata(features=None):
    spec = load_spec()
    spec.pop("calendar", None)  # Legacy model fixtures use epoch-0 ticks.
    return dict(target="BTC_spot", horizon_s=900, version="test", stride_s=10,
                label_spec=spec, features=features or ["binance_spot_spread_bps"],
                engine_version=["test"], replay_modes=["as_live"], classifier_class_order=[-1, 0, 1])


class Producer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, **kwargs):
        self.messages.append((topic, kwargs))


def service():
    reg = SimpleNamespace(predict=lambda x, **kw: np.array([.2]))
    clf = SimpleNamespace(predict=lambda x, **kw: np.array([[.1, .2, .7]]))
    return Service([Model(metadata(), reg, clf)], Producer())


def feed(svc, *rows):
    for r in rows:
        svc.ingest(json.dumps(r))


def test_parity_full_multinstrument_frame_and_online_ticks():
    rows = []
    for t in [0, 10, 899, 900, 910, 1800, 1810]:
        rows.append(row(t, warmup=t == 0, ret_60s=None if t == 910 else .01))
        rows.append(row(t, market="perp", spread_bps=3.))
        if t != 910:
            rows.append(row(t, exchange="okx", symbol="BTC-USDT", spread_bps=4.))
        rows.append(row(t, exchange="cross", market="cross", symbol="BTC", basis_bps_binance=1.5,
                        lead_kp_ks_gap_dev_60s=t * .01, lead_kp_bp_gap_chg_5s=-.25))
        rows.append(row(t, symbol="ETHUSDT", spread_bps=999.))
    frame = pl.DataFrame(rows, schema=SCHEMA)
    matrix, features = build_matrix(frame, "BTC_spot", spec=metadata()["label_spec"])
    # Deliberately use metadata order and include a producible but absent instrument.
    features = list(reversed(features)) + ["kraken_spot_spread_bps"]
    meta = metadata(features)
    captured = {}
    svc = service()
    svc.models[0].metadata = meta
    svc.models[0].regressor = SimpleNamespace(predict=lambda x, **kw: (captured.update({svc.tick: x.copy()}) or np.array([.2])))
    feed(svc, *rows, row(1811))
    for tick in matrix["tick_ns"]:
        current = [r for r in rows if r["tick_ns"] == tick]
        history = [r for r in rows if tick - 901 * NS <= r["tick_ns"] < tick
                   and r["exchange"] == "binance" and r["market"] == "spot" and r["symbol"] == "BTCUSDT"]
        actual = vector_for(current, history, meta)
        expected = matrix.filter(pl.col("tick_ns") == tick).with_columns(
            pl.lit(None).alias("kraken_spot_spread_bps")).select(features).to_numpy().astype(np.float64)
        np.testing.assert_array_equal(actual, expected)
        np.testing.assert_array_equal(captured[tick], expected)
    assert np.isnan(captured[910 * NS][0, features.index("okx_spot_spread_bps")])
    assert np.isfinite(captured[900 * NS][0, features.index("momentum_return_900s")])
    assert all(r["exchange"] == "binance" and r["market"] == "spot" for r in svc.history)
    assert min(r["tick_ns"] for r in svc.history) >= (1810 - 901) * NS


def test_completion_stride_skips_and_schema():
    svc = service()
    feed(svc, row(0), row(1))
    assert svc.counts["ticks"] == 1 and len(svc.producer.messages) == 1
    feed(svc, row(10, warmup=True), row(20, book_valid=False), row(30, mid_e9=None),
         row(40, market="perp"), row(50), row(51))
    assert svc.skipped == dict(stride=1, warm_reference=1, invalid_reference=2, missing_reference=1)
    assert svc.counts["predictions"] == 2
    topic, message = svc.producer.messages[-1]
    payload = json.loads(message["value"])
    sql = Path("docker/dashboard/clickhouse/init/001-crypto.sql").read_text()
    schema = re.search(r"CREATE TABLE IF NOT EXISTS crypto.predictions_kafka\s*\((.*?)\)\s*ENGINE", sql, re.S)[1]
    expected = {line.strip().split()[0] for line in schema.strip().splitlines()}
    assert set(payload) == expected
    assert topic == "predictions.v1" and message["key"] == "BTC_spot" and message["partition"] == 0
    assert type(payload["pred_return_pct"]) is float and payload["direction"] == 1
    assert all(type(payload[k]) is int for k in ("horizon_s", "tick_ns", "available_ns", "direction"))
    assert all(type(payload[k]) is str for k in ("model_version", "target"))
    assert payload["available_ns"] >= row(50)["available_ns"]


@pytest.mark.parametrize("payload", [b"broken", b"null", b"[]", b"{}", b"\xff",
    json.dumps(row(0, surprise=1)), json.dumps(row(0, spread_bps=float("nan"))),
    json.dumps(row(0, mid_e9="bad")), json.dumps(row(0, tick_ns=1)),
    json.dumps(row(0, available_ns=-1)), json.dumps(row(0, warmup=None)),
    json.dumps(row(0, tick_ns=2**100))])
def test_malformed_does_not_stop_stream(payload):
    svc = service()
    svc.ingest(payload)
    feed(svc, row(10), row(11))
    assert svc.counts["malformed"] == 1
    assert svc.counts["predictions"] == 1


def test_duplicates_late_rows_inference_errors_and_future_availability(caplog):
    svc = service()
    feed(svc, row(10), row(10), row(0), row(11))
    assert svc.counts["malformed"] == 2 and svc.counts["predictions"] == 1
    svc.models[0].regressor = SimpleNamespace(predict=lambda *a, **kw: np.array([np.nan]))
    feed(svc, row(20), row(21))
    assert svc.skipped["inference_error"] == 1
    svc.models[0].regressor = SimpleNamespace(predict=lambda *a, **kw: np.array([.1]))
    feed(svc, row(30, available_ns=2**62), row(31), row(40), row(41))
    assert svc.skipped["future_available"] == 1 and svc.counts["predictions"] == 2
    svc.models[0].metadata["replay_modes"] = ["ideal"]
    feed(svc, row(50), row(51), row(60), row(61))
    assert sum("differs from live" in m for m in caplog.messages) == 1


@pytest.mark.parametrize('legacy_context', [False, True], ids=['current', 'legacy-context'])
def test_load_and_predict_tiny_native_models(tmp_path, legacy_context):
    features = (['ctx_usdt_okx_spot_premium_bps', 'binance_spot_spread_bps',
                 'ctx_usdt_binance_spot_spread_bps'] if legacy_context else None)
    meta = metadata(features)
    x = np.arange(120).reshape(-1, 1).astype(float)
    y = np.sin(x[:, 0])
    if legacy_context:
        x = np.column_stack([np.full(120, np.nan), x, np.full(120, np.nan)])
    reg, clf, _ = fit_models(x, y, np.sign(y).astype(int), meta["features"], 3)
    path = tmp_path / "models/BTC_spot/900/test"
    path.mkdir(parents=True)
    reg.save_model(str(path / "regressor.txt"))
    clf.save_model(str(path / "classifier.txt"))
    (path / "metadata.json").write_text(json.dumps(meta))
    models = load_models(tmp_path, "test", ["BTC_spot"], [900])
    svc = Service(models, Producer())
    feed(svc, row(10), row(11))
    assert svc.counts["predictions"] == 1
    vector = vector_for([row(10)], [], meta)
    if legacy_context:
        assert vector.dtype == np.float64
        np.testing.assert_array_equal(vector, [[np.nan, 2., np.nan]])
    expected = float(reg.predict(vector, num_threads=2)[0])
    payload = json.loads(svc.producer.messages[0][1]["value"])
    assert payload["pred_return_pct"] == expected
    assert models[0].stops == {}
    assert all(value is None for key, value in payload.items() if key.startswith("stop_"))
    meta["features"] = ["impossible"]
    (path / "metadata.json").write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="cannot produce"):
        load_models(tmp_path, "test")


def test_pipeline_lifecycle_in_isolated_directory(tmp_path):
    # Fake runtime commands; never launch collectors, touch the live tree, or signal a PID.
    import os
    script = tmp_path / "scripts/pipeline.sh"
    script.parent.mkdir()
    script.write_text(Path("scripts/pipeline.sh").read_text())
    runtime = tmp_path / ".venv/bin/python"
    runtime.parent.mkdir(parents=True)
    runtime.write_text('#!/bin/bash\nexit 0\n')
    runtime.chmod(0o755)
    env = {**os.environ, "PYTHON_DOTENV_DISABLED": "1", "INFERENCE_MODEL_VERSION": ""}
    result = subprocess.run(["bash", str(script), "status"], env=env, capture_output=True, text=True, check=True)
    assert "inference" not in result.stdout
    env["INFERENCE_MODEL_VERSION"] = "test"
    result = subprocess.run(["bash", str(script), "status"], env=env, capture_output=True, text=True, check=True)
    assert "inference: stopped" in result.stdout
    # Limit the isolated start to inference and replace sleep/caffeinate with no-ops.
    script.write_text(script.read_text().replace('NAMES=(binance okx kraken deribit recorder)', 'NAMES=()')
                      .replace('nohup caffeinate -s -w "$(cat data/state/recorder.pid)" >/dev/null 2>&1 &', ':'))
    result = subprocess.run(["bash", str(script), "start"], env=env, capture_output=True, text=True)
    assert "started inference" in result.stdout and result.returncode == 1
    assert "FAILED to start inference" in result.stdout
    result = subprocess.run(["bash", str(script), "stop"], env=env, capture_output=True, text=True, check=True)
    assert not result.stdout


def test_main_anomaly_recovery_latest_group_and_signal_flush(monkeypatch):
    import inference.service as module
    configurations, handlers, closed, flushed = [], {}, [], []
    messages = iter([b"invalid", json.dumps(row(10)), json.dumps(row(11))])
    producer = Producer()
    producer.poll = lambda timeout: None
    producer.flush = lambda timeout: flushed.append(timeout) or 0

    class Consumer:
        def __init__(self, config):
            configurations.append(config)

        def subscribe(self, topics):
            assert topics == ["features.1s.v1"]

        def poll(self, timeout):
            try:
                payload = next(messages)
                return SimpleNamespace(error=lambda: None, value=lambda: payload)
            except StopIteration:
                handlers[module.signal.SIGINT](None, None)
                return None

        def close(self):
            closed.append(True)

    monkeypatch.setattr(module, "Consumer", Consumer)
    monkeypatch.setattr(module, "Producer", lambda cfg: producer)
    monkeypatch.setattr(module, "load_models", lambda *args: service().models)
    monkeypatch.setattr(module.signal, "signal", lambda sig, handler: handlers.update({sig: handler}))
    module.main(["--version", "test"])
    assert len(producer.messages) == 1 and closed and flushed == [10]
    assert configurations[0]["auto.offset.reset"] == "latest"
    assert configurations[0]["group.id"].startswith("inference-")
    assert module.signal.SIGTERM in handlers


def test_dashboard_preserves_panels_and_fixes_versioned_references():
    from research.labels import reference_for
    panels = json.loads(Path("dashboards/market.json").read_text())["panels"]
    assert len({p["id"] for p in panels}) == len(panels)
    for panel in panels[-2:]:
        sql = panel["targets"][0]["rawSql"]
        assert "crypto.features FINAL" in sql and "crypto.predictions AS p FINAL" in sql
        assert "argMax(mid_e9" in sql and "finish.tick_ns = p.tick_ns +" in sql
        for target in ("BTC_spot", "BTC_perp", "ETH_spot", "ETH_perp"):
            ref = reference_for(target)
            assert f"market = '{ref['market']}' AND symbol = '{ref['symbol']}', '{target}'" in sql
    assert "avg(toFloat64(p.direction = sign(" in panels[-1]["targets"][0]["rawSql"]
    assert "finish.reference_mid != start.reference_mid" in panels[-1]["targets"][0]["rawSql"]


def test_live_schema_matches_rust_row_fields():
    rust = Path("engine/core/src/row.rs").read_text()
    base_fields = set(re.findall(r"pub (\w+):", rust)) - {"memory"}
    parquet = Path("engine/replay/src/output.rs").read_text().split('const SCHEMA:')[1]
    fields = set(re.findall(r"(?:REQUIRED|OPTIONAL) \w+ (\w+)", parquet))
    assert base_fields <= fields
    assert fields == set(SCHEMA) - {"replay_mode"}


def test_explicit_download_filters_artifacts_and_preserves_existing(tmp_path, monkeypatch):
    import boto3
    from inference.service import download_models
    keys = ["models/BTC_spot/900/test/" + name for name in
            ("metadata.json", "regressor.txt", "classifier.txt", "stop_long_q80.txt", "stop_long_q90.txt",
             "stop_short_q80.txt", "stop_short_q90.txt", "training_rows.parquet")]
    keys += ["models/BTC_spot/1800/test/metadata.json", "models/ETH_spot/900/test/metadata.json",
             "models/BTC_spot/900/other/metadata.json", "models/../../test/metadata.json"]
    calls = []
    paginator = SimpleNamespace(paginate=lambda **kw: [{"Contents": [{"Key": k} for k in keys]}])

    def download(bucket, key, output):
        calls.append((bucket, key))
        output.write(b"fixture")

    client = SimpleNamespace(get_paginator=lambda name: paginator, download_fileobj=download)
    monkeypatch.setenv("S3_BUCKET", "fixture-bucket")
    monkeypatch.setattr(boto3, "client", lambda name: client)
    download_models(tmp_path, "test", ["BTC_spot"], [900])
    assert {key for _, key in calls} == set(keys[:7])
    with pytest.raises(FileExistsError):
        download_models(tmp_path, "test", ["BTC_spot"], [900])


def test_matrices_shared_across_horizons_reindexed_per_model_and_reset_per_tick(monkeypatch):
    import inference.service as module
    original = module.build_matrix
    builds, vectors = [], []

    def build(rows, target, stride, spec):
        builds.append((target, stride, spec['version']))
        return original(rows, target, stride, spec)

    def predict(x, **kwargs):
        assert kwargs == {'num_threads': 1}
        vectors.append(x.copy())
        return np.array([.2])

    def classify(x, **kwargs):
        assert kwargs == {'num_threads': 1}
        return np.array([[.1, .2, .7]])

    monkeypatch.setattr(module, 'build_matrix', build)
    first = metadata(['binance_spot_spread_bps', 'binance_spot_ret_60s'])
    second = {**first, 'horizon_s': 1800,
              'features': ['binance_spot_ret_60s', 'kraken_spot_spread_bps', 'binance_spot_spread_bps']}
    models = [Model(meta, SimpleNamespace(predict=predict), SimpleNamespace(predict=classify))
              for meta in (first, second)]
    svc = Service(models, Producer())
    feed(svc, row(10), row(11), row(20, spread_bps=3.), row(21))
    assert builds == [('BTC_spot', 10, 'v1')] * 2
    assert svc.counts['predictions'] == 4
    np.testing.assert_array_equal(vectors[0], np.array([[2., .01]], dtype=np.float64))
    np.testing.assert_array_equal(vectors[1], np.array([[.01, np.nan, 2.]], dtype=np.float64))
    np.testing.assert_array_equal(vectors[2], np.array([[3., .01]], dtype=np.float64))
    np.testing.assert_array_equal(vectors[3], np.array([[.01, np.nan, 3.]], dtype=np.float64))


@pytest.mark.parametrize('change', [{'target': 'BTC_perp'}, {'stride_s': 20},
                                   {'label_spec': {**metadata()['label_spec'], 'version': 'v2'}}])
def test_matrix_cache_separates_target_stride_and_spec_version(monkeypatch, change):
    import inference.service as module
    original = module.build_matrix
    builds = []

    def build(*args):
        builds.append((args[1], args[2], args[3]['version']))
        return original(*args)

    monkeypatch.setattr(module, 'build_matrix', build)
    svc = service()
    other = service().models[0]
    other.metadata.update(change)
    svc.models.append(other)
    svc.references.append(module.reference_for(other.metadata['target'], other.metadata['label_spec']))
    feed(svc, row(20), row(20, market='perp'), row(21))
    assert len(builds) == 2 and builds[0] != builds[1]
    assert svc.counts['predictions'] == 2


def test_asset_field_matches_rust_and_clickhouse():
    assert SCHEMA['asset'] == pl.String
    assert list(SCHEMA).index('asset') == list(SCHEMA).index('symbol') + 1
    sql = Path('docker/dashboard/clickhouse/init/001-crypto.sql').read_text()
    assert sql.count('symbol LowCardinality(String),\n    asset LowCardinality(String),') == 2
    svc = service()
    feed(svc, row(0, asset='BTC'))
    assert next(iter(svc.rows.values()))['asset'] == 'BTC'
    old = row(10)
    old.pop('asset')
    feed(svc, old)
    assert next(iter(svc.rows.values()))['asset'] == ''
    assert svc.counts['malformed'] == 0


def test_context_trained_models_match_live_vectors_and_missing_context_nulls(tmp_path, compact_folds):
    spec = {**metadata()["label_spec"], "context_assets": ["USDT"]}
    rows = []
    for second in range(0, 7201, 10):
        rows.append(row(second, asset="BTC", mid_e9=100 * NS + int(200_000_000 * np.sin(second / 170)),
                        spread_bps=2. + second % 30 / 100))
        if second != 910:
            for exchange, symbol in [("binance", "USDTUSD"), ("okx", "USDT-USD")]:
                rows.append(row(second, exchange=exchange, symbol=symbol, asset="USDT",
                                mid_e9=NS + (second % 100 - 50) * 10_000,
                                spread_bps=1. + second % 40 / 100,
                                available_ns=second * NS + 700_000_000))
    frame = pl.DataFrame(rows, schema=SCHEMA)
    report = train_target(frame, "BTC_spot", 900, tmp_path, "context",
                          rounds=2, spec=spec, lockbox_from="1970-01-03")
    assert report["status"] == "trained"
    models = load_models(tmp_path, "context")
    features = models[0].metadata["features"]
    assert "ctx_usdt_binance_spot_premium_bps" in features
    assert "ctx_usdt_okx_spot_spread_bps" in features
    matrix, _ = build_matrix(frame, "BTC_spot", spec=spec)
    native = models[0].regressor
    captured = {}
    svc = Service(models, Producer())

    def predict(x, **kwargs):
        captured[svc.tick] = x.copy()
        return native.predict(x, **kwargs)

    models[0].regressor = SimpleNamespace(predict=predict)
    feed(svc, *[r for r in rows if r["tick_ns"] <= 920 * NS], row(921))
    predictions = {json.loads(message["value"])["tick_ns"]: json.loads(message["value"])
                   for _, message in svc.producer.messages}
    for second in [900, 910, 920]:
        tick = second * NS
        expected = matrix.filter(pl.col("tick_ns") == tick).select(features).to_numpy().astype(np.float64)
        current = [r for r in rows if r["tick_ns"] == tick]
        history = [r for r in rows if 0 <= r["tick_ns"] < tick and r["symbol"] == "BTCUSDT"]
        np.testing.assert_array_equal(vector_for(current, history, models[0].metadata), expected)
        np.testing.assert_array_equal(captured[tick], expected)
        assert predictions[tick]["pred_return_pct"] == native.predict(expected, num_threads=1)[0]
    context_indices = [i for i, name in enumerate(features) if name.startswith("ctx_")]
    assert np.isnan(captured[910 * NS][0, context_indices]).all()
    assert captured[900 * NS][0, features.index("ctx_usdt_binance_spot_premium_bps")] == -5.
    assert svc.counts["malformed"] == svc.skipped["inference_error"] == 0
    # Context names must still belong to this model's frozen configuration.
    path = Path(report["model_dir"]) / "metadata.json"
    meta = json.loads(path.read_text())
    meta["features"] = ["ctx_usdc_binance_spot_spread_bps"]
    path.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="cannot produce"):
        load_models(tmp_path, "context")


def test_matrix_cache_separates_context_settings_with_same_spec_version():
    plain = service().models[0]
    plain.metadata["label_spec"] = {**metadata()["label_spec"], "context_assets": []}
    context = service().models[0]
    context.metadata["features"] = ["ctx_usdt_binance_spot_premium_bps"]
    captured = []
    context.regressor = SimpleNamespace(predict=lambda x, **kw: (captured.append(x.copy()) or np.array([.2])))
    svc = Service([plain, context], Producer())
    feed(svc, row(10), row(10, symbol="USDTUSD", asset="USDT", mid_e9=1_000_250_000), row(11))
    np.testing.assert_array_equal(captured[0], [[2.5]])
    assert svc.counts["predictions"] == 2


def test_memory_features_are_numeric_and_reach_the_training_matrix():
    rows = [row(900, vol_900s=12.5, flow_900s=-2., trades_900s=7,
                cov_900s=.9, ofi1_900s=.2, ofi10_900s=-.3, range_bps_900s=15.,
                l3_add_bid_900s=3, ret_900s=.01, rv_900s=.02, microprice_dev_bps=.5)]
    matrix, features = build_matrix(pl.DataFrame(rows, schema=SCHEMA), "BTC_spot")
    for name in ("vol_900s", "flow_900s", "trades_900s", "cov_900s", "ofi1_900s",
                 "ofi10_900s", "range_bps_900s", "l3_add_bid_900s", "ret_900s",
                 "rv_900s", "microprice_dev_bps"):
        assert f"binance_spot_{name}" in features
        assert matrix[f"binance_spot_{name}"][0] == rows[0][name]


def test_five_minute_trained_stop_models_and_live_vector_parity(tmp_path, compact_folds):
    from research.risk import STOP_NAMES, predict_stops
    spec = metadata()["label_spec"]
    rows = [row(t, mid_e9=100 * NS + int(200_000_000 * np.sin(t / 130)),
                rv_300s=float(1 + abs(np.sin(t / 130)))) for t in range(4501)]
    frame = pl.DataFrame(rows, schema=SCHEMA)
    report = train_target(frame, "BTC_spot", 300, tmp_path, "risk", rounds=3, spec=spec, lockbox_from="1970-01-03")
    models = load_models(tmp_path, "risk", horizons=[300])
    assert set(models[0].stops) == set(STOP_NAMES)
    assert report["stop_labeled_rows"]["long"] == report["labeled_rows"]
    matrix, _ = build_matrix(frame, "BTC_spot", spec=spec)
    features = models[0].metadata["features"]
    expected = matrix.filter(pl.col("tick_ns") == 300 * NS).select(features).to_numpy().astype(np.float64)
    svc = Service(models, Producer())
    feed(svc, *rows[:302])
    np.testing.assert_array_equal(vector_for([rows[300]], rows[:300], models[0].metadata), expected)
    payload = next(json.loads(m["value"]) for _, m in svc.producer.messages
                   if json.loads(m["value"])["tick_ns"] == 300 * NS)
    predicted = predict_stops(models[0].stops, expected, num_threads=1)
    mid = rows[300]["mid_e9"] / NS
    for side in ("long", "short"):
        for q in (80, 90):
            bps = payload[f"stop_{side}_bps_q{q}"]
            assert bps == predicted[f"stop_{side}_q{q}"][0]
            assert payload[f"stop_{side}_price_q{q}"] == mid * (1 + (-1 if side == "long" else 1) * bps / 10000)
        assert payload[f"stop_{side}_bps_q90"] >= payload[f"stop_{side}_bps_q80"] >= 0
    assert payload["horizon_s"] == 300
    assert np.isfinite(expected[0, features.index("momentum_return_300s")])
    # Stop artifacts must obey the same frozen feature order as return models.
    path = Path(report["model_dir"]) / "stop_long_q80.txt"
    path.write_text(path.read_text().replace("binance_spot_rv_300s", "binance_spot_rv_999s"))
    with pytest.raises(ValueError, match="Stop model feature order"):
        load_models(tmp_path, "risk")


def test_live_stops_clip_and_do_not_cross_using_reference_mid():
    svc = service()
    values = {"stop_long_q80": -2, "stop_long_q90": -4, "stop_short_q80": 20, "stop_short_q90": 10}
    svc.models[0].stops = {name: SimpleNamespace(predict=lambda x, v=v, **kw: np.array([v]))
                           for name, v in values.items()}
    feed(svc, row(10, mid_e9=200 * NS, mid=999.), row(10, exchange="okx", mid_e9=500 * NS), row(11))
    payload = json.loads(svc.producer.messages[0][1]["value"])
    assert payload["stop_long_bps_q80"] == payload["stop_long_bps_q90"] == 0
    assert payload["stop_short_bps_q80"] == payload["stop_short_bps_q90"] == 20
    assert payload["stop_long_price_q90"] == 200
    assert payload["stop_short_price_q90"] == 200 * 1.002
