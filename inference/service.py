"""Consume ordered feature ticks and publish versioned LightGBM predictions."""

import argparse
from collections import Counter, deque
from dataclasses import dataclass, field
import json
import logging
import math
import os
from pathlib import Path
import signal
import time
import uuid

from confluent_kafka import Consumer, Producer
import lightgbm as lgb
import numpy as np
import polars as pl

from collectors.common.config import kafka_bootstrap
from research.calendar import Calendar, verify_calendar
from research.dataset import build_matrix
from research.labels import NS, load_spec, reference_for
from research.risk import STOP_NAMES, predict_stops

LOG = logging.getLogger(__name__)
STRINGS = "exchange market symbol asset engine_version".split()
BOOLS = "book_valid warmup".split()
INTS = "tick_ns available_ns mid_e9 trade_count liq_count_1s l3_adds_1s l3_modifies_1s l3_deletes_1s".split()
FLOATS = """book_age_ms best_bid best_ask mid microprice spread_bps bid_depth10 ask_depth10
imbalance10 volume_base taker_buy_base taker_sell_base ret_1s ret_10s ret_60s rv_60s rv_300s
funding_rate mark_price open_interest liq_base_1s basis_bps_binance basis_bps_okx
basis_bps_kraken spot_dev_bps_okx spot_dev_bps_kraken""".split()
WINDOWS = (10, 60, 300, 900, 1800, 3600)
INTS += [f"{name}_{w}s" for w in WINDOWS
         for name in ("trades", "l3_add_bid", "l3_add_ask", "l3_del_bid", "l3_del_ask")]
FLOATS += [f"{name}_{w}s" for w in WINDOWS
           for name in ("cov", "vol", "flow", "ofi1", "ofi10", "range_bps")]
FLOATS += [f"ret_{w}s" for w in (300, 900, 1800, 3600)]
FLOATS += [f"rv_{w}s" for w in (900, 1800, 3600)] + ["microprice_dev_bps"]
INTS += [f"liq_{side}_count_{w}s" for w in WINDOWS for side in ("sell", "buy")]
FLOATS += [f"liq_{side}_base_{w}s" for w in WINDOWS for side in ("sell", "buy")]
FLOATS += ["funding_min_to_next", "mark_index_bps"]
FLOATS += [f"oi_chg_pct_{w}s" for w in (300, 900, 3600)]
FLOATS += [f"depth_{side}_{bps}bps" for side in ("bid", "ask") for bps in (10, 25, 50)]
FLOATS += ['dvol', 'dvol_chg_300s', 'dvol_chg_900s', 'dvol_chg_3600s', 'dvol_rv_gap']
SCHEMA = {**dict.fromkeys(STRINGS, pl.String), **dict.fromkeys(BOOLS, pl.Boolean),
          **dict.fromkeys(INTS, pl.Int64), **dict.fromkeys(FLOATS, pl.Float64),
          "replay_mode": pl.String}
VENUES = [(e, m) for e in ("binance", "okx", "kraken") for m in ("spot", "perp")] + [("cross", "cross")]


def _supported_features(meta):
    """Ask the training builder which predictors this raw schema can produce.

    One typed row per configured asset/venue exposes all possible column names,
    including absent-venue nulls. Context naming and premium logic stay in research.
    """
    spec = meta["label_spec"]
    asset = meta["target"].rsplit("_", 1)[0]
    ref = reference_for(meta["target"], spec)
    rows = []
    for base in sorted({asset, *spec.get("context_assets", [])}):
        for exchange, market in VENUES:
            symbol = (ref["symbol"] if base == asset and (exchange, market) == (ref["exchange"], ref["market"])
                      else base)
            rows.append(dict(exchange=exchange, market=market, symbol=symbol, asset=base,
                             tick_ns=0, available_ns=0, engine_version="schema", replay_mode="as_live",
                             book_valid=True, warmup=False, mid_e9=NS))
    _, features = build_matrix(pl.DataFrame(rows, schema=SCHEMA), meta["target"], meta["stride_s"], spec)
    return set(features)


@dataclass
class Model:
    metadata: dict
    regressor: lgb.Booster
    classifier: lgb.Booster
    warned: bool = False
    calendar_warned: bool = False
    stops: dict = field(default_factory=dict)


def download_models(root, version, targets=None, horizons=None):
    """Explicit opt-in; only download inference artifacts, never overwrite."""
    import boto3
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise ValueError("--download requires S3_BUCKET")
    client = boto3.client("s3")
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="models/"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if (len(parts) != 5 or parts[3] != version or parts[1] not in
                [f"{a}_{m}" for a in load_spec()["references"] for m in ("spot", "perp")]
                or not parts[2].isdigit() or parts[4] not in ("metadata.json", "regressor.txt", "classifier.txt",
                                       *(f"{name}.txt" for name in STOP_NAMES))):
                continue
            if (targets and parts[1] not in targets) or (horizons and int(parts[2]) not in horizons):
                continue
            path = root.joinpath(*parts)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as output:
                client.download_fileobj(bucket, obj["Key"], output)


def load_models(root, version, targets=None, horizons=None):
    if not version or Path(version).name != version or version in (".", ".."):
        raise ValueError("--version must be a single directory name")
    models = []
    for path in sorted(Path(root).glob(f"models/*/*/{version}")):
        target, horizon = path.parents[1].name, int(path.parent.name)
        if (targets and target not in targets) or (horizons and horizon not in horizons):
            continue
        meta = json.loads((path / "metadata.json").read_text())
        reference_for(target, meta["label_spec"])
        if ((meta["target"], meta["horizon_s"], meta["version"]) != (target, horizon, version)
                or not 0 < horizon <= 65535 or type(meta["stride_s"]) is not int or meta["stride_s"] <= 0):
            raise ValueError(f"Invalid model identity/stride: {path}")
        if meta["label_spec"].get("calendar"):
            try:
                verify_calendar(Calendar.load(meta["label_spec"]["calendar"]), meta)
            except (ValueError, KeyError, OSError) as exc:
                LOG.error("Refusing model %s: calendar validation failed: %s", path, exc)
                continue
        features = meta["features"]
        possible = _supported_features(meta)
        if not features or len(set(features)) != len(features) or set(features) - possible:
            raise ValueError(f"Features the live schema cannot produce: {set(features) - possible}")
        reg, clf = [lgb.Booster(model_file=str(path / name)) for name in ("regressor.txt", "classifier.txt")]
        if reg.feature_name() != features or clf.feature_name() != features or clf.num_model_per_iteration() != 3:
            raise ValueError(f"Model feature/class order disagrees with metadata: {path}")
        if meta.get("classifier_class_order") != [-1, 0, 1]:
            raise ValueError("Classifier class order must be [-1, 0, 1]")
        stops = {name: lgb.Booster(model_file=str(path / f"{name}.txt"))
                 for name in STOP_NAMES if (path / f"{name}.txt").is_file()}
        if any(model.feature_name() != features or model.num_model_per_iteration() != 1
               for model in stops.values()):
            raise ValueError(f"Stop model feature order disagrees with metadata: {path}")
        models.append(Model(meta, reg, clf, stops=stops))
    if not models:
        raise ValueError("No matching models found")
    return models


def _matrix_for(rows, history, meta, calendar=None):
    frame = pl.DataFrame([*history, *rows], schema=SCHEMA)
    options = {"calendar": calendar} if calendar is not None else {}
    matrix, _ = build_matrix(frame, meta["target"], meta["stride_s"], meta["label_spec"], **options)
    return matrix.filter(pl.col("tick_ns") == rows[0]["tick_ns"])


def _reindex(matrix, features):
    x = matrix.select([pl.col(c) if c in matrix.columns else pl.lit(None).alias(c)
                       for c in features]).to_numpy().astype(np.float64)
    x[~np.isfinite(x)] = np.nan
    return x


def vector_for(rows, history, meta):
    return _reindex(_matrix_for(rows, history, meta), meta["features"])


class Service:
    def __init__(self, models, producer):
        self.models, self.producer = models, producer
        self.tick, self.rows, self.history = None, {}, deque()
        self.references = [reference_for(m.metadata["target"], m.metadata["label_spec"]) for m in models]
        self.counts, self.skipped = Counter(ticks=0, predictions=0, malformed=0), Counter()
        self.latency, self.compute = deque(maxlen=10000), deque(maxlen=10000)

    def bad(self, reason):
        self.counts["malformed"] += 1
        if self.counts["malformed"] <= 5 or self.counts["malformed"] % 100 == 0:
            LOG.warning("Malformed input (%s), total=%s", reason, self.counts["malformed"])

    def ingest(self, payload):
        try:
            row = json.loads(payload)
            if not isinstance(row, dict) or set(row) - SCHEMA.keys():
                raise ValueError("unexpected fields/shape")
            row.setdefault("asset", "")  # Rust Row.asset has serde(default).
            if any(type(row.get(c)) is not str for c in STRINGS) or any(type(row.get(c)) is not bool for c in BOOLS):
                raise ValueError("missing identity/flags")
            for c in INTS:
                v = row.get(c)
                if v is not None and (type(v) is not int or not -(2**63) <= v < 2**63):
                    raise ValueError(f"invalid {c}")
            if (row.get("tick_ns") is None or row.get("available_ns") is None
                    or row["tick_ns"] % NS or row["available_ns"] < row["tick_ns"]):
                raise ValueError("invalid timestamps")
            for c in FLOATS:
                v = row.get(c)
                if v is not None and (type(v) not in (float, int) or not math.isfinite(v)):
                    raise ValueError(f"invalid {c}")
            row["replay_mode"] = "as_live"
            tick = row["tick_ns"]
            if self.tick is not None and tick < self.tick:
                raise ValueError("out-of-order tick")
            if self.tick is not None and tick > self.tick:
                self.complete()
                self.rows = {}
            self.tick = tick
            key = tuple(row[c] for c in ("exchange", "market", "symbol"))
            if key in self.rows or len(self.rows) >= 64:
                raise ValueError("duplicate/excess instruments")
            self.rows[key] = row
        except Exception as exc:
            self.bad(str(exc))

    def complete(self):
        self.counts["ticks"] += 1
        rows = list(self.rows.values())
        while self.history and self.history[0]["tick_ns"] < self.tick - 901 * NS:
            self.history.popleft()
        started = time.perf_counter()
        due = False
        matrices, calendars = {}, {}
        for model in self.models:
            meta = model.metadata
            if self.tick % (meta["stride_s"] * NS):
                self.skipped["stride"] += 1
                continue
            due = True
            ref = reference_for(meta["target"], meta["label_spec"])
            reference = next((r for r in rows if all(r[k] == v for k, v in ref.items())), None)
            reason = "missing_reference"
            if reference:
                reason = ("invalid_reference" if not reference["book_valid"] or (reference.get("mid_e9") or 0) <= 0
                          else "warm_reference" if reference["warmup"] else None)
            if reason:
                self.skipped[reason] += 1
                continue
            try:
                calendar = None
                if meta["label_spec"].get("calendar"):
                    path = meta["label_spec"]["calendar"]
                    if path not in calendars:
                        calendars[path] = Calendar.load(path)
                    calendar = calendars[path]
                    verify_calendar(calendar, meta)
                    if not calendar.contains(self.tick):
                        self.skipped["calendar_coverage"] += 1
                        if not model.calendar_warned:
                            LOG.warning("Skipped %s/%s: live tick %s outside calendar coverage %s through %s "
                                        "with required 7-day margins; run scripts/update_events.py",
                                        meta["target"], meta["horizon_s"], self.tick,
                                        calendar.covers_from, calendar.covers_through)
                            model.calendar_warned = True
                        continue
                    model.calendar_warned = False
                if not model.warned and (any(r["engine_version"] not in meta["engine_version"] for r in rows)
                                         or "as_live" not in meta["replay_modes"]):
                    LOG.warning("Model %s/%s/%s engine_version/replay_mode differs from live rows",
                                meta["target"], meta["horizon_s"], meta["version"])
                    model.warned = True
                key = (meta["target"], meta["stride_s"], json.dumps(meta["label_spec"], sort_keys=True))
                if key not in matrices:
                    matrices[key] = (_matrix_for(rows, self.history, meta, calendar) if calendar is not None
                                     else _matrix_for(rows, self.history, meta))
                x = _reindex(matrices[key], meta["features"])
                predicted = float(model.regressor.predict(x, num_threads=1)[0])
                probabilities = model.classifier.predict(x, num_threads=1)[0]
                if not math.isfinite(predicted) or not np.isfinite(probabilities).all():
                    raise ValueError("nonfinite prediction")
                stops = predict_stops(model.stops, x, num_threads=1)
                risk_output = {}
                mid = reference["mid_e9"] / NS
                for name in STOP_NAMES:
                    _, side, quantile = name.split("_")
                    bps = float(stops[name][0]) if name in stops else None
                    risk_output[f"stop_{side}_bps_{quantile}"] = bps
                    risk_output[f"stop_{side}_price_{quantile}"] = (
                        mid * (1 + (-1 if side == "long" else 1) * bps / 10_000)
                        if bps is not None else None)
                available = time.time_ns()
                if available < max(r["available_ns"] for r in rows):
                    self.skipped["future_available"] += 1
                    continue
                output = dict(model_version=meta["version"], target=meta["target"],
                              horizon_s=meta["horizon_s"], tick_ns=self.tick, available_ns=available,
                              pred_return_pct=predicted, direction=int(np.argmax(probabilities)) - 1,
                              **risk_output)
                self.producer.produce("predictions.v1", key=meta["target"], partition=0,
                                      value=json.dumps(output, allow_nan=False), on_delivery=self.delivered)
                self.counts["predictions"] += 1
                self.latency.append((available - self.tick) / 1e6)
            except Exception as exc:
                self.skipped["inference_error"] += 1
                LOG.warning("Skipped %s/%s: %s", meta["target"], meta["horizon_s"], exc)
        if due:
            self.compute.append((time.perf_counter() - started) * 1000)
        self.history.extend(r for r in rows if any(all(r[k] == v for k, v in ref.items()) for ref in self.references))

    def delivered(self, error, message):
        if error:
            self.skipped["delivery_error"] += 1
            LOG.warning("Prediction delivery failed: %s", error)

    def stats(self):
        return dict(self.counts, skipped=dict(self.skipped),
                    latency_ms_p50=float(np.median(self.latency)) if self.latency else None,
                    latency_ms_max=max(self.latency, default=None),
                    compute_ms_p50=float(np.median(self.compute)) if self.compute else None,
                    compute_ms_p99=float(np.percentile(self.compute, 99)) if self.compute else None)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models-root", type=Path, default=Path(os.environ.get("INFERENCE_MODELS_ROOT", "data")))
    parser.add_argument("--version", default=os.environ.get("INFERENCE_MODEL_VERSION", ""))
    parser.add_argument("--target", action="append")
    parser.add_argument("--horizon-s", type=int, action="append")
    parser.add_argument("--download", action="store_true")
    args = parser.parse_args(argv)
    if not args.version:
        parser.error("Set --version or INFERENCE_MODEL_VERSION (empty disables inference)")
    if not args.version or Path(args.version).name != args.version or args.version in (".", ".."):
        parser.error("--version must be a single directory name")
    if args.download:
        download_models(args.models_root, args.version, args.target, args.horizon_s)
    models = load_models(args.models_root, args.version, args.target, args.horizon_s)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    producer = Producer({"bootstrap.servers": kafka_bootstrap(), "enable.idempotence": True, "linger.ms": 0})
    consumer = Consumer({"bootstrap.servers": kafka_bootstrap(), "group.id": f"inference-{uuid.uuid4()}",
                         "auto.offset.reset": "latest", "enable.auto.commit": True,
                         "allow.auto.create.topics": False})
    service, stopping = Service(models, producer), False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, stop)
    consumer.subscribe(["features.1s.v1"])
    last_stats = time.monotonic()
    try:
        while not stopping:
            message = consumer.poll(0.25)
            if message is not None:
                if message.error():
                    service.bad(str(message.error()))
                else:
                    service.ingest(message.value())
            producer.poll(0)
            if time.monotonic() - last_stats >= 60:
                LOG.info("stats %s", json.dumps(service.stats()))
                last_stats = time.monotonic()
    finally:
        consumer.close()
        remaining = producer.flush(10)
        LOG.info("stopped pending=%s stats %s", remaining, json.dumps(service.stats()))


if __name__ == "__main__":
    main()
