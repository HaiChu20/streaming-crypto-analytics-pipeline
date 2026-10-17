"""Run offline walk-forward baselines: PYTHON_DOTENV_DISABLED=1 uv run python -m research.train ROOT."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import lightgbm as lgb
import numpy as np
import polars as pl

from research.dataset import build_matrix, feature_paths, read_features
from research.labels import NS, load_spec, make_labels


def walk_forward_folds(tick_ns, horizon_s: int, n_folds: int = 3,
                       purge_s: int | None = None, label_available_ns=None,
                       feature_available_ns=None):
    """Expanding training prefix, contiguous disjoint test blocks, strict purge.

    Label endpoint AND emission must precede test tick. This excludes a label
    ending at the test start, including its normal 250 ms emission latency.
    """
    ticks = np.asarray(tick_ns, dtype=np.int64)
    purge_s = horizon_s if purge_s is None else purge_s
    if horizon_s <= 0 or purge_s < horizon_s:
        raise ValueError("purge_s must be >= the positive label horizon")
    if n_folds < 1 or len(ticks) < n_folds + 1 or np.any(np.diff(ticks) <= 0):
        raise ValueError("Need enough strictly time-ordered unique rows for walk-forward folds")
    known = ticks + horizon_s * NS
    if label_available_ns is not None:
        known = np.maximum(known, np.asarray(label_available_ns, dtype=np.int64))
    if feature_available_ns is not None:
        known = np.maximum(known, np.asarray(feature_available_ns, dtype=np.int64))
    bounds = np.linspace(0, len(ticks), n_folds + 2, dtype=int)
    folds = []
    for start, stop in zip(bounds[1:-1], bounds[2:]):
        train = np.flatnonzero((ticks + purge_s * NS < ticks[start]) & (known < ticks[start]))
        test = np.arange(start, stop)
        if not len(train) or not len(test):
            raise ValueError("Insufficient history for requested folds and purge; collect more data")
        folds.append((train, test))
    return folds


def average_ranks(values):
    """One-based average ranks, including ties; no scipy/sklearn metric dependency."""
    values = np.asarray(values)
    order = np.argsort(values, kind="stable")
    ordered = values[order]
    starts = np.r_[0, np.flatnonzero(ordered[1:] != ordered[:-1]) + 1]
    ends = np.r_[starts[1:], len(values)]
    ranks = np.empty(len(values), dtype=float)
    ranks[order] = np.repeat((starts + ends - 1) / 2 + 1, ends - starts)
    return ranks


def metrics(actual, predicted, predicted_direction=None) -> dict:
    actual, predicted = np.asarray(actual), np.asarray(predicted)
    keep = np.isfinite(actual) & np.isfinite(predicted)
    y, p = actual[keep], predicted[keep]
    result = {"n": int(len(y)), "mae": None, "rmse": None,
              "directional_accuracy": None, "directional_n": 0, "spearman_ic": None}
    if not len(y):
        return result
    error = p - y
    result.update(mae=float(np.mean(np.abs(error))), rmse=float(np.sqrt(np.mean(error ** 2))))
    direction = np.sign(p) if predicted_direction is None else np.asarray(predicted_direction)[keep]
    nonflat = y != 0
    result["directional_n"] = int(nonflat.sum())
    if nonflat.any():
        result["directional_accuracy"] = float(np.mean(direction[nonflat] == np.sign(y[nonflat])))
    if len(y) > 1:
        a, b = average_ranks(y), average_ranks(p)
        a, b = a - a.mean(), b - b.mean()
        denom = np.sqrt(np.dot(a, a) * np.dot(b, b))
        if denom > 0:
            result["spearman_ic"] = float(np.dot(a, b) / denom)
    return result


def fit_models(x, y, directions, features, rounds: int):
    params = {"verbosity": -1, "num_threads": 2, "seed": 17, "deterministic": True,
              "force_col_wise": True, "num_leaves": 15, "learning_rate": 0.05,
              "min_data_in_leaf": 20, "feature_pre_filter": False}
    regressor = lgb.train(
        dict(params, objective="regression"),
        lgb.Dataset(x, label=y, feature_name=features), num_boost_round=rounds,
    )
    classifier = lgb.train(
        dict(params, objective="multiclass", num_class=3),
        lgb.Dataset(x, label=directions + 1, feature_name=features), num_boost_round=rounds,
    )
    # Classifier has no native % return. Its implied return is probability times
    # each class's mean return, estimated ONLY on this training fold.
    class_means = np.array([y[directions == d].mean() if np.any(directions == d) else 0.0
                            for d in (-1, 0, 1)])
    return regressor, classifier, class_means


def train_target(rows: pl.DataFrame, target: str, horizon_s: int, output_root: str | Path,
                 version: str, stride_s: int = 10, n_folds: int = 3,
                 purge_s: int | None = None, rounds: int = 100,
                 spec: dict | None = None) -> dict:
    spec = spec or load_spec()
    if horizon_s not in spec["horizons_s"]:
        raise ValueError("Horizon is not in the versioned label spec")
    if rounds < 1 or not version or Path(version).name != version or version in {".", ".."}:
        raise ValueError("Need positive rounds and a single directory name for version")
    matrix, features = build_matrix(rows, target, stride_s, spec)
    labeled = matrix.join(make_labels(rows, target, horizon_s, spec), on="tick_ns", how="left")
    # A delayed emission after the target endpoint cannot be a forecast.
    data = labeled.filter(pl.col("return_pct").is_not_null()
                          & (pl.col("available_ns") < pl.col("label_end_ns"))).sort("tick_ns")
    if not features or data.is_empty():
        raise ValueError(f"No labeled training data/features for {target}/{horizon_s}")
    x = data.select(features).to_numpy().astype(np.float64)
    x[~np.isfinite(x)] = np.nan
    y = data["return_pct"].to_numpy()
    directions = data["direction"].to_numpy().astype(np.int32)
    ticks = data["tick_ns"].to_numpy()
    folds = walk_forward_folds(ticks, horizon_s, n_folds, purge_s,
                               data["label_available_ns"].to_numpy(), data["available_ns"].to_numpy())
    momentum = data["momentum_return_900s"].to_numpy() * (horizon_s / 900)
    results, actuals = [], []
    oof = {name: [] for name in ("regressor", "classifier", "zero", "momentum")}
    class_directions = []
    for i, (train, test) in enumerate(folds, 1):
        regressor, classifier, class_means = fit_models(
            x[train], y[train], directions[train], features, rounds
        )
        probabilities = classifier.predict(x[test], num_threads=2)
        predicted_direction = np.argmax(probabilities, axis=1) - 1
        predictions = {"regressor": regressor.predict(x[test], num_threads=2),
                       "classifier": probabilities @ class_means,
                       "zero": np.zeros(len(test)), "momentum": momentum[test]}
        fold_metrics = {name: metrics(y[test], p, predicted_direction if name == "classifier" else None)
                        for name, p in predictions.items()}
        results.append({"fold": i, "train_rows": len(train), "test_rows": len(test),
                        "train_start_ns": int(ticks[train[0]]), "train_end_ns": int(ticks[train[-1]]),
                        "train_label_end_ns": int(data["label_end_ns"][train].max()),
                        "train_label_available_ns": int(data["label_available_ns"][train].max()),
                        "train_feature_available_ns": int(data["available_ns"][train].max()),
                        "test_start_ns": int(ticks[test[0]]), "test_end_ns": int(ticks[test[-1]]),
                        "gap_s": float((ticks[test[0]] - ticks[train[-1]]) / NS),
                        "metrics": fold_metrics})
        actuals.append(y[test])
        class_directions.append(predicted_direction)
        for name, p in predictions.items():
            oof[name].append(p)
    # Concatenated held-out predictions, not averages of differently sized folds.
    summary = {name: metrics(np.concatenate(actuals), np.concatenate(predictions),
                            np.concatenate(class_directions) if name == "classifier" else None)
               for name, predictions in oof.items()}
    regressor, classifier, class_means = fit_models(x, y, directions, features, rounds)
    directory = Path(output_root) / "models" / target / str(horizon_s) / version
    directory.mkdir(parents=True, exist_ok=False)
    regressor.save_model(str(directory / "regressor.txt"))
    classifier.save_model(str(directory / "classifier.txt"))
    data.select("asset", "tick_ns", "available_ns", "label_end_ns", "label_available_ns",
                "engine_version", "replay_mode").write_parquet(directory / "training_rows.parquet")
    report = {
        "target": target, "horizon_s": horizon_s, "version": version,
        "label_spec_version": spec["version"], "label_spec": spec,
        "features": features, "engine_version": sorted(data["engine_version"].unique().to_list()),
        "replay_modes": sorted(data["replay_mode"].unique().to_list()),
        "data_window": {"start_ns": int(ticks[0]), "end_ns": int(ticks[-1]),
                        "last_label_end_ns": int(data["label_end_ns"].max())},
        "matrix_rows": matrix.height, "labeled_rows": data.height,
        "late_prediction_rows_excluded": labeled.filter(pl.col("return_pct").is_not_null()
            & (pl.col("available_ns") >= pl.col("label_end_ns"))).height,
        "label_coverage_pct": 100 * data.height / matrix.height,
        "stride_s": stride_s, "purge_s": horizon_s if purge_s is None else purge_s,
        "rounds": rounds, "folds": results, "metrics": summary,
        "classifier_class_order": [-1, 0, 1], "classifier_class_mean_returns": class_means.tolist(),
        "metric_notes": "Returns/MAE/RMSE in percentage points. Classifier implied return = "
                        "probabilities @ training-only class mean returns; direction = argmax. "
                        "Directional accuracy excludes actual flats. Undefined IC is null. "
                        "Momentum = trailing 900 s return * horizon/900; missing endpoints excluded "
                        "from its metrics (see n). Zero predicts flat, scoring 0 on non-flat directions.",
        "availability": "Prediction time is max available_ns of same-tick inputs. "
                        "No later tick enters X. Train labels must be emitted before test tick.",
        "warning": "Several weeks are required for meaningful walk-forward results. "
                   "A roughly 9-hour sample only verifies that the pipeline runs without leakage. "
                   "These metrics are not trading P&L; no fees/spread/funding execution backtest.",
        "model_dir": str(directory),
    }
    (directory / "metadata.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    return report


def upload_model(directory: str | Path, output_root: str | Path, bucket: str | None = None):
    """Explicit opt-in only. Never called by the default offline training path."""
    import boto3

    bucket = bucket or os.environ.get("S3_BUCKET")
    if not bucket:
        raise ValueError("--upload requires S3_BUCKET")
    directory, output_root = Path(directory), Path(output_root)
    client = boto3.client("s3")
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            client.upload_file(str(path), bucket, path.relative_to(output_root).as_posix())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="Contains features/date=D/features.parquet")
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--target", action="append", choices=["BTC_spot", "BTC_perp", "ETH_spot", "ETH_perp"])
    parser.add_argument("--horizon-s", action="append", type=int, choices=[900, 1800])
    parser.add_argument("--stride-s", type=int, default=10)
    parser.add_argument("--folds", type=int, default=3)
    parser.add_argument("--purge-s", type=int)
    parser.add_argument("--rounds", type=int, default=100)
    parser.add_argument("--version", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args(argv)
    output = args.output_root or (args.root.parent if args.root.is_file() else args.root)
    paths = feature_paths(args.root)
    rows, spec = read_features(paths), load_spec()
    reports = []
    for target in args.target or [f"{asset}_{market}" for asset in spec["references"] for market in ("spot", "perp")]:
        for horizon in args.horizon_s or spec["horizons_s"]:
            report = train_target(rows, target, horizon, output, args.version, args.stride_s,
                                  args.folds, args.purge_s, args.rounds, spec)
            reports.append(report)
            print(json.dumps({k: report[k] for k in ("target", "horizon_s", "matrix_rows", "labeled_rows",
                                                    "label_coverage_pct", "metrics")}, allow_nan=False), flush=True)
            if args.upload:
                upload_model(report["model_dir"], output)
    result = {"input_files": [str(p) for p in paths], "input_rows": rows.height, "results": reports}
    report_path = output / f"research_report_{args.version}.json"
    with report_path.open("x") as f:
        json.dump(result, f, indent=2, allow_nan=False)
        f.write("\n")
    print(f"Report: {report_path}", flush=True)
    print(reports[0]["warning"], flush=True)
    return result


if __name__ == "__main__":
    main()
