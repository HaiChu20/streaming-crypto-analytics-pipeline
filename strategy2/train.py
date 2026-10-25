"""Expanding monthly, purged Strategy 2 training. The lockbox cannot be opened here."""

import argparse
from copy import deepcopy
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import subprocess

import lightgbm as lgb
import numpy as np
import polars as pl
import yaml

DEFAULT_CONFIG = Path(__file__).parent / "config" / "train_v1.yaml"
LOCKBOX_MS = 1_775_001_600_000  # 2026-04-01 UTC
LABELS = (1, -1, 0)  # LightGBM class indices: up, down, none


def timestamp_ms(value):
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, date):
        value = value.isoformat()
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def month_shift(ts_ms, months):
    dt = datetime.fromtimestamp(ts_ms / 1000, timezone.utc)
    index = dt.year * 12 + dt.month - 1 + months
    return int(datetime(index // 12, index % 12 + 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def load_config(path=None, overrides=()):
    with DEFAULT_CONFIG.open() as stream:
        config = yaml.safe_load(stream)
    def merge(base, extra):
        for key, value in extra.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                merge(base[key], value)
            else:
                base[key] = value
    for extra_path in ([path] if path else []) + list(overrides):
        with Path(extra_path).open() as stream:
            merge(config, yaml.safe_load(stream) or {})
    config.setdefault("model", "three_class")
    return config


def validate_config(config):
    if config.get("model", "three_class") not in ("three_class", "two_stage"):
        raise ValueError("model must be three_class or two_stage")
    if config.get("side_calibration", "isotonic") not in ("isotonic", "platt", "none"):
        raise ValueError("side_calibration must be isotonic, platt or none")
    lockbox = timestamp_ms(config["lockbox_from"])
    if lockbox > LOCKBOX_MS or config.get("include_lockbox", False):
        raise ValueError("Lockbox from 2026-04-01 is sealed; this command cannot include it")
    if month_shift(lockbox, 0) != lockbox:
        raise ValueError("lockbox_from must be a calendar month boundary")
    if timestamp_ms(config["dev_from"]) >= lockbox:
        raise ValueError("dev_from must precede the lockbox")
    for key in ("dev_to", "test_to", "test_until"):
        if key in config and timestamp_ms(config[key]) > lockbox:
            raise ValueError(f"{key} would include the sealed lockbox")
    if config["test_months"] < 1 or config["inner_validation_months"] < 1:
        raise ValueError("Month counts must be positive")
    if config["embargo_days"] < 0:
        raise ValueError("Embargo cannot be negative")
    return lockbox


def monthly_folds(config):
    lockbox = validate_config(config)
    for offset in range(-int(config["test_months"]), 0):
        start, end = month_shift(lockbox, offset), month_shift(lockbox, offset + 1)
        yield datetime.fromtimestamp(start / 1000, timezone.utc).strftime("%Y-%m"), start, end


def fold_masks(start, end, target, config):
    embargo = int(config["embargo_days"] * 86_400_000)
    past = ((pl.col("ts_ms") < start) &
            (pl.col(f"label_end_ms_{target}") <= start - embargo))
    boundary = month_shift(start, -int(config["inner_validation_months"]))
    return (
        past & (pl.col("ts_ms") < boundary) &
        (pl.col(f"label_end_ms_{target}") <= boundary - embargo),
        past & (pl.col("ts_ms") >= boundary),
        (pl.col("ts_ms") >= start) & (pl.col("ts_ms") < end),
    )


def fold_split(frame, start, end, target, config):
    """Inner train, inner validation, test; label-end equality at cutoff is allowed."""
    return tuple(frame.filter(mask) for mask in fold_masks(start, end, target, config))


def class_indices(labels):
    values = np.asarray(labels)
    if not np.isin(values, LABELS).all():
        raise ValueError("Labels must be -1, 0 or 1")
    return np.where(values == 1, 0, np.where(values == -1, 1, 2))


def fit_isotonic(x, y):
    """Unweighted PAVA with tied scores pooled first; JSON-serialisable knots."""
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    unique, inverse, counts = np.unique(x, return_inverse=True, return_counts=True)
    sums = np.bincount(inverse, weights=y)
    blocks = []
    for index, (total, count) in enumerate(zip(sums, counts)):
        blocks.append([index, index, float(total), int(count)])
        while len(blocks) >= 2 and blocks[-2][2] / blocks[-2][3] > blocks[-1][2] / blocks[-1][3]:
            right = blocks.pop()
            left = blocks.pop()
            blocks.append([left[0], right[1], left[2] + right[2], left[3] + right[3]])
    fitted = np.zeros(len(unique))
    for left, right, total, count in blocks:
        fitted[left:right + 1] = total / count
    return {"x": unique.tolist(), "y": fitted.tolist()}


def fit_calibration(probabilities, labels):
    return [fit_isotonic(probabilities[:, c], np.asarray(labels) == c) for c in range(3)]


def apply_calibration(probabilities, calibrators):
    calibrated = np.column_stack([np.interp(probabilities[:, c], knots["x"], knots["y"])
                                  for c, knots in enumerate(calibrators)])
    totals = calibrated.sum(axis=1, keepdims=True)
    return np.divide(calibrated, totals, out=np.full_like(calibrated, 1 / 3), where=totals > 0)


def fit_binary_calibration(scores, labels, method="isotonic"):
    """Fit on inner validation only. Platt uses a damped Newton fit on logits."""
    scores, labels = np.asarray(scores, dtype=float), np.asarray(labels, dtype=float)
    if not len(scores) or len(scores) != len(labels):
        raise ValueError("Calibration requires aligned nonempty observations")
    if method == "none":
        return {"method": "none"}
    if method == "isotonic":
        return {"method": method, **fit_isotonic(scores, labels)}
    if method != "platt":
        raise ValueError("Unknown binary calibration method")
    clipped = np.clip(scores, 1e-8, 1 - 1e-8)
    logits = np.log(clipped / (1 - clipped))
    x = np.column_stack([logits, np.ones(len(logits))])
    # A tiny ridge keeps separated or constant-score samples well posed.
    ridge = 1e-6
    beta = np.array([0.0, np.log((labels.sum() + 0.5) / (len(labels) - labels.sum() + 0.5))])
    def objective(b):
        z = x @ b
        return np.mean(np.logaddexp(0, z) - labels * z) + ridge * np.dot(b, b) / 2
    for _ in range(50):
        z = np.clip(x @ beta, -40, 40)
        p = 1 / (1 + np.exp(-z))
        gradient = x.T @ (p - labels) / len(labels) + ridge * beta
        hessian = (x.T * (p * (1 - p))) @ x / len(labels) + ridge * np.eye(2)
        step = np.linalg.solve(hessian, gradient)
        scale = 1.0
        previous = objective(beta)
        while scale > 1e-6 and objective(beta - scale * step) > previous:
            scale *= 0.5
        beta -= scale * step
        if np.max(np.abs(scale * step)) < 1e-8:
            break
    return {"method": method, "slope": float(beta[0]), "intercept": float(beta[1])}


def apply_binary_calibration(scores, calibrator):
    scores = np.asarray(scores, dtype=float)
    if calibrator["method"] == "none":
        return scores.copy()
    if calibrator["method"] == "isotonic":
        return np.interp(scores, calibrator["x"], calibrator["y"])
    clipped = np.clip(scores, 1e-8, 1 - 1e-8)
    z = calibrator["slope"] * np.log(clipped / (1 - clipped)) + calibrator["intercept"]
    return 1 / (1 + np.exp(-np.clip(z, -40, 40)))


def combine_probabilities(p_move, p_side):
    return np.column_stack([p_move * p_side, p_move * (1 - p_side), 1 - p_move])


def train_two_stage(fit, valid, test, features, target, config, params, output, name):
    """Materialise each feature matrix once; side datasets contain touched rows only."""
    x_fit, x_valid, x_test = (part.select(features).to_numpy() for part in (fit, valid, test))
    y_fit, y_valid = (part[f"label_{target}"].to_numpy() for part in (fit, valid))
    touched_fit, touched_valid = y_fit != 0, y_valid != 0
    if not touched_fit.any() or not touched_valid.any():
        raise ValueError(f"Fold {name} requires touched fit and inner-validation rows")
    best, calibrators, raw_valid, raw_test, calibrated_valid, calibrated_test = {}, {}, {}, {}, {}, {}
    for stage in ("move", "side"):
        side = stage == "side"
        train_x, valid_x = (x_fit[touched_fit], x_valid[touched_valid]) if side else (x_fit, x_valid)
        train_y = (y_fit[touched_fit] == 1) if side else touched_fit
        valid_y = (y_valid[touched_valid] == 1) if side else touched_valid
        training = lgb.Dataset(train_x, label=train_y, feature_name=features,
                               categorical_feature=[f for f in features if f == "f_coin_id"])
        validation = lgb.Dataset(valid_x, label=valid_y, reference=training)
        stage_params = dict(params)
        stage_params.pop("num_class", None)
        stage_params.update(config.get(f"{stage}_lightgbm", config["lightgbm"]))
        stage_params.update(objective="binary", metric="binary_logloss")
        model = lgb.train(stage_params, training, num_boost_round=int(config["num_boost_round"]),
                          valid_sets=[validation], callbacks=[lgb.early_stopping(
                              int(config["early_stopping_rounds"]), verbose=False)])
        best[stage] = model.best_iteration or model.current_iteration()
        model.save_model(str(output / f"{name}-{stage}.txt"), num_iteration=best[stage])
        pl.DataFrame({"feature": features, "gain": model.feature_importance(
            importance_type="gain", iteration=best[stage])}).write_csv(output / f"{name}-{stage}-importance.csv")
        raw_valid[stage] = model.predict(x_valid, num_iteration=best[stage], num_threads=params["num_threads"])
        raw_test[stage] = model.predict(x_test, num_iteration=best[stage], num_threads=params["num_threads"])
        method = config.get("side_calibration", "isotonic") if side else "isotonic"
        if not config.get("calibration", True):
            method = "none"
        calibration_scores = raw_valid[stage][touched_valid] if side else raw_valid[stage]
        calibrators[stage] = fit_binary_calibration(calibration_scores, valid_y, method)
        calibrated_valid[stage] = apply_binary_calibration(raw_valid[stage], calibrators[stage])
        calibrated_test[stage] = apply_binary_calibration(raw_test[stage], calibrators[stage])
    return (best, calibrators, raw_valid, raw_test, calibrated_valid, calibrated_test)


def confidence(probabilities):
    denominator = probabilities[:, 0] + probabilities[:, 1]
    return np.divide(np.maximum(probabilities[:, 0], probabilities[:, 1]), denominator,
                     out=np.full(len(probabilities), 0.5), where=denominator > 0)


def threshold_name(source, fraction):
    return f"threshold_{source}_{int(round(100 * fraction))}"


def baseline_probabilities(frame, source):
    if source in ("vol", "prior"):
        return frame.select([f"{source}_p_{side}" for side in ("up", "down", "none")]).to_numpy()
    values = frame[f"{source}_dir"].to_numpy()
    result = np.column_stack([values == 1, values == -1, values == 0]).astype(float)
    result[~np.isin(values, (-1, 0, 1))] = np.nan
    return result


def train(data, work, version, target="a", coins=None, config=None, threads=None, feature_groups=None):
    config = deepcopy(config if config is not None else load_config())
    lockbox = validate_config(config)
    if target not in config["targets"]:
        raise ValueError(f"Unknown target {target}")
    if not version or Path(version).name != version or version in (".", ".."):
        raise ValueError("version must be a single nonempty path component")
    output = Path(work) / "models" / "s2" / target / version
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite existing version: {output}")
    root = Path(data)
    paths = [root / f"{coin}.parquet" for coin in coins] if coins else sorted(root.glob("*.parquet"))
    if not paths:
        raise ValueError("No coin parquet files found")
    # Filter before collection, label checks, feature discovery, counts or folds.
    frame = pl.concat([pl.scan_parquet(path).filter(pl.col("ts_ms") < lockbox)
                       for path in paths], how="diagonal_relaxed").collect()
    frame = frame.filter(pl.col("ts_ms") >= timestamp_ms(config["dev_from"]))
    metadata_path = root / "metadata.json"
    source_metadata = json.loads(metadata_path.read_text()) if metadata_path.exists() else {}
    # The dataset writes names in feature_columns and {name, group} records in features.
    features = source_metadata.get("feature_columns", source_metadata.get("features"))
    if isinstance(features, list) and features and isinstance(features[0], dict):
        features = [item["name"] for item in features]
    if features is None:
        features = sorted(name for name in frame.columns if name.startswith("f_"))
    if not isinstance(features, list) or not features or "f_coin_id" not in features:
        raise ValueError("Feature metadata must list numeric f_* features including f_coin_id")
    if len(set(features)) != len(features) or any(not f.startswith("f_") for f in features):
        raise ValueError("Feature list must contain distinct f_* columns only")
    records = source_metadata.get("features", [])
    group_map = {item["name"]: item["group"] for item in records
                 if isinstance(item, dict) and "name" in item and "group" in item}
    if feature_groups is not None:
        requested = feature_groups.split(",") if isinstance(feature_groups, str) else feature_groups
        requested = {group.strip().upper() for group in requested}
        if not requested or "" in requested:
            raise ValueError("feature-groups must select at least one group")
        if any(name not in group_map for name in features):
            raise ValueError("feature-groups requires dataset metadata features with name and group")
        unknown = requested - {group_map[name] for name in features}
        if unknown:
            raise ValueError(f"Unknown feature groups: {sorted(unknown)}")
        features = [name for name in features if group_map[name] in requested]
    chosen_groups = (list(dict.fromkeys(group_map[name] for name in features))
                     if all(name in group_map for name in features) else None)
    for feature in features:
        if feature not in frame.columns or not frame.schema[feature].is_numeric():
            raise ValueError(f"Missing or nonnumeric feature: {feature}")
    frame = frame.filter(pl.col(f"label_{target}").is_not_null()).sort(["ts_ms", "coin"])
    class_indices(frame[f"label_{target}"].to_numpy())
    required = [f"{name}_{target}" for name in ("label_end_ms", "exit_return", "entry_price")]
    required += ["entry_ms", "ts_ms", "coin"]
    if frame.select(pl.any_horizontal([pl.col(name).is_null() for name in required]).any()).item():
        raise ValueError("Non-null labels require complete entry, exit-return and label-end fields")
    if frame.select(pl.struct(["ts_ms", "coin"]).is_duplicated().any()).item():
        raise ValueError("Duplicate coin/decision rows")
    # Even a pre-lockbox decision must not consume a post-boundary outcome.
    frame = frame.filter(pl.col(f"label_end_ms_{target}") < lockbox)
    # Require all planned folds; never silently select a favourable subset.
    splits = list(monthly_folds(config))
    for name, start, end in splits:
        counts = frame.select([mask.sum().alias(f"count_{i}") for i, mask in
                               enumerate(fold_masks(start, end, target, config))]).row(0)
        if min(counts) == 0:
            raise ValueError(f"Fold {name} has empty fit/inner-validation/test partition")
    output.mkdir(parents=True, exist_ok=False)
    seed = int(config["seed"])
    params = dict(config["lightgbm"], objective="multiclass", num_class=3,
                  metric="multi_logloss", verbosity=-1, seed=seed,
                  feature_fraction_seed=seed, bagging_seed=seed, data_random_seed=seed,
                  deterministic=True, force_col_wise=True,
                  num_threads=threads if threads is not None else (os.cpu_count() or 1))
    if params["num_threads"] < 1:
        raise ValueError("threads must be positive")
    oof, folds = [], []
    for name, start, end in splits:
        # Materialise one fold at a time: never retain 24 copies of the fit matrix.
        fit, valid, test = fold_split(frame, start, end, target, config)
        y_fit, y_valid = class_indices(fit[f"label_{target}"].to_numpy()), class_indices(valid[f"label_{target}"].to_numpy())
        two_stage = config.get("model", "three_class") == "two_stage"
        if two_stage:
            best, calibrators, rv, rt, cv, ct = train_two_stage(
                fit, valid, test, features, target, config, params, output, name)
            raw_valid, raw_test = (combine_probabilities(p["move"], p["side"]) for p in (rv, rt))
            p_valid, p_test = (combine_probabilities(p["move"], p["side"]) for p in (cv, ct))
        else:
            training = lgb.Dataset(fit.select(features).to_numpy(), label=y_fit,
                                   feature_name=features, categorical_feature=[f for f in features if f == "f_coin_id"])
            validation = lgb.Dataset(valid.select(features).to_numpy(), label=y_valid, reference=training)
            model = lgb.train(params, training, num_boost_round=int(config["num_boost_round"]),
                              valid_sets=[validation], callbacks=[lgb.early_stopping(
                                  int(config["early_stopping_rounds"]), verbose=False)])
            best = model.best_iteration or model.current_iteration()
            model.save_model(str(output / f"{name}.txt"), num_iteration=best)
            raw_valid = model.predict(valid.select(features).to_numpy(), num_iteration=best, num_threads=params["num_threads"])
            raw_test = model.predict(test.select(features).to_numpy(), num_iteration=best, num_threads=params["num_threads"])
            calibrators = fit_calibration(raw_valid, y_valid) if config["calibration"] else None
            p_valid = apply_calibration(raw_valid, calibrators) if calibrators else raw_valid
            p_test = apply_calibration(raw_test, calibrators) if calibrators else raw_test
        prior = np.bincount(y_fit, minlength=3) / len(y_fit)
        def common(part):
            expressions = [pl.col(name) for name in ("ts_ms", "coin", "entry_ms", "last_funding_rate", "next_funding_ms", "funding_interval_ms")]
            expressions += [pl.col(f"{column}_{target}").alias(column) for column in (
                "label", "exit_return", "minutes_to_touch", "entry_price", "mom_dir", "rev_dir")]
            expressions += [pl.col(f"vol_p_{side}_{target}").alias(f"vol_p_{side}") for side in ("up", "down", "none")]
            expressions += [pl.lit(float(prior[c])).alias(f"prior_p_{side}") for c, side in enumerate(("up", "down", "none"))]
            return part.select(expressions)
        inner, outer = common(valid), common(test)
        thresholds = {}
        for source in ("model", "raw", "vol", "prior", "mom", "rev"):
            probabilities = p_valid if source == "model" else raw_valid if source == "raw" else baseline_probabilities(inner, source)
            eligible = np.isfinite(probabilities).all(axis=1)
            scores = confidence(probabilities[eligible])
            if two_stage and source in ("model", "raw"):
                side_scores = (cv if source == "model" else rv)["side"][eligible]
                scores = np.maximum(side_scores, 1 - side_scores)
            for fraction in config["selective_fractions"]:
                thresholds[threshold_name(source, fraction)] = float(np.quantile(scores, 1 - fraction)) if len(scores) else None
        direction = np.sign(cv["side"] - 0.5) if two_stage else np.sign(p_valid[:, 0] - p_valid[:, 1])
        none = valid[f"label_{target}"].to_numpy() == 0
        timeout = {}
        for sign, side in ((1, "long"), (-1, "short")):
            returns = valid[f"exit_return_{target}"].to_numpy()[none & (direction == sign)] * sign
            timeout[f"timeout_mean_{side}"] = float(returns.mean()) if len(returns) else 0.0
        outer = outer.with_columns(
            pl.lit(name).alias("fold"),
            *[pl.Series(f"p_{side}", p_test[:, c]) for c, side in enumerate(("up", "down", "none"))],
            *[pl.Series(f"raw_p_{side}", raw_test[:, c]) for c, side in enumerate(("up", "down", "none"))],
            *[pl.lit(value, dtype=pl.Float64).alias(key) for key, value in (thresholds | timeout).items()],
            pl.lit(float(config["ev_threshold"])).alias("ev_threshold"))
        if two_stage:
            outer = outer.with_columns(
                pl.Series("p_move", ct["move"]), pl.Series("p_side", ct["side"]),
                pl.Series("raw_p_move", rt["move"]), pl.Series("raw_p_side", rt["side"]))
        oof.append(outer)
        fold = {"fold": name, "start_ms": start, "end_ms": end, "best_iteration": best,
                "fit_rows": len(fit), "inner_rows": len(valid), "test_rows": len(test),
                "fit_min_ts_ms": int(fit["ts_ms"].min()), "fit_max_ts_ms": int(fit["ts_ms"].max()),
                "fit_max_label_end_ms": int(fit[f"label_end_ms_{target}"].max()),
                "inner_start_ms": month_shift(start, -int(config["inner_validation_months"])),
                "inner_max_label_end_ms": int(valid[f"label_end_ms_{target}"].max()),
                "prior": prior.tolist(), "thresholds": thresholds, **timeout,
                "ev_threshold": config["ev_threshold"], "calibrators": calibrators}
        if two_stage:
            fold.update(side_fit_rows=int((y_fit != 2).sum()), side_inner_rows=int((y_valid != 2).sum()))
        else:
            pl.DataFrame({"feature": features, "gain": model.feature_importance(importance_type="gain", iteration=best)}).write_csv(output / f"{name}-importance.csv")
        (output / f"{name}.json").write_text(json.dumps(fold, indent=2))
        folds.append(fold)
    pl.concat(oof).sort(["ts_ms", "coin"]).write_parquet(output / "oof.parquet")
    sha = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    metadata = {"target": target, "version": version, "config": config, "git_sha": sha,
                "features": features, "feature_groups": chosen_groups,
                "class_order": ["up", "down", "none"], "folds": folds,
                "lockbox_exclusive_ms": lockbox, "num_threads": params["num_threads"],
                "timeout_mean_method": "Signed none-row returns, conditional on predicted direction in inner validation; zero when absent."}
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str))
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--target", required=True, choices=("a", "b", "c"))
    parser.add_argument("--coins", nargs="+")
    parser.add_argument("--feature-groups", help="Comma-separated dataset groups, e.g. A,B,K; default: all")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--threads", type=int)
    parser.add_argument("--override", action="append", default=[], help="Merge a candidate YAML over --config")
    args = parser.parse_args()
    print(train(args.data, args.work, args.version, args.target, args.coins,
                load_config(args.config, args.override), args.threads, args.feature_groups))


if __name__ == "__main__":
    main()
