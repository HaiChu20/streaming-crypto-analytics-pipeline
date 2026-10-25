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


def load_config(path=None):
    with DEFAULT_CONFIG.open() as stream:
        config = yaml.safe_load(stream)
    if path and Path(path).resolve() != DEFAULT_CONFIG.resolve():
        with Path(path).open() as stream:
            update = yaml.safe_load(stream) or {}
        def merge(base, extra):
            for key, value in extra.items():
                if isinstance(value, dict) and isinstance(base.get(key), dict):
                    merge(base[key], value)
                else:
                    base[key] = value
        merge(config, update)
    return config


def validate_config(config):
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


def train(data, work, version, target="a", coins=None, config=None, threads=None):
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
        training = lgb.Dataset(fit.select(features).to_numpy(), label=y_fit,
                               feature_name=features, categorical_feature=["f_coin_id"])
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
            for fraction in config["selective_fractions"]:
                thresholds[threshold_name(source, fraction)] = float(np.quantile(scores, 1 - fraction)) if len(scores) else None
        direction = np.sign(p_valid[:, 0] - p_valid[:, 1])
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
        oof.append(outer)
        fold = {"fold": name, "start_ms": start, "end_ms": end, "best_iteration": best,
                "fit_rows": len(fit), "inner_rows": len(valid), "test_rows": len(test),
                "fit_min_ts_ms": int(fit["ts_ms"].min()), "fit_max_ts_ms": int(fit["ts_ms"].max()),
                "fit_max_label_end_ms": int(fit[f"label_end_ms_{target}"].max()),
                "inner_start_ms": month_shift(start, -int(config["inner_validation_months"])),
                "inner_max_label_end_ms": int(valid[f"label_end_ms_{target}"].max()),
                "prior": prior.tolist(), "thresholds": thresholds, **timeout,
                "ev_threshold": config["ev_threshold"], "calibrators": calibrators}
        (output / f"{name}.json").write_text(json.dumps(fold, indent=2))
        pl.DataFrame({"feature": features, "gain": model.feature_importance(importance_type="gain", iteration=best)}).write_csv(output / f"{name}-importance.csv")
        folds.append(fold)
    pl.concat(oof).sort(["ts_ms", "coin"]).write_parquet(output / "oof.parquet")
    sha = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    metadata = {"target": target, "version": version, "config": config, "git_sha": sha,
                "features": features, "class_order": ["up", "down", "none"], "folds": folds,
                "lockbox_exclusive_ms": lockbox, "num_threads": params["num_threads"],
                "timeout_mean_method": "Signed none-row returns, conditional on predicted direction in inner validation; zero when absent."}
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str))
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--target", required=True, choices=("a", "b"))
    parser.add_argument("--coins", nargs="+")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--threads", type=int)
    args = parser.parse_args()
    print(train(args.data, args.work, args.version, args.target, args.coins,
                load_config(args.config), args.threads))


if __name__ == "__main__":
    main()
