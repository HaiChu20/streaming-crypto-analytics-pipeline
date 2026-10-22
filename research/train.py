"""Run offline walk-forward baselines: PYTHON_DOTENV_DISABLED=1 uv run python -m research.train ROOT."""

import argparse
from datetime import date, datetime, timezone
import json
import hashlib
import math
import os
from pathlib import Path
import resource
import re
import subprocess
import sys

import lightgbm as lgb
import numpy as np
import polars as pl

from research.calendar import Calendar, verify_calendar
from research.dataset import (FEATURE_GROUPS, build_matrix, feature_dates, feature_paths,
                              read_features, select_features)
from research.fitting import fit_booster, inner_split, parse_params, shrinkage_coefficient
from research.labels import NS, load_spec, make_labels, reference_for, reference_present
from research.risk import (PARAMS as STOP_PARAMS, STOP_NAMES, evaluate_stops, fit_stop_models,
                           pinball_losses, predict_stops, stop_metrics)
from research.evaluation import (DAY_NS, date_split, feature_array, memory_estimate,
                                 class_prior, date_intervals_overlap, day_block_permutation,
                                 leak_check_metrics, model_importance, paired_day_block_bootstrap,
                                 probability_losses, probability_scores)
from research.threads import available_cpus, resolve_threads
from research.linear import fit_ridge, ridge_predict

PARAMS = {"verbosity": -1, "seed": 17, "deterministic": True,
          "force_col_wise": True, "num_leaves": 15, "learning_rate": 0.05,
          "min_data_in_leaf": 20, "feature_pre_filter": False}


def effective_model_params(params, risk_params, num_threads):
    direction = dict(PARAMS, **(params or {}), num_threads=num_threads)
    risk = dict(STOP_PARAMS, **(risk_params or {}), num_threads=num_threads)
    return {'direction': direction, 'risk': risk, 'num_threads': num_threads,
            'regressor': dict(direction, objective='regression'),
            'classifier': dict(direction, objective='multiclass', num_class=3),
            'stops': {name: dict(risk, objective='quantile', alpha=int(name[-2:]) / 100) for name in STOP_NAMES}}


def feature_schema(features):
    return {'feature_count': len(features),
            'feature_sha256': hashlib.sha256(json.dumps(features, separators=(',', ':')).encode()).hexdigest()}


def params_argument(value):
    try:
        return parse_params(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


class InsufficientHistory(ValueError):
    """Valid input that cannot yet support the requested walk-forward split."""


def print_peak_rss(stage):
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    gib = peak / (1024**3 if sys.platform == 'darwin' else 1024**2)
    print(f'Peak RSS after {stage}: {gib:.3f} GiB', flush=True)


def walk_forward_folds(tick_ns, horizon_s: int, purge_s: int | None = None,
                       label_available_ns=None, feature_available_ns=None, *,
                       fold_days=7, min_train_days=14, fold_embargo_s=None, max_folds=12):
    """Expanding UTC calendar blocks, with horizon purge plus a time embargo.

    Both label endpoint/emission and feature emission precede the embargo start.
    Only the latest nonempty max_folds blocks allocate index arrays.
    """
    ticks = np.asarray(tick_ns, dtype=np.int64)
    purge_s = horizon_s if purge_s is None else purge_s
    embargo = horizon_s if fold_embargo_s is None else fold_embargo_s
    if horizon_s <= 0 or purge_s < horizon_s or embargo < 0:
        raise ValueError('purge_s must be >= the positive label horizon; embargo >= 0')
    if np.any(np.diff(ticks) <= 0):
        raise ValueError('Need strictly time-ordered unique rows for walk-forward folds')
    if fold_days < 1 or min_train_days < 1 or max_folds < 1:
        raise ValueError('fold_days, min_train_days and max_folds must be positive')
    if not len(ticks):
        raise InsufficientHistory('Insufficient history for requested folds; collect more data')
    known = ticks + horizon_s * NS
    for availability in (label_available_ns, feature_available_ns):
        if availability is not None:
            known = np.maximum(known, np.asarray(availability, dtype=np.int64))
    first = (ticks[0] // DAY_NS + min_train_days) * DAY_NS
    blocks = []
    for start in range(int(first), int(ticks[-1]) + 1, fold_days * DAY_NS):
        left, right = np.searchsorted(ticks, [start, start + fold_days * DAY_NS])
        if right > left:
            blocks.append((start, left, right))
    folds = []
    for start, left, right in blocks[-max_folds:]:
        train = np.flatnonzero((ticks + (purge_s + embargo) * NS < start)
                              & (known + embargo * NS < start))
        if len(train):
            folds.append((train, np.arange(left, right)))
    if not folds:
        raise InsufficientHistory('Insufficient history for requested folds and purge; collect more data')
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


def fit_models(x, y, directions, features, rounds: int, num_threads=None, *, params=None,
               inner=None, early_stopping=False, early_stopping_rounds=50, return_selection=False):
    num_threads = resolve_threads(num_threads)
    params = dict(PARAMS, **(params or {}), num_threads=num_threads)
    regressor, reg_selection, validation = fit_booster(x, y, features, dict(params, objective='regression'),
        rounds, inner=inner, early_stopping=early_stopping, patience=early_stopping_rounds, metric='l2')
    classifier, cls_selection, _ = fit_booster(x, directions + 1, features,
        dict(params, objective='multiclass', num_class=3), rounds, inner=inner,
        early_stopping=early_stopping, patience=early_stopping_rounds, metric='multi_logloss')
    # Classifier has no native % return. Its implied return is probability times
    # each class's mean return, estimated ONLY on this training fold.
    class_means = np.array([y[directions == d].mean() if np.any(directions == d) else 0.0
                            for d in (-1, 0, 1)])
    result = (regressor, classifier, class_means)
    if return_selection:
        shrinkage = None
        if early_stopping:
            shrinkage = {'coefficient': shrinkage_coefficient(y[validation[0]], validation[1])
                          if validation is not None else 1.,
                         'status': 'selected' if validation is not None else 'fallback_identity'}
        return (*result, {'regressor': reg_selection, 'classifier': cls_selection}, shrinkage)
    return result


def volatility_edges(rv):
    values = np.asarray(rv)
    values = values[np.isfinite(values) & (values >= 0)]
    return np.quantile(values, [1 / 3, 2 / 3]).tolist() if len(values) else None


def regime_assignments(ticks, rv, edges):
    groups = np.full(len(ticks), -1)
    valid = np.isfinite(rv) & (rv >= 0)
    if edges is not None:
        groups[valid] = np.searchsorted(edges, rv[valid], side='right')
    days = ticks // DAY_NS
    return {'volatility_tercile': groups, 'utc_hour_4h': (ticks % DAY_NS) // (3600 * 4 * NS),
            'week_part': ((days + 3) % 7 >= 5).astype(int)}


def forecast_metrics(actual, predictions, probabilities):
    """Return metrics stay unchanged; class probabilities add proper scores."""
    result = {}
    for name, prediction in predictions.items():
        probs = probabilities.get(name)
        result[name] = metrics(actual, prediction, np.argmax(probs, axis=1) - 1 if probs is not None else None)
        if probs is not None:
            result[name].update(probability_scores(np.sign(actual), probs))
    return result


def direction_regimes(actuals, predictions, probabilities, regimes):
    result = {}
    for axis, labels in {'volatility_tercile': ['low', 'medium', 'high'],
                         'utc_hour_4h': ['00–04', '04–08', '08–12', '12–16', '16–20', '20–24'],
                         'week_part': ['weekday', 'weekend']}.items():
        groups = regimes[axis]
        result[axis] = {label: forecast_metrics(actuals[groups == i],
                           {name: values[groups == i] for name, values in predictions.items()},
                           {name: values[groups == i] for name, values in probabilities.items()})
                       for i, label in enumerate(labels)}
    return result


def oof_forecasts(frame):
    predictions = {name: frame[column].to_numpy() for name, column in {
        'regressor': 'regressor', 'classifier': 'classifier_expected', 'zero': 'zero',
        'momentum': 'momentum', 'reversal': 'reversal', 'prior': 'prior_expected'}.items()}
    predictions.update({name: frame[name].to_numpy() for name in ('ridge', 'regressor_shrunk') if name in frame.columns})
    probabilities = {name: frame.select([prefix + 'p_' + c for c in ('down', 'flat', 'up')]).to_numpy()
                     for name, prefix in [('classifier', ''), ('prior', 'prior_')]}
    return predictions, probabilities


def bootstrap_comparisons(frame):
    """Paired comparisons on the saved OOF rows and their common finite masks."""
    y, directions = frame['return_pct'].to_numpy(), frame['direction'].to_numpy()
    predictions, probabilities = oof_forecasts(frame)
    cls_direction = np.argmax(probabilities['classifier'], axis=1) - 1
    reversal = predictions['reversal']
    valid_direction = ((directions != 0) & np.isfinite(y) & np.isfinite(reversal)
                       & np.isfinite(probabilities['classifier']).all(axis=1))
    differences = {
        'classifier_vs_reversal_directional_accuracy': np.where(valid_direction,
            (cls_direction == directions).astype(np.float64) - (np.sign(reversal) == directions), np.nan),
        'classifier_vs_prior_log_loss': (probability_losses(directions, probabilities['classifier'])[0]
                                        - probability_losses(directions, probabilities['prior'])[0]),
        'regressor_vs_zero_mse': (y - predictions['regressor']) ** 2 - (y - predictions['zero']) ** 2,
        'classifier_expected_vs_zero_mse': (y - predictions['classifier']) ** 2 - (y - predictions['zero']) ** 2,
    }
    for name in ('ridge', 'regressor_shrunk'):
        if name in predictions:
            differences[name + '_vs_zero_mse'] = (y - predictions[name]) ** 2 - (y - predictions['zero']) ** 2
    for name in STOP_NAMES:
        actual = frame[f'adverse_{name.split("_")[1]}_bps'].to_numpy()
        q = int(name[-2:]) / 100
        differences[name + '_vs_unconditional_pinball'] = (
            pinball_losses(actual, frame[name].to_numpy(), q)
            - pinball_losses(actual, frame[name + '_unconditional'].to_numpy(), q))
    return paired_day_block_bootstrap(frame['tick_ns'].to_numpy(), differences)


def risk_calibration(adverse, rv):
    result = {}
    for side, values in adverse.items():
        valid = np.isfinite(values)
        positive = valid & np.isfinite(rv) & (rv > 0)
        for q in (.8, .9):
            result[f'stop_{side}_q{int(100*q)}'] = {
                'constant': float(np.quantile(values[valid], q)) if valid.any() else None,
                'k': float(np.quantile(values[positive] / rv[positive], q)) if positive.any() else None,
                'edges': volatility_edges(rv[valid])}
    return result


def train_target(rows: pl.DataFrame, target: str, horizon_s: int, output_root: str | Path,
                 version: str, stride_s: int = 10,
                 purge_s: int | None = None, rounds: int = 100,
                 spec: dict | None = None, *, split=None, lockbox_pct=20, lockbox_from=None,
                 embargo_days=1, fold_days=7, min_train_days=14, fold_embargo_s=None,
                 max_folds=12, shuffle_test=False, max_memory_gb=None, num_threads=None,
                 params=None, risk_params=None, early_stopping=False, inner_valid_days=7,
                 early_stopping_rounds=50, exclude_groups=(), exclude_features=()) -> dict:
    num_threads = resolve_threads(num_threads)
    params, risk_params = parse_params(params), parse_params(risk_params)
    if inner_valid_days < 1 or early_stopping_rounds < 1:
        raise ValueError('inner_valid_days and early_stopping_rounds must be positive')
    spec = spec or load_spec()
    if horizon_s not in spec["horizons_s"]:
        raise ValueError("Horizon is not in the versioned label spec")
    if rounds < 1 or not version or Path(version).name != version or version in {".", ".."}:
        raise ValueError("Need positive rounds and a single directory name for version")
    if (stride_s < 1 or min(fold_days, min_train_days, max_folds) < 1
            or (fold_embargo_s is not None and fold_embargo_s < 0)
            or (purge_s is not None and purge_s < horizon_s)):
        raise ValueError("Need positive stride/folds and purge_s >= horizon_s")
    split = split or date_split(rows['tick_ns'].to_numpy(), lockbox_pct, lockbox_from,
                                embargo_days, max(spec['horizons_s']))
    # Neither label endpoints nor path labels can borrow embargo/lockbox rows.
    rows = rows.filter(pl.col('tick_ns') < split['development_end_ns'])
    skipped = {"target": target, "horizon_s": horizon_s, "version": version,
               "status": "skipped", "reference": reference_for(target, spec), "date_split": split,
               'model_params': effective_model_params(params, risk_params, num_threads),
               "labeled_rows": 0, "matrix_rows": 0}
    if not reference_present(rows, target, spec):
        return dict(skipped, reason="not enough labeled rows: no development reference rows" if split["development"] is None
                    else "reference instrument has no rows in input")
    calendar = Calendar.load(spec["calendar"]) if spec.get("calendar") else None
    matrix, features = build_matrix(rows, target, stride_s, spec, calendar=calendar)
    features, exclusions = select_features(features, exclude_groups, exclude_features)
    skipped.update(features=features, exclusions=exclusions, **feature_schema(features))
    labeled = matrix.join(make_labels(rows, target, horizon_s, spec), on="tick_ns", how="left")
    # A delayed emission after the target endpoint cannot be a forecast.
    data = labeled.filter(pl.col("return_pct").is_not_null()
                          & (pl.col("available_ns") < pl.col("label_end_ns"))
                          & (pl.col("label_end_ns") < split['development_end_ns'])
                          & (pl.col("label_available_ns") < split['development_end_ns'])).sort("tick_ns")
    print_peak_rss(f'matrix/labels ({target}/{horizon_s})')
    if not features or data.is_empty():
        return dict(skipped, reason="not enough labeled rows", matrix_rows=matrix.height,
                    labeled_rows=data.height)
    ticks = data["tick_ns"].to_numpy()
    try:
        folds = walk_forward_folds(ticks, horizon_s, purge_s,
                                   data["label_available_ns"].to_numpy(), data["available_ns"].to_numpy(),
                                   fold_days=fold_days, min_train_days=min_train_days,
                                   fold_embargo_s=fold_embargo_s, max_folds=max_folds)
    except InsufficientHistory as exc:
        return dict(skipped, reason=f"not enough labeled rows: {exc}",
                    matrix_rows=matrix.height, labeled_rows=data.height)
    memory = memory_estimate(data.height, len(features), max_memory_gb)
    x = feature_array(data, features)
    y = data["return_pct"].to_numpy()
    directions = data["direction"].to_numpy().astype(np.int32)
    momentum = (data["momentum_return_300s"].to_numpy() if horizon_s == 300 else
                data["momentum_return_900s"].to_numpy() * (horizon_s / 900))
    adverse = {side: data[f"adverse_{side}_bps"].to_numpy() for side in ("long", "short")}
    ref = reference_for(target, spec)
    rv_column = f"{ref['exchange']}_{ref['market']}_rv_300s"
    rv = data[rv_column].to_numpy() if rv_column in data.columns else np.full(data.height, np.nan)
    def choose_inner(indices):
        if not early_stopping:
            return None
        return inner_split(ticks[indices], data['label_end_ns'].to_numpy()[indices],
            data['label_available_ns'].to_numpy()[indices], horizon_s, inner_valid_days,
            purge_s=purge_s, embargo_s=fold_embargo_s,
            feature_available_ns=data['available_ns'].to_numpy()[indices])

    inner_splits = [choose_inner(train) for train, _ in folds]
    fitting = dict(params=params, early_stopping=early_stopping, early_stopping_rounds=early_stopping_rounds)
    stop_folds, stop_summary, stop_oof = evaluate_stops(
        x, adverse, rv, features, folds, rounds, num_threads=num_threads, return_oof=True,
        params=risk_params, inner_splits=inner_splits, early_stopping=early_stopping,
        early_stopping_rounds=early_stopping_rounds)
    results, oof_frames = [], []
    for i, (train, test) in enumerate(folds, 1):
        inner = inner_splits[i - 1]
        ridge, ridge_info = fit_ridge(x, y, train, tuple(train[v] for v in inner) if inner is not None else None)
        regressor, classifier, class_means, selected, shrinkage = fit_models(
            x[train], y[train], directions[train], features, rounds, num_threads=num_threads,
            inner=inner, return_selection=True, **fitting
        )
        probabilities = classifier.predict(x[test], num_threads=num_threads)
        prior = np.tile(class_prior(directions[train]), (len(test), 1))
        predictions = {"regressor": regressor.predict(x[test], num_threads=num_threads),
                       "classifier": probabilities @ class_means,
                       "zero": np.zeros(len(test)), "momentum": momentum[test],
                       "reversal": -momentum[test], "prior": prior @ class_means,
                       'ridge': ridge_predict(ridge, x, test)}
        if shrinkage is not None:
            predictions['regressor_shrunk'] = predictions['regressor'] * shrinkage['coefficient']
        class_probs = {'classifier': probabilities, 'prior': prior}
        fold_metrics = forecast_metrics(y[test], predictions, class_probs)
        edges = volatility_edges(rv[train])
        regimes = regime_assignments(ticks[test], rv[test], edges)
        saved = {"regressor": predictions['regressor'], 'classifier_expected': predictions['classifier'],
                 'zero': predictions['zero'], 'momentum': predictions['momentum'],
                 'reversal': predictions['reversal'], 'prior_expected': predictions['prior'],
                 'p_up_minus_down': probabilities[:, 2] - probabilities[:, 0],
                 **regimes, **stop_oof[i - 1]}
        saved.update({name: predictions[name] for name in ('ridge', 'regressor_shrunk') if name in predictions})
        for prefix, probs in [('', probabilities), ('prior_', prior)]:
            saved.update({prefix + 'p_' + name: probs[:, j] for j, name in enumerate(('down', 'flat', 'up'))})
        results.append({"fold": i, "train_rows": len(train), "test_rows": len(test),
                        "train_start_ns": int(ticks[train[0]]), "train_end_ns": int(ticks[train[-1]]),
                        "train_label_end_ns": int(data["label_end_ns"][train].max()),
                        "train_label_available_ns": int(data["label_available_ns"][train].max()),
                        "train_feature_available_ns": int(data["available_ns"][train].max()),
                        "test_start_ns": int(ticks[test[0]]), "test_end_ns": int(ticks[test[-1]]),
                        "gap_s": float((ticks[test[0]] - ticks[train[-1]]) / NS),
                        "train_flat_labels": int((directions[train] == 0).sum()),
                        "test_flat_labels": int((directions[test] == 0).sum()),
                        "metrics": fold_metrics, "stop_metrics": stop_folds[i - 1],
                        'volatility_tercile_edges': edges,
                        'chosen_rounds': dict(selected, **{name: stop_folds[i - 1][name]['selection'] for name in STOP_NAMES}),
                        'ridge': ridge_info, 'regressor_shrinkage': shrinkage})
        if shuffle_test:
            shuffle_probs = np.full((len(test), 3), np.nan)
            shuffle_predictions = {name: np.full(len(test), np.nan) for name in ('regressor', 'classifier')}
            if len(np.unique(ticks[train] // DAY_NS)) < 2:
                results[-1]['shuffle_test'] = {'status': 'insufficient training days'}
            else:
                permutation = day_block_permutation(ticks[train], seed=17 + i)
                shuffled_reg, shuffled_clf, means = fit_models(
                    x[train], y[train][permutation], directions[train][permutation], features, rounds,
                    num_threads=num_threads, inner=inner, **fitting)
                shuffle_probs = shuffled_clf.predict(x[test], num_threads=num_threads)
                shuffle_predictions = {'regressor': shuffled_reg.predict(x[test], num_threads=num_threads),
                                       'classifier': shuffle_probs @ means}
                results[-1]['shuffle_test'] = forecast_metrics(y[test], shuffle_predictions, {'classifier': shuffle_probs})
            saved.update({'shuffle_regressor': shuffle_predictions['regressor'],
                          'shuffle_classifier_expected': shuffle_predictions['classifier'],
                          **{'shuffle_p_' + name: shuffle_probs[:, j] for j, name in enumerate(('down', 'flat', 'up'))}})
        fold_frame = data[test].select('tick_ns', 'available_ns', 'label_end_ns', 'label_available_ns',
                                       'return_pct', 'direction', 'adverse_long_bps', 'adverse_short_bps')
        oof_frames.append(fold_frame.with_columns(pl.lit(i, dtype=pl.Int32).alias('fold'),
                           *[pl.Series(name, values, nan_to_null=True) for name, values in saved.items()]))
    oof = pl.concat(oof_frames, rechunk=False)
    del oof_frames, stop_oof
    actuals = oof['return_pct'].to_numpy()
    predictions, probabilities = oof_forecasts(oof)
    summary = forecast_metrics(actuals, predictions, probabilities)
    regime_summary = direction_regimes(actuals, predictions, probabilities,
                                       {axis: oof[axis].to_numpy() for axis in regimes})
    bootstrap = bootstrap_comparisons(oof)
    shuffle_summary = None
    if shuffle_test:
        shuffled_predictions = {name: oof[column].to_numpy() for name, column in
                                [('regressor', 'shuffle_regressor'), ('classifier', 'shuffle_classifier_expected')]}
        shuffled_probs = oof.select('shuffle_p_down', 'shuffle_p_flat', 'shuffle_p_up').to_numpy()
        effective_days = len(np.unique(oof['tick_ns'].to_numpy()[np.isfinite(shuffled_predictions['classifier'])] // DAY_NS))
        shuffle_summary = {name: leak_check_metrics(dict(value, effective_days=effective_days))
                           for name, value in forecast_metrics(actuals, shuffled_predictions,
                                                              {'classifier': shuffled_probs}).items()}
    all_rows = np.arange(len(x))
    final_inner = choose_inner(all_rows)
    ridge, ridge_info = fit_ridge(x, y, all_rows, final_inner)
    regressor, classifier, class_means, final_selection, final_shrinkage = fit_models(
        x, y, directions, features, rounds, num_threads=num_threads,
        inner=final_inner, return_selection=True, **fitting)
    directory = Path(output_root) / "models" / target / str(horizon_s) / version
    directory.mkdir(parents=True, exist_ok=False)
    regressor.save_model(str(directory / "regressor.txt"))
    classifier.save_model(str(directory / "classifier.txt"))
    stop_models, stop_selection = fit_stop_models(x, adverse, features, rounds, num_threads=num_threads,
        params=risk_params, inner=final_inner, early_stopping=early_stopping,
        early_stopping_rounds=early_stopping_rounds, return_selection=True)
    final_selection.update(stop_selection)
    np.savez(directory / 'ridge.npz', **ridge)
    print_peak_rss(f'fitting ({target}/{horizon_s})')
    for name, model in stop_models.items():
        model.save_model(str(directory / f"{name}.txt"))
    data.select("asset", "tick_ns", "available_ns", "label_end_ns", "label_available_ns",
                "engine_version", "replay_mode").write_parquet(directory / "training_rows.parquet")
    oof.write_parquet(directory / "oof.parquet")
    report = {
        "target": target, "horizon_s": horizon_s, "version": version, "status": "trained",
        "date_split": split, "memory": memory, "num_threads": num_threads,
        "feature_dtype": "float64; shared research.evaluation.feature_array in training and inference; labels float64",
        "regimes": regime_summary, "shuffle_test": shuffle_summary, "bootstrap": bootstrap,
        "oof_file": "oof.parquet", "oof_rows": oof.height,
        "flat_labels": int((directions == 0).sum()), "oof_flat_labels": int((oof["direction"] == 0).sum()),
        "model_params": effective_model_params(params, risk_params, num_threads),
        'exclusions': exclusions, **feature_schema(features),
        'early_stopping': {'enabled': early_stopping, 'inner_valid_days': inner_valid_days,
                           'patience': early_stopping_rounds},
        'chosen_rounds': final_selection, 'ridge': ridge_info, 'ridge_file': 'ridge.npz',
        'regressor_shrinkage': final_shrinkage,
        "fold_config": {"fold_days": fold_days, "min_train_days": min_train_days, "max_folds": max_folds,
                        "fold_embargo_s": horizon_s if fold_embargo_s is None else fold_embargo_s},
        "importance": {name: model_importance(model, x, features, num_threads=num_threads) for name, model in
                       {"regressor": regressor, "classifier": classifier, **stop_models}.items()},
        "risk_calibration": risk_calibration(adverse, rv),
        "volatility_edges": volatility_edges(rv),
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
        "stop_models": {name: {"file": f"{name}.txt", "quantile": int(name[-2:]) / 100,
                               "label": f"adverse_{name.split('_')[1]}_bps"}
                        for name in stop_models},
        "stop_labeled_rows": {side: int(np.isfinite(a).sum()) for side, a in adverse.items()},
        "stop_metrics": stop_summary,
        "stop_notes": "Positive mid-price excursion in (t,t+h]; every whole second must be valid, "
                      "non-warm and positive. No spread/slippage. Hit means actual > predicted stop; "
                      "target hit rate 1-q. Predictions clipped to zero and q90 raised to q80. "
                      "Baselines and rv_300s terciles fitted on each purged training fold only; "
                      "k=quantile(adverse/rv_300s) for positive finite rv. Missing/negative rv excluded "
                      "from volatility baseline/regimes; zero rv predicts zero if k is available. "
                      "Overall metrics pool held-out rows and their fold-local regime assignments.",
        "classifier_class_order": [-1, 0, 1], "classifier_class_mean_returns": class_means.tolist(),
        "classifier_class_prior": class_prior(directions).tolist(),
        "metric_notes": "Returns/MAE/RMSE in percentage points. Classifier implied return = "
                        "probabilities @ training-only class mean returns; direction = argmax. "
                        "Directional accuracy excludes actual flats. Undefined IC is null. "
                        "Momentum = trailing 300 s return for h=300, otherwise 900 s return * horizon/900; "
                        "Reversal = exact negative momentum. Prior = add-one training-fold class frequencies; "
                        "prior implied return uses the training-fold class mean returns. Ties use class order [-1,0,1]. "
                        "Log loss clips at 1e-15; Brier sums squared errors over three classes. "
                        "missing endpoints excluded "
                        "from its metrics (see n). Zero predicts flat, scoring 0 on non-flat directions.",
        "availability": "Prediction time is max available_ns of same-tick inputs. "
                        "No later tick enters X. Train labels must be emitted before test tick.",
        "warning": "Several weeks are required for meaningful walk-forward results. "
                   "A roughly 9-hour sample only verifies that the pipeline runs without leakage. "
                   "These metrics are not trading P&L; no fees/spread/funding execution backtest.",
        "model_dir": str(directory.resolve()),
    }
    if calendar:
        report["calendar"] = calendar.metadata(int(ticks[-1]))
    (directory / "metadata.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    from research.report import write_model_report
    write_model_report(report, directory)
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


def lockbox_reports(output, version, targets=None, horizons=None):
    """Read the frozen model configurations used for loading and evaluation."""
    paths = sorted(p / 'metadata.json' for p in (Path(output) / 'models').glob('*/*/*')
                   if p.name == version and (p / 'metadata.json').is_file())
    reports = [json.loads(path.read_text()) for path in paths]
    reports = [r for r in reports if (not targets or r['target'] in targets)
               and (not horizons or r['horizon_s'] in horizons)]
    if not reports:
        raise ValueError('--open-lockbox needs already-trained models of --version')
    return reports


def open_lockbox(rows, output, version, targets=None, horizons=None, acknowledge_reuse=False,
                 max_memory_gb=None, num_threads=None, input_days=None, lockbox_ledger=None):
    """Evaluate saved boosters and frozen calibration only. No fitting code here."""
    num_threads = resolve_threads(num_threads)
    import fcntl
    from research.report import write_model_report, generate_report

    if not version or Path(version).name != version or version in {'.', '..'}:
        raise ValueError('version must be a single directory name')
    reports = lockbox_reports(output, version, targets, horizons)
    ranges = {json.dumps(r['date_split']['lockbox'], sort_keys=True) for r in reports}
    if 'null' in ranges:
        raise ValueError('This version has no lockbox days')
    ledger_path = Path(lockbox_ledger) if lockbox_ledger is not None else Path(output) / 'lockbox_ledger.jsonl'
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open('a+') as ledger:
        fcntl.flock(ledger, fcntl.LOCK_EX)
        ledger.seek(0)
        previous = [json.loads(line) for line in ledger if line.strip()]
        reused = {span: any(date_intervals_overlap(json.loads(span), item['lockbox_range'])
                            for item in previous) for span in ranges}
        if any(reused.values()):
            print('WARNING: LOCKBOX REUSE — these results are no longer untouched!', flush=True)
            if not acknowledge_reuse:
                raise ValueError('Pass --i-understand-lockbox-reuse to reopen this lockbox range')
        outcomes = {}
        try:
            for report in reports:
                split, spec = report['date_split'], report['label_spec']
                stop_day = date.fromisoformat(split['lockbox']['last_day'])
                end_ns = ((stop_day - date(1970, 1, 1)).days + 1) * DAY_NS
                held = rows.filter((pl.col('tick_ns') >= split['lockbox_start_ns'])
                                   & (pl.col('tick_ns') < end_ns))
                observed = None
                if input_days is not None:
                    days = [day for day in input_days
                            if split['lockbox']['first_day'] <= day <= split['lockbox']['last_day']]
                    observed = ({'first_day': days[0], 'last_day': days[-1], 'days': len(days)} if days else None)
                elif held.height:
                    observed = date_split(held['tick_ns'].to_numpy(),
                                          lockbox_from=split['lockbox']['first_day'],
                                          max_horizon_s=max(spec['horizons_s']))['lockbox']
                if observed != split['lockbox']:
                    raise ValueError('Lockbox feature dates differ from the frozen training split')
                calendar = Calendar.load(spec['calendar']) if spec.get('calendar') else None
                if calendar:
                    verify_calendar(calendar, report)
                matrix, _ = build_matrix(held, report['target'], report['stride_s'], spec, calendar=calendar)
                data = matrix.join(make_labels(held, report['target'], report['horizon_s'], spec),
                                   on='tick_ns', how='left').filter(
                    pl.col('return_pct').is_not_null() & (pl.col('available_ns') < pl.col('label_end_ns')))
                print_peak_rss(f'matrix/labels (lockbox {report["target"]}/{report["horizon_s"]})')
                memory_estimate(data.height, len(report['features']), max_memory_gb)
                x = feature_array(data, report['features'])
                directory = Path(report['model_dir'])
                reg = lgb.Booster(model_file=str(directory / 'regressor.txt'))
                clf = lgb.Booster(model_file=str(directory / 'classifier.txt'))
                y = data['return_pct'].to_numpy()
                probs = clf.predict(x, num_threads=num_threads) if len(x) else np.empty((0, 3))
                h = report['horizon_s']
                momentum = (data['momentum_return_300s'].to_numpy() if h == 300 else
                            data['momentum_return_900s'].to_numpy() * h / 900)
                predictions = {'regressor': reg.predict(x, num_threads=num_threads) if len(x) else np.array([]),
                               'classifier': probs @ report['classifier_class_mean_returns'],
                               'zero': np.zeros(len(x)), 'momentum': momentum, 'reversal': -momentum}
                if report.get('ridge_file'):
                    with np.load(directory / report['ridge_file'], allow_pickle=False) as saved_ridge:
                        predictions['ridge'] = ridge_predict(dict(saved_ridge), x)
                if report.get('regressor_shrinkage') is not None:
                    predictions['regressor_shrunk'] = predictions['regressor'] * report['regressor_shrinkage']['coefficient']
                class_probs = {'classifier': probs}
                if 'classifier_class_prior' in report:
                    class_probs['prior'] = np.tile(report['classifier_class_prior'], (len(x), 1))
                    predictions['prior'] = class_probs['prior'] @ report['classifier_class_mean_returns']
                measured = forecast_metrics(y, predictions, class_probs)
                ref = reference_for(report['target'], spec)
                rv_col = f"{ref['exchange']}_{ref['market']}_rv_300s"
                rv = data[rv_col].to_numpy() if rv_col in data.columns else np.full(len(x), np.nan)
                stop_models = {name: lgb.Booster(model_file=str(directory / config['file']))
                               for name, config in report['stop_models'].items()}
                stops = predict_stops(stop_models, x, num_threads=num_threads) if len(x) else {n: np.array([]) for n in stop_models}
                risk = {}
                for name, p in stops.items():
                    side, q = name.split('_')[1], int(name[-2:]) / 100
                    actual = data[f'adverse_{side}_bps'].to_numpy()
                    calibration = report['risk_calibration'][name]
                    constant = np.full(len(x), calibration['constant'])
                    scaled = np.where(np.isfinite(rv) & (rv >= 0), rv * calibration['k'], np.nan) if calibration['k'] is not None else np.full(len(x), np.nan)
                    groups = regime_assignments(data['tick_ns'].to_numpy(), rv, calibration['edges'])['volatility_tercile']
                    risk[name] = {'quantile': q, 'target_hit_rate': 1-q,
                                  **{key: stop_metrics(actual, values, q, groups) for key, values in
                                     [('model', p), ('unconditional', constant), ('volatility_scaled', scaled)]}}
                report['lockbox_results'] = {'range': split['lockbox'], 'metrics': measured, 'stop_metrics': risk,
                    'num_threads': num_threads,
                    'flat_labels': int((data['direction'] == 0).sum()),
                    'regimes': direction_regimes(y, predictions, class_probs,
                        regime_assignments(data['tick_ns'].to_numpy(), rv, report['volatility_edges'])),
                    'notes': 'Saved models only; no refit or tuning; baseline calibration frozen on development.'}
                outcomes[f"{report['target']}/{h}"] = report['lockbox_results']
                (directory / 'metadata.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
                write_model_report(report, directory)
        finally:
            # Failed attempts also consume the untouched status if labels were opened.
            for span in sorted(ranges):
                entry = {'version': version, 'utc_time': datetime.now(timezone.utc).isoformat(),
                         'lockbox_range': json.loads(span),
                         'reused': reused[span],
                         'metrics_digest': hashlib.sha256(json.dumps(outcomes, sort_keys=True, allow_nan=False).encode()).hexdigest(),
                         'completed_targets': sorted(outcomes)}
                ledger.write(json.dumps(entry, allow_nan=False) + '\n')
            ledger.flush()
            os.fsync(ledger.fileno())
    result_path = Path(output) / f'research_report_{version}.json'
    result = json.loads(result_path.read_text()) if result_path.exists() else {'results': reports}
    updated = {(r['target'], r['horizon_s']): r for r in reports}
    result['results'] = [updated.get((r['target'], r['horizon_s']), r) for r in result['results']]
    result_path.write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    generate_report(output, version)
    return result


def write_experiment(output, args, argv, targets, horizons, reports, split, rows, error, num_threads):
    """One locked append per CLI invocation, including skipped or failed work."""
    import fcntl
    git = {'sha': None, 'dirty': None}
    try:
        repo = Path(__file__).resolve().parents[1]
        git['sha'] = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=repo, capture_output=True,
                                    text=True, check=True, timeout=5).stdout.strip()
        git['dirty'] = bool(subprocess.run(['git', 'status', '--porcelain'], cwd=repo, capture_output=True,
                                           text=True, check=True, timeout=5).stdout.strip())
    except (OSError, subprocess.SubprocessError):
        pass
    by_key = {(r['target'], r['horizon_s']): r for r in reports}
    models = []
    pairs = list(by_key) if (args.open_lockbox or args.list_feature_groups) and by_key else [(t, h) for t in targets for h in horizons]
    for target, horizon in pairs:
        report = by_key.get((target, horizon), {})
        features = report.get('features')
        measured = report.get('lockbox_results', {}) if args.open_lockbox else report
        models.append({'target': target, 'horizon_s': horizon,
            'status': ('failed' if error else 'evaluated') if args.open_lockbox else report.get('status', 'failed'),
            'reason': report.get('reason') if report else error, 'metrics': measured.get('metrics'),
            'stop_metrics': measured.get('stop_metrics'), 'split': report.get('date_split'),
            'stride_s': report.get('stride_s', args.stride_s), 'rounds': report.get('rounds', args.rounds),
            'params': report.get('model_params'),
            'exclusions': report.get('exclusions'), 'chosen_rounds': report.get('chosen_rounds'),
            'feature_count': len(features) if features is not None else None,
            'feature_sha256': hashlib.sha256(json.dumps(features, separators=(',', ':')).encode()).hexdigest()
                              if features is not None else None})
    if split is None and reports:
        splits = [r.get('date_split') for r in reports]
        split = splits[0] if all(s == splits[0] for s in splits) else splits
    entry = {'version': args.version, 'utc_time': datetime.now(timezone.utc).isoformat(), 'git': git,
             'argv': list(argv), 'mode': 'list_feature_groups' if args.list_feature_groups else ('open_lockbox' if args.open_lockbox else 'train'),
             'targets': targets, 'horizons_s': horizons, 'stride_s': args.stride_s, 'rounds': args.rounds,
             'params': effective_model_params(args.params, args.risk_params, num_threads),
             'exclude_groups': args.exclude_group, 'exclude_features': args.exclude_features,
             'split': split, 'engine_versions': sorted(rows['engine_version'].unique().drop_nulls().to_list())
                       if rows is not None else [], 'status': 'failed' if error else 'completed',
             'error': error, 'results': models}
    Path(output).mkdir(parents=True, exist_ok=True)
    with (Path(output) / 'experiments.jsonl').open('a') as ledger:
        fcntl.flock(ledger, fcntl.LOCK_EX)
        ledger.write(json.dumps(entry, allow_nan=False) + '\n')
        ledger.flush()
        os.fsync(ledger.fileno())


def main(argv=None):
    from research.report import generate_report
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path, help='Contains features/date=D/features.parquet')
    parser.add_argument('--output-root', type=Path)
    parser.add_argument('--target', action='append', choices=['BTC_spot', 'BTC_perp', 'ETH_spot', 'ETH_perp'])
    parser.add_argument('--horizon-s', action='append', type=int, choices=[300, 900, 1800])
    parser.add_argument('--threads', type=int, default=available_cpus(),
                        help='LightGBM CPU threads; defaults to the CPUs available to this process')
    parser.add_argument('--stride-s', type=int, default=10)
    parser.add_argument('--purge-s', type=int)
    parser.add_argument('--rounds', type=int, default=100)
    parser.add_argument('--params', type=params_argument, default={}, help='Inline JSON object or .json file; return/direction parameters')
    parser.add_argument('--risk-params', type=params_argument, default={}, help='Inline JSON object or .json file; stop parameters')
    parser.add_argument('--early-stopping', action='store_true')
    parser.add_argument('--inner-valid-days', type=int, default=7)
    parser.add_argument('--early-stopping-rounds', type=int, default=50)
    parser.add_argument('--exclude-group', action='append', default=[], choices=sorted(FEATURE_GROUPS))
    parser.add_argument('--exclude-features', action='append', default=[], metavar='REGEX')
    parser.add_argument('--list-feature-groups', action='store_true')
    parser.add_argument('--version')
    parser.add_argument('--lockbox-pct', type=float, default=20)
    parser.add_argument('--lockbox-from')
    parser.add_argument('--embargo-days', type=int, default=1)
    parser.add_argument('--fold-days', type=int, default=7)
    parser.add_argument('--min-train-days', type=int, default=14)
    parser.add_argument('--fold-embargo-s', type=int)
    parser.add_argument('--max-folds', type=int, default=12)
    parser.add_argument('--shuffle-test', action='store_true')
    parser.add_argument('--max-memory-gb', type=float)
    parser.add_argument('--open-lockbox', action='store_true')
    parser.add_argument('--lockbox-ledger', type=Path,
                        help='Reuse ledger; defaults to ROOT/lockbox_ledger.jsonl even with --output-root')
    parser.add_argument('--i-understand-lockbox-reuse', action='store_true')
    parser.add_argument('--upload', action='store_true')
    args = parser.parse_args(argv)
    if args.open_lockbox and not args.version:
        parser.error('--open-lockbox requires the already-trained --version')
    args.version = args.version or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    if not args.version or Path(args.version).name != args.version or args.version in {'.', '..'}:
        parser.error('--version must be a single directory name')
    output = args.output_root or (args.root.parent if args.root.is_file() else args.root)
    reports, targets, horizons = [], args.target or [], args.horizon_s or []
    split = rows = num_threads = error = None
    invocation_argv = list(argv) if argv is not None else sys.argv[1:]
    try:
        num_threads = resolve_threads(args.threads)
        paths = feature_paths(args.root)
        spec = load_spec()
        targets = args.target or [f'{asset}_{market}' for asset, markets in spec['references'].items() for market in markets]
        horizons = args.horizon_s or spec['horizons_s']
        load_stride = args.stride_s
        if args.open_lockbox:
            frozen = lockbox_reports(output, args.version, args.target, args.horizon_s)
            reports = frozen
            targets = sorted({report['target'] for report in frozen})
            horizons = sorted({report['horizon_s'] for report in frozen})
            # A shared read must retain full payloads for every saved model's stride.
            load_stride = math.gcd(*(report['stride_s'] for report in frozen))
        try:
            input_days = feature_dates(paths)
        except ValueError:
            input_days = None  # Single-file/legacy inputs still derive dates from full rows.
        split = None
        load_paths = paths
        before_ns = None
        if input_days is not None and not args.open_lockbox:
            split = date_split([], args.lockbox_pct, args.lockbox_from, args.embargo_days,
                               max(spec['horizons_s']), input_days=input_days)
            before_ns = split['development_end_ns']
            load_paths = [p for p in paths if feature_dates([p])[0] < split['embargo']['first_day']]
            # Preserve the input schema for the ordinary no-development-days skip.
            load_paths = load_paths or paths[:1]
        if args.list_feature_groups:
            target, path = targets[0], load_paths[0]
            if input_days is not None:
                first = (date.fromisoformat(feature_dates([path])[0]) - date(1970, 1, 1)).days * DAY_NS
            else:
                first = pl.scan_parquet(path).select(pl.col('tick_ns').min()).collect().item()
                if first is None:
                    raise ValueError('No feature rows to list')
            end = (first // DAY_NS + 1) * DAY_NS
            rows = read_features([path], targets=[target], spec=spec, stride_s=args.stride_s,
                                 before_ns=min(end, before_ns) if before_ns is not None else end)
            _, features = build_matrix(rows, target, args.stride_s, spec)
            features, exclusions = select_features(features, args.exclude_group, args.exclude_features)
            counts = {name: sum(bool(re.search(pattern, f)) for f in features) for name, pattern in FEATURE_GROUPS.items()}
            for name, count in counts.items():
                print(f'{name}: {count}', flush=True)
            print('Groups may overlap (e.g. cross-venue DVOL is in volatility and cross); '
                  'interpret ablations per group, not by summing group results.', flush=True)
            listing = {'target': target, 'horizon_s': None, 'status': 'listed', 'features': features,
                       'feature_groups': counts, 'exclusions': exclusions, **feature_schema(features)}
            targets, horizons, reports = [target], [], [listing]
            return listing
        rows = read_features(load_paths, targets=targets, spec=spec,
                             stride_s=load_stride if input_days is not None or args.open_lockbox else None,
                             before_ns=before_ns)
        print_peak_rss('loading')
        if input_days is None:
            input_days = feature_dates(paths, rows['tick_ns'].to_numpy())
        if args.open_lockbox:
            root = args.root.parent if args.root.is_file() else args.root
            result = open_lockbox(rows, output, args.version, args.target, args.horizon_s,
                                  args.i_understand_lockbox_reuse, args.max_memory_gb,
                                  num_threads=num_threads, input_days=input_days,
                                  lockbox_ledger=args.lockbox_ledger or root / 'lockbox_ledger.jsonl')
            opened = {(r['target'], r['horizon_s']) for r in frozen}
            reports = [r for r in result.get('results', frozen) if (r['target'], r['horizon_s']) in opened]
            return result
        split = split or date_split(rows['tick_ns'].to_numpy(), args.lockbox_pct, args.lockbox_from,
                                   args.embargo_days, max(spec['horizons_s']), input_days=input_days)
        print('Date split (UTC): ' + json.dumps(split), flush=True)
        reports = []
        for target in targets:
            for horizon in horizons:
                report = train_target(rows, target, horizon, output, args.version, args.stride_s,
                                      args.purge_s, args.rounds, spec, split=split,
                                      fold_days=args.fold_days, min_train_days=args.min_train_days,
                                      fold_embargo_s=args.fold_embargo_s, max_folds=args.max_folds,
                                      shuffle_test=args.shuffle_test, max_memory_gb=args.max_memory_gb,
                                      num_threads=num_threads, params=args.params, risk_params=args.risk_params,
                                      early_stopping=args.early_stopping, inner_valid_days=args.inner_valid_days,
                                      early_stopping_rounds=args.early_stopping_rounds,
                                      exclude_groups=args.exclude_group, exclude_features=args.exclude_features)
                reports.append(report)
                if report['status'] == 'skipped':
                    print(f"Skipped {target}/{horizon}: {report['reason']}", flush=True)
                else:
                    print(json.dumps({k: report[k] for k in ('target', 'horizon_s', 'status', 'matrix_rows', 'labeled_rows',
                                                            'label_coverage_pct', 'metrics')}, allow_nan=False), flush=True)
                    if args.upload:
                        upload_model(report['model_dir'], output)
        trained = sum(report['status'] == 'trained' for report in reports)
        result = {'input_files': [str(p) for p in paths], 'input_rows': rows.height, 'results': reports,
                  'trained': trained, 'skipped': len(reports) - trained}
        output.mkdir(parents=True, exist_ok=True)
        report_path = output / f'research_report_{args.version}.json'
        with report_path.open('x') as f:
            json.dump(result, f, indent=2, allow_nan=False)
            f.write('\n')
        html_path = generate_report(output, args.version)
        print(f'Report: {report_path}; HTML: {html_path}', flush=True)
        print(f'Trained {trained} target/horizon combinations; skipped {len(reports) - trained}.', flush=True)
        if not trained:
            print('No targets could train: see skipped reasons above. No models written (exit 0).', flush=True)
        else:
            print(next(report['warning'] for report in reports if report['status'] == 'trained'), flush=True)
        return result
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        write_experiment(output, args, invocation_argv, targets, horizons, reports, split, rows, error, num_threads)


if __name__ == '__main__':
    main()
