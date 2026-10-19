"""Mid-price stop quantiles, shared guards, and training-only risk baselines."""

import lightgbm as lgb
import numpy as np

QUANTILES = (0.8, 0.9)
STOP_NAMES = tuple(f"stop_{side}_q{int(q * 100)}" for side in ("long", "short") for q in QUANTILES)
PARAMS = {"verbosity": -1, "num_threads": 2, "seed": 17, "deterministic": True,
          "force_col_wise": True, "num_leaves": 15, "learning_rate": 0.05,
          "min_data_in_leaf": 20, "feature_pre_filter": False}


def fit_stop_models(x, adverse, features, rounds):
    """Fit each side on its non-null path labels; missing volatility is allowed."""
    models = {}
    for side, actual in adverse.items():
        keep = np.isfinite(actual)
        if not keep.any():
            continue
        for q in QUANTILES:
            name = f"stop_{side}_q{int(q * 100)}"
            models[name] = lgb.train(
                dict(PARAMS, objective="quantile", alpha=q),
                lgb.Dataset(x[keep], label=actual[keep], feature_name=features),
                num_boost_round=rounds,
            )
    return models


def guard_stop_predictions(predictions):
    """Clip negatives and raise q90 to q80 for both evaluation and inference."""
    result = {name: np.maximum(np.asarray(values, dtype=float), 0)
              for name, values in predictions.items()}
    if any(not np.isfinite(values).all() for values in result.values()):
        raise ValueError("nonfinite stop prediction")
    for side in ("long", "short"):
        low, high = f"stop_{side}_q80", f"stop_{side}_q90"
        if low in result and high in result:
            result[high] = np.maximum(result[low], result[high])
    return result


def predict_stops(models, x, num_threads=2):
    return guard_stop_predictions({name: model.predict(x, num_threads=num_threads)
                                   for name, model in models.items()})


def stop_metrics(actual, predicted, q, terciles=None):
    actual, predicted = np.asarray(actual), np.asarray(predicted)
    keep = np.isfinite(actual) & np.isfinite(predicted)
    y, p = actual[keep], predicted[keep]
    error = y - p
    result = {"n": int(keep.sum()), "hit_rate": float(np.mean(y > p)) if len(y) else None,
              "pinball_loss": float(np.mean(np.maximum(q * error, (q - 1) * error))) if len(y) else None}
    if terciles is not None:
        groups = np.asarray(terciles)
        result["by_volatility_tercile"] = {
            name: stop_metrics(actual[groups == i], predicted[groups == i], q)
            for i, name in enumerate(("low", "medium", "high"))
        }
    return result


def evaluate_stops(x, adverse, rv, features, folds, rounds):
    """Out-of-fold risk metrics using the return models' exact purged splits.

    k is the training quantile of adverse/rv for positive finite reference rv.
    Zero rv predicts zero; missing/negative rv has no volatility baseline or
    regime assignment. Tercile edges use only this side's valid training labels.
    Overall regime metrics pool rows assigned using their own fold's edges.
    """
    rv = np.asarray(rv, dtype=float)
    oof = {name: [] for name in STOP_NAMES}
    reports = []
    for train, test in folds:
        models = fit_stop_models(x[train], {s: a[train] for s, a in adverse.items()}, features, rounds)
        predictions = predict_stops(models, x[test])
        metrics = {}
        for side, actual in adverse.items():
            valid_train = train[np.isfinite(actual[train])]
            valid_rv = valid_train[np.isfinite(rv[valid_train]) & (rv[valid_train] >= 0)]
            positive_rv = valid_rv[rv[valid_rv] > 0]
            edges = np.quantile(rv[valid_rv], [1 / 3, 2 / 3]) if len(valid_rv) else None
            groups = np.full(len(test), -1, dtype=int)
            known_rv = np.isfinite(rv[test]) & (rv[test] >= 0)
            if edges is not None:
                groups[known_rv] = np.searchsorted(edges, rv[test][known_rv], side="right")
            for q in QUANTILES:
                name = f"stop_{side}_q{int(q * 100)}"
                if name not in predictions:
                    metrics[name] = {"train_rows": 0, "status": "no_training_labels"}
                    continue
                constant = float(np.quantile(actual[valid_train], q))
                k = float(np.quantile(actual[positive_rv] / rv[positive_rv], q)) if len(positive_rv) else None
                scaled = np.full(len(test), np.nan)
                if k is not None:
                    scaled[known_rv] = k * rv[test][known_rv]
                values = (actual[test], predictions[name], np.full(len(test), constant), scaled, groups)
                oof[name].append(values)
                metrics[name] = {
                    "train_rows": len(valid_train), "quantile": q, "target_hit_rate": 1 - q,
                    "training_quantile_bps": constant, "volatility_k": k,
                    "volatility_tercile_edges": edges.tolist() if edges is not None else None,
                    **{key: stop_metrics(values[0], prediction, q, groups)
                       for key, prediction in zip(("model", "unconditional", "volatility_scaled"), values[1:4])},
                }
        reports.append(metrics)
    overall = {}
    for name, records in oof.items():
        q = int(name[-2:]) / 100
        actual, model, constant, scaled, groups = (
            [np.concatenate(parts) for parts in zip(*records)] if records else [np.array([])] * 5)
        overall[name] = {
            "quantile": q, "target_hit_rate": 1 - q,
            **{key: stop_metrics(actual, prediction, q, groups)
               for key, prediction in zip(("model", "unconditional", "volatility_scaled"), (model, constant, scaled))},
        }
    return reports, overall
