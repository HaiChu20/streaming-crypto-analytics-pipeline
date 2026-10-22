"""Small shared LightGBM fitting and chronological inner-validation helpers."""

import json
from pathlib import Path

import lightgbm as lgb
import numpy as np

from research.evaluation import DAY_NS
from research.labels import NS


def parse_params(value):
    """A JSON object, supplied inline or as a .json filename."""
    if value is None:
        return {}
    try:
        text = Path(value).read_text() if isinstance(value, (str, Path)) and str(value).endswith('.json') else value
        params = json.loads(text) if isinstance(text, str) else text
    except (OSError, ValueError) as exc:
        raise ValueError(f'Parameters must be a JSON object or readable .json file: {exc}') from exc
    if not isinstance(params, dict) or any(not isinstance(k, str) for k in params):
        raise ValueError('Parameters must be a JSON object with string keys')
    for key, val in params.items():
        values = val if isinstance(val, list) else [val]
        if not values or any(type(v) not in (str, bool, int, float) or
                             (isinstance(v, float) and not np.isfinite(v)) for v in values):
            raise ValueError(f'Unsupported parameter value for {key}: use finite scalars or lists of scalars')
    # These would silently override the selected iteration count or task shape.
    reserved = {'objective', 'application', 'app', 'loss', 'num_class', 'num_classes', 'alpha',
                'num_iterations', 'num_iteration', 'n_iter', 'num_tree', 'num_trees', 'num_round',
                'num_rounds', 'nrounds', 'num_boost_round', 'n_estimators', 'max_iter',
                'early_stopping_round', 'early_stopping_rounds', 'early_stopping',
                'num_threads', 'num_thread', 'nthread', 'nthreads', 'n_jobs'}
    if reserved & params.keys():
        raise ValueError(f'Parameters controlled by model tasks/CLI flags: {sorted(reserved & params.keys())}')
    return dict(params)


def inner_split(ticks, label_end_ns, label_available_ns, horizon_s, valid_days=7, *,
                purge_s=None, embargo_s=None, feature_available_ns=None):
    """Last observed UTC days, with the outer fold's strict purge/availability rule."""
    if valid_days < 1:
        raise ValueError('inner_valid_days must be positive')
    ticks = np.asarray(ticks, dtype=np.int64)
    days = np.unique(ticks // DAY_NS)
    if len(days) <= valid_days:
        return None
    purge_s = horizon_s if purge_s is None else purge_s
    embargo_s = horizon_s if embargo_s is None else embargo_s
    start = days[-valid_days] * DAY_NS
    known = np.maximum(np.asarray(label_end_ns), np.asarray(label_available_ns))
    if feature_available_ns is not None:
        known = np.maximum(known, feature_available_ns)
    train = np.flatnonzero((ticks + (purge_s + embargo_s) * NS < start)
                           & (known + embargo_s * NS < start))
    valid = np.flatnonzero(ticks >= start)
    return (train, valid) if len(train) and len(valid) else None


def shrinkage_coefficient(actual, predicted):
    actual, predicted = np.asarray(actual, dtype=np.float64), np.asarray(predicted, dtype=np.float64)
    keep = np.isfinite(actual) & np.isfinite(predicted)
    denom = np.dot(predicted[keep], predicted[keep])
    return float(np.clip(np.dot(actual[keep], predicted[keep]) / denom, 0, 1)) if denom > 0 else 0.


def fit_booster(x, y, features, params, rounds, *, inner=None, early_stopping=False,
                patience=50, metric=None):
    """Choose rounds on inner labels only, then refit on every eligible input row."""
    y = np.asarray(y)
    finite = np.isfinite(y)
    selection = {'rounds': rounds, 'status': 'fixed'}
    validation = None
    if early_stopping:
        selection['status'] = 'fallback'
        selection['reason'] = 'insufficient inner training/validation days or labels'
        if inner is not None:
            train, valid = (indices[finite[indices]] for indices in inner)
            if len(train) and len(valid):
                model = lgb.train(dict(params, metric=metric),
                    lgb.Dataset(x[train], label=y[train], feature_name=features), num_boost_round=rounds,
                    valid_sets=[lgb.Dataset(x[valid], label=y[valid], feature_name=features)],
                    callbacks=[lgb.early_stopping(patience, first_metric_only=True, verbose=False)])
                chosen = int(model.best_iteration or rounds)
                validation = (valid, model.predict(x[valid], num_iteration=chosen,
                                                   num_threads=params['num_threads']))
                selection = {'rounds': chosen, 'status': 'selected',
                             'inner_train_rows': len(train), 'inner_valid_rows': len(valid)}
                del model
    model = lgb.train(params, lgb.Dataset(x if finite.all() else x[finite], label=y if finite.all() else y[finite],
                                         feature_name=features), num_boost_round=selection['rounds'])
    return model, selection, validation
