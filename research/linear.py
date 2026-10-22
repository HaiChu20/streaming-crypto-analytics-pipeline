"""Train-only ridge control; column statistics and chunked Float64 normal equations."""

import numpy as np

CHUNK_ROWS = 2048
ALPHA_FACTORS = (1., 10., 100., 1000.)
FIXED_FACTOR = 100.


def ridge_transform(model, x, rows):
    """Only a row chunk is expanded; no full imputed/indicator matrix is kept."""
    p = x.shape[1]
    result = np.empty((len(rows), len(model['active'])), dtype=np.float64)
    for j, column in enumerate(model['active']):
        if column < p:
            result[:, j] = x[rows, column]
            missing = ~np.isfinite(result[:, j])
            result[missing, j] = model['medians'][column]
        else:
            result[:, j] = ~np.isfinite(x[rows, model['missing_columns'][column - p]])
        result[:, j] -= model['mean'][column]
        result[:, j] /= model['scale'][column]
    return result


def ridge_system(x, y, rows):
    medians, mean, scale, missing_columns, indicator_mean, indicator_scale = [], [], [], [], [], []
    for column in range(x.shape[1]):
        values = x[rows, column]
        missing = ~np.isfinite(values)
        median = float(np.median(values[~missing])) if (~missing).any() else 0.
        values[missing] = median
        medians.append(median)
        mean.append(values.mean())
        scale.append(values.std())
        if missing.any():
            missing_columns.append(column)
            fraction = missing.mean()
            indicator_mean.append(fraction)
            indicator_scale.append(np.sqrt(fraction * (1 - fraction)))
    scale = np.array(scale + indicator_scale, dtype=np.float64)
    model = {'medians': np.array(medians), 'missing_columns': np.array(missing_columns, dtype=np.int64),
             'mean': np.array(mean + indicator_mean), 'scale': scale,
             'active': np.flatnonzero(scale > 0), 'intercept': float(np.mean(y[rows]))}
    gram = np.zeros((len(model['active']), len(model['active'])), dtype=np.float64)
    rhs = np.zeros(len(model['active']), dtype=np.float64)
    for start in range(0, len(rows), CHUNK_ROWS):
        indices = rows[start:start + CHUNK_ROWS]
        z = ridge_transform(model, x, indices)
        gram += z.T @ z
        rhs += z.T @ (y[indices] - model['intercept'])
    return model, gram, rhs


def ridge_solve(gram, rhs, alpha):
    system = gram.copy()
    system.flat[::len(system) + 1] += alpha
    return np.linalg.solve(system, rhs) if len(rhs) else np.array([], dtype=np.float64)


def ridge_predict(model, x, rows=None):
    rows = np.arange(len(x)) if rows is None else rows
    predicted = np.empty(len(rows), dtype=np.float64)
    for start in range(0, len(rows), CHUNK_ROWS):
        indices = rows[start:start + CHUNK_ROWS]
        predicted[start:start + len(indices)] = ridge_transform(model, x, indices) @ model['coef'] + model['intercept']
    return predicted


def fit_ridge(x, y, rows, inner=None):
    """alpha = factor * n_train/1000; fixed factor 100, or inner MSE over 1/10/100/1000."""
    factor, losses = FIXED_FACTOR, None
    if inner is not None:
        train, valid = inner
        inner_model, gram, rhs = ridge_system(x, y, train)
        coefficients = np.column_stack([ridge_solve(gram, rhs, f * len(train) / 1000) for f in ALPHA_FACTORS])
        errors = np.zeros(len(ALPHA_FACTORS), dtype=np.float64)
        for start in range(0, len(valid), CHUNK_ROWS):
            indices = valid[start:start + CHUNK_ROWS]
            residual = ridge_transform(inner_model, x, indices) @ coefficients + inner_model['intercept'] - y[indices, None]
            errors += np.sum(residual ** 2, axis=0)
        losses = (errors / len(valid)).tolist()
        factor = ALPHA_FACTORS[int(np.argmin(errors))]
        del inner_model, gram, rhs, coefficients
    model, gram, rhs = ridge_system(x, y, rows)
    alpha = factor * len(rows) / 1000
    model['coef'] = ridge_solve(gram, rhs, alpha)
    info = {'alpha': alpha, 'factor': factor, 'alpha_scale': 'n_train/1000',
            'grid_factors': list(ALPHA_FACTORS), 'fixed_factor': FIXED_FACTOR,
            'status': 'selected' if inner is not None else 'fixed', 'inner_mse': losses,
            'active_columns': len(model['active']), 'missing_indicator_columns': len(model['missing_columns'])}
    return model, info
