"""Paired B-minus-A OOF comparisons using the evaluation UTC-day sampler."""
import argparse
import json
from pathlib import Path
import re

import numpy as np
import polars as pl

from research.evaluation import DAY_NS, moving_day_blocks, probability_losses
from research.risk import pinball_losses


def _load_run(path):
    path = Path(path)
    oof = path / 'oof.parquet' if path.is_dir() else path
    metadata_path = oof.with_name('metadata.json')
    metadata = json.loads(metadata_path.read_text()) if metadata_path.is_file() else None
    return oof, pl.read_parquet(oof), metadata


def _require_equal(a, b, columns):
    for column in columns:
        if column not in a.columns or column not in b.columns:
            raise ValueError(f'Both OOF files require {column}')
        if not a[column].equals(b[column]):
            raise ValueError(f'OOF {column} must be identical in identical row order; no intersection is taken')


def _row_scores(frame, stops, shrunk):
    required = ['return_pct', 'direction', 'regressor', 'ridge', 'p_down', 'p_flat', 'p_up']
    required += [f'adverse_{side}_bps' for _, side, _ in stops]
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f'OOF missing required columns: {", ".join(missing)}')
    y = frame['return_pct'].to_numpy()
    probs = frame.select('p_down', 'p_flat', 'p_up').to_numpy()
    # forecast_metrics uses sign(return_pct), argmax in [-1, 0, 1] order,
    # and the finite classifier expected-return mask when that column is saved.
    valid = np.isfinite(y) & (y != 0)
    valid &= (np.isfinite(frame['classifier_expected'].to_numpy()) if 'classifier_expected' in frame.columns
              else np.isfinite(probs).all(axis=1))
    accuracy = np.where(valid, (np.argmax(probs, axis=1) - 1 == np.sign(y)).astype(float), np.nan)
    log_loss, brier = probability_losses(np.sign(y), probs)
    scores = {'classifier_directional_accuracy': accuracy,
              'classifier_log_loss': log_loss, 'classifier_brier_score': brier}
    for name in ['regressor', 'ridge'] + (['regressor_shrunk'] if shrunk else []):
        predicted = frame[name].to_numpy()
        scores[name + '_mse'] = np.where(np.isfinite(y) & np.isfinite(predicted), (predicted - y) ** 2, np.nan)
    for name, side, q in stops:
        actual, predicted = frame[f'adverse_{side}_bps'].to_numpy(), frame[name].to_numpy()
        scores[name + '_pinball_loss'] = np.where(np.isfinite(actual) & np.isfinite(predicted),
                                                 pinball_losses(actual, predicted, q), np.nan)
    return scores


def compare_runs(a, b, block_lengths=(1, 3, 7), draws=1000, seed=17):
    """Compare identical OOF rows; recompute each run's ratios in every draw.

    Finite masks follow the per-run reports (stops may lack labels/predictions).
    Both runs always use the same sampled days, including days without scores.
    """
    block_lengths = tuple(block_lengths)
    if not block_lengths or min(block_lengths) < 1 or draws < 1:
        raise ValueError('Need positive block lengths and draws')
    if seed < 0:
        raise ValueError('seed must be non-negative')
    path_a, frame_a, metadata_a = _load_run(a)
    path_b, frame_b, metadata_b = _load_run(b)
    _require_equal(frame_a, frame_b, ['tick_ns', 'label_start_ns', 'fold', 'entry_delay_s'])
    for column in ('tick_ns', 'label_start_ns', 'fold', 'entry_delay_s'):
        if frame_a[column].null_count():
            raise ValueError(f'OOF {column} cannot contain nulls')
    if not frame_a.height:
        raise ValueError('Need nonempty OOF rows')
    if metadata_a is not None and metadata_b is not None:
        for key in ('target', 'horizon_s', 'label_spec_version', 'date_split'):
            if key not in metadata_a or key not in metadata_b or metadata_a[key] != metadata_b[key]:
                raise ValueError(f'metadata {key} must be present and equal in both runs')
    # Equal keys must also describe the same outcomes; never compare changed labels.
    labels = ['label_end_ns', 'return_pct', 'direction']
    common = set(frame_a.columns) & set(frame_b.columns)
    stops = []
    for name in sorted(common):
        match = re.fullmatch(r'stop_(long|short)_q(\d+)', name)
        if match:
            side, quantile = match.groups()
            q = int(quantile) / 100
            if not 0 < q < 1:
                raise ValueError(f'Invalid stop quantile: {name}')
            stops.append((name, side, q))
            labels.append(f'adverse_{side}_bps')
    _require_equal(frame_a, frame_b, labels)
    scores_a = _row_scores(frame_a, stops, 'regressor_shrunk' in common)
    scores_b = _row_scores(frame_b, stops, 'regressor_shrunk' in common)
    days, inverse = np.unique(frame_a['tick_ns'].to_numpy() // DAY_NS, return_inverse=True)
    daily, metrics = {}, {}
    for name, values_a in scores_a.items():
        daily[name] = []
        metric = {'lower_is_better': name != 'classifier_directional_accuracy'}
        masks = []
        for run, values in (('a', values_a), ('b', scores_b[name])):
            valid = np.isfinite(values)
            sums = np.bincount(inverse, weights=np.where(valid, values, 0), minlength=len(days))
            counts = np.bincount(inverse, weights=valid, minlength=len(days))
            daily[name].append((sums, counts))
            metric[run] = float(values[valid].mean()) if valid.any() else None
            metric['rows_' + run], metric['days_' + run] = int(valid.sum()), int((counts > 0).sum())
            masks.append(valid)
        paired = masks[0] & masks[1]
        metric['paired_rows'] = int(paired.sum())
        metric['paired_days'] = int(len(np.unique(inverse[paired])))
        metric['mean_difference'] = metric['b'] - metric['a'] if metric['a'] is not None and metric['b'] is not None else None
        metrics[name] = metric
    blocks = {}
    for length in block_lengths:
        # Reuse evaluation's moving whole-day blocks, including trimming and seed.
        sampled = moving_day_blocks(len(days), length, draws, seed)
        block = {}
        for name, runs in daily.items():
            means = []
            for sums, counts in runs:
                sampled_counts = counts[sampled].sum(axis=1)
                means.append(np.divide(sums[sampled].sum(axis=1), sampled_counts,
                                       out=np.full(draws, np.nan), where=sampled_counts > 0))
            differences = means[1] - means[0]
            valid = np.isfinite(differences)
            quantiles = np.quantile(differences[valid], [.025, .5, .975]).tolist() if valid.any() else [None] * 3
            block[name] = {'mean_difference': metrics[name]['mean_difference'],
                           'p2_5': quantiles[0], 'p50': quantiles[1], 'p97_5': quantiles[2],
                           'valid_draws': int(valid.sum())}
        blocks[str(length)] = block
    return {'run_a': str(path_a), 'run_b': str(path_b), 'comparison': 'B - A',
            'n_rows': frame_a.height, 'test_days': len(days), 'draws': draws, 'seed': seed,
            'metrics': metrics, 'blocks': blocks}


def _print_table(result):
    def number(value):
        return 'n/a' if value is None else f'{value:.6g}'

    print(f'B - A | {result["n_rows"]:,} rows | {result["test_days"]} UTC days | '
          f'{result["draws"]} draws | seed {result["seed"]}')
    print(f'{"metric (better)":48} {"A":>11} {"B":>11} {"B - A":>11} '
          f'{"block":>5} {"2.5%":>11} {"50%":>11} {"97.5%":>11} {"rows A/B":>13} {"days A/B":>9}')
    for name, metric in result['metrics'].items():
        label = name + (' (lower)' if metric['lower_is_better'] else ' (higher)')
        for length, scores in result['blocks'].items():
            score = scores[name]
            print(f'{label:48} {number(metric["a"]):>11} {number(metric["b"]):>11} '
                  f'{number(score["mean_difference"]):>11} {length:>5} '
                  f'{number(score["p2_5"]):>11} {number(score["p50"]):>11} {number(score["p97_5"]):>11} '
                  f'{str(metric["rows_a"]) + "/" + str(metric["rows_b"]):>13} '
                  f'{str(metric["days_a"]) + "/" + str(metric["days_b"]):>9}')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('a', help='Run A model directory or oof.parquet')
    parser.add_argument('b', help='Run B model directory or oof.parquet')
    parser.add_argument('--blocks', nargs='+', type=int, default=[1, 3, 7])
    parser.add_argument('--draws', type=int, default=1000)
    parser.add_argument('--seed', type=int, default=17)
    parser.add_argument('--json-out', type=Path)
    args = parser.parse_args(argv)
    try:
        result = compare_runs(args.a, args.b, args.blocks, args.draws, args.seed)
        if args.json_out:
            args.json_out.write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
        parser.error(str(exc))
    _print_table(result)


if __name__ == '__main__':
    main()
