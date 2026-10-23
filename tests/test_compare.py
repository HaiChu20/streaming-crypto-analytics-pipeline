"""Run comparisons use tiny synthetic OOF files only."""
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import polars as pl
import pytest

from research.compare import compare_runs, main
from research.evaluation import DAY_NS, moving_day_blocks, paired_day_block_bootstrap, probability_scores
from research.risk import STOP_NAMES, pinball_losses, stop_metrics
from research.train import forecast_metrics, metrics


@pytest.fixture
def oof():
    # Unequal rows per UTC day distinguish pooled ratios from daily-ratio means.
    ticks = np.concatenate([day * DAY_NS + np.arange(day + 2) for day in range(10)])
    n = len(ticks)
    y = np.resize([-.2, 0., .3, -.1, .4], n)
    probs = np.tile([.25, .5, .25], (n, 1))
    adverse = np.resize([1., 5., 9., np.nan], n)
    return pl.DataFrame({
        'tick_ns': ticks, 'label_start_ns': ticks, 'label_end_ns': ticks + 900_000_000_000,
        'fold': np.repeat(1, n), 'entry_delay_s': np.repeat(0, n),
        'return_pct': y, 'direction': np.sign(y).astype(int),
        'regressor': y + np.linspace(.1, .8, n), 'ridge': y + .5,
        'regressor_shrunk': y + .3, 'classifier_expected': probs @ [-.2, 0., .3],
        **{f'p_{c}': probs[:, i] for i, c in enumerate(('down', 'flat', 'up'))},
        'adverse_long_bps': adverse, 'adverse_short_bps': adverse * 2,
        **{name: np.full(n, 3.) for name in STOP_NAMES},
    }, nan_to_null=True)


def save_run(tmp_path, name, frame, metadata=True, **overrides):
    directory = tmp_path / name
    directory.mkdir(exist_ok=True)
    frame.write_parquet(directory / 'oof.parquet')
    if metadata:
        report = {'target': 'btc_spot', 'horizon_s': 900, 'label_spec_version': 1,
                  'date_split': {'lockbox_start_ns': 20 * DAY_NS}, **overrides}
        (directory / 'metadata.json').write_text(json.dumps(report))
    return directory


def test_identical_runs_have_exact_zero_differences(tmp_path, oof):
    run = save_run(tmp_path, 'a', oof)
    result = compare_runs(run, run / 'oof.parquet', draws=70)
    assert result['n_rows'] == oof.height and result['test_days'] == 10
    assert len(result['metrics']) == 10
    for block in result['blocks'].values():
        for name, score in block.items():
            assert [score[k] for k in ('mean_difference', 'p2_5', 'p50', 'p97_5')] == [0.] * 4
            assert result['metrics'][name]['a'] == result['metrics'][name]['b']
            assert score['valid_draws'] == 70


@pytest.mark.parametrize('column', ['tick_ns', 'label_start_ns', 'fold', 'entry_delay_s',
                                    'return_pct', 'direction', 'label_end_ns', 'adverse_long_bps'])
def test_mismatched_keys_delay_or_labels_fail(tmp_path, oof, column):
    a = save_run(tmp_path, 'a', oof)
    changed = oof.with_columns((pl.col(column) + 1).alias(column))
    b = save_run(tmp_path, 'b', changed)
    with pytest.raises(ValueError, match=column):
        compare_runs(a, b)


@pytest.mark.parametrize('changed', ['reordered', 'shorter', 'missing_key', 'null_key'])
def test_rows_must_match_without_intersection(tmp_path, oof, changed):
    a = save_run(tmp_path, 'a', oof)
    frames = {'reordered': oof.reverse(), 'shorter': oof.head(oof.height - 1),
              'missing_key': oof.drop('fold'), 'null_key': oof.with_columns(pl.lit(None).alias('fold'))}
    b = save_run(tmp_path, 'b', frames[changed])
    with pytest.raises(ValueError, match='tick_ns|fold'):
        compare_runs(a, b)


@pytest.mark.parametrize(('key', 'value'), [('target', 'eth_spot'), ('horizon_s', 1800),
                                          ('label_spec_version', 2), ('date_split', {})])
def test_metadata_must_match(tmp_path, oof, key, value):
    a = save_run(tmp_path, 'a', oof)
    b = save_run(tmp_path, 'b', oof, **{key: value})
    with pytest.raises(ValueError, match=key):
        compare_runs(a, b)


def test_missing_metadata_fields_are_not_assumed_equal(tmp_path, oof):
    a = save_run(tmp_path, 'a', oof)
    b = save_run(tmp_path, 'b', oof)
    for run in (a, b):
        (run / 'metadata.json').write_text('{}')
    with pytest.raises(ValueError, match='target'):
        compare_runs(a, b)


def test_strictly_better_run_intervals_have_correct_sign(tmp_path, oof):
    y = oof['return_pct'].to_numpy()
    probs = np.eye(3)[np.sign(y).astype(int) + 1]
    better = oof.with_columns(
        *[pl.Series(name, y) for name in ('regressor', 'ridge', 'regressor_shrunk')],
        pl.Series('classifier_expected', y),
        *[pl.Series(f'p_{c}', probs[:, i]) for i, c in enumerate(('down', 'flat', 'up'))],
        *[pl.col(f'adverse_{name.split("_")[1]}_bps').alias(name) for name in STOP_NAMES])
    a, b = save_run(tmp_path, 'a', oof), save_run(tmp_path, 'b', better)
    result = compare_runs(a, b, draws=100)
    for block in result['blocks'].values():
        for name, score in block.items():
            if result['metrics'][name]['lower_is_better']:
                assert score['mean_difference'] < 0 and score['p97_5'] < 0
            else:
                assert score['mean_difference'] > 0 and score['p2_5'] > 0


def test_scores_match_training_and_evaluation_definitions(tmp_path, oof):
    # Include an argmax tie, flat actuals, missing stop labels/predictions and
    # missing expected returns, which are excluded by train.metrics.
    n = oof.height
    expected = oof['classifier_expected'].to_numpy().copy()
    expected[2] = np.nan
    oof = oof.with_columns(pl.Series('classifier_expected', expected, nan_to_null=True),
                           pl.when(pl.int_range(pl.len()) == 0).then(.5)
                             .otherwise(pl.col('p_down')).alias('p_down'),
                           pl.when(pl.int_range(pl.len()) == 0).then(0.)
                             .otherwise(pl.col('p_up')).alias('p_up'),
                           pl.when(pl.int_range(pl.len()) == 2).then(None)
                             .otherwise(pl.col('stop_long_q80')).alias('stop_long_q80'))
    a = save_run(tmp_path, 'a', oof)
    b_frame = oof.with_columns(pl.col('regressor') * .5, pl.col('p_flat') - .1, pl.col('p_up') + .1)
    b = save_run(tmp_path, 'b', b_frame)
    result = compare_runs(a, b, draws=30)
    for run, frame in (('a', oof), ('b', b_frame)):
        y = frame['return_pct'].to_numpy()
        probs = frame.select('p_down', 'p_flat', 'p_up').to_numpy()
        clf = forecast_metrics(y, {'classifier': frame['classifier_expected'].to_numpy()},
                               {'classifier': probs})['classifier']
        accuracy = result['metrics']['classifier_directional_accuracy']
        assert accuracy[run] == clf['directional_accuracy']
        assert accuracy['rows_' + run] == clf['directional_n']
        probabilities = probability_scores(np.sign(y), probs)
        assert result['metrics']['classifier_log_loss'][run] == probabilities['log_loss']
        assert result['metrics']['classifier_brier_score'][run] == probabilities['brier_score']
        for name in ('regressor', 'regressor_shrunk', 'ridge'):
            measured = metrics(y, frame[name].to_numpy())
            assert result['metrics'][name + '_mse'][run] == pytest.approx(measured['rmse'] ** 2)
        for name in STOP_NAMES:
            actual = frame[f'adverse_{name.split("_")[1]}_bps'].to_numpy()
            score = stop_metrics(actual, frame[name].to_numpy(), int(name[-2:]) / 100)
            measured = result['metrics'][name + '_pinball_loss']
            assert measured[run] == score['pinball_loss']
            assert measured['rows_' + run] == score['n'] < n
    for score in result['metrics'].values():
        assert score['mean_difference'] == score['b'] - score['a']


def test_paired_bootstrap_pools_row_counts_and_reuses_day_sampler(tmp_path, oof):
    # Different finite masks require a separate denominator for each run, using
    # the same sampled days. A mean of daily ratios gives the wrong intervals.
    a = save_run(tmp_path, 'a', oof)
    b_frame = oof.with_columns(
        pl.when(pl.col('tick_ns') // DAY_NS < 3).then(None)
          .otherwise(pl.col('regressor') - .2).alias('regressor'),
        (pl.col('stop_long_q80') + (pl.col('tick_ns') // DAY_NS) / 10).alias('stop_long_q80'))
    b = save_run(tmp_path, 'b', b_frame)
    result = compare_runs(a, b, block_lengths=[1, 3], draws=60, seed=31)
    y, ticks = oof['return_pct'].to_numpy(), oof['tick_ns'].to_numpy()
    a_loss = (y - oof['regressor'].to_numpy()) ** 2
    b_loss = (y - b_frame['regressor'].to_numpy()) ** 2
    for length in (1, 3):
        expected = []
        for sample in moving_day_blocks(10, length, draws=60, seed=31):
            indices = np.concatenate([np.flatnonzero(ticks // DAY_NS == day) for day in sample])
            if np.isfinite(b_loss[indices]).any():
                expected.append(np.nanmean(b_loss[indices]) - np.mean(a_loss[indices]))
        score = result['blocks'][str(length)]['regressor_mse']
        np.testing.assert_allclose([score['p2_5'], score['p50'], score['p97_5']],
                                   np.quantile(expected, [.025, .5, .975]))
        assert score['valid_draws'] == len(expected)
        # Same-mask metrics reproduce the existing paired difference bootstrap.
        stop = 'stop_long_q80'
        actual = oof['adverse_long_bps'].to_numpy()
        delta = pinball_losses(actual, b_frame[stop].to_numpy(), .8) - pinball_losses(actual, oof[stop].to_numpy(), .8)
        reference = paired_day_block_bootstrap(ticks, {'stop': delta}, [length], draws=60, seed=31)
        for key in ('mean_difference', 'p2_5', 'p50', 'p97_5', 'valid_draws'):
            assert result['blocks'][str(length)][stop + '_pinball_loss'][key] == pytest.approx(reference['blocks'][str(length)]['stop'][key])
    assert result['metrics']['regressor_mse']['b'] == pytest.approx(np.nanmean(b_loss))
    assert result['metrics']['regressor_mse']['days_b'] == 7


def test_seed_determinism(tmp_path, oof):
    a = save_run(tmp_path, 'a', oof)
    b = save_run(tmp_path, 'b', oof.with_columns(pl.col('regressor') - .2))
    first = compare_runs(a, b, draws=50, seed=41)
    assert first == compare_runs(a, b, draws=50, seed=41)
    assert first['blocks'] != compare_runs(a, b, draws=50, seed=42)['blocks']


def test_optional_columns_and_metadata(tmp_path, oof):
    a = save_run(tmp_path, 'a', oof)
    b = save_run(tmp_path, 'b', oof.drop('regressor_shrunk', 'stop_short_q90', 'classifier_expected'), metadata=False)
    result = compare_runs(a, b, draws=10)
    assert 'regressor_shrunk_mse' not in result['metrics']
    assert 'stop_short_q90_pinball_loss' not in result['metrics']
    assert result['metrics']['classifier_directional_accuracy']['mean_difference'] == 0
    # Baseline and regime columns are not stop model predictions.
    b_frame = oof.with_columns(pl.lit(1.).alias('stop_long_q80_unconditional'),
                               pl.lit(0).alias('stop_long_q80_volatility_tercile'))
    b = save_run(tmp_path, 'b', b_frame, metadata=False)
    assert len(compare_runs(b, b, draws=10)['metrics']) == 10


def test_all_null_stop_labels_and_no_directional_rows(tmp_path, oof):
    frame = oof.with_columns(pl.lit(None, dtype=pl.Float64).alias('adverse_long_bps'),
                             pl.lit(0.).alias('return_pct'), pl.lit(0).alias('direction'))
    run = save_run(tmp_path, 'a', frame)
    result = compare_runs(run, run, draws=10)
    for name in ('stop_long_q80_pinball_loss', 'classifier_directional_accuracy'):
        assert result['metrics'][name]['mean_difference'] is None
        for block in result['blocks'].values():
            assert block[name]['p50'] is None and block[name]['valid_draws'] == 0
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize('kwargs', [{'block_lengths': []}, {'block_lengths': [0]}, {'draws': 0}, {'seed': -1}])
def test_invalid_bootstrap_parameters(tmp_path, oof, kwargs):
    run = save_run(tmp_path, 'a', oof)
    with pytest.raises(ValueError, match='positive|seed'):
        compare_runs(run, run, **kwargs)


def test_cli_module_table_and_json(tmp_path, oof):
    run = save_run(tmp_path, 'a', oof)
    output = tmp_path / 'comparison.json'
    completed = subprocess.run([sys.executable, '-m', 'research.compare', str(run), str(run / 'oof.parquet'),
                                '--blocks', '1', '3', '7', '--draws', '20', '--seed', '7', '--json-out', str(output)],
                               cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    assert 'B - A' in completed.stdout and '2.5%' in completed.stdout and '(lower)' in completed.stdout
    assert 'rows A/B' in completed.stdout and 'days A/B' in completed.stdout
    saved = json.loads(output.read_text())
    assert list(saved['blocks']) == ['1', '3', '7'] and saved['seed'] == 7 and saved['draws'] == 20
    assert saved == compare_runs(run, run / 'oof.parquet', draws=20, seed=7)


def test_cli_errors_are_clear(tmp_path, oof, capsys):
    a = save_run(tmp_path, 'a', oof)
    b = save_run(tmp_path, 'b', oof, target='eth_spot')
    with pytest.raises(SystemExit) as exc:
        main([str(a), str(b)])
    assert exc.value.code == 2
    assert 'metadata target' in capsys.readouterr().err
