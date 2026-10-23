"""Self-contained evaluation pages: python -m research.report OUTPUT --version V."""
import argparse
from datetime import datetime, timezone
from html import escape
import json
from pathlib import Path

COLORS = ('#315ba8', '#cc661d', '#607d68', '#9a5489')


def fmt(value):
    if value is None:
        return '—'
    return f'{value:.5g}' if isinstance(value, float) else str(value)


def table(headers, rows):
    return '<table><thead><tr>' + ''.join(f'<th>{escape(str(h))}</th>' for h in headers) + '</tr></thead><tbody>' + ''.join(
        '<tr>' + ''.join(f'<td>{escape(fmt(c))}</td>' for c in row) + '</tr>' for row in rows) + '</tbody></table>'


def chart(title, series, explanation):
    """Inline SVG bar chart with labels/values; signed axis and no external assets."""
    series = [(str(label), value, color) for label, value, color in series if value is not None]
    result = f'<h4>{escape(title)}</h4>'
    if not series:
        return result + '<p>No usable observations.</p><p class="caption">' + escape(explanation) + '</p>'
    lower = min(0, min(v for _, v, _ in series))
    upper = max(0, max(v for _, v, _ in series))
    width = max(upper - lower, 1e-12)
    x = lambda value: 360 + 430 * (value - lower) / width
    height = 32 * len(series) + 35
    result += f'<svg viewBox="0 0 910 {height}" role="img" aria-label="{escape(title)}">'
    result += f'<line x1="{x(0)}" y1="4" x2="{x(0)}" y2="{height-20}" stroke="#889"/>'
    for i, (label, value, color) in enumerate(series):
        y = 12 + i * 32
        result += (f'<text x="4" y="{y+16}" font-size="13"><title>{escape(label)}</title>{escape(label if len(label) <= 46 else label[:45] + chr(8230))}</text>'
                   f'<rect x="{min(x(0), x(value))}" y="{y}" width="{max(abs(x(value)-x(0)), .5)}" height="21" fill="{color}"/>'
                   f'<text x="805" y="{y+16}" font-size="13">{fmt(value)}</text>')
    return result + '</svg><p class="caption">' + escape(explanation) + '</p>'


def direction_charts(values, title='Direction'):
    result = f'<h3>{escape(title)}</h3>'
    for key, label, caption in [('directional_accuracy', 'Direction hit rate',
        'Higher means more correct up/down calls; 0.50 is a coin flip, while the zero baseline always calls flat.'),
        ('spearman_ic', 'Rank correlation (IC)',
         'IC measures whether larger forecasts tend to precede larger returns; zero means no ranking skill.')]:
        result += chart(label, [(name, m.get(key), COLORS[i % 4]) for i, (name, m) in enumerate(values.items())], caption)
    result += table(['Model', 'Rows', 'Hit rate', 'IC', 'MAE', 'RMSE', 'Log loss', 'Brier'], [
        [name, m.get('n'), m.get('directional_accuracy'), m.get('spearman_ic'), m.get('mae'), m.get('rmse'),
         m.get('log_loss'), m.get('brier_score')]
        for name, m in values.items()])
    return result


def stop_section(stops):
    result = '<h3>Stops: calibration and pinball loss</h3>'
    hit, losses, regimes = [], [], []
    for name, values in stops.items():
        hit.append((name + ' target', values.get('target_hit_rate'), '#777'))
        for i, baseline in enumerate(('model', 'unconditional', 'volatility_scaled')):
            m = values.get(baseline, {})
            hit.append((name + ' ' + baseline, m.get('hit_rate'), COLORS[i]))
            losses.append((name + ' ' + baseline, m.get('pinball_loss'), COLORS[i]))
            for group, item in m.get('by_volatility_tercile', {}).items():
                regimes.append((name + ' ' + baseline + ' ' + group, item.get('hit_rate'), COLORS[i]))
    result += chart('Stop hit rate vs target by side / quantile', hit,
                    'A hit means price moved beyond the stop; q90 aims for 10% hits and q80 for 20%, before trading costs.')
    result += chart('Pinball loss vs baselines', losses,
                    'Lower is better: this score penalizes stops that are too close or too far for their chosen probability.')
    result += chart('Stop hit rate by volatility tercile', regimes,
                    'Low, medium and high volatility thresholds come only from the corresponding training fold.')
    rows = []
    for name, values in stops.items():
        for baseline in ('model', 'unconditional', 'volatility_scaled'):
            m = values.get(baseline, {})
            rows.append([name, baseline, m.get('n'), values.get('target_hit_rate'), m.get('hit_rate'), m.get('pinball_loss')])
    return result + table(['Stop', 'Method', 'Rows', 'Target hit', 'Actual hit', 'Pinball'], rows)


def timestamp(value):
    return datetime.fromtimestamp(value / 1e9, timezone.utc).isoformat() if value is not None else '—'


def target_section(r):
    result = f"<article><h2>{escape(r['target'])} · {r['horizon_s']} seconds · entry delay {r.get('entry_delay_s', 0)} s</h2>"
    result += '<h3>Data window, lockbox and embargo</h3>'
    window = r.get('data_window', {})
    result += table(['Start UTC', 'End UTC', 'Last label endpoint UTC'],
                    [[timestamp(window.get('start_ns')), timestamp(window.get('end_ns')),
                      timestamp(window.get('last_label_end_ns'))]])
    result += table(['Split', 'First UTC date', 'Last UTC date', 'Days'],
        [[name, (span or {}).get('first_day'), (span or {}).get('last_day'), (span or {}).get('days', 0)]
         for name, span in r.get('date_split', {}).items() if name in ('development', 'embargo', 'lockbox')])
    if r.get('memory'):
        memory = r['memory']
        result += f"<p>X: {memory['rows']:,} rows × {memory['features']:,} features × 8 bytes = {memory['bytes']/1024**3:.3f} GiB; float64.</p>"
    if 'exclusions' in r:
        excluded = r['exclusions']['excluded']
        result += f"<p>Feature exclusions: {len(excluded)} removed; {r.get('feature_count', 0)} retained.</p>"
        if excluded:
            result += '<details><summary>Excluded names</summary><pre>' + escape('\n'.join(excluded)) + '</pre></details>'
    if r.get('status') != 'trained':
        return result + '<p>Skipped: ' + escape(r.get('reason', '')) + '</p></article>'
    result += '<p>' + escape(r.get('feature_dtype', '')) + '</p><p>' + escape(r.get('warning', '')) + '</p>'
    folds = r.get('folds', [])
    result += '<h3>Walk-forward folds</h3>' + table(
        ['Fold', 'Train start UTC', 'Train end UTC', 'Test start UTC', 'Test end UTC', 'Train rows', 'Test rows', 'Test flats', 'Gap seconds'],
        [[f['fold'], timestamp(f['train_start_ns']), timestamp(f['train_end_ns']), timestamp(f['test_start_ns']),
          timestamp(f['test_end_ns']), f['train_rows'], f['test_rows'], f.get('test_flat_labels'), f['gap_s']] for f in folds])
    if 'flat_labels' in r:
        result += f"<p>Exact-flat labels: {r['flat_labels']} total; {r.get('oof_flat_labels', 0)} held out.</p>"
    if r.get('chosen_rounds'):
        selections = [(str(f['fold']), name, chosen) for f in folds for name, chosen in f.get('chosen_rounds', {}).items()]
        selections += [('Final', name, chosen) for name, chosen in r['chosen_rounds'].items()]
        result += '<details><summary>Chosen rounds per model</summary>' + table(
            ['Fold', 'Model', 'Rounds', 'Selection'],
            [[fold, name, chosen.get('rounds'), chosen['status']] for fold, name, chosen in selections]) + '</details>'
    if r.get('ridge'):
        result += '<p>Ridge uses training-only median imputation, missing indicators and standardisation. '
        result += 'Alpha = factor × training rows / 1000; factors 1, 10, 100, 1000 use inner validation when available, otherwise factor 100.</p>'
    result += '<p>Each model learns from earlier rows only; labels and feature emissions must be available before the test block.</p>'
    for metric, title in [('directional_accuracy', 'Direction hit rate per fold'), ('spearman_ic', 'IC per fold')]:
        result += chart(title, [(f"Fold {f['fold']} {name}", m.get(metric), COLORS[i % 4])
                       for f in folds for i, (name, m) in enumerate(f['metrics'].items())],
                       'Compare the model with zero, momentum, reversal and the training prior in every held-out period.')
    result += direction_charts(r.get('metrics', {}), 'Pooled out-of-fold direction')
    if r.get('bootstrap'):
        bootstrap = r['bootstrap']
        result += '<h3>Paired day-block bootstrap</h3><p>' + escape(
            f"{bootstrap['test_days']} UTC test days; {bootstrap['draws']} draws. "
            'Whole ordered days are resampled in moving blocks. Positive accuracy differences and negative loss '
            'differences favor the model; paired rows are identical within each comparison.') + '</p>'
        result += table(['Comparison', 'Block days', 'Paired rows', 'Mean difference', '2.5%', '50%', '97.5%'], [
            [name, length, values['paired_rows'], values['mean_difference'], values['p2_5'], values['p50'], values['p97_5']]
            for length, scores in bootstrap['blocks'].items() for name, values in scores.items()])
    result += '<h3>Regime breakdown</h3><p>Volatility groups use training-fold terciles; clock buckets are UTC; weekends are Saturday and Sunday. Day phase separates 00:00–01:00 UTC from the rest of each day, using the feature tick.</p>'
    for axis, groups in r.get('regimes', {}).items():
        result += f'<h4>{escape(axis)}</h4>' + table(['Group', 'Model', 'Rows', 'Hit rate', 'IC', 'Log loss', 'Brier'], [
            [group, name, m['n'], m['directional_accuracy'], m['spearman_ic'], m.get('log_loss'), m.get('brier_score')]
            for group, models in groups.items() for name, m in models.items()])
    result += stop_section(r.get('stop_metrics', {}))
    result += '<details><summary>Stop metrics per fold</summary>' + table(
        ['Fold', 'Stop', 'Method', 'Rows', 'Target hit', 'Actual hit', 'Pinball'],
        [[f['fold'], name, method, values.get(method, {}).get('n'), values.get('target_hit_rate'),
          values.get(method, {}).get('hit_rate'), values.get(method, {}).get('pinball_loss')]
         for f in folds for name, values in f.get('stop_metrics', {}).items()
         for method in ('model', 'unconditional', 'volatility_scaled')]) + '</details>'
    if r.get('shuffle_test'):
        result += '<h3>Shuffle / leak check</h3><p>Training labels move in whole-day blocks. Useful direction skill should disappear: IC near zero and hit rate near 50%. A flag is a diagnostic, not proof of leakage.</p>'
        result += direction_charts(r['shuffle_test'], 'Shuffled training labels')
        result += chart('Shuffled IC per fold', [(f"Fold {f['fold']} {name}", m.get('spearman_ic'), COLORS[i % 4])
            for f in folds for i, (name, m) in enumerate(f.get('shuffle_test', {}).items()) if isinstance(m, dict)],
            'Day blocks are shuffled separately within each training window; persistent held-out ranking skill merits investigation.')
        for name, values in r['shuffle_test'].items():
            result += '<p><strong>' + escape(name + ': ' + values['flag']) + '</strong> · SE=' + fmt(values['ic_zero_se']) + '; ' + escape(values['se_formula']) + '</p>'
    result += '<h3>Feature importance: top 30 and groups</h3>'
    for name, values in r.get('importance', {}).items():
        result += '<h4>' + escape(name) + '</h4><p>' + escape(values['notes']) + f" Sample: {values['sample_rows']} development rows.</p>"
        for key, metric, title in [('top_gain', 'gain', 'LightGBM gain'), ('top_shap', 'mean_abs_shap', 'Mean absolute SHAP')]:
            result += chart(title, [(v['feature'], v[metric], COLORS[0]) for v in values[key]],
                            'Larger values mean the model relied more on this feature; importance does not establish cause or future profitability.')
        for key in ('by_group', 'by_source'):
            result += '<h4>' + key + '</h4>' + table(['Group / source', 'Gain', 'Mean |SHAP|'],
                [[group, v['gain'], v['mean_abs_shap']] for group, v in sorted(values[key].items(), key=lambda kv: -kv[1]['mean_abs_shap'])])
    if 'lockbox_results' in r:
        result += '<h3>Opened lockbox</h3><p>' + escape(r['lockbox_results']['notes']) + '</p>'
        result += direction_charts(r['lockbox_results']['metrics'], 'Lockbox direction')
        result += stop_section(r['lockbox_results']['stop_metrics'])
        if r['lockbox_results'].get('bootstrap'):
            result += '<h4>Lockbox paired day-block bootstrap</h4>' + table(
                ['Comparison', 'Block days', 'Paired rows', 'Mean difference', '2.5%', '50%', '97.5%'], [
                [name, length, values['paired_rows'], values['mean_difference'], values['p2_5'], values['p50'], values['p97_5']]
                for length, scores in r['lockbox_results']['bootstrap']['blocks'].items()
                for name, values in scores.items()])
    return result + '</article>'


def render(reports, version):
    return ('<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">'
            '<title>Evaluation ' + escape(version) + '</title><style>'
            'body{font:15px system-ui,sans-serif;max-width:1200px;margin:32px auto;padding:0 20px;color:#202b39;background:#fafafa}'
            'article{border-top:3px solid #315ba8;margin-top:40px}table{border-collapse:collapse;width:100%;margin:15px 0;font-size:13px}'
            'th,td{border-bottom:1px solid #ddd;text-align:left;padding:7px}svg{width:100%;max-width:1000px;background:white}'
            '.caption{color:#52616e}pre{white-space:pre-wrap}h3{margin-top:32px}</style><body><h1>Trustworthy evaluation · '
            + escape(version) + '</h1><p>Returns and stop distances describe mid-price moves; these are not trading profits.</p>'
            + ''.join(target_section(r) for r in reports) + '</body></html>')


def write_model_report(report, directory):
    path = Path(directory).parent / f"report_{report['version']}.html"
    path.write_text(render([report], report['version']))
    return path


def generate_report(output_root, version):
    output = Path(output_root)
    source = output / f'research_report_{version}.json'
    if source.exists():
        reports = json.loads(source.read_text())['results']
    else:
        reports = [json.loads((p / 'metadata.json').read_text()) for p in sorted((output / 'models').glob('*/*/*'))
                   if p.name == version and (p / 'metadata.json').is_file()]
    if not reports:
        raise ValueError(f'No results for version {version}')
    for report in reports:
        if report.get('model_dir'):
            write_model_report(report, report['model_dir'])
    path = output / f'report_{version}.html'
    path.write_text(render(reports, version))
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output_root', type=Path)
    parser.add_argument('--version', required=True)
    args = parser.parse_args(argv)
    if Path(args.version).name != args.version or args.version in {'.', '..'}:
        parser.error('version must be a single directory name')
    print(generate_report(args.output_root, args.version))


if __name__ == '__main__':
    main()
