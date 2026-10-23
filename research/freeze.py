"""Freeze saved models before opening their shared lockbox."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess

from research.evaluation import date_intervals_overlap


def model_snapshot(root, version, report):
    root = Path(root).resolve()
    directory = root / 'models' / report['target'] / str(report['horizon_s']) / version
    if Path(report['model_dir']).resolve() != directory.resolve():
        raise ValueError(f'Model directory differs from discovery: {directory}')
    files = {p.relative_to(directory).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in sorted(directory.rglob('*')) if p.is_file()
             and p.name not in {'metadata.json', 'oof.parquet', 'training_rows.parquet'}
             and p.suffix.lower() != '.html'}
    metadata = {k: v for k, v in report.items() if k != 'lockbox_results'}
    return {'root': str(root), 'version': version, 'target': report['target'],
            'horizon_s': report['horizon_s'], 'files': files,
            'metadata_sha256': hashlib.sha256(json.dumps(metadata, sort_keys=True,
                separators=(',', ':'), allow_nan=False).encode()).hexdigest()}


def validate_plan(path, root, version, reports):
    """Validate every selected artifact before reading lockbox rows or labels."""
    raw = Path(path).read_bytes()
    plan = json.loads(raw)
    for report in reports:
        current = model_snapshot(root, version, report)
        matches = [m for m in plan['models'] if all(m[k] == current[k]
                   for k in ('root', 'version', 'target', 'horizon_s'))]
        if len(matches) != 1 or report['date_split']['lockbox'] != plan['lockbox_range']:
            raise ValueError('Model or lockbox range is not listed in the lockbox plan')
        frozen = matches[0]
        # Opening adds this output; it is not an input to subsequent evaluation.
        if 'lockbox_results' in report and 'lockbox.parquet' not in frozen['files']:
            current['files'].pop('lockbox.parquet', None)
        if current != frozen:
            raise ValueError(f'Lockbox plan hashes differ: {version}:{report["target"]}/{report["horizon_s"]}')
    return hashlib.sha256(raw).hexdigest()


def freeze_plan(path, entries, rules, only=(), lockbox_ledger=None):
    from research.train import lockbox_reports

    selected = {}
    for value in only:
        version, pair = value.rsplit(':', 1)
        target, horizon = pair.rsplit('/', 1)
        selected.setdefault(version, set()).add((target, int(horizon)))
    models, lockbox_range, ledgers, found = [], None, set(), set()
    for value in entries:
        root, version = value.rsplit(':', 1)
        root = Path(root).resolve()
        if not version or Path(version).name != version or version in {'.', '..'}:
            raise ValueError('version must be a single directory name')
        reports = lockbox_reports(root, version)
        if version in selected:
            reports = [r for r in reports if (r['target'], r['horizon_s']) in selected[version]]
        if not reports:
            raise ValueError(f'Entry finds no models: {value}')
        for report in reports:
            if 'lockbox_results' in report:
                raise ValueError('Cannot freeze an already opened model')
            span = report['date_split']['lockbox']
            if span is None:
                raise ValueError('Model has no lockbox days')
            if lockbox_range is not None and span != lockbox_range:
                raise ValueError('Models have different lockbox ranges')
            lockbox_range = span
            model = model_snapshot(root, version, report)
            if model in models:
                raise ValueError('Duplicate model in lockbox plan')
            models.append(model)
            found.add((version, report['target'], report['horizon_s']))
        ledgers.add(Path(lockbox_ledger) if lockbox_ledger is not None else root / 'lockbox_ledger.jsonl')
    if not models:
        raise ValueError('Plan needs at least one model')
    if any((v, t, h) not in found for v, pairs in selected.items() for t, h in pairs):
        raise ValueError('--only names a model not found in the entries')
    for ledger in ledgers:
        if ledger.exists():
            previous = [json.loads(line) for line in ledger.read_text().splitlines() if line.strip()]
            if any(date_intervals_overlap(lockbox_range, item['lockbox_range']) for item in previous):
                raise ValueError('Ledger already overlaps the lockbox range; freeze before any opening')
    git = {'sha': None, 'dirty': None}
    try:
        repo = Path(__file__).resolve().parents[1]
        git['sha'] = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=repo, capture_output=True,
                                    text=True, check=True, timeout=5).stdout.strip()
        git['dirty'] = bool(subprocess.run(['git', 'status', '--porcelain'], cwd=repo, capture_output=True,
                                           text=True, check=True, timeout=5).stdout.strip())
    except (OSError, subprocess.SubprocessError):
        pass
    plan = {'created_utc': datetime.now(timezone.utc).isoformat(), 'git': git,
            'lockbox_range': lockbox_range, 'rules': list(rules), 'models': models}
    with Path(path).open('x') as out:
        json.dump(plan, out, indent=2, allow_nan=False)
        out.write('\n')
    print(f'Frozen {len(models)} models: {lockbox_range["first_day"]} through '
          f'{lockbox_range["last_day"]}; {len(rules)} rules. Plan: {path}')
    return plan


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan_out', type=Path)
    parser.add_argument('--entry', action='append', required=True, metavar='ROOT:VERSION')
    parser.add_argument('--only', action='append', default=[], metavar='VERSION:TARGET/H')
    parser.add_argument('--rule', action='append', required=True)
    parser.add_argument('--lockbox-ledger', type=Path)
    args = parser.parse_args(argv)
    try:
        return freeze_plan(args.plan_out, args.entry, args.rule, args.only, args.lockbox_ledger)
    except (OSError, ValueError, KeyError) as exc:
        parser.error(str(exc))


if __name__ == '__main__':
    main()
