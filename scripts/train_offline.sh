#!/usr/bin/env bash
# Replay closed v3 days, then train local CPU baselines. No services or credentials.
set -euo pipefail

usage() {
    echo "Usage: $0 <archive_root> <work_dir> [--jobs N] [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--version V] [--allow-missing-days] [--rebuild-stale] [--replay-only] [--exchange NAME] [--market NAME] [--exchange-clock-ms N] [--skip-marker-only-days] [research.train flags: --label-spec PATH, --train-from YYYY-MM-DD, --threads, --lockbox-pct/--lockbox-from/--embargo-days, --fold-days/--min-train-days/--fold-embargo-s/--max-folds, --shuffle-test, --max-memory-gb, --open-lockbox/--i-understand-lockbox-reuse]" >&2
}
fail() { echo "train_offline: $*" >&2; exit 1; }
[[ $# -ge 2 ]] || { usage; exit 2; }
archive_root=$1
work_dir=$2
shift 2
jobs=""
from_day=""
to_day=""
version="offline-$(date -u +%Y%m%dT%H%M%SZ)-$$"
allow_missing=0
rebuild_stale=0
train_args=(--embargo-days 1)
lockbox_pct=20
lockbox_from=""
embargo_days=1
open_lockbox=0
version_set=0
replay_only=0
exchange_filter=""
market_filter=""
exchange_clock_ms=""
skip_marker_only=0
replay_args=(--mode ideal)
while [[ $# -gt 0 ]]; do
    case "$1" in
        --jobs|--from|--to|--version)
            [[ $# -ge 2 && -n $2 ]] || fail "$1 requires a value"
            case "$1" in
                --jobs) jobs=$2 ;;
                --from) from_day=$2 ;;
                --to) to_day=$2 ;;
                --version) version=$2; version_set=1 ;;
            esac
            shift 2 ;;
        --exchange|--market|--exchange-clock-ms)
            [[ $# -ge 2 && -n $2 ]] || fail "$1 requires a value"
            case "$1" in
                --exchange) exchange_filter=$2 ;;
                --market) market_filter=$2 ;;
                --exchange-clock-ms) exchange_clock_ms=$2 ;;
            esac
            replay_args+=("$1" "$2")
            shift 2 ;;
        --skip-marker-only-days)
            skip_marker_only=1; replay_args+=("$1"); shift ;;
        --threads|--label-spec|--train-from|--lockbox-pct|--lockbox-from|--embargo-days|--fold-days|--min-train-days|--fold-embargo-s|--max-folds|--max-memory-gb|--stride-s|--target|--horizon-s|--rounds|--purge-s)
            [[ $# -ge 2 && -n $2 ]] || fail "$1 requires a value"
            train_args+=("$1" "$2")
            case "$1" in
                --lockbox-pct) lockbox_pct=$2 ;;
                --lockbox-from) lockbox_from=$2 ;;
                --embargo-days) embargo_days=$2 ;;
                --threads) [[ $2 =~ ^[1-9][0-9]*$ ]] || fail "--threads must be a positive integer" ;;
            esac
            shift 2 ;;
        --shuffle-test|--open-lockbox|--i-understand-lockbox-reuse)
            train_args+=("$1")
            if [[ $1 == --open-lockbox ]]; then open_lockbox=1; fi
            shift ;;
        --allow-missing-days) allow_missing=1; shift ;;
        --rebuild-stale) rebuild_stale=1; shift ;;
        --replay-only) replay_only=1; shift ;;
        *) usage; fail "unknown option: $1" ;;
    esac
done
[[ -z $exchange_filter || $exchange_filter =~ ^(binance|kraken|okx|deribit)$ ]] || fail "unknown --exchange: $exchange_filter"
[[ -z $market_filter || $market_filter =~ ^(spot|perp|index)$ ]] || fail "unknown --market: $market_filter"
if [[ -n $exchange_clock_ms ]]; then
    [[ $exchange_clock_ms =~ ^[0-9]+$ && $exchange_filter == kraken && $market_filter == spot ]] || fail "--exchange-clock-ms requires non-negative integer milliseconds, --exchange kraken --market spot"
fi
[[ $version =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || fail "--version must be a single directory name"
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
[[ -d $archive_root ]] || fail "archive directory does not exist: $archive_root"
archive_root=$(cd "$archive_root" && pwd)
mkdir -p "$work_dir"
work_dir=$(cd "$work_dir" && pwd)
cd "$repo_root"
export PYTHON_DOTENV_DISABLED=1
export PATH="${HOME}/.cargo/bin:$PATH"
if [[ -x $repo_root/.venv/bin/python ]]; then
    python_cmd=("$repo_root/.venv/bin/python")
elif command -v uv >/dev/null 2>&1; then
    python_cmd=(uv run --no-sync python)
else
    fail "Python environment is missing. Install uv (https://docs.astral.sh/uv/getting-started/installation/), then run uv sync in $repo_root."
fi
"${python_cmd[@]}" -c 'import lightgbm, numpy, polars, pyarrow, yaml' || fail "Python dependencies are unavailable. Run uv sync (LightGBM also needs the system OpenMP runtime)."
if [[ $open_lockbox -eq 1 ]]; then
    [[ $version_set -eq 1 ]] || fail "--open-lockbox requires an already-trained --version"
    "${python_cmd[@]}" -m research.train "$work_dir" --version "$version" "${train_args[@]}"
    exit 0
fi
command -v cargo >/dev/null 2>&1 || fail "cargo is missing. Install Rust with rustup (https://rustup.rs), then restart your shell."
if [[ -z $jobs ]]; then
    jobs=$("${python_cmd[@]}" -c 'from research.threads import available_cpus; print(max(1, available_cpus() // 2))')
fi
[[ $jobs =~ ^[1-9][0-9]*$ ]] || fail "--jobs must be a positive integer"
logs="$work_dir/logs"
mkdir -p "$logs" "$work_dir/features"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
days_file="$logs/days-$run_id.txt"
# Inspect only the accepted three-level exchange/market/pair tree. .open files
# are intentionally excluded; missing channels/venues do not invent missing days.
"${python_cmd[@]}" - "$archive_root" "$from_day" "$to_day" "$exchange_filter" "$market_filter" > "$days_file" <<'PY'
from datetime import date
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
def bound(value):
    if not value:
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise SystemExit(f'Invalid date {value!r}; expected YYYY-MM-DD') from None
    if parsed.isoformat() != value:
        raise SystemExit(f'Invalid date {value!r}; expected YYYY-MM-DD')
    return parsed

start, end = map(bound, sys.argv[2:4])
exchange_filter, market_filter = sys.argv[4:6]
if start and end and start > end:
    raise SystemExit('--from must be <= --to')
root = root / 'raw' if (root / 'raw').is_dir() else root
days = set()
streams = {}
for exchange in root.iterdir():
    if not exchange.is_dir() or not exchange.name.lower().startswith(('binance', 'kraken', 'okx', 'deribit')):
        continue
    name = exchange.name.lower()
    canonical_exchange = next(e for e in ('binance', 'kraken', 'okx', 'deribit') if name.startswith(e))
    if exchange_filter and canonical_exchange != exchange_filter:
        continue
    for market in exchange.iterdir():
        if not market.is_dir() or not market.name.lower().startswith(('spot', 'fut', 'perp', 'swap', 'index')):
            continue
        canonical_market = ('spot' if market.name.lower().startswith('spot') else
                            'index' if market.name.lower().startswith('index') else 'perp')
        if market_filter and canonical_market != market_filter:
            continue
        spot = canonical_market == 'spot'
        if name.startswith('deribit'):
            channels = {'dvol'} if market.name.lower().startswith('index') else set()
        elif name.startswith('binance'):
            channels = {'depth', 'snapshot', 'bookticker', 'trades'}
            if not spot:
                channels |= {'aggtrades', 'markprice', 'forceorder'}
        elif name.startswith('okx'):
            channels = {'book', 'bbo', 'trades', 'tradesall'}
        else:
            channels = {'book', 'level3', 'trades'} if spot else {'book', 'trades', 'ticker'}
        for pair in market.iterdir():
            if not pair.is_dir():
                continue
            for path in pair.iterdir():
                match = re.fullmatch(r'(\d{8})_([a-z0-9]+)\.jsonl(?:\.gz)?', path.name)
                if not path.is_file() or not match or match[2] not in channels:
                    continue
                day = date.fromisoformat(match[1])
                if (start is None or day >= start) and (end is None or day <= end):
                    symbol = pair.name.replace('-', '/') if canonical_exchange == 'kraken' and spot else pair.name
                    key = canonical_exchange, canonical_market, symbol, match[2], day
                    previous = streams.setdefault(key, path)
                    if previous.parent != path.parent:
                        raise SystemExit(f'duplicate archive stream for {day}: {previous} and {path}')
                    days.add(day.isoformat())
for day in sorted(days):
    print(day)
PY
[[ -s $days_file ]] || fail "no closed v3 archive days found in the requested range"
# Projected ranges from selected archive dates and all already-completed feature
# dates. Training recomputes the exact split from tick_ns after replay.
"${python_cmd[@]}" - "$days_file" "$work_dir" "$lockbox_pct" "$lockbox_from" "$embargo_days" <<'PYRANGES'
from datetime import date, timedelta
from pathlib import Path
import math
import sys
archive = [date.fromisoformat(d) for d in Path(sys.argv[1]).read_text().splitlines()]
existing = [date.fromisoformat(p.parent.name.removeprefix('date='))
            for p in Path(sys.argv[2]).glob('features/date=*/features.parquet')]
days = sorted(set(archive + existing))
pct, embargo = float(sys.argv[3]), int(sys.argv[5])
if not math.isfinite(pct) or not 0 < pct < 100 or embargo < 1:
    raise SystemExit('Need 0 < --lockbox-pct < 100 and --embargo-days >= 1')
start = date.fromisoformat(sys.argv[4]) if sys.argv[4] else days[-max(1, math.ceil(len(days)*pct/100))]
end_dev = start - timedelta(days=embargo)
def span(values):
    return f'{values[0]} .. {values[-1]} ({len(values)} days)' if values else 'none'
print('Planned UTC ranges before replay (exact tick-based ranges printed by training):')
print('Development: ' + span([d for d in days if d < end_dev]))
print(f'Embargo: {end_dev} .. {start-timedelta(days=1)}')
print('Lockbox (closed): ' + span([d for d in days if d >= start]))
PYRANGES
echo "Building release replay (log: $logs/build-$run_id.log)"
if ! (cd "$repo_root/engine" && cargo build --release --locked -p engine-replay) > "$logs/build-$run_id.log" 2>&1; then
    fail "Rust build failed; see $logs/build-$run_id.log. Install/update the toolchain required by engine/ via rustup."
fi
replay="$repo_root/engine/target/release/engine-replay"
engine_version=$("$replay" --engine-version) || fail "could not read engine version from $replay"
[[ -n $engine_version ]] || fail "replay returned an empty engine version: $replay"
replay_mode=ideal
if [[ -n $exchange_clock_ms ]]; then
    clock_value=$("${python_cmd[@]}" -c 'import sys; print(int(sys.argv[1]))' "$exchange_clock_ms")
    replay_mode="ideal+exchange_clock_${clock_value}ms"
fi
# Training includes every completed day in work_dir, even outside --from/--to.
# Validate all of them before any replay or replacement, then plan selected days.
replay_plan="$logs/replay-plan-$run_id.txt"
"${python_cmd[@]}" - "$work_dir" "$days_file" "$engine_version" "$rebuild_stale" "$replay_mode" "$skip_marker_only" > "$replay_plan" <<'PY'
from pathlib import Path
import json
import sys
import pyarrow.parquet as pq

work, days_file = map(Path, sys.argv[1:3])
expected, rebuild = sys.argv[3], sys.argv[4] == '1'
expected_mode, skip_marker_only = sys.argv[5], sys.argv[6] == '1'
days = days_file.read_text().splitlines()
stale = {}
for path in sorted(work.glob('features/date=*/features.parquet')):
    try:
        metadata = pq.read_metadata(path).metadata or {}
        actual = metadata.get(b'engine_version')
        mode = metadata.get(b'mode', metadata.get(b'replay_mode', b'ideal')).decode()
        reason = f'engine_version={actual!r}, replay_mode={mode!r}'
    except (OSError, ValueError) as error:
        actual, mode, reason = None, None, f'unreadable metadata: {error}'
    if actual != expected.encode() or mode != expected_mode:
        stale[path.parent.name.removeprefix('date=')] = reason
for path in sorted(work.glob('features/date=*/skipped.json')):
    if (path.parent / 'features.parquet').exists():
        raise SystemExit(f'Conflicting completed features and skipped day: {path.parent}')
    saved = json.loads(path.read_text())
    if not skip_marker_only:
        raise SystemExit(f'{path}: resubmit with --skip-marker-only-days or use a new work dir')
    if saved.get('engine_version') != expected or saved.get('replay_mode') != expected_mode:
        stale[path.parent.name.removeprefix('date=')] = 'stale skipped.json provenance'
if stale and (not rebuild or stale.keys() - set(days)):
    details = '\n'.join(f'  {day}: {reason}' for day, reason in stale.items())
    raise SystemExit(
        f'Stale feature days (expected engine_version={expected!r}, replay_mode={expected_mode!r}):\n{details}\n'
        'Use a new work dir, or --rebuild-stale with archive inputs and a date range covering every stale day.'
    )
for day in days:
    exists = any((work / f'features/date={day}' / name).is_file() for name in ('features.parquet', 'skipped.json'))
    action = 'rebuild' if day in stale else ('reuse' if exists else 'replay')
    print(day, action)
PY
replay_day() {
    local day=$1
    local action=$2
    local final="$work_dir/features/date=$day/features.parquet"
    local staging="$work_dir/.replay-$day-$run_id"
    local report_tmp="$logs/$day-$run_id.json.tmp"
    if [[ $action == reuse ]]; then
        echo "Skip $day: complete features already exist or day recorded in skipped.json"
        return 0
    fi
    echo "Replay $day (log: $logs/$day.log)"
    local replay_status=0
    "$replay" --root "$archive_root" --date "$day" --out "$staging" "${replay_args[@]}" > "$report_tmp" 2> "$logs/$day.log" || replay_status=$?
    if [[ $replay_status -eq 3 && $skip_marker_only -eq 1 ]]; then
        "${python_cmd[@]}" - "$report_tmp" "$work_dir/features/date=$day/skipped.json" "$engine_version" "$replay_mode" <<'PYSKIP'
import json
import os
from pathlib import Path
import sys
source, target = map(Path, sys.argv[1:3])
report = json.loads(source.read_text())
if (report.get('status') != 'skipped' or report.get('reason') != 'marker_only_inputs'
        or not report.get('files') or report.get('engine_version') != sys.argv[3]
        or report.get('replay_mode') != sys.argv[4]):
    raise SystemExit('Invalid marker-only skip report')
if (target.parent / 'features.parquet').exists():
    raise SystemExit('Cannot publish skipped.json over existing features; use a new work dir')
target.parent.mkdir(parents=True, exist_ok=True)
os.rename(source, target)
PYSKIP
        return $?
    fi
    if [[ $replay_status -ne 0 ]]; then
        echo "Replay failed for $day; see $logs/$day.log" >&2
        return 1
    fi
    # Publish only newly generated outputs. Source archives are never modified.
    # Staging and reports stay in work_dir, so these renames are atomic. Failed
    # or interrupted staging is retained and never treated as a completed day.
    if ! "${python_cmd[@]}" - "$staging/date=$day/features.parquet" "$final" "$report_tmp" "$logs/$day.json" "$engine_version" "$replay_mode" <<'PY'
import json
import os
from pathlib import Path
import sys
import pyarrow.parquet as pq
source, target, report_tmp, report_path = map(Path, sys.argv[1:5])
report = json.loads(report_tmp.read_text())
if not source.is_file():
    raise SystemExit(f'Replay did not produce {source}')
metadata = pq.read_metadata(source).metadata or {}
if metadata.get(b'engine_version') != sys.argv[5].encode():
    raise SystemExit(f'Replay produced an unexpected engine_version in {source}')
mode = metadata.get(b'mode', metadata.get(b'replay_mode', b'ideal')).decode()
if mode != sys.argv[6]:
    raise SystemExit(f'Replay produced an unexpected replay mode in {source}: {mode!r}')
target.parent.mkdir(parents=True, exist_ok=True)
report['output'] = str(target)
report_tmp.write_text(json.dumps(report, indent=2) + '\n')
os.rename(report_tmp, report_path)
previous_skip = target.parent / 'skipped.json'
if previous_skip.exists():
    os.rename(previous_skip, report_tmp.with_suffix('.skipped.json'))
os.rename(source, target)  # The complete-day marker is published last.
PY
    then
        echo "Could not publish $day; staging retained at $staging" >&2
        return 1
    fi
}
# Bounded batches also work with macOS Bash 3.2 (no wait -n dependency).
pids=()
running_days=()
running_actions=()
active=0
failures=0
failed_days=""
failed_stale_days=""
wait_batch() {
    local i
    for ((i=0; i<active; i++)); do
        if ! wait "${pids[$i]}"; then
            failures=$((failures + 1))
            failed_days="$failed_days ${running_days[$i]}"
            if [[ ${running_actions[$i]} == rebuild ]]; then
                failed_stale_days="$failed_stale_days ${running_days[$i]}"
            fi
        fi
    done
    active=0
    [[ -z $failed_stale_days ]] || fail "failed stale rebuild days:$failed_stale_days. Training stopped; retry --rebuild-stale or use a new work dir."
    if [[ $failures -gt 0 && $allow_missing -eq 0 ]]; then
        fail "failed replay days:$failed_days. Training stopped; rerun to retry, or explicitly use --allow-missing-days."
    fi
}
while read -r day action; do
    replay_day "$day" "$action" &
    pids[$active]=$!
    running_days[$active]=$day
    running_actions[$active]=$action
    active=$((active + 1))
    if [[ $active -ge $jobs ]]; then wait_batch; fi
done < "$replay_plan"
wait_batch
if [[ $failures -gt 0 ]]; then
    echo "WARNING: --allow-missing-days: training without failed days:$failed_days" >&2
fi
"${python_cmd[@]}" - "$work_dir" "$days_file" "$logs/run-summary-$run_id.json" <<'PYSUMMARY'
import json
from pathlib import Path
import sys
work, selected, output = map(Path, sys.argv[1:])
skipped = []
for day in selected.read_text().splitlines():
    path = work / f'features/date={day}/skipped.json'
    if path.is_file():
        skipped.append(json.loads(path.read_text()))
output.write_text(json.dumps({'skipped_days': skipped}, indent=2) + '\n')
print('Skipped marker-only days: ' + (', '.join(item['date'] for item in skipped) or 'none'))
print(f'Run summary: {output}')
PYSUMMARY
if [[ $replay_only -eq 1 ]]; then
    echo "Replay complete (--replay-only); training skipped"
    exit 0
fi
if ! compgen -G "$work_dir/features/date=*/features.parquet" > /dev/null; then
    echo "No completed feature days; training skipped"
    exit 0
fi
# Discovered successful outputs are the only published feature files; training
# reads all complete days in work_dir, including successful earlier invocations.
echo "Training CPU baselines, version=$version (log: $logs/train-$version.log)"
"${python_cmd[@]}" -m research.train "$work_dir" --version "$version" "${train_args[@]}" 2>&1 | tee "$logs/train-$version.log"
