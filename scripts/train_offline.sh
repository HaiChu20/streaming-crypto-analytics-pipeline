#!/usr/bin/env bash
# Replay closed v3 days, then train local CPU baselines. No services or credentials.
set -euo pipefail

usage() {
    echo "Usage: $0 <archive_root> <work_dir> [--jobs N] [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--version V] [--allow-missing-days] [--rebuild-stale]" >&2
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
while [[ $# -gt 0 ]]; do
    case "$1" in
        --jobs|--from|--to|--version)
            [[ $# -ge 2 && -n $2 ]] || fail "$1 requires a value"
            case "$1" in
                --jobs) jobs=$2 ;;
                --from) from_day=$2 ;;
                --to) to_day=$2 ;;
                --version) version=$2 ;;
            esac
            shift 2 ;;
        --allow-missing-days) allow_missing=1; shift ;;
        --rebuild-stale) rebuild_stale=1; shift ;;
        *) usage; fail "unknown option: $1" ;;
    esac
done
[[ $version =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || fail "--version must be a single directory name"
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
[[ -d $archive_root ]] || fail "archive directory does not exist: $archive_root"
archive_root=$(cd "$archive_root" && pwd)
mkdir -p "$work_dir"
work_dir=$(cd "$work_dir" && pwd)
cd "$repo_root"
export PYTHON_DOTENV_DISABLED=1
export PATH="${HOME}/.cargo/bin:$PATH"
command -v cargo >/dev/null 2>&1 || fail "cargo is missing. Install Rust with rustup (https://rustup.rs), then restart your shell."
if [[ -x $repo_root/.venv/bin/python ]]; then
    python_cmd=("$repo_root/.venv/bin/python")
elif command -v uv >/dev/null 2>&1; then
    python_cmd=(uv run --no-sync python)
else
    fail "Python environment is missing. Install uv (https://docs.astral.sh/uv/getting-started/installation/), then run uv sync in $repo_root."
fi
"${python_cmd[@]}" -c 'import lightgbm, numpy, polars, pyarrow, yaml' || fail "Python dependencies are unavailable. Run uv sync (LightGBM also needs the system OpenMP runtime)."
if [[ -z $jobs ]]; then
    jobs=$("${python_cmd[@]}" -c 'import os; print(max(1, (os.cpu_count() or 2) // 2))')
fi
[[ $jobs =~ ^[1-9][0-9]*$ ]] || fail "--jobs must be a positive integer"
logs="$work_dir/logs"
mkdir -p "$logs" "$work_dir/features"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
days_file="$logs/days-$run_id.txt"
# Inspect only the accepted three-level exchange/market/pair tree. .open files
# are intentionally excluded; missing channels/venues do not invent missing days.
"${python_cmd[@]}" - "$archive_root" "$from_day" "$to_day" > "$days_file" <<'PY'
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

start, end = map(bound, sys.argv[2:])
if start and end and start > end:
    raise SystemExit('--from must be <= --to')
root = root / 'raw' if (root / 'raw').is_dir() else root
days = set()
for exchange in root.iterdir():
    if not exchange.is_dir() or not exchange.name.lower().startswith(('binance', 'kraken', 'okx', 'deribit')):
        continue
    name = exchange.name.lower()
    for market in exchange.iterdir():
        if not market.is_dir() or not market.name.lower().startswith(('spot', 'fut', 'perp', 'swap', 'index')):
            continue
        spot = market.name.lower().startswith('spot')
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
                    days.add(day.isoformat())
for day in sorted(days):
    print(day)
PY
[[ -s $days_file ]] || fail "no closed v3 archive days found in the requested range"
echo "Building release replay (log: $logs/build-$run_id.log)"
if ! (cd "$repo_root/engine" && cargo build --release --locked -p engine-replay) > "$logs/build-$run_id.log" 2>&1; then
    fail "Rust build failed; see $logs/build-$run_id.log. Install/update the toolchain required by engine/ via rustup."
fi
replay="$repo_root/engine/target/release/engine-replay"
engine_version=$("$replay" --engine-version) || fail "could not read engine version from $replay"
[[ -n $engine_version ]] || fail "replay returned an empty engine version: $replay"
# Training includes every completed day in work_dir, even outside --from/--to.
# Validate all of them before any replay or replacement, then plan selected days.
replay_plan="$logs/replay-plan-$run_id.txt"
"${python_cmd[@]}" - "$work_dir" "$days_file" "$engine_version" "$rebuild_stale" > "$replay_plan" <<'PY'
from pathlib import Path
import sys
import pyarrow.parquet as pq

work, days_file = map(Path, sys.argv[1:3])
expected, rebuild = sys.argv[3], sys.argv[4] == '1'
days = days_file.read_text().splitlines()
stale = {}
for path in sorted(work.glob('features/date=*/features.parquet')):
    try:
        actual = (pq.read_metadata(path).metadata or {}).get(b'engine_version')
        reason = repr(actual)
    except (OSError, ValueError) as error:
        actual, reason = None, f'unreadable metadata: {error}'
    if actual != expected.encode():
        stale[path.parent.name.removeprefix('date=')] = reason
if stale and (not rebuild or stale.keys() - set(days)):
    details = '\n'.join(f'  {day}: {reason}' for day, reason in stale.items())
    raise SystemExit(
        f'Stale feature days (expected engine_version={expected!r}):\n{details}\n'
        'Use a new work dir, or --rebuild-stale with archive inputs and a date range covering every stale day.'
    )
for day in days:
    exists = (work / f'features/date={day}/features.parquet').is_file()
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
        echo "Skip $day: complete features already exist"
        return 0
    fi
    echo "Replay $day (log: $logs/$day.log)"
    if ! "$replay" --root "$archive_root" --date "$day" --out "$staging" > "$report_tmp" 2> "$logs/$day.log"; then
        echo "Replay failed for $day; see $logs/$day.log" >&2
        return 1
    fi
    # Publish only newly generated outputs. Source archives are never modified.
    # Staging and reports stay in work_dir, so these renames are atomic. Failed
    # or interrupted staging is retained and never treated as a completed day.
    if ! "${python_cmd[@]}" - "$staging/date=$day/features.parquet" "$final" "$report_tmp" "$logs/$day.json" "$engine_version" <<'PY'
import json
import os
from pathlib import Path
import sys
import pyarrow.parquet as pq
source, target, report_tmp, report_path = map(Path, sys.argv[1:5])
report = json.loads(report_tmp.read_text())
if not source.is_file():
    raise SystemExit(f'Replay did not produce {source}')
if (pq.read_metadata(source).metadata or {}).get(b'engine_version') != sys.argv[5].encode():
    raise SystemExit(f'Replay produced an unexpected engine_version in {source}')
target.parent.mkdir(parents=True, exist_ok=True)
report['output'] = str(target)
report_tmp.write_text(json.dumps(report, indent=2) + '\n')
os.rename(report_tmp, report_path)
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
# Discovered successful outputs are the only published feature files; training
# reads all complete days in work_dir, including successful earlier invocations.
echo "Training CPU baselines, version=$version (log: $logs/train-$version.log)"
"${python_cmd[@]}" -m research.train "$work_dir" --version "$version" 2>&1 | tee "$logs/train-$version.log"
