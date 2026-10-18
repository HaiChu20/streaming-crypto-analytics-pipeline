#!/usr/bin/env bash
# Replay closed v3 days, then train local CPU baselines. No services or credentials.
set -euo pipefail

usage() {
    echo "Usage: $0 <archive_root> <work_dir> [--jobs N] [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--version V] [--allow-missing-days]" >&2
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
    if not exchange.is_dir() or not exchange.name.lower().startswith(('binance', 'kraken', 'okx')):
        continue
    name = exchange.name.lower()
    for market in exchange.iterdir():
        if not market.is_dir() or not market.name.lower().startswith(('spot', 'fut', 'perp', 'swap')):
            continue
        spot = market.name.lower().startswith('spot')
        if name.startswith('binance'):
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
replay_day() {
    local day=$1
    local final="$work_dir/features/date=$day/features.parquet"
    local staging="$work_dir/.replay-$day-$run_id"
    local report_tmp="$logs/$day-$run_id.json.tmp"
    if [[ -f $final ]]; then
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
    if ! "${python_cmd[@]}" - "$staging/date=$day/features.parquet" "$final" "$report_tmp" "$logs/$day.json" <<'PY'
import json
import os
from pathlib import Path
import sys
source, target, report_tmp, report_path = map(Path, sys.argv[1:])
report = json.loads(report_tmp.read_text())
if not source.is_file():
    raise SystemExit(f'Replay did not produce {source}')
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
active=0
failures=0
failed_days=""
wait_batch() {
    local i
    for ((i=0; i<active; i++)); do
        if ! wait "${pids[$i]}"; then
            failures=$((failures + 1))
            failed_days="$failed_days ${running_days[$i]}"
        fi
    done
    active=0
    if [[ $failures -gt 0 && $allow_missing -eq 0 ]]; then
        fail "failed replay days:$failed_days. Training stopped; rerun to retry, or explicitly use --allow-missing-days."
    fi
}
while IFS= read -r day; do
    replay_day "$day" &
    pids[$active]=$!
    running_days[$active]=$day
    active=$((active + 1))
    if [[ $active -ge $jobs ]]; then wait_batch; fi
done < "$days_file"
wait_batch
if [[ $failures -gt 0 ]]; then
    echo "WARNING: --allow-missing-days: training without failed days:$failed_days" >&2
fi
# Discovered successful outputs are the only published feature files; training
# reads all complete days in work_dir, including successful earlier invocations.
echo "Training CPU baselines, version=$version (log: $logs/train-$version.log)"
"${python_cmd[@]}" -m research.train "$work_dir" --version "$version" 2>&1 | tee "$logs/train-$version.log"
