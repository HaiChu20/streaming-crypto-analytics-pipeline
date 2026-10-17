#!/usr/bin/env bash
# Start/stop the phase 1 pipeline on this Mac: 3 collectors + raw recorder (Kafka runs in Docker).
#
#   scripts/pipeline.sh start | stop | status | rotate | restart <name>
#
# Logs: data/logs/<name>.log   PIDs: data/state/<name>.pid
# `start` also runs `caffeinate -s` so the Mac doesn't sleep while the recorder runs (on AC power).
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
NAMES=(binance okx kraken recorder)
# Resolve .env through the same loader as collectors; print only the enable flag.
if [[ -n "${INFERENCE_MODEL_VERSION:-}" ]] || [[ "$("$PY" -c 'from collectors.common import config; import os; print("enabled" if os.environ.get("INFERENCE_MODEL_VERSION") else "")')" == enabled ]]; then
  NAMES+=(inference)
fi
mkdir -p data/logs data/state

cmd_for() {
  case "$1" in
    inference) echo "$PY -m inference.service" ;;
    recorder) echo "$PY -m archiver.record_raw" ;;
    *) echo "$PY -m collectors $1" ;;
  esac
}

alive() { [[ -f "data/state/$1.pid" ]] && kill -0 "$(cat "data/state/$1.pid")" 2>/dev/null; }

case "${1:-}" in
  start)
    failed=0
    for name in "${NAMES[@]}"; do
      if alive "$name"; then echo "$name already running"; continue; fi
      nohup $(cmd_for "$name") >> "data/logs/$name.log" 2>&1 &
      echo $! > "data/state/$name.pid"
      echo "started $name (pid $!)"
      sleep 1
      if ! alive "$name"; then echo "FAILED to start $name, see data/logs/$name.log"; failed=1; fi
    done
    if [[ "$failed" -ne 0 ]]; then exit 1; fi
    nohup caffeinate -s -w "$(cat data/state/recorder.pid)" >/dev/null 2>&1 &
    ;;
  stop)
    for name in "${NAMES[@]}"; do
      if alive "$name"; then kill -TERM "$(cat "data/state/$name.pid")" || true; echo "stopping $name"; fi
    done
    for attempt in {0..120}; do
      live=""
      for name in "${NAMES[@]}"; do
        if alive "$name"; then live+="$name "; fi
      done
      if [[ -z "$live" ]]; then exit 0; fi
      if [[ "$attempt" -lt 120 ]]; then sleep 0.5; fi
    done
    echo "still alive after 60s: $live" >&2
    exit 1
    ;;
  restart)
    name="${2:-}"
    valid=0
    for service in "${NAMES[@]}"; do
      if [[ "$name" == "$service" ]]; then valid=1; fi
    done
    if [[ "$valid" -ne 1 || "$#" -ne 2 ]]; then
      echo "usage: $0 restart <name> (${NAMES[*]})" >&2
      exit 2
    fi
    if alive "$name"; then kill -TERM "$(cat "data/state/$name.pid")" || true; echo "stopping $name"; fi
    for attempt in {0..120}; do
      if ! alive "$name"; then break; fi
      if [[ "$attempt" -eq 120 ]]; then echo "still alive after 60s: $name" >&2; exit 1; fi
      sleep 0.5
    done
    nohup $(cmd_for "$name") >> "data/logs/$name.log" 2>&1 &
    echo $! > "data/state/$name.pid"
    echo "started $name (pid $!)"
    sleep 1
    if ! alive "$name"; then echo "FAILED to start $name, see data/logs/$name.log" >&2; exit 1; fi
    if [[ "$name" == recorder ]]; then
      nohup caffeinate -s -w "$(cat data/state/recorder.pid)" >/dev/null 2>&1 &
    fi
    ;;
  status)
    for name in "${NAMES[@]}"; do
      if alive "$name"; then echo "$name: running (pid $(cat "data/state/$name.pid"))"; else echo "$name: stopped"; fi
    done
    ;;
  rotate)
    for name in binance okx kraken; do
      if alive "$name"; then kill -HUP "$(cat "data/state/$name.pid")"; echo "rotating $name"; fi
    done
    ;;
  *)
    echo "usage: $0 start|stop|status|rotate|restart <name>" >&2
    exit 2
    ;;
esac
