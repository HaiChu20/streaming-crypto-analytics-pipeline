#!/usr/bin/env bash
# Start/stop the phase 1 pipeline on this Mac: 3 collectors + raw recorder (Kafka runs in Docker).
#
#   scripts/pipeline.sh start | stop | status | rotate
#
# Logs: data/logs/<name>.log   PIDs: data/state/<name>.pid
# `start` also runs `caffeinate -s` so the Mac doesn't sleep while the recorder runs (on AC power).
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
NAMES=(binance okx kraken recorder)
mkdir -p data/logs data/state

cmd_for() {
  case "$1" in
    recorder) echo "$PY -m archiver.record_raw" ;;
    *) echo "$PY -m collectors $1" ;;
  esac
}

alive() { [[ -f "data/state/$1.pid" ]] && kill -0 "$(cat "data/state/$1.pid")" 2>/dev/null; }

case "${1:-}" in
  start)
    for name in "${NAMES[@]}"; do
      if alive "$name"; then echo "$name already running"; continue; fi
      nohup $(cmd_for "$name") >> "data/logs/$name.log" 2>&1 &
      echo $! > "data/state/$name.pid"
      echo "started $name (pid $!)"
      sleep 1
    done
    nohup caffeinate -s -w "$(cat data/state/recorder.pid)" >/dev/null 2>&1 &
    ;;
  stop)
    for name in "${NAMES[@]}"; do
      if alive "$name"; then kill -TERM "$(cat "data/state/$name.pid")"; echo "stopping $name"; fi
    done
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
    echo "usage: $0 start|stop|status|rotate" >&2
    exit 2
    ;;
esac
