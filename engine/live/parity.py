"""Compare Kafka-captured JSONEachRow with as_live JSONEachRow, exactly except availability.

PYTHON_DOTENV_DISABLED=1 uv run python engine/live/parity.py LIVE REPLAY --startup-tick N
Uses only the Python standard library; it does not connect to Kafka or change data.
"""
import argparse
import json
import math
from collections import Counter
from pathlib import Path


def compare(live_path, replay_path, startup_tick):
    def rows(path):
        with Path(path).open() as source:
            for line in source:
                row = json.loads(line)
                if row["tick_ns"] >= startup_tick:
                    yield row

    live = list(rows(live_path))
    replay = list(rows(replay_path))
    identity = lambda r: (r["tick_ns"], r["exchange"], r["market"], r["symbol"], r["engine_version"])
    assert [identity(r) for r in live] == sorted(identity(r) for r in live), "live order changed"
    assert len({identity(r) for r in live}) == len(live), "duplicate live row identity"
    assert len({identity(r) for r in replay}) == len(replay), "duplicate replay row identity"
    mismatches = abs(len(live) - len(replay))
    differences = []
    latency = []
    coverage = Counter()
    valid = Counter()
    for index, (a, b) in enumerate(zip(live, replay)):
        latency.append(a["available_ns"] - a["tick_ns"])
        if a["exchange"] != "cross":
            instrument = "/".join(a[k] for k in ("exchange", "market", "symbol"))
            coverage[instrument] += 1
            valid[instrument] += a["book_valid"]
        a = {k: v for k, v in a.items() if k != "available_ns"}
        b = {k: v for k, v in b.items() if k != "available_ns"}
        if a != b:
            mismatches += 1
            if len(differences) < 5:
                differences.append({"row": index, "identity": identity(a), "columns": {k: [a.get(k), b.get(k)] for k in a.keys() | b.keys() if a.get(k) != b.get(k)}})
    latency.sort()
    percentile = lambda p: latency[max(0, math.ceil(len(latency) * p) - 1)] / 1e6 if latency else None
    return {"rows_compared": min(len(live), len(replay)), "live_rows": len(live), "replay_rows": len(replay), "mismatches": mismatches, "differences": differences,
            "ticks": len({r["tick_ns"] for r in live}), "first_tick": live[0]["tick_ns"] if live else None,
            "last_tick": live[-1]["tick_ns"] if live else None,
            "columns_compared": len(live[0])-1 if live else 0,
            "emit_latency_ms": {"p50": percentile(.5), "p99": percentile(.99), "max": percentile(1)},
            "book_valid_pct": 100*sum(valid.values())/sum(coverage.values()) if coverage else None,
            "book_coverage": {k: {"rows": coverage[k], "valid": valid[k], "valid_pct": 100*valid[k]/coverage[k]} for k in sorted(coverage)}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("live")
    parser.add_argument("replay")
    parser.add_argument("--startup-tick", type=int, required=True)
    args = parser.parse_args()
    result = compare(args.live, args.replay, args.startup_tick)
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["mismatches"] or not result["rows_compared"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
