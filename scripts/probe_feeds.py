"""Subscribe to every configured feed for a while, without Kafka.

Saves the first records of each stream to tests/fixtures/<exchange>/<market>/<feed>.jsonl and a
summary (acks, steady rates, estimated GB/day including daily rotation, drops) to tests/fixtures/probe-summary.json.

    uv run python scripts/probe_feeds.py --seconds 60
"""

import argparse
import asyncio
import gzip
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collectors.__main__ import HANDLERS, setup_logging  # noqa: E402
from collectors.common.config import ROOT, all_streams, load_config  # noqa: E402
from collectors.common.runner import run_exchange  # noqa: E402
from collectors.common.sinks import FixtureSink  # noqa: E402

FIXTURES = ROOT / "tests" / "fixtures"


async def probe(seconds: float, exchanges: list[str], sink: FixtureSink) -> None:
    cfg = load_config()
    stop = asyncio.Event()
    tasks = [asyncio.create_task(run_exchange(x, HANDLERS[x], sink, cfg, None, stop)) for x in exchanges]
    await asyncio.sleep(seconds)
    stop.set()
    await asyncio.gather(*tasks)


def summarize(sink: FixtureSink, seconds: float, exchanges: list[str]) -> dict:
    streams = {}
    for s in all_streams(load_config()):
        if s.exchange not in exchanges:
            continue
        events = sink.events.get(s.key, [])
        msgs = sink.total.get(s.key, 0)
        bps = sink.bytes.get(s.key, 0) / seconds
        initial = sink.initial_snapshot_bytes.get(s.key, 0)
        steady_bps = max(0, (sink.bytes.get(s.key, 0) - initial) / seconds)
        path = sink.root / s.exchange / s.market / f"{s.feed}.jsonl"
        sample, initial_gzip = [], 0
        if path.exists():
            for line in path.read_bytes().splitlines(keepends=True):
                record = orjson.loads(line)
                if record["symbol"] != s.symbol or "payload" not in record:
                    continue
                if initial and not initial_gzip:
                    initial_gzip = len(gzip.compress(line))
                else:
                    sample.append(line)
        sample = b"".join(sample)
        ratio = len(gzip.compress(sample)) / len(sample) if sample else None
        streams[s.key] = {
            "exchange": s.exchange,
            "market": s.market,
            "feed": s.feed,
            "symbol": s.symbol,
            "channel": s.channel,
            "started": any(e == "start" for e, _ in events),
            "msgs": msgs,
            "msgs_per_s": round(msgs / seconds, 2),
            "bytes_per_s": round(bps),
            "est_gib_per_day": round(bps * 86400 / 2**30, 3),
            "drops": [r for e, r in events if e == "drop"],
            "initial_snapshot_bytes": initial,
            "steady_msgs_per_s": round((msgs - bool(initial)) / seconds, 2),
            "steady_bytes_per_s": round(steady_bps, 2),
            "daily_gb_with_rotation": round((steady_bps * 86400 + initial) / 1e9, 6),
            "est_gzip_ratio": round(ratio, 4) if ratio is not None else None,
            "est_gzip_daily_gb_with_rotation": round((steady_bps * ratio * 86400 + initial_gzip) / 1e9, 6)
                if ratio is not None else None,
        }
    return {
        "probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "seconds": seconds,
        "streams_started": sum(v["started"] for v in streams.values()),
        "streams_total": len(streams),
        "steady_state_method": "Exclude initial snapshot bytes, add one original snapshot per stream for daily rotation; gzip estimated from retained subsequent envelopes. Sampling window includes connection startup.",
        "daily_gb_with_rotation_total": round(sum(v["daily_gb_with_rotation"] for v in streams.values()), 6),
        "est_gzip_daily_gb_with_rotation_total": round(sum(v["est_gzip_daily_gb_with_rotation"] or 0 for v in streams.values()), 6),
        "est_gib_per_day_total": round(sum(v["est_gib_per_day"] for v in streams.values()), 2),
        "streams": streams,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--exchange", action="append", choices=sorted(HANDLERS), help="default: all")
    parser.add_argument("--limit", type=int, default=300, help="records kept per stream in fixtures")
    parser.add_argument("--fixtures", type=Path, default=FIXTURES, help="fixture output root (existing files are never deleted)")
    parser.add_argument("--summary", type=Path, default=FIXTURES / "probe-summary.json", help="summary output path")
    args = parser.parse_args()
    exchanges = args.exchange or sorted(HANDLERS)
    setup_logging()

    for x in exchanges:
        if any((args.fixtures / x).rglob("*.jsonl")):
            parser.error(f"fixtures already exist for {x}; refusing to overwrite them")
    sink = FixtureSink(args.fixtures, limit=args.limit)
    started = time.monotonic()
    asyncio.run(probe(args.seconds, exchanges, sink))
    sink.close()
    summary = summarize(sink, time.monotonic() - started, exchanges)
    args.summary.write_bytes(orjson.dumps(summary, option=orjson.OPT_INDENT_2))

    print(f"\n{'stream':38} {'started':7} {'msg/s':>8} {'KiB/s':>8} {'GB/day':>8} {'gzip GB/d':>10}  drops")
    for v in summary["streams"].values():
        name = f"{v['exchange']}.{v['market']}.{v['feed']}.{v['symbol']}"
        print(
            f"{name:38} {'yes' if v['started'] else 'NO':7} {v['steady_msgs_per_s']:8.1f} "
            f"{v['steady_bytes_per_s'] / 1024:8.1f} {v['daily_gb_with_rotation']:8.2f} {v['est_gzip_daily_gb_with_rotation'] or 0:10.2f}  {','.join(v['drops'])}"
        )
    print(
        f"\nstarted {summary['streams_started']}/{summary['streams_total']} streams; "
        f"estimated {summary['daily_gb_with_rotation_total']:.2f} GB/day uncompressed (steady traffic + daily rotation snapshot)"
    )
    sys.exit(0 if summary["streams_started"] == summary["streams_total"] else 1)


if __name__ == "__main__":
    main()
