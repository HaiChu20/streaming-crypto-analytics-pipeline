"""Subscribe to every configured feed for a while, without Kafka.

Saves the first records of each stream to tests/fixtures/<exchange>/<market>/<feed>.jsonl and a
summary (acks, rates, estimated GB/day, drops) to tests/fixtures/probe-summary.json.

    uv run python scripts/probe_feeds.py --seconds 60
"""

import argparse
import asyncio
import shutil
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
        }
    return {
        "probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "seconds": seconds,
        "streams_started": sum(v["started"] for v in streams.values()),
        "streams_total": len(streams),
        "est_gib_per_day_total": round(sum(v["est_gib_per_day"] for v in streams.values()), 2),
        "streams": streams,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--exchange", action="append", choices=sorted(HANDLERS), help="default: all")
    parser.add_argument("--limit", type=int, default=300, help="records kept per stream in fixtures")
    args = parser.parse_args()
    exchanges = args.exchange or sorted(HANDLERS)
    setup_logging()

    for x in exchanges:
        shutil.rmtree(FIXTURES / x, ignore_errors=True)
    sink = FixtureSink(FIXTURES, limit=args.limit)
    started = time.monotonic()
    asyncio.run(probe(args.seconds, exchanges, sink))
    sink.close()
    summary = summarize(sink, time.monotonic() - started, exchanges)
    (FIXTURES / "probe-summary.json").write_bytes(orjson.dumps(summary, option=orjson.OPT_INDENT_2))

    print(f"\n{'stream':38} {'started':7} {'msg/s':>8} {'KiB/s':>8} {'GiB/day':>8}  drops")
    for v in summary["streams"].values():
        name = f"{v['exchange']}.{v['market']}.{v['feed']}.{v['symbol']}"
        print(
            f"{name:38} {'yes' if v['started'] else 'NO':7} {v['msgs_per_s']:8.1f} "
            f"{v['bytes_per_s'] / 1024:8.1f} {v['est_gib_per_day']:8.2f}  {','.join(v['drops'])}"
        )
    print(
        f"\nstarted {summary['streams_started']}/{summary['streams_total']} streams; "
        f"estimated {summary['est_gib_per_day_total']} GiB/day uncompressed"
    )
    sys.exit(0 if summary["streams_started"] == summary["streams_total"] else 1)


if __name__ == "__main__":
    main()
