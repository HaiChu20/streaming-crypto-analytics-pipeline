"""Data-quality report for recorded raw files (plan.md §11, phase 1).

    uv run python scripts/check_raw.py                    # all recorded dates
    uv run python scripts/check_raw.py --date 2026-10-17

Per stream it checks:
  - Kafka offsets are contiguous across files (a hole = lost data)
  - control events: start / stop / drop (by reason); collector_crash = uncertain interval
  - book continuity per connection, re-run from the raw payloads with the collectors' own rules
    (Binance update IDs, OKX seqId, Kraken spot CRC32, Kraken futures seq); unexplained breaks are errors
  - duplicate trade IDs inside one connection (error) vs across overlapping connections (expected)
Exits 1 if any error is found.
"""

import argparse
import os
import sys
from collections import Counter, deque
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from archiver.record_raw import OPEN, stream_path  # noqa: E402
from collectors.binance import PerpDepthSync, SpotDepthSync  # noqa: E402
from collectors.common.config import Stream, all_streams, connections, data_dir, load_config  # noqa: E402
from collectors.kraken import L2Book, L3Book, _decimal_json  # noqa: E402
from collectors.okx import BookSeq  # noqa: E402


def trade_ids(stream: Stream, payload: dict) -> list:
    if stream.exchange == "binance":
        data = payload["data"]
        return [data["a"] if stream.feed == "trades_agg" else data["t"]]
    if stream.exchange == "okx":
        return [t["tradeId"] for t in payload["data"]]
    if stream.market == "perp":  # kraken futures
        return [t["uid"] for t in payload.get("trades", [payload])]
    return [t["trade_id"] for t in payload["data"]]


class BookCheck:
    """Re-validates one connection's book stream; returns an error string or None per record."""

    def __init__(self, stream: Stream, depth: int | None):
        self.stream = stream
        self.depth = depth
        self.state = None

    def feed(self, kind: str, payload_text: str) -> str | None:
        s = self.stream
        if s.exchange == "binance":
            if self.state is None:
                self.state = PerpDepthSync() if s.market == "perp" else SpotDepthSync()
            p = orjson.loads(payload_text)
            if kind == "snapshot":
                self.state.on_snapshot(p["lastUpdateId"])
                return None
            d = p["data"]
            r = self.state.on_delta(d["U"], d["u"], d["pu"]) if s.market == "perp" else self.state.on_delta(d["U"], d["u"])
            return f"update-id break U={d['U']} u={d['u']}" if r in ("gap", "stale") else None
        if s.exchange == "okx":
            book = orjson.loads(payload_text)["data"][0]
            if kind == "snapshot":
                self.state = BookSeq()
                self.state.on_snapshot(book["seqId"])
            elif self.state is not None and not self.state.on_update(book["prevSeqId"], book["seqId"]):
                return f"seqId break prev={book['prevSeqId']} last={self.state.last}"
            return None
        if s.market == "perp":  # kraken futures
            seq = orjson.loads(payload_text)["seq"]
            if kind != "snapshot" and self.state is not None and seq != self.state + 1:
                err = f"seq break {self.state} -> {seq}"
                self.state = seq
                return err
            self.state = seq
            return None
        data = _decimal_json(payload_text)["data"][0]  # kraken spot l2/l3
        if kind == "snapshot":
            self.state = L3Book(self.depth) if s.feed == "l3" else L2Book(self.depth)
            self.state.apply(data, snapshot=True)
            return None if self.state.checksum() == int(data["checksum"]) else "checksum fails on snapshot"
        if self.state is None:
            return None
        self.state.apply(data, snapshot=False)
        if self.state.checksum() != int(data["checksum"]):
            return "checksum mismatch"
        return None


def files_for(root: Path, stream: Stream, dates: list[str] | None) -> list[Path]:
    pattern = str(stream_path(root, stream, "*").relative_to(root))
    paths = list(root.glob(pattern)) + list(root.glob(pattern + OPEN))
    if dates:
        paths = [p for p in paths if any(f"date={d}" in p.parts for d in dates)]
    return sorted(paths, key=lambda p: next(x for x in p.parts if x.startswith("date=")))


def check_stream(stream: Stream, paths: list[Path], depth: int | None, deep: bool) -> dict:
    r = {"records": 0, "files": len(paths), "conns": set(), "events": Counter(), "errors": [], "overlap_dup_trades": 0}
    last_offset = None
    books: dict[str, BookCheck] = {}
    started: set[str] = set()
    broken: set[str] = set()
    pending: dict[str, str] = {}
    seen_trades: dict[str, set] = {}
    recent: deque[str] = deque()
    for path in paths:
        with open(path, "rb") as f:
            for raw in f:
                if not raw.endswith(b"\n"):
                    continue  # partial last line of a file being written
                rec = orjson.loads(raw)
                r["records"] += 1
                off = rec["offset"]
                if last_offset is not None and off != last_offset + 1:
                    r["errors"].append(f"offset hole {last_offset} -> {off} ({path.name})")
                last_offset = off
                conn = rec["conn"]
                r["conns"].add(conn)
                if conn in pending:
                    err = pending.pop(conn)
                    if rec["kind"] == "control" and rec["event"] == "drop":
                        r["events"]["explained_breaks"] += 1
                    else:
                        r["errors"].append(err)
                if rec["kind"] == "control":
                    ev = rec["event"]
                    r["events"][f"{ev}:{rec.get('reason')}" if ev == "drop" else ev] += 1
                    # Startup crash drops describe the previous session, not their new carrier.
                    if ev in ("drop", "stop") and not (ev == "drop" and rec.get("reason") == "collector_crash"):
                        broken.add(conn)
                        started.discard(conn)
                        books.pop(conn, None)
                        if conn in recent:
                            recent.remove(conn)
                        recent.append(conn)
                        if len(recent) > 2:
                            seen_trades.pop(recent.popleft(), None)
                    elif ev == "start":
                        started.add(conn)
                    continue
                if not deep:
                    continue
                if stream.is_book and conn not in broken:
                    check = books.get(conn)
                    if check is None:
                        check = books[conn] = BookCheck(stream, depth)
                    if conn in started and check.state is None and rec["kind"] != "snapshot" and stream.exchange != "binance":
                        err = "book update before first snapshot"
                    else:
                        err = check.feed(rec["kind"], rec["payload"])
                    if err:
                        pending[conn] = f"{err} (conn {conn}, offset {off}) not marked by a drop"
                        broken.add(conn)
                if stream.feed in ("trades", "trades_agg"):
                    seen = seen_trades.setdefault(conn, set())
                    for tid in trade_ids(stream, orjson.loads(rec["payload"])):
                        if tid in seen and rec["kind"] != "snapshot":
                            r["errors"].append(f"duplicate trade id {tid} within conn {conn}")
                        if any(tid in ids for other, ids in seen_trades.items() if other != conn):
                            r["overlap_dup_trades"] += 1
                        seen.add(tid)
    r["errors"].extend(pending.values())
    r["conns"] = len(r["conns"])
    return r


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", action="append", help="UTC date(s) to check; default all")
    parser.add_argument("--quick", action="store_true", help="offsets and control events only")
    args = parser.parse_args()

    cfg = load_config()
    root = data_dir()
    auth_streams = {
        s.key for exchange in cfg["exchanges"] for spec in connections(cfg, exchange)
        if spec.options.get("auth") for s in spec.streams
    }
    total_errors = 0
    print(f"{'stream':38} {'files':>5} {'records':>9} {'conns':>5}  events / errors")
    for stream in all_streams(cfg):
        name = f"{stream.exchange}.{stream.market}.{stream.feed}.{stream.symbol}"
        paths = files_for(root, stream, args.date)
        if not paths:
            if stream.key in auth_streams and "KRAKEN_API_KEY" not in os.environ:
                print(f"{name:38} skipped (no API key)")
            else:
                print(f"{name:38} MISSING")
                total_errors += 1
            continue
        depth = cfg["exchanges"][stream.exchange][stream.market].get("depth", {}).get(stream.feed)
        r = check_stream(stream, paths, depth, deep=not args.quick)
        events = " ".join(f"{k}={v}" for k, v in sorted(r["events"].items()))
        if r["overlap_dup_trades"]:
            events += f" overlap_dup_trades={r['overlap_dup_trades']}"
        if any(k.startswith("drop:collector_crash") for k in r["events"]):
            events += " UNCERTAIN(collector crash)"
        print(f"{name:38} {r['files']:5} {r['records']:9} {r['conns']:5}  {events}")
        for err in r["errors"][:5]:
            print(f"    ERROR {err}")
        if len(r["errors"]) > 5:
            print(f"    ... {len(r['errors']) - 5} more errors")
        total_errors += len(r["errors"])
    print(f"\n{total_errors} errors")
    sys.exit(1 if total_errors else 0)


if __name__ == "__main__":
    main()
