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
import sys
from collections import Counter, defaultdict
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from archiver.record_raw import OPEN, stream_path  # noqa: E402
from collectors.binance import PerpDepthSync, SpotDepthSync  # noqa: E402
from collectors.common.config import Stream, all_streams, data_dir, load_config  # noqa: E402
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
        self.verify = True

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
            self.verify = self.state.checksum() == int(data["checksum"])
            return None if self.verify else "checksum fails on snapshot (verification off)"
        if self.state is None:
            return None
        self.state.apply(data, snapshot=False)
        if self.verify and self.state.checksum() != int(data["checksum"]):
            self.verify = False
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
    broken: set[str] = set()
    seen_trades: dict[object, str] = {}
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
                if rec["kind"] == "control":
                    ev = rec["event"]
                    r["events"][f"{ev}:{rec.get('reason')}" if ev == "drop" else ev] += 1
                    if ev in ("drop", "stop"):
                        broken.add(conn)
                    continue
                if not deep:
                    continue
                if stream.is_book and conn not in broken:
                    check = books.setdefault(conn, BookCheck(stream, depth))
                    err = check.feed(rec["kind"], rec["payload"])
                    if err:
                        r["errors"].append(f"{err} (conn {conn}, offset {off}) not marked by a drop")
                        broken.add(conn)
                if stream.feed in ("trades", "trades_agg"):
                    for tid in trade_ids(stream, orjson.loads(rec["payload"])):
                        prev = seen_trades.get(tid)
                        if prev is None:
                            seen_trades[tid] = conn
                        elif prev == conn and rec["kind"] != "snapshot":
                            r["errors"].append(f"duplicate trade id {tid} within conn {conn}")
                        else:
                            r["overlap_dup_trades"] += 1
    r["conns"] = len(r["conns"])
    return r


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", action="append", help="UTC date(s) to check; default all")
    parser.add_argument("--quick", action="store_true", help="offsets and control events only")
    args = parser.parse_args()

    cfg = load_config()
    root = data_dir()
    total_errors = 0
    print(f"{'stream':38} {'files':>5} {'records':>9} {'conns':>5}  events / errors")
    for stream in all_streams(cfg):
        paths = files_for(root, stream, args.date)
        if not paths:
            print(f"{stream.exchange}.{stream.market}.{stream.feed}.{stream.symbol:<20} no files")
            continue
        depth = cfg["exchanges"][stream.exchange][stream.market].get("depth", {}).get(stream.feed)
        r = check_stream(stream, paths, depth, deep=not args.quick)
        name = f"{stream.exchange}.{stream.market}.{stream.feed}.{stream.symbol}"
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
