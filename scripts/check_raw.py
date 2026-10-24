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
import gzip
import heapq
import json
import os
import sys
from collections import Counter, deque
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from archiver import rawfmt  # noqa: E402
from archiver.record_raw import OPEN  # noqa: E402
from collectors.binance import PerpDepthSync, SpotDepthSync  # noqa: E402
from collectors.bybit import BookSeq as BybitBookSeq  # noqa: E402
from collectors.common.config import Stream, all_streams, connections, data_dir, load_config  # noqa: E402
from collectors.kraken import L2Book, L3Book, L3TimestampError, _decimal_json  # noqa: E402
from collectors.okx import BookSeq  # noqa: E402


def trade_ids(stream: Stream, payload: dict) -> list:
    if stream.exchange == "binance":
        data = payload.get("data", payload)
        return [data["a"] if stream.feed == "trades_agg" else data["t"]]
    if stream.exchange == "okx":
        return [t["tradeId"] for t in payload["data"]]
    if stream.exchange == "coinbase":
        return [t["trade_id"] for event in payload["events"] for t in event.get("trades", [])
                if t["product_id"] == stream.symbol]
    if stream.exchange == "bybit":
        return [t["i"] for t in payload["data"] if t["s"] == stream.symbol]
    if stream.market == "perp":  # kraken futures
        return [t["uid"] for t in payload.get("trades", [payload])]
    return [t["trade_id"] for t in payload["data"]]


class BookCheck:
    """Re-validates one connection's book stream; returns an error string or None per record."""

    def __init__(self, stream: Stream, depth: int | None):
        self.stream = stream
        self.depth = depth
        self.state = None
        self.crc = None
        self.synced = False

    def feed(self, kind: str, payload_text: str | dict) -> str | None:
        s = self.stream
        self.crc = None
        if s.exchange == "binance":
            if self.state is None:
                self.state = PerpDepthSync() if s.market == "perp" else SpotDepthSync()
            p = payload_text if isinstance(payload_text, dict) else orjson.loads(payload_text)
            if kind == "snapshot":
                if self.synced:
                    return None
                self.synced = self.state.on_snapshot(p["lastUpdateId"]) == "synced"
                return None
            d = p.get("data", p)
            r = self.state.on_delta(d["U"], d["u"], d["pu"]) if s.market == "perp" else self.state.on_delta(d["U"], d["u"])
            return f"update-id break U={d['U']} u={d['u']}" if r in ("gap", "stale") else None
        if s.exchange == "okx":
            book = (payload_text if isinstance(payload_text, dict) else orjson.loads(payload_text))["data"][0]
            if kind == "snapshot":
                self.state = BookSeq()
                self.state.on_snapshot(book["seqId"])
            elif self.state is not None and not self.state.on_update(book["prevSeqId"], book["seqId"]):
                return f"seqId break prev={book['prevSeqId']} last={self.state.last}"
            return None
        if s.exchange == "coinbase":
            p = payload_text if isinstance(payload_text, dict) else orjson.loads(payload_text)
            seq = p["sequence_num"]
            if self.state is not None and seq <= self.state:
                return f"sequence_num regression {self.state} -> {seq}"
            self.state = seq
            # Other products/channels, subscriptions and heartbeats share this sequence.
            # Per-file gaps are expected; adjacency is checked on every frame by the collector.
            for event in p["events"]:
                if event.get("product_id") != s.symbol:
                    continue
                if event["type"] == "snapshot":
                    self.synced = True
                elif not self.synced:
                    return "book update before first snapshot"
            return None
        if s.exchange == "bybit":
            p = payload_text if isinstance(payload_text, dict) else orjson.loads(payload_text)
            book = p["data"]
            if self.state is None:
                self.state = BybitBookSeq()
            previous = self.state.last_u, self.state.last_seq
            if s.feed == "l1":
                valid = p["type"] == "snapshot" and self.state.on_l1(book["u"], book["seq"])
            elif p["type"] == "snapshot":
                self.state.on_snapshot(book["u"], book["seq"])
                return None
            else:
                valid = self.state.on_update(book["u"], book["seq"])
            return None if valid else f"update-id break u/seq={previous} -> {(book['u'], book['seq'])}"
        if s.market == "perp":  # kraken futures
            seq = (payload_text if isinstance(payload_text, dict) else orjson.loads(payload_text))["seq"]
            if kind != "snapshot" and self.state is not None and seq != self.state + 1:
                err = f"seq break {self.state} -> {seq}"
                self.state = seq
                return err
            self.state = seq
            return None
        data = (payload_text if isinstance(payload_text, dict) else _decimal_json(payload_text))["data"][0]  # kraken spot l2/l3
        if kind == "snapshot":
            depth = self.depth
            if depth is None:
                # Archive snapshots carry their subscribed depth, including L3
                # price levels (not order count). No config pair lookup needed.
                key = "limit_price" if s.feed == "l3" else "price"
                depth = max(len({x[key] for x in data.get(side, [])}) for side in ("bids", "asks"))
            self.state = L3Book(depth) if s.feed == "l3" else L2Book(depth)
        elif self.state is None:
            return None
        try:
            self.state.apply(data, snapshot=kind == "snapshot")
        except L3TimestampError:
            self.crc = False
            return "book anomaly: invalid level3 timestamp"
        self.crc = self.state.checksum() == int(data["checksum"])
        return None if self.crc else ("checksum fails on snapshot" if kind == "snapshot" else "checksum mismatch")


def files_for(root: Path, stream: Stream, dates: list[str] | None, *, raw_root=False) -> list[Path]:
    """Discover v3 (or original v1) layout; root normally denotes data/, or a raw tree explicitly."""
    tree = root if raw_root else root / "raw"
    channels = rawfmt.file_channels(stream.exchange, stream.market, stream.feed)
    paths = [p for p in rawfmt.archive_files(tree)
             if rawfmt.archive_identity(p)[:3] == (stream.exchange, stream.market, stream.file_symbol)
             and rawfmt.archive_identity(p)[3] in channels]
    old = f"date=*/exchange={stream.exchange}/market={stream.market}/feed={stream.feed}/{stream.file_symbol}.jsonl"
    def day(p):
        return next((x[5:].replace("-", "") for x in p.parts if x.startswith("date=")), p.name[:8])
    v2_days = {day(p) for p in paths}
    # Converted v2 days are authoritative; v1 files are pre-conversion copies.
    paths.extend(p for suffix in ("", OPEN, ".gz") for p in tree.glob(old + suffix) if day(p) not in v2_days)
    if dates:
        paths = [p for p in paths if any(f"date={d}" in p.parts or p.name.startswith(d.replace("-", "") + "_") for d in dates)]
    return sorted(paths, key=day)


def disk_record(rec: dict, stream: Stream) -> dict:
    """Normalize only the checker header. Native v3 data stays as parsed string decimals."""
    if "payload" in rec or rec.get("kind") == "control":
        return rec
    conn, offset = rec["conn"], rec["offset"]
    if rec.get("marker") == "ws_event":
        event = {"connected": "start", "disconnected": "stop", "connection_error": "drop"}.get(rec["event"], rec["event"])
        return dict(kind="control", event=event, conn=conn, offset=offset, reason=rec.get("reason"))
    if "raw" in rec:
        try:
            payload = rawfmt.loads(rec["raw"])
        except ValueError:
            payload = None
    elif stream.exchange != "kraken" or stream.market == "perp":
        payload = rec.get("data", rec)
    else:
        payload = rec
    if not isinstance(payload, dict):
        return dict(kind="raw", payload=None, conn=conn, offset=offset)
    snapshot = (rec.get("channel") == "snapshot" or payload.get("type") == "snapshot"
                or payload.get("action") == "snapshot" or payload.get("feed") in ("book_snapshot", "trade_snapshot"))
    if stream.exchange == "coinbase":
        snapshot = any(event.get("type") == "snapshot" and (
            event.get("product_id") == stream.symbol or any(t["product_id"] == stream.symbol for t in event.get("trades", [])))
            for event in payload.get("events", []))
    if stream.exchange == "bybit" and stream.feed == "trades":
        snapshot = False  # publicTrade calls every realtime batch a snapshot
    return dict(kind="snapshot" if snapshot else "update", payload=payload, conn=conn, offset=offset,
                event=rec.get("event"))


def stream_records(path, stats):
    headers = rawfmt.Headers(path)
    meta_path = path.with_name(path.name + ".meta.json")
    synthetic_file = meta_path.exists() and orjson.loads(meta_path.read_bytes()).get("synthetic_offsets", False)
    with (gzip.open(path, "rb") if path.suffix == ".gz" else open(path, "rb")) as f:
        for index, raw in enumerate(f):
            if not raw.endswith(b"\n"):
                continue
            stats["lines"] += 1
            rec = rawfmt.loads(raw.decode())
            if rec.get("marker") == "rollover":
                continue
            synthetic = "offset" not in rec or synthetic_file
            rec = headers.read(rec, index)
            # Real Kafka offsets merge split files; archive-only inputs merge by
            # receive time, keeping each file's synthetic offset independent.
            order = rec.get("recv_time_ns", index) if synthetic else rec["offset"]
            yield order, str(path), index, rec, synthetic, stats


def check_stream(stream: Stream, paths: list[Path], depth: int | None, deep: bool) -> dict:
    r = {"records": 0, "files": len(paths), "conns": set(), "events": Counter(), "errors": [], "overlap_dup_trades": 0,
         "crc_pass": 0, "crc_fail": 0, "duplicate_trades": 0, "trade_id_gaps": 0, "file_stats": []}
    last_offset = None
    books: dict[str, BookCheck] = {}
    started: set[str] = set()
    broken: set[str] = set()
    pending: dict[str, str] = {}
    seen_trades: dict[str, set] = {}
    recent: deque[str] = deque()
    last_trade: dict[str, int] = {}
    inputs = []
    for path in paths:
        stats = dict(file=str(path), lines=0, crc_pass=0, crc_fail=0)
        r["file_stats"].append(stats)
        inputs.append(stream_records(path, stats))
    binance_check = None
    for _, path, _, rec, synthetic, stats in heapq.merge(*inputs, key=lambda row: row[:3]):
        rec = disk_record(rec, stream)
        r["records"] += 1
        off = rec["offset"]
        if not synthetic and last_offset is not None and off != last_offset + 1:
            r["errors"].append(f"offset hole {last_offset} -> {off} ({Path(path).name})")
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
            if ev in ("drop", "stop", "rotating", "shutdown") and not (ev == "drop" and rec.get("reason") == "collector_crash"):
                broken.add(conn)
                started.discard(conn)
                books.pop(conn, None)
                if conn in recent:
                    recent.remove(conn)
                recent.append(conn)
                if len(recent) > 2:
                    seen_trades.pop(recent.popleft(), None)
            elif ev == "start":
                broken.discard(conn)
                books.pop(conn, None)
                started.add(conn)
            if stream.exchange == "binance" and ev in ("start", "drop", "stop", "rotating", "shutdown"):
                binance_check = None
            continue
        if rec.get("event") == "periodic" and rec["kind"] == "snapshot":
            r["events"]["periodic_snapshot"] += 1
        if not deep or rec["kind"] == "raw":
            continue
        if (stream.is_book or stream.exchange == "bybit" and stream.feed == "l1") and conn not in broken:
            check = binance_check if stream.exchange == "binance" else books.get(conn)
            if check is None:
                check = books[conn] = BookCheck(stream, depth)
                if stream.exchange == "binance":
                    binance_check = check
            if conn in started and check.state is None and rec["kind"] != "snapshot" and stream.exchange != "binance":
                err = "book update before first snapshot"
            else:
                err = check.feed(rec["kind"], rec["payload"])
                if check.crc is not None:
                    field = "crc_pass" if check.crc else "crc_fail"
                    r[field] += 1
                    stats[field] += 1
            if err:
                pending[conn] = f"{err} (conn {conn}, offset {off}) not marked by a drop"
                broken.add(conn)
        if stream.feed in ("trades", "trades_agg"):
            seen = seen_trades.setdefault(conn, set())
            for tid in trade_ids(stream, rec["payload"] if isinstance(rec["payload"], dict) else orjson.loads(rec["payload"])):
                if tid in seen and rec["kind"] != "snapshot":
                    r["duplicate_trades"] += 1
                    r["errors"].append(f"duplicate trade id {tid} within conn {conn}")
                if any(tid in ids for other, ids in seen_trades.items() if other != conn):
                    r["overlap_dup_trades"] += 1
                if isinstance(tid, int) and rec["kind"] != "snapshot":
                    previous = last_trade.get(conn)
                    if previous is not None and tid > previous + 1:
                        r["trade_id_gaps"] += tid - previous - 1
                    last_trade[conn] = max(tid, previous if previous is not None else tid)
                seen.add(tid)
    r["errors"].extend(pending.values())
    r["conns"] = len(r["conns"])
    return r


def discovered_streams(tree):
    streams = {}
    for p in rawfmt.archive_files(tree):
        exchange, market, pair, file = rawfmt.archive_identity(p)
        feed = next((f for f, (c, _) in rawfmt.CHANNELS[exchange].items() if c == file), None)
        if feed == "snapshot":
            feed = "l2"
        if feed is None:
            continue
        symbol = pair.replace("-", "/") if exchange == "kraken" and market == "spot" else pair
        s = Stream(exchange, market, feed, pair, symbol, file, f"raw.{exchange}.{market}.{feed}.v1", 0)
        streams[exchange, market, pair, feed] = s
    return list(streams.values())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", action="append", help="UTC date(s) to check; default all")
    parser.add_argument("--quick", action="store_true", help="offsets and control events only")
    parser.add_argument("--root", type=Path, help="data directory or raw tree (v3)")
    parser.add_argument("--present-only", action="store_true", help="check existing streams only")
    parser.add_argument("--json", action="store_true", help="machine-readable per-stream and per-file metrics")
    args = parser.parse_args()

    cfg = load_config()
    root = args.root or data_dir()
    is_raw_root = bool(args.root and not (root / "raw").exists())
    auth_streams = {
        s.key for exchange in cfg["exchanges"] for spec in connections(cfg, exchange)
        if spec.options.get("auth") for s in spec.streams
    }
    total_errors = 0
    if not args.json:
        print(f"{'stream':38} {'files':>5} {'records':>9} {'conns':>5}  events / errors")
    tree = root if is_raw_root else root / "raw"
    streams = discovered_streams(tree) if args.present_only or is_raw_root else all_streams(cfg)
    for stream in streams:
        name = f"{stream.exchange}.{stream.market}.{stream.feed}.{stream.symbol}"
        paths = files_for(root, stream, args.date, raw_root=is_raw_root)
        if not paths:
            if args.present_only:
                continue
            if stream.key in auth_streams and "KRAKEN_API_KEY" not in os.environ:
                print(f"{name:38} skipped (no API key)")
            else:
                print(f"{name:38} MISSING")
                total_errors += 1
            continue
        depth = None if is_raw_root else cfg["exchanges"][stream.exchange][stream.market].get("depth", {}).get(stream.feed)
        r = check_stream(stream, paths, depth, deep=not args.quick)
        if args.json:
            print(json.dumps(dict(stream=name, **r)), flush=True)
            total_errors += len(r["errors"])
            continue
        events = " ".join(f"{k}={v}" for k, v in sorted(r["events"].items()))
        events += f" crc_pass={r['crc_pass']} crc_fail={r['crc_fail']} trade_duplicates={r['duplicate_trades']} trade_id_gaps={r['trade_id_gaps']}"
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
    if not args.json:
        print(f"\n{total_errors} errors")
    sys.exit(1 if total_errors else 0)


if __name__ == "__main__":
    main()
