"""Stream v1 envelopes or legacy Kraken recordings into new raw v2 files.

uv run python scripts/convert_raw.py data/raw --output-root /new/raw
uv run python scripts/convert_raw.py data/Test/BTC-USD --legacy --exchange kraken --market spot --output-root /new/raw

Outputs are exclusive-create, including metadata; originals are never changed.
Legacy offsets are synthetic: the zero-based physical input line index, per file.
.gz inputs produce streamed .gz outputs. .open stays .open; an incomplete tail is skipped.
"""

import argparse
import gzip
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from archiver import rawfmt  # noqa: E402
from archiver.record_raw import write_meta  # noqa: E402

FEEDS = {"book": "l2", "level3": "l3", "trades": "trades"}


def inputs(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.jsonl*") if p.name.endswith((".jsonl", ".jsonl.open", ".jsonl.gz")))


def open_input(path: Path):
    return gzip.open(path, "rb") if path.suffix == ".gz" else path.open("rb")


def instrument_for(root: Path, symbol: str) -> dict | None:
    """Copy only public instrument metadata from a recorded start of the same symbol."""
    pattern = f"date=*/exchange=kraken/market=spot/feed=*/{symbol}.jsonl*"
    for path in sorted(root.glob(pattern)):
        if not path.name.endswith((".jsonl", ".jsonl.open", ".jsonl.gz")):
            continue
        with open_input(path) as f:
            for line in f:
                if not line.endswith(b"\n"):
                    break
                r = json.loads(line)
                if r.get("event") == "start":
                    instrument = r.get("detail", {}).get("instrument")
                    if instrument and all(k in instrument for k in ("pair_decimals", "lot_decimals", "tick_size")):
                        return instrument
    return None


def legacy_ns(rec: dict) -> int:
    if "recv_time_ns" in rec:
        return rec["recv_time_ns"]
    dt = datetime.fromisoformat(rec["timestamp"].replace("Z", "+00:00"))
    delta = dt - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1000


@dataclass
class LegacyState:
    label: str
    epochs: dict[str, int] = field(default_factory=dict)
    active: str | None = None
    orphan: str | None = None
    orphan_epochs: int = 0
    orphan_records: int = 0

    def carrier(self) -> str:
        if self.active is not None:
            return self.active
        if self.orphan is None:
            self.orphan_epochs += 1
            self.orphan = f"{self.label}#pre{self.orphan_epochs}"
        return self.orphan


def legacy_line(rec: dict, offset: int, state: LegacyState, channel: str, instrument: dict | None) -> bytes:
    if rec.get("marker") == "rollover":
        return rawfmt.dumps(rec) + b"\n"
    ns = legacy_ns(rec)
    if rec.get("marker"):
        event = rec["event"]
        if event == "connected":
            carrier = rec.get("conn", channel)
            state.epochs[carrier] = state.epochs.get(carrier, 0) + 1
            state.active = f"{carrier}#{state.epochs[carrier]}"
            state.orphan = None
        conn = state.active or state.orphan or f"{rec.get('conn', state.label)}#0"
        rec = dict(rec, conn=conn, recv_time_ns=ns, offset=offset)
        if event == "rotating":
            rec.update(event="disconnected", reason="rotation")
        elif event == "connection_error":
            rec["reason"] = "disconnect"
        elif event == "connected" and instrument:
            rec.update(instrument=instrument, instrument_source="copied")
        if event in ("rotating", "disconnected", "connection_error"):
            state.active = None
            state.orphan = None
        return rawfmt.dumps(rec) + b"\n"
    # Legacy data often has no conn: it belongs to the last connected marker,
    # never to the channel name or a connection which has already failed.
    conn = state.carrier()
    if state.active is None:
        state.orphan_records += 1
    payload = {k: v for k, v in rec.items() if k not in ("recv_time_ns", "conn", "offset")}
    return rawfmt.dumps(rawfmt.inject(payload, {"recv_time_ns": ns, "conn": conn, "offset": offset})) + b"\n"


def destination(source: Path, output: Path, legacy: bool, exchange: str | None, market: str | None) -> Path:
    if legacy:
        day, channel = source.name.split("_", 1)
        channel = channel.split(".", 1)[0]
        target = rawfmt.path(output, exchange, market, source.parent.name, day, FEEDS[channel])
    else:
        parts = {p.split("=", 1)[0]: p.split("=", 1)[1] for p in source.parts if "=" in p}
        target = rawfmt.path(output, parts["exchange"], parts["market"], source.name.split(".", 1)[0],
                             parts["date"], parts["feed"])
    if source.name.endswith(".open"):
        return target.with_name(target.name + ".open")
    if source.suffix == ".gz":
        return target.with_name(target.name + ".gz")
    return target


def convert(root: Path, output: Path, *, legacy=False, exchange=None, market=None,
            instrument_root: Path = Path("data/raw")) -> list[dict]:
    if legacy and (exchange != "kraken" or market != "spot"):
        raise ValueError("legacy recordings require --exchange kraken --market spot")
    sources = inputs(root)
    if not legacy:
        sources = [p for p in sources if any(x.startswith("date=") for x in p.parts)]
    jobs = [(p, destination(p, output, legacy, exchange, market)) for p in sources]
    targets = set()
    for _, target in jobs:
        # Refuse the whole run before creating anything if any target already exists.
        if target in targets or target.exists() or target.with_name(target.name + ".meta.json").exists():
            raise FileExistsError(target)
        targets.add(target)
    instruments = {}
    results = []
    for source, target in jobs:
        start = time.perf_counter()
        instrument = None
        if legacy:
            symbol = source.parent.name
            if symbol not in instruments:
                instruments[symbol] = instrument_for(instrument_root, symbol)
            instrument = instruments[symbol]
        target.parent.mkdir(parents=True, exist_ok=True)
        state = LegacyState(source.name)
        rows, partial_bytes = 0, 0
        with open_input(source) as src, (gzip.open(target, "xb", compresslevel=6) if target.suffix == ".gz" else target.open("xb")) as dst:
            for offset, raw in enumerate(src):
                if not raw.endswith(b"\n"):
                    if not source.name.endswith(".open"):
                        raise ValueError(f"incomplete line: {source}")
                    partial_bytes = len(raw)
                    break
                rec = rawfmt.loads(raw.decode()) if legacy else orjson.loads(raw)
                out = legacy_line(rec, offset, state, source.name.split("_", 1)[1].split(".")[0], instrument) if legacy else rawfmt.line(rec, rec["offset"])
                dst.write(out)
                rows += 1
            if not legacy and not source.name.endswith(".open"):
                day = next(p.removeprefix("date=") for p in source.parts if p.startswith("date="))
                tomorrow = (datetime.fromisoformat(day) + timedelta(days=1)).strftime("%Y%m%d")
                dst.write(rawfmt.rollover(tomorrow))
                rows += 1
        if not target.name.endswith(".open"):
            write_meta(target, extra={"synthetic_offsets": legacy,
                                   **({"orphan_records": state.orphan_records, "orphan_epochs": state.orphan_epochs} if legacy else {}),
                                   **({"offset_source": "zero-based source line index"} if legacy else {})}, exclusive=True)
        info = dict(source=str(source), output=str(target), rows=rows, bytes_in=source.stat().st_size,
                    bytes_out=target.stat().st_size, partial_bytes=partial_bytes,
                    seconds=round(time.perf_counter() - start, 3),
                    **({"orphan_records": state.orphan_records} if legacy else {}))
        results.append(info)
        print(json.dumps(info), flush=True)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input_root", type=Path)
    parser.add_argument("--output-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--legacy", action="store_true")
    parser.add_argument("--exchange")
    parser.add_argument("--market")
    parser.add_argument("--instrument-root", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    convert(args.input_root, args.output_root, legacy=args.legacy, exchange=args.exchange,
            market=args.market, instrument_root=args.instrument_root)


if __name__ == "__main__":
    main()
