"""Raw disk format v3. Kafka envelopes remain v1; decimals become exact token strings."""

import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import orjson

log = logging.getLogger(__name__)
collisions = Counter()
EVENTS = {"start": "connected", "stop": "disconnected", "drop": "connection_error", "gap": "gap"}


def _invalid_constant(token):
    raise ValueError(f"not a JSON number: {token}")


# Reuse the decoder: json.loads(..., parse_float=str) constructs one per message.
_DECODER = json.JSONDecoder(parse_float=str, parse_constant=_invalid_constant)


def loads(text: str):
    return _DECODER.decode(text)


def dumps(value) -> bytes:
    try:
        return orjson.dumps(value)
    except TypeError:  # JSON integers outside orjson's 64-bit range
        return json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode()


def timestamp(ns: int) -> str:
    seconds, nanos = divmod(ns, 1_000_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}+00:00"


# Single source for file channel / wrapper line name. Kraken Futures line names
# additionally distinguish snapshots using the native feed.
CHANNELS = {
    "binance": {"l2": ("depth", "depth"), "snapshot": ("snapshot", "snapshot"),
                "l1": ("bookticker", "bookTicker"), "trades": ("trades", "trade"),
                "trades_agg": ("aggtrades", "aggTrade"), "mark": ("markprice", "markPrice"),
                "liquidations": ("forceorder", "forceOrder")},
    "okx": {"l2": ("book", "book"), "l1": ("bbo", "bbo"),
            "trades_agg": ("trades", "trades"), "trades": ("tradesall", "tradesAll")},
    "kraken": {"l2": ("book", "book"), "l3": ("level3", "level3"),
               "trades": ("trades", "trade"), "ticker": ("ticker", "ticker")},
}


def channel(exchange, market, feed, kind=None, native=None):
    key = "snapshot" if exchange == "binance" and feed == "l2" and (
        kind == "snapshot" or isinstance(native, dict) and "lastUpdateId" in native) else feed
    file, name = CHANNELS.get(exchange, {}).get(key, (key, key))
    if exchange == "kraken" and market == "perp":
        if isinstance(native, dict) and "feed" in native:
            name = native["feed"]
        elif kind == "snapshot":
            name += "_snapshot"
    return file, name


def file_channels(exchange, market, feed):
    first = channel(exchange, market, feed)[0]
    return (first, "snapshot") if exchange == "binance" and feed == "l2" else (first,)


def path(root: Path, exchange: str, market: str, symbol: str, day: str, feed: str,
         kind=None, native=None) -> Path:
    """root is the raw tree (usually data/raw), day accepts ISO or YYYYMMDD."""
    day = day.replace("-", "")
    if exchange == "control":
        return root / "control" / f"{day}_control.jsonl"
    file = channel(exchange, market, feed, kind, native)[0]
    return (root / {"binance": "Binance", "kraken": "Kraken", "okx": "OKX"}[exchange]
            / ("Spot" if market == "spot" else "Futures") / symbol.replace("/", "-") / f"{day}_{file}.jsonl")


def inject(payload: dict, fields: dict) -> dict:
    """Add non-colliding fields immediately before native data, if present."""
    if "data" not in payload:
        return payload | fields
    out = {}
    for key, value in payload.items():
        if key == "data":
            out.update(fields)
        out[key] = value
    return out


def count_collision(e: dict, key: str) -> None:
    stream = (e.get("exchange"), e.get("market"), e.get("symbol"), e.get("feed"))
    collisions[stream] += 1
    if collisions[stream] == 1:
        log.warning("%s/%s/%s/%s: native key %s collides with raw metadata; storing exact raw text",
                    *stream, key)


def line(envelope: dict | bytes, offset: int) -> bytes:
    e = loads(envelope.decode()) if isinstance(envelope, bytes) else envelope
    ns = e["recv_ns"]
    fields = {"recv_time_ns": ns, "conn": e["conn"], "offset": offset}
    if e["kind"] == "control":
        detail = e.get("detail") or {}
        name = channel(e.get("exchange"), e.get("market"), e.get("feed"))[1]
        name = name if e.get("exchange") in CHANNELS else detail.get("channel", name)
        rec = {
            "marker": "ws_event", "event": EVENTS.get(e["event"], e["event"]),
            "channel": name, "conn": e["conn"],
            "timestamp": timestamp(ns), "recv_time_ns": ns, "offset": offset,
        }
        if "reason" in e:
            rec["reason"] = e["reason"]
        if detail:
            clashes = {k: v for k, v in detail.items() if k in rec}
            rec.update({k: v for k, v in detail.items() if k not in rec})
            if clashes:
                # A native detail named "detail" is nested too when necessary.
                if "detail" in rec:
                    clashes["detail"] = rec.pop("detail")
                rec["detail"] = clashes
    else:
        text = e["payload"]
        try:
            payload = loads(text)
        except ValueError:
            payload = None
        exchange, market = e.get("exchange"), e.get("market")
        if exchange == "binance" and isinstance(payload, dict) and "stream" in payload and "data" in payload:
            payload = payload["data"]
        _, name = channel(exchange, market, e.get("feed"), e["kind"], payload)
        fallback = {"channel": name, **fields, "raw": text}
        if isinstance(payload, dict):
            if not payload.keys().isdisjoint(fields):
                count_collision(e, next(key for key in fields if key in payload))
                rec = fallback
            elif (exchange == "kraken" and market != "perp") or exchange is None:
                rec = inject(payload, fields)
            else:
                rec = {"feed" if exchange == "kraken" else "channel": name}
                if exchange == "binance" and name == "snapshot":
                    rec.update(event=e.get("event", "anchor"), symbol=e["symbol"])
                rec.update(fields)
                rec["data"] = payload
        else:
            rec = fallback
    return dumps(rec) + b"\n"


def rollover(new_period: str) -> bytes:
    return dumps({"marker": "rollover", "new_period": new_period.replace("-", ""),
                  "timestamp": datetime.now(timezone.utc).isoformat()}) + b"\n"


def archive_identity(path):
    """Lenient archive directories; return canonical exchange/market/pair/channel."""
    ex, market, pair = path.parts[-4:-1]
    exchange = next((x for x in CHANNELS if ex.lower().startswith(x)), None)
    m = market.lower()
    market = "spot" if m.startswith("spot") else "perp" if m.startswith(("fut", "perp", "swap")) else None
    return exchange, market, pair, path.name.split("_", 1)[1].split(".", 1)[0]


def archive_files(tree):
    """Choose one copy of each v3 file, preferring closed plain over gzip/open."""
    chosen = {}
    for p in tree.glob("*/*/*/*.jsonl*"):
        if not p.name.endswith((".jsonl", ".jsonl.gz", ".jsonl.open")):
            continue
        exchange, market, pair, file = archive_identity(p)
        if exchange is None or market is None:
            continue
        key = exchange, market, pair, p.name[:8], file
        rank = 0 if p.suffix == ".jsonl" else 1 if p.suffix == ".gz" else 2
        if key not in chosen or rank < chosen[key][0]:
            chosen[key] = rank, p
    return sorted(p for _, p in chosen.values())


class Headers:
    """Optional archive metadata: physical line offsets and per-file epochs."""
    def __init__(self, file):
        self.file = str(file)
        self.epochs = 0
        self.conn = self.file + "#pre"

    def read(self, rec, index):
        rec = dict(rec)
        if rec.get("event") == "connected" and rec.get("marker") == "ws_event":
            self.epochs += 1
            self.conn = f"{rec.get('conn', self.file)}#{self.epochs}"
        if "offset" not in rec:
            # Archive marker conn names repeat across reconnects. Give marker and
            # following data the same synthetic epoch when offsets are absent.
            if rec.get("marker") == "ws_event":
                rec["conn"] = self.conn
            else:
                rec.setdefault("conn", self.conn)
            rec["offset"] = index
        else:
            rec.setdefault("conn", self.conn)
        if "recv_time_ns" not in rec and "timestamp" in rec:
            value = rec["timestamp"].replace("Z", "+00:00")
            dt = datetime.fromisoformat(value)
            fraction = value.split(".", 1)[1].split("+", 1)[0] if "." in value else ""
            rec["recv_time_ns"] = int(dt.replace(microsecond=0).timestamp()) * 1_000_000_000 + int(fraction[:9].ljust(9, "0") or "0")
        return rec
