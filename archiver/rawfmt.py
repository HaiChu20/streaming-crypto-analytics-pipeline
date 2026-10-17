"""Raw disk format v2. Kafka envelopes remain v1; decimals become exact token strings."""

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


def path(root: Path, exchange: str, market: str, symbol: str, day: str, feed: str) -> Path:
    """root is the raw tree (usually data/raw), day accepts ISO or YYYYMMDD."""
    day = day.replace("-", "")
    if exchange == "control":
        return root / "control" / f"{day}_control.jsonl"
    return root / exchange / market / symbol.replace("/", "-") / f"{day}_{feed}.jsonl"


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
    e = orjson.loads(envelope) if isinstance(envelope, bytes) else envelope
    ns = e["recv_ns"]
    fields = {"recv_time_ns": ns, "conn": e["conn"], "offset": offset}
    if e["kind"] == "control":
        detail = e.get("detail") or {}
        channel = detail.get("channel", e.get("feed"))
        if "channel" not in detail and e.get("exchange") == "kraken":
            channel = {"l2": "book", "l3": "level3", "trades": "trade"}.get(channel, channel)
        rec = {
            "marker": "ws_event", "event": EVENTS.get(e["event"], e["event"]),
            "channel": channel, "conn": e["conn"],
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
        if isinstance(payload, dict):
            if e.get("exchange") == "binance" and e["kind"] == "snapshot" and "lastUpdateId" in payload:
                fields["type"] = "snapshot"
            if payload.keys().isdisjoint(fields):
                rec = inject(payload, fields)
            else:
                collision = next(key for key in fields if key in payload)
                count_collision(e, collision)
                rec = {"recv_time_ns": ns, "conn": e["conn"], "offset": offset, "raw": text}
        else:
            rec = fields | {"raw": text}
    return dumps(rec) + b"\n"


def rollover(new_period: str) -> bytes:
    return dumps({"marker": "rollover", "new_period": new_period.replace("-", ""),
                  "timestamp": datetime.now(timezone.utc).isoformat()}) + b"\n"
