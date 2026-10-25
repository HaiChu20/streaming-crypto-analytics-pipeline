"""Envelope format for every record sent to Kafka.

`payload` is the raw frame text exactly as received; it is never parsed and re-serialized.
Control records (start / stop / drop) carry no payload.
"""

import orjson

from collectors.common.config import Stream

SCHEMA_VERSION = 1


def data_record(stream: Stream, kind: str, conn: str, seq: int, recv_ns: int, payload: str, event: str | None = None) -> bytes:
    return orjson.dumps(
        {
            "v": SCHEMA_VERSION,
            "exchange": stream.exchange,
            "market": stream.market,
            "feed": stream.feed,
            "symbol": stream.symbol,
            "kind": kind,
            "conn": conn,
            "seq": seq,
            "recv_ns": recv_ns,
            "payload": payload,
            **({"event": event} if event is not None else {}),
        }
    )


def control_record(
    stream: Stream,
    event: str,
    conn: str,
    seq: int,
    recv_ns: int,
    reason: str | None = None,
    detail: dict | None = None,
) -> bytes:
    record = {
        "v": SCHEMA_VERSION,
        "exchange": stream.exchange,
        "market": stream.market,
        "feed": stream.feed,
        "symbol": stream.symbol,
        "kind": "control",
        "event": event,
        "conn": conn,
        "seq": seq,
        "recv_ns": recv_ns,
    }
    if reason is not None:
        record["reason"] = reason
    if detail is not None:
        record["detail"] = detail
    return orjson.dumps(record)
