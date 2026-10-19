"""Deribit DVOL minute REST poller; no WebSocket connection is opened.

JSON numbers are decoded as token strings (including integers). Each row is
serialized by joining those validated tokens, never through a binary float or
Decimal formatter. Whitespace changes; every numeric token is byte-for-byte
unchanged from the response. The envelope contains only that single row array.
"""

import asyncio
from dataclasses import dataclass
from decimal import Decimal
import json
import logging
import random
import time
import uuid

import httpx

from collectors.common.config import connections
from collectors.common.envelope import control_record, data_record
from collectors.common.runner import stamp

URL = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
MINUTE_MS = 60_000
LOG = logging.getLogger(__name__)


class Number(str):
    """A JSON number's original lexical representation, distinguished from strings."""


@dataclass(frozen=True)
class Candle:
    ts_ms: int
    text: str


def _invalid(token):
    raise ValueError(f"invalid JSON constant: {token}")


def decode_page(text: str) -> tuple[list[Candle], int | None]:
    doc = json.loads(text, parse_float=Number, parse_int=Number, parse_constant=_invalid)
    result = doc["result"]
    candles = []
    for row in result["data"]:
        if not isinstance(row, list) or len(row) != 5 or not all(isinstance(n, Number) for n in row):
            raise ValueError("DVOL candle must contain five JSON numbers")
        ts = int(row[0])
        if ts < 0 or ts % MINUTE_MS or any(Decimal(n) < 0 for n in row[1:]):
            raise ValueError("invalid DVOL timestamp or negative OHLC")
        candles.append(Candle(ts, "[" + ",".join(row) + "]"))
    continuation = result.get("continuation")
    if continuation is not None and not isinstance(continuation, Number):
        raise ValueError("invalid DVOL continuation")
    return candles, int(continuation) if continuation is not None else None


async def fetch_page(http, currency: str, start_ms: int, end_ms: int):
    response = await http.get(URL, params={"currency": currency, "start_timestamp": start_ms,
                                         "end_timestamp": end_ms, "resolution": 60})
    recv_ns = time.time_ns()
    response.raise_for_status()
    return (*decode_page(response.text), recv_ns)


class DvolPoller:
    def __init__(self, http, sink, cfg, session=None):
        self.http, self.sink, self.session = http, sink, session
        self.streams = connections(cfg, "deribit")[0].streams
        self.seen = {s.symbol: set() for s in self.streams}
        self.conn = {s.symbol: uuid.uuid4().hex[:12] for s in self.streams}
        self.started = set()
        self.seq = 0
        self.crash_pending = set(s.symbol for s in self.streams) if session and session.crash_detail() else set()
        self.day = time.time_ns() // 86_400_000_000_000

    def control(self, stream, event, reason=None, detail=None):
        self.seq += 1
        value = control_record(stream, event, self.conn[stream.symbol], self.seq, stamp(), reason, detail)
        if not self.sink.send(stream, value):
            raise RuntimeError("Kafka sink unavailable")

    def start(self, stream):
        if stream.symbol in self.started:
            return
        if stream.symbol in self.crash_pending:
            self.control(stream, "drop", "collector_crash", self.session.crash_detail())
            self.crash_pending.remove(stream.symbol)
        self.control(stream, "start", detail={"url": URL, "channel": "dvol", "resolution": 60,
                                              "session": self.session.id if self.session else None})
        self.started.add(stream.symbol)

    def drop(self, stream, reason, detail=None):
        if stream.symbol in self.started:
            try:
                self.control(stream, "drop", reason, detail)
            finally:
                self.started.discard(stream.symbol)
                self.conn[stream.symbol] = uuid.uuid4().hex[:12]

    async def poll_once(self, stream):
        now_ms = time.time_ns() // 1_000_000
        start_ms = now_ms - 5 * MINUTE_MS
        self.start(stream)
        candles, _, recv_ns = await fetch_page(self.http, stream.symbol, start_ms, now_ms)
        for candle in sorted(candles, key=lambda c: c.ts_ms):
            if (candle.ts_ms < start_ms or candle.ts_ms + MINUTE_MS > recv_ns // 1_000_000
                    or candle.ts_ms in self.seen[stream.symbol]):
                continue
            self.seq += 1
            value = data_record(stream, "update", self.conn[stream.symbol], self.seq, stamp(recv_ns), candle.text)
            if not self.sink.send(stream, value):
                raise RuntimeError("Kafka sink unavailable")
            self.seen[stream.symbol].add(candle.ts_ms)

    async def run(self, stop, rotate):
        delay, backoff = 0.0, 1.0
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
                break
            except TimeoutError:
                pass
            try:
                if not self.sink.has_room():
                    raise RuntimeError("Kafka sink unavailable")
                today = time.time_ns() // 86_400_000_000_000
                if rotate.is_set() or today != self.day:
                    for stream in self.streams:
                        if stream.symbol in self.started:
                            self.control(stream, "stop", "rotation")
                            self.started.discard(stream.symbol)
                            self.conn[stream.symbol] = uuid.uuid4().hex[:12]
                    self.day = today
                    rotate.clear()
                for stream in self.streams:
                    if stream.key in self.sink.lost:
                        # Failed sends may also have lost the original drop event.
                        # Emit a recoverable in-band boundary before clearing lost.
                        self.control(stream, "drop", "kafka_unavailable")
                        self.started.discard(stream.symbol)
                        self.conn[stream.symbol] = uuid.uuid4().hex[:12]
                        self.seen[stream.symbol].clear()  # re-fetch the overlap after data loss
                        self.sink.clear_lost({stream.key})
                    try:
                        await self.poll_once(stream)
                    except Exception as exc:
                        self.drop(stream, "disconnect", {"error": repr(exc)})
                        raise
                backoff = 1.0
                delay = 60 - ((time.time() - 5) % 60)
            except Exception as exc:
                LOG.warning("DVOL poll failed: %r; retrying", exc)
                delay = backoff + random.random()
                backoff = min(60.0, backoff * 2)
        for stream in self.streams:
            if stream.symbol in self.started:
                self.control(stream, "stop", "shutdown")


async def run_poller(sink, cfg, session, stop, rotate):
    clean = False
    async with httpx.AsyncClient(timeout=20) as http:
        tasks = [asyncio.create_task(sink.run()), asyncio.create_task(session.heartbeat())]
        try:
            await DvolPoller(http, sink, cfg, session).run(stop, rotate)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            clean = sink.flush(30) == 0
            if clean:
                session.stop()
    return clean
