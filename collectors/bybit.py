"""Bybit v5 public spot + linear perpetual collector.

Depth-200 u advanced by one per delta in live probes; seq is a cross-depth sequence,
so only its ordering is checked. Any snapshot (including service restart u=1) resets.
"""

import asyncio

import orjson

from collectors.common.config import ConnSpec, Stream
from collectors.common.runner import ConnRun, Handler

REST = "https://api.bybit.com"


class BookSeq:
    def __init__(self):
        self.last_u: int | None = None
        self.last_seq: int | None = None

    def on_snapshot(self, u: int, seq: int) -> None:
        self.last_u, self.last_seq = u, seq

    def on_update(self, u: int, seq: int) -> bool:
        if self.last_u is None or u != self.last_u + 1 or seq <= self.last_seq:
            return False
        self.on_snapshot(u, seq)
        return True

    def on_l1(self, u: int, seq: int) -> bool:
        # L1 periodically repeats an unchanged snapshot; u=1 is a service restart.
        if u != 1 and self.last_u is not None and (u < self.last_u or seq < self.last_seq):
            return False
        self.on_snapshot(u, seq)
        return True


class BybitHandler(Handler):
    def __init__(self, spec: ConnSpec, http):
        super().__init__(spec, http)
        self.by_topic: dict[str, Stream] = {f"{s.channel}.{s.symbol}": s for s in spec.streams}
        self.pending: dict[str, list[str]] = {}
        self.seq: dict[str, BookSeq] = {}

    async def prepare(self) -> str:
        category = "linear" if self.spec.market == "perp" else "spot"
        for symbol in sorted({s.symbol for s in self.spec.streams}):
            resp = await self.http.get(REST + "/v5/market/instruments-info", params={"category": category, "symbol": symbol})
            resp.raise_for_status()
            msg = resp.json()
            if msg["retCode"] != 0:
                raise RuntimeError(f"instrument request rejected: {msg}")
            info = msg["result"]["list"][0]
            self.instrument[symbol] = {
                k: info.get(k) for k in ("contractType", "status", "baseCoin", "quoteCoin", "priceFilter",
                                         "lotSizeFilter", "settleCoin", "fundingInterval")
            }
        return self.spec.url

    async def on_open(self, run: ConnRun) -> None:
        topics = list(self.by_topic)
        # Spot permits at most 10 args per request; use the same small batches on linear.
        for index in range(0, len(topics), 10):
            request = str(index // 10)
            self.pending[request] = topics[index:index + 10]
            await run.ws.send(orjson.dumps({"op": "subscribe", "req_id": request, "args": self.pending[request]}).decode())
        self.tasks.append(asyncio.create_task(self._ping(run)))

    async def _ping(self, run: ConnRun) -> None:
        while True:
            await asyncio.sleep(20)
            await run.ws.send('{"op":"ping"}')

    def on_frame(self, run: ConnRun, text: str, recv_ns: int) -> None:
        msg = orjson.loads(text)
        if msg.get("success") is False or msg.get("op") == "error":
            raise RuntimeError(f"subscription rejected: {text[:300]}")
        if msg.get("op") == "subscribe":
            if msg.get("success") is not True:
                raise RuntimeError(f"subscription rejected: {text[:300]}")
            for topic in self.pending.pop(msg.get("req_id"), []):
                run.start_stream(self.by_topic[topic], ack=msg)
            return
        stream = self.by_topic.get(msg.get("topic"))
        if stream is None:
            return  # ping/pong or unrelated control frame
        snapshot = msg.get("type") == "snapshot"
        kind = "trade" if stream.feed == "trades" else "snapshot" if snapshot and stream.feed != "liquidations" else "update"
        run.emit(stream, kind, text, recv_ns)
        if stream.feed not in ("l1", "l2") or run.is_broken(stream):
            return
        book = msg["data"]
        seq = self.seq.setdefault(stream.key, BookSeq())
        previous = {"last_u": seq.last_u, "last_seq": seq.last_seq, "u": book["u"], "seq": book["seq"]}
        if stream.feed == "l1":
            valid = snapshot and seq.on_l1(book["u"], book["seq"])
        elif snapshot:
            seq.on_snapshot(book["u"], book["seq"])
            run.stream_synced(stream)
            return
        else:
            valid = seq.on_update(book["u"], book["seq"])
        if not valid:
            run.gap(stream, "sequence_gap", previous)


def handler_for(spec: ConnSpec) -> type[Handler]:
    return BybitHandler
