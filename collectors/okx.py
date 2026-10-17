"""OKX spot + swap collector (plan.md §4, §5.2).

Public connection: bbo-tbt, books, trades. Business connection: trades-all.
`books` continuity: each update's prevSeqId must equal the previous seqId.
"""

import logging

import orjson

from collectors.common.config import ConnSpec, Stream
from collectors.common.runner import ConnRun, Handler

log = logging.getLogger(__name__)

REST = "https://www.okx.com"
TRADE_CHANNELS = {"trades", "trades-all"}


class BookSeq:
    def __init__(self):
        self.last: int | None = None

    def on_snapshot(self, seq_id: int) -> None:
        self.last = seq_id

    def on_update(self, prev_seq_id: int, seq_id: int) -> bool:
        if self.last is None or prev_seq_id != self.last:
            return False
        self.last = seq_id
        return True


class OkxHandler(Handler):
    idle_ping = (25.0, "ping")  # OKX closes connections after 30s without traffic

    def __init__(self, spec: ConnSpec, http):
        super().__init__(spec, http)
        self.by_arg: dict[tuple[str, str], Stream] = {(s.channel, s.symbol): s for s in spec.streams}
        self.seq: dict[str, BookSeq] = {}

    async def prepare(self) -> str:
        inst_type = "SWAP" if self.spec.market == "perp" else "SPOT"
        for symbol in {s.symbol for s in self.spec.streams}:
            resp = await self.http.get(
                REST + "/api/v5/public/instruments", params={"instType": inst_type, "instId": symbol}
            )
            resp.raise_for_status()
            data = resp.json()["data"]
            if data:
                info = data[0]
                self.instrument[symbol] = {
                    k: info.get(k) for k in ("instType", "tickSz", "lotSz", "minSz", "ctVal", "ctValCcy", "state")
                }
        return self.spec.url

    async def on_open(self, run: ConnRun) -> None:
        args = [{"channel": s.channel, "instId": s.symbol} for s in self.spec.streams]
        await run.ws.send(orjson.dumps({"op": "subscribe", "args": args}).decode())

    def on_frame(self, run: ConnRun, text: str, recv_ns: int) -> None:
        if text == "pong":
            return
        msg = orjson.loads(text)
        arg = msg.get("arg") or {}
        stream = self.by_arg.get((arg.get("channel"), arg.get("instId")))
        event = msg.get("event")
        if event == "subscribe" and stream is not None:
            run.start_stream(stream, ack=msg)
            return
        if event == "error":
            raise RuntimeError(f"subscription rejected: {text[:300]}")
        if event is not None:
            log.warning("%s event: %s", self.spec.label, text[:300])
            return
        if stream is None:
            log.debug("%s unrouted frame: %s", self.spec.label, text[:200])
            return

        if stream.feed == "l2":
            action = msg.get("action")
            run.emit(stream, "snapshot" if action == "snapshot" else "update", text, recv_ns)
            if run.is_broken(stream):
                return
            book = msg["data"][0]
            seq = self.seq.setdefault(stream.key, BookSeq())
            if action == "snapshot":
                seq.on_snapshot(book["seqId"])
                run.stream_synced(stream)
            elif not seq.on_update(book["prevSeqId"], book["seqId"]):
                run.gap(stream, "sequence_gap", {"last_seqId": seq.last, "prevSeqId": book["prevSeqId"]})
        else:
            run.emit(stream, "trade" if stream.channel in TRADE_CHANNELS else "update", text, recv_ns)


def handler_for(spec: ConnSpec) -> type[Handler]:
    return OkxHandler
