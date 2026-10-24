"""Coinbase Advanced Trade public spot collector; sequence_num is connection-wide."""

import orjson

from collectors.common.config import ConnSpec, Stream
from collectors.common.runner import ConnRun, Handler

REST = "https://api.coinbase.com/api/v3/brokerage/market/products/"


class CoinbaseHandler(Handler):
    def __init__(self, spec: ConnSpec, http):
        super().__init__(spec, http)
        self.by_arg: dict[tuple[str, str], Stream] = {
            ("l2_data" if s.channel == "level2" else s.channel, s.symbol): s for s in spec.streams
        }
        self.last_seq: int | None = None

    async def prepare(self) -> str:
        for symbol in sorted({s.symbol for s in self.spec.streams}):
            resp = await self.http.get(REST + symbol)
            resp.raise_for_status()
            info = resp.json()
            self.instrument[symbol] = {
                k: info.get(k) for k in ("base_increment", "quote_increment", "price_increment", "base_min_size",
                                         "quote_min_size", "product_type", "status", "is_disabled", "trading_disabled")
            }
        return self.spec.url

    async def on_open(self, run: ConnRun) -> None:
        # Subscribe immediately (within Coinbase's 5s deadline), one request per channel.
        await run.ws.send(orjson.dumps({"type": "subscribe", "channel": "heartbeats"}).decode())
        for channel in dict.fromkeys(s.channel for s in self.spec.streams):
            symbols = sorted({s.symbol for s in self.spec.streams if s.channel == channel})
            await run.ws.send(orjson.dumps({"type": "subscribe", "channel": channel, "product_ids": symbols}).decode())

    def on_frame(self, run: ConnRun, text: str, recv_ns: int) -> None:
        msg = orjson.loads(text)
        if msg.get("type") == "error" or msg.get("channel") == "error":
            raise RuntimeError(f"subscription rejected: {text[:300]}")
        # Include subscriptions and heartbeats: checking a channel in isolation creates false gaps.
        seq = msg.get("sequence_num")
        gap = seq is not None and self.last_seq is not None and seq != self.last_seq + 1
        previous = self.last_seq
        if seq is not None:
            self.last_seq = seq
        channel = msg.get("channel")
        if channel == "subscriptions":
            for event in msg.get("events", []):
                for name, symbols in event.get("subscriptions", {}).items():
                    for symbol in symbols:
                        stream = self.by_arg.get(("l2_data" if name == "level2" else name, symbol))
                        if stream is not None:
                            run.start_stream(stream, ack=msg)
        else:
            routed: dict[str, tuple[Stream, list[str]]] = {}
            for event in msg.get("events", []):
                symbols = ({t["product_id"] for t in event.get("trades", [])} if channel == "market_trades"
                           else {event.get("product_id")})
                for symbol in symbols:
                    stream = self.by_arg.get((channel, symbol))
                    if stream is not None:
                        routed.setdefault(stream.key, (stream, []))[1].append(event.get("type"))
            for stream, types in routed.values():
                snapshot = "snapshot" in types
                run.emit(stream, "snapshot" if snapshot else "trade" if stream.feed == "trades" else "update", text, recv_ns)
                if stream.is_book and not run.is_broken(stream):
                    if snapshot and not gap:
                        run.stream_synced(stream)
                    elif not snapshot and stream.key not in run.synced:
                        run.gap(stream, "sequence_gap", {"error": "update before snapshot", "sequence_num": seq})
        if gap:
            for stream in self.spec.streams:
                run.gap(stream, "sequence_gap", {"last_sequence_num": previous, "sequence_num": seq})


def handler_for(spec: ConnSpec) -> type[Handler]:
    return CoinbaseHandler
