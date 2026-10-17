"""Kraken spot (WS v2: book, trade, level3) + futures (WS v1: book, trade, ticker) collector.

Spot L2/L3 books are kept in memory only to verify Kraken's CRC32 checksums (plan.md §5.2).
Prices/quantities are parsed as Decimal, exactly as delivered, so checksums can be rebuilt.
Futures books are checked with `seq` (+1 per message per product).
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.parse
import zlib
from bisect import insort
from datetime import datetime
from decimal import Decimal

import orjson

from collectors.common.config import ConnSpec, Stream
from collectors.common.runner import ConnRun, Disabled, Handler

log = logging.getLogger(__name__)

SPOT_REST = "https://api.kraken.com"
FUTURES_REST = "https://futures.kraken.com"


def _num(value) -> Decimal:
    """Exact decimal from a JSON number (parsed as Decimal) or a string, keeping trailing zeros."""
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _fmt(value: Decimal) -> str:
    return format(value, "f").replace(".", "").lstrip("0")


def _decimal_json(text: str) -> dict:
    return json.loads(text, parse_float=Decimal, parse_int=Decimal)


class L3TimestampError(ValueError):
    """A level3 order cannot be placed in timestamp queue order."""


def _l3_timestamp(value: str) -> str:
    if isinstance(value, str) and len(value) == 30:
        return value  # Kraken's usual nine fractional digits need no normalization.
    try:
        if not isinstance(value, str) or not value.endswith("Z"):
            raise ValueError
        body = value[:-1]
        base, dot, fraction = body.partition(".")
        if (len(base) != 19 or (dot and not fraction) or len(fraction) > 9
                or (fraction and not (fraction.isascii() and fraction.isdigit()))):
            raise ValueError
        datetime.strptime(base, "%Y-%m-%dT%H:%M:%S")
        return base + "." + fraction.ljust(9, "0") + "Z"
    except ValueError as exc:
        raise L3TimestampError("invalid level3 order timestamp") from exc


class L2Book:
    """Price -> qty per side, truncated to the subscribed depth."""

    def __init__(self, depth: int):
        self.depth = depth
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}

    def apply(self, data: dict, snapshot: bool) -> None:
        if snapshot:
            self.bids, self.asks = {}, {}
        for side, book in (("bids", self.bids), ("asks", self.asks)):
            for level in data.get(side, []):
                price, qty = _num(level["price"]), _num(level["qty"])
                if qty == 0:
                    book.pop(price, None)
                else:
                    book[price] = qty
        for book, best_first in ((self.bids, True), (self.asks, False)):
            if len(book) > self.depth:
                for price in sorted(book, reverse=best_first)[self.depth :]:
                    del book[price]

    def checksum(self) -> int:
        asks = sorted(self.asks)[:10]
        bids = sorted(self.bids, reverse=True)[:10]
        text = "".join(_fmt(p) + _fmt(self.asks[p]) for p in asks)
        text += "".join(_fmt(p) + _fmt(self.bids[p]) for p in bids)
        return zlib.crc32(text.encode()) & 0xFFFFFFFF


class L3Book:
    """Price -> timestamp-ordered order queue, truncated to the subscribed depth."""

    def __init__(self, depth: int):
        self.depth = depth
        self.sides: dict[str, dict[Decimal, list[tuple[str, int, str, Decimal]]]] = {"bids": {}, "asks": {}}
        self.where: dict[str, tuple[str, Decimal]] = {}
        self.seq = 0

    def _remove(self, oid: str) -> None:
        location = self.where.pop(oid, None)
        if location is None:
            return
        side, price = location
        level = self.sides[side][price]
        for i, (_, _, order_id, _) in enumerate(level):
            if order_id == oid:
                level.pop(i)
                break
        if not level:
            del self.sides[side][price]

    def apply(self, data: dict, snapshot: bool) -> None:
        if snapshot:
            self.sides = {"bids": {}, "asks": {}}
            self.where = {}
            self.seq = 0
        for side in ("bids", "asks"):
            book = self.sides[side]
            for order in data.get(side, []):
                oid = order["order_id"]
                event = "add" if snapshot else order.get("event")
                timestamp = _l3_timestamp(order.get("timestamp")) if event in ("add", "modify") else None
                self._remove(oid)
                if event in ("add", "modify"):
                    price = _num(order["limit_price"])
                    self.seq += 1
                    insort(book.setdefault(price, []), (timestamp, self.seq, oid, _num(order["order_qty"])))
                    self.where[oid] = (side, price)
        for side, best_first in (("bids", True), ("asks", False)):
            book = self.sides[side]
            if len(book) > self.depth:
                for price in sorted(book, reverse=best_first)[self.depth :]:
                    for _, _, oid, _ in book.pop(price):
                        self.where.pop(oid, None)

    def checksum(self) -> int:
        asks = self.sides["asks"]
        bids = self.sides["bids"]
        text = "".join(
            _fmt(p) + _fmt(qty) for p in sorted(asks)[:10] for _, _, _, qty in asks[p]
        )
        text += "".join(
            _fmt(p) + _fmt(qty) for p in sorted(bids, reverse=True)[:10] for _, _, _, qty in bids[p]
        )
        return zlib.crc32(text.encode()) & 0xFFFFFFFF


async def ws_token(http) -> str:
    """REST GetWebSocketsToken, signed with the key in .env (key needs only WebSocket permission)."""
    key, secret = os.environ.get("KRAKEN_API_KEY"), os.environ.get("KRAKEN_API_SECRET")
    if not key or not secret:
        raise Disabled("KRAKEN_API_KEY / KRAKEN_API_SECRET not set in .env; level3 needs them")
    path = "/0/private/GetWebSocketsToken"
    nonce = str(time.time_ns() // 1_000_000)
    body = urllib.parse.urlencode({"nonce": nonce})
    message = path.encode() + hashlib.sha256((nonce + body).encode()).digest()
    signature = base64.b64encode(hmac.new(base64.b64decode(secret), message, hashlib.sha512).digest())
    resp = await http.post(
        SPOT_REST + path,
        content=body,
        headers={"API-Key": key, "API-Sign": signature.decode(), "Content-Type": "application/x-www-form-urlencoded"},
    )
    result = resp.json()
    if result.get("error"):
        raise RuntimeError(f"GetWebSocketsToken failed: {result['error']}")
    return result["result"]["token"]


class KrakenSpotHandler(Handler):
    """book + trade on ws.kraken.com, or level3 on ws-l3.kraken.com (with token)."""

    def __init__(self, spec: ConnSpec, http):
        super().__init__(spec, http)
        self.by_arg: dict[tuple[str, str], Stream] = {(s.channel, s.symbol): s for s in spec.streams}
        self.books: dict[str, L2Book | L3Book] = {}
        self.token: str | None = None

    async def prepare(self) -> str:
        if self.spec.options.get("auth"):
            self.token = await ws_token(self.http)
        resp = await self.http.get(SPOT_REST + "/0/public/AssetPairs", params={"pair": "XBTUSD,ETHUSD"})
        resp.raise_for_status()
        for info in resp.json()["result"].values():
            symbol = info["wsname"].replace("XBT", "BTC")
            self.instrument[symbol] = {
                k: info.get(k) for k in ("altname", "pair_decimals", "lot_decimals", "tick_size", "ordermin", "status")
            }
        return self.spec.url

    async def on_open(self, run: ConnRun) -> None:
        channels: dict[str, list[str]] = {}
        for s in self.spec.streams:
            channels.setdefault(s.channel, []).append(s.symbol)
        for channel, symbols in channels.items():
            params = {"channel": channel, "symbol": symbols, "snapshot": True}
            if channel in ("book", "level3"):
                params["depth"] = self.spec.options["depth"]["l3" if channel == "level3" else "l2"]
            if channel == "level3":
                params["token"] = self.token
            await run.ws.send(orjson.dumps({"method": "subscribe", "params": params}).decode())

    def on_frame(self, run: ConnRun, text: str, recv_ns: int) -> None:
        msg = orjson.loads(text)
        method = msg.get("method")
        if method == "subscribe":
            if msg.get("success") is False:
                raise RuntimeError(f"subscription rejected: {text[:300]}")
            result = msg.get("result") or {}
            stream = self.by_arg.get((result.get("channel"), result.get("symbol")))
            if msg.get("success") and stream is not None:
                run.start_stream(stream, ack=msg)
            else:
                log.error("%s unrouted subscribe ack: %s", self.spec.label, text[:300])
            return
        channel = msg.get("channel")
        if method is not None or channel in ("heartbeat", "status") or "data" not in msg:
            return
        data = msg["data"]
        if not data:
            return
        symbols = {d.get("symbol") for d in data}
        if len(symbols) > 1:
            log.warning("%s frame carries several symbols %s; routed by the first", self.spec.label, symbols)
        stream = self.by_arg.get((channel, data[0].get("symbol")))
        if stream is None:
            return
        snapshot = msg.get("type") == "snapshot"
        kind = "snapshot" if snapshot else ("trade" if stream.feed == "trades" else "update")
        run.emit(stream, kind, text, recv_ns)
        if stream.is_book and not run.is_broken(stream):
            self._check_book(run, stream, text, snapshot)

    def _check_book(self, run: ConnRun, stream: Stream, text: str, snapshot: bool) -> None:
        data = _decimal_json(text)["data"][0]
        try:
            if snapshot:
                depth = self.spec.options["depth"][stream.feed]
                book = self.books[stream.key] = L3Book(depth) if stream.feed == "l3" else L2Book(depth)
                book.apply(data, snapshot=True)
            else:
                book = self.books.get(stream.key)
                if book is None:
                    return
                book.apply(data, snapshot=False)
        except L3TimestampError:
            run.gap(stream, "book_anomaly", {"error": "invalid_l3_timestamp", "snapshot": snapshot})
            return
        got = book.checksum()
        expected = int(data["checksum"])
        if got != expected:
            detail = {"expected": expected, "got": got}
            if snapshot:
                detail["snapshot"] = True
            run.gap(stream, "checksum_mismatch", detail)
        elif snapshot:
            run.stream_synced(stream)


class KrakenFuturesHandler(Handler):
    def __init__(self, spec: ConnSpec, http):
        super().__init__(spec, http)
        self.by_arg: dict[tuple[str, str], Stream] = {(s.channel, s.symbol): s for s in spec.streams}
        self.seq: dict[str, int] = {}

    async def prepare(self) -> str:
        resp = await self.http.get(FUTURES_REST + "/derivatives/api/v3/instruments")
        resp.raise_for_status()
        symbols = {s.symbol for s in self.spec.streams}
        for info in resp.json()["instruments"]:
            if info["symbol"] in symbols:
                self.instrument[info["symbol"]] = {
                    k: info.get(k) for k in ("type", "tickSize", "contractSize", "tradeable", "contractValueTradePrecision")
                }
        return self.spec.url

    async def on_open(self, run: ConnRun) -> None:
        feeds: dict[str, list[str]] = {}
        for s in self.spec.streams:
            feeds.setdefault(s.channel, []).append(s.symbol)
        for feed, products in feeds.items():
            await run.ws.send(orjson.dumps({"event": "subscribe", "feed": feed, "product_ids": products}).decode())

    def on_frame(self, run: ConnRun, text: str, recv_ns: int) -> None:
        msg = orjson.loads(text)
        event = msg.get("event")
        if event == "subscribed":
            for product in msg.get("product_ids", []):
                stream = self.by_arg.get((msg.get("feed"), product))
                if stream is not None:
                    run.start_stream(stream, ack=msg)
            return
        if event in ("error", "alert", "subscribed_failed"):
            raise RuntimeError(f"subscription rejected: {text[:300]}")
        if event is not None:
            if event != "info":
                log.warning("%s event: %s", self.spec.label, text[:300])
            return
        feed = msg.get("feed", "")
        snapshot = feed.endswith("_snapshot")
        stream = self.by_arg.get((feed.removesuffix("_snapshot"), msg.get("product_id")))
        if stream is None:
            return
        kind = "snapshot" if snapshot else ("trade" if stream.feed == "trades" else "update")
        run.emit(stream, kind, text, recv_ns)
        if stream.feed == "l2" and not run.is_broken(stream):
            seq = msg["seq"]
            if snapshot:
                self.seq[stream.key] = seq
                run.stream_synced(stream)
            elif stream.key in self.seq:
                if seq != self.seq[stream.key] + 1:
                    run.gap(stream, "sequence_gap", {"last_seq": self.seq[stream.key], "seq": seq})
                self.seq[stream.key] = seq


def handler_for(spec: ConnSpec) -> type[Handler]:
    return KrakenFuturesHandler if spec.market == "perp" else KrakenSpotHandler
