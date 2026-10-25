"""Binance spot + USDⓈ-M perp collector.

Connections use combined-stream URLs (no subscribe messages). Depth books are synced to a
REST snapshot with Binance's documented update-ID rules; the snapshot itself is recorded too.
"""

import asyncio
import logging
import time

import orjson

from collectors.common.config import ConnSpec, Stream
from collectors.common.runner import ConnRun, Handler

log = logging.getLogger(__name__)

SNAPSHOT_INTERVAL_S = 7 * 60

TRADE_LIKE = {"trades", "trades_agg"}


class SpotDepthSync:
    """Spot update-ID continuity (official "manage a local order book" procedure)."""

    def __init__(self):
        self.pending: list[tuple[int, int]] = []
        self.local_id: int | None = None
        self.waiting_first = False

    def on_delta(self, U: int, u: int) -> str:
        if self.local_id is None:
            self.pending.append((U, u))
            return "buffered"
        if self.waiting_first:
            if u <= self.local_id:
                return "ignored"
            if U > self.local_id + 1:
                return "stale"
            self.waiting_first = False
            self.local_id = u
            return "applied"
        if u < self.local_id:
            return "ignored"
        if U > self.local_id + 1:
            return "gap"
        self.local_id = u
        return "applied"

    def on_snapshot(self, last_id: int) -> str:
        """Returns 'synced', or 'stale' when the snapshot is older than the buffered deltas."""
        events = [(U, u) for U, u in self.pending if u > last_id]
        self.pending = []
        self.local_id = last_id
        if not events:
            self.waiting_first = True
            return "synced"
        U, u = events[0]
        if not U <= last_id + 1 <= u:
            self.local_id = None
            return "stale"
        for U, u in events:
            if U > self.local_id + 1:
                self.local_id = None
                return "stale"
            self.local_id = max(self.local_id, u)
        return "synced"


class PerpDepthSync:
    """USDⓈ-M futures continuity: first event U <= lastUpdateId <= u, then pu == previous u."""

    def __init__(self):
        self.pending: list[tuple[int, int, int]] = []
        self.last_id: int | None = None
        self.prev_u: int | None = None

    def on_delta(self, U: int, u: int, pu: int) -> str:
        if self.last_id is None:
            self.pending.append((U, u, pu))
            return "buffered"
        if self.prev_u is None:
            if u < self.last_id:
                return "ignored"
            if not U <= self.last_id <= u:
                return "stale"
            self.prev_u = u
            return "applied"
        if pu != self.prev_u:
            return "gap"
        self.prev_u = u
        return "applied"

    def on_snapshot(self, last_id: int) -> str:
        events = [e for e in self.pending if e[1] >= last_id]
        self.pending = []
        self.last_id = last_id
        self.prev_u = None
        for U, u, pu in events:
            result = self.on_delta(U, u, pu)
            if result in ("stale", "gap"):
                self.last_id = None
                return "stale"
        return "synced"


class BinanceHandler(Handler):
    def __init__(self, spec: ConnSpec, http):
        super().__init__(spec, http)
        self.by_name: dict[str, Stream] = {f"{s.symbol.lower()}@{s.channel}": s for s in spec.streams}
        self.perp = spec.market == "perp"
        self.sync: dict[str, SpotDepthSync | PerpDepthSync] = {}

    async def prepare(self) -> str:
        path = "/fapi/v1/exchangeInfo" if self.perp else "/api/v3/exchangeInfo"
        resp = await self.http.get(self.spec.options["rest"] + path)
        resp.raise_for_status()
        symbols = {s.symbol for s in self.spec.streams}
        for info in resp.json()["symbols"]:
            if info["symbol"] in symbols:
                self.instrument[info["symbol"]] = {
                    "status": info.get("status"),
                    "contractType": info.get("contractType"),
                    "filters": info.get("filters"),
                }
        return f"{self.spec.url}?streams={'/'.join(self.by_name)}"

    async def on_open(self, run: ConnRun) -> None:
        for stream in self.spec.streams:
            run.start_stream(stream)
            if stream.feed == "l2":
                self.sync[stream.key] = PerpDepthSync() if self.perp else SpotDepthSync()
                self.tasks.append(asyncio.create_task(self._snapshot(run, stream)))
                self.tasks.append(asyncio.create_task(self._periodic_snapshots(run, stream)))

    async def _snapshot(self, run: ConnRun, stream: Stream, *, periodic=False) -> None:
        try:
            path = "/fapi/v1/depth" if self.perp else "/api/v3/depth"
            await asyncio.sleep(0 if periodic else 1.5)  # let deltas buffer first, per the documented procedure
            for attempt in range(10):
                resp = await self.http.get(
                    self.spec.options["rest"] + path,
                    params={"symbol": stream.symbol, "limit": self.spec.options["snapshot_limit"]},
                )
                recv_ns = time.time_ns()
                if resp.status_code != 200:
                    log.warning("%s snapshot HTTP %s: %s", stream.key, resp.status_code, resp.text[:200])
                    await asyncio.sleep(2 + attempt)
                    continue
                run.emit(stream, "snapshot", resp.text, recv_ns, event="periodic" if periodic else "anchor")
                if periodic:
                    return  # archival only: never reset the collector sync state
                last_id = orjson.loads(resp.content)["lastUpdateId"]
                if self.sync[stream.key].on_snapshot(last_id) == "synced":
                    run.stream_synced(stream)
                    return
                log.info("%s snapshot %s older than buffered deltas; refetching", stream.key, last_id)
                await asyncio.sleep(1)
            if not periodic:
                run.gap(stream, "sequence_gap", {"error": "could not sync REST snapshot"})
        except Exception as e:
            log.exception("%s snapshot failed", stream.key)
            if not periodic:
                run.gap(stream, "sequence_gap", {"error": f"snapshot failed: {e!r}"})

    async def _periodic_snapshots(self, run: ConnRun, stream: Stream) -> None:
        while True:
            await asyncio.sleep(SNAPSHOT_INTERVAL_S)
            await self._snapshot(run, stream, periodic=True)

    def on_frame(self, run: ConnRun, text: str, recv_ns: int) -> None:
        msg = orjson.loads(text)
        stream = self.by_name.get(msg.get("stream", ""))
        if stream is None:
            log.debug("%s unrouted frame: %s", self.spec.label, text[:200])
            return
        kind = "trade" if stream.feed in TRADE_LIKE else "update"
        run.emit(stream, kind, text, recv_ns)
        if stream.feed == "l2" and not run.is_broken(stream):
            data = msg["data"]
            sync = self.sync[stream.key]
            if self.perp:
                result = sync.on_delta(data["U"], data["u"], data["pu"])
            else:
                result = sync.on_delta(data["U"], data["u"])
            if result == "gap":
                run.gap(stream, "sequence_gap", {"U": data["U"], "u": data["u"], "pu": data.get("pu")})
            elif result == "stale":
                run.gap(stream, "sequence_gap", {"error": "first delta after snapshot did not line up"})


def handler_for(spec: ConnSpec) -> type[Handler]:
    return BinanceHandler
