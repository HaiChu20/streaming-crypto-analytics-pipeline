"""Connection lifecycle shared by all exchanges.

ConnRun     one WebSocket connection: connect, subscribe, receive, write in-band control events.
Supervisor  keeps one logical connection alive: reconnect with backoff, and replace it
            make-before-break at midnight UTC, at max age (Binance 23h), on a gap, or after Kafka loss.
"""

import asyncio
import logging
import random
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import httpx
from websockets.asyncio.client import connect

from collectors.common.config import ConnSpec, Stream, connections
from collectors.common.envelope import control_record, data_record
from collectors.common.session import Session

log = logging.getLogger(__name__)
_last_recv_ns = 0


def stamp(recv_ns: int | None = None) -> int:
    """One process-wide clock clamp at the envelope stamping point (no await)."""
    global _last_recv_ns
    _last_recv_ns = max(time.time_ns() if recv_ns is None else recv_ns, _last_recv_ns)
    return _last_recv_ns



class Disabled(Exception):
    """Raised by Handler.prepare when a connection cannot run (e.g. missing API key)."""


class Handler:
    """Exchange-specific logic for one connection. A fresh instance is created per ConnRun."""

    idle_ping: tuple[float, str] | None = None  # (seconds without a frame, text to send)

    def __init__(self, spec: ConnSpec, http: httpx.AsyncClient):
        self.spec = spec
        self.http = http
        self.instrument: dict[str, object] = {}  # native symbol -> exchange metadata
        self.tasks: list[asyncio.Task] = []

    @property
    def log_url(self) -> str:
        return self.spec.url

    async def prepare(self) -> str:
        """Fetch instrument metadata / tokens before connecting; return the URL to open."""
        return self.spec.url

    async def on_open(self, run: "ConnRun") -> None:
        """Send subscriptions. Streams are started when their ack arrives (run.start_stream)."""

    def on_frame(self, run: "ConnRun", text: str, recv_ns: int) -> None:
        raise NotImplementedError


class ConnRun:
    def __init__(self, sup: "Supervisor"):
        self.sup = sup
        self.spec = sup.spec
        self.sink = sup.sink
        self.handler: Handler = sup.handler_cls(sup.spec, sup.http)
        self.conn = uuid.uuid4().hex[:12]
        self.seq = 0
        self.ws = None
        self.opened_at: float | None = None
        self.started: set[str] = set()
        self.received: set[str] = set()
        self.synced: set[str] = set()
        self.broken: set[str] = set()
        self.ready = asyncio.Event()
        self.finished = asyncio.Event()
        self.stopping = False
        self.error: str | None = None
        self.task: asyncio.Task | None = None

    # ---- records -------------------------------------------------------------

    def _next_seq(self) -> int:
        self.seq += 1
        return self.seq

    def emit(self, stream: Stream, kind: str, payload: str, recv_ns: int, *, event: str | None = None) -> None:
        if stream.key not in self.started:
            self.start_stream(stream)
        value = data_record(stream, kind, self.conn, self._next_seq(), stamp(recv_ns), payload, event)
        if self.sink.send(stream, value):
            self.sup.stats.add(stream, len(value))
        if stream.key not in self.received:
            self.received.add(stream.key)
            self._check_ready()

    def control(self, stream: Stream, event: str, reason: str | None = None, detail: dict | None = None) -> bool:
        value = control_record(stream, event, self.conn, self._next_seq(), stamp(), reason, detail)
        return self.sink.send(stream, value)

    def start_stream(self, stream: Stream, ack: object = None) -> None:
        if stream.key in self.started:
            return
        crash = self.sup.take_crash_detail(stream)
        if crash is not None:
            self.control(stream, "drop", "collector_crash", crash)
        self.control(
            stream,
            "start",
            detail={
                "url": self.handler.log_url,
                "session": self.sup.session_id,
                "channel": stream.channel,
                "ack": ack,
                "instrument": self.handler.instrument.get(stream.symbol),
            },
        )
        self.started.add(stream.key)
        self._check_ready()

    def stream_synced(self, stream: Stream) -> None:
        """A book stream received (and validated) its snapshot."""
        self.synced.add(stream.key)
        self._check_ready()

    def _check_ready(self) -> None:
        if self.ready.is_set():
            return
        for s in self.spec.streams:
            if s.key not in self.started or s.key not in (self.synced if s.is_book else self.received):
                return
        self.ready.set()

    def gap(self, stream: Stream, reason: str, detail: dict) -> None:
        """Continuity broke: drop the stream and replace the connection to resync it."""
        if stream.key in self.broken:
            return
        self.broken.add(stream.key)
        self.control(stream, "drop", reason, detail)
        self.sup.stats.drops[stream.key] += 1
        log.warning("%s %s %s: %s %s", self.spec.label, stream.feed, stream.symbol, reason, detail)
        self.sup.request_replace("resync")

    def is_broken(self, stream: Stream) -> bool:
        return stream.key in self.broken

    # ---- lifecycle -----------------------------------------------------------

    async def run(self) -> None:
        try:
            url = await self.handler.prepare()
            async with connect(
                url,
                open_timeout=15,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=2**24,
            ) as ws:
                self.ws = ws
                self.opened_at = time.monotonic()
                log.info("%s connected conn=%s", self.spec.label, self.conn)
                await self.handler.on_open(self)
                await self._receive(ws)
        except Disabled as e:
            self.sup.disabled = True
            self.error = str(e)
            log.error("%s disabled: %s", self.spec.label, e)
        except Exception as e:  # noqa: BLE001 - any failure ends this connection; the supervisor reconnects
            self.error = repr(e)
            if not self.stopping:
                log.warning("%s conn=%s ended: %r", self.spec.label, self.conn, e)
        finally:
            for task in self.handler.tasks:
                task.cancel()
            if not self.stopping:
                for s in self.spec.streams:
                    if s.key in self.started and s.key not in self.broken:
                        self.control(s, "drop", "disconnect", {"error": self.error})
                        self.sup.stats.drops[s.key] += 1
            self.finished.set()

    async def _receive(self, ws) -> None:
        idle = self.handler.idle_ping
        while True:
            if idle:
                try:
                    text = await asyncio.wait_for(ws.recv(), timeout=idle[0])
                except TimeoutError:
                    await ws.send(idle[1])
                    continue
            else:
                text = await ws.recv()
            recv_ns = time.time_ns()
            if self.stopping:
                continue
            if isinstance(text, bytes):
                text = text.decode()
            self.handler.on_frame(self, text, recv_ns)

    async def stop(self, reason: str) -> None:
        self.stopping = True
        for s in self.spec.streams:
            if s.key in self.started:
                self.control(s, "stop", reason)
        if self.ws is None:
            if self.task is not None:
                self.task.cancel()
        else:
            await self.ws.close()
            try:
                await asyncio.wait_for(self.finished.wait(), timeout=10)
            except TimeoutError:
                if self.task is not None:
                    self.task.cancel()
        if self.task is not None:
            await asyncio.gather(self.task, return_exceptions=True)
            # Cancellation before run() starts cannot enter its finally block.
            self.finished.set()
        await self.finished.wait()


class Stats:
    def __init__(self):
        self.msgs: dict[str, int] = defaultdict(int)
        self.bytes: dict[str, int] = defaultdict(int)
        self.drops: dict[str, int] = defaultdict(int)
        self.names: dict[str, str] = {}
        self.clean_shutdown = True
        self.lag_max_ms = 0.0

    def add(self, stream: Stream, size: int) -> None:
        self.msgs[stream.key] += 1
        self.bytes[stream.key] += size
        self.names[stream.key] = f"{stream.market}.{stream.feed}.{stream.asset}"

    async def lag_probe(self) -> None:
        while True:
            before = time.monotonic()
            await asyncio.sleep(0.1)
            lag_ms = (time.monotonic() - before - 0.1) * 1000
            self.lag_max_ms = max(self.lag_max_ms, lag_ms)

    async def report(self, label: str, sink, interval: float = 60.0) -> None:
        last_msgs: dict[str, int] = {}
        last_bytes: dict[str, int] = {}
        while True:
            await asyncio.sleep(interval)
            parts = []
            for key in sorted(self.msgs, key=lambda k: self.names[k]):
                m = self.msgs[key] - last_msgs.get(key, 0)
                b = self.bytes[key] - last_bytes.get(key, 0)
                parts.append(f"{self.names[key]}={m / interval:.1f}/s,{b / interval / 1024:.1f}KiB/s")
            last_msgs, last_bytes = dict(self.msgs), dict(self.bytes)
            log.info(
                "[stats %s] queued=%d lost=%d drops=%d lag_max_ms=%.1f | %s",
                label, sink.queued(), len(sink.lost), sum(self.drops.values()), self.lag_max_ms, " ".join(parts),
            )
            self.lag_max_ms = 0.0


class Supervisor:
    def __init__(self, spec, handler_cls, sink, stats, session_id, crash_detail, http, rotation):
        self.spec = spec
        self.handler_cls = handler_cls
        self.sink = sink
        self.stats = stats
        self.session_id = session_id
        self.http = http
        self.rotation = rotation
        self.active: ConnRun | None = None
        self.disabled = False
        self._replacing = False
        self._closing = False
        self._last_resync_started: float | None = None
        self._replace_pending: asyncio.TimerHandle | None = None
        self._crash_detail = crash_detail
        self._crash_pending = {s.key for s in spec.streams} if crash_detail else set()

    def take_crash_detail(self, stream: Stream) -> dict | None:
        """The previous session crashed: each stream gets one drop(collector_crash) before its first start."""
        if stream.key in self._crash_pending:
            self._crash_pending.discard(stream.key)
            return self._crash_detail
        return None

    def _new_run(self) -> ConnRun:
        run = ConnRun(self)
        run.task = asyncio.create_task(run.run(), name=f"{self.spec.label}:{run.conn}")
        return run

    async def run(self) -> None:
        helpers = [asyncio.create_task(self._scheduler()), asyncio.create_task(self._loss_watch())]
        backoff = 1.0
        try:
            self.active = self._new_run()
            while not self._closing:
                run = self.active
                await run.finished.wait()
                while self._replacing:
                    await asyncio.sleep(0.1)
                if run is not self.active or self._closing:
                    continue
                if self.disabled:
                    return
                lived = run.opened_at is not None and time.monotonic() - run.opened_at > 60
                backoff = 1.0 if lived else min(backoff * 2, 60.0)
                delay = backoff * (0.5 + random.random())
                log.warning("%s reconnecting in %.1fs", self.spec.label, delay)
                await asyncio.sleep(delay)
                if not self._closing:
                    self.active = self._new_run()
        finally:
            for task in helpers:
                task.cancel()

    def request_replace(self, reason: str) -> None:
        if self._closing or self.disabled:
            return
        if self._replacing:
            if reason == "resync":
                self._schedule_replace(1, reason)
        else:
            asyncio.create_task(self.replace(reason))

    def _schedule_replace(self, delay: float, reason: str) -> None:
        if self._replace_pending is None:
            self._replace_pending = asyncio.get_running_loop().call_later(delay, self._retry_replace, reason)

    def _retry_replace(self, reason: str) -> None:
        self._replace_pending = None
        self.request_replace(reason)

    async def replace(self, reason: str) -> None:
        """Make before break: open B, wait until B is ready (or timeout), then stop A."""
        if self._replacing or self._closing or self.disabled or self.active is None:
            return
        if reason == "resync":
            now = time.monotonic()
            if self._last_resync_started is not None and now - self._last_resync_started < 30:
                self._schedule_replace(30 - (now - self._last_resync_started), reason)
                return
            self._last_resync_started = now
            if self._replace_pending is not None:
                self._replace_pending.cancel()
                self._replace_pending = None
        self._replacing = True
        try:
            old = self.active
            new = self._new_run()
            log.info("%s replacing conn=%s (%s)", self.spec.label, old.conn, reason)
            try:
                await asyncio.wait_for(new.ready.wait(), timeout=self.rotation["ready_timeout_s"])
            except TimeoutError:
                log.warning("%s replacement not fully ready after %ss", self.spec.label, self.rotation["ready_timeout_s"])
            if new.finished.is_set():
                log.error("%s replacement failed (%s); keeping conn=%s", self.spec.label, new.error, old.conn)
                if self._replace_pending is not None:
                    self._replace_pending.cancel()
                    self._replace_pending = None
                self._schedule_replace(30, reason)
                return
            self.active = new
            await old.stop("rotation" if reason == "rotation" else reason)
        finally:
            self._replacing = False

    async def _scheduler(self) -> None:
        day = datetime.now(timezone.utc).date()
        stagger = self.rotation["stagger_s"].get(self.spec.exchange, 0)
        max_age_h = self.rotation["max_age_h"].get(self.spec.exchange)
        while True:
            await asyncio.sleep(1)
            now = datetime.now(timezone.utc)
            run = self.active
            if now.date() != day and now.second >= stagger:
                day = now.date()
                await self.replace("rotation")
            elif (
                max_age_h is not None
                and run is not None
                and run.opened_at is not None
                and time.monotonic() - run.opened_at > max_age_h * 3600
            ):
                await self.replace("rotation")

    async def _loss_watch(self) -> None:
        mine = {s.key: s for s in self.spec.streams}
        while True:
            await asyncio.sleep(1)
            lost = set(mine) & self.sink.lost
            if not lost or self._replacing or self.active is None or not self.sink.has_room():
                continue
            self.sink.clear_lost(lost)
            for key in lost:
                self.active.control(mine[key], "drop", "kafka_unavailable", {"queued": self.sink.queued()})
                self.active.broken.add(key)
                self.stats.drops[key] += 1
            self.request_replace("resync")

    async def shutdown(self) -> None:
        self._closing = True
        if self._replace_pending is not None:
            self._replace_pending.cancel()
            self._replace_pending = None
        while self._replacing:
            await asyncio.sleep(0.1)
        if self.active is not None and not self.active.finished.is_set():
            await self.active.stop("shutdown")


async def _rotate_on(event: asyncio.Event, sups: list[Supervisor]) -> None:
    while True:
        await event.wait()
        event.clear()
        log.info("manual rotation requested")
        await asyncio.gather(*(sup.replace("rotation") for sup in sups))


async def run_exchange(
    exchange, handler_for, sink, cfg, session: Session | None, stop: asyncio.Event, rotate: asyncio.Event | None = None
) -> Stats:
    """Run every connection of one exchange until `stop` is set, then shut down cleanly.
    Setting `rotate` replaces every connection make-before-break (e.g. on SIGHUP)."""
    stats = Stats()
    crash = session.crash_detail() if session else None
    if crash:
        log.warning("%s: previous collector session %s crashed; writing collector_crash drops", exchange, crash)
    async with httpx.AsyncClient(timeout=20) as http:
        sups = [
            Supervisor(spec, handler_for(spec), sink, stats, session.id if session else "probe", crash, http, cfg["rotation"])
            for spec in connections(cfg, exchange)
        ]
        helpers = [asyncio.create_task(sink.run()), asyncio.create_task(stats.report(exchange, sink))]
        helpers.append(asyncio.create_task(stats.lag_probe()))
        if session:
            helpers.append(asyncio.create_task(session.heartbeat()))
        if rotate:
            helpers.append(asyncio.create_task(_rotate_on(rotate, sups)))
        sup_tasks = []
        for sup in sups:
            sup_tasks.append(asyncio.create_task(sup.run()))
            await asyncio.sleep(0.5)  # stay under per-IP connection rate limits
        await stop.wait()
        await asyncio.gather(*(sup.shutdown() for sup in sups))
        for task in sup_tasks + helpers:
            task.cancel()
    remaining = sink.flush(30)
    stats.clean_shutdown = remaining == 0 and not sink.lost
    if not stats.clean_shutdown:
        log.error("%s: shutdown incomplete: %d records not delivered, %d lost streams", exchange, remaining, len(sink.lost))
    elif session:
        session.stop()
    return stats
