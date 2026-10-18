"""Collector reliability checks with in-memory sinks and connections (no network)."""

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import orjson
import pytest

from collectors import __main__ as cli
from collectors.binance import BinanceHandler, SpotDepthSync
from collectors.common import runner
from collectors.common.config import ConnSpec, Stream
from collectors.common.runner import ConnRun, Handler, Stats, Supervisor
from collectors.kraken import KrakenFuturesHandler, KrakenSpotHandler
from collectors.okx import OkxHandler


class Sink:
    def __init__(self):
        self.records = []
        self.lost = set()
        self.reject = False
        self.remaining = 0

    def send(self, stream, value):
        if self.reject:
            self.reject = False
            self.lost.add(stream.key)
            return False
        self.records.append(orjson.loads(value))
        return True

    def clear_lost(self, keys):
        self.lost -= keys

    def has_room(self):
        return True

    def queued(self):
        return 0

    async def run(self):
        await asyncio.Event().wait()

    def flush(self, timeout):
        assert timeout == 30
        return self.remaining


def supervisor(handler=Handler, feed="l2", exchange="binance", channel="depth"):
    stream = Stream(exchange, "spot", feed, "BTC", "BTC/USD", channel, f"raw.{feed}", 0)
    spec = ConnSpec(
        exchange, "spot", "test", "ws://unused", [stream],
        {"rest": "https://unused", "snapshot_limit": 100, "depth": {"l2": 10, "l3": 100}},
    )
    return Supervisor(
        spec, handler, Sink(), Stats(), "test", None, None,
        {"ready_timeout_s": 0.01, "stagger_s": {}, "max_age_h": {}},
    )


def drops(sink):
    return [r for r in sink.records if r.get("event") == "drop"]


@pytest.mark.parametrize("failure", [httpx.ReadTimeout("timeout"), httpx.ConnectError("transport"), b"bad json", b"{}"])
def test_binance_snapshot_failure_requests_resync(monkeypatch, failure):
    async def check():
        sup = supervisor(BinanceHandler)
        sup.request_replace = Mock()
        run = ConnRun(sup)
        stream = sup.spec.streams[0]
        run.handler.sync[stream.key] = SpotDepthSync()
        if isinstance(failure, Exception):
            run.handler.http = SimpleNamespace(get=AsyncMock(side_effect=failure))
        else:
            response = SimpleNamespace(status_code=200, text=failure.decode(), content=failure)
            run.handler.http = SimpleNamespace(get=AsyncMock(return_value=response))
        monkeypatch.setattr(runner.asyncio, "sleep", AsyncMock())
        await run.handler._snapshot(run, stream)
        assert len(drops(sup.sink)) == 1
        assert drops(sup.sink)[0]["reason"] == "sequence_gap"
        assert drops(sup.sink)[0]["detail"]["error"].startswith("snapshot failed: ")
        sup.request_replace.assert_called_once_with("resync")
    asyncio.run(check())


def test_binance_snapshot_cancellation_propagates(monkeypatch):
    async def check():
        sup = supervisor(BinanceHandler)
        sup.request_replace = Mock()
        run = ConnRun(sup)
        run.handler.http = SimpleNamespace(get=AsyncMock(side_effect=asyncio.CancelledError))
        monkeypatch.setattr(runner.asyncio, "sleep", AsyncMock())
        with pytest.raises(asyncio.CancelledError):
            await run.handler._snapshot(run, sup.spec.streams[0])
        assert not drops(sup.sink)
        sup.request_replace.assert_not_called()
    asyncio.run(check())


def test_binance_snapshot_http_retries_preserve_final_gap(monkeypatch):
    async def check():
        sup = supervisor(BinanceHandler)
        sup.request_replace = Mock()
        run = ConnRun(sup)
        run.handler.http = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(status_code=503, text="retry")))
        monkeypatch.setattr(runner.asyncio, "sleep", AsyncMock())
        await run.handler._snapshot(run, sup.spec.streams[0])
        assert run.handler.http.get.await_count == 10
        assert drops(sup.sink)[0]["detail"] == {"error": "could not sync REST snapshot"}
        sup.request_replace.assert_called_once_with("resync")
    asyncio.run(check())


@pytest.mark.parametrize("tampered", [False, True])
def test_kraken_snapshot_checksum_controls_readiness(tampered):
    sup = supervisor(KrakenSpotHandler, exchange="kraken", channel="book")
    sup.request_replace = Mock()
    run = ConnRun(sup)
    stream = sup.spec.streams[0]
    path = Path(__file__).parent / "fixtures/kraken_docs/l2_checksum_example.json"
    msg = orjson.loads(path.read_bytes())
    if tampered:
        msg["data"][0]["checksum"] = 1
    run.handler.on_frame(run, orjson.dumps(msg).decode(), 123)
    assert (stream.key in run.synced) is not tampered
    assert run.ready.is_set() is not tampered
    if tampered:
        assert drops(sup.sink)[0]["detail"] == {"expected": 1, "got": 3310070434, "snapshot": True}
        sup.request_replace.assert_called_once_with("resync")
    else:
        assert not drops(sup.sink)


def replacement(failed=False, ready=True):
    run = SimpleNamespace(conn="new", ready=asyncio.Event(), finished=asyncio.Event(), error="failed", stop=AsyncMock())
    if ready:
        run.ready.set()
    if failed:
        run.finished.set()
    return run


def test_resync_replacements_are_rate_limited():
    async def check():
        sup = supervisor()
        sup.active = replacement()
        sup._new_run = Mock(side_effect=lambda: replacement())
        await sup.replace("resync")
        sup.request_replace("resync")
        sup.request_replace("resync")
        await asyncio.sleep(0)
        pending = sup._replace_pending
        assert pending is not None
        assert 29 < pending.when() - asyncio.get_running_loop().time() <= 30
        await sup.replace("resync")  # direct callers obey the same limit
        assert sup._replace_pending is pending
        assert sup._new_run.call_count == 1
        pending.cancel()
        sup._last_resync_started -= 30
        sup._retry_replace("resync")
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert sup._new_run.call_count == 2
        await sup.shutdown()
    asyncio.run(check())


@pytest.mark.parametrize("reason", ["rotation", "resync", "manual"])
def test_failed_replacement_retries_and_keeps_old(reason):
    async def check():
        sup = supervisor()
        old = sup.active = replacement()
        sup._new_run = Mock(return_value=replacement(failed=True))
        await sup.replace(reason)
        assert sup.active is old
        old.stop.assert_not_awaited()
        assert 29 < sup._replace_pending.when() - asyncio.get_running_loop().time() <= 30
        await sup.shutdown()
        assert sup._replace_pending is None
    asyncio.run(check())


def test_resync_uses_remaining_delay_without_delaying_rotation(monkeypatch):
    async def check():
        clock = SimpleNamespace(now=100.0)
        monkeypatch.setattr(runner, "time", SimpleNamespace(monotonic=lambda: clock.now))
        sup = supervisor()
        sup.active = replacement()
        sup._new_run = Mock(side_effect=lambda: replacement())
        await sup.replace("resync")
        clock.now += 10
        sup.request_replace("resync")
        await asyncio.sleep(0)
        assert 19 < sup._replace_pending.when() - asyncio.get_running_loop().time() <= 20
        await sup.replace("rotation")
        assert sup._new_run.call_count == 2
        await sup.shutdown()
    asyncio.run(check())


def test_failed_replacement_resets_pending_retry_to_full_delay():
    async def check():
        sup = supervisor()
        sup.active = replacement()
        previous = None

        def new_run():
            nonlocal previous
            sup.request_replace("resync")
            previous = sup._replace_pending
            return replacement(failed=True)

        sup._new_run = new_run
        await sup.replace("resync")
        assert previous.cancelled()
        assert sup._replace_pending is not previous
        assert 29 < sup._replace_pending.when() - asyncio.get_running_loop().time() <= 30
        await sup.shutdown()
    asyncio.run(check())


def test_rotation_ready_timeout_still_replaces():
    async def check():
        sup = supervisor()
        old = sup.active = replacement()
        new = replacement(ready=False)
        sup._new_run = Mock(return_value=new)
        await sup.replace("rotation")
        assert sup.active is new
        old.stop.assert_awaited_once_with("rotation")
    asyncio.run(check())


@pytest.mark.parametrize("remaining,lost,clean", [(0, False, True), (1, False, False), (0, True, False)])
def test_shutdown_requires_flush_and_no_loss(monkeypatch, caplog, remaining, lost, clean):
    async def check():
        sink = Sink()
        sink.remaining = remaining
        if lost:
            sink.lost.add("lost")
        session = SimpleNamespace(id="test", crash_detail=lambda: None, heartbeat=sink.run, stop=Mock())
        monkeypatch.setattr(runner, "connections", lambda *_: [])
        stop = asyncio.Event()
        stop.set()
        stats = await runner.run_exchange("test", None, sink, {}, session, stop)
        assert stats.clean_shutdown is clean
        assert session.stop.call_count == int(clean)
    asyncio.run(check())
    if not clean:
        assert "shutdown" in caplog.text


def test_cli_exits_nonzero_for_unclean_shutdown(monkeypatch):
    monkeypatch.setattr(cli, "_run", AsyncMock(return_value=1))
    monkeypatch.setattr("sys.argv", ["collectors", "binance"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1


def test_loss_watch_retries_rejected_drop(monkeypatch):
    async def check():
        sup = supervisor()
        stream = sup.spec.streams[0]
        sup.active = ConnRun(sup)
        sup.request_replace = Mock()
        sup.sink.lost.add(stream.key)
        sup.sink.reject = True
        ticks = 0

        async def tick(_):
            nonlocal ticks
            ticks += 1
            if ticks == 2:
                assert stream.key in sup.sink.lost
                assert not drops(sup.sink)
            if ticks == 3:
                raise asyncio.CancelledError

        monkeypatch.setattr(runner.asyncio, "sleep", tick)
        with pytest.raises(asyncio.CancelledError):
            await sup._loss_watch()
        assert not sup.sink.lost
        assert len(drops(sup.sink)) == 1
        assert drops(sup.sink)[0]["reason"] == "kafka_unavailable"
        assert sup.active.control(stream, "stop") is True
    asyncio.run(check())


@pytest.mark.parametrize("phase", ["prepare", "connect", "before_start"])
def test_stop_cancels_connection_before_websocket(monkeypatch, phase):
    async def check():
        entered = asyncio.Event()

        class SleepingHandler(Handler):
            async def prepare(self):
                if phase == "prepare":
                    entered.set()
                    await asyncio.Event().wait()
                return self.spec.url

            on_frame = Mock()

        class Connecting:
            async def __aenter__(self):
                entered.set()
                await asyncio.Event().wait()

            async def __aexit__(self, *args):
                pass

        monkeypatch.setattr(runner, "connect", lambda *a, **kw: Connecting())
        sup = supervisor(SleepingHandler)
        run = sup._new_run()
        if phase != "before_start":
            await entered.wait()
        await asyncio.wait_for(run.stop("shutdown"), 0.2)
        assert run.finished.is_set()
        assert run.task.done()
        run.handler.on_frame.assert_not_called()
    asyncio.run(check())


@pytest.mark.parametrize("stuck", [False, True])
def test_stop_closes_websocket_and_bounds_wait(monkeypatch, stuck):
    async def check():
        opened, closed = asyncio.Event(), asyncio.Event()

        class OpenHandler(Handler):
            async def on_open(self, run):
                run.start_stream(run.spec.streams[0])
                opened.set()

            on_frame = Mock()

        async def recv():
            await closed.wait()
            raise RuntimeError("closed")

        async def close():
            if not stuck:
                closed.set()

        ws = SimpleNamespace(recv=recv, close=AsyncMock(side_effect=close))

        class Connection:
            async def __aenter__(self):
                return ws

            async def __aexit__(self, *args):
                return False

        wait_for = asyncio.wait_for

        async def bounded_wait(awaitable, timeout):
            assert timeout == 10
            return await wait_for(awaitable, 0.01)

        monkeypatch.setattr(runner, "connect", lambda *a, **kw: Connection())
        monkeypatch.setattr(runner.asyncio, "wait_for", bounded_wait)
        sup = supervisor(OpenHandler)
        run = sup._new_run()
        await opened.wait()
        await wait_for(run.stop("shutdown"), 0.2)
        ws.close.assert_awaited_once()
        assert run.finished.is_set()
        assert run.task.cancelled() is stuck
        assert [r["event"] for r in sup.sink.records] == ["start", "stop"]
        run.handler.on_frame.assert_not_called()
    asyncio.run(check())


def test_nonbook_readiness_requires_first_data():
    sup = supervisor(feed="trades")
    run = ConnRun(sup)
    stream = sup.spec.streams[0]
    run.start_stream(stream, ack={"success": True})
    assert not run.ready.is_set()
    run.emit(stream, "trade", "{}", 123)
    assert run.ready.is_set()


def test_book_readiness_requires_validated_snapshot():
    run = ConnRun(supervisor())
    stream = run.spec.streams[0]
    run.emit(stream, "snapshot", "{}", 123)
    assert not run.ready.is_set()
    run.stream_synced(stream)
    assert run.ready.is_set()


@pytest.mark.parametrize("handler,event", [
    (OkxHandler, {"event": "error", "code": "60012", "msg": "invalid request"}),
    (KrakenSpotHandler, {"method": "subscribe", "success": False, "error": "invalid symbol"}),
    (KrakenFuturesHandler, {"event": "alert", "message": "subscription failed"}),
    (KrakenFuturesHandler, {"event": "error", "message": "subscription failed"}),
    (KrakenFuturesHandler, {"event": "subscribed_failed", "message": "subscription failed"}),
])
def test_subscription_rejection_ends_connection(monkeypatch, handler, event):
    async def check():
        sup = supervisor(handler)
        run = ConnRun(sup)
        stream = sup.spec.streams[0]
        run.handler.prepare = AsyncMock(return_value="ws://unused")
        run.handler.on_open = AsyncMock(side_effect=lambda r: r.start_stream(stream))
        child = asyncio.create_task(asyncio.Event().wait())
        run.handler.tasks.append(child)
        async def recv():
            await asyncio.sleep(0)
            return orjson.dumps(event).decode()

        ws = SimpleNamespace(recv=AsyncMock(side_effect=recv))

        class Connection:
            async def __aenter__(self):
                return ws

            async def __aexit__(self, *args):
                return False

        monkeypatch.setattr(runner, "connect", lambda *a, **kw: Connection())
        await asyncio.wait_for(run.run(), 0.2)
        await asyncio.gather(child, return_exceptions=True)
        assert child.cancelled()
        assert run.finished.is_set()
        assert run.error is not None
        assert len(drops(sup.sink)) == 1
        assert drops(sup.sink)[0]["reason"] == "disconnect"
        ws.recv.assert_awaited_once()
    asyncio.run(check())


@pytest.mark.parametrize("handler,event", [
    (OkxHandler, {"event": "notice", "msg": "maintenance"}),
    (KrakenSpotHandler, {"channel": "status", "data": []}),
    (KrakenFuturesHandler, {"event": "info", "version": 1}),
])
def test_informational_events_do_not_raise(handler, event):
    run = ConnRun(supervisor(handler))
    run.handler.on_frame(run, orjson.dumps(event).decode(), 123)
    assert not run.started


def test_lag_probe_and_stats_interval_reset(monkeypatch, caplog):
    async def check():
        stats = Stats()
        clock = SimpleNamespace(now=0.0)
        ticks = 0

        async def tick(delay):
            nonlocal ticks
            ticks += 1
            if ticks == 3:
                raise asyncio.CancelledError
            clock.now += delay + (0.025 if ticks == 1 else 0.010)

        monkeypatch.setattr(runner.time, "monotonic", lambda: clock.now)
        monkeypatch.setattr(runner.asyncio, "sleep", tick)
        with pytest.raises(asyncio.CancelledError):
            await stats.lag_probe()
        assert stats.lag_max_ms == pytest.approx(25)
        ticks = 0
        with caplog.at_level(logging.INFO):
            with pytest.raises(asyncio.CancelledError):
                await stats.report("test", Sink())
        assert "lag_max_ms=25.0" in caplog.text
        assert "lag_max_ms=0.0" in caplog.text
        assert stats.lag_max_ms == 0
    asyncio.run(check())




def test_process_receive_clock_never_goes_backwards(monkeypatch):
    from collectors.common import runner
    monkeypatch.setattr(runner, "_last_recv_ns", 0)
    clock = iter([100, 90, 90, 120])
    monkeypatch.setattr(runner.time, "time_ns", lambda: next(clock))
    assert [runner.stamp() for _ in range(4)] == [100, 100, 100, 120]
    assert runner.stamp(110) == 120  # buffered frames share the same clamp


def test_envelope_stamp_shared_between_connections_and_controls(monkeypatch):
    monkeypatch.setattr(runner, '_last_recv_ns', 0)
    monkeypatch.setattr(runner.time, 'time_ns', lambda: 100)
    sup = supervisor(feed='trades')
    a, b = ConnRun(sup), ConnRun(sup)
    stream = sup.spec.streams[0]
    a.emit(stream, 'trade', '{}', 90)  # auto-start at 100 precedes this older frame
    b.emit(stream, 'trade', '{}', 80)
    a.control(stream, 'stop', 'rotation')
    assert [r['recv_ns'] for r in sup.sink.records] == [100] * 5


@pytest.mark.parametrize("market", ["spot", "perp"])
def test_binance_periodic_snapshots_leave_synced_book_unchanged(monkeypatch, market):
    from collectors.binance import PerpDepthSync
    async def check():
        sup = supervisor(BinanceHandler)
        sup.spec.market = market
        sup.request_replace = Mock()
        run = ConnRun(sup)
        stream = sup.spec.streams[0]
        sync = PerpDepthSync() if market == "perp" else SpotDepthSync()
        sync.on_snapshot(100)
        if market == "perp":
            assert sync.on_delta(100, 105, 99) == "applied"
        else:
            assert sync.on_delta(101, 105) == "applied"
        run.handler.sync[stream.key] = sync
        before = dict(vars(sync))
        response = SimpleNamespace(status_code=200, text='{"lastUpdateId":9999}', content=b'{"lastUpdateId":9999}')
        run.handler.http = SimpleNamespace(get=AsyncMock(return_value=response))
        monkeypatch.setattr(runner.asyncio, "sleep", AsyncMock())
        await run.handler._snapshot(run, stream, periodic=True)
        assert vars(sync) == before
        records = [r for r in sup.sink.records if r["kind"] == "snapshot"]
        assert len(records) == 1 and records[0]["event"] == "periodic"
        assert not drops(sup.sink)
        result = sync.on_delta(106, 110, 105) if market == "perp" else sync.on_delta(106, 110)
        assert result == "applied"
        sup.request_replace.assert_not_called()
    asyncio.run(check())


def test_binance_periodic_snapshot_schedule(monkeypatch):
    async def check():
        sup = supervisor(BinanceHandler)
        run = ConnRun(sup)
        run.handler._snapshot = AsyncMock(side_effect=asyncio.CancelledError)
        sleep = AsyncMock()
        monkeypatch.setattr(runner.asyncio, "sleep", sleep)
        with pytest.raises(asyncio.CancelledError):
            await run.handler._periodic_snapshots(run, sup.spec.streams[0])
        sleep.assert_awaited_once_with(420)
        run.handler._snapshot.assert_awaited_once_with(run, sup.spec.streams[0], periodic=True)
    asyncio.run(check())
