"""Public Coinbase/Bybit routing, continuity, archival and live fixture replay.

Coinbase snapshot fixtures retain only the best 50 bid/offer levels; all live
update payloads are verbatim. See fixtures/coinbase/README.md for capture details.
"""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import orjson
import pytest

from archiver import rawfmt
from collectors.__main__ import HANDLERS
from collectors.bybit import BookSeq, BybitHandler
from collectors.coinbase import CoinbaseHandler
from collectors.common.config import all_streams, connections, load_config
from collectors.common.runner import ConnRun, Stats, Supervisor
from scripts import check_raw, create_topics
from scripts.convert_raw import convert_tree

FIXTURES = Path(__file__).parent / "fixtures"


def make_run(exchange, market="spot"):
    spec = next(s for s in connections(load_config(), exchange) if s.market == market)
    records = []
    sink = SimpleNamespace(send=lambda stream, value: records.append(orjson.loads(value)) or True)
    sup = Supervisor(spec, HANDLERS[exchange](spec), sink, Stats(), "test", None, None,
                     {"ready_timeout_s": .01, "stagger_s": {}, "max_age_h": {}})
    sup.request_replace = Mock()
    run = ConnRun(sup)
    run.ws = SimpleNamespace(send=AsyncMock())
    return run, records


def frame(run, msg):
    text = orjson.dumps(msg).decode()
    run.handler.on_frame(run, text, 123)
    return text


def cb(seq, channel="l2_data", symbol="BTC-USD", kind="update"):
    return {"channel": channel, "sequence_num": seq,
            "events": [{"type": kind, "product_id": symbol, "updates": []}]}


def bb(u, seq, kind="delta", depth=200):
    return {"topic": f"orderbook.{depth}.BTCUSDT", "type": kind,
            "data": {"s": "BTCUSDT", "u": u, "seq": seq, "a": [], "b": []}}


def test_coinbase_connection_sequence_includes_control_frames_and_all_products():
    run, records = make_run("coinbase")
    assert isinstance(run.handler, CoinbaseHandler)
    frame(run, {"channel": "subscriptions", "sequence_num": 0,
                "events": [{"subscriptions": {"level2": ["BTC-USD", "ETH-USD"], "market_trades": ["BTC-USD"]}}]})
    text = frame(run, cb(1, kind="snapshot"))
    frame(run, cb(2, symbol="ETH-USD", kind="snapshot"))
    frame(run, {"channel": "heartbeats", "sequence_num": 3, "events": []})
    frame(run, cb(4))
    assert not run.broken
    assert len(run.synced) == 2
    assert any(r.get("payload") == text and r["kind"] == "snapshot" for r in records)
    assert records[0]["detail"]["ack"]["channel"] == "subscriptions"
    frame(run, {"channel": "heartbeats", "sequence_num": 6, "events": []})
    drops = [r for r in records if r.get("event") == "drop"]
    assert {r["feed"] for r in drops} == {"l2", "trades"}
    assert len(drops) == len(run.spec.streams)
    assert all(r["detail"] == {"last_sequence_num": 4, "sequence_num": 6} for r in drops)
    assert all(call.args == ("resync",) for call in run.sup.request_replace.call_args_list)
    before = len(drops)
    frame(run, cb(10))
    assert len([r for r in records if r.get("event") == "drop"]) == before


def test_coinbase_multi_product_frame_routes_once_per_stream_without_rewriting():
    run, records = make_run("coinbase")
    msg = {"channel": "market_trades", "sequence_num": 0,
           "events": [{"type": "snapshot", "trades": [
               {"product_id": "BTC-USD", "trade_id": "1"},
               {"product_id": "ETH-USD", "trade_id": "2"}]}]}
    text = frame(run, msg)
    data = [r for r in records if "payload" in r]
    assert len(data) == 2 and all(r["payload"] == text for r in data)
    for stream in run.spec.streams:
        if stream.feed == "trades":
            assert check_raw.trade_ids(stream, msg) == (["1"] if stream.asset == "BTC" else ["2"])


def test_coinbase_missing_snapshot_and_subscription_error():
    run, _ = make_run("coinbase")
    frame(run, cb(0))
    assert run.broken
    with pytest.raises(RuntimeError, match="subscription rejected"):
        frame(run, {"type": "error", "message": "unauthorized"})


def test_public_subscriptions_and_bybit_ack_batches_and_ping(monkeypatch):
    async def check():
        run, _ = make_run("coinbase")
        await run.handler.on_open(run)
        requests = [orjson.loads(call.args[0]) for call in run.ws.send.call_args_list]
        assert [r["channel"] for r in requests] == ["heartbeats", "level2", "market_trades"]
        assert all("jwt" not in r for r in requests)
        run, records = make_run("bybit", "perp")
        # Exercise spot's 10-arg batching with more than one request.
        run.handler.by_topic["extra"] = run.spec.streams[0]
        await run.handler.on_open(run)
        requests = [orjson.loads(call.args[0]) for call in run.ws.send.call_args_list]
        assert [len(r["args"]) for r in requests] == [10, 1]
        assert all("auth" not in r for r in requests)
        for request in requests:
            frame(run, {"op": "subscribe", "req_id": request["req_id"], "success": True})
        assert len(run.started) == 10 and not run.handler.pending
        assert all(r["detail"]["ack"]["success"] for r in records)
        for task in run.handler.tasks:
            task.cancel()
        await asyncio.gather(*run.handler.tasks, return_exceptions=True)
        run.ws.send.reset_mock()
        sleeps = []
        async def sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) > 1:
                raise asyncio.CancelledError
        monkeypatch.setattr("collectors.bybit.asyncio.sleep", sleep)
        with pytest.raises(asyncio.CancelledError):
            await run.handler._ping(run)
        run.ws.send.assert_awaited_once_with('{"op":"ping"}')
        assert sleeps == [20, 20]
    asyncio.run(check())


@pytest.mark.parametrize("market", ["spot", "perp"])
def test_bybit_snapshot_reset_delta_gap_and_l1_repeat(market):
    run, records = make_run("bybit", market)
    assert isinstance(run.handler, BybitHandler)
    frame(run, bb(10, 100, "snapshot"))
    frame(run, bb(11, 200))  # seq is increasing, not adjacent
    frame(run, bb(1, 1, "snapshot"))  # service restart overwrites
    frame(run, bb(2, 20))
    frame(run, bb(20, 100, "snapshot", 1))
    frame(run, bb(20, 100, "snapshot", 1))
    frame(run, bb(25, 120, "snapshot", 1))  # no adjacency requirement for L1
    assert not run.broken
    frame(run, bb(4, 40))
    drop = next(r for r in records if r.get("event") == "drop")
    assert drop["detail"] == {"last_u": 2, "last_seq": 20, "u": 4, "seq": 40}
    run.sup.request_replace.assert_called_once_with("resync")


def test_bybit_rejects_delta_before_snapshot_seq_regression_and_subscription_failure():
    seq = BookSeq()
    assert not seq.on_update(1, 1)
    seq.on_snapshot(10, 100)
    assert not seq.on_update(11, 100)
    assert not seq.on_update(11, 99)
    run, _ = make_run("bybit")
    frame(run, bb(11, 100))
    assert run.broken
    for msg in [{"op": "subscribe", "success": False}, {"op": "subscribe"}, {"op": "error"}]:
        with pytest.raises(RuntimeError, match="subscription rejected"):
            frame(run, msg)


@pytest.mark.parametrize("exchange,market", [("coinbase", "spot"), ("bybit", "spot"), ("bybit", "perp")])
def test_metadata_is_public_and_preserves_native_filters(exchange, market):
    async def check():
        run, _ = make_run(exchange, market)
        info = {"base_increment": "0.00000001", "quote_increment": "0.01", "base_min_size": "0.00000001",
                "status": "Trading", "contractType": "LinearPerpetual",
                "priceFilter": {"tickSize": "0.1"}, "lotSizeFilter": {"qtyStep": "0.001", "basePrecision": "0.000001", "minOrderQty": "0.001"}}
        def response(request):
            assert "authorization" not in request.headers
            if exchange == "bybit":
                assert request.url.params["category"] == ("linear" if market == "perp" else "spot")
                return httpx.Response(200, json={"retCode": 0, "result": {"list": [info]}})
            assert "/market/products/" in request.url.path
            return httpx.Response(200, json=info)
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
            run.handler.http = http
            assert await run.handler.prepare() == run.spec.url
        for instrument in run.handler.instrument.values():
            assert instrument["status"] == info["status"]
            key = "lotSizeFilter" if exchange == "bybit" else "base_increment"
            assert instrument[key] == info[key]
    asyncio.run(check())


@pytest.mark.parametrize("exchange,market", [("coinbase", "spot"), ("bybit", "spot"), ("bybit", "perp")])
def test_live_fixtures_route_archive_convert_and_check(tmp_path, exchange, market):
    run, _ = make_run(exchange, market)
    checked = 0
    for stream in run.spec.streams:
        path = FIXTURES / exchange / market / f"{stream.feed}.jsonl"
        assert path.exists(), f"missing live fixture: {path}"
        records = [orjson.loads(line) for line in path.read_bytes().splitlines()]
        records = [r for r in records if r["symbol"] == stream.symbol]
        assert records
        tree = tmp_path / stream.asset / stream.feed
        source = tree / f"date=2026-10-24/exchange={exchange}/market={market}/feed={stream.feed}/{stream.symbol}.jsonl"
        source.parent.mkdir(parents=True)
        with source.open("wb") as f:
            for offset, record in enumerate(records):
                f.write(orjson.dumps(dict(record, offset=offset)) + b"\n")
        output = tree / "converted"
        convert_tree(tree, output)
        paths = check_raw.files_for(output, stream, None, raw_root=True)
        assert paths and rawfmt.archive_identity(paths[0])[:3] == (exchange, market, stream.symbol)
        result = check_raw.check_stream(stream, paths, None, True)
        assert not result["errors"], result["errors"]
        for record in records:
            if "payload" not in record:
                continue
            replay, emitted = make_run(exchange, market)
            msg = orjson.loads(record["payload"])
            if exchange == "coinbase":
                replay.handler.last_seq = msg["sequence_num"] - 1  # omitted channels/heartbeats are not fixture feeds
            frame(replay, msg)
            assert any(r.get("payload") == record["payload"] and r["symbol"] == stream.symbol for r in emitted)
            checked += 1
    assert checked > 100


@pytest.mark.parametrize("exchange,feed,folder,file", [
    ("coinbase", "l2", "Coinbase", "book"), ("coinbase", "trades", "Coinbase", "trades"),
    ("bybit", "l1", "Bybit", "bbo"), ("bybit", "l2", "Bybit", "book"),
    ("bybit", "trades", "Bybit", "trades"), ("bybit", "ticker", "Bybit", "ticker"),
    ("bybit", "liquidations", "Bybit", "liquidations"),
])
def test_rawfmt_paths(exchange, feed, folder, file):
    for market, name in [("spot", "Spot"), ("perp", "Futures")]:
        path = rawfmt.path(Path("raw"), exchange, market, "BTC-USD", "2026-10-24", feed)
        assert path == Path(f"raw/{folder}/{name}/BTC-USD/20261024_{file}.jsonl")
        assert rawfmt.archive_identity(path) == (exchange, market, "BTC-USD", file)


def test_topic_creation_uses_new_configured_feeds(monkeypatch):
    created = []
    class Admin:
        def __init__(self, config):
            pass
        def list_topics(self, timeout):
            return SimpleNamespace(topics={})
        def create_topics(self, topics):
            created.extend(topics)
            return {t.topic: SimpleNamespace(result=lambda: None) for t in topics}
    monkeypatch.setattr(create_topics, "AdminClient", Admin)
    monkeypatch.setattr("sys.argv", ["create_topics"])
    create_topics.main()
    expected = {s.topic for s in all_streams(load_config()) if s.exchange in ("coinbase", "bybit")}
    assert len(expected) == 10
    assert {t.topic for t in created if t.topic in expected} == expected
    assert all(t.num_partitions == 2 for t in created if t.topic in expected)


def test_checker_coinbase_snapshot_and_increasing_sequence():
    run, _ = make_run("coinbase")
    stream = run.spec.streams[0]
    check = check_raw.BookCheck(stream, None)
    assert check.feed("update", cb(1)) == "book update before first snapshot"
    check = check_raw.BookCheck(stream, None)
    assert check.feed("snapshot", cb(1, kind="snapshot")) is None
    assert check.feed("update", cb(100)) is None  # other connection channels used 2..99
    assert "regression" in check.feed("update", cb(100))


@pytest.mark.parametrize("exchange", ["coinbase", "bybit"])
def test_checker_trade_uniqueness_in_v3(tmp_path, exchange):
    run, _ = make_run(exchange)
    stream = next(s for s in run.spec.streams if s.feed == "trades")
    msg = ({"channel": "market_trades", "sequence_num": 10,
            "events": [{"type": "update", "trades": [{"product_id": stream.symbol, "trade_id": "id"}]}]}
           if exchange == "coinbase" else {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "data": [{"s": stream.symbol, "i": "id"}]})
    record = dict(exchange=exchange, market="spot", feed="trades", symbol=stream.symbol,
                  kind="trade", recv_ns=1, conn="c", payload=orjson.dumps(msg).decode())
    path = tmp_path / "20261024_trades.jsonl"
    path.write_bytes(rawfmt.line(record, 0) + rawfmt.line(record, 1))
    result = check_raw.check_stream(stream, [path], None, True)
    assert result["duplicate_trades"] == 1 and len(result["errors"]) == 1


@pytest.mark.parametrize("market", ["spot", "perp"])
def test_bybit_gap_from_mutated_live_fixture(tmp_path, market):
    run, records = make_run("bybit", market)
    source = FIXTURES / "bybit" / market / "l2.jsonl"
    data = [orjson.loads(line) for line in source.read_bytes().splitlines()]
    data = [r for r in data if r.get("symbol") == "BTCUSDT" and "payload" in r]
    snapshot = orjson.loads(data[0]["payload"])
    delta = orjson.loads(data[1]["payload"])
    assert snapshot["type"] == "snapshot" and delta["type"] == "delta"
    delta["data"]["u"] += 1  # simulate one missing update
    frame(run, snapshot)
    frame(run, delta)
    drop = next(r for r in records if r.get("event") == "drop")
    assert drop["detail"]["u"] == snapshot["data"]["u"] + 2
    stream = next(s for s in run.spec.streams if s.feed == "l2" and s.asset == "BTC")
    check = check_raw.BookCheck(stream, None)
    assert check.feed("snapshot", snapshot) is None
    assert "update-id break" in check.feed("update", delta)
    # The same bad native frame must be explained by its immediate collector drop.
    selected = [r for r in records if r["feed"] == "l2" and r["symbol"] == stream.symbol]
    path = tmp_path / "20261024_book.jsonl"
    path.write_bytes(b"".join(rawfmt.line(r, i) for i, r in enumerate(selected)))
    result = check_raw.check_stream(stream, [path], None, True)
    assert not result["errors"] and result["events"]["explained_breaks"] == 1


def test_coinbase_gap_from_mutated_live_fixture():
    run, records = make_run("coinbase")
    source = FIXTURES / "coinbase" / "spot" / "l2.jsonl"
    data = [orjson.loads(line) for line in source.read_bytes().splitlines()]
    data = [r for r in data if r.get("symbol") == "BTC-USD" and "payload" in r]
    snapshot = orjson.loads(data[0]["payload"])
    update = orjson.loads(data[1]["payload"])
    update["sequence_num"] = snapshot["sequence_num"] + 2
    frame(run, snapshot)
    frame(run, update)
    assert len([r for r in records if r.get("event") == "drop"]) == len(run.spec.streams)


def test_new_fixture_sizes_and_coinbase_snapshot_trimming():
    assert sum(p.stat().st_size for p in (FIXTURES / "coinbase").rglob("*.*")) < 300_000
    assert all(p.stat().st_size < 300_000 for p in (FIXTURES / "bybit").glob("*/*.jsonl"))
    for line in (FIXTURES / "coinbase/spot/l2.jsonl").read_bytes().splitlines():
        record = orjson.loads(line)
        if record["kind"] == "snapshot":
            for event in orjson.loads(record["payload"])["events"]:
                for side in ("bid", "offer"):
                    assert sum(u["side"] == side for u in event["updates"]) == 50


def test_probe_steady_volume_excludes_initial_snapshot_and_adds_daily_rotation(tmp_path, monkeypatch):
    from collectors.common.sinks import FixtureSink
    from scripts import probe_feeds
    run, records = make_run("bybit")
    frame(run, bb(1, 10, "snapshot"))
    frame(run, bb(2, 100))
    stream = next(s for s in run.spec.streams if s.feed == "l2" and s.asset == "BTC")
    sink = FixtureSink(tmp_path)
    for record in records:
        sink.send(stream, orjson.dumps(record))
    sink.close()
    monkeypatch.setattr(probe_feeds, "all_streams", lambda cfg: [stream])
    result = probe_feeds.summarize(sink, 10, ["bybit"])["streams"][stream.key]
    snapshot = next(r for r in records if r["kind"] == "snapshot")
    update = next(r for r in records if r["kind"] == "update")
    snapshot_size, update_size = len(orjson.dumps(snapshot)), len(orjson.dumps(update))
    assert result["initial_snapshot_bytes"] == snapshot_size
    assert result["steady_bytes_per_s"] == update_size / 10
    assert result["daily_gb_with_rotation"] == round((update_size / 10 * 86400 + snapshot_size) / 1e9, 6)
    assert 0 < result["est_gzip_ratio"] < 1
