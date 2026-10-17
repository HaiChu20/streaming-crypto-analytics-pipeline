"""Continuity and checksum rules (plan.md §5.2): synthetic cases, Kraken's documented examples,
and replays of live fixtures captured by scripts/probe_feeds.py."""

from pathlib import Path

import orjson
import pytest

from collectors.binance import PerpDepthSync, SpotDepthSync
from collectors.kraken import L2Book, L3Book, _decimal_json
from collectors.okx import BookSeq

FIXTURES = Path(__file__).parent / "fixtures"


# ---- Binance ---------------------------------------------------------------


def test_spot_sync_accepts_overlapping_updates_and_detects_gaps():
    sync = SpotDepthSync()
    for U, u in [(1, 5), (6, 10), (11, 15)]:
        assert sync.on_delta(U, u) == "buffered"
    assert sync.on_snapshot(7) == "synced"  # first kept event (6, 10) contains 8
    assert sync.local_id == 15
    assert sync.on_delta(14, 22) == "applied"  # overlaps 14..15: valid under the range rule
    assert sync.on_delta(10, 12) == "ignored"  # entirely old
    assert sync.on_delta(25, 30) == "gap"  # 23..24 missing


def test_spot_snapshot_older_than_buffer_is_stale():
    sync = SpotDepthSync()
    sync.on_delta(6, 10)
    assert sync.on_snapshot(3) == "stale"
    assert sync.on_delta(11, 12) == "buffered"  # waits for a new snapshot


def test_spot_snapshot_before_any_delta():
    sync = SpotDepthSync()
    assert sync.on_snapshot(100) == "synced"
    assert sync.on_delta(90, 100) == "ignored"
    assert sync.on_delta(95, 105) == "applied"
    assert sync.on_delta(106, 110) == "applied"


def test_perp_sync_uses_pu_chain():
    sync = PerpDepthSync()
    sync.on_delta(1, 4, 0)
    sync.on_delta(5, 9, 4)
    sync.on_delta(10, 12, 9)
    assert sync.on_snapshot(7) == "synced"  # (5, 9) contains 7, then pu 9 == 9
    assert sync.on_delta(13, 15, 12) == "applied"
    assert sync.on_delta(18, 20, 17) == "gap"


# ---- OKX -------------------------------------------------------------------


def test_okx_seq():
    seq = BookSeq()
    assert not seq.on_update(1, 2)  # no snapshot yet
    seq.on_snapshot(10)
    assert seq.on_update(10, 11)
    assert seq.on_update(11, 11)  # heartbeat-style update with an unchanged seqId
    assert not seq.on_update(12, 13)


# ---- Kraken: documented examples -------------------------------------------


def test_kraken_l2_documented_checksum():
    msg = _decimal_json((FIXTURES / "kraken_docs" / "l2_checksum_example.json").read_text())
    book = L2Book(depth=10)
    book.apply(msg["data"][0], snapshot=True)
    assert book.checksum() == 3310070434


def test_kraken_l3_documented_checksum():
    msg = _decimal_json((FIXTURES / "kraken_docs" / "l3_checksum_example.json").read_text())
    book = L3Book(depth=100)
    book.apply(msg["data"][0], snapshot=True)
    assert book.checksum() == 1063832831


def test_kraken_l3_events_keep_queue_order_and_truncate():
    book = L3Book(depth=2)
    book.apply(
        {"bids": [{"order_id": "a", "limit_price": "10.0", "order_qty": "1.0"},
                  {"order_id": "b", "limit_price": "10.0", "order_qty": "2.0"},
                  {"order_id": "c", "limit_price": "9.0", "order_qty": "1.0"}], "asks": []},
        snapshot=True,
    )
    book.apply({"bids": [{"event": "modify", "order_id": "a", "limit_price": "10.0", "order_qty": "0.5"},
                         {"event": "add", "order_id": "d", "limit_price": "8.0", "order_qty": "1.0"}]}, snapshot=False)
    bids = book.sides["bids"]
    assert [str(q) for q in bids[min(bids, key=lambda p: -p)].values()] == ["0.5", "2.0"]  # a keeps its place
    assert len(bids) == 2  # 8.0 truncated (depth 2)
    book.apply({"bids": [{"event": "delete", "order_id": "c", "limit_price": "9.0", "order_qty": "0"}]}, snapshot=False)
    assert len(bids) == 1


# ---- Live fixtures (skipped until the probe has run) -----------------------


def _records(path: Path):
    if not path.exists():
        pytest.skip(f"{path} missing; run scripts/probe_feeds.py")
    for line in path.read_bytes().splitlines():
        yield orjson.loads(line)


def _per_stream(path: Path):
    streams: dict[tuple[str, str], list[dict]] = {}
    for rec in _records(path):
        streams.setdefault((rec["symbol"], rec["conn"]), []).append(rec)
    return streams


@pytest.mark.parametrize("market", ["spot", "perp"])
def test_binance_fixture_depth_is_continuous(market):
    for records in _per_stream(FIXTURES / "binance" / market / "l2.jsonl").values():
        sync = PerpDepthSync() if market == "perp" else SpotDepthSync()
        synced = False
        for rec in records:
            if rec["kind"] == "control":
                continue
            p = orjson.loads(rec["payload"])
            if rec["kind"] == "snapshot":
                synced = sync.on_snapshot(p["lastUpdateId"]) == "synced" or synced
                continue
            d = p["data"]
            result = sync.on_delta(d["U"], d["u"], d["pu"]) if market == "perp" else sync.on_delta(d["U"], d["u"])
            assert result not in ("gap", "stale")
        assert synced


@pytest.mark.parametrize("market", ["spot", "perp"])
def test_okx_fixture_books_are_continuous(market):
    for records in _per_stream(FIXTURES / "okx" / market / "l2.jsonl").values():
        seq = BookSeq()
        for rec in records:
            if rec["kind"] == "control":
                continue
            book = orjson.loads(rec["payload"])["data"][0]
            if rec["kind"] == "snapshot":
                seq.on_snapshot(book["seqId"])
            else:
                assert seq.on_update(book["prevSeqId"], book["seqId"])


def test_kraken_fixture_l2_checksums_match():
    checked = 0
    for records in _per_stream(FIXTURES / "kraken" / "spot" / "l2.jsonl").values():
        book = None
        for rec in records:
            if rec["kind"] == "control":
                continue
            data = _decimal_json(rec["payload"])["data"][0]
            if rec["kind"] == "snapshot":
                book = L2Book(depth=100)
            if book is None:
                continue
            book.apply(data, snapshot=rec["kind"] == "snapshot")
            assert book.checksum() == int(data["checksum"])
            checked += 1
    assert checked > 10


def test_kraken_fixture_futures_seq_is_continuous():
    for records in _per_stream(FIXTURES / "kraken" / "perp" / "l2.jsonl").values():
        last = None
        for rec in records:
            if rec["kind"] == "control":
                continue
            seq = orjson.loads(rec["payload"])["seq"]
            if last is not None and rec["kind"] != "snapshot":
                assert seq == last + 1
            last = seq
