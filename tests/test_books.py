"""Continuity and checksum rules: synthetic cases, Kraken's documented examples,
and replays of live fixtures captured by scripts/probe_feeds.py."""

from copy import deepcopy
from pathlib import Path

import orjson
import pytest

from collectors.binance import PerpDepthSync, SpotDepthSync
from collectors.kraken import L2Book, L3Book, L3TimestampError, _decimal_json, _l3_timestamp
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


@pytest.mark.parametrize("delete_qty", ["0", "1.0"])
def test_kraken_l3_events_keep_queue_order_and_truncate(delete_qty):
    book = L3Book(depth=2)
    book.apply(
        {"bids": [{"order_id": "a", "limit_price": "10.0", "order_qty": "1.0", "timestamp": "2026-10-16T00:00:00.000000001Z"},
                  {"order_id": "b", "limit_price": "10.0", "order_qty": "2.0", "timestamp": "2026-10-16T00:00:00.000000002Z"},
                  {"order_id": "c", "limit_price": "9.0", "order_qty": "1.0", "timestamp": "2026-10-16T00:00:00.000000003Z"}], "asks": []},
        snapshot=True,
    )
    book.apply({"bids": [{"event": "modify", "order_id": "a", "limit_price": "10.0", "order_qty": "0.5", "timestamp": "2026-10-16T00:00:00.000000001Z"},
                         {"event": "add", "order_id": "d", "limit_price": "8.0", "order_qty": "1.0", "timestamp": "2026-10-16T00:00:00.000000004Z"}]}, snapshot=False)
    bids = book.sides["bids"]
    assert [str(order[3]) for order in bids[max(bids)]] == ["0.5", "2.0"]  # a keeps its place
    assert len(bids) == 2  # 8.0 truncated (depth 2)
    expected = deepcopy(book)
    expected.apply({"bids": [{"event": "delete", "order_id": "c"}]}, snapshot=False)
    book.apply({"bids": [{"event": "delete", "order_id": "c", "limit_price": "9.0", "order_qty": delete_qty}]}, snapshot=False)
    assert len(bids) == 1
    assert book.sides == expected.sides
    assert book.where == expected.where
    assert book.checksum() == expected.checksum()


def _l3_queue_excerpt():
    """Six original lines from data/Test/BTC-USD/20261016_level3.jsonl.gz: 2, 5, 10, 158, 7887, 7889."""
    with (FIXTURES / "legacy" / "l3_queue_excerpt.jsonlines").open("rb") as source:
        return [orjson.loads(line)["data"][0] for line in source]


def test_kraken_l3_old_timestamp_reentry_and_duplicate_add():
    snapshot, higher_add, reentry, *_ = _l3_queue_excerpt()
    old = next(o for o in snapshot["bids"] if o["order_id"] == "OB5NHP-LBEPT-WNLFSE")
    newer = next(o for o in snapshot["bids"] if o["order_id"] == "OFH6EC-G3ZNS-QV5J7H")
    old_add = next(o for o in reentry["bids"] if o["order_id"] == old["order_id"])
    assert old_add["timestamp"] < newer["timestamp"]

    book = L3Book(depth=1)
    book.apply({"bids": [old, newer]}, snapshot=True)
    book.apply(higher_add, snapshot=False)  # the lower price leaves the depth window
    assert old["order_id"] not in book.where
    book.apply({"bids": [{"event": "delete", "order_id": higher_add["bids"][0]["order_id"]}]}, snapshot=False)
    book.apply({"bids": [dict(newer, event="add")]}, snapshot=False)
    book.apply({"bids": [old_add]}, snapshot=False)  # old timestamp returns after the newer order
    level = book.sides["bids"][next(iter(book.sides["bids"]))]
    assert [entry[2] for entry in level] == [old["order_id"], newer["order_id"]]

    book.apply({"bids": [old_add]}, snapshot=False)  # duplicate add replaces the prior copy
    assert [entry[2] for entry in level] == [old["order_id"], newer["order_id"]]
    book = L3Book(depth=2)
    book.apply({"bids": [old, newer]}, snapshot=True)
    moved = dict(old_add, limit_price="82071.6")
    book.apply({"bids": [moved]}, snapshot=False)
    assert old["order_id"] not in [entry[2] for entry in book.sides["bids"][next(p for p in book.sides["bids"] if str(p) == "82071.5")]]
    assert book.where[old["order_id"]][1] != book.where[newer["order_id"]][1]


def test_kraken_l3_modify_uses_event_timestamp_for_queue_position():
    snapshot, _, _, decrease, initial_increase, increase = _l3_queue_excerpt()
    first, second = snapshot["asks"][:2]
    book = L3Book(depth=100)
    book.apply({"asks": [first, second]}, snapshot=True)
    book.apply(decrease, snapshot=False)
    level = book.sides["asks"][next(iter(book.sides["asks"]))]
    assert [entry[2] for entry in level] == [first["order_id"], second["order_id"]]
    assert str(level[0][3]) == decrease["asks"][0]["order_qty"]

    old = initial_increase["bids"][0]
    raised = increase["bids"][0]
    peer = dict(snapshot["bids"][0], limit_price=old["limit_price"], timestamp="2026-10-16T00:01:23.208000000Z")
    assert old["timestamp"] < peer["timestamp"] < raised["timestamp"]
    book.apply({"bids": [old, peer]}, snapshot=True)
    book.apply(increase, snapshot=False)
    level = book.sides["bids"][next(iter(book.sides["bids"]))]
    assert [entry[2] for entry in level] == [peer["order_id"], old["order_id"]]
    assert str(level[1][3]) == raised["order_qty"]


def test_kraken_l3_equal_timestamps_keep_arrival_order():
    book = L3Book(depth=1)
    book.apply({"bids": [
        {"order_id": "z", "limit_price": "10", "order_qty": "1", "timestamp": "2026-10-16T00:00:00Z"},
        {"order_id": "a", "limit_price": "10", "order_qty": "2", "timestamp": "2026-10-16T00:00:00Z"},
    ]}, snapshot=True)
    level = book.sides["bids"][next(iter(book.sides["bids"]))]
    assert [entry[2] for entry in level] == ["z", "a"]
    book.apply({"bids": [{"event": "modify", "order_id": "z", "limit_price": "10",
                           "order_qty": "0.5", "timestamp": "2026-10-16T00:00:00Z"}]}, snapshot=False)
    assert [entry[2] for entry in level] == ["a", "z"]


def test_kraken_l3_short_timestamp_and_anomaly():
    assert _l3_timestamp("2026-10-16T00:00:00.9Z") > _l3_timestamp("2026-10-16T00:00:00.10Z")
    with pytest.raises(L3TimestampError):
        _l3_timestamp("not a timestamp")


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
