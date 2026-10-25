"""Recorder day-close and crash-recovery rules, driven without Kafka."""

import hashlib
import logging
from datetime import datetime, timezone

import orjson
import pytest

from archiver import record_raw, rawfmt
from archiver.record_raw import OPEN, Recorder, stream_path, utc_date, write_meta
from collectors.common.config import all_streams, load_config
from collectors.common.envelope import control_record, data_record

STREAM = next(s for s in all_streams(load_config()) if s.exchange == "kraken" and s.feed == "l2" and s.asset == "BTC")
KEY = (STREAM.topic, STREAM.partition)


def ns(day: int, hour: int = 12) -> int:
    return int(datetime(2026, 10, day, hour, tzinfo=timezone.utc).timestamp() * 1e9)


def feed(rec: Recorder, offset: int, recv_ns: int) -> None:
    rec.handle(STREAM.topic, STREAM.partition, offset, data_record(STREAM, "update", "c", offset, recv_ns, "{}"))


def offsets(path) -> list[int]:
    return [orjson.loads(line)["offset"] for line in path.read_bytes().splitlines()
            if orjson.loads(line).get("marker") != "rollover"]


def test_day_closes_on_first_next_day_record(tmp_path):
    rec = Recorder(tmp_path, {KEY: STREAM})
    rec.recover()
    feed(rec, 0, ns(17, 23))
    feed(rec, 1, ns(17, 23))
    d17 = stream_path(tmp_path, STREAM, "2026-10-17")
    assert d17.with_name(d17.name + OPEN).exists() and not d17.exists()
    feed(rec, 2, ns(18, 0))
    feed(rec, 3, ns(17, 23))  # clock stepped back: stays in the current (18th) file
    feed(rec, 4, ns(19, 0))
    rec.sync()
    rec.shutdown()
    assert offsets(d17) == [0, 1]
    assert orjson.loads(d17.read_bytes().splitlines()[-1])["new_period"] == "20261018"
    d18 = stream_path(tmp_path, STREAM, "2026-10-18")
    assert offsets(d18) == [2, 3]
    meta = orjson.loads(d17.with_name(d17.name + ".meta.json").read_bytes())
    assert (meta["lines"], meta["first_offset"], meta["last_offset"]) == (3, 0, 1)
    assert meta["sha256"] == hashlib.sha256(d17.read_bytes()).hexdigest()
    assert meta["offset_gaps"] == []
    assert meta["foreign_day_records"] == 0
    meta18 = orjson.loads(d18.with_name(d18.name + ".meta.json").read_bytes())
    assert meta18["offset_gaps"] == []
    assert meta18["foreign_day_records"] == 1


def test_crash_recovery_truncates_partial_line_and_resumes(tmp_path):
    rec = Recorder(tmp_path, {KEY: STREAM})
    rec.recover()
    for offset in range(5):
        feed(rec, offset, ns(17))
    rec.sync()
    rec.shutdown()
    path = stream_path(tmp_path, STREAM, "2026-10-17")
    open_path = path.with_name(path.name + OPEN)
    with open(open_path, "ab") as f:
        f.write(b'{"v":1,"half written')  # crash mid-write

    rec2 = Recorder(tmp_path, {KEY: STREAM})
    assert rec2.recover()[KEY] == 5
    feed(rec2, 5, ns(17))
    feed(rec2, 6, ns(18))  # the reopened day still closes on the next-day record
    rec2.sync()
    rec2.shutdown()
    assert offsets(path) == [0, 1, 2, 3, 4, 5]


def test_recovery_after_close_before_next_day_write(tmp_path):
    rec = Recorder(tmp_path, {KEY: STREAM})
    rec.recover()
    feed(rec, 0, ns(17))
    rec.writers[KEY].close_day()  # crash right after closing the 17th, before the 18th record landed
    rec.shutdown()
    path = stream_path(tmp_path, STREAM, "2026-10-17")
    path.with_name(path.name + ".meta.json").unlink()  # meta not written either

    rec2 = Recorder(tmp_path, {KEY: STREAM})
    assert rec2.recover()[KEY] == 1
    assert path.with_name(path.name + ".meta.json").exists()


def test_new_stream_has_no_resume_offset(tmp_path):
    assert Recorder(tmp_path, {KEY: STREAM}).recover()[KEY] is None


@pytest.mark.parametrize("recv_ns,date", [
    (1792281599999999999, "2026-10-17"),
    (1792281600000000000, "2026-10-18"),
])
def test_utc_date_exact_midnight(recv_ns, date):
    assert utc_date(recv_ns) == date


@pytest.mark.parametrize("control", [False, True])
def test_handle_uses_envelope_recv_ns(tmp_path, control):
    text = '{"recv_ns": 1}'
    if control:
        value = control_record(STREAM, "start", "c", 0, ns(17), detail={"text": text, "recv_ns": 1})
    else:
        value = data_record(STREAM, "snapshot", "c", 0, ns(17), text)
    rec = Recorder(tmp_path, {KEY: STREAM})
    rec.handle(*KEY, 0, value)
    rec.shutdown()
    path = stream_path(tmp_path, STREAM, "2026-10-17")
    line = path.with_name(path.name + OPEN).read_bytes()
    assert line == rawfmt.line(value, 0)
    assert orjson.loads(line)["recv_time_ns"] == ns(17)



def test_meta_derives_integrity_from_file(tmp_path):
    path = stream_path(tmp_path, STREAM, "2026-10-17")
    path.parent.mkdir(parents=True)
    records = [(7, ns(17, 22)), (9, ns(17)), (9, ns(16)), (5, ns(17, 1))]
    lines = []
    for offset, recv_ns in records:
        value = data_record(STREAM, "update", "c", offset, recv_ns, '{"offset":999,"recv_ns":1}')
        lines.append(rawfmt.line(value, offset))
    content = b"".join(lines)
    path.write_bytes(content)
    write_meta(path)
    meta = orjson.loads(path.with_name(path.name + ".meta.json").read_bytes())
    assert meta == {
        "format": "v3", "file": path.name, "lines": 4, "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
        "first_offset": 7, "last_offset": 5, "first_recv_ns": ns(17, 22), "last_recv_ns": ns(17, 1),
        "offset_gaps": [[8, 9], [10, 9], [10, 5]], "foreign_day_records": 1,
    }


def test_meta_handles_snapshot_longer_than_block(tmp_path):
    rec = Recorder(tmp_path, {KEY: STREAM})
    payload = '{"recv_ns":1,"offset":999,"blob":"' + "x" * (2 << 20) + '"}'
    value = data_record(STREAM, "snapshot", "c", 0, ns(17), payload)
    rec.handle(*KEY, 0, value)
    feed(rec, 1, ns(18))
    rec.shutdown()
    path = stream_path(tmp_path, STREAM, "2026-10-17")
    meta = orjson.loads(path.with_name(path.name + ".meta.json").read_bytes())
    assert meta["lines"] == 2
    assert meta["bytes"] == path.stat().st_size
    assert meta["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert meta["first_offset"] == meta["last_offset"] == 0
    assert meta["first_recv_ns"] == meta["last_recv_ns"] == ns(17)
    assert meta["offset_gaps"] == []
    assert meta["foreign_day_records"] == 0


def test_meta_reconstructs_gaps_after_restart(tmp_path):
    rec = Recorder(tmp_path, {KEY: STREAM})
    feed(rec, 10, ns(17))
    feed(rec, 12, ns(17))
    rec.shutdown()
    rec2 = Recorder(tmp_path, {KEY: STREAM})
    assert rec2.recover()[KEY] == 13
    feed(rec2, 15, ns(17))
    feed(rec2, 16, ns(18))
    rec2.shutdown()
    path = stream_path(tmp_path, STREAM, "2026-10-17")
    meta = orjson.loads(path.with_name(path.name + ".meta.json").read_bytes())
    assert meta["lines"] == 4
    assert meta["offset_gaps"] == [[11, 12], [13, 15]]
    assert (meta["first_offset"], meta["last_offset"]) == (10, 15)


def test_background_meta_failure_logged_and_recovered(tmp_path, monkeypatch, caplog):
    def fail(path):
        raise OSError("meta failed")

    with monkeypatch.context() as patch:
        patch.setattr(record_raw, "write_meta", fail)
        rec = Recorder(tmp_path, {KEY: STREAM})
        feed(rec, 0, ns(17))
        with caplog.at_level(logging.ERROR, logger="recorder"):
            feed(rec, 1, ns(18))
            rec.shutdown()
    path = stream_path(tmp_path, STREAM, "2026-10-17")
    assert str(path) in caplog.text
    assert "meta failed" in caplog.text
    assert not path.with_name(path.name + ".meta.json").exists()
    rec2 = Recorder(tmp_path, {KEY: STREAM})
    assert rec2.recover()[KEY] == 2
    rec2.shutdown()
    meta = orjson.loads(path.with_name(path.name + ".meta.json").read_bytes())
    assert meta["lines"] == 2
    assert meta["offset_gaps"] == []
    assert meta["foreign_day_records"] == 0


def test_clock_stepback_warns_once_per_file(tmp_path, caplog):
    rec = Recorder(tmp_path, {KEY: STREAM})
    with caplog.at_level(logging.WARNING, logger="recorder"):
        feed(rec, 0, ns(18))
        feed(rec, 1, ns(17))
        feed(rec, 2, ns(17))
        feed(rec, 3, ns(19))
        feed(rec, 4, ns(18))
        feed(rec, 5, ns(18))
        feed(rec, 6, ns(20))
        rec.shutdown()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "clock stepped back" in r.message]
    assert len(warnings) == 2
    for date in ("2026-10-18", "2026-10-19"):
        path = stream_path(tmp_path, STREAM, date)
        meta = orjson.loads(path.with_name(path.name + ".meta.json").read_bytes())
        assert meta["lines"] == 4
        assert meta["foreign_day_records"] == 2


def test_meta_empty_file(tmp_path):
    path = stream_path(tmp_path, STREAM, "2026-10-17")
    path.parent.mkdir(parents=True)
    path.touch()
    write_meta(path)
    meta = orjson.loads(path.with_name(path.name + ".meta.json").read_bytes())
    assert meta == {
        "format": "v3", "file": path.name, "lines": 0, "bytes": 0, "sha256": hashlib.sha256(b"").hexdigest(),
        "offset_gaps": [], "foreign_day_records": 0,
    }


def test_recovery_skips_rollover_and_finds_long_previous_line(tmp_path):
    rec = Recorder(tmp_path, {KEY: STREAM})
    payload = '{"blob":"' + 'x'*(2<<16) + '"}'
    rec.handle(*KEY, 42, data_record(STREAM, 'update', 'c', 1, ns(17), payload))
    rec.writers[KEY].close_day('2026-10-18')
    rec.shutdown()
    path = stream_path(tmp_path, STREAM, '2026-10-17')
    assert orjson.loads(record_raw.last_line(path))['offset'] == 42
    again = Recorder(tmp_path, {KEY: STREAM})
    assert again.recover()[KEY] == 43
    again.shutdown()


def test_empty_crashed_open_file_is_preserved(tmp_path):
    path = stream_path(tmp_path, STREAM, '2026-10-17')
    path.parent.mkdir(parents=True)
    opened = path.with_name(path.name+OPEN)
    opened.touch()
    rec = Recorder(tmp_path, {KEY: STREAM})
    assert rec.recover()[KEY] is None
    assert opened.exists()
    rec.shutdown()


@pytest.mark.parametrize("last_kind", ["update", "snapshot"])
def test_binance_partition_two_file_crash_recovery(tmp_path, last_kind):
    stream = next(s for s in all_streams(load_config()) if s.exchange == "binance" and s.feed == "l2")
    key = stream.topic, stream.partition
    def put(rec, offset, kind, day=17):
        payload = '{"lastUpdateId":100,"bids":[],"asks":[]}' if kind == "snapshot" else '{"stream":"btcusdt@depth","data":{"U":101,"u":102}}'
        rec.handle(*key, offset, data_record(stream, kind, "c", offset, ns(day), payload))
    rec = Recorder(tmp_path, {key: stream})
    rec.recover()
    for offset, kind in enumerate(["update", "snapshot", "update", last_kind]):
        put(rec, offset, kind)
    rec.shutdown()
    # The interrupted fifth write is in the other file: recovering just one
    # file would replay already persisted Kafka offsets from its sibling.
    partial_kind = "snapshot" if last_kind == "update" else "update"
    target = stream_path(tmp_path, stream, "2026-10-17", "snapshot" if partial_kind == "snapshot" else None)
    with target.with_name(target.name + OPEN).open("ab") as f:
        f.write(b'{"channel":"partial"')
    again = Recorder(tmp_path, {key: stream})
    assert again.recover()[key] == 4
    put(again, 4, partial_kind)
    put(again, 5, "update", 18)
    again.shutdown()
    paths = [stream_path(tmp_path, stream, "2026-10-17", kind) for kind in (None, "snapshot")]
    assert sorted(offset for path in paths for offset in offsets(path)) == list(range(5))
    for path in paths:
        rows = path.read_bytes().splitlines()
        assert orjson.loads(rows[-1])["marker"] == "rollover"
        meta = orjson.loads(path.with_name(path.name + ".meta.json").read_bytes())
        assert meta["format"] == "v3" and "shared by depth and snapshot" in meta["offset_gaps_scope"]


@pytest.mark.parametrize('renamed', [False, True])
def test_binance_recovery_completes_interrupted_two_file_close(tmp_path, renamed):
    stream = next(s for s in all_streams(load_config()) if s.exchange == 'binance' and s.feed == 'l2')
    key = stream.topic, stream.partition
    for offset, kind in enumerate(['update', 'snapshot']):
        final = stream_path(tmp_path, stream, '2026-10-17', 'snapshot' if kind == 'snapshot' else None)
        path = final if renamed and kind == 'update' else final.with_name(final.name + OPEN)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = rawfmt.line(data_record(stream, kind, 'c', offset, ns(17), '{}'), offset)
        path.write_bytes(line + (rawfmt.rollover('20261018') if kind == 'update' else b''))
    root = tmp_path
    again = Recorder(root, {key:stream})
    assert again.recover()[key] == 2
    again.handle(*key, 2, data_record(stream, 'update', 'c', 2, ns(18), '{}'))
    again.shutdown()
    paths = [stream_path(root, stream, '2026-10-17', kind) for kind in (None,'snapshot')]
    assert sorted(o for p in paths for o in offsets(p)) == [0,1]
    assert all(orjson.loads(p.read_bytes().splitlines()[-1])['marker'] == 'rollover' for p in paths)


@pytest.mark.parametrize("first_kind", ["update", "snapshot"])
def test_binance_file_switch_flushes_previous_buffer_before_write(tmp_path, first_kind):
    from unittest.mock import Mock
    stream = next(s for s in all_streams(load_config()) if s.exchange == "binance" and s.feed == "l2")
    key = stream.topic, stream.partition
    rec = Recorder(tmp_path, {key: stream})
    writer = rec.writers[key]
    writer.open("2026-10-17")
    first = stream_path(tmp_path, stream, "2026-10-17", "snapshot" if first_kind == "snapshot" else None)
    second_kind = "snapshot" if first_kind == "update" else "update"
    second = stream_path(tmp_path, stream, "2026-10-17", "snapshot" if second_kind == "snapshot" else None)
    prior = writer.files[first] = Mock(wraps=writer.files[first])
    destination = writer.files[second]
    first_open = first.with_name(first.name + OPEN)
    try:
        for offset in range(2):
            rec.handle(*key, offset, data_record(stream, first_kind, "c", offset, ns(17), "{}"))
        # Same-file lines remain buffered; no per-line flush or write syscall.
        assert first_open.read_bytes() == b""
        prior.flush.assert_not_called()

        def write(line):
            # This assertion runs BEFORE writing to the new destination.
            prior.flush.assert_called_once_with()
            assert offsets(first_open) == [0, 1]
            return destination.write(line)

        writer.files[second] = Mock(wraps=destination)
        writer.files[second].write.side_effect = write
        rec.handle(*key, 2, data_record(stream, second_kind, "c", 2, ns(17), "{}"))
        writer.files[second].write.assert_called_once()
        assert second.with_name(second.name + OPEN).read_bytes() == b""
    finally:
        rec.shutdown()


def test_binance_close_flushes_last_buffer_before_any_rollover(tmp_path):
    from unittest.mock import Mock
    stream = next(s for s in all_streams(load_config()) if s.exchange == "binance" and s.feed == "l2")
    key = stream.topic, stream.partition
    rec = Recorder(tmp_path, {key: stream})
    rec.handle(*key, 0, data_record(stream, "update", "c", 0, ns(17), "{}"))
    rec.handle(*key, 1, data_record(stream, "snapshot", "c", 1, ns(17), "{}"))
    snapshot = stream_path(tmp_path, stream, "2026-10-17", "snapshot")
    snapshot_open = snapshot.with_name(snapshot.name + OPEN)
    depth = stream_path(tmp_path, stream, "2026-10-17")
    writer = rec.writers[key]
    destination = writer.files[depth]
    assert snapshot_open.read_bytes() == b""

    def write(marker):
        # A rollover must never survive while an earlier offset stays buffered.
        assert offsets(snapshot_open) == [1]
        return destination.write(marker)

    writer.files[depth] = Mock(wraps=destination)
    writer.files[depth].write.side_effect = write
    try:
        writer.close_day("20261018")
    finally:
        rec.shutdown()
