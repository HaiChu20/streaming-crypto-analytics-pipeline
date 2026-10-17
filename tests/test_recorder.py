"""Recorder day-close and crash-recovery rules (plan.md §5.4, §7), driven without Kafka."""

import hashlib
from datetime import datetime, timezone

import orjson

from archiver.record_raw import OPEN, Recorder, stream_path
from collectors.common.config import all_streams, load_config
from collectors.common.envelope import data_record

STREAM = next(s for s in all_streams(load_config()) if s.exchange == "kraken" and s.feed == "l2" and s.asset == "BTC")
KEY = (STREAM.topic, STREAM.partition)


def ns(day: int, hour: int = 12) -> int:
    return int(datetime(2026, 10, day, hour, tzinfo=timezone.utc).timestamp() * 1e9)


def feed(rec: Recorder, offset: int, recv_ns: int) -> None:
    rec.handle(STREAM.topic, STREAM.partition, offset, data_record(STREAM, "update", "c", offset, recv_ns, "{}"))


def offsets(path) -> list[int]:
    return [orjson.loads(line)["offset"] for line in path.read_bytes().splitlines()]


def test_day_closes_on_first_next_day_record(tmp_path):
    rec = Recorder(tmp_path, {KEY: STREAM})
    rec.recover()
    feed(rec, 0, ns(17, 23))
    feed(rec, 1, ns(17, 23))
    d17 = stream_path(tmp_path, STREAM, "2026-10-17")
    assert d17.with_name(d17.name + OPEN).exists() and not d17.exists()
    feed(rec, 2, ns(18, 0))
    feed(rec, 3, ns(17, 23))  # clock stepped back: stays in the current (18th) file
    rec.sync()
    rec.shutdown()
    assert offsets(d17) == [0, 1]
    d18 = stream_path(tmp_path, STREAM, "2026-10-18")
    assert offsets(d18.with_name(d18.name + OPEN)) == [2, 3]
    meta = orjson.loads(d17.with_name(d17.name + ".meta.json").read_bytes())
    assert (meta["lines"], meta["first_offset"], meta["last_offset"]) == (2, 0, 1)
    assert meta["sha256"] == hashlib.sha256(d17.read_bytes()).hexdigest()


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
