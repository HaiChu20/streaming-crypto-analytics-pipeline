"""Raw recorder: Kafka raw.* topics -> one JSONL file per (topic, partition) per UTC day (plan.md §7).

    uv run python -m archiver.record_raw

- Day = UTC date of the record's recv_ns. A stream's day-D file closes when that stream delivers
  its first day-D+1 record (never force-closed; still-open old days are reported as overdue).
- Files are the source of truth for progress: on restart each stream resumes from the last
  offset in its newest file, so a crash creates no duplicates and no holes.
"""

import argparse
import gzip
import hashlib
import logging
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

import orjson
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition

from archiver import rawfmt
from collectors.common.config import Stream, data_dir, kafka_bootstrap, load_config, stream_index
from collectors.common.lock import single_instance

log = logging.getLogger("recorder")

OPEN = ".open"
SYNC_INTERVAL_S = 2.0


def utc_date(recv_ns: int) -> str:
    return _day_date(recv_ns // 86_400_000_000_000)


@lru_cache(maxsize=32)
def _day_date(day: int) -> str:
    return datetime.fromtimestamp(day * 86400, tz=timezone.utc).strftime("%Y-%m-%d")


def _file_date(path: Path) -> str:
    day = path.name.split("_", 1)[0]
    return f"{day[:4]}-{day[4:6]}-{day[6:8]}"


def stream_path(root: Path, stream: Stream, date: str) -> Path:
    return rawfmt.path(root / "raw", stream.exchange, stream.market, stream.symbol, date, stream.feed)


def last_line(path: Path) -> bytes | None:
    """Last complete line with an offset, skipping rollover markers; bounded memory."""
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        pos, buf = f.tell(), b""
        while pos:
            step = min(1 << 16, pos)
            pos -= step
            f.seek(pos)
            parts = (f.read(step) + buf).split(b"\n")
            buf = parts[0]
            for line in reversed(parts[1:]):
                if line:
                    try:
                        rec = orjson.loads(line)
                    except orjson.JSONDecodeError:
                        rec = rawfmt.loads(line.decode())
                    if "offset" in rec:
                        return line
        if buf:
            try:
                rec = orjson.loads(buf)
            except orjson.JSONDecodeError:
                rec = rawfmt.loads(buf.decode())
            if "offset" in rec:
                return buf
    return None


def repair(path: Path) -> None:
    """Truncate a partially written last line (crash mid-write)."""
    with open(path, "rb+") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        pos, tail = size, b""
        while pos > 0:
            step = min(1 << 16, pos)
            pos -= step
            f.seek(pos)
            tail = f.read(step) + tail
            idx = tail.rfind(b"\n")
            if idx >= 0:
                keep = pos + idx + 1
                break
        else:
            keep = 0
        if keep != size:
            log.warning("%s: truncating %d bytes of a partial last line", path, size - keep)
            f.truncate(keep)


def write_meta(path: Path, *, extra: dict | None = None, exclusive: bool = False) -> None:
    """Write <file>.meta.json for a closed day file: count, offsets, time range, sha256."""
    sha, lines, size = hashlib.sha256(), 0, 0
    first = None
    last_offset = None
    offset_gaps = []
    foreign_day_records = 0
    date = _file_date(path)
    with (gzip.open(path, "rb") if path.suffix == ".gz" else open(path, "rb")) as f:
        for line in f:
            sha.update(line)
            lines += 1
            size += len(line)
            try:
                rec = orjson.loads(line)
            except orjson.JSONDecodeError:
                rec = rawfmt.loads(line.decode())
            if rec.get("marker") == "rollover":
                continue
            offset = rec["offset"]
            recv_ns = rec["recv_time_ns"]
            if first is None:
                first = (offset, recv_ns)
            elif offset != last_offset + 1:
                offset_gaps.append([last_offset + 1, offset])
            last_offset, last_recv_ns = offset, recv_ns
            foreign_day_records += utc_date(recv_ns) != date
    meta = {
        "format": "v2", "file": path.name, "lines": lines, "bytes": size, "sha256": sha.hexdigest(),
        "offset_gaps": offset_gaps, "foreign_day_records": foreign_day_records,
    }
    if first is not None:
        meta.update(
            first_offset=first[0], last_offset=last_offset, first_recv_ns=first[1], last_recv_ns=last_recv_ns
        )
    meta.update(extra or {})
    if exclusive:
        with path.with_name(path.name + ".meta.json").open("xb") as f:
            f.write(orjson.dumps(meta, option=orjson.OPT_INDENT_2))
        return
    tmp = path.with_name(path.name + ".meta.tmp")
    tmp.write_bytes(orjson.dumps(meta, option=orjson.OPT_INDENT_2))
    os.replace(tmp, path.with_name(path.name + ".meta.json"))


class StreamWriter:
    def __init__(self, root: Path, stream: Stream, closer: ThreadPoolExecutor):
        self.root = root
        self.stream = stream
        self.closer = closer
        self.date: str | None = None
        self.fh = None
        self.dirty = False
        self.last_offset: int | None = None
        self.foreign_reported = False

    def open(self, date: str) -> None:
        final = stream_path(self.root, self.stream, date)
        final.parent.mkdir(parents=True, exist_ok=True)
        self.fh = open(final.with_name(final.name + OPEN), "ab")
        self.date = date
        self.foreign_reported = False

    def close_day(self, new_period: str | None = None) -> None:
        if new_period is None:
            new_period = (datetime.fromisoformat(self.date) + timedelta(days=1)).strftime("%Y%m%d")
        self.fh.write(rawfmt.rollover(new_period))
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.close()
        self.fh = None
        final = stream_path(self.root, self.stream, self.date)
        os.replace(final.with_name(final.name + OPEN), final)
        log.info("closed %s", final.relative_to(self.root))

        def on_done(future):
            error = future.exception()
            if error is not None:
                log.error("%s: metadata failed: %r", final, error)

        self.closer.submit(write_meta, final).add_done_callback(on_done)

    def write(self, date: str, line: bytes, offset: int) -> None:
        if self.date is None:
            self.open(date)
        elif date > self.date:
            self.close_day(date)
            self.open(date)
        # date < self.date (clock stepped back) stays in the current file: days never reopen.
        self.fh.write(line)
        self.dirty = True
        self.last_offset = offset

    def sync(self) -> None:
        if self.dirty:
            self.fh.flush()
            os.fsync(self.fh.fileno())
            self.dirty = False

    def shutdown(self) -> None:
        if self.fh is not None:
            self.sync()
            self.fh.close()
            self.fh = None


class Recorder:
    """Kafka-independent core, so tests can drive it directly."""

    def __init__(self, root: Path, streams: dict[tuple[str, int], Stream]):
        self.root = root
        self.streams = streams
        self.closer = ThreadPoolExecutor(max_workers=2, thread_name_prefix="meta")
        self.writers = {key: StreamWriter(root, s, self.closer) for key, s in streams.items()}
        self.expected: dict[tuple[str, int], int] = {}
        self._overdue_reported: set[tuple[tuple[str, int], str]] = set()

    def recover(self) -> dict[tuple[str, int], int | None]:
        """Repair open files, write missing metas, reopen today's file, and return the next
        offset per stream (None = the stream has no file yet)."""
        resume: dict[tuple[str, int], int | None] = {}
        for key, stream in self.streams.items():
            pattern = str(stream_path(self.root, stream, "*").relative_to(self.root))
            closed = list(self.root.glob(pattern))
            for path in closed:
                if not path.with_name(path.name + ".meta.json").exists():
                    write_meta(path)
            candidates = sorted(closed + list(self.root.glob(pattern + OPEN)), key=_file_date, reverse=True)
            resume[key] = None
            for path in candidates:
                if path.name.endswith(OPEN):
                    repair(path)
                    if path.stat().st_size == 0:
                        continue
                line = last_line(path)
                if not line:
                    continue
                try:
                    rec = orjson.loads(line)
                except orjson.JSONDecodeError:
                    rec = rawfmt.loads(line.decode())
                resume[key] = rec["offset"] + 1
                if path.name.endswith(OPEN):
                    # keep appending to the open day so its first next-day record closes it
                    self.writers[key].open(_file_date(path))
                break
        return resume

    def handle(self, topic: str, partition: int, offset: int, value: bytes) -> None:
        key = (topic, partition)
        writer = self.writers.get(key)
        if writer is None:
            return
        expected = self.expected.get(key)
        if expected is not None and offset != expected:
            log.error("%s/%d: offset jump %d -> %d (data expired from Kafka?)", topic, partition, expected, offset)
        self.expected[key] = offset + 1
        envelope = orjson.loads(value)
        recv_ns = envelope["recv_ns"]
        date = utc_date(recv_ns)
        if writer.date is not None and date < writer.date and not writer.foreign_reported:
            writer.foreign_reported = True
            log.warning("%s/%d: clock stepped back to %s; keeping record in day %s", topic, partition, date, writer.date)
        line = rawfmt.line(envelope, offset)
        writer.write(date, line, offset)

    def sync(self) -> list[TopicPartition]:
        """fsync every dirty file; return the positions that are now safely on disk."""
        positions = []
        for (topic, partition), writer in self.writers.items():
            writer.sync()
            if writer.last_offset is not None:
                positions.append(TopicPartition(topic, partition, writer.last_offset + 1))
        return positions

    def report_overdue(self, now: datetime) -> None:
        if now.hour < 6:
            return
        today = now.strftime("%Y-%m-%d")
        for key, writer in self.writers.items():
            if writer.date is not None and writer.date < today and (key, writer.date) not in self._overdue_reported:
                self._overdue_reported.add((key, writer.date))
                log.warning("OVERDUE: %s/%d day %s still open (waiting for catch-up)", *key, writer.date)

    def shutdown(self) -> None:
        for writer in self.writers.values():
            writer.shutdown()
        self.closer.shutdown(wait=True)


def committed_offsets(consumer: Consumer, keys: list[tuple[str, int]]) -> dict[tuple[str, int], int]:
    """Committed offsets for streams that have no files yet. Retries while the group coordinator
    starts (fresh broker); fails loudly rather than silently re-recording from the beginning."""
    if not keys:
        return {}
    for attempt in range(20):
        try:
            tps = consumer.committed([TopicPartition(t, p) for t, p in keys], timeout=10)
            return {(tp.topic, tp.partition): tp.offset for tp in tps}
        except KafkaException as e:
            log.warning("committed offsets unavailable (%s); retrying", e)
            time.sleep(3)
    raise RuntimeError("could not read committed offsets from Kafka")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args()
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(name)s: %(message)s")

    lock = single_instance(data_dir() / "state" / "recorder.lock")  # noqa: F841 - held until exit
    cfg = load_config()
    index = stream_index(cfg)
    recorder = Recorder(data_dir(), index)
    resume = recorder.recover()

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap(),
            "group.id": "raw-recorder",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.partition.fetch.bytes": 16 * 1024 * 1024,
        }
    )
    committed = committed_offsets(consumer, [key for key in index if resume[key] is None])
    assignment = []
    for key in index:
        offset = resume[key]
        source = "files"
        if offset is None:
            offset = committed.get(key, -1001)
            source = "commit"
            if offset < 0:
                offset, source = OFFSET_BEGINNING, "beginning"
        else:
            recorder.expected[key] = offset
        assignment.append(TopicPartition(key[0], key[1], offset))
        log.info("%s/%d: resume from %s (%s)", key[0], key[1], offset, source)
    consumer.assign(assignment)

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    last_sync = time.monotonic()
    count = 0
    try:
        while running:
            for msg in consumer.consume(num_messages=2000, timeout=0.5):
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("consume error: %s", msg.error())
                    continue
                recorder.handle(msg.topic(), msg.partition(), msg.offset(), msg.value())
                count += 1
            if time.monotonic() - last_sync >= SYNC_INTERVAL_S:
                positions = recorder.sync()
                if positions:
                    consumer.commit(offsets=positions, asynchronous=False)
                recorder.report_overdue(datetime.now(timezone.utc))
                last_sync = time.monotonic()
    finally:
        positions = recorder.sync()
        if positions:
            consumer.commit(offsets=positions, asynchronous=False)
        recorder.shutdown()
        consumer.close()
        log.info("stopped after %d records", count)


if __name__ == "__main__":
    main()
