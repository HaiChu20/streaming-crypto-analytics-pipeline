"""Raw recorder: Kafka raw.* topics -> one JSONL file per (topic, partition) per UTC day (plan.md §7).

    uv run python -m archiver.record_raw

- Day = UTC date of the record's recv_ns. A stream's day-D file closes when that stream delivers
  its first day-D+1 record (never force-closed; still-open old days are reported as overdue).
- Files are the source of truth for progress: on restart each stream resumes from the last
  offset in its newest file, so a crash creates no duplicates and no holes.
"""

import argparse
import hashlib
import logging
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import orjson
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition

from collectors.common.config import Stream, data_dir, kafka_bootstrap, load_config, stream_index
from collectors.common.lock import single_instance

log = logging.getLogger("recorder")

OPEN = ".open"
SYNC_INTERVAL_S = 2.0


def utc_date(recv_ns: int) -> str:
    return datetime.fromtimestamp(recv_ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d")


def _file_date(path: Path) -> str:
    return next(part for part in path.parts if part.startswith("date=")).removeprefix("date=")


def stream_path(root: Path, stream: Stream, date: str) -> Path:
    return (
        root / "raw" / f"date={date}" / f"exchange={stream.exchange}" / f"market={stream.market}"
        / f"feed={stream.feed}" / f"{stream.file_symbol}.jsonl"
    )


def last_line(path: Path) -> bytes | None:
    """Last complete line of a file, reading backwards from the end."""
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        if end == 0:
            return None
        chunk, pos, buf = 1 << 16, end, b""
        while pos > 0:
            step = min(chunk, pos)
            pos -= step
            f.seek(pos)
            buf = f.read(step) + buf
            # buf ends with "\n"; find the newline before the last line
            idx = buf.rfind(b"\n", 0, len(buf) - 1)
            if idx >= 0:
                return buf[idx + 1 : -1]
        return buf[:-1]


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


def write_meta(path: Path) -> None:
    """Write <file>.meta.json for a closed day file: count, offsets, time range, sha256."""
    sha, lines, size = hashlib.sha256(), 0, 0
    first = None
    with open(path, "rb") as f:
        while block := f.read(1 << 20):
            if first is None:
                first = block.split(b"\n", 1)[0]
            sha.update(block)
            lines += block.count(b"\n")
            size += len(block)
    meta = {"file": path.name, "lines": lines, "bytes": size, "sha256": sha.hexdigest()}
    last = last_line(path)
    if first and last:
        a, b = orjson.loads(first), orjson.loads(last)
        meta.update(
            first_offset=a["offset"], last_offset=b["offset"], first_recv_ns=a["recv_ns"], last_recv_ns=b["recv_ns"]
        )
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

    def open(self, date: str) -> None:
        final = stream_path(self.root, self.stream, date)
        final.parent.mkdir(parents=True, exist_ok=True)
        self.fh = open(final.with_name(final.name + OPEN), "ab")
        self.date = date

    def close_day(self) -> None:
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.close()
        self.fh = None
        final = stream_path(self.root, self.stream, self.date)
        os.replace(final.with_name(final.name + OPEN), final)
        log.info("closed %s", final.relative_to(self.root))
        self.closer.submit(write_meta, final)

    def write(self, date: str, line: bytes, offset: int) -> None:
        if self.date is None:
            self.open(date)
        elif date > self.date:
            self.close_day()
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
                        path.unlink()
                        continue
                line = last_line(path)
                if not line:
                    continue
                resume[key] = orjson.loads(line)["offset"] + 1
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
        recv_ns = orjson.loads(value)["recv_ns"]
        line = value[:-1] + b',"topic":"%s","partition":%d,"offset":%d}\n' % (topic.encode(), partition, offset)
        writer.write(utc_date(recv_ns), line, offset)

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
