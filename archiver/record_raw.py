"""Raw recorder: Kafka raw.* topics -> v3 channel files per UTC day (plan.md §7).

    uv run python -m archiver.record_raw

- Day = UTC date of the record's recv_ns. A stream's day-D file closes when that stream delivers
  its first day-D+1 record (never force-closed; still-open old days are reported as overdue).
- Files are the source of truth for progress: on restart each stream resumes from the last
  offset across its newest day files, so a crash creates no duplicates and no holes.
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


def stream_path(root: Path, stream: Stream, date: str, kind=None) -> Path:
    return rawfmt.path(root / "raw", stream.exchange, stream.market, stream.symbol, date, stream.feed, kind)


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


def closing_period(path: Path) -> str | None:
    """A crash may leave a fully written rollover before the atomic close."""
    with path.open("rb") as f:
        f.seek(max(0, path.stat().st_size - 512))
        tail = f.read().splitlines()
    if tail:
        try:
            rec = orjson.loads(tail[-1])
            if rec.get("marker") == "rollover":
                return rec["new_period"]
        except orjson.JSONDecodeError:
            pass
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
        "format": "v3", "file": path.name, "lines": lines, "bytes": size, "sha256": sha.hexdigest(),
        "offset_gaps": offset_gaps, "foreign_day_records": foreign_day_records,
    }
    if first is not None:
        meta.update(
            first_offset=first[0], last_offset=last_offset, first_recv_ns=first[1], last_recv_ns=last_recv_ns
        )
    # Split Binance l2 files have sparse offsets by design. These are per-file
    # gaps, not lost Kafka records; consumers must check the partition union.
    if "binance" in str(path).lower() and any(f"_{c}.jsonl" in path.name for c in ("depth", "snapshot")):
        meta["offset_gaps_scope"] = "file; Binance l2 offsets are shared by depth and snapshot"
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
        self.files = {}
        self.last_file: Path | None = None
        self.dirty = False
        self.last_offset: int | None = None
        self.foreign_reported = False

    def paths(self, date):
        base = stream_path(self.root, self.stream, date)
        return [base.with_name(f"{date.replace('-', '')}_{c}.jsonl")
                for c in rawfmt.file_channels(self.stream.exchange, self.stream.market, self.stream.feed)]

    def open(self, date: str) -> None:
        for final in self.paths(date):
            final.parent.mkdir(parents=True, exist_ok=True)
            self.files[final] = open(final.with_name(final.name + OPEN), "ab")
        self.last_file = None
        self.date = date
        self.foreign_reported = False

    def close_day(self, new_period: str | None = None) -> None:
        if new_period is None:
            new_period = (datetime.fromisoformat(self.date) + timedelta(days=1)).strftime("%Y%m%d")
        # For split files, flush the partition's last data before any rollover
        # can close the day, even when that data belongs to the second file.
        if len(self.files) > 1 and self.last_file is not None:
            self.files[self.last_file].flush()
        for final, fh in self.files.items():
            fh.write(rawfmt.rollover(new_period))
            fh.flush()
            os.fsync(fh.fileno())
            fh.close()
            os.replace(final.with_name(final.name + OPEN), final)
            log.info("closed %s", final.relative_to(self.root))

            def on_done(future, final=final):
                error = future.exception()
                if error is not None:
                    log.error("%s: metadata failed: %r", final, error)

            self.closer.submit(write_meta, final).add_done_callback(on_done)
        self.files = {}
        self.last_file = None

    def write(self, date: str, line: bytes, offset: int, kind=None) -> None:
        if self.date is None:
            self.open(date)
        elif date > self.date:
            self.close_day(date)
            self.open(date)
        # date < self.date (clock stepped back) stays in the current file.
        final = stream_path(self.root, self.stream, self.date, kind)
        # Only the last destination may retain buffered data. Flush it before
        # switching so process-crash recovery sees a contiguous partition prefix.
        if self.last_file is not None and self.last_file != final:
            self.files[self.last_file].flush()
        # BufferedWriter retries raw short writes; write/flush errors propagate
        # before offsets can be committed, so checking write's count adds nothing.
        self.files[final].write(line)
        self.last_file = final
        self.dirty = True
        self.last_offset = offset

    def sync(self) -> None:
        if self.dirty:
            for fh in self.files.values():
                fh.flush()
                os.fsync(fh.fileno())
            self.dirty = False

    def shutdown(self) -> None:
        self.sync()
        for fh in self.files.values():
            fh.close()
        self.files = {}
        self.last_file = None


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
            patterns = [str(p.relative_to(self.root)) for p in self.writers[key].paths("*")]
            closed = [p for pattern in patterns for p in self.root.glob(pattern)]
            opened = [p for pattern in patterns for p in self.root.glob(pattern + OPEN)]
            for path in opened:
                repair(path)
            # Finishing a two-file close is idempotent even if a crash happened
            # between rollover writes or between the two atomic renames.
            closing = {}
            for path in closed + opened:
                period = closing_period(path)
                if period is not None:
                    closing[_file_date(path)] = period
            for path in list(opened):
                day = _file_date(path)
                if day not in closing:
                    continue
                final = path.with_name(path.name.removesuffix(OPEN))
                if final.exists():
                    raise FileExistsError(final)
                with path.open("ab") as f:
                    if closing_period(path) is None:
                        f.write(rawfmt.rollover(closing[day]))
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(path, final)
                opened.remove(path)
                closed.append(final)
            for path in closed:
                if not path.with_name(path.name + ".meta.json").exists():
                    write_meta(path)
            candidates = closed + opened
            resume[key] = None
            if not candidates:
                continue
            newest = max(_file_date(p) for p in candidates)
            day_files = [p for p in candidates if _file_date(p) == newest]
            offsets = []
            for day in sorted({_file_date(p) for p in candidates}, reverse=True):
                for path in candidates:
                    if _file_date(path) != day:
                        continue
                    line = last_line(path)
                    if line:
                        offsets.append(rawfmt.loads(line.decode())["offset"])
                if offsets:
                    break
            # A partition can span depth + snapshot: its union, not either file,
            # determines the next Kafka offset.
            if offsets:
                resume[key] = max(offsets) + 1
                self.expected[key] = resume[key]
            if any(p.name.endswith(OPEN) for p in day_files):
                self.writers[key].open(newest)
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
        envelope = rawfmt.loads(value.decode())
        recv_ns = envelope["recv_ns"]
        date = utc_date(recv_ns)
        if writer.date is not None and date < writer.date and not writer.foreign_reported:
            writer.foreign_reported = True
            log.warning("%s/%d: clock stepped back to %s; keeping record in day %s", topic, partition, date, writer.date)
        line = rawfmt.line(envelope, offset)
        writer.write(date, line, offset, "snapshot" if line.startswith(b'{"channel":"snapshot"') else None)

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
