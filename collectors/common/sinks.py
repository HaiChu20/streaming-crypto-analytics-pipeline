"""Where collectors send records: Kafka in production, fixture files for the probe.

Both sinks expose the same interface:
  send(stream, value) -> bool   False = the record was lost (buffer full or delivery failed)
  lost                          stream keys that lost data since the last `clear_lost`
  has_room()                    True once the sink can accept data again
"""

import asyncio
import logging
from pathlib import Path

import orjson
from confluent_kafka import KafkaError, Producer

from collectors.common.config import Stream

log = logging.getLogger(__name__)


class KafkaSink:
    def __init__(self, bootstrap: str, client_id: str, buffer_kbytes: int):
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 10,
                "compression.type": "zstd",
                # Wait forever for Kafka; the buffer cap bounds memory (plan.md §5.3).
                "message.timeout.ms": 0,
                "queue.buffering.max.kbytes": buffer_kbytes,
                "queue.buffering.max.messages": 50_000_000,
                "message.max.bytes": 8_388_608,
            }
        )
        self.lost: set[str] = set()
        self.delivery_errors = 0
        self._full = False

    def send(self, stream: Stream, value: bytes) -> bool:
        try:
            self._producer.produce(
                stream.topic,
                value=value,
                key=stream.symbol.encode(),
                partition=stream.partition,
                on_delivery=lambda err, msg, key=stream.key: self._on_delivery(err, key),
            )
        except BufferError:
            if not self._full:
                log.error("producer buffer full; discarding records until Kafka drains it")
            self._full = True
            self.lost.add(stream.key)
            return False
        return True

    def _on_delivery(self, err: KafkaError | None, key: str) -> None:
        if err is not None:
            self.delivery_errors += 1
            self.lost.add(key)
            log.error("delivery failed for %s: %s", key, err)

    def has_room(self) -> bool:
        # Resume only after the backlog has mostly drained, to avoid flapping.
        room = len(self._producer) < 10_000
        if room:
            self._full = False
        return room

    def clear_lost(self, keys: set[str]) -> None:
        self.lost -= keys

    def queued(self) -> int:
        return len(self._producer)

    async def run(self) -> None:
        while True:
            self._producer.poll(0)
            await asyncio.sleep(0.02)

    def flush(self, timeout: float = 30.0) -> int:
        return self._producer.flush(timeout)


class FixtureSink:
    """Writes the first `limit` records of each stream to tests/fixtures/<exchange>/<market>/<feed>.jsonl."""

    def __init__(self, root: Path, limit: int = 300):
        self.root = root
        self.limit = limit
        self.counts: dict[str, int] = {}
        self.bytes: dict[str, int] = {}
        self.total: dict[str, int] = {}
        self.events: dict[str, list[tuple[str, str | None]]] = {}
        self.lost: set[str] = set()
        self._files: dict[Path, object] = {}

    def send(self, stream: Stream, value: bytes) -> bool:
        key = stream.key
        if b'"kind":"control"' in value:
            record = orjson.loads(value)
            self.events.setdefault(key, []).append((record["event"], record.get("reason")))
        else:
            self.total[key] = self.total.get(key, 0) + 1
            self.bytes[key] = self.bytes.get(key, 0) + len(value)
        if self.counts.get(key, 0) < self.limit:
            self.counts[key] = self.counts.get(key, 0) + 1
            path = self.root / stream.exchange / stream.market / f"{stream.feed}.jsonl"
            if path not in self._files:
                path.parent.mkdir(parents=True, exist_ok=True)
                self._files[path] = open(path, "wb")
            self._files[path].write(value + b"\n")
        return True

    def has_room(self) -> bool:
        return True

    def clear_lost(self, keys: set[str]) -> None:
        self.lost -= keys

    def queued(self) -> int:
        return 0

    async def run(self) -> None:
        await asyncio.Event().wait()

    def flush(self, timeout: float = 0) -> int:
        for f in self._files.values():
            f.flush()
        return 0

    def close(self) -> None:
        for f in self._files.values():
            f.close()
