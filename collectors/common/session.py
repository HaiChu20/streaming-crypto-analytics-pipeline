"""Collector session file used to detect a crashed previous run (plan.md §5.3)."""

import asyncio
import os
import time
import uuid
from pathlib import Path

import orjson

from collectors.common.lock import single_instance


class Session:
    def __init__(self, exchange: str, state_dir: Path):
        self.path = state_dir / f"collector-{exchange}.json"
        self._lock = single_instance(state_dir / f"collector-{exchange}.lock")
        self.id = uuid.uuid4().hex[:12]
        previous = self._read()
        # A previous session still marked running never shut down cleanly: it crashed.
        self.crashed_previous = previous if previous and previous.get("status") == "running" else None
        self._write("running")

    def _read(self) -> dict | None:
        try:
            return orjson.loads(self.path.read_bytes())
        except (FileNotFoundError, orjson.JSONDecodeError):
            return None

    def _write(self, status: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_bytes(orjson.dumps({"session": self.id, "status": status, "last_alive_ns": time.time_ns()}))
        os.replace(tmp, self.path)

    def crash_detail(self) -> dict | None:
        if self.crashed_previous is None:
            return None
        return {
            "prev_session": self.crashed_previous.get("session"),
            "prev_last_alive_ns": self.crashed_previous.get("last_alive_ns"),
        }

    async def heartbeat(self, interval: float = 5.0) -> None:
        while True:
            await asyncio.sleep(interval)
            self._write("running")

    def stop(self) -> None:
        self._write("stopped")
