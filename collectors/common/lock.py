"""Single-instance guard: two recorders (or two collectors for one exchange) would corrupt files."""

import fcntl
import os
from pathlib import Path


def single_instance(path: Path):
    """Hold an exclusive lock on `path` for the life of the process; exit if another process has it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(path, "a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit(f"another instance is already running (lock {path})") from None
    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    return handle
