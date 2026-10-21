"""One CPU budget for offline LightGBM fits, predictions and diagnostics."""
import os


def available_cpus():
    affinity = getattr(os, 'sched_getaffinity', None)
    return len(affinity(0)) if affinity is not None else (os.cpu_count() or 1)


def resolve_threads(threads=None):
    if threads is None:
        threads = available_cpus()
    if isinstance(threads, bool) or int(threads) != threads or threads < 1:
        raise ValueError('threads must be a positive integer')
    return int(threads)
