import os

import pytest

from research.threads import available_cpus, resolve_threads


def test_affinity_controls_thread_default_and_explicit_override(monkeypatch):
    monkeypatch.setattr(os, 'sched_getaffinity', lambda pid: {0, 2, 4, 6}, raising=False)
    assert available_cpus() == 4
    assert resolve_threads() == 4
    assert max(1, available_cpus() // 2) == 2


def test_macos_fallback_when_affinity_api_is_missing(monkeypatch):
    monkeypatch.delattr(os, 'sched_getaffinity', raising=False)
    monkeypatch.setattr(os, 'cpu_count', lambda: 12)
    assert available_cpus() == 12
    assert resolve_threads() == 12


def test_macos_fallback_when_cpu_count_is_unknown(monkeypatch):
    monkeypatch.delattr(os, 'sched_getaffinity', raising=False)
    monkeypatch.setattr(os, 'cpu_count', lambda: None)
    assert available_cpus() == 1
    assert resolve_threads() == 1
