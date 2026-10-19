"""Small pipeline fixtures; real calendar-fold behavior is tested separately."""
import numpy as np
import pytest


@pytest.fixture
def compact_folds(monkeypatch):
    """Keep seconds-long model/parity fixtures small without a production row split."""
    def split(tick_ns, horizon_s, purge_s=None, label_available_ns=None,
              feature_available_ns=None, **kwargs):
        ticks = np.asarray(tick_ns)
        known = ticks + horizon_s * 1_000_000_000
        for available in (label_available_ns, feature_available_ns):
            if available is not None:
                known = np.maximum(known, available)
        bounds = np.linspace(0, len(ticks), 4, dtype=int)
        return [(np.flatnonzero(known < ticks[start]), np.arange(start, end))
                for start, end in zip(bounds[1:-1], bounds[2:])]
    monkeypatch.setattr('research.train.walk_forward_folds', split)
    return split
