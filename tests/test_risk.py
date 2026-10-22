"""Complete-path excursion arithmetic and held-out conditional stop calibration."""

from fractions import Fraction

import numpy as np
import polars as pl
import pytest

from research.labels import NS, make_labels, momentum_300s, momentum_900s
from research.risk import STOP_NAMES, evaluate_stops, guard_stop_predictions, stop_metrics
from research.train import walk_forward_folds


def frame(mids):
    n = len(mids)
    return pl.DataFrame({"exchange": ["binance"] * n, "market": ["spot"] * n,
                         "symbol": ["BTCUSDT"] * n, "tick_ns": np.arange(n) * NS,
                         "available_ns": np.arange(n) * NS + 250_000_000,
                         "mid_e9": mids, "book_valid": [True] * n, "warmup": [False] * n})


def test_hand_path_excludes_start_includes_endpoint_and_has_shared_availability():
    rows = frame([100, 97, 104, 99, 106])
    labels = make_labels(rows, "BTC_spot", 3)
    assert labels["adverse_long_bps"].to_list() == [300., 0., None, None, None]
    assert labels["adverse_short_bps"].to_list() == [400., 10000 * 9 / 97, None, None, None]
    assert labels["favorable_long_bps"].to_list() == labels["adverse_short_bps"].to_list()
    assert labels["favorable_short_bps"].to_list() == labels["adverse_long_bps"].to_list()
    assert labels["label_available_ns"][0] == 3 * NS + 250_000_000
    assert labels["label_end_ns"][0] == 3 * NS


@pytest.mark.parametrize("defect", ["missing", "invalid", "warm", "null_mid", "zero_mid", "offgrid"])
def test_every_whole_second_is_required_even_if_return_endpoints_are_good(defect):
    rows = frame([100, 99, 101, 100, 103])
    if defect == "missing":
        rows = rows.filter(pl.col("tick_ns") != 2 * NS)
    else:
        column, value = {"invalid": ("book_valid", False), "warm": ("warmup", True),
                         "null_mid": ("mid_e9", None), "zero_mid": ("mid_e9", 0),
                         "offgrid": ("tick_ns", 2 * NS + 1)}[defect]
        rows = rows.with_columns(pl.when(pl.col("tick_ns") == 2 * NS).then(pl.lit(value))
                                 .otherwise(pl.col(column)).alias(column))
    labels = make_labels(rows, "BTC_spot", 4)
    assert labels["return_pct"][0] == 3.
    assert labels["adverse_long_bps"][0] is None
    assert labels["adverse_short_bps"][0] is None


@pytest.mark.parametrize("defect", [{"book_valid": False}, {"warmup": True}, {"mid_e9": 0},
                                     {"mid_e9": None}, {"tick_ns": 1}])
def test_paths_require_usable_start(defect):
    rows = frame([100, 99, 101]).with_columns([
        pl.when(pl.col("tick_ns") == 0).then(pl.lit(v)).otherwise(pl.col(k)).alias(k)
        for k, v in defect.items()
    ])
    assert make_labels(rows, "BTC_spot", 2)["adverse_long_bps"][0] is None


@pytest.mark.parametrize("start,low,high", [(2**53 + 1, 2**53, 2**53 + 2),
                                           (2**63 - 2, 1, 2**63 - 1),
                                           (1, 1, 2**63 - 1)])
def test_paths_preserve_large_integer_one_unit_moves_and_avoid_overflow(start, low, high):
    labels = make_labels(frame([start, low, high]), "BTC_spot", 2)
    assert labels["adverse_long_bps"][0] == pytest.approx(float(Fraction(10_000 * (start-low), start)), rel=3e-16, abs=0)
    assert labels["adverse_short_bps"][0] == pytest.approx(float(Fraction(10_000 * (high-start), start)), rel=3e-16, abs=0)
    if start > 2**53 and high - start == 1:
        assert labels["adverse_short_bps"][0] > 0


def test_300s_excursion_and_trailing_momentum_rules():
    mids = np.full(601, 100_000_000_005, dtype=np.int64)
    mids[100] -= 5_000_000_000
    mids[200] += 7_000_000_000
    mids[300] += 1_000_000_000
    rows = frame(mids)
    labels = make_labels(rows, "BTC_spot", 300)
    assert labels["adverse_long_bps"][0] == pytest.approx(10_000 * 5_000_000_000 / mids[0])
    assert labels["adverse_short_bps"][0] == pytest.approx(10_000 * 7_000_000_000 / mids[0])
    trailing = momentum_300s(rows, "BTC_spot")
    assert trailing["momentum_return_300s"][299] is None
    assert trailing["momentum_return_300s"][300] == labels["return_pct"][0]
    assert momentum_900s(rows, "BTC_spot")["momentum_return_900s"].null_count() == 601
    missing = rows.filter(pl.col("tick_ns") != 100 * NS)
    assert make_labels(missing, "BTC_spot", 300)["adverse_long_bps"][0] is None
    # Momentum intentionally only requires exact endpoints, with past emission known.
    assert momentum_300s(missing, "BTC_spot")["momentum_return_300s"][299] == labels["return_pct"][0]
    delayed = rows.with_columns(pl.when(pl.col("tick_ns") == 0).then(1000 * NS)
                                .otherwise(pl.col("available_ns")).alias("available_ns"))
    assert momentum_300s(delayed, "BTC_spot")["momentum_return_300s"][300] is None


@pytest.mark.parametrize("horizon", [1, 17, 300])
def test_vectorized_paths_match_simple_random_walk_reference(horizon):
    rng = np.random.default_rng(27)
    n = 2200
    mids = 2**53 + np.cumsum(rng.integers(-100, 101, n))
    rows = frame(mids).with_columns(
        (~pl.col("tick_ns").is_in([550 * NS, 1700 * NS])).alias("book_valid"),
        (pl.col("tick_ns") == 950 * NS).alias("warmup"),
    ).filter(pl.col("tick_ns") != 1300 * NS)
    expected = {}
    lookup = {r["tick_ns"]: r for r in rows.to_dicts()}
    for tick, r in lookup.items():
        path = [lookup.get(tick + j * NS) for j in range(1, horizon + 1)]
        usable = r["book_valid"] and not r["warmup"] and all(
            p and p["book_valid"] and not p["warmup"] and p["mid_e9"] > 0 for p in path)
        expected[tick] = (max(0, r["mid_e9"] - min(p["mid_e9"] for p in path)) * 10_000 / r["mid_e9"],
                          max(0, max(p["mid_e9"] for p in path) - r["mid_e9"]) * 10_000 / r["mid_e9"]) if usable else (None, None)
    actual = make_labels(rows, "BTC_spot", horizon)
    for tick, long, short in actual.select("tick_ns", "adverse_long_bps", "adverse_short_bps").iter_rows():
        for a, e in zip((long, short), expected[tick]):
            assert a is None if e is None else a == pytest.approx(e, rel=3e-16, abs=0)


def test_guard_clips_negatives_and_enforces_non_crossing():
    result = guard_stop_predictions({"stop_long_q80": [-1, 10, 2], "stop_long_q90": [-5, 4, 3],
                                     "stop_short_q80": [7, -1, 2], "stop_short_q90": [2, -2, 4]})
    np.testing.assert_array_equal(result["stop_long_q80"], [0, 10, 2])
    np.testing.assert_array_equal(result["stop_long_q90"], [0, 10, 3])
    np.testing.assert_array_equal(result["stop_short_q90"], [7, 0, 4])
    with pytest.raises(ValueError, match="nonfinite"):
        guard_stop_predictions({"stop_long_q80": [np.nan]})
    assert stop_metrics([1, 2, 3], [1, 2, 2], .9)["hit_rate"] == 1 / 3


def test_quantile_oof_calibration_and_pinball_on_heteroscedastic_data():
    rng = np.random.default_rng(430)
    n = 18000
    rv = rng.uniform(1, 20, n)
    x = rv[:, None]
    adverse = {side: rv * rng.exponential(1, n) * scale
               for side, scale in (("long", 1.), ("short", 1.5))}
    adverse["long"][::41] = np.nan
    ticks = np.arange(n, dtype=np.int64) * 300 * NS
    folds = walk_forward_folds(ticks, 300, max_folds=3)
    reports, overall = evaluate_stops(x, adverse, rv, ["rv"], folds, rounds=100)
    assert set(overall) == set(STOP_NAMES)
    for name, result in overall.items():
        q = result["quantile"]
        assert result["model"]["hit_rate"] == pytest.approx(1 - q, abs=.035)
        assert result["model"]["pinball_loss"] < .9 * result["unconditional"]["pinball_loss"]
        groups = result["model"]["by_volatility_tercile"]
        assert sum(g["n"] for g in groups.values()) == result["model"]["n"]
        for group in groups.values():
            assert group["hit_rate"] == pytest.approx(1 - q, abs=.05)
        side = name.split("_")[1]
        for (train, test), fold in zip(folds, reports):
            valid = train[np.isfinite(adverse[side][train])]
            assert fold[name]["train_rows"] == len(valid)
            assert fold[name]["model"]["n"] == np.isfinite(adverse[side][test]).sum()
            assert fold[name]["training_quantile_bps"] == np.quantile(adverse[side][valid], q)
            assert fold[name]["volatility_k"] == np.quantile(adverse[side][valid] / rv[valid], q)
            np.testing.assert_array_equal(fold[name]["volatility_tercile_edges"], np.quantile(rv[valid], [1/3, 2/3]))


def test_empty_excursion_training_and_missing_volatility_are_reported():
    x = np.arange(200)[:, None].astype(float)
    adverse = {"long": np.full(200, np.nan), "short": np.linspace(0, 20, 200)}
    reports, overall = evaluate_stops(x, adverse, np.full(200, np.nan), ["x"],
                                      [(np.arange(100), np.arange(100, 200))], rounds=1)
    assert overall["stop_long_q90"]["model"]["n"] == 0
    assert reports[0]["stop_long_q90"]["status"] == "no_training_labels"
    assert overall["stop_short_q90"]["model"]["n"] == 100
    assert overall["stop_short_q90"]["volatility_scaled"]["n"] == 0


def test_stop_oof_values_remain_aligned_when_a_fold_has_no_training_labels():
    x = np.arange(300, dtype=np.float64)[:, None]
    adverse = {'long': np.r_[np.full(100, np.nan), np.linspace(1, 20, 200)],
               'short': np.linspace(1, 20, 300)}
    folds = [(np.arange(100), np.arange(100, 200)), (np.arange(200), np.arange(200, 300))]
    reports, overall, saved = evaluate_stops(x, adverse, np.ones(300), ['x'], folds,
                                            rounds=1, num_threads=2, return_oof=True)
    assert len(saved) == len(folds)
    assert all(len(values) == 100 for fold in saved for values in fold.values())
    assert np.isnan(saved[0]['stop_long_q80']).all()
    assert np.isfinite(saved[1]['stop_long_q80']).all()
    for name in STOP_NAMES:
        actual = np.concatenate([adverse[name.split('_')[1]][test] for _, test in folds])
        prediction = np.concatenate([fold[name] for fold in saved])
        q = int(name[-2:]) / 100
        assert overall[name]['model']['pinball_loss'] == stop_metrics(actual, prediction, q)['pinball_loss']
