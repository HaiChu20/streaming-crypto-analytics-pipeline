"""V2 modelling checks use synthetic observations only, including the CLI smoke."""

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import time

import numpy as np
import polars as pl
import pytest

from test_train_eval import synthetic_dataset, trade_row
from strategy2.evaluate import (align_reference, binary_log_loss_rows, evaluate,
                                side_metrics, summarize)
from strategy2.signal import make_signal, round_trip_cost
from strategy2.simulate import (choose_ev_threshold, period_frames, simulate,
                                simulation_report)
from strategy2.train import (LOCKBOX_MS, apply_binary_calibration, fit_binary_calibration,
                             fold_split, load_config, month_shift, timestamp_ms, train)

CONFIG = Path(__file__).resolve().parents[2] / "strategy2" / "config"


def config_v2():
    config = load_config(CONFIG / "train_v2.yaml")
    config.update(test_months=2, num_boost_round=8, early_stopping_rounds=3, bootstrap_draws=40)
    for stage in ("move", "side"):
        config[f"{stage}_lightgbm"].update(min_data_in_leaf=10, learning_rate=0.2,
            feature_fraction=1.0, bagging_fraction=1.0)
    return config


@pytest.fixture(scope="module")
def two_stage(tmp_path_factory):
    root = tmp_path_factory.mktemp("two-stage")
    data = synthetic_dataset(root / "features", step_minutes=360)
    config = config_v2()
    output = train(data, root, "two-stage", config=config, threads=1)
    return root, data, config, output


def test_two_stage_probabilities_and_artifacts(two_stage):
    _, _, _, output = two_stage
    frame = pl.read_parquet(output / "oof.parquet")
    np.testing.assert_allclose(frame.select("p_up", "p_down", "p_none").to_numpy().sum(axis=1), 1)
    np.testing.assert_allclose(frame["p_up"], frame["p_move"] * frame["p_side"])
    np.testing.assert_allclose(frame["p_down"], frame["p_move"] * (1 - frame["p_side"]))
    assert {"raw_p_move", "raw_p_side"}.issubset(frame.columns)
    assert frame["ts_ms"].max() < LOCKBOX_MS
    for fold in json.loads((output / "metadata.json").read_text())["folds"]:
        assert fold["inner_start_ms"] == month_shift(fold["start_ms"], -6)
        assert fold["fit_max_label_end_ms"] <= fold["inner_start_ms"] - 86_400_000
        assert fold["inner_max_label_end_ms"] <= fold["start_ms"] - 86_400_000
        assert fold["side_fit_rows"] < fold["fit_rows"]
        assert fold["side_inner_rows"] < fold["inner_rows"]
        for stage in ("move", "side"):
            assert (output / f"{fold['fold']}-{stage}.txt").is_file()
            assert (output / f"{fold['fold']}-{stage}-importance.csv").is_file()
            assert fold["best_iteration"][stage] > 0


def test_side_datasets_contain_only_touched(tmp_path, monkeypatch):
    import strategy2.train as module
    data = synthetic_dataset(tmp_path / "features")
    real_train = module.lgb.train
    calls = []
    def checked(params, training, *args, **kwargs):
        side = len(calls) % 2 == 1
        calls.append(len(training.label))
        assert params["objective"] == "binary"
        if side:
            assert np.all(np.abs(training.data[:, 0]) > 1 / 3)
            np.testing.assert_array_equal(training.label, training.data[:, 0] > 0)
            validation = kwargs["valid_sets"][0]
            assert np.all(np.abs(validation.data[:, 0]) > 1 / 3)
            np.testing.assert_array_equal(validation.label, validation.data[:, 0] > 0)
        return real_train(params, training, *args, **kwargs)
    monkeypatch.setattr(module.lgb, "train", checked)
    train(data, tmp_path, "checked", config=config_v2(), threads=1)
    assert len(calls) == 4
    assert calls[1] < calls[0] and calls[3] < calls[2]


def test_six_month_inner_purge_boundary():
    config = config_v2()
    start = timestamp_ms("2025-04-01")
    boundary = timestamp_ms("2024-10-01")
    frame = pl.DataFrame({"id": [0, 1, 2, 3],
        "ts_ms": [boundary - 2 * 86_400_000, boundary - 2 * 86_400_000, boundary, start],
        "label_end_ms_a": [boundary - 86_400_000, boundary - 86_400_000 + 1,
                           start - 86_400_000, start + 60_000]})
    fit, valid, test = fold_split(frame, start, timestamp_ms("2025-05-01"), "a", config)
    assert fit["id"].to_list() == [0]
    assert valid["id"].to_list() == [2]
    assert test["id"].to_list() == [3]


@pytest.mark.parametrize("method", ["isotonic", "platt"])
def test_calibration_improves_overconfidence(method):
    rng = np.random.default_rng(10)
    raw = np.repeat([0.01, 0.1, 0.9, 0.99], 3000)
    truth = np.repeat([0.3, 0.4, 0.6, 0.7], 3000)
    fit_y, test_y = rng.random(len(raw)) < truth, rng.random(len(raw)) < truth
    calibrator = fit_binary_calibration(raw, fit_y, method)
    calibrated = apply_binary_calibration(raw, calibrator)
    assert binary_log_loss_rows(test_y, calibrated).mean() + 0.3 < binary_log_loss_rows(test_y, raw).mean()
    assert np.all(np.diff(calibrated) >= 0)
    assert np.all((calibrated >= 0) & (calibrated <= 1))


@pytest.mark.parametrize("method", ["none", "isotonic", "platt"])
def test_binary_calibration_constant_or_disabled(method):
    scores = np.full(20, 0.8)
    calibrated = apply_binary_calibration(scores, fit_binary_calibration(scores, np.ones(20), method))
    assert np.isfinite(calibrated).all()
    if method == "none":
        np.testing.assert_array_equal(scores, calibrated)
    else:
        assert calibrated.min() > 0.99


def test_side_metrics_touched_only_and_win_bins():
    result = side_metrics([1, -1, 0, 1], [0.8, 0.2, 0.999, 0.6])
    assert result["rows"] == 3
    assert result["auc"] == 1
    assert result["log_loss"] == pytest.approx(-np.log([0.8, 0.8, 0.6]).mean())
    assert result["brier"] == pytest.approx(np.mean(np.square([0.2, 0.2, 0.4])))
    assert sum(row["rows"] for row in result["calibration"]) == 3
    assert sum(row["rows"] for row in result["calibration_deciles"]) == 3
    assert all(row["realised"] == 1 for row in result["calibration"] if row["rows"])


def test_period_boundaries():
    cutoff = timestamp_ms("2025-04-01")
    frame = pl.DataFrame({"ts_ms": [timestamp_ms("2024-04-01"), cutoff - 1, cutoff, LOCKBOX_MS - 1]})
    periods = period_frames(frame, config_v2())
    assert [len(periods[name]) for name in ("all", "tuning", "confirmation")] == [4, 2, 2]
    assert periods["confirmation"]["ts_ms"].min() == cutoff


def selection_rows():
    rows = []
    for period, base in (("tuning", "2024-07-01"), ("confirmation", "2025-07-01")):
        for index in range(240):
            high = index % 2 == 1
            label = 1 if high == (period == "tuning") else -1
            row = trade_row(ts=timestamp_ms(base) + index * 3_600_000, label=label)
            row.update(p_up=0.85 if high else 0.65, p_down=0.15 if high else 0.35, p_none=0.0)
            rows.append(row)
    return pl.DataFrame(rows)


def test_ev_selection_uses_tuning_despite_confirmation_optimum():
    report = simulation_report(selection_rows(), config_v2())
    chosen = report["ev_selection"]
    assert chosen["threshold"] == 0.0005
    assert chosen["periods"]["tuning"]["mean_net_return"] > 0
    assert chosen["periods"]["confirmation"]["mean_net_return"] < 0
    assert chosen["periods"]["confirmation"]["mean_net_return_ci"]["ci_high"] < 0
    confirmation_optimum = choose_ev_threshold(report["periods"]["confirmation"]["ev_threshold_sweep"])
    assert confirmation_optimum == 0
    assert chosen["threshold"] != confirmation_optimum
    assert list(report["scenarios"])[0] == "headline"
    assert "Headline" in report["scenarios"]["headline"]["label"]


def test_no_threshold_reaches_minimum():
    report = simulation_report(selection_rows().head(10), config_v2())
    assert report["ev_selection"]["threshold"] is None
    assert "100 tuning trades" in report["ev_selection"]["reason"]


@pytest.mark.parametrize("scenario,cost", [("headline", 0.0013), ("maker_entry", 0.0009)])
def test_cost_scenarios_hand_computed(scenario, cost):
    config = config_v2()
    config["costs"] = config["cost_scenarios"][scenario]
    assert round_trip_cost(config) == pytest.approx(cost)
    trade = simulate([trade_row()], config)[0]
    assert trade["net_return"] == pytest.approx(0.005 - cost)
    signal = make_signal(0, "BTC", "a", 0.85, 0.05, 0.1, 100, config)
    assert signal["ev"] == pytest.approx(0.8 * 0.005 - cost)


def test_calibrated_side_drives_signal_and_simulator():
    config = config_v2()
    signal = make_signal(0, "BTC", "a", 0.85, 0.05, 0.1, 100, config, p_move=0.9, p_side=0.6)
    assert signal["p_win"] == 0.6
    assert signal["ev"] == pytest.approx(0.9 * 0.2 * 0.005 - 0.0013)
    assert not signal["trade"]
    assert not simulate([trade_row() | {"p_move": 0.9, "p_side": 0.6}], config)
    legacy = make_signal(0, "BTC", "a", 0.85, 0.05, 0.1, 100, config)
    assert legacy["trade"]


def test_compare_refuses_mismatched_sets_and_checks_labels():
    frame = pl.DataFrame({"ts_ms": [1, 2, 3], "coin": ["BTC"] * 3, "label": [1, -1, 0]})
    other = frame.tail(2).reverse()
    with pytest.raises(ValueError, match="Mismatched"):
        align_reference(frame, other)
    left, right = align_reference(frame, other, intersection=True)
    assert left.equals(right)
    assert len(left) == 2
    with pytest.raises(ValueError, match="outcomes"):
        align_reference(frame, frame.with_columns(pl.lit(1).alias("label")))
    with pytest.raises(ValueError, match="Duplicate"):
        align_reference(frame, pl.concat([frame, frame.head(1)]))


def test_compare_cli_and_weekly_side_pairing(two_stage):
    root, _, _, output = two_stage
    result = evaluate(output, compare=output)
    for period in ("all", "confirmation"):
        comparison = result["periods"][period]["paired_side_log_loss_difference"]["reference"]
        assert comparison["mean"] == comparison["ci_low"] == comparison["ci_high"] == 0
    reference = root / "subset.parquet"
    pl.read_parquet(output / "oof.parquet").slice(1).write_parquet(reference)
    command = [sys.executable, "-m", "strategy2.evaluate", str(output), "--compare", str(reference)]
    failure = subprocess.run(command, capture_output=True, text=True, timeout=90)
    assert failure.returncode != 0 and "Mismatched OOF" in failure.stderr
    success = subprocess.run(command + ["--compare-intersection"], capture_output=True, text=True, timeout=90)
    assert success.returncode == 0, success.stderr
    report = json.loads((output / "report.json").read_text())
    assert report["comparison"]["intersection"]
    assert report["comparison"]["rows"] == len(pl.read_parquet(reference))


def test_summary_table_selects_tuning_only(two_stage, monkeypatch):
    import strategy2.evaluate as module
    root, _, _, output = two_stage
    template = evaluate(output)
    def report_for(path):
        report = deepcopy(template)
        # Opposite optima: run-b wins tuning; run-a wins confirmation.
        for period, score in (("all", 0.5), ("tuning", 0.6 if path == "run-a" else 0.5),
                              ("confirmation", 0.2 if path == "run-a" else 0.8)):
            report["periods"][period]["metrics"]["model"]["side"]["log_loss"] = score
        return report
    monkeypatch.setattr(module, "evaluate", report_for)
    summary = summarize(["run-a", "run-b"], root / "summary")
    assert summary["best_by_target"]["a"]["run"] == "run-b"
    assert len(summary["rows"]) == 12
    assert (root / "summary" / "summary.csv").is_file()
    assert json.loads((root / "summary" / "summary.json").read_text())["selection_period"] == "tuning"


def test_v1_default_and_side_grid():
    assert load_config()["model"] == "three_class"
    assert load_config()["inner_validation_months"] == 2
    for index, leaves, minimum in ((1, 15, 500), (2, 7, 2000), (3, 31, 1000), (4, 7, 5000)):
        config = load_config(CONFIG / "train_v2.yaml", [CONFIG / "v2" / f"c{index}.yaml"])
        assert config["move_lightgbm"] == load_config()["lightgbm"]
        assert config["side_lightgbm"]["num_leaves"] == leaves
        assert config["side_lightgbm"]["min_data_in_leaf"] == minimum
        assert config["inner_validation_months"] == 6


def test_simulator_million_rows_speed():
    count = 1_000_000
    # Sparse gates, dense timestamps and two coins exercise the actual hot path.
    times = timestamp_ms("2024-04-01") + np.arange(count, dtype=np.int64) // 2 * 60_000
    confident = np.arange(count) % 100 == 0
    frame = pl.DataFrame({"ts_ms": times, "coin": np.where(np.arange(count) % 2, "ETH", "BTC"),
        "entry_ms": times + 60_000, "entry_price": np.full(count, 100.0),
        "p_up": np.where(confident, 0.85, 0.45), "p_down": np.where(confident, 0.05, 0.45),
        "p_none": np.full(count, 0.1), "label": np.ones(count, dtype=np.int8),
        "exit_return": np.full(count, 0.005), "minutes_to_touch": np.full(count, 10)})
    config = config_v2()
    start = time.perf_counter()
    trades = simulate(frame, config)
    elapsed = time.perf_counter() - start
    assert len(trades) == 10_000
    assert elapsed < 10
    # Full six-threshold/two-scenario/three-period report, same million rows.
    config["bootstrap_draws"] = 20
    start = time.perf_counter()
    report = simulation_report(frame, config)
    full_elapsed = time.perf_counter() - start
    assert len(report["scenarios"]) == 2
    assert full_elapsed < 30
    print(f"\nV2 simulator 1M: single={elapsed:.3f}s full 6x2x3={full_elapsed:.3f}s")


def test_v2_c1_c2_cli_smoke(tmp_path):
    data = synthetic_dataset(tmp_path / "features", step_minutes=30)
    # Use only the development part for this smoke; lockbox invariance is tested separately.
    for path in data.glob("*.parquet"):
        pl.scan_parquet(path).filter(pl.col("ts_ms") < LOCKBOX_MS).collect().write_parquet(path)
    overrides = tmp_path / "few-rounds.yaml"
    overrides.write_text("num_boost_round: 5\\nearly_stopping_rounds: 3\\nbootstrap_draws: 40\\n".replace("\\n", "\n"))
    started = time.perf_counter()
    runs = []
    for candidate in ("c1", "c2"):
        command = [sys.executable, "-m", "strategy2.train", "--data", str(data), "--work", str(tmp_path),
            "--version", candidate, "--target", "a", "--config", str(CONFIG / "train_v2.yaml"),
            "--override", str(CONFIG / "v2" / f"{candidate}.yaml"), "--override", str(overrides), "--threads", "2"]
        result = subprocess.run(command, capture_output=True, text=True, timeout=240)
        assert result.returncode == 0, result.stderr
        run = tmp_path / "models" / "s2" / "a" / candidate
        runs.append(run)
        result = subprocess.run([sys.executable, "-m", "strategy2.evaluate", str(run)],
            capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, result.stderr
    summary_dir = tmp_path / "summary"
    result = subprocess.run([sys.executable, "-m", "strategy2.evaluate", "--summary", *map(str, runs),
        "--out", str(summary_dir)], capture_output=True, text=True, timeout=180)
    assert result.returncode == 0, result.stderr
    summary = json.loads((summary_dir / "summary.json").read_text())
    assert len(summary["rows"]) == 12
    metrics = []
    for run in runs:
        report = json.loads((run / "report.json").read_text())
        assert report["metrics"]["model"]["rows"] > 60_000
        assert set(report["periods"]) == {"all", "tuning", "confirmation"}
        assert len(json.loads((run / "metadata.json").read_text())["folds"]) == 24
        metrics.append({"run": run.name, "rows": report["metrics"]["model"]["rows"],
            "side_log_loss": report["periods"]["tuning"]["metrics"]["model"]["side_log_loss"],
            "touch_auc": report["metrics"]["model"]["touch_auc"],
            "ev_threshold": report["simulation"]["ev_selection"]["threshold"]})
    print(f"\nV2 c1/c2 CLI smoke: 2 coins, 30-minute rows, 2023-01 through 2026-03; "
          f"24 folds each; {time.perf_counter() - started:.2f}s; {json.dumps(metrics)}")
