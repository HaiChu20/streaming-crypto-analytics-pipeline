"""Small synthetic schema fixtures; never read market-data archives or the lockbox."""

from copy import deepcopy
from datetime import datetime, timezone
import json
import subprocess
import sys
import time

import numpy as np
import polars as pl
import pytest

from strategy2.evaluate import (binary_auc, classification_metrics, evaluate,
                                log_loss_rows)
from strategy2.signal import make_signal, round_trip_cost
from strategy2.simulate import (funding_crossings, simulate, simulation_report,
                                trade_metrics, weekly_bootstrap)
from strategy2.train import (LOCKBOX_MS, apply_calibration, fit_calibration,
                             fit_isotonic, fold_split, load_config, monthly_folds,
                             timestamp_ms, train, validate_config)


def config_small():
    config = load_config()
    config.update(test_months=3, calibration=False, num_boost_round=35,
                  early_stopping_rounds=8, bootstrap_draws=1000)
    config["lightgbm"].update(min_data_in_leaf=12, learning_rate=0.2,
                              feature_fraction=1.0, bagging_fraction=1.0, lambda_l2=1)
    return config


def synthetic_dataset(root, planted=True, step_minutes=720, coins=("BTC", "ETH")):
    root.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(135)
    times = np.arange(timestamp_ms("2023-01-01"), timestamp_ms("2026-07-01"),
                      step_minutes * 60_000, dtype=np.int64)
    for coin_id, coin in enumerate(coins):
        signal = rng.uniform(-1, 1, len(times))
        labels = np.where(signal > 1 / 3, 1, np.where(signal < -1 / 3, -1, 0))
        if not planted:
            labels = rng.choice([-1, 0, 1], len(times))
        frame = {"ts_ms": times, "coin": [coin] * len(times), "entry_ms": times + 60_000,
                 "f_signal": signal, "f_coin_id": np.full(len(times), coin_id),
                 "last_funding_rate": np.zeros(len(times)),
                 "next_funding_ms": (times // 28_800_000 + 1) * 28_800_000,
                 "funding_interval_ms": np.full(len(times), 28_800_000)}
        for target, barrier, horizon in (("a", 0.005, 60), ("b", 0.01, 240), ("c", 0.02, 480)):
            frame.update({f"label_{target}": pl.Series(labels, dtype=pl.Int8),
                          f"exit_return_{target}": labels * barrier,
                          f"minutes_to_touch_{target}": [10 if label else None for label in labels],
                          f"max_up_{target}": np.full(len(times), barrier),
                          f"max_down_{target}": np.full(len(times), -barrier),
                          f"entry_price_{target}": np.full(len(times), 100.0),
                          f"label_end_ms_{target}": times + 60_000 + np.where(labels == 0, horizon, 10) * 60_000,
                          f"vol_p_up_{target}": np.full(len(times), 1 / 3),
                          f"vol_p_down_{target}": np.full(len(times), 1 / 3),
                          f"vol_p_none_{target}": np.full(len(times), 1 / 3),
                          f"mom_dir_{target}": frame["mom_dir_b"] if target == "c" else rng.choice([-1, 1], len(times)),
                          f"rev_dir_{target}": frame["rev_dir_b"] if target == "c" else rng.choice([-1, 1], len(times))})
        pl.DataFrame(frame).write_parquet(root / f"{coin}.parquet")
    (root / "metadata.json").write_text(json.dumps({"features": ["f_signal", "f_coin_id"]}))
    return root


@pytest.fixture(scope="module")
def trained(tmp_path_factory):
    root = tmp_path_factory.mktemp("s2")
    dataset = synthetic_dataset(root / "features")
    config = config_small()
    output = train(dataset, root, "planted", config=config, threads=1)
    return root, dataset, config, output


def test_lockbox_and_expanding_months(trained):
    _, _, config, output = trained
    frame = pl.read_parquet(output / "oof.parquet")
    metadata = json.loads((output / "metadata.json").read_text())
    assert frame["ts_ms"].max() < LOCKBOX_MS
    assert frame["ts_ms"].min() == timestamp_ms("2026-01-01")
    folds = metadata["folds"]
    assert [f["fold"] for f in folds] == ["2026-01", "2026-02", "2026-03"]
    assert [f["fit_rows"] for f in folds] == sorted({f["fit_rows"] for f in folds})
    for fold in folds:
        assert fold["fit_min_ts_ms"] == timestamp_ms(config["dev_from"])
        assert fold["fit_max_label_end_ms"] <= fold["inner_start_ms"] - 86_400_000
        assert fold["inner_max_label_end_ms"] <= fold["start_ms"] - 86_400_000
        assert (output / f"{fold['fold']}.txt").is_file()
        assert (output / f"{fold['fold']}-importance.csv").is_file()
        assert fold["best_iteration"] > 0
    all_folds = list(monthly_folds(load_config()))
    assert len(all_folds) == 24
    assert all_folds[0][0] == "2024-04"
    assert all_folds[-1][0] == "2026-03"


@pytest.mark.parametrize("change", [dict(lockbox_from="2026-05-01"), dict(include_lockbox=True),
                                     dict(dev_to="2026-06-01"), dict(test_to="2026-07-01")])
def test_cannot_open_lockbox(change):
    config = config_small() | change
    with pytest.raises(ValueError, match="[Ll]ockbox"):
        validate_config(config)


@pytest.mark.parametrize("target", ["a", "b", "c"])
def test_purge_embargo_equality(target):
    config = config_small()
    start, end = timestamp_ms("2026-03-01"), timestamp_ms("2026-04-01")
    inner = timestamp_ms("2026-01-01")
    cutoff, inner_cutoff = start - 86_400_000, inner - 86_400_000
    frame = pl.DataFrame({"id": range(7),
                          "ts_ms": [inner - 2 * 86_400_000] * 3 + [inner, inner, start, end],
                          f"label_end_ms_{target}": [inner_cutoff - 1, inner_cutoff, inner_cutoff + 1,
                                             cutoff, cutoff + 1, start + 1000, end + 1000]})
    fit, valid, test = fold_split(frame, start, end, target, config)
    assert fit["id"].to_list() == [0, 1]
    assert valid["id"].to_list() == [3]
    assert test["id"].to_list() == [5]


def test_refuses_existing_version(trained):
    root, data, config, _ = trained
    with pytest.raises(FileExistsError, match="overwrite"):
        train(data, root, "planted", config=config)


def test_determinism_and_lockbox_invariance(trained):
    root, data, config, output = trained
    # Change every lockbox feature and outcome: model and OOF must remain identical.
    altered = root / "altered"
    altered.mkdir()
    for path in data.glob("*.parquet"):
        pl.read_parquet(path).with_columns(
            pl.when(pl.col("ts_ms") >= LOCKBOX_MS).then(999.0).otherwise(pl.col("f_signal")).alias("f_signal"),
            pl.when(pl.col("ts_ms") >= LOCKBOX_MS).then(1).otherwise(pl.col("label_a")).alias("label_a")
        ).write_parquet(altered / path.name)
    (altered / "metadata.json").write_text((data / "metadata.json").read_text())
    second = train(altered, root, "repeat", config=config, threads=1)
    first_oof, second_oof = pl.read_parquet(output / "oof.parquet"), pl.read_parquet(second / "oof.parquet")
    assert first_oof.equals(second_oof)
    for fold in json.loads((output / "metadata.json").read_text())["folds"]:
        assert (output / f"{fold['fold']}.txt").read_bytes() == (second / f"{fold['fold']}.txt").read_bytes()


def test_planted_model_beats_baselines_and_report(trained):
    _, _, _, output = trained
    report = evaluate(output)
    assert report["metrics"]["model"]["log_loss"] < 0.2
    assert report["metrics"]["model"]["touch_auc"] > 0.98
    for baseline in ("vol", "prior", "mom", "rev"):
        assert report["paired_log_loss_difference"][baseline]["ci_high"] < 0
    assert set(report["per_coin"]) == {"BTC", "ETH"}
    assert set(report["per_year"]) == {"2026"}
    assert (output / "report.json").is_file()
    html = (output / "report.html").read_text()
    assert "<html" in html and "<script src" not in html
    assert report["simulation"]["trades"] > 0


def test_noise_no_significant_improvement(tmp_path):
    data = synthetic_dataset(tmp_path / "noise", planted=False, step_minutes=360)
    config = config_small()
    config.update(test_months=6)
    output = train(data, tmp_path, "noise", config=config, threads=1)
    report = evaluate(output)
    assert report["paired_log_loss_difference"]["prior"]["ci_high"] >= 0
    assert report["paired_log_loss_difference"]["vol"]["ci_high"] >= 0


def test_hand_metrics():
    labels = np.array([1, -1, 0, 1])
    p = np.array([[0.7, 0.2, 0.1], [0.2, 0.6, 0.2], [0.2, 0.1, 0.7], [0.2, 0.6, 0.2]])
    metrics = classification_metrics(labels, p)
    assert metrics["log_loss"] == pytest.approx(-np.log([0.7, 0.6, 0.7, 0.2]).mean())
    assert metrics["touch_auc"] == 1
    assert metrics["side_accuracy"] == pytest.approx(2 / 3)
    assert sum(row["rows"] for row in metrics["calibration"]["up"]) == 4
    assert binary_auc([1, 0], [0.5, 0.5]) == 0.5
    assert binary_auc([1, 1], [0.1, 0.9]) is None
    with pytest.raises(ValueError, match="probability"):
        log_loss_rows(labels, p * 2)


def test_isotonic_pooling_and_renormalising():
    knots = fit_isotonic([0.1, 0.2, 0.2, 0.3], [1, 0, 1, 0])
    assert knots["y"] == [0.5, 0.5, 0.5]
    p = np.array([[0.7, 0.2, 0.1], [0.1, 0.8, 0.1], [0.1, 0.2, 0.7]])
    calibrated = apply_calibration(p, fit_calibration(p, [0, 1, 2]))
    np.testing.assert_allclose(calibrated.sum(axis=1), 1)
    assert np.argmax(calibrated, axis=1).tolist() == [0, 1, 2]


def test_calibration_end_to_end(trained):
    root, data, config, _ = trained
    config = deepcopy(config)
    config.update(calibration=True, test_months=1)
    output = train(data, root, "calibrated", config=config, threads=1)
    report = evaluate(output)
    assert "raw" in report["metrics"]
    oof = pl.read_parquet(output / "oof.parquet")
    np.testing.assert_allclose(oof.select("p_up", "p_down", "p_none").to_numpy().sum(axis=1), 1)
    assert json.loads((output / "2026-03.json").read_text())["calibrators"]


def test_bootstrap_reproducible_and_paired():
    ts = [timestamp_ms("2025-12-29"), timestamp_ms("2026-01-01"), timestamp_ms("2026-01-05")]
    result = weekly_bootstrap([-1.0, -1.0, -1.0], ts)
    assert result == weekly_bootstrap([-1.0, -1.0, -1.0], ts, seed=17)
    assert result == {"mean": -1, "ci_low": -1, "ci_high": -1, "weeks": 2}
    assert weekly_bootstrap([1, 2, 3], ts) == weekly_bootstrap([1, 2, 3], ts)


def trade_row(ts=0, coin="BTC", label=1, direction=1, minutes=10, timeout_return=0.001):
    return {"ts_ms": ts, "coin": coin, "entry_ms": ts + 60_000, "entry_price": 100.0,
            "p_up": 0.85 if direction == 1 else 0.05,
            "p_down": 0.05 if direction == 1 else 0.85, "p_none": 0.1,
            "label": label, "exit_return": label * 0.005 if label else timeout_return,
            "minutes_to_touch": minutes if label else None, "last_funding_rate": 0.0,
            "next_funding_ms": 28_800_000, "funding_interval_ms": 28_800_000}


@pytest.mark.parametrize("label,direction,expected", [(1, 1, 0.005), (-1, 1, -0.005),
                                                       (-1, -1, 0.005), (1, -1, -0.005),
                                                       (0, 1, 0.001), (0, -1, -0.001)])
def test_trade_net_and_exit(label, direction, expected):
    config = load_config()
    trade = simulate([trade_row(label=label, direction=direction)], config)[0]
    assert trade["net_return"] == pytest.approx(expected - 0.0013)
    assert trade["outcome"] == label * direction
    assert trade["exit_ms"] == 60_000 + (10 if label else 60) * 60_000
    metrics = trade_metrics([trade], config)
    assert metrics["break_even_p"] == pytest.approx(0.63)


def test_one_position_per_coin_and_decision_time():
    rows = [trade_row(), trade_row(ts=5 * 60_000), trade_row(ts=10 * 60_000),
            trade_row(ts=11 * 60_000), trade_row(ts=5 * 60_000, coin="ETH")]
    trades = simulate(rows, load_config())
    assert [(t["coin"], t["ts_ms"]) for t in trades] == [("BTC", 0), ("ETH", 300_000), ("BTC", 660_000)]


@pytest.mark.parametrize("direction", [1, -1])
def test_funding_count_and_sign(direction):
    row = trade_row(direction=direction, label=0)
    row.update(next_funding_ms=60_000, funding_interval_ms=15 * 60_000, last_funding_rate=0.0001)
    trade = simulate([row], load_config())[0]
    assert trade["funding_crossings"] == 4  # entry excluded, exit included
    assert trade["funding"] == pytest.approx(direction * 0.0004)
    assert trade["net_return"] == pytest.approx(direction * 0.001 - 0.0013 - direction * 0.0004)
    assert funding_crossings(0, 10, 10, 10) == 1
    assert funding_crossings(0, 9, 10, 10) == 0


@pytest.mark.parametrize("direction", [1, -1])
def test_signal_prices_and_ev(direction):
    config = load_config()
    config.update(timeout_mean_long=0.001, timeout_mean_short=-0.001)
    up, down = (0.7, 0.2) if direction == 1 else (0.2, 0.7)
    signal = make_signal(0, "BTC", "a", up, down, 0.1, 100, config)
    assert signal["direction"] == direction
    assert signal["tp_price"] == pytest.approx(100 * (1 + direction * 0.005))
    assert signal["sl_price"] == pytest.approx(100 * (1 - direction * 0.005))
    assert signal["ev"] == pytest.approx(0.5 * 0.005 + 0.1 * direction * 0.001 - 0.0013)
    assert signal["p_win"] == pytest.approx(7 / 9)
    assert signal["p_move"] == 0.9
    assert signal["time_stop_ms"] == 61 * 60_000
    assert signal["trade"]
    config["ev_threshold"] = signal["ev"]
    assert not make_signal(0, "BTC", "a", up, down, 0.1, 100, config)["trade"]


def test_signal_no_direction_and_validation():
    config = load_config()
    assert not make_signal(0, "BTC", "a", 0, 0, 1, 100, config)["trade"]
    assert make_signal(0, "BTC", "b", 0, 0, 1, 100, config)["time_stop_ms"] == 241 * 60_000
    with pytest.raises(ValueError):
        make_signal(0, "BTC", "a", 0.8, 0.5, 0.1, 100, config)
    assert round_trip_cost(config) == pytest.approx(0.0013)


def test_simulation_statistics_zero_days_and_sweep():
    config = load_config()
    rows = pl.DataFrame([trade_row(), trade_row(ts=3 * 86_400_000, label=-1)])
    report = simulation_report(rows, config)
    assert report["trades"] == 2
    assert report["win_rate"] == report["loss_rate"] == 0.5
    assert report["timeout_rate"] == 0
    assert report["total_return"] == pytest.approx(-0.0026)
    assert report["max_drawdown"] > 0
    assert report["daily_sharpe"] is not None
    assert len(report["ev_threshold_sweep"]) == len(config["ev_threshold_sweep"])
    assert report["ev_threshold_sweep"][-1]["trades"] == 2


def test_missing_baseline_rows(trained):
    from strategy2.evaluate import evaluate_frame
    _, _, config, output = trained
    frame = pl.read_parquet(output / "oof.parquet").with_columns(
        pl.lit(None, dtype=pl.Float64).alias("vol_p_up"))
    report = evaluate_frame(frame, config)
    assert report["metrics"]["vol"]["rows"] == 0
    assert report["paired_log_loss_difference"]["vol"]["mean"] is None


def test_cli_smoke(tmp_path):
    """Full 24-month train/evaluate CLIs on two coins, 30-minute rows through June 2026."""
    data = synthetic_dataset(tmp_path / "features", step_minutes=30)
    config = config_small()
    config.update(test_months=24, num_boost_round=5, early_stopping_rounds=3,
                  calibration=True, bootstrap_draws=1000)
    config_path = tmp_path / "smoke.json"
    config_path.write_text(json.dumps(config))  # JSON is valid YAML
    started = time.perf_counter()
    result = subprocess.run([sys.executable, "-m", "strategy2.train", "--data", str(data),
                             "--work", str(tmp_path), "--version", "smoke", "--target", "a",
                             "--config", str(config_path), "--threads", "2"],
                            capture_output=True, text=True, timeout=180)
    assert result.returncode == 0, result.stderr
    output = tmp_path / "models" / "s2" / "a" / "smoke"
    result = subprocess.run([sys.executable, "-m", "strategy2.evaluate", str(output)],
                            capture_output=True, text=True, timeout=180)
    assert result.returncode == 0, result.stderr
    elapsed = time.perf_counter() - started
    report = json.loads((output / "report.json").read_text())
    assert report["metrics"]["model"]["rows"] > 60_000
    assert pl.read_parquet(output / "oof.parquet")["ts_ms"].max() < LOCKBOX_MS
    print(f"\nS2 CLI smoke: 2 coins, 30-min rows, 2023-01 through 2026-06; 24 folds; {elapsed:.2f}s; "
          f"OOF={report['metrics']['model']['rows']}; log_loss={report['metrics']['model']['log_loss']:.6f}")



@pytest.mark.parametrize("target,break_even", [("b", 0.565), ("c", 0.5325)])
def test_larger_targets_end_to_end(trained, target, break_even):
    root, data, config, _ = trained
    config = deepcopy(config)
    config["test_months"] = 1
    output = train(data, root, f"target-{target}", target=target, config=config, threads=1)
    report = evaluate(output)
    assert report["target"] == target
    assert report["simulation"]["break_even_p"] == pytest.approx(break_even)
    assert report["metrics"]["model"]["log_loss"] < 0.2


@pytest.mark.parametrize("target", ["a", "c"])
def test_cross_boundary_outcomes_and_null_labels_are_excluded(trained, target):
    root, data, config, _ = trained
    altered = root / f"boundary-{target}"
    altered.mkdir()
    for path in data.glob("*.parquet"):
        frame = pl.read_parquet(path)
        crossing = frame.tail(1).with_columns(
            pl.lit(LOCKBOX_MS - 5 * 60_000).alias("ts_ms"),
            pl.lit(LOCKBOX_MS - 4 * 60_000).alias("entry_ms"),
            pl.lit(LOCKBOX_MS + 6 * 60_000).alias(f"label_end_ms_{target}"))
        incomplete = frame.tail(1).with_columns(
            pl.lit(LOCKBOX_MS - 10 * 60_000).alias("ts_ms"),
            pl.lit(None, dtype=pl.Int8).alias(f"label_{target}"))
        pl.concat([frame, crossing, incomplete]).write_parquet(altered / path.name)
    config = deepcopy(config)
    config["test_months"] = 1
    output = train(altered, root, f"boundary-{target}", target=target, config=config, threads=1)
    oof = pl.read_parquet(output / "oof.parquet")
    assert oof["ts_ms"].max() < LOCKBOX_MS - 10 * 60_000
    assert oof["label"].null_count() == 0


def test_selective_uses_inner_threshold_not_test_quantile():
    from strategy2.evaluate import selective_metrics
    frame = pl.DataFrame({"fold": ["2026-01"] * 2, "label": [1, -1],
                          "threshold_model_20": [0.7, 0.7]})
    probabilities = np.array([[0.54, 0.36, 0.1], [0.18, 0.72, 0.1]])
    selective = selective_metrics(frame, probabilities, "model", [0.2])["top_20_pct"]
    assert selective["coverage"] == 0.5
    assert selective["win_rate"] == 1


def test_feature_metadata_cannot_include_outcomes(trained):
    root, data, config, _ = trained
    altered = root / "leaky-metadata"
    altered.mkdir()
    # Keep fixture copies inside pytest's worktree-local base directory.
    for path in data.glob("*.parquet"):
        (altered / path.name).write_bytes(path.read_bytes())
    (altered / "metadata.json").write_text(json.dumps({"features": ["f_coin_id", "label_a"]}))
    with pytest.raises(ValueError, match=r"f_\*"):
        train(altered, root, "leaky", config=config, threads=1)
