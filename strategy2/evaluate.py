"""Evaluate saved development OOF predictions and render a self-contained report."""

import argparse
from datetime import datetime, timezone
import html
import json
from pathlib import Path

import numpy as np
import polars as pl

from strategy2.simulate import simulation_report, weekly_bootstrap
from strategy2.train import (baseline_probabilities, class_indices, confidence,
                             threshold_name)


def log_loss_rows(labels, probabilities):
    probabilities = np.asarray(probabilities, dtype=float)
    if probabilities.shape != (len(labels), 3):
        raise ValueError("Expected up/down/none probabilities")
    if (not np.isfinite(probabilities).all() or (probabilities < 0).any()
            or (probabilities > 1).any() or not np.allclose(probabilities.sum(axis=1), 1)):
        raise ValueError("Invalid probability distributions")
    return -np.log(np.clip(probabilities[np.arange(len(labels)), class_indices(labels)], 1e-15, 1))


def binary_auc(labels, scores):
    labels, scores = np.asarray(labels, dtype=bool), np.asarray(scores, dtype=float)
    positive, negative = int(labels.sum()), int((~labels).sum())
    if not positive or not negative:
        return None
    order = np.argsort(scores, kind="stable")
    sorted_scores = scores[order]
    starts = np.r_[0, np.flatnonzero(np.diff(sorted_scores)) + 1]
    ends = np.r_[starts[1:], len(scores)]
    ranks = np.empty(len(scores), dtype=float)
    for start, end in zip(starts, ends):
        ranks[order[start:end]] = (start + 1 + end) / 2
    return float((ranks[labels].sum() - positive * (positive + 1) / 2) / (positive * negative))


def calibration_table(labels, probabilities):
    indices = class_indices(labels)
    result = {}
    for c, name in enumerate(("up", "down", "none")):
        scores = probabilities[:, c]
        bins = np.minimum((scores * 10).astype(int), 9)
        result[name] = []
        for decile in range(10):
            selected = bins == decile
            result[name].append({"lower": decile / 10, "upper": (decile + 1) / 10,
                                 "rows": int(selected.sum()),
                                 "predicted": float(scores[selected].mean()) if selected.any() else None,
                                 "realised": float((indices[selected] == c).mean()) if selected.any() else None})
    return result


def classification_metrics(labels, probabilities):
    labels, probabilities = np.asarray(labels), np.asarray(probabilities, dtype=float)
    if not len(labels):
        return {"rows": 0, "log_loss": None, "touch_auc": None,
                "side_accuracy": None, "calibration": None}
    losses = log_loss_rows(labels, probabilities)
    touched = labels != 0
    directions = np.sign(probabilities[:, 0] - probabilities[:, 1])
    return {"rows": len(labels), "log_loss": float(losses.mean()),
            "touch_auc": binary_auc(touched, 1 - probabilities[:, 2]),
            "side_accuracy": float((directions[touched] == labels[touched]).mean()) if touched.any() else None,
            "calibration": calibration_table(labels, probabilities)}


def source_probabilities(frame, source):
    if source in ("model", "raw"):
        prefix = "" if source == "model" else "raw_"
        return frame.select([f"{prefix}p_{side}" for side in ("up", "down", "none")]).to_numpy()
    return baseline_probabilities(frame, source)


def selective_metrics(frame, probabilities, source, fractions):
    labels = frame["label"].to_numpy()
    scores = confidence(probabilities)
    directions = np.sign(probabilities[:, 0] - probabilities[:, 1])
    result = {}
    for fraction in fractions:
        column = threshold_name(source, fraction)
        thresholds = frame[column].to_numpy()
        selected = (scores >= thresholds) & np.isfinite(thresholds) & (directions != 0)
        touched = selected & (labels != 0)
        result[f"top_{int(round(fraction * 100))}_pct"] = {
            "selected": int(selected.sum()), "coverage": float(selected.mean()) if len(frame) else None,
            "touched": int(touched.sum()), "touched_coverage": float(touched.sum() / max(1, (labels != 0).sum())),
            "win_rate": float((directions[touched] == labels[touched]).mean()) if touched.any() else None,
            "thresholds_per_fold": {fold: group[column][0] for (fold,), group in frame.group_by("fold")},
        }
    return result


def evaluate_frame(frame, config):
    sources = ["model", "vol", "mom", "rev", "prior"]
    if config.get("calibration"):
        sources.insert(1, "raw")
    report = {"metrics": {}, "paired_log_loss_difference": {}, "per_coin": {}, "per_year": {},
              "notes": ["Probability order is up/down/none; momentum and reversal directions are scored as deterministic one-hot distributions (clipped at 1e-15 for log loss).",
                        "Selective thresholds come from each fold's inner validation; ties can yield coverage above the nominal fraction. Win rate conditions on touched rows.",
                        "Missing baseline rows are excluded only for that baseline; paired comparisons use the same available rows.",
                        "Bootstrap samples ISO weeks with replacement, pooled across coins, preserving all observations within each sampled week."]}
    for source in sources:
        probabilities = source_probabilities(frame, source)
        eligible = np.isfinite(probabilities).all(axis=1)
        sub = frame.filter(pl.Series(eligible))
        p = probabilities[eligible]
        report["metrics"][source] = classification_metrics(sub["label"].to_numpy(), p)
        report["metrics"][source]["available_coverage"] = float(eligible.mean())
        report["metrics"][source]["selective"] = selective_metrics(sub, p, source, config["selective_fractions"])
        if source not in ("model", "raw"):
            difference = (log_loss_rows(sub["label"].to_numpy(), source_probabilities(sub, "model"))
                          - log_loss_rows(sub["label"].to_numpy(), p))
            report["paired_log_loss_difference"][source] = weekly_bootstrap(
                difference, sub["ts_ms"].to_list(), config.get("bootstrap_draws", 1000), config.get("bootstrap_seed", 17))
        for (coin,), group in sub.group_by("coin"):
            report["per_coin"].setdefault(coin, {})[source] = classification_metrics(
                group["label"].to_numpy(), source_probabilities(group, source))
        years = sub.with_columns(pl.from_epoch("ts_ms", time_unit="ms").dt.year().alias("year"))
        for (year,), group in years.group_by("year"):
            report["per_year"].setdefault(str(year), {})[source] = classification_metrics(
                group["label"].to_numpy(), source_probabilities(group, source))
    return report


def render_html(report):
    """No external scripts, fonts or assets; all diagnostics are visible in details."""
    lines = ["<!doctype html><html lang='en'><meta charset='utf-8'><title>Strategy 2 evaluation</title>",
             "<style>body{font:15px system-ui;margin:2rem;max-width:1100px;color:#17212b}table{border-collapse:collapse}td,th{padding:.5rem;border:1px solid #bbb;text-align:right}pre{white-space:pre-wrap;overflow-wrap:anywhere}summary{cursor:pointer;font-weight:bold;margin:1rem 0}</style>",
             "<h1>Strategy 2 development evaluation</h1>",
             f"<p>Target {html.escape(report['target'])}; version {html.escape(report['version'])}. Sealed lockbox excluded.</p>",
             "<table><tr><th>Source</th><th>Rows</th><th>Log loss</th><th>Touch AUC</th><th>Side accuracy</th></tr>"]
    def fmt(value):
        return f"{value:.6f}" if isinstance(value, float) else html.escape(str(value))
    for source, metrics in report["metrics"].items():
        lines.append("<tr>" + "".join(f"<td>{fmt(value)}</td>" for value in
                     (source, metrics["rows"], metrics["log_loss"], metrics["touch_auc"], metrics["side_accuracy"])) + "</tr>")
    lines.append("</table>")
    for key in ("notes", "paired_log_loss_difference", "metrics", "simulation", "per_coin", "per_year"):
        lines.append(f"<details><summary>{html.escape(key.replace('_', ' ').title())}</summary><pre>{html.escape(json.dumps(report[key], indent=2))}</pre></details>")
    lines.append("</html>")
    return "\n".join(lines)


def evaluate(directory):
    directory = Path(directory)
    metadata = json.loads((directory / "metadata.json").read_text())
    frame = pl.read_parquet(directory / "oof.parquet")
    if frame.is_empty():
        raise ValueError("No OOF observations")
    if frame["ts_ms"].max() >= metadata["lockbox_exclusive_ms"]:
        raise ValueError("OOF contains sealed lockbox rows")
    config = metadata["config"]
    report = evaluate_frame(frame, config)
    report.update(target=metadata["target"], version=metadata["version"], git_sha=metadata["git_sha"],
                  generated_at=datetime.now(timezone.utc).isoformat(),
                  simulation=simulation_report(frame, config, metadata["target"]))
    (directory / "report.json").write_text(json.dumps(report, indent=2, allow_nan=False))
    (directory / "report.html").write_text(render_html(report))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory")
    args = parser.parse_args()
    report = evaluate(args.directory)
    print(json.dumps({"model": report["metrics"]["model"], "trades": report["simulation"]["trades"]}))


if __name__ == "__main__":
    main()
