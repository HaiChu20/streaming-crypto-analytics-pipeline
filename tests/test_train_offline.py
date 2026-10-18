"""Local-only launcher checks; actual release replay on tiny archive excerpts."""
import json
import os
from pathlib import Path
import subprocess

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts/train_offline.sh"
FIXTURES = REPO / "tests/fixtures/archive"


def run(archive, work, *args):
    env = dict(os.environ, PYTHON_DOTENV_DISABLED="1", POLARS_MAX_THREADS="2",
               OMP_NUM_THREADS="2", OPENBLAS_NUM_THREADS="2")
    return subprocess.run([str(SCRIPT), str(archive), str(work), "--jobs", "1", *args],
                          cwd=REPO, env=env, text=True, capture_output=True, timeout=180)


def test_train_offline_replays_both_fixture_days_and_resumes(tmp_path):
    work = tmp_path / "offline work"
    result = run(FIXTURES, work, "--version", "fixture")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "not enough labeled rows" in result.stdout
    assert "No targets could train" in result.stdout
    completed = {}
    for day, records in [("2026-09-15", 33511), ("2026-10-08", 75235)]:
        feature = work / f"features/date={day}/features.parquet"
        assert feature.is_file()
        completed[day] = feature.stat().st_mtime_ns
        report = json.loads((work / f"logs/{day}.json").read_text())
        assert report["records"] == records and report["book_breaks"] == 0
        assert report["output"] == str(feature)
        assert (work / f"logs/{day}.log").is_file()
    report = json.loads((work / "research_report_fixture.json").read_text())
    assert report["trained"] == 0 and report["skipped"] == 8
    assert not (work / "models").exists()
    resumed = run(FIXTURES, work, "--version", "resume")
    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    assert resumed.stdout.count("complete features already exist") == 2
    for day, modified in completed.items():
        assert (work / f"features/date={day}/features.parquet").stat().st_mtime_ns == modified


def test_failed_day_stops_training_unless_explicitly_allowed(tmp_path):
    archive = tmp_path / "archive/BinanceRaw/Spotv6/BTCUSDT"
    archive.mkdir(parents=True)
    (archive / "20260915_snapshot.jsonl").write_text(json.dumps({
        "channel":"snapshot", "event":"anchor", "recv_time_ns":1789430401000000000,
        "data":{"lastUpdateId":1,"bids":[["100","1"]],"asks":[["101","1"]]}
    }) + "\n")
    (archive / "20260916_depth.jsonl").write_text("broken JSON\n")
    # Foreign exchange trees and unknown channels do not discover extra days.
    ignored = tmp_path / "archive/Other/Spot/BTCUSDT"
    ignored.mkdir(parents=True)
    (ignored / "20260917_depth.jsonl").write_text("ignored\n")
    (archive / "20260918_unknown.jsonl").write_text("ignored\n")
    work = tmp_path / "work"
    failed = run(tmp_path / "archive", work, "--version", "failed")
    assert failed.returncode != 0
    assert "Training stopped" in failed.stderr and "2026-09-16" in failed.stderr
    assert not (work / "research_report_failed.json").exists()
    assert not (work / "features/date=2026-09-16/features.parquet").exists()
    allowed = run(tmp_path / "archive", work, "--allow-missing-days", "--version", "allowed")
    assert allowed.returncode == 0, allowed.stdout + allowed.stderr
    assert "training without failed days: 2026-09-16" in allowed.stderr
    report = json.loads((work / "research_report_allowed.json").read_text())
    assert len(report["input_files"]) == 1 and report["trained"] == 0
    filtered = run(tmp_path / "archive", work, "--from", "2026-09-15", "--to", "2026-09-15", "--version", "filtered")
    assert filtered.returncode == 0, filtered.stdout + filtered.stderr
    assert "2026-09-16" not in filtered.stdout + filtered.stderr
