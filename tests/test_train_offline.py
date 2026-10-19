"""Local-only launcher checks; actual release replay on tiny archive excerpts."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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
    assert report["trained"] == 0 and report["skipped"] == 12
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


@pytest.fixture
def launcher(tmp_path):
    """Exercise Bash with local build/replay/training stubs and real Parquet metadata."""
    repo = tmp_path / "repo"
    script = repo / "scripts/train_offline.sh"
    script.parent.mkdir(parents=True)
    script.write_bytes(SCRIPT.read_bytes())
    python = repo / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    python.write_text(f'''#!{sys.executable}
import os
from pathlib import Path
import sys
if sys.argv[1:3] == ['-m', 'research.train']:
    (Path(sys.argv[3]) / 'trained').write_text('trained')
else:
    os.execv({sys.executable!r}, [{sys.executable!r}, *sys.argv[1:]])
''')
    python.chmod(0o755)
    replay = repo / "engine/target/release/engine-replay"
    replay.parent.mkdir(parents=True)
    replay.write_text(f'''#!{sys.executable}
import json
from pathlib import Path
import sys
import pyarrow as pa
import pyarrow.parquet as pq
if sys.argv[1:] == ['--engine-version']:
    print('9.8.7-features-test')
    sys.exit(0)
args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
day = args['--date']
staging = Path(args['--out'])
work = staging.parent
target = work / f'features/date={{day}}/features.parquet'
output = staging / f'date={{day}}/features.parquet'
output.parent.mkdir(parents=True)
output.write_bytes(b'incomplete parquet')
if target.exists():
    (work / f'observed-{{day}}').write_bytes(target.read_bytes())
if (work / 'fail-replay').exists():
    sys.exit(1)
version = b'old' if (work / 'wrong-version').exists() else b'9.8.7-features-test'
table = pa.table({{'new_feature': [74]}}).replace_schema_metadata(
    {{b'engine_version': version}})
pq.write_table(table, output)
print(json.dumps({{'output': str(output)}}))
''')
    replay.chmod(0o755)
    bash_env = tmp_path / "build-stub.sh"
    bash_env.write_text('cargo() { echo "stub build"; }\n')
    archive = tmp_path / "archive"
    pair = archive / "BinanceRaw/Spotv6/BTCUSDT"
    pair.mkdir(parents=True)
    for day in ("20260915", "20260916"):
        (pair / f"{day}_snapshot.jsonl").write_text("stub archive\n")
    work = tmp_path / "work"
    work.mkdir()

    def launch(*args):
        return subprocess.run(
            ["bash", str(script), str(archive), str(work), "--jobs", "1", *args],
            env=dict(os.environ, BASH_ENV=str(bash_env), PYTHON_DOTENV_DISABLED="1"),
            text=True, capture_output=True, timeout=30,
        )

    return work, launch


def write_feature(work, day, version):
    path = work / f"features/date={day}/features.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata = {} if version is None else {b"engine_version": version}
    pq.write_table(pa.table({"old_feature": [1]}).replace_schema_metadata(metadata), path)
    return path


def test_empty_engine_version_blocks_replay_and_training(launcher):
    work, launch = launcher
    replay = work.parent / "repo/engine/target/release/engine-replay"
    replay.write_text(f"#!{sys.executable}\n")
    result = launch()
    assert result.returncode != 0
    assert "empty engine version" in result.stderr
    assert not list(work.glob(".replay-*"))
    assert not (work / "trained").exists()


def test_matching_engine_version_reuses_existing_day(launcher):
    work, launch = launcher
    feature = write_feature(work, "2026-09-15", b"9.8.7-features-test")
    before = feature.read_bytes(), feature.stat()
    result = launch()
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Skip 2026-09-15: complete features already exist" in result.stdout
    assert feature.read_bytes() == before[0]
    assert feature.stat().st_mtime_ns == before[1].st_mtime_ns
    assert not (work / "observed-2026-09-15").exists()
    assert (work / "trained").exists()


@pytest.mark.parametrize("version", [b"0.1.0-features-v2", None, b"corrupt-parquet"])
def test_stale_engine_versions_fail_before_replay_and_preserve_files(launcher, version):
    work, launch = launcher
    files = [write_feature(work, day, version) for day in ("2026-09-15", "2026-09-16")]
    if version == b"corrupt-parquet":
        for path in files:
            path.write_bytes(b"corrupt parquet")
    before = [(path.read_bytes(), path.stat().st_mtime_ns) for path in files]
    result = launch("--allow-missing-days")
    assert result.returncode != 0
    assert all(day in result.stderr for day in ("2026-09-15", "2026-09-16"))
    assert "engine_version" in result.stderr and "9.8.7-features-test" in result.stderr
    assert "new work dir" in result.stderr and "--rebuild-stale" in result.stderr
    assert [(path.read_bytes(), path.stat().st_mtime_ns) for path in files] == before
    assert not list(work.glob(".replay-*"))
    assert not (work / "trained").exists()


def test_rebuild_stale_publishes_complete_file_atomically(launcher):
    work, launch = launcher
    stale = write_feature(work, "2026-09-15", b"old")
    matching = write_feature(work, "2026-09-16", b"9.8.7-features-test")
    old_bytes, old_inode = stale.read_bytes(), stale.stat().st_ino
    matching_stat = matching.stat()
    result = launch("--rebuild-stale")
    assert result.returncode == 0, result.stdout + result.stderr
    assert (work / "observed-2026-09-15").read_bytes() == old_bytes
    assert stale.stat().st_ino != old_inode
    assert pq.read_metadata(stale).metadata[b"engine_version"] == b"9.8.7-features-test"
    assert pq.ParquetFile(stale).read().to_pydict() == {"new_feature": [74]}
    assert matching.stat().st_ino == matching_stat.st_ino
    assert matching.stat().st_mtime_ns == matching_stat.st_mtime_ns
    assert (work / "trained").exists()


@pytest.mark.parametrize("failure", ["fail-replay", "wrong-version"])
def test_failed_stale_rebuild_preserves_old_file_and_blocks_training(launcher, failure):
    work, launch = launcher
    stale = write_feature(work, "2026-09-15", b"old")
    before = stale.read_bytes(), stale.stat().st_mtime_ns
    (work / failure).touch()
    result = launch("--rebuild-stale", "--allow-missing-days")
    assert result.returncode != 0
    assert "2026-09-15" in result.stderr
    assert (stale.read_bytes(), stale.stat().st_mtime_ns) == before
    assert not (work / "trained").exists()


@pytest.mark.parametrize("args", [(), ("--rebuild-stale",)])
def test_stale_day_outside_selected_archive_days_blocks_training(launcher, args):
    work, launch = launcher
    stale = write_feature(work, "2026-09-14", b"old")
    before = stale.read_bytes()
    result = launch(*args)
    assert result.returncode != 0
    assert "2026-09-14" in result.stderr
    assert stale.read_bytes() == before
    assert not (work / "trained").exists()
