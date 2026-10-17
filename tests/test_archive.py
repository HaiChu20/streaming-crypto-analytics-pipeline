"""Daily S3 archive with an in-memory client; no AWS credentials or network."""

import gzip
import io
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import orjson
import pytest
from botocore.exceptions import ClientError

from archiver import archive_daily
from archiver.archive_daily import DailyArchive
from archiver.record_raw import OPEN, stream_path, write_meta
from collectors.common.config import all_streams
from collectors.common.envelope import data_record

DAY = "2026-10-16"
NOW = datetime(2026, 10, 17, 7, tzinfo=timezone.utc)
CFG = {
    "topic_version": "v1", "partitions": {"BTC": 0, "ETH": 1},
    "exchanges": {"kraken": {"spot": {
        "symbols": {"BTC": "BTC/USD"},
        "connections": {"spot": {"url": "ws://unused", "feeds": {"l2": "book"}}},
    }}},
}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.events = []

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key]["body"])}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        obj = self.objects[Key]
        return {"Metadata": dict(obj["meta"]), "ContentLength": len(obj["body"])}

    def upload_file(self, Filename, Bucket, Key, ExtraArgs):
        assert ExtraArgs["ChecksumAlgorithm"] == "SHA256"
        self.objects[Key] = {"body": Path(Filename).read_bytes(), "meta": ExtraArgs["Metadata"]}
        self.events.append(("upload", Key))

    def put_object(self, Bucket, Key, Body, **kwargs):
        assert kwargs["ChecksumAlgorithm"] == "SHA256"
        self.objects[Key] = {"body": Body, "meta": {}}
        self.events.append(("manifest", Key))


@pytest.fixture(autouse=True)
def no_credentials(monkeypatch):
    monkeypatch.delenv("KRAKEN_API_KEY", raising=False)
    monkeypatch.setattr(archive_daily.boto3, "client", lambda *a, **k: pytest.fail("real S3 client requested"))


def config(two=False, auth=False):
    cfg = deepcopy(CFG)
    if two:
        cfg["exchanges"]["kraken"]["spot"]["symbols"]["ETH"] = "ETH/USD"
    if auth:
        cfg["exchanges"]["kraken"]["spot"]["connections"]["l3"] = {
            "url": "ws://unused", "feeds": {"l3": "level3"}, "auth": True,
        }
    return cfg


def day_file(root, stream, day=DAY, opened=False, gaps=False):
    path = stream_path(root, stream, day)
    if opened:
        path = path.with_name(path.name + OPEN)
    path.parent.mkdir(parents=True, exist_ok=True)
    recv_ns = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    lines = []
    for offset in (0, 2 if gaps else 1):
        value = data_record(stream, "snapshot", "c", offset, recv_ns, "{}")
        lines.append(
            value[:-1] + b',"topic":"%s","partition":%d,"offset":%d}\n' %
            (stream.topic.encode(), stream.partition, offset)
        )
    path.write_bytes(b"".join(lines))
    if not opened:
        write_meta(path)
    return path


@pytest.mark.parametrize("gaps", [False, True])
def test_complete_day_uploads_manifest_last_and_reruns_nothing(tmp_path, gaps):
    cfg, s3 = config(two=True), FakeS3()
    paths = [day_file(tmp_path, s, gaps=gaps) for s in all_streams(cfg)]
    job = DailyArchive(tmp_path, cfg, s3, "bucket")
    manifest = job.archive_day(DAY, NOW)
    assert manifest["status"] == "complete"
    assert manifest["has_offset_gaps"] is gaps
    assert not manifest["pending"]
    assert len(manifest["files"]) == 2
    assert s3.events[-1] == ("manifest", f"manifests/date={DAY}.json")
    for path, info in zip(sorted(paths), manifest["files"]):
        meta = orjson.loads(path.with_name(path.name + ".meta.json").read_bytes())
        assert info["sha256"] == meta["sha256"]
        assert info["lines"] == 2
        assert info["bytes"] == path.stat().st_size
        assert info["gz_bytes"] == len(s3.objects[info["key"]]["body"])
        assert (info["first_offset"], info["last_offset"]) == (0, 2 if gaps else 1)
        assert info["offset_gaps"] == ([[1, 2]] if gaps else [])
        assert info["foreign_day_records"] == 0
        assert not path.with_name(path.name + ".gz").exists()
    events = list(s3.events)
    assert job.archive_day(DAY, NOW + timedelta(hours=1)) == manifest
    assert s3.events == events
    job.verify_day(DAY)


@pytest.mark.parametrize("hour,overdue", [(5, False), (6, True)])
def test_open_and_missing_streams_are_provisional(tmp_path, caplog, hour, overdue):
    cfg, s3 = config(two=True), FakeS3()
    day_file(tmp_path, all_streams(cfg)[0], opened=True)
    job = DailyArchive(tmp_path, cfg, s3, "bucket", delete_after_h=0)
    manifest = job.archive_day(DAY, NOW.replace(hour=hour))
    assert manifest["status"] == "provisional"
    assert {p["reason"] for p in manifest["pending"]} == {"open", "missing"}
    assert ("overdue" in caplog.text) is overdue
    assert not any(ev == "upload" for ev, _ in s3.events)


def test_gzip_verification_mismatch_uploads_no_raw_data(tmp_path, caplog):
    s3 = FakeS3()
    path = day_file(tmp_path, all_streams(CFG)[0])
    meta_path = path.with_name(path.name + ".meta.json")
    meta = orjson.loads(meta_path.read_bytes())
    meta["sha256"] = "0" * 64
    meta_path.write_bytes(orjson.dumps(meta))
    job = DailyArchive(tmp_path, CFG, s3, "bucket")
    assert job.archive_day(DAY, NOW)["status"] == "provisional"
    assert job.errors == 1
    assert "mismatch" in caplog.text
    assert not any(ev == "upload" for ev, _ in s3.events)
    assert path.exists()


def test_partial_rerun_skips_matching_object(tmp_path, monkeypatch):
    cfg, s3 = config(two=True), FakeS3()
    streams = all_streams(cfg)
    first = day_file(tmp_path, streams[0])
    job = DailyArchive(tmp_path, cfg, s3, "bucket")
    assert job.archive_day(DAY, NOW)["status"] == "provisional"
    day_file(tmp_path, streams[1])
    compress = archive_daily.compress

    def only_new(path, meta):
        assert path != first
        return compress(path, meta)

    monkeypatch.setattr(archive_daily, "compress", only_new)
    assert job.archive_day(DAY, NOW)["status"] == "complete"
    assert len([e for e in s3.events if e[0] == "upload"]) == 2
    assert not job.errors


@pytest.mark.parametrize("corrupt", ["content", "count"])
def test_verify_detects_corrupted_object_or_count(tmp_path, corrupt):
    s3 = FakeS3()
    day_file(tmp_path, all_streams(CFG)[0])
    job = DailyArchive(tmp_path, CFG, s3, "bucket")
    manifest = job.archive_day(DAY, NOW)
    if corrupt == "content":
        s3.objects[manifest["files"][0]["key"]]["body"] = gzip.compress(b"corrupt\n")
    else:
        manifest["files"][0]["lines"] += 1
        s3.objects[f"manifests/date={DAY}.json"]["body"] = orjson.dumps(manifest)
    with pytest.raises(ValueError, match="mismatch"):
        job.verify_day(DAY)


@pytest.mark.parametrize("enabled,age,deleted", [(None, 72, False), (48, 24, False), (48, 72, True)])
def test_deletion_only_enabled_complete_and_aged(tmp_path, enabled, age, deleted):
    s3 = FakeS3()
    path = day_file(tmp_path, all_streams(CFG)[0])
    DailyArchive(tmp_path, CFG, s3, "bucket").archive_day(DAY, NOW)
    job = DailyArchive(tmp_path, CFG, s3, "bucket", delete_after_h=enabled)
    job.archive_day(DAY, NOW + timedelta(hours=age))
    assert path.exists() is not deleted
    assert path.with_name(path.name + ".meta.json").exists() is not deleted


def test_deletion_checks_all_heads_before_deleting_anything(tmp_path):
    cfg, s3 = config(two=True), FakeS3()
    paths = [day_file(tmp_path, s) for s in all_streams(cfg)]
    manifest = DailyArchive(tmp_path, cfg, s3, "bucket").archive_day(DAY, NOW)
    s3.objects[manifest["files"][-1]["key"]]["meta"]["raw-sha256"] = "bad"
    job = DailyArchive(tmp_path, cfg, s3, "bucket", delete_after_h=48)
    with pytest.raises(ValueError, match="refusing local deletion"):
        job.archive_day(DAY, NOW + timedelta(hours=72))
    assert all(p.exists() for p in paths)


def test_provisional_day_keeps_uploaded_local_files(tmp_path):
    cfg, s3 = config(two=True), FakeS3()
    path = day_file(tmp_path, all_streams(cfg)[0])
    job = DailyArchive(tmp_path, cfg, s3, "bucket", delete_after_h=0)
    assert job.archive_day(DAY, NOW)["status"] == "provisional"
    assert path.exists()
    assert path.with_name(path.name + ".meta.json").exists()


@pytest.mark.parametrize("hours", [-1, float("nan"), float("inf")])
def test_invalid_retention_rejected(tmp_path, hours):
    with pytest.raises(ValueError, match="finite nonnegative"):
        DailyArchive(tmp_path, CFG, FakeS3(), "bucket", delete_after_h=hours)


@pytest.mark.parametrize("key", [None, "", "fake-key"])
def test_auth_streams_skipped_without_key(tmp_path, monkeypatch, key):
    cfg, s3 = config(auth=True), FakeS3()
    if key is not None:
        monkeypatch.setenv("KRAKEN_API_KEY", key)
    day_file(tmp_path, all_streams(cfg)[0])
    manifest = DailyArchive(tmp_path, cfg, s3, "bucket").archive_day(DAY, NOW)
    assert manifest["status"] == ("provisional" if key else "complete")
    assert len(manifest["skipped"]) == (0 if key else 1)
    if not key:
        assert manifest["skipped"][0]["reason"] == "no API key"


@pytest.mark.parametrize("opened", [False, True])
def test_existing_auth_file_not_skipped_without_key(tmp_path, opened):
    cfg, s3 = config(auth=True), FakeS3()
    public, auth = all_streams(cfg)
    day_file(tmp_path, public)
    path = day_file(tmp_path, auth, opened=opened)
    manifest = DailyArchive(tmp_path, cfg, s3, "bucket").archive_day(DAY, NOW)
    assert not manifest["skipped"]
    assert manifest["status"] == ("provisional" if opened else "complete")
    if opened:
        assert manifest["pending"][0]["reason"] == "open"
    else:
        assert len(manifest["files"]) == 2
        assert path.relative_to(tmp_path).as_posix() + ".gz" in s3.objects


def test_run_catches_up_only_past_dates(tmp_path):
    s3 = FakeS3()
    (tmp_path / "raw/date=2026-10-14").mkdir(parents=True)  # already cleaned up
    for day in ("2026-10-15", DAY, "2026-10-17", "2026-10-18"):
        day_file(tmp_path, all_streams(CFG)[0], day)
    DailyArchive(tmp_path, CFG, s3, "bucket").run(NOW)
    manifests = [key for key in s3.objects if key.startswith("manifests/")]
    assert sorted(manifests) == ["manifests/date=2026-10-15.json", f"manifests/date={DAY}.json"]


def test_verify_cli_exits_one_on_corruption(tmp_path, monkeypatch):
    s3 = FakeS3()
    day_file(tmp_path, all_streams(CFG)[0])
    manifest = DailyArchive(tmp_path, CFG, s3, "bucket").archive_day(DAY, NOW)
    s3.objects[manifest["files"][0]["key"]]["body"] = b"bad gzip"
    def client(*args, **kwargs):
        assert archive_daily.os.environ["AWS_PROFILE"] == "crypto-archiver"
        return s3

    monkeypatch.setattr(archive_daily.boto3, "client", client)
    monkeypatch.setattr(archive_daily, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(archive_daily, "load_config", lambda: CFG)
    monkeypatch.setenv("S3_BUCKET", "bucket")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setattr("sys.argv", ["archive_daily", "--verify", DAY])
    with pytest.raises(SystemExit) as exc:
        archive_daily.main()
    assert exc.value.code == 1
