"""Archive closed UTC days to S3 (plan.md §7).

    uv run python -m archiver.archive_daily [--date YYYY-MM-DD | --verify YYYY-MM-DD]
"""

import argparse
import gzip
import hashlib
import logging
import math
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
import orjson
from botocore.exceptions import ClientError

from archiver.record_raw import OPEN, stream_path
from collectors.common.config import all_streams, connections, data_dir, load_config
from collectors.common.lock import single_instance

log = logging.getLogger("archive")
BLOCK = 1 << 20


def verify_gzip(body, meta: dict) -> None:
    sha, lines = hashlib.sha256(), 0
    with gzip.GzipFile(fileobj=body, mode="rb") as f:
        while block := f.read(BLOCK):
            sha.update(block)
            lines += block.count(b"\n")
    if sha.hexdigest() != meta["sha256"] or lines != meta["lines"]:
        raise ValueError("gzip sha256/line count mismatch")


def compress(path: Path, meta: dict) -> Path:
    gz = path.with_name(path.name + ".gz")
    with open(path, "rb") as src, gzip.open(gz, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst, length=BLOCK)
    with open(gz, "rb") as f:
        verify_gzip(f, meta)
    return gz


class DailyArchive:
    def __init__(self, root: Path, cfg: dict, s3, bucket: str, delete_after_h: float | None = None):
        if delete_after_h is not None and (not math.isfinite(delete_after_h) or delete_after_h < 0):
            raise ValueError("ARCHIVE_DELETE_LOCAL_AFTER_H must be a finite nonnegative number")
        self.root = root
        self.streams = all_streams(cfg)
        self.skipped = {
            s.key for exchange in cfg["exchanges"] for spec in connections(cfg, exchange)
            if spec.options.get("auth") and not os.environ.get("KRAKEN_API_KEY") for s in spec.streams
        }
        self.s3 = s3
        self.bucket = bucket
        self.delete_after_h = delete_after_h
        self.errors = 0

    def _get_manifest(self, day: str) -> dict | None:
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=f"manifests/date={day}.json")
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound"):
                return None
            raise
        with closing(response["Body"]) as body:
            return orjson.loads(body.read())

    def _head(self, key: str) -> dict | None:
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound"):
                return None
            raise

    def _upload(self, path: Path) -> dict:
        meta = orjson.loads(path.with_name(path.name + ".meta.json").read_bytes())
        info = {k: meta[k] for k in ("lines", "bytes", "sha256", "offset_gaps", "foreign_day_records")}
        info.update(first_offset=meta.get("first_offset"), last_offset=meta.get("last_offset"))
        key = path.relative_to(self.root).as_posix() + ".gz"
        info["key"] = key
        head = self._head(key)
        if head is not None and head.get("Metadata", {}).get("raw-sha256") == meta["sha256"]:
            info["gz_bytes"] = head["ContentLength"]
            return info
        gz = compress(path, meta)
        self.s3.upload_file(str(gz), self.bucket, key, ExtraArgs={
            "ChecksumAlgorithm": "SHA256", "ContentType": "application/gzip",
            "Metadata": {"raw-sha256": meta["sha256"], "lines": str(meta["lines"])},
        })
        head = self._head(key)
        if head is None or head.get("Metadata", {}).get("raw-sha256") != meta["sha256"]:
            raise ValueError(f"uploaded object metadata mismatch: {key}")
        info["gz_bytes"] = gz.stat().st_size
        gz.unlink()  # our verified upload's temporary compressed file
        return info

    def archive_day(self, day: str, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        if date.fromisoformat(day) >= now.date():
            raise ValueError("only past UTC dates can be archived")
        previous = self._get_manifest(day)
        if previous is not None and previous["status"] == "complete":
            self._delete_local(previous, now)
            return previous
        manifest = {"date": day, "status": "provisional", "files": [], "pending": [], "skipped": []}
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="gzip") as pool:
            tasks = {}
            for stream in self.streams:
                path = stream_path(self.root, stream, day)
                open_path = path.with_name(path.name + OPEN)
                key = path.relative_to(self.root).as_posix() + ".gz"
                if stream.key in self.skipped and not path.exists() and not open_path.exists():
                    manifest["skipped"].append({"key": key, "reason": "no API key"})
                    continue
                if open_path.exists():
                    reason = "open"
                elif not path.exists():
                    reason = "missing"
                elif not path.with_name(path.name + ".meta.json").exists():
                    reason = "missing metadata"
                else:
                    tasks[pool.submit(self._upload, path)] = key
                    continue
                manifest["pending"].append({"key": key, "reason": reason})
            for task in as_completed(tasks):
                key = tasks[task]
                try:
                    manifest["files"].append(task.result())
                except Exception as e:
                    self.errors += 1
                    log.error("%s: archive failed: %r", key, e)
                    manifest["pending"].append({"key": key, "reason": "upload failed"})
        # TODO: archive data/engine files when the engine exists.
        manifest["files"].sort(key=lambda f: f["key"])
        manifest["pending"].sort(key=lambda f: f["key"])
        manifest["has_offset_gaps"] = any(f["offset_gaps"] for f in manifest["files"])
        if not manifest["pending"]:
            manifest["status"] = "complete"
            manifest["completed_at"] = now.isoformat()
        elif now >= datetime.fromisoformat(day).replace(tzinfo=timezone.utc) + timedelta(days=1, hours=6):
            log.warning("%s overdue: %d pending streams", day, len(manifest["pending"]))
        self.s3.put_object(
            Bucket=self.bucket, Key=f"manifests/date={day}.json",
            Body=orjson.dumps(manifest, option=orjson.OPT_INDENT_2),
            ContentType="application/json", ChecksumAlgorithm="SHA256",
        )
        log.info("%s %s: %d files, %d pending, %d skipped", day, manifest["status"],
                 len(manifest["files"]), len(manifest["pending"]), len(manifest["skipped"]))
        self._delete_local(manifest, now)
        return manifest

    def _delete_local(self, manifest: dict, now: datetime) -> None:
        if self.delete_after_h is None or manifest["status"] != "complete":
            return
        completed = datetime.fromisoformat(manifest["completed_at"])
        if now - completed < timedelta(hours=self.delete_after_h):
            return
        paths = []
        for file in manifest["files"]:
            key = file["key"]
            if not key.startswith(f"raw/date={manifest['date']}/") or ".." in Path(key).parts or not key.endswith(".jsonl.gz"):
                raise ValueError(f"invalid archive key: {key}")
            head = self._head(key)
            if head is None or head.get("Metadata", {}).get("raw-sha256") != file["sha256"]:
                raise ValueError(f"refusing local deletion: object metadata mismatch: {key}")
            paths.append(self.root / key.removesuffix(".gz"))
        for path in paths:
            path.unlink(missing_ok=True)
            path.with_name(path.name + ".meta.json").unlink(missing_ok=True)
        log.info("%s: deleted %d archived local files", manifest["date"], len(paths))

    def verify_day(self, day: str) -> None:
        date.fromisoformat(day)
        manifest = self._get_manifest(day)
        if manifest is None:
            raise ValueError(f"no manifest for {day}")
        if manifest["status"] != "complete":
            raise ValueError(f"{day} is still provisional")
        for file in manifest["files"]:
            response = self.s3.get_object(Bucket=self.bucket, Key=file["key"])
            with closing(response["Body"]) as body:
                verify_gzip(body, file)
        log.info("%s verified: %d files", day, len(manifest["files"]))

    def run(self, now: datetime | None = None) -> None:
        now = now or datetime.now(timezone.utc)
        for path in sorted((self.root / "raw").glob("date=*")):
            day = path.name.removeprefix("date=")
            if date.fromisoformat(day) < now.date() and any(path.rglob("*.jsonl*")):
                try:
                    self.archive_day(day, now)
                except Exception:
                    self.errors += 1
                    log.exception("%s: archive failed", day)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    dates = parser.add_mutually_exclusive_group()
    dates.add_argument("--date", help="archive one past UTC date")
    dates.add_argument("--verify", metavar="DATE", help="download and verify a complete day")
    args = parser.parse_args()
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(name)s: %(message)s")
    try:
        bucket = os.environ["S3_BUCKET"]
        if not bucket:
            raise ValueError("set S3_BUCKET")
        if not os.environ.get("AWS_PROFILE"):
            os.environ["AWS_PROFILE"] = "crypto-archiver"
        root = data_dir()
        lock = single_instance(root / "state/archive.lock")  # noqa: F841 - held until exit
        retention = os.environ.get("ARCHIVE_DELETE_LOCAL_AFTER_H", "").strip()
        archive = DailyArchive(root, load_config(), boto3.client("s3"), bucket, float(retention) if retention else None)
        if args.verify:
            archive.verify_day(args.verify)
        elif args.date:
            archive.archive_day(args.date)
        else:
            archive.run()
    except Exception:
        log.exception("archive job failed")
        raise SystemExit(1) from None
    raise SystemExit(1 if archive.errors else 0)


if __name__ == "__main__":
    main()
