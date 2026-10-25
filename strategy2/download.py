"""Download checksummed, resumable public history, with inclusive UTC dates.

Dataset names: perp_klines, spot_klines, premium_index, funding_rate, metrics,
book_depth, dvol. Monthly archives can contain dates before --from. A partial
final month uses daily klines/premium archives and REST funding snapshots.
REST CSVs have local checksums plus range metadata. Partial DVOL snapshots can
only be replaced by strict supersets with unchanged overlapping values. Expanding
an existing DVOL CSV's range is a conflict; use another root for a new range.
"""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import fcntl
import hashlib
from io import StringIO
import os
from pathlib import Path
import re
import sys
import tempfile
import time

import httpx

BINANCE = "https://data.binance.vision/data/"
FUNDING = "https://fapi.binance.com/fapi/v1/fundingRate"
DERIBIT = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
USER_AGENT = "strategy2-public-history/1.0"
COINS = ("BTC", "ETH", "SOL", "XRP", "DOGE")
DATASETS = ("perp_klines", "spot_klines", "premium_index", "funding_rate",
            "metrics", "book_depth", "dvol")
HOUR_MS = 3_600_000
COVERAGE_FIELDS = ("dataset", "coin", "period", "url", "status", "bytes", "error", "missing_hours")


@dataclass(frozen=True)
class Download:
    dataset: str
    coin: str
    period: str
    url: str
    path: str
    start_ms: int = 0
    end_ms: int = 0  # exclusive, for REST snapshots


def _next_month(day: date) -> date:
    return (day.replace(day=28) + timedelta(days=4)).replace(day=1)


def _ms(day: date) -> int:
    return int(datetime.combine(day, datetime.min.time(), timezone.utc).timestamp() * 1000)


def _dvol_url(coin: str, start: int, end: int) -> str:
    return str(httpx.URL(DERIBIT, params={"currency": coin, "start_timestamp": start,
                                        "end_timestamp": end, "resolution": 3600}))


def _funding_url(coin: str, start: int, end: int) -> str:
    return str(httpx.URL(FUNDING, params={"symbol": f"{coin}USDT", "startTime": start,
                                        "endTime": end, "limit": 1000}))


def plan_downloads(start: date, end: date, coins=COINS, datasets=DATASETS) -> list[Download]:
    """Plan one coverage row per source archive/month, without network or disk I/O."""
    if end < start:
        raise ValueError("--to must be on or after --from")
    if not coins or set(coins) - set(COINS):
        raise ValueError(f"coins must be selected from {','.join(COINS)}")
    if not datasets or set(datasets) - set(DATASETS):
        raise ValueError(f"datasets must be selected from {','.join(DATASETS)}")
    result = []
    for dataset in dict.fromkeys(datasets):
        for coin in dict.fromkeys(coins):
            if dataset == "dvol" and coin not in ("BTC", "ETH"):
                continue
            month = start.replace(day=1)
            while month <= end:
                following = _next_month(month)
                first, last = max(start, month), min(end, following - timedelta(days=1))
                monthly = (
                    dataset in ("perp_klines", "spot_klines", "premium_index", "funding_rate")
                    and following <= end + timedelta(days=1))
                if dataset == "dvol":
                    lower, upper = _ms(first), _ms(last + timedelta(days=1))
                    period = month.strftime("%Y-%m")
                    result.append(Download(dataset, coin, period,
                                           _dvol_url(coin, lower, upper - 1),
                                           f"deribit/dvol/{coin}/1h/{period}.csv", lower, upper))
                elif dataset == "funding_rate" and not monthly:
                    lower, upper = _ms(first), _ms(last + timedelta(days=1))
                    period = f"{first.isoformat()}_{last.isoformat()}"
                    symbol = f"{coin}USDT"
                    path = f"binance/futures/um/rest/fundingRate/{symbol}/{symbol}-fundingRate-{period}.csv"
                    result.append(Download(dataset, coin, period,
                                           _funding_url(coin, lower, upper - 1), path, lower, upper))
                else:
                    day = first
                    while day <= last:
                        period = month.strftime("%Y-%m") if monthly else day.isoformat()
                        cadence = "monthly" if monthly else "daily"
                        symbol = f"{coin}USDT"
                        if dataset in ("perp_klines", "spot_klines", "premium_index"):
                            venue = "spot" if dataset == "spot_klines" else "futures/um"
                            kind = "premiumIndexKlines" if dataset == "premium_index" else "klines"
                            path = f"{venue}/{cadence}/{kind}/{symbol}/1m/{symbol}-1m-{period}.zip"
                        else:
                            kind = {"funding_rate": "fundingRate", "metrics": "metrics",
                                    "book_depth": "bookDepth"}[dataset]
                            path = f"futures/um/{cadence}/{kind}/{symbol}/{symbol}-{kind}-{period}.zip"
                        result.append(Download(dataset, coin, period, BINANCE + path, "binance/" + path))
                        if monthly:
                            break
                        day += timedelta(days=1)
                month = following
    return result


def _get(client: httpx.Client, url: str, attempts: int = 4) -> httpx.Response:
    """Retry transient failures only; 404 is an ordinary coverage result."""
    for attempt in range(attempts):
        try:
            response = client.get(url, headers={"User-Agent": USER_AGENT})
            if response.status_code == 404:
                return response
            response.raise_for_status()
            return response
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            retryable = (isinstance(exc, httpx.TransportError)
                         or exc.response.status_code in (408, 429)
                         or exc.response.status_code >= 500)
            if not retryable or attempt + 1 == attempts:
                raise
            time.sleep(0.5 * 2**attempt)
    raise AssertionError("unreachable")


def _digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _checksum(content: bytes) -> str:
    words = content.decode("ascii").split()
    if not words or not re.fullmatch(r"[a-fA-F0-9]{64}", words[0]):
        raise ValueError("invalid SHA256 checksum")
    return words[0].lower()


def _atomic_write(path: Path, content: bytes, *, replace: bool = False) -> bool:
    """Publish complete bytes atomically, preserving existing data by default.

    A hard-link/unlink implements a no-clobber rename across concurrent runs.
    Only our own temporary name is removed. Coverage reports and validated DVOL
    superset upgrades explicitly replace their previous files.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.",
                                     suffix=".part", delete=False) as output:
        temporary = Path(output.name)
        output.write(content)
        output.flush()
        os.fsync(output.fileno())
    try:
        if replace:
            temporary.replace(path)
        else:
            try:
                os.link(temporary, path)
            except FileExistsError:
                return False
        return True
    finally:
        if temporary.exists():
            temporary.unlink()


def _row(item: Download, status: str, size: int = 0, *, error: str = "",
         missing_hours: int = 0) -> dict:
    return dict(dataset=item.dataset, coin=item.coin, period=item.period,
                url=item.url, status=status, bytes=size, error=error, missing_hours=missing_hours)


def _binance(item: Download, root: Path, client: httpx.Client) -> dict:
    path = root / item.path
    checksum_path = path.with_name(path.name + ".CHECKSUM")
    response = _get(client, item.url + ".CHECKSUM")
    if response.status_code == 404:
        return _row(item, "missing")
    try:
        expected = _checksum(response.content)
    except (ValueError, UnicodeError):
        return _row(item, "checksum_failed")
    if path.exists() and _digest(path) != expected:
        return _row(item, "conflict", path.stat().st_size)
    if checksum_path.exists():
        try:
            if _checksum(checksum_path.read_bytes()) != expected:
                return _row(item, "conflict", path.stat().st_size if path.exists() else 0)
        except (ValueError, UnicodeError):
            return _row(item, "conflict", path.stat().st_size if path.exists() else 0)
    status = "present"
    if not path.exists():
        archive = _get(client, item.url)
        if archive.status_code == 404:
            return _row(item, "missing")
        if hashlib.sha256(archive.content).hexdigest() != expected:
            return _row(item, "checksum_failed", len(archive.content))
        if _atomic_write(path, archive.content):
            status = "downloaded"
        elif _digest(path) != expected:
            return _row(item, "conflict", path.stat().st_size)
    if not _atomic_write(checksum_path, response.content):
        try:
            if _checksum(checksum_path.read_bytes()) != expected:
                return _row(item, "conflict", path.stat().st_size)
        except (ValueError, UnicodeError):
            return _row(item, "conflict", path.stat().st_size)
    return _row(item, status, path.stat().st_size)


@contextmanager
def _snapshot_lock(path: Path):
    """Serialize snapshot reads/upgrades across processes; lock files persist."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_name(path.name + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _read_snapshot(path: Path, item: Download) -> tuple[bytes, int, int]:
    content = path.read_bytes()
    checksum = path.with_name(path.name + ".CHECKSUM").read_bytes()
    lower, upper = map(int, checksum.decode("ascii").splitlines()[1].split())
    if (hashlib.sha256(content).hexdigest() != _checksum(checksum)
            or lower > item.start_ms or upper < item.end_ms):
        raise ValueError("snapshot checksum or range differs")
    return content, lower, upper


def _csv_bytes(columns, rows) -> bytes:
    output = StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(columns)
    writer.writerows(rows)
    return output.getvalue().encode()


def _save_snapshot(item: Download, path: Path, content: bytes, lower: int, upper: int,
                   *, replace: bool = False) -> dict:
    checksum_path = path.with_name(path.name + ".CHECKSUM")
    expected = hashlib.sha256(content).hexdigest()
    checksum = f"{expected}  {path.name}\n{lower} {upper}\n".encode()
    if not replace and checksum_path.exists() and checksum_path.read_bytes() != checksum:
        return _row(item, "conflict")
    created = _atomic_write(path, content, replace=replace)
    if not created and _digest(path) != expected:
        return _row(item, "conflict", path.stat().st_size)
    if (not _atomic_write(checksum_path, checksum, replace=replace)
            and checksum_path.read_bytes() != checksum):
        return _row(item, "conflict", path.stat().st_size)
    return _row(item, "downloaded" if created else "present", path.stat().st_size)


def _dvol(item: Download, root: Path, client: httpx.Client) -> dict:
    path = root / item.path
    lower, upper = item.start_ms, item.end_ms
    old = {}
    expected = set(range(lower, upper, HOUR_MS))
    existing = path.exists()
    if existing:
        try:
            content, lower, upper = _read_snapshot(path, item)
            reader = csv.reader(StringIO(content.decode()))
            if next(reader) != ["ts_ms", "open", "high", "low", "close"]:
                raise ValueError("invalid DVOL header")
            for stamp, *values in reader:
                if len(values) != 4:
                    raise ValueError("invalid DVOL OHLC row")
                stamp = int(stamp)
                if stamp in old or stamp not in range(lower, upper, HOUR_MS):
                    raise ValueError("duplicate or off-grid DVOL hour")
                old[stamp] = [stamp, *map(float, values)]
        except (OSError, ValueError, IndexError, StopIteration):
            return _row(item, "conflict", path.stat().st_size)
        if expected <= old.keys():
            return _row(item, "present", path.stat().st_size)
    points = {}
    cursor = upper - 1
    while cursor >= lower:
        response = _get(client, _dvol_url(item.coin, lower, cursor))
        if response.status_code == 404:
            return _row(item, "missing", missing_hours=len(expected - old.keys()))
        payload = response.json()
        if "error" in payload:
            raise ValueError(f"Deribit API error: {payload['error']}")
        result = payload["result"]
        for point in result["data"]:
            if len(point) != 5 or not isinstance(point[0], int):
                raise ValueError("invalid DVOL OHLC row")
            if lower <= point[0] < upper:
                if (point[0] - lower) % HOUR_MS:
                    raise ValueError("off-grid DVOL hour")
                if point[0] in points and points[point[0]] != point:
                    raise ValueError("inconsistent duplicate DVOL hour")
                points[point[0]] = point
        continuation = result.get("continuation")
        if continuation is None:
            break
        if not isinstance(continuation, int) or continuation >= cursor:
            raise ValueError("DVOL continuation did not move backwards")
        cursor = continuation
    missing = len(expected - points.keys())
    if existing and (not old.keys() < points.keys()
                          or any(points[stamp] != point for stamp, point in old.items())):
        return _row(item, "conflict", path.stat().st_size,
                    missing_hours=len(expected - old.keys()))
    if not points:
        return _row(item, "missing", missing_hours=missing)
    content = _csv_bytes(("ts_ms", "open", "high", "low", "close"),
                         (points[stamp] for stamp in sorted(points)))
    row = _save_snapshot(item, path, content, lower, upper, replace=existing)
    if row["status"] != "conflict":
        row["missing_hours"] = missing
        if missing:
            row["status"] = "partial"
    return row


def _funding(item: Download, root: Path, client: httpx.Client) -> dict:
    path = root / item.path
    previous = None
    if path.exists():
        try:
            previous, lower, upper = _read_snapshot(path, item)
            if (lower, upper) != (item.start_ms, item.end_ms):
                raise ValueError("funding range differs")
        except (OSError, ValueError, IndexError):
            return _row(item, "conflict", path.stat().st_size)
    points = {}
    cursor = item.start_ms
    while cursor < item.end_ms:
        response = _get(client, _funding_url(item.coin, cursor, item.end_ms - 1))
        if response.status_code == 404:
            return _row(item, "missing")
        page = response.json()
        if not isinstance(page, list):
            raise ValueError(f"Binance funding API error: {str(page)[:160]}")
        if not page:
            break
        for point in page:
            stamp = point["fundingTime"]
            if (point["symbol"] != f"{item.coin}USDT" or not isinstance(stamp, int)
                    or not isinstance(point["fundingRate"], str)
                    or not isinstance(point["markPrice"], str)
                    or not cursor <= stamp < item.end_ms):
                raise ValueError("invalid funding row")
            row = [stamp, point["fundingRate"], point["markPrice"]]
            if stamp in points and points[stamp] != row:
                raise ValueError("inconsistent duplicate funding time")
            points[stamp] = row
        cursor = max(point["fundingTime"] for point in page) + 1
        if len(page) < 1000:
            break
    if not points:
        return _row(item, "conflict" if previous is not None else "missing")
    content = _csv_bytes(("funding_time_ms", "funding_rate", "mark_price"),
                         (points[stamp] for stamp in sorted(points)))
    if previous is not None:
        return _row(item, "present" if content == previous else "conflict", len(previous))
    return _save_snapshot(item, path, content, item.start_ms, item.end_ms)


def download(item: Download, root: Path, client: httpx.Client) -> dict:
    if item.path.endswith(".csv"):
        with _snapshot_lock(root / item.path):
            return (_dvol if item.dataset == "dvol" else _funding)(item, root, client)
    return _binance(item, root, client)


def run_downloads(plan: list[Download], root: Path, jobs: int = 4,
                  client: httpx.Client | None = None) -> list[dict]:
    """Download with bounded workers, then atomically replace the coverage report."""
    if jobs <= 0:
        raise ValueError("--jobs must be positive")
    if client is None:
        with httpx.Client(timeout=60, follow_redirects=True,
                          limits=httpx.Limits(max_connections=jobs), trust_env=False) as owned:
            return run_downloads(plan, root, jobs, owned)

    def worker(item):
        try:
            return download(item, root, client)
        except Exception as exc:
            message = " ".join(f"{type(exc).__name__}: {exc}".split())[:240]
            return _row(item, "error", error=message)

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        rows = list(pool.map(worker, plan))
    output = StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=COVERAGE_FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    _atomic_write(root / "coverage.csv", output.getvalue().encode(), replace=True)
    return rows


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--from", dest="start", type=date.fromisoformat, required=True)
    parser.add_argument("--to", dest="end", type=date.fromisoformat, required=True)
    parser.add_argument("--coins", default=",".join(COINS))
    parser.add_argument("--datasets", default=",".join(DATASETS), help=",".join(DATASETS))
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.jobs <= 0:
            raise ValueError("--jobs must be positive")
        plan = plan_downloads(args.start, args.end,
                              tuple(part.strip().upper() for part in args.coins.split(",")),
                              tuple(part.strip() for part in args.datasets.split(",")))
    except ValueError as exc:
        parser.error(str(exc))
    if args.dry_run:
        print(f"{len(plan)} files planned (ZIP archives also fetch each URL + .CHECKSUM)")
        for item in plan:
            print(item.url)
        return 0
    try:
        rows = run_downloads(plan, args.root, args.jobs)
    except (httpx.HTTPError, OSError, ValueError, KeyError, TypeError) as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        return 1
    totals = defaultdict(Counter)
    for row in rows:
        totals[row["dataset"]][row["status"]] += 1
        totals[row["dataset"]]["bytes"] += row["bytes"]
        if row.get("missing_hours"):
            totals[row["dataset"]]["missing_hours"] += row["missing_hours"]
    for dataset, counts in sorted(totals.items()):
        print(f"{dataset}: " + ", ".join(f"{key}={value}" for key, value in sorted(counts.items())))
    return int(any(row["status"] in ("error", "conflict", "checksum_failed") for row in rows))


def main(argv: list[str] | None = None) -> int:
    try:
        result = _main(argv)
        sys.stdout.flush()
        return result
    except BrokenPipeError:
        # Prevent another flush of the broken pipe during interpreter shutdown.
        with open(os.devnull, "w") as sink:
            os.dup2(sink.fileno(), sys.stdout.fileno())
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
