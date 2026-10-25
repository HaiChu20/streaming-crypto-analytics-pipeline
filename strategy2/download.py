"""Download checksummed, resumable public history, with inclusive UTC dates.

Dataset names: perp_klines, spot_klines, premium_index, funding_rate, metrics,
book_depth, dvol, coinbase_klines, aggflow (opt-in). Monthly archives can contain
dates before --from. A partial final month uses daily klines/premium archives and REST funding
snapshots. Coinbase uses five requests per fully past UTC day.
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
from decimal import Decimal, InvalidOperation
import fcntl
import hashlib
import json
from io import BytesIO, StringIO
import os
from pathlib import Path
import re
import sys
import tempfile
import time
from threading import BoundedSemaphore, Lock

import httpx

BINANCE = "https://data.binance.vision/data/"
FUNDING = "https://fapi.binance.com/fapi/v1/fundingRate"
COINBASE = "https://api.exchange.coinbase.com/products/{coin}-USD/candles"
DERIBIT = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
USER_AGENT = "strategy2-public-history/1.0"
COINS = ("BTC", "ETH", "SOL", "XRP", "DOGE")
EXTRA_COINS = ("BNB", "ADA", "LINK", "AVAX", "LTC", "TRX", "DOT", "BCH", "ETC", "NEAR")
ALL_COINS = COINS + EXTRA_COINS
COINBASE_UNAVAILABLE = ("BNB", "TRX")
DATASETS = ("perp_klines", "spot_klines", "premium_index", "funding_rate",
            "metrics", "book_depth", "dvol", "coinbase_klines")
# Preserve the original default selection; large trade archives are opt-in.
ALL_DATASETS = DATASETS + ("aggflow",)
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


def _coinbase_url(coin: str, start: int, end: int) -> str:
    iso = lambda stamp: datetime.fromtimestamp(stamp / 1000, timezone.utc).isoformat()
    return str(httpx.URL(COINBASE.format(coin=coin), params={
        "granularity": 60, "start": iso(start), "end": iso(end)}))


# One limiter shared by all downloader threads, including retries. Serialize
# request starts at 125ms intervals, rather than allowing bursts of eight.
_COINBASE_LOCK = Lock()
_coinbase_last_request = float("-inf")


def _coinbase_throttle():
    global _coinbase_last_request
    with _COINBASE_LOCK:
        delay = .125 - (time.monotonic() - _coinbase_last_request)
        if delay > 0:
            time.sleep(delay)
        _coinbase_last_request = time.monotonic()


def plan_downloads(start: date, end: date, coins=COINS, datasets=DATASETS) -> list[Download]:
    """Plan one coverage row per source archive/month, without network or disk I/O."""
    if end < start:
        raise ValueError("--to must be on or after --from")
    if not coins or set(coins) - set(ALL_COINS):
        raise ValueError(f"coins must be selected from {','.join(ALL_COINS)}")
    if not datasets or set(datasets) - set(ALL_DATASETS):
        raise ValueError(f"datasets must be selected from {','.join(ALL_DATASETS)}")
    result = []
    for dataset in dict.fromkeys(datasets):
        for coin in dict.fromkeys(coins):
            if dataset == "dvol" and coin not in ("BTC", "ETH"):
                continue
            if dataset == "coinbase_klines":
                if coin in COINBASE_UNAVAILABLE:
                    continue
                day = start
                today = datetime.now(timezone.utc).date()
                while day <= end and day < today:
                    lower, upper = _ms(day), _ms(day + timedelta(days=1))
                    result.append(Download(dataset, coin, day.isoformat(),
                        _coinbase_url(coin, lower, min(lower + 300 * 60_000, upper) - 1),
                        f"coinbase/candles_1m/{coin}-USD/{day.isoformat()}.csv", lower, upper))
                    day += timedelta(days=1)
                continue
            month = start.replace(day=1)
            while month <= end:
                following = _next_month(month)
                first, last = max(start, month), min(end, following - timedelta(days=1))
                monthly = (
                    dataset in ("perp_klines", "spot_klines", "premium_index", "funding_rate", "aggflow")
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
                                    "book_depth": "bookDepth", "aggflow": "aggTrades"}[dataset]
                            path = f"futures/um/{cadence}/{kind}/{symbol}/{symbol}-{kind}-{period}.zip"
                        destination = (f"binance/aggflow/{coin}/{period}.parquet"
                                       if dataset == "aggflow" else "binance/" + path)
                        result.append(Download(dataset, coin, period, BINANCE + path, destination))
                        if monthly:
                            break
                        day += timedelta(days=1)
                month = following
    return result


def _get(client: httpx.Client, url: str, attempts: int = 4) -> httpx.Response:
    """Retry transient failures only; 404 is an ordinary coverage result."""
    for attempt in range(attempts):
        try:
            if httpx.URL(url).host == "api.exchange.coinbase.com":
                _coinbase_throttle()
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


# Two workers at this cap leave room for Arrow batches and minute aggregates
# within 6 GiB. Refuse unexpectedly large archives rather than spilling to disk.
AGGFLOW_ZIP_RAM_LIMIT = 1536 * 1024 * 1024


def _aggflow(item: Download, root: Path, client: httpx.Client) -> dict:
    from .aggflow import aggregate_zip
    import pyarrow.parquet as pq

    path = root / item.path

    def present(expected=None):
        metadata = pq.read_metadata(path).metadata or {}
        status = ("conflict" if expected is not None
                  and metadata.get(b"zip_sha256") != expected.encode() else "present")
        return _row(item, status, int(metadata.get(b"zip_bytes", b"0")))

    if path.exists():
        return present()
    response = _get(client, item.url + ".CHECKSUM")
    if response.status_code == 404:
        return _row(item, "missing")
    try:
        expected = _checksum(response.content)
    except (ValueError, UnicodeError):
        return _row(item, "checksum_failed")
    for attempt in range(4):
        try:
            with BytesIO() as compressed:
                digest = hashlib.sha256()
                size = 0
                with client.stream("GET", item.url, headers={"User-Agent": USER_AGENT}) as archive:
                    if archive.status_code == 404:
                        return _row(item, "missing")
                    archive.raise_for_status()
                    for chunk in archive.iter_raw():
                        size += len(chunk)
                        if size > AGGFLOW_ZIP_RAM_LIMIT:
                            raise ValueError("aggTrades archive exceeds RAM limit")
                        digest.update(chunk)
                        compressed.write(chunk)
                if digest.hexdigest() != expected:
                    return _row(item, "checksum_failed", size)
                compressed.seek(0)
                table = aggregate_zip(compressed).to_arrow()
            break
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            retryable = (isinstance(exc, httpx.TransportError)
                         or exc.response.status_code in (408, 429)
                         or exc.response.status_code >= 500)
            if not retryable or attempt == 3:
                raise
            time.sleep(.5 * 2**attempt)
    table = table.replace_schema_metadata({b"zip_bytes": str(size).encode(),
                                           b"zip_sha256": expected.encode()})
    output = BytesIO()
    pq.write_table(table, output)
    # Use the existing atomic no-clobber publisher; only aggregate bytes reach disk.
    if _atomic_write(path, output.getvalue()):
        return _row(item, "downloaded", size)
    return present(expected)


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


def _coinbase(item: Download, root: Path, client: httpx.Client) -> dict:
    path = root / item.path
    previous = None
    if path.exists():
        try:
            previous, lower, upper = _read_snapshot(path, item)
            if (lower, upper) != (item.start_ms, item.end_ms):
                raise ValueError("Coinbase range differs")
        except (OSError, ValueError, IndexError):
            return _row(item, "conflict", path.stat().st_size)
    points = {}
    # Fixed time windows cover empty minutes too: pagination must not stop at
    # a short/empty page. Half-open filtering removes API boundary overlap.
    for lower in range(item.start_ms, item.end_ms, 300 * 60_000):
        upper = min(lower + 300 * 60_000, item.end_ms)
        response = _get(client, _coinbase_url(item.coin, lower, upper - 1))
        if response.status_code == 404:
            return _row(item, "missing")
        # Preserve the exact JSON numeric lexemes (including trailing zeros).
        page = json.loads(response.content, parse_float=str)
        if not isinstance(page, list):
            raise ValueError(f"Coinbase API error: {str(page)[:160]}")
        if len(page) > 300:
            raise ValueError("Coinbase page exceeds 300 rows")
        for point in page:
            if (not isinstance(point, list) or len(point) != 6
                    or type(point[0]) is not int):
                raise ValueError("invalid Coinbase candle")
            stamp = point[0] * 1000
            if not lower <= stamp < upper:
                continue
            if stamp % 60_000:
                raise ValueError("off-grid Coinbase candle")
            try:
                values = [Decimal(str(value)) for value in point[1:]]
            except InvalidOperation as exc:
                raise ValueError("invalid Coinbase decimal") from exc
            if any(not value.is_finite() for value in values):
                raise ValueError("non-finite Coinbase decimal")
            _, low, high, opening, close, volume = point
            row = [stamp, opening, high, low, close, volume]
            if stamp in points and points[stamp] != row:
                raise ValueError("inconsistent duplicate Coinbase candle")
            points[stamp] = row
    content = _csv_bytes(("ts_ms", "open", "high", "low", "close", "volume"),
                         (points[stamp] for stamp in sorted(points)))
    if previous is not None:
        row = _row(item, "present" if content == previous else "conflict", len(previous))
    else:
        row = _save_snapshot(item, path, content, item.start_ms, item.end_ms)
    if not points and row["status"] in ("present", "downloaded"):
        row["error"] = "empty"
    return row


def download(item: Download, root: Path, client: httpx.Client) -> dict:
    if item.dataset == "aggflow":
        return _aggflow(item, root, client)
    if item.path.endswith(".csv"):
        with _snapshot_lock(root / item.path):
            handler = {"dvol": _dvol, "funding_rate": _funding, "coinbase_klines": _coinbase}[item.dataset]
            return handler(item, root, client)
    return _binance(item, root, client)


def run_downloads(plan: list[Download], root: Path, jobs: int = 4,
                  client: httpx.Client | None = None, *, aggflow_jobs: int = 2) -> list[dict]:
    """Download with bounded workers, then atomically replace the coverage report."""
    if jobs <= 0 or aggflow_jobs <= 0:
        raise ValueError("--jobs and --aggflow-jobs must be positive")
    if client is None:
        with httpx.Client(timeout=60, follow_redirects=True,
                          limits=httpx.Limits(max_connections=jobs), trust_env=False) as owned:
            return run_downloads(plan, root, jobs, owned, aggflow_jobs=aggflow_jobs)

    slots = BoundedSemaphore(jobs)

    def worker(item):
        with slots:
            try:
                return download(item, root, client)
            except Exception as exc:
                message = " ".join(f"{type(exc).__name__}: {exc}".split())[:240]
                return _row(item, "error", error=message)

    # Separate queues keep normal downloads moving while bounding archive RAM.
    with ThreadPoolExecutor(max_workers=jobs) as pool, \
            ThreadPoolExecutor(max_workers=min(jobs, aggflow_jobs)) as aggpool:
        futures = [(aggpool if item.dataset == "aggflow" else pool).submit(worker, item)
                   for item in plan]
        rows = [future.result() for future in futures]
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
    parser.add_argument("--coins", default=",".join(COINS), help="comma-separated coins, or all (15)")
    parser.add_argument("--datasets", default=",".join(DATASETS), help=",".join(ALL_DATASETS))
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--aggflow-jobs", type=int, default=2, help="aggflow workers, capped by --jobs")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.jobs <= 0 or args.aggflow_jobs <= 0:
            raise ValueError("--jobs and --aggflow-jobs must be positive")
        coins = (ALL_COINS if args.coins.strip().lower() == "all" else
                 tuple(part.strip().upper() for part in args.coins.split(",")))
        datasets = tuple(part.strip() for part in args.datasets.split(","))
        plan = plan_downloads(args.start, args.end, coins, datasets)
    except ValueError as exc:
        parser.error(str(exc))
    if "coinbase_klines" in datasets:
        skipped = [coin for coin in COINBASE_UNAVAILABLE if coin in coins]
        if skipped:
            print(f"Coverage note: Coinbase has no USD product for {', '.join(skipped)}; skipped")
    if args.dry_run:
        print(f"{len(plan)} files planned (ZIP archives also fetch each URL + .CHECKSUM)")
        for item in plan:
            print(item.url)
        return 0
    try:
        rows = run_downloads(plan, args.root, args.jobs, aggflow_jobs=args.aggflow_jobs)
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
