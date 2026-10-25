"""Download checksummed, resumable public history, with inclusive UTC dates.

Dataset names: perp_klines, spot_klines, premium_index, funding_rate, metrics,
book_depth, dvol. Monthly archives can contain dates before --from. A partial
final month uses daily klines/premium archives; funding is always monthly.
DVOL CSVs cover the requested dates and have local checksums plus range metadata
(Deribit supplies no checksum). Expanding an existing CSV's range is a conflict;
use another root to preserve the original snapshot.
"""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
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
DERIBIT = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
USER_AGENT = "strategy2-public-history/1.0"
COINS = ("BTC", "ETH", "SOL", "XRP", "DOGE")
DATASETS = ("perp_klines", "spot_klines", "premium_index", "funding_rate",
            "metrics", "book_depth", "dvol")
HOUR_MS = 3_600_000
COVERAGE_FIELDS = ("dataset", "coin", "period", "url", "status", "bytes")


@dataclass(frozen=True)
class Download:
    dataset: str
    coin: str
    period: str
    url: str
    path: str
    start_ms: int = 0
    end_ms: int = 0  # exclusive, for DVOL only


def _next_month(day: date) -> date:
    return (day.replace(day=28) + timedelta(days=4)).replace(day=1)


def _ms(day: date) -> int:
    return int(datetime.combine(day, datetime.min.time(), timezone.utc).timestamp() * 1000)


def _dvol_url(coin: str, start: int, end: int) -> str:
    return str(httpx.URL(DERIBIT, params={"currency": coin, "start_timestamp": start,
                                        "end_timestamp": end, "resolution": 3600}))


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
                monthly = dataset == "funding_rate" or (
                    dataset in ("perp_klines", "spot_klines", "premium_index")
                    and following <= end + timedelta(days=1))
                if dataset == "dvol":
                    lower, upper = _ms(first), _ms(last + timedelta(days=1))
                    period = month.strftime("%Y-%m")
                    result.append(Download(dataset, coin, period,
                                           _dvol_url(coin, lower, upper - 1),
                                           f"deribit/dvol/{coin}/1h/{period}.csv", lower, upper))
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
    """Publish complete bytes atomically; data files must never replace a winner.

    A hard-link/unlink implements a no-clobber rename across concurrent runs.
    Only our own temporary name is removed. Coverage reports explicitly replace
    the previous report, while archives, CSVs and checksums never do.
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


def _row(item: Download, status: str, size: int = 0) -> dict:
    return dict(dataset=item.dataset, coin=item.coin, period=item.period,
                url=item.url, status=status, bytes=size)


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


def _dvol(item: Download, root: Path, client: httpx.Client) -> dict:
    path = root / item.path
    checksum_path = path.with_name(path.name + ".CHECKSUM")
    # Locally generated sidecars pin both the bytes and the requested interval.
    if path.exists():
        try:
            checksum = checksum_path.read_bytes()
            bounds = checksum.decode("ascii").splitlines()[1].split()
            lower, upper = map(int, bounds)
            valid = (_digest(path) == _checksum(checksum)
                     and lower <= item.start_ms and upper >= item.end_ms)
        except (OSError, ValueError, IndexError, UnicodeError):
            valid = False
        return _row(item, "present" if valid else "conflict", path.stat().st_size)
    points = {}
    upper = item.end_ms - 1
    while upper >= item.start_ms:
        response = _get(client, _dvol_url(item.coin, item.start_ms, upper))
        if response.status_code == 404:
            return _row(item, "missing")
        payload = response.json()
        if "error" in payload:
            raise ValueError(f"Deribit API error: {payload['error']}")
        result = payload["result"]
        for point in result["data"]:
            if len(point) != 5 or not isinstance(point[0], int):
                raise ValueError("invalid DVOL OHLC row")
            if item.start_ms <= point[0] < item.end_ms:
                points[point[0]] = point
        continuation = result.get("continuation")
        if continuation is None:
            break
        if not isinstance(continuation, int) or continuation >= upper:
            raise ValueError("DVOL continuation did not move backwards")
        upper = continuation
    # Never publish a silent partial month/day; incomplete history is missing.
    if set(points) != set(range(item.start_ms, item.end_ms, HOUR_MS)):
        return _row(item, "missing")
    output = StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(("ts_ms", "open", "high", "low", "close"))
    writer.writerows(points[stamp] for stamp in sorted(points))
    content = output.getvalue().encode()
    expected = hashlib.sha256(content).hexdigest()
    checksum = f"{expected}  {path.name}\n{item.start_ms} {item.end_ms}\n".encode()
    if checksum_path.exists() and checksum_path.read_bytes() != checksum:
        return _row(item, "conflict")
    created = _atomic_write(path, content)
    if not created and _digest(path) != expected:
        return _row(item, "conflict", path.stat().st_size)
    if not _atomic_write(checksum_path, checksum) and checksum_path.read_bytes() != checksum:
        return _row(item, "conflict", path.stat().st_size)
    return _row(item, "downloaded" if created else "present", path.stat().st_size)


def download(item: Download, root: Path, client: httpx.Client) -> dict:
    return (_dvol if item.dataset == "dvol" else _binance)(item, root, client)


def run_downloads(plan: list[Download], root: Path, jobs: int = 4,
                  client: httpx.Client | None = None) -> list[dict]:
    """Download with bounded workers, then atomically replace the coverage report."""
    if jobs <= 0:
        raise ValueError("--jobs must be positive")
    if client is None:
        with httpx.Client(timeout=60, follow_redirects=True,
                          limits=httpx.Limits(max_connections=jobs), trust_env=False) as owned:
            return run_downloads(plan, root, jobs, owned)
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        rows = list(pool.map(lambda item: download(item, root, client), plan))
    output = StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=COVERAGE_FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    _atomic_write(root / "coverage.csv", output.getvalue().encode(), replace=True)
    return rows


def main(argv: list[str] | None = None) -> int:
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
        print(f"{len(plan)} files planned (Binance also fetches each URL + .CHECKSUM)")
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
    for dataset, counts in sorted(totals.items()):
        print(f"{dataset}: " + ", ".join(f"{key}={value}" for key, value in sorted(counts.items())))
    return int(any(row["status"] in ("conflict", "checksum_failed") for row in rows))


if __name__ == "__main__":
    raise SystemExit(main())
