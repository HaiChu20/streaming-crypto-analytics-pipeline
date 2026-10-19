"""Server-only DVOL backfill; never overwrites or removes an existing archive.

--from/--to are inclusive, closed UTC receive dates. Files follow the recorder's
receive-day rule: a 23:59 candle has synthetic recv=00:00:10 on the next day.
Use only on the server, after stopping any writer for the target dates.
"""

import argparse
import asyncio
from datetime import date, datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import sys
import time

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from archiver import rawfmt  # noqa: E402
from archiver.record_raw import write_meta  # noqa: E402
from collectors.deribit import fetch_page  # noqa: E402

LAG_MS = 70_000
DAY_MS = 86_400_000


async def request_page(http, currency, start, end, sleep=asyncio.sleep):
    for attempt in range(7):
        try:
            page = await fetch_page(http, currency, start, end)
            await sleep(0.25)
            return page
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 429 and exc.response.status_code < 500:
                raise
            if attempt == 6:
                raise
        except httpx.TransportError:
            if attempt == 6:
                raise
        await sleep(min(60, 2 ** attempt))
    raise AssertionError("unreachable")


async def day_content(http, currency, day, sleep=asyncio.sleep):
    start_recv = int(datetime.combine(day, datetime.min.time(), timezone.utc).timestamp()) * 1000
    start, end = max(0, start_recv - LAG_MS), start_recv + DAY_MS - LAG_MS - 1
    candles = {}
    while True:
        rows, continuation, _ = await request_page(http, currency, start, end, sleep)
        for row in rows:
            if start_recv <= row.ts_ms + LAG_MS < start_recv + DAY_MS:
                if row.ts_ms in candles and candles[row.ts_ms] != row.text:
                    raise ValueError(f"conflicting DVOL candle {currency}/{row.ts_ms}")
                candles[row.ts_ms] = row.text
        if continuation is None or continuation < start:
            break
        if continuation >= end:
            raise ValueError("DVOL continuation did not move backwards")
        end = continuation
    lines = []
    for offset, (ts, text) in enumerate(sorted(candles.items())):
        lines.append(rawfmt.line(dict(exchange="deribit", market="index", feed="dvol", symbol=currency,
                                      kind="update", conn="backfill", recv_ns=(ts + LAG_MS) * 1_000_000,
                                      payload=text), offset))
    next_day = day + timedelta(days=1)
    # A deterministic close marker makes repeated fetches byte-identical.
    lines.append(rawfmt.dumps({"marker": "rollover", "new_period": next_day.strftime("%Y%m%d"),
                               "timestamp": next_day.isoformat() + "T00:00:00+00:00"}) + b"\n")
    return b"".join(lines)


def verify_target(path, content):
    for suffix in (".open", ".gz"):
        alternate = path.with_name(path.name + suffix)
        if alternate.exists():
            raise FileExistsError(f"existing archive representation: {alternate}")
    meta = path.with_name(path.name + ".meta.json")
    if path.exists():
        if path.read_bytes() != content:
            raise FileExistsError(f"different DVOL content; refusing overwrite: {path}")
        if meta.exists():
            info = rawfmt.loads(meta.read_text())
            if info.get("sha256") != hashlib.sha256(content).hexdigest():
                raise FileExistsError(f"different metadata checksum: {meta}")
    elif meta.exists():
        raise FileExistsError(f"metadata exists without data: {meta}")


async def backfill(http, archive_root, first, last, currencies, *, sleep=asyncio.sleep, now_ms=None):
    now_ms = time.time_ns() // 1_000_000 if now_ms is None else now_ms
    today = datetime.fromtimestamp(now_ms // 1000, timezone.utc).date()
    if first > last or first < date(1970, 1, 1) or last >= today:
        raise ValueError("--from/--to must be ordered, closed UTC dates since 1970")
    if not currencies or any(c not in ("BTC", "ETH") for c in currencies):
        raise ValueError("currency must be BTC or ETH")
    results = []
    for currency in dict.fromkeys(currencies):
        day = first
        while day <= last:
            content = await day_content(http, currency, day, sleep)
            target = rawfmt.path(Path(archive_root), "deribit", "index", currency, day.isoformat(), "dvol")
            verify_target(target, content)
            target.parent.mkdir(parents=True, exist_ok=True)
            existed = target.exists()
            if not existed:
                # Exclusive creation also protects against a concurrent backfiller.
                with target.open("xb") as output:
                    output.write(content)
                    output.flush()
                    os.fsync(output.fileno())
            meta = target.with_name(target.name + ".meta.json")
            if not meta.exists():
                write_meta(target, exclusive=True, extra={"synthetic_offsets": True, "source": "deribit_dvol_backfill",
                                                         "receive_lag_ms": LAG_MS})
            results.append((target, "skipped identical" if existed else "written"))
            day += timedelta(days=1)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", required=True, type=Path)
    parser.add_argument("--from", dest="first", required=True, type=date.fromisoformat)
    parser.add_argument("--to", dest="last", required=True, type=date.fromisoformat)
    parser.add_argument("--currency", nargs="+", choices=("BTC", "ETH"), default=["BTC", "ETH"])
    args = parser.parse_args()

    async def run():
        async with httpx.AsyncClient(timeout=30) as http:
            return await backfill(http, args.archive_root, args.first, args.last, args.currency)

    for path, status in asyncio.run(run()):
        print(f"{status}: {path}")


if __name__ == "__main__":
    main()
