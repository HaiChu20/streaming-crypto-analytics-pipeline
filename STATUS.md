# Status

Updated 2026-10-17. The design lives in [plan.md](plan.md); this file tracks what is built and what comes next.

## Phases

| Phase | What | State |
|---|---|---|
| **1** | Collectors + raw recorder + data checker | **Built and verified on live data.** Open items: Kraken L3 key, 24h run, first real midnight |
| 2 | Daily gzip → S3 (Terraform, launchd) | Next |
| 3 | Rust engine core + replay → feature Parquet | Not started |
| 4 | Rust live engine + cutoffs + ClickHouse + Grafana | Not started |
| 5 | Labels + baseline 15/30-min models (spot + perp) | Not started |
| 6 | Live inference | Not started |
| 7 | Long-run stability + cost | Not started |

## Built in phase 1

- `collectors/`: one asyncio process per exchange, 10 WebSocket connections, 44 streams.
  - Envelope with `recv_ns`; payload kept byte-for-byte.
  - In-band `start` / `stop` / `drop` events.
  - Gap checks with resync: Binance update IDs (spot range rule, perp `pu`), OKX `seqId`, Kraken CRC32 (L2 and L3), Kraken futures `seq`.
  - Make-before-break replacement at 00:00 UTC, at Binance 23h, after a gap, after Kafka loss, and on `SIGHUP`.
  - Crash detection (`drop(collector_crash)`) and a single-instance lock.
- `archiver/record_raw.py`: one file per stream per UTC day.
  - A day closes on that stream's first next-day record; it is never force-closed, and still-open days are reported as overdue.
  - Calls fsync, then commits offsets. Resumes from the files after a crash. `.meta.json` per closed file.
- `scripts/`:
  - `create_topics.py`: 22 topics × 2 partitions, BTC = 0, ETH = 1.
  - `probe_feeds.py`: fixtures and rates.
  - `check_raw.py`: offsets, continuity, checksums, duplicates, drops.
  - `pipeline.sh`: start / stop / status / rotate.
- Repo:
  - Single-broker Kafka with host + Docker listeners.
  - Python 3.12 with pinned deps.
  - `.env` gitignored.
  - Flink, Snowflake and the legacy producer/consumer removed.

## Verified (2026-10-17, about 02:00 UTC)

| Check | Result |
|---|---|
| `uv run pytest` | 21 passed, including Kraken's official L2/L3 checksum examples and replays of live fixtures |
| Live probe | 42/44 streams started. The 2 missing are Kraken L3: no key in `.env` yet |
| End to end: collectors → Kafka → recorder → `check_raw` | 0 errors. Offsets contiguous; checksums and sequences re-verified from the archive |
| Recorder `kill -9` + restart | 0 errors: no duplicates, no holes |
| OKX collector `kill -9` + restart | `drop(collector_crash)` written; the interval is reported as UNCERTAIN |
| `docker restart kafka` mid-run | No loss, no drops (the producers buffered through it) |
| `SIGHUP` rotation (all connections) | `stop` → `start`, overlap duplicate trades counted, 0 errors |
| Unplanned Binance perp disconnect | `drop:disconnect` recorded; reconnected and re-synced, 0 errors |

## Measured (short samples at a quiet hour, excluding L3)

- **Uncompressed:** about 26–42 GiB/day. The busiest streams are Kraken perp `book` BTC (~300 msg/s) and Binance perp `bookTicker` (~200 msg/s per symbol).
- **gzip:** about 10× smaller, so roughly 3–4 GiB/day for S3. This is an estimate; the 24h run will confirm it.
- Disk is fine for now (~314 GiB free), but plan for about 45 GiB/day locally until phase 2 deletes uploaded days.

## Next

1. **Kraken L3:**
   - Put `KRAKEN_API_KEY` / `KRAKEN_API_SECRET` in `.env`, using a key with WebSocket permission only.
   - Run `scripts/pipeline.sh stop && scripts/pipeline.sh start`, then `probe_feeds.py --exchange kraken`.
   - The L3 checksum is only proven on Kraken's documented example so far.
2. **24h run:** leave the pipeline running, then `check_raw.py --date <day>`. Record GB/day per feed in plan.md, and tune `retention_bytes_per_partition` and `producer_buffer_kbytes`.
3. **First real midnight UTC:** confirm every stream rotates and each day-D file closes on its first day-D+1 record.
4. **Not yet drilled:** a real Wi-Fi drop. The disconnect path itself is proven by the unplanned Binance drop above.
5. **Phase 2:**
   - Install the AWS CLI + Terraform.
   - Create the S3 bucket, a least-privilege IAM user and a budget alert.
   - Write `archiver/archive_daily.py` (provisional → complete manifests, overdue report) and schedule it with launchd.
