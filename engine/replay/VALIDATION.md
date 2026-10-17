# P5b validation — 2026-10-17 (collector-crash correction)

Schema keeps exact **mid_e9** and nullable DOUBLE **book_age_ms**. Shared
`STALE_S = 60` expires silent books/extras after 60 seconds, preserving zero-flow
semantics. A startup **drop(collector_crash)** now invalidates/retires the previous
active connection while keeping the new carrying connection eligible for its
following start and snapshot/first trade. The crash still counts as a break.
Normal drops/stops and later continuity failures still retire their carrier.
`check_raw.check_stream` preserves the crash carrier's check state, event count
and UNCERTAIN annotation instead of marking that new connection broken.

## Acceptance

- `cargo test --release --locked`: **51 passed, 0 failed, 1 ignored** (32 book
  integration tests, 1 core bounded-ID unit, 18 replay tests; ignored test is timing).
- Three new Rust regressions cover startup crash with no previous source, previous
  source with another ID, and an already-active carrying ID; same-ID start,
  snapshot and valid updates; a later detected gap; trade dedup/old-source ignoring;
  normal-drop behavior; and OKX new-source ctVal/book-feature eligibility.
- `cargo clippy --locked --all-targets -- -D warnings`: exit 0.
- `cargo fmt --check`: exit 0.
- Scoped `git diff --check -- engine scripts/check_raw.py tests/test_check_raw.py`:
  exit 0 (unrelated working-tree edits excluded).
- `PYTHON_DOTENV_DISABLED=1 UV_OFFLINE=1 uv run pytest -q`:
  **117 passed in 6.27s**. New raw-checker cases spy on actual BookCheck calls,
  verify clean same-ID crash/start/snapshot/updates, and detect a subsequent gap.
- Independent Parquet read-back verifies strict ordering, metadata and frozen-tail
  nullability across all output rows.

## Real-day replay after FIX2

```sh
/usr/bin/time -l engine/target/release/engine-replay --root data --date 2026-10-17 --allow-open --out /private/tmp/claude-502/-Users-bfhach-Projects-streaming-crypto-analytics-pipeline/60166cad-1579-46c0-bd19-31bf3993d4db/scratchpad/features
```

- Input: 42 streams; 16,054,399,210 capped bytes;
  **33,480,192 records** processed.
- Full process wall: **105.99 s**; internal replay/write: 105.664 s.
  Throughput: **316,854 records/s** internal;
  315,881 records/s using full wall.
- Peak RSS (macOS time bytes): **94,765,056 bytes / 90.38 MiB**.
- Output: **1,112,572 rows**, 26,949,857 bytes, Zstandard Parquet.
- Observed receive range: 2026-10-17T01:55:29.605520+00:00 through
  2026-10-17T10:39:17.332052+00:00.
- 36 file-order clock steps retained. Continuity counters remain
  64 disconnect drops + 4 collector-crash drops, with zero newly detected sequence
  gaps/checksum mismatches. Freshness expiry is counted independently.

## Corrected observed stale counts

**All 12 instruments now have zero stale observed ticks.** Each OKX stream's
previous artificial 93-tick interval (0.304369%) is gone. Its startup crash carrier
is now accepted, so valid new snapshots and updates replace the previous source.
The true crash interruptions are shorter than the 60-second stale timeout.
Ordinary invalid startup/resynchronization ticks still account for valid % <100.
Denominators begin when each instrument is seen and end at the captured receive
range, excluding the future tail.

| Instrument | Observed ticks | Stale ticks | Stale % | Observed valid % | Full-day valid % |
|---|---:|---:|---:|---:|---:|
| binance/perp/BTCUSDT | 31427 | 0 | 0.000000 | 99.334967 | 39.358744 |
| binance/perp/ETHUSDT | 31427 | 0 | 0.000000 | 99.328603 | 39.356227 |
| binance/spot/BTCUSDT | 31427 | 0 | 0.000000 | 99.719986 | 39.511004 |
| binance/spot/ETHUSDT | 31427 | 0 | 0.000000 | 99.716804 | 39.509746 |
| kraken/perp/PF_ETHUSD | 31428 | 0 | 0.000000 | 99.872725 | 39.572166 |
| kraken/perp/PF_XBTUSD | 31428 | 0 | 0.000000 | 99.872725 | 39.572166 |
| kraken/spot/BTC/USD | 31428 | 0 | 0.000000 | 99.866361 | 39.569649 |
| kraken/spot/ETH/USD | 31428 | 0 | 0.000000 | 99.866361 | 39.569649 |
| okx/perp/BTC-USDT-SWAP | 31427 | 0 | 0.000000 | 99.859993 | 39.566372 |
| okx/perp/ETH-USDT-SWAP | 31427 | 0 | 0.000000 | 99.856811 | 39.565113 |
| okx/spot/BTC-USDT | 31427 | 0 | 0.000000 | 99.882267 | 39.575180 |
| okx/spot/ETH-USDT | 31427 | 0 | 0.000000 | 99.879085 | 39.572664 |

`observed_stale_ranges` is empty. The former OKX spot interval
02:01:28–02:03:00 UTC and perp interval 02:01:29–02:03:01 UTC no longer appear.

## Frozen-tail proof remains intact

For each instrument, all **47,982 ticks** later than final captured receive time
plus 60 seconds are stale and **book_valid=false**, with zero valid books:
**575,784 instrument rows**. The audit asserts null book-derived values,
returns/RV, extras and cross values throughout the tail, including its
95,964 cross rows. Age remains populated and flow behavior is unchanged.

Maximum absolute cross values (bps):

| Asset | Binance basis | OKX basis | Kraken basis | OKX spot deviation | Kraken spot deviation |
|---|---:|---:|---:|---:|---:|
| BTC | 7.449978 | 6.290082 | 4.045101 | 3.910095 | 17.171358 |
| ETH | 10.411630 | 7.640063 | 3.315288 | 6.972943 | 17.613350 |

## Independent Kraken BTC reference

The new Parquet read-back selected the same five observed Kraken BTC ticks; all
five were compared again with the independent stdin Python/Decimal reconstruction
results (which verified **1,123,502 checksums**). They still match exactly.
That independent replay's sampled prefix is unchanged; it was not rerun for FIX2.

| Tick UTC | Python mid | Python mid_e9 | FIX2 Parquet mid_e9 |
|---|---:|---:|---:|
| 2026-10-17T02:10:30+00:00 | 81514.45 | 81514450000000 | 81514450000000 |
| 2026-10-17T04:22:38+00:00 | 81410.55 | 81410550000000 | 81410550000000 |
| 2026-10-17T04:53:38+00:00 | 81418.65 | 81418650000000 | 81418650000000 |
| 2026-10-17T06:50:36+00:00 | 81477.15 | 81477150000000 | 81477150000000 |
| 2026-10-17T09:09:26+00:00 | 81606.95 | 81606950000000 | 81606950000000 |

## Artifacts

- Parquet: `/private/tmp/claude-502/-Users-bfhach-Projects-streaming-crypto-analytics-pipeline/60166cad-1579-46c0-bd19-31bf3993d4db/scratchpad/features/date=2026-10-17/features.parquet`
- Input caps/metrics: `/private/tmp/claude-502/-Users-bfhach-Projects-streaming-crypto-analytics-pipeline/60166cad-1579-46c0-bd19-31bf3993d4db/scratchpad/p5b-fix2-report.json`
- Timing/RSS: `/private/tmp/claude-502/-Users-bfhach-Projects-streaming-crypto-analytics-pipeline/60166cad-1579-46c0-bd19-31bf3993d4db/scratchpad/p5b-fix2-time.txt`
- Ordering/staleness/tail audit: `/private/tmp/claude-502/-Users-bfhach-Projects-streaming-crypto-analytics-pipeline/60166cad-1579-46c0-bd19-31bf3993d4db/scratchpad/p5b-fix2-audit.json`
- Independent Python reference: `/private/tmp/claude-502/-Users-bfhach-Projects-streaming-crypto-analytics-pipeline/60166cad-1579-46c0-bd19-31bf3993d4db/scratchpad/p5b-fix-crosscheck.json`

Feature contract and official exchange field-semantic citations remain in
[README.md](README.md). Raw data and the running pipeline were not modified.
