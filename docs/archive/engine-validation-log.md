# Engine validation log

Historical validation and benchmark records, retained for context. Counts, engine
versions, acceptance thresholds and local artifact paths describe the original
runs; they are not current release guarantees. No measurements were rerun for
this documentation move. Use the [engine guide](../../engine/README.md) and
[replay guide](../../engine/replay/README.md) for current operation.

## P5b validation — 2026-10-17 (collector-crash correction)

Schema keeps exact **mid_e9** and nullable DOUBLE **book_age_ms**. Shared
`STALE_S = 60` expires silent books/extras after 60 seconds, preserving zero-flow
semantics. A startup **drop(collector_crash)** now invalidates/retires the previous
active connection while keeping the new carrying connection eligible for its
following start and snapshot/first trade. The crash still counts as a break.
Normal drops/stops and later continuity failures still retire their carrier.
`check_raw.check_stream` preserves the crash carrier's check state, event count
and UNCERTAIN annotation instead of marking that new connection broken.

### Acceptance

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

### Real-day replay after FIX2

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

### Corrected observed stale counts

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

### Frozen-tail proof remains intact

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

### Independent Kraken BTC reference

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

### Artifacts

- Parquet: `/private/tmp/claude-502/-Users-bfhach-Projects-streaming-crypto-analytics-pipeline/60166cad-1579-46c0-bd19-31bf3993d4db/scratchpad/features/date=2026-10-17/features.parquet`
- Input caps/metrics: `/private/tmp/claude-502/-Users-bfhach-Projects-streaming-crypto-analytics-pipeline/60166cad-1579-46c0-bd19-31bf3993d4db/scratchpad/p5b-fix2-report.json`
- Timing/RSS: `/private/tmp/claude-502/-Users-bfhach-Projects-streaming-crypto-analytics-pipeline/60166cad-1579-46c0-bd19-31bf3993d4db/scratchpad/p5b-fix2-time.txt`
- Ordering/staleness/tail audit: `/private/tmp/claude-502/-Users-bfhach-Projects-streaming-crypto-analytics-pipeline/60166cad-1579-46c0-bd19-31bf3993d4db/scratchpad/p5b-fix2-audit.json`
- Independent Python reference: `/private/tmp/claude-502/-Users-bfhach-Projects-streaming-crypto-analytics-pipeline/60166cad-1579-46c0-bd19-31bf3993d4db/scratchpad/p5b-fix-crosscheck.json`

Feature contract and official exchange field-semantic citations remain in
[replay guide](../../engine/replay/README.md). Raw data and the running pipeline were not modified.

## P10 validation — v2 raw replay and partition ordering

Ideal, as_live, and live checkpoints now merge only per-partition heads. A
backward `recv_ns` cannot reorder a later offset ahead of an earlier one; flow
from a late record is attributed to the tick that processes it. A regression
holds a Kraken futures sequence valid through a backward step, and a no-step
fixture produces exactly the former global-sort order.

The converted v2-ours tree contains 42 streams and 70,934,797 records. Its
full-day ideal replay emitted 1,112,572 rows at 302,771 records/s (internal
wall); peak RSS was 109,346,816 bytes. All 92 book breaks were explicit drops
(88 disconnect, 4 collector crash), with zero detected sequence or CRC breaks.
The 55 receive-clock steps were preserved in file order.

For an exact comparison with the earlier P5b v1 Parquet, the v2 replay was
capped at the same final offset of each stream: 33,480,192 records, 1,112,572
rows, and the same 68 explicit book breaks. Ignoring `available_ns` and the
engine-version label, 86 rows differ, all in OKX perp BTC/ETH trade volume and
buy/sell volume during four trade-connection stop/start gaps (01:59:17–01:59:26,
02:02:53–02:03:02, 02:05:43–02:05:55, and 09:18:00–09:18:09 UTC).
The old artifact emitted zero; the current engine correctly emits null because
the disconnected connection's `ctVal` is no longer available. The earlier
artifact used engine version `0.1.0-ideal-v1`; the current version is
`0.1.0-features-v2`. There are no other row differences in the capped input.

The corrected offline live reference replayed 1,622,211 original v1 envelopes
through the shared checkpoint and queue logic over the P6a parity window.
Converted v2 as_live produced exactly the same 25,340 rows, with zero field
mismatches (excluding nondeterministic publication time) and the same four
pre-existing `missing_snapshot` breaks. Compared with the old captured live
rows, the corrected reference changes 3,050 rows: three Kraken books (perp
PF_ETHUSD, perp PF_XBTUSD, spot ETH/USD) and their two cross rows, each for
610 ticks from 15:04:03 through 15:14:12 UTC. The old global batch sort
reversed offsets at a backward clock step and falsely retired those books;
the corrected reference has no such continuity breaks. No late flow records
occurred in this parity window.

The real Kraken BTC/USD 2026-10-16 audit uses the completed corrected
`v2-legacy-fixed` tree. Kraken L3 queue order is determined by order timestamp,
with arrival-order ties; duplicate adds replace prior entries and price-level
depth is truncated after each frame. The old queue model produced 15 false
checksum failures. An independent full-day reconstruction verified
11,053,252 L3 CRCs and zero failures with these rules. The Rust full-day audit
agrees: **11,053,154 L3 CRC passes, 0 failures**. The converted and source
L3 files have the same 11,053,281 lines. The source-file audit also checks 98
data frames timestamped after the last 23:59:59 replay tick; those await the
next day's tick and account exactly for the count difference. L2 has
**11,052,596 CRC passes, 0 failures**. The Rust replay processed 22,250,352
records, emitted 172,794 Parquet rows, and preserved zero clock steps. Its
506.63 s internal wall time yields **43,918 records/s**; peak RSS was
25,427,968 bytes (24.25 MiB). This rate includes continuous L3 CRC checks;
the former, invalid L3 model retired most of each connection and therefore
appeared faster.

The L2 book is valid on **86,330/86,397 ticks (99.92245%)** and the L3 book
on **86,344/86,397 (99.93866%)**. The 14 L2 and 12 L3 invalidation breaks
align one-for-one with `connection_error` markers and recover at the next
snapshot; the only additional invalid interval is initial startup for each
book. There are no checksum or sequence breaks. The L3 feed carries 9,797,458
adds, 49,736 modifies, and 5,439,670 deletes across all levels. Within the
top ten, applied event counts are 2,303,508 adds, 47,090 modifies, and
2,240,916 deletes.

At the 86,279 ticks where both books are valid, the L3-aggregated and L2
books agree on both best bid/ask prices for **85,305 ticks (98.871%)**, on
best price and size for **81,099 (93.996%)**, on summed top-ten size on both
sides for **76,123 (88.229%)**, and on all top-ten price/size levels for
**75,726 (87.769%)**. These are comparisons at one-second replay ticks, not
exchange-sequenced snapshots of the same instant. For example, at 00:00:13
the L2 exchange timestamp is 00:00:12.449740 and L3 is
00:00:12.530621502; best prices agree but sizes differ. Kraken documents
that the feeds use different market-data stacks and that larger L3 payloads
take longer to encode and transmit; [its L3 guide](https://docs.kraken.com/exchange/guides/general/l3-data)
supports this timing explanation.

P10 artifacts are under `scratchpad/p10/`: `legacy-ts-features`,
`legacy-ts-report.json`, `legacy-ts-audit.json`, `legacy-ts-time.txt`,
`ours-features` and `ours-report.json`,
`v2-aslive-rows.jsonl`, `reference-live-rows.jsonl`, and the row-diff JSONs.
All raw inputs were read only.

## OKX checksum observations — 2026-10-17

On 2026-10-17 all 192,432 OKX spot BTC book messages checked in the live archive
had checksum zero (supervisor observation). The supplied fixtures likewise have
zero in all 564 spot and 598 perp messages. As explicitly agreed, zero means
"not provided": it increments `okx_checksum_absent` without verification.
Nonzero checksums remain strictly verified; sequence continuity is always checked.
`checksums_verified` counts only actual successful checksum comparisons.

## Timing (release, 100 iterations, 2026-10-17)

| Exchange | Messages | Messages/s | ns/message | Spot ns/message | Perp ns/message |
|---|---:|---:|---:|---:|---:|
| Binance | 120000 | 73410 | 13622 | 12454 | 14788 |
| Kraken | 120000 | 258581 | 3867 | 3921 | 3812 |
| OKX | 116800 | 149394 | 6694 | 4443 | 8823 |

These include fresh snapshot reconstruction each iteration and optional envelope
fields. OKX fixtures exercise continuity and count absent checksums; they do not
measure nonzero CRC verification. Kraken spot L2 at 3.92 us/message is about 3.8x
faster than the supplied Python reference of approximately 15 us/message (that
baseline was not remeasured here).

## V3 fixture benchmark (P16)

Original
`tests/fixtures/v2` files remain on disk, unused; `fixtures/v3` replaces them.

The fixture benchmark command for the records below runs from `engine/`:

```sh
cargo run --release -p engine-replay --example benchmark_archive -- ../tests/fixtures/archive
```

It measures archive reading, parsing and ingestion; it excludes full-day empty
ticks and Parquet output. Run 0 warms the cache, and runs 1–3 supply the samples.

P16 fixture measurement (release, local machine, three warm samples): spot/L3
33,511 records at 279,346 / 286,586 / 288,175 records/s (median 286,586);
Kraken futures 75,235 records at 670,425 / 656,071 / 639,964 records/s
(median 656,071). Both ingestion medians exceed the ~255k records/s reference.
This is a fixture ingestion comparison, not a paired full-day benchmark. Full-day
CLI output on these sparse excerpts produced 691,160 rows in 0.674 s for spot
(49,730 records/s) and 172,794 rows in 0.251 s for futures (300,133 records/s).
Most spot output rows follow the excerpt's end and contain stale/null values.

## Features v4 fixture validation (P21)

Required checks: `cargo test --release --locked` (109 passed, one pre-existing
ignored timing test), `cargo clippy --locked --all-targets -- -D warnings`,
`cargo fmt --check`, and root `PYTHON_DOTENV_DISABLED=1 uv run pytest -q`
(336 passed). Tests include archive replay, live/as_live/from_state parity,
a full hour of liquidation and OI history, and a pending liquidation at restart.

The benchmark command above ran before edits and after all checks. Warm samples
are records/s (run 0 excluded). Three unchanged post-change batches were retained
because individual batches varied; the reported after median includes **all nine**
warm samples, rather than selecting one batch. This measures ingestion only,
not the new once-per-second band calculations or Parquet output.

| Fixture | Before warm samples | After warm samples, in three batches | Before median | After median | Regression |
| --- | --- | --- | ---: | ---: | ---: |
| Spot, 33,511 records | 330409, 342982, 340212 | 322473, 321620, 322394; 322130, 329200, 329955; 305944, 305476, 311901 | 340212 | 322130 | 5.3% |
| Futures, 75,235 records | 641786, 646099, 644572 | 551758, 538423, 540489; 607191, 615092, 609306; 607110, 598264, 604099 | 644572 | 604099 | 6.3% |

All benchmark runs reported no data anomalies. Individual batch regressions
reached 10.1% (spot) and 16.1% (futures); aggregate medians meet the 10% threshold.

## Deribit DVOL validation (P22)

P22 validation: `cargo test --release --locked` passed 114 tests (one existing
ignored timing test); Clippy with `--locked --all-targets -- -D warnings` and
`cargo fmt --check` passed. Root `PYTHON_DOTENV_DISABLED=1 uv run pytest -q`
passed 383 tests. New offline fixtures cover exact response number tokens,
recorder/reader round trips, poll retries/dedupe/closed candles, Kafka recovery,
server backfill pagination/receive days/no-clobber behavior, all DVOL windows,
RV units, Parquet and checkpoint/from-state parity with a pending candle.

`benchmark_archive` before/after warm records/s (run 0 excluded):

| Fixture | Before samples | After samples | Before median | After median | Regression |
| --- | --- | --- | ---: | ---: | ---: |
| Spot, 33,511 records | 288323, 274507, 276290 | 321458, 322784, 325630 | 276290 | 322784 | none (−16.8%) |
| Futures, 75,235 records | 491008, 460519, 454109 | 586371, 586325, 612476 | 460519 | 586371 | none (−27.3%) |

Both measured regressions are below 5%, with zero data anomalies. These short
fixture ingestion runs vary with local load; the increases are not evidence of
an algorithmic speedup. They exclude ticks and Parquet, as described above.
