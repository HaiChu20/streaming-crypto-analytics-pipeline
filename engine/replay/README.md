# Ideal replay

```sh
# From the repository root (no Kafka, Docker or exchange access):
$HOME/.cargo/bin/cargo build --manifest-path engine/Cargo.toml --release --locked -p engine-replay
engine/target/release/engine-replay --root data --date 2026-10-17 --allow-open --out data/features
```

The output is `<out>/date=D/features.parquet`. The default output base is
`<root>/features`. The executable prints a JSON report with input byte limits,
record/row counts, clock steps, break reasons, coverage and cross-price maxima.

## Input and ordering

V3 files under `<Exchange>/<Market>/<Pair>/YYYYMMDD_<channel>.jsonl` define
streams; the root may also contain a `raw` directory. Exchange directories match
case-insensitive `binance*`, `kraken*`, `okx*`, `deribit*`; `spot*` means spot and
`fut*`/`perp*`/`swap*` means perp, and `index*` means index. `--exchange` and `--market` filter canonical
names. Every native pair is accepted. Kraken spot directory hyphens become `/`
in the native output symbol; other symbols retain their native spelling.

| Exchange | File channels → engine feeds |
| --- | --- |
| Binance | depth/snapshot → l2; bookticker → l1; trades → trades |
| Binance perp | aggtrades → trades_agg; markprice → mark; forceorder → liquidations |
| OKX | book → l2; bbo → l1; trades → trades_agg; tradesall → trades |
| Kraken spot | book → l2; level3 → l3; trades → trades |
| Kraken futures | book → l2; trades → trades; ticker → ticker |
| Deribit index | dvol → dvol |

Unknown feeds are skipped with a warning. If several representations of the same
stream exist, prefer `.jsonl`, then `.jsonl.gz`, then `.jsonl.open`. Open files
require `--allow-open`. Every reader is capped at the file's size when opened;
unterminated final lines of open files are ignored. Concatenated gzip members
are supported. File paths and byte limits in the report permit reproduction on
an append-only archive without stopping collection.

A min-heap contains one head per buffered reader and compares
`(recv_ns, topic, partition, offset, file_index)`. The final file index only breaks
otherwise identical coordinates. Each file is an independent input partition,
including separate Binance depth and snapshot files feeding one book. Partition
numbers are deterministic, assigned in sorted path order within each engine topic;
they carry no asset identity. Only the current head participates in the merge.

Wrapped lines supply their exact inner `data` text to the core. Kraken spot keeps
its native line, including inserted archive metadata. No decimal reserialization
occurs. The shared Binance decoder also accepts live combined-stream payloads.
Missing offsets use the zero-based physical line index, including ignored markers.
Missing data connection IDs use `<marker conn>#<connected count>` (per file), or
`<file>#pre` before the first connected marker. Explicit recorder conn/offset
coordinates are retained. Marker time uses `recv_time_ns`, otherwise ISO UTC
`timestamp` (Z or +00:00, up to nine fractional digits).

Connected markers start epochs. Connection errors, disconnected, rotating and
shutdown end them; corrupt_message is counted without changing state. Gap and
rollover markers are ignored. A receive clock step-back cannot move a later line
before its predecessor: its older time is exposed only after that predecessor.
This preserves file order even when a
globally increasing timestamp order is impossible. Older hidden records become
eligible at that later tick. Live queues and checkpoint/as_live batches use the
same head-only ordering rule. Flow attribution uses one shared helper:
`max(ceil_to_second(recv_ns), current_tick)`. Thus a late record's trade,
liquidation or L3 event flow appears in the tick that processes it, never a
closed bucket. No-step input produces the same order as the former global sort.

Binance archive anchors initialize the current depth epoch, including deltas
buffered for minutes before the anchor arrives. A snapshot is applied only while
that connection is unsynced; later periodic snapshots increment `snapshots_ignored`
and leave the book unchanged. Sequence breaks discard its book/sync state without
retiring the epoch, allowing buffering and a later anchor to resync it. Ended epochs
remain retired until a new connected epoch. Each unsynced connection retains at
most 100,000 pending deltas in a FIFO. Evicting the oldest increments
`pending_evicted`; an anchor older than retained contiguous history stays stale
until a usable anchor arrives. Snapshot `lastUpdateId` filters obsolete deltas.

## Feature contract

All pure state and row definitions live in `engine_core::{features,row}`, with no
I/O. Replay handles discovery, merging, ticking and Parquet only. The shared core
can therefore be used by the future live engine without Parquet dependencies.

Ticks are every whole UTC second in `[D 00:00:00, D+1 00:00:00)`. At T, all visible
heads with `recv_ns <= T` are applied before emitting rows. Records before D are
excluded; records after the final tick wait for a later day's tick and are not
assigned invented earlier timestamps. Rows begin when an instrument is first
seen. `available_ns = T + 250_000_000` is a deterministic ideal-mode assumption,
not a measured live publication time. Full-day replay continues to the last tick
even for an open current day. Shared core constant `STALE_S = 60` expires book
features when their L2 stream has been silent for more than 60 seconds, including
within a closed day or after captured input ends. `book_age_ms` is the elapsed
receive age in fractional milliseconds, null before the first book-stream record.
Data and control records refresh the age; ignored retired-connection records do
not. The 60-second boundary remains fresh, and the next record can restore book
features if continuity is still valid. Funding, mark price and open interest use
the same timeout independently on their source stream. Trades, liquidations and
L3 event flow keep their existing zero-activity semantics. Reported coverage counts
emitted instrument ticks. The audit separately reports observed stale percentages
and asserts all book/extras/cross fields are null in the tail after the grace period.

The schema appends required UTF8 `asset`; existing columns, types and order are
unchanged. JSON rows also expose `asset` (old row JSON can omit it). The base is
parsed from Binance's quote suffix (FDUSD, USDT, USDC, USD, BTC, ETH, EUR), OKX's
first dash component, Kraken spot's first slash/dash component, or Kraken futures
`PF_<BASE><QUOTE>`; XBT normalizes to BTC. Cross rows are emitted per observed
asset, and their symbol and asset both contain the base. If multiple pairs share
an asset on one venue/market, the lexicographically first currently valid native
symbol supplies the midpoint. Missing instruments or stale books produce null
cross fields. Kafka partitions never imply a pair: malformed envelopes with a
recoverable symbol use it; wholly corrupt envelopes invalidate the observed feed's
instruments conservatively.

Rows are sorted by `(tick_ns, exchange, market, symbol, engine_version)`. The
version is the shared `engine_core::ENGINE_VERSION`.

- L2 supplies book features. Invalid, empty or crossed books have null book values.
  Midpoint source values remain exact fixed point. `mid_e9` is an exact i64
  with nine implied decimals, so half of a 1e-8 atom is represented without
  rounding. Returns use `mid_e9`. Extreme values outside its i64 range produce an
  explicit error; ordinary market prices fit comfortably. There is no decimal-text
  midpoint column.
- Sizes use base units. OKX linear swaps multiply each connection's native size
  by its own start metadata `ctVal`; missing swap metadata is never replaced with
  one. Spot uses one. Trade/liquidation totals accumulate exact i128 units of
  1e-16 before conversion to f64. Top-ten native level sums use i128.
- Only `trades` contributes trade flow; `trades_agg` and l1 are read but contribute
  no duplicated features. Trade connections switch at their first trade frame,
  including a trade snapshot. Retired connections stay ignored. A 50,000-ID FIFO
  set deduplicates across replacements. A startup `drop(collector_crash)` ends
  the previous active source, not its new carrying connection; the same carrying
  ID remains eligible for the following start/book snapshot or first trade. The
  crash still counts as a book break and remains UNCERTAIN in `check_raw` reports.
  Historical trade snapshots seed IDs but
  contribute no new one-second volume. Window membership is receive time in
  `(T-1s,T]`, clamped to the processing tick for late/backward records, independent
  of exchange timestamps. Absent optional feeds have null
  metrics; a seen feed with no events in the window has zero metrics.
- Returns are log(mid_e9(T)/mid_e9(T-h)); missing/invalid endpoints yield null.
  Realized volatility is `sqrt(sum(ret_1s^2))`, not annualized, requiring every
  one-second return in the 60/300-second window. Warm-up lasts until 300 elapsed
  seconds after the first valid midpoint, including periods of invalidity.
- Memory windows use `W = 10, 60, 300, 900, 1800, 3600` seconds and suffix
  `_{W}s`. Each market retains 3,600 one-second buckets, with exact integer
  add/evict sums and rolling midpoint extrema. Skipped ticks occupy missing
  seconds. All memory columns remain null on `cross` rows.
  `cov_{W}s` is null until W buckets have elapsed since this market's first tick;
  then it is the fraction with a non-warmup valid book and, if trades were seen,
  known trades (the same rule as non-null `trade_count`). Other window fields
  are null until full or when coverage is zero. `vol_`, `flow_` (buy minus sell),
  and `trades_` sum covered seconds in calibrated base units/counts; markets
  without trades keep these null. If covered trade counts exist but contract
  calibration is missing, `vol_`/`flow_` stay null until those buckets evict;
  counts and coverage remain available. A silent but valid feed retains today's
  known-zero semantics; a book expires after the existing 60-second timeout.
- `ofi1_{W}s` and `ofi10_{W}s` sum Cont OFI by rank (level 1 or levels 1–10),
  divided by mean side depth at those levels over non-warmup valid ticks.
  Depth and OFI use identical contract scaling; a zero mean depth yields null.
  Each applied exchange message compares its immediately preceding/following
  active L2 book, with fixed-size top-ten arrays and i128 accumulation. A side
  missing that rank at either endpoint contributes zero. Binance OFI is per
  **100 ms depth batch**, and multi-level Kraken/OKX updates are one message.
  Snapshots, resyncs/catch-up, connection switches, invalid books, and non-active
  connections contribute **zero OFI**. Attribution follows the same receive-time
  bucket/late-record clamp as trades. Valid tick buckets supply the numerator
  and depth; trade uncertainty lowers coverage without discarding valid book flow.
- `range_bps_{W}s` is `(max_mid_e9-min_mid_e9)/current_mid_e9*10000` over
  non-warmup valid ticks; extrema stay integer until the final ratio.
  `ret_{300,900,1800,3600}s` use the existing log-return endpoint rule, also
  gated by window coverage. `rv_{900,1800,3600}s` is the square root of the sum
  of all existing one-second log returns squared, including warmup endpoints
  and allowing gaps (coverage gating still applies); the existing strict `rv_60s`/`rv_300s` semantics are unchanged.
- Kraken spot `l3_{add,del}_{bid,ask}_{W}s` sum per-side event counts over valid
  buckets, using the existing top-ten rules (stored side before deletes, rank
  after adds). Snapshots count zero; modifications do not become adds/deletes.
  Markets without L3 keep these null; existing `l3_*_1s` fields remain.
  `microprice_dev_bps` is `(microprice-mid)/mid*10000`, null if unavailable.
- Checkpoints/start states serialize the ring, sums, extrema, return history,
  pending bucket OFI/L3 counts and sync eligibility. The version is bumped to
  `features-v4`; older-version checkpoints are rejected. Numeric columns are
  automatically selected by `research/dataset.py`; inference explicitly includes
  them. Dashboard SQL stays unchanged: ClickHouse's Kafka engine defaults to
  skipping unknown JSON fields, so dashboard ingestion tolerates new features
  ([upstream implementation](https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/Kafka/StorageKafkaUtils.cpp)).
- Funding values are the native fields requested by the spec, without assuming
  identical exchange units/periods. Kraken ticker mark/open interest and Binance
  mark/funding are carried until their active stream ends or its receive age
  exceeds 60 seconds. Liquidation flow counts
  pushed sampled forceOrder messages and uses `z`, cumulative filled quantity,
  rather than the original order quantity `q`.
- Features v4 adds **35 columns**, all null on `cross` rows:
  `liq_{sell,buy}_{base,count}_{W}s` (24), `funding_min_to_next`,
  `mark_index_bps`, `oi_chg_pct_{300,900,3600}s`, and
  `depth_{bid,ask}_{10,25,50}bps`. Liquidation base quantities/counts use the
  same covered-second sums, full-window and nonzero-coverage gating as `vol_{W}s`.
  A source applies after Binance perp's `liquidations` feed is seen or Kraken
  perp's `trades` feed is seen, including control frames. Spot and OKX stay null;
  applicable sources with no events produce zero after window gating.
- Liquidation `sell` is a forced SELL closing a long; `buy` closes a short.
  [Binance forceOrder](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams)
  supplies order side `o.S` and cumulative filled quantity `o.z`; the existing
  `liq:{o}` dedup is unchanged. Binance publishes only the latest liquidation
  order per symbol per **1000 ms**, so its totals are sampled, not total venue
  liquidation activity. Kraken's complete
  [Futures trade stream](https://docs.kraken.com/exchange/api-reference/futures-websocket/trade)
  supplies `type="liquidation"`, `qty`, and taker `side`.
  [Kraken's fee semantics](https://support.kraken.com/in/articles/360048917612-fee-schedule)
  identify the liquidated party as taker and counterparty as maker; together these
  establish that trade `side` is the forced-order direction (no inversion).
  Liquidation trades retain normal volume/flow/count contributions and UID dedup;
  historical snapshots seed IDs only. Existing `liq_count_1s`/`liq_base_1s` now
  also include these Kraken trades, without warmup/book gating as before.
- `funding_min_to_next = (next_funding_ms - tick_ns/1e6)/60000`, using
  [Binance markPrice](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream)
  `T` or [Kraken ticker](https://docs.kraken.com/exchange/api-reference/futures-websocket/ticker)
  `next_funding_rate_time`. Both are epoch milliseconds. Kraken describes the
  latter as “time until,” but its official example and the archived
  `KrakenRaw/Futurev6/PF_XBTUSD/20261008_ticker.jsonl.gz` show absolute timestamps
  (`time=1791471234981`, `next_funding_rate_time=1791471600000`). The countdown
  uses the row tick, may become negative, and never invents a funding schedule.
  It follows funding freshness: null after source end or receive age >60s.
- `mark_index_bps = (mark-index)/index*10000` requires both fields in the same
  fresh message and a positive index: Binance `p`/`i`, Kraken `markPrice`/`index`.
  Missing fields clear prior values. `oi_chg_pct_{W}s = 100*(OI_t-OI_{t-W})/OI_{t-W}`
  uses per-second carried Kraken `openInterest`, requiring both fresh/known
  endpoints and positive previous OI; interior gaps are allowed. Skipped ticks
  store missing endpoints. These extras are independent of book warmup/coverage.
- `depth_{bid,ask}_{10,25,50}bps` sums base size on that side within the inclusive
  mid ± band edge. Integer price comparisons preserve exact boundaries; OKX uses
  the active L2 connection's `ctVal`, just like `bid_depth10`. Each side must have
  a retained level **strictly beyond** its edge, otherwise that band is null
  because the book may be truncated (including limited-depth Kraken spot).
  Invalid/stale/uncalibrated books give null. Bands are calculated once per row,
  never per message. Checkpoints serialize liquidation rings/sums/pending buckets,
  the OI endpoint history, and the new carried extras.
- L3 snapshots produce no order-event counts. Adds/modifications use their event's
  top-ten rank, deletes the rank immediately before removal. Events are applied
  in frame order, so a formerly eleventh level can enter the top ten after an
  earlier deletion in the same frame. These counters use the same L3 state that
  verifies checksums, without a second reconstruction.
- Cross rows use `10000*(perp_mid/spot_mid-1)` for each exchange and
  `10000*(other_spot_mid/binance_spot_mid-1)` for spot deviations. Missing valid
  inputs yield null. Their unrelated book/trade columns are null/false.

Parquet uses the low-level column writer, optional definition levels and Zstandard,
with 8,192-row groups. Arrow is disabled. Metadata includes `mode=ideal`,
`warmup_s=300`, and `engine_version`. Writes use a same-directory exclusive temporary
file, close/fsync, then atomic rename and directory fsync. A failed write leaves
its temporary file for diagnosis and does not publish a partial final file.

## Field semantics checked on 2026-10-17

- [Binance individual trade stream](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#trade-streams):
  `m` identifies a buyer who is the maker; true therefore means a taker sell.
  Spot/perp archived individual trades use `t`, `q`, `m`.
- [OKX trades channel](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-trades-channel):
  explicitly defines `side` as the taker's side, and swap `sz` as contracts.
  [All trades channel](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-all-trades-channel)
  supplies individual trade IDs, quantities, and buy/sell direction; replay uses
  the canonical `trades-all` archive rather than adding aggregate `trades` volume.
  [Instrument metadata](https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments)
  defines linear contract notional as size × ctVal × mark price, confirming base
  quantity is size × ctVal for these USDT swaps.
- [Kraken spot trades](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade)
  and [futures trades](https://docs.kraken.com/exchange/api-reference/futures-websocket/trade)
  explicitly define `side` as taker side. Their snapshots contain prior trades.
- [Kraken futures ticker](https://docs.kraken.com/exchange/api-reference/futures-websocket/ticker)
  documents `funding_rate`, `markPrice`, and `openInterest`.
  [Kraken L3](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/level3)
  documents order events, per-event prices and quantities, top-ten checksums and
  truncation after a frame.

## Validation and audit

```sh
cd engine
$HOME/.cargo/bin/cargo test --release --locked
$HOME/.cargo/bin/cargo clippy --locked --all-targets -- -D warnings
$HOME/.cargo/bin/cargo fmt --check
# Validate Parquet ordering and reservoir-sample five observed Kraken BTC mids:
$HOME/.cargo/bin/cargo run --release --locked -p engine-replay --example audit -- /path/to/replay-report.json
```

Tests cover heap ties/clock steps, exact cutoff, A/B trade dedup, bounded IDs,
ctVal book/trade scaling, exact volume totals, warm-up/returns/volatility,
invalid-book nulls, cross math, native extras/trade snapshots, sequential L3 ranks,
gzip/open partial lines, atomic nullable Parquet output and a synthetic day CLI.

The measured archive replay and independent Python results are in [VALIDATION.md](VALIDATION.md).

## V3 fixture checks and throughput

`cargo test --workspace` includes CLI replay with `--root ../tests/fixtures/archive`
for 2026-09-15 and 2026-10-08, checking every pair, book validity, Kraken L2/L3
CRCs, sequence breaks, per-asset rows and the appended Parquet column. Original
`tests/fixtures/v2` files remain on disk, unused; `fixtures/v3` replaces them.
Set `ENGINE_TEST_SCRATCH` to a repository-local target directory to keep generated
test reports and Parquet files in the repository.

`cargo run --release -p engine-replay --example benchmark_archive -- ../tests/fixtures/archive`
measures gzip reading, exact v3 parsing, head merging and shared core ingestion,
including book/checksum/trade processing. It runs each fixture date four times;
run 0 warms the cache, and runs 1–3 are the measured samples. It excludes full-day
empty ticks and Parquet serialization: sparse excerpts still produce full-day
rows, so the CLI's end-to-end `records_per_s` measures a different workload.
The CLI report includes per-stream `books` counters and `row_counts` in addition
to the existing coverage and timing fields.

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

## Deribit DVOL (features v5)

`ENGINE_VERSION` ends in `-features-v5`; checkpoints from earlier versions are
rejected. The five new nullable DOUBLE columns are appended after existing
Parquet columns and also appear in JSON/live output and inference's raw schema.
They populate only the `cross/cross/BTC` and `cross/cross/ETH` rows. DVOL never
creates an index market row, and existing market features retain their values.
If several Binance spot pairs have the same asset, the first native symbol with
a known `rv_3600s` supplies realized volatility, matching deterministic venue
selection elsewhere.

- `dvol`: CLOSE of the newest usable one-minute DVOL candle, in annualized
  volatility percentage points (52.3 means 52.3% annualized implied volatility).
  Candle timestamps mark the START, not the close. At tick T a candle is usable
  iff `max(recv_ns, candle_start_ns + 70 seconds) <= T`; the 70-second floor gives
  early live receives and fixed-lag backfill identical feature availability.
  Later live receives remain unavailable until actually received. A newest usable
  candle older than 180 seconds **from its start** is null (exactly 180 is fresh).
- `dvol_chg_300s`, `dvol_chg_900s`, `dvol_chg_3600s`: current usable value minus
  the usable value as of `T-W`, in volatility points, with the same availability
  and staleness rules at both endpoints. Either unknown endpoint makes the change
  null; no interpolation is used and interior gaps are allowed.
- `dvol_rv_gap`: `dvol - Binance_spot_rv_3600s * sqrt(365*24) * 100`, for the
  matching asset. The underlying RV is the square root of the sum of existing
  one-second squared log returns over an hour. Scaling annualizes the hourly
  value and converts it to the same volatility points as DVOL. Null if either
  value is missing; this uses the existing RV coverage semantics.

Closed candles are immutable observations: duplicate timestamps keep the first
received value/availability, and connection controls do not erase known candles.
State retains the hour plus 180-second endpoint grace and pending candles, and
is included in checkpoints, `as_live` and `--from-state` replay. Archive input
accepts numeric and exact-string OHLC values; the reader retains the inner row's
text without decimal reserialization. See [server backfill](../../README.md#deribit-dvol-p22-features-v5)
for receive-day files and operational instructions.

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
