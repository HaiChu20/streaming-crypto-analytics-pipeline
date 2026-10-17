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

Only v2 files under `raw/<exchange>/<market>/<SYMBOL>/YYYYMMDD_<feed>.jsonl`
define the streams (the root may be either the data directory or `raw` itself).
`--exchange NAME` and `--market NAME` filter ideal replay. Missing streams are
absent; cross features requiring them are null. Native symbols and BTC/ETH
partitions follow `config/markets.yaml`; no YAML parser is required. Known feeds are l1, l2, l3, trades, trades_agg, mark, liquidations and ticker.
Unknown feeds are skipped with a warning. If several representations of the same
stream exist, prefer `.jsonl`, then `.jsonl.gz`, then `.jsonl.open`. Open files
require `--allow-open`. Every reader is capped at the file's size when opened;
unterminated final lines of open files are ignored. Concatenated gzip members
are supported. File paths and byte limits in the report permit reproduction on
an append-only archive without stopping collection.

A min-heap contains one head per buffered reader and compares
`(recv_ns, topic, partition, offset, file_index)`. The final file index only breaks
otherwise identical coordinates. Archive lines require `recv_time_ns`, `conn` and `offset`; paths supply topic,
partition, exchange, market, symbol and feed. Marker events map back to controls,
including start metadata; gap and rollover markers are ignored. Native payload text is
retained without reserialization, preserving exact decimal tokens for CRC32.
Files of a partition are read sequentially, and each partition is advanced only
after popping its previous head. Thus a receive
clock step-back cannot move a later line before its predecessor: its older time
is exposed only after that predecessor. This preserves file order even when a
globally increasing timestamp order is impossible. Older hidden records become
eligible at that later tick. Live queues and checkpoint/as_live batches use the
same head-only ordering rule. Flow attribution uses one shared helper:
`max(ceil_to_second(recv_ns), current_tick)`. Thus a late record's trade,
liquidation or L3 event flow appears in the tick that processes it, never a
closed bucket. No-step input produces the same order as the former global sort.

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

Rows are sorted by `(tick_ns, exchange, market, symbol, engine_version)`. The
version is the shared `engine_core::ENGINE_VERSION`.

- L2 supplies book features. Invalid, empty or crossed books have null book values.
  Midpoint source values remain exact fixed point. `mid_e9` is an exact i64
  with nine implied decimals, so half of a 1e-8 atom is represented without
  rounding. Returns use `mid_e9`. Extreme values outside its i64 range produce an
  explicit error; these BTC/ETH prices fit comfortably. There is no decimal-text
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
- Funding values are the native fields requested by the spec, without assuming
  identical exchange units/periods. Kraken ticker mark/open interest and Binance
  mark/funding are carried until their active stream ends or its receive age
  exceeds 60 seconds. Liquidation flow counts
  pushed sampled forceOrder messages and uses `z`, cumulative filled quantity,
  rather than the original order quantity `q`.
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
