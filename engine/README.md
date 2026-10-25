# Rust engine: books, features, replay and live ingestion

`engine_core` is synchronous and performs no I/O. Construct one `StreamBook` per
(exchange, market, feed, symbol), parse Kafka v1 envelopes with `Record::parse`, and
call `consume_at_tick` in partition offset order. Replay reads native v3 raw files
through a path/line adapter, with an explicit exchange-clock option for older
Kraken spot archives missing receive timestamps.
The Kafka envelope path is unchanged. Pure feature state is shared in
`engine_core::features`. The [replay CLI](replay/README.md) handles archive ordering,
ticks and Parquet; `engine-live` handles Kafka ingestion, checkpoints and cutoffs.
The engine version is `0.1.0-features-v7`.

Kraken and Binance spot/perpetual books and flows, OKX spot/swap books and trades,
and Deribit BTC/ETH DVOL are supported. OKX swaps require contract metadata for
base-quantity scaling. Coinbase and Bybit are collected and archived but have no
engine adapters yet; live assignment skips their raw topics. The
[Strategy 1 results](../docs/strategy-1.md) use Kraken/Binance/Deribit inputs.

`Fixed` uses signed i64 units of 10^-8; parsing rejects excess precision and
overflow. `Book::mid` returns `Mid` in i128 units of 10^-9 so half an atom is exact.
Native decimal text is retained for checksums, including Kraken's trailing zeros
and exponent expansion (the collector's Decimal `_fmt`, without metadata padding).
Kraken L3 price queues sort by each order's timestamp, breaking ties by arrival
order. The book truncates each side to its subscribed price-level depth after
every frame; orders outside that window need no delete and may re-enter through
an add with their original timestamp. Duplicate adds replace the old order.
Modifies remove and reinsert using the event timestamp: decreases normally
retain priority, while increases receive a newer timestamp and move back.

`valid()` must be checked before using `book()` for features. Pending or stale
books can retain levels for diagnostics; ended connections release their books
and buffered deltas. `connection_count()` reports retained live/pending states.
`Outcome`, `last_break()`, and `counters()` expose continuity, checksum results,
applied updates, and breaks. Ended connections retire permanently. Binance depth
sequence breaks reset synchronization within the same epoch, permitting buffered
deltas and a later REST anchor to resynchronize. Other book breaks require a new
connection start and snapshot.
`drop(reason=collector_crash)` is carried by the restarted collector's new
connection: it retires the previous active source, preserving the carrying ID for
its following start and snapshot. Stale Binance
REST snapshots wait for a retry without a break. A replacement connection's first
snapshot retires the old source, including its later control and snapshot records.

From `engine/`:

```sh
$HOME/.cargo/bin/cargo test --release --locked
$HOME/.cargo/bin/cargo clippy --release --locked --all-targets -- -D warnings
$HOME/.cargo/bin/cargo fmt --check
ENGINE_BENCH_N=100 $HOME/.cargo/bin/cargo test --release timing -- --ignored --nocapture
```

The root toolchain file selects `stable` with Clippy and rustfmt; the crates
require Rust 1.99 or newer. `ENGINE_BENCH_EXCHANGE=binance` (or `kraken`,
`okx`) selects one exchange. Timing includes envelope parsing, payload parsing,
reconstruction and available checksum verification; fixture reads are outside the
timer. Each iteration creates fresh stream states.

## OKX checksum availability

A checksum of zero means "not provided": it increments `okx_checksum_absent`
without verification. Nonzero checksums are strictly verified, and sequence
continuity is always checked. `checksums_verified` counts only successful
checksum comparisons.

The CRC32 algorithm interleaves descending bids and ascending asks, up to 25
levels per side, joins native price and size text with colons, and returns a
signed i32. Synthetic tests use independently Python-zlib-checked constants,
including signed checksums, unequal side lengths, updates, deletes, the top-25
boundary, and retained decimal text. The fixtures remain unchanged.

<a id="timing-release-100-iterations-2026-10-17"></a>
## Validation history

Dated acceptance checks, archive audits and fixture throughput measurements are
kept in the [engine validation log](../docs/archive/engine-validation-log.md).
Use the commands above to check the current checkout.

## Live ingestion and reproducible replay

Build from `engine/` with `$HOME/.cargo/bin/cargo build --release --locked`.
From the repository root:

```sh
PYTHON_DOTENV_DISABLED=1 uv run python scripts/create_topics.py --engine-only
engine/target/release/engine-live --root data
# Optional bounded validation session: append --run-seconds 1805.
engine/target/release/engine-replay --root data --date YYYY-MM-DD \
  --mode as_live --allow-open --from-state data/engine/date=YYYY-MM-DD/start_state_<tick>.json
```

Live connects only to `localhost:9092`, assigns the 46 supported `raw.*` partitions
at their current high watermarks, and never commits consumer offsets. Topics
starting with `raw.coinbase.` and `raw.bybit.` are skipped because those exchanges
are collected but not yet parsed. The exact 46-partition check remains in force
for the configured Kraken/Binance/OKX/Deribit streams; other topic-layout changes
require a corresponding engine update. It produces only to
`features.1s.v1` and `engine.cutoffs.v1` (one partition, 72-hour retention). No
collector or pipeline process is started or changed by this command. SIGINT and
SIGTERM finish the current tick, flush acknowledged output, and fsync the journals.
A later invocation starts a new session at latest; automatic crash recovery is not
implemented. A mid-session restart leaves a gap that `as_live` reports rather than
inventing ticks across it.

Tick T runs at T + 250 ms. Each partition queue stays in offset order. The merge
chooses among partition heads by `(recv_ns, topic, partition, offset)` and stops
when no head is eligible at T. A future head blocks older timestamps behind it;
backward clock steps never reorder a partition. `as_live` uses the same merge.
Ideal replay likewise merges one head per input file. Live rows use actual
wall-clock emission times for `available_ns`; replay uses T + 250 ms.

Before the startup tick, live atomically saves
`data/engine/date=D/start_state_<tick>.json`. At UTC midnight, before that day's
first tick, it saves `start_state.json` (also at startup if the startup tick is
midnight). The versioned JSON contains books, pending Binance deltas, L3 queues,
rolling history, trade-ID sets/FIFOs, active/retired connections, contract metadata,
freshness timestamps, and the previous cutoff. Floating-point state round-trips
exactly. Prefetched transport queues are recovered from archives using offsets.

Cutoffs are published after all feature rows have been acknowledged, then appended
to `cutoffs.jsonl`. Carry-in consists of exact recorder-compatible original lines
with Kafka coordinates, copied to `carry_in.jsonl` when their receive date precedes
the tick's day. Journals are fsynced every two seconds and on shutdown; each day
checkpoint uses write/fsync/rename/directory-fsync. This is not a Kafka transaction:
a crash can leave feature rows without a cutoff, or a Kafka cutoff not yet in the
local fsynced journal. Replay uses complete archived cutoffs; stable row identities
permit downstream deduplication. Do not run multiple live writers against the same
root concurrently.

`as_live` reads only the selected day's raw files and its carry-in, starts from the
specified checkpoint (default `start_state.json`), and applies exactly offsets
`(previous_cutoff, cutoff]`. Missing or nonconsecutive offsets, skipped ticks, and
incompatible engine versions are errors. Open raw files are bounded at their
initial length and only complete lines are read. Wait for the recorder to catch
up before replaying a just-ended session. `--json-out path` additionally writes
replay rows as JSONEachRow for exact parity comparisons; Parquet metadata records
`mode=as_live`. Rows differ from live only in `available_ns`.

Starting at latest does not reconstruct snapshots or contract metadata already
sent before startup. Book features remain invalid/null until a genuine snapshot
(and required OKX metadata) arrives. No old price is fabricated. The same 60-second
book/extras expiry and 300-second warm-up apply in both modes.
OKX swap trades seen before their connection's `start` are counted and deduplicated,
but volume fields stay null while `ctVal` is unknown or a tick contains any
uncalibrated trade. A malformed exchange frame increments `data_anomalies` and
invalidates the affected book connection or flow bucket; later streams and ticks
continue. Malformed outer JSON also retains its Kafka offset for replay; when its
receive timestamp is missing, its trade flow stays null because its 1s window
cannot be established. The `as_live` report counts `late_records` whose receive timestamp was
already eligible at the previous tick but whose offset first appears in the
current cutoff.
The replay report also lists L2 connection switches, their tick, and the source
validity on each side of the switch for overlap audits.

## Replay ordering and late flow

All modes merge only each partition's next record by
`(recv_ns, topic, partition, offset)`. Offset order survives backward receive-clock
steps, both inside tick batches and across raw files. Live queues and checkpoints
share one batch merge implementation; ideal replay streams partition heads.
Flow buckets use `max(window_tick(recv_ns), current_tick)` in the shared core, so
late trade, liquidation and L3 flow reaches an emitted row. Freshness still uses
the original receive timestamp. No-step ordering equals the former global sort.

Raw v3 paths are `raw/<Exchange>/<Market>/<Pair>/YYYYMMDD_<channel>.jsonl[.gz|.open]`.
The root may be the raw tree or its parent. The adapter derives stream coordinates
from the path, maps markers to controls and retains native decimal text. Missing
offsets use physical line indices; missing connection IDs use per-file epochs.
Receive time is required for data unless the explicit Kraken spot
`--exchange-clock-ms` option is used. Open files require `--allow-open`.
JSON decimal strings and number tokens both use exact fixed point;
Kraken CRC32 uses their original decimal characters. See
[replay usage](replay/README.md) and [historical validation](../docs/archive/engine-validation-log.md).

## Fast cross-venue lead features (features-v7)

Binance spot and perpetual bookTicker (`l1`) quotes supply a private fast midpoint.
Prices are parsed as exact fixed integers; the midpoint retains half an atom in
10^-9 units. Only the active connection's increasing update IDs are accepted.
Stop/drop and collector crashes clear its quotes. L1 uses the same 60-second
freshness limit as L2; stale, locked, crossed or absent L1 falls back to validated
L2. Kraken spot and perpetual fast mids use validated L2. L1 does not affect the
existing book, flow, return, basis or mids-grid columns; OKX BBO remains unused.

Each per-asset `cross` row adds 20 nullable columns: `lead_<pair>_<suffix>` for
pairs `kp_ks`, `bs_ks`, `bp_ks`, `kp_bs`, `kp_bp` and suffixes `gap_dev_60s`,
`gap_dev_300s`, `gap_chg_1s`, `gap_chg_5s`. Codes mean Kraken perp (`kp`), Kraken
spot (`ks`), Binance spot (`bs`) and Binance perp (`bp`), ordered leader→follower.
The gap is `10000 * ln(fast_mid_leader / fast_mid_follower)`. Deviations subtract
the mean of available gaps in the last 60 or 300 one-second ticks, including the
current tick, and require at least half that window to be covered. Changes
subtract the exact one- or five-second lag. Missing legs or lag endpoints produce
nulls; skipped seconds enter history as nulls. Histories retain at most 300 ticks
per asset/pair and use only information consumed by the current tick. All 20
columns are null on instrument rows. Research can exclude them with
`--exclude-group lead` (or exclude all cross columns with `--exclude-group cross`).
