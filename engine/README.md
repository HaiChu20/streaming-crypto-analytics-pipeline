# Engine core

`engine_core` is synchronous and performs no I/O. Construct one `StreamBook` per
(exchange, market, feed, symbol), parse archived lines with `Record::parse`, and
call `consume` in file order. Kafka coordinates are optional for probe envelopes.
Pure feature state is shared in `engine_core::features`. The [ideal replay CLI](replay/README.md)
handles file ordering, ticks and Parquet; live ingestion is a later phase.

`Fixed` uses signed i64 units of 10^-8; parsing rejects excess precision and
overflow. `Book::mid` returns `Mid` in i128 units of 10^-9 so half an atom is exact.
Native decimal text is retained for checksums, including Kraken's trailing zeros
and exponent expansion (the collector's Decimal `_fmt`, without metadata padding).
L3 queues preserve insertion order, including when an order is modified.

`valid()` must be checked before using `book()` for features. Pending or stale
books can retain levels for diagnostics; ended connections release their books
and buffered deltas. `connection_count()` reports retained live/pending states.
`Outcome`, `last_break()`, and `counters()` expose continuity, checksum results,
applied updates, and breaks. Normal drops or detected breaks retire the connection
ID permanently; recovery uses a new connection start plus a snapshot.
`drop(reason=collector_crash)` is carried by the restarted collector's new
connection: it retires the previous active source, preserving the carrying ID for
its following start and snapshot. Stale Binance
REST snapshots wait for a retry without a break. A replacement connection's first
snapshot retires the old source, including its later control and snapshot records.

From `engine/`:

```sh
$HOME/.cargo/bin/cargo test --release --locked
$HOME/.cargo/bin/cargo clippy --locked --all-targets -- -D warnings
$HOME/.cargo/bin/cargo fmt --check
ENGINE_BENCH_N=100 $HOME/.cargo/bin/cargo test --release timing -- --ignored --nocapture
```

The root toolchain file uses the installed `stable` alias, currently rustc 1.99.1
(58cf8f9c3 2026-10-13), because a separate `1.99.1` alias is not installed and
would trigger a rustup download. `ENGINE_BENCH_EXCHANGE=binance` (or `kraken`,
`okx`) selects one exchange. Timing includes envelope parsing, payload parsing,
reconstruction and available checksum verification; fixture reads are outside the
timer. Each iteration creates fresh stream states.

## OKX checksum availability

On 2026-10-17 all 192,432 OKX spot BTC book messages checked in the live archive
had checksum zero (supervisor observation). The supplied fixtures likewise have
zero in all 564 spot and 598 perp messages. As explicitly agreed, zero means
"not provided": it increments `okx_checksum_absent` without verification.
Nonzero checksums remain strictly verified; sequence continuity is always checked.
`checksums_verified` counts only actual successful checksum comparisons.

The CRC32 algorithm interleaves descending bids and ascending asks, up to 25
levels per side, joins native price and size text with colons, and returns a
signed i32. Synthetic tests use independently Python-zlib-checked constants,
including signed checksums, unequal side lengths, updates, deletes, the top-25
boundary, and retained decimal text. The fixtures remain unchanged.

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

Live connects only to `localhost:9092`, assigns the 44 `raw.*` partitions at their
current high watermarks, and never commits consumer offsets. It produces only to
`features.1s.v1` and `engine.cutoffs.v1` (one partition, 72-hour retention). No
collector or pipeline process is started or changed by this command. SIGINT and
SIGTERM finish the current tick, flush acknowledged output, and fsync the journals.
A later invocation starts a new session at latest; automatic crash recovery is not
implemented. A mid-session restart leaves a gap that `as_live` reports rather than
inventing ticks across it.

Tick T runs at T + 250 ms. Each partition queue is in offset order: extraction
stops at its first `recv_ns > T`, even if subsequent offsets have earlier timestamps.
After selecting these prefixes, the entire selected batch is sorted by
`(recv_ns, topic, partition, offset)`. This is also the order used by `as_live`;
within a selected prefix a clock step can therefore change application order.
Ideal replay retains its original one-head-per-file merge behavior. Live rows
use actual wall-clock emission times for `available_ns`; replay uses T + 250 ms.

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
