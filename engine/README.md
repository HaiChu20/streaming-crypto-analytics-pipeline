# Engine core

`engine_core` is synchronous and performs no I/O. Construct one `StreamBook` per
(exchange, market, feed, symbol), parse archived lines with `Record::parse`, and
call `consume` in file order. Kafka coordinates are optional for probe envelopes.
Tick ordering, features, replay file readers, and live ingestion are later phases.

`Fixed` uses signed i64 units of 10^-8; parsing rejects excess precision and
overflow. `Book::mid` returns `Mid` in i128 units of 10^-9 so half an atom is exact.
Native decimal text is retained for checksums, including Kraken's trailing zeros
and exponent expansion (the collector's Decimal `_fmt`, without metadata padding).
L3 queues preserve insertion order, including when an order is modified.

`valid()` must be checked before using `book()` for features. Pending or stale
books can retain levels for diagnostics; ended connections release their books
and buffered deltas. `connection_count()` reports retained live/pending states.
`Outcome`, `last_break()`, and `counters()` expose continuity, checksum results,
applied updates, and breaks. A drop or detected break retires the connection ID
permanently; recovery uses a new connection start plus a snapshot. Stale Binance
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
