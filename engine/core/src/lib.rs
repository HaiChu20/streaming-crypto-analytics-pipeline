//! Deterministic, synchronous books and feature state. Consume records in stream file order.
//! No I/O or clock access; replay/live routing and tick ordering belong to their callers.
mod binance;
mod book;
pub mod features;
mod fixed;
pub mod kraken;
mod okx;
pub mod row;
pub mod session;

pub const SECOND: i64 = 1_000_000_000;
pub const ENGINE_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-features-v2");

pub use book::{Book, Side};
pub use fixed::{Fixed, Mid};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub String);
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self(e.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Snapshot,
    Update,
    Trade,
    Control,
}

/// Archived recorder line, or an envelope without Kafka coordinates.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Record {
    pub exchange: String,
    pub market: String,
    pub feed: String,
    pub symbol: String,
    pub kind: Kind,
    pub conn: String,
    pub seq: u64,
    /// i128 accepts the complete i64 and u64 timestamp domains without float conversion.
    pub recv_ns: i128,
    #[serde(default)]
    pub payload: String,
    pub event: Option<String>,
    pub reason: Option<String>,
    pub detail: Option<Value>,
    pub topic: Option<String>,
    pub partition: Option<i32>,
    pub offset: Option<i64>,
}
impl Record {
    pub fn parse(line: &str) -> Result<Self> {
        Ok(serde_json::from_str(line)?)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum BreakReason {
    MissingSnapshot,
    SequenceGap { expected: i64, received: i64 },
    ChecksumMismatch { expected: i64, received: i64 },
    Drop { reason: Option<String> },
    Malformed(String),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ignored,
    Buffered,
    Stale,
    Applied,
    Snapshot { buffered_updates: u64 },
    Break(BreakReason),
}
impl Outcome {
    fn gap(expected: i64, received: i64) -> Self {
        Self::Break(BreakReason::SequenceGap { expected, received })
    }
}
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct Counters {
    pub updates_applied: u64,
    pub breaks: u64,
    pub checksums_verified: u64,
    pub okx_checksum_absent: u64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Connection {
    book: Book,
    sync: binance::Sync,
    seq: Option<i64>,
    l3: Option<kraken::L3Book>,
    l3_counts: [i64; 3],
    depth: usize,
    valid: bool,
}
impl Connection {
    fn apply(
        &mut self,
        exchange: &str,
        market: &str,
        feed: &str,
        record: &Record,
        counters: &mut Counters,
    ) -> Result<Outcome> {
        let snapshot = record.kind == Kind::Snapshot;
        let payload: Value = serde_json::from_str(&record.payload)?;
        if exchange == "binance" {
            let data = if snapshot { &payload } else { &payload["data"] };
            return self
                .sync
                .apply(&mut self.book, data, snapshot, market == "perp");
        }
        if !snapshot && self.seq.is_none() {
            return Ok(if exchange == "okx" {
                Outcome::Break(BreakReason::MissingSnapshot)
            } else {
                Outcome::Ignored
            });
        }
        if snapshot {
            self.book = Book::default();
        }
        if exchange == "okx" {
            let data = &payload["data"][0];
            let seq = integer(data, "seqId")?;
            if !snapshot {
                let prev = integer(data, "prevSeqId")?;
                if self.seq != Some(prev) {
                    return Ok(Outcome::gap(self.seq.unwrap(), prev));
                }
            }
            self.book.arrays(data, "bids", "asks")?;
            let expected = integer(data, "checksum")?;
            if expected == 0 {
                // 2026-10-17: live OKX books carry checksum 0, meaning not provided.
                counters.okx_checksum_absent += 1;
            } else {
                let received = i64::from(okx::checksum(&self.book));
                if expected != received {
                    return Ok(Outcome::Break(BreakReason::ChecksumMismatch {
                        expected,
                        received,
                    }));
                }
                counters.checksums_verified += 1;
            }
            self.seq = Some(seq);
        } else if market == "perp" {
            let seq = integer(&payload, "seq")?;
            if !snapshot && i128::from(seq) != i128::from(self.seq.unwrap()) + 1 {
                return Ok(Outcome::gap(self.seq.unwrap().saturating_add(1), seq));
            }
            if snapshot {
                self.book.objects(&payload)?;
            } else {
                let side = match payload["side"].as_str() {
                    Some("buy") => Side::Bid,
                    Some("sell") => Side::Ask,
                    _ => return Err(Error("invalid futures side".into())),
                };
                self.book.set(side, &payload["price"], &payload["qty"])?;
            }
            self.seq = Some(seq);
        } else {
            let data = &payload["data"][0];
            let received = if feed == "l3" {
                let book = self
                    .l3
                    .get_or_insert_with(|| kraken::L3Book::new(self.depth));
                self.l3_counts = book.apply_counted_into(data, snapshot, &mut self.book)?;
                book.checksum()
            } else {
                self.book.objects(data)?;
                self.book.truncate(self.depth);
                kraken::l2_checksum(&self.book)
            };
            let expected = integer(data, "checksum")?;
            let received = i64::from(received);
            if expected != received {
                return Ok(Outcome::Break(BreakReason::ChecksumMismatch {
                    expected,
                    received,
                }));
            }
            counters.checksums_verified += 1;
            self.seq = Some(0); // spot has checksums rather than exchange sequence IDs
        }
        Ok(if snapshot {
            Outcome::Snapshot {
                buffered_updates: 0,
            }
        } else {
            Outcome::Applied
        })
    }
}

/// One (exchange, market, feed, symbol) stream, including overlapping connections.
#[derive(Serialize, Deserialize, Debug)]
pub struct StreamBook {
    exchange: String,
    market: String,
    feed: String,
    symbol: String,
    active: Option<String>,
    connections: BTreeMap<String, Connection>,
    retired: BTreeSet<String>,
    counters: Counters,
    last_break: Option<BreakReason>,
}
impl StreamBook {
    pub fn new(record: &Record) -> Result<Self> {
        if !matches!(
            (
                record.exchange.as_str(),
                record.market.as_str(),
                record.feed.as_str()
            ),
            ("binance" | "okx", "spot" | "perp", "l2")
                | ("kraken", "spot", "l2" | "l3")
                | ("kraken", "perp", "l2")
        ) {
            return Err(Error("unsupported book stream".into()));
        }
        Ok(Self {
            exchange: record.exchange.clone(),
            market: record.market.clone(),
            feed: record.feed.clone(),
            symbol: record.symbol.clone(),
            active: None,
            connections: BTreeMap::new(),
            retired: BTreeSet::new(),
            counters: Counters::default(),
            last_break: None,
        })
    }
    pub fn valid(&self) -> bool {
        self.active
            .as_ref()
            .and_then(|id| self.connections.get(id))
            .is_some_and(|c| c.valid)
    }
    /// Pending/stale levels may be invalid; ended connections have no book.
    pub fn book(&self) -> Option<&Book> {
        self.active
            .as_ref()
            .and_then(|id| self.connections.get(id))
            .map(|c| &c.book)
    }
    /// Counts for the last applied L3 frame; snapshots are not order events.
    pub fn l3_event_counts(&self) -> Option<[i64; 3]> {
        self.active
            .as_ref()
            .and_then(|id| self.connections.get(id))
            .filter(|c| c.valid && c.l3.is_some())
            .map(|c| c.l3_counts)
    }
    pub fn active_connection(&self) -> Option<&str> {
        self.active.as_deref()
    }
    /// Number of live or pending connection states retained by this stream.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }
    fn retire(&mut self, id: &str) {
        self.connections.remove(id);
        self.retired.insert(id.to_owned());
    }
    pub fn invalidate_malformed(&mut self, id: &str) -> bool {
        if self.retired.contains(id) {
            return false;
        }
        self.retire(id);
        self.counters.breaks += 1;
        self.last_break = Some(BreakReason::Malformed("invalid exchange frame".into()));
        true
    }
    pub fn counters(&self) -> Counters {
        self.counters
    }
    pub fn last_break(&self) -> Option<&BreakReason> {
        self.last_break.as_ref()
    }
    pub fn consume(&mut self, record: &Record) -> Result<Outcome> {
        if (
            record.exchange.as_str(),
            record.market.as_str(),
            record.feed.as_str(),
            record.symbol.as_str(),
        ) != (
            self.exchange.as_str(),
            self.market.as_str(),
            self.feed.as_str(),
            self.symbol.as_str(),
        ) {
            return Err(Error("record belongs to another stream".into()));
        }
        if self.retired.contains(&record.conn) || record.kind == Kind::Trade {
            return Ok(Outcome::Ignored);
        }
        if record.kind == Kind::Control {
            match record.event.as_deref() {
                Some("start") => {
                    let depth = record
                        .detail
                        .as_ref()
                        .and_then(|v| v["ack"]["result"]["depth"].as_u64())
                        .unwrap_or(100);
                    self.connections.insert(
                        record.conn.clone(),
                        Connection {
                            depth: usize::try_from(depth)
                                .map_err(|_| Error("depth overflow".into()))?,
                            ..Default::default()
                        },
                    );
                    return Ok(Outcome::Ignored);
                }
                Some("drop") => {
                    if record.reason.as_deref() == Some("collector_crash") {
                        // The new connection reports the previous session's crash before its start.
                        if let Some(old) = self.active.take() {
                            if old != record.conn {
                                self.retire(&old);
                            } else {
                                self.connections.remove(&old);
                            }
                        }
                    } else {
                        self.retire(&record.conn);
                    }
                    let reason = BreakReason::Drop {
                        reason: record.reason.clone(),
                    };
                    self.counters.breaks += 1;
                    self.last_break = Some(reason.clone());
                    return Ok(Outcome::Break(reason));
                }
                Some("stop") => {
                    self.retire(&record.conn);
                    return Ok(Outcome::Ignored);
                }
                _ => return Err(Error("unknown control event".into())),
            }
        }
        if record.kind == Kind::Snapshot
            && self.active.as_deref() != Some(&record.conn)
            && let Some(old) = self.active.replace(record.conn.clone())
        {
            self.retire(&old);
        }
        let connection = self
            .connections
            .entry(record.conn.clone())
            .or_insert_with(|| Connection {
                depth: 100,
                ..Default::default()
            });
        let result = connection.apply(
            &self.exchange,
            &self.market,
            &self.feed,
            record,
            &mut self.counters,
        );
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(error) => {
                self.retire(&record.conn);
                self.counters.breaks += 1;
                self.last_break = Some(BreakReason::Malformed(error.0.clone()));
                return Err(error);
            }
        };
        match &outcome {
            Outcome::Snapshot { buffered_updates } => {
                connection.valid = true;
                self.counters.updates_applied += buffered_updates;
            }
            Outcome::Applied => {
                self.counters.updates_applied += 1;
            }
            Outcome::Stale => {
                connection.valid = false;
            }
            Outcome::Break(reason) => {
                self.retire(&record.conn);
                self.counters.breaks += 1;
                self.last_break = Some(reason.clone());
            }
            _ => {}
        }
        Ok(outcome)
    }
}

pub(crate) fn integer(value: &Value, field: &str) -> Result<i64> {
    value[field]
        .as_i64()
        .ok_or_else(|| Error(format!("missing or invalid integer {field}")))
}
