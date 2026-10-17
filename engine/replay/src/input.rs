use crate::{Result, SECOND, error};
use engine_core::{Kind, Record};
use flate2::read::MultiGzDecoder;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap, VecDeque},
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub path: PathBuf,
    pub bytes: u64,
}
/// Path metadata follows config/markets.yaml's native symbols and asset partitions.
#[derive(Debug, Clone)]
pub struct StreamPath {
    pub exchange: String,
    pub market: String,
    pub feed: String,
    pub symbol: String,
    pub partition: i32,
}
impl StreamPath {
    pub fn new(path: &Path) -> Result<Self> {
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| error("invalid raw filename"))?;
        let stem = name
            .strip_suffix(".jsonl")
            .or_else(|| name.strip_suffix(".jsonl.gz"))
            .or_else(|| name.strip_suffix(".jsonl.open"))
            .ok_or_else(|| error("expected v2 .jsonl[.gz|.open] file"))?;
        let (date, feed) = stem
            .split_once('_')
            .ok_or_else(|| error("expected YYYYMMDD_feed filename"))?;
        if date.len() != 8 || !date.bytes().all(|b| b.is_ascii_digit()) || !known_feed(feed) {
            return Err(error("invalid v2 date/feed"));
        }
        let symbol_dir = path
            .parent()
            .ok_or_else(|| error("missing symbol directory"))?;
        let market_dir = symbol_dir
            .parent()
            .ok_or_else(|| error("missing market directory"))?;
        let exchange_dir = market_dir
            .parent()
            .ok_or_else(|| error("missing exchange directory"))?;
        let component = |p: &Path| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(str::to_owned)
                .ok_or_else(|| error("invalid raw path"))
        };
        let exchange = component(exchange_dir)?;
        let market = component(market_dir)?;
        let disk_symbol = component(symbol_dir)?;
        let symbol = if exchange == "kraken" && market == "spot" {
            disk_symbol.replace('-', "/")
        } else {
            disk_symbol
        };
        let partition = match (exchange.as_str(), market.as_str(), symbol.as_str()) {
            ("binance", "spot" | "perp", "BTCUSDT")
            | ("okx", "spot", "BTC-USDT")
            | ("okx", "perp", "BTC-USDT-SWAP")
            | ("kraken", "spot", "BTC/USD")
            | ("kraken", "perp", "PF_XBTUSD") => 0,
            ("binance", "spot" | "perp", "ETHUSDT")
            | ("okx", "spot", "ETH-USDT")
            | ("okx", "perp", "ETH-USDT-SWAP")
            | ("kraken", "spot", "ETH/USD")
            | ("kraken", "perp", "PF_ETHUSD") => 1,
            _ => {
                return Err(error(format!(
                    "unknown configured stream {}",
                    path.display()
                )));
            }
        };
        Ok(Self {
            exchange,
            market,
            feed: feed.into(),
            symbol,
            partition,
        })
    }
    /// The payload is the native line itself; exchange decoders ignore archive keys.
    pub fn parse(&self, line: &str) -> Result<Option<Record>> {
        #[derive(Deserialize)]
        struct Meta<'a> {
            marker: Option<&'a str>,
            event: Option<&'a str>,
            #[serde(rename = "type")]
            kind: Option<&'a str>,
            action: Option<&'a str>,
            feed: Option<&'a str>,
            conn: Option<String>,
            offset: Option<i64>,
            recv_time_ns: Option<i128>,
            reason: Option<String>,
            raw: Option<String>,
        }
        let meta: Meta<'_> = serde_json::from_str(line)?;
        if matches!(meta.marker, Some("rollover" | "gap")) || meta.event == Some("gap") {
            return Ok(None);
        }
        let (kind, event, reason, detail) = if meta.raw.is_some() && meta.marker.is_none() {
            // The recorder could not store a native JSON object. Keep its offset,
            // and make the uncertainty visible to the same core path used by a
            // malformed Kafka envelope.
            (
                Kind::Control,
                Some("drop".into()),
                Some("malformed_envelope".into()),
                None,
            )
        } else if let Some(marker) = meta.marker {
            let event = match (marker, meta.event) {
                ("ws_event", Some("connected")) => "start",
                ("ws_event", Some("disconnected")) => "stop",
                ("ws_event", Some("connection_error")) => "drop",
                _ => {
                    return Err(error(format!(
                        "unknown v2 marker/event {marker}/{:?}",
                        meta.event
                    )));
                }
            };
            let mut detail: serde_json::Value = serde_json::from_str(line)?;
            if let Some(nested) = detail.get("detail").and_then(|v| v.as_object()).cloned() {
                detail.as_object_mut().unwrap().extend(nested);
            }
            (Kind::Control, Some(event.into()), meta.reason, Some(detail))
        } else {
            let snapshot = meta.kind == Some("snapshot")
                || meta.action == Some("snapshot")
                || meta.feed.is_some_and(|f| f.ends_with("_snapshot"));
            let kind = if snapshot {
                Kind::Snapshot
            } else if matches!(self.feed.as_str(), "trades" | "trades_agg") {
                Kind::Trade
            } else {
                Kind::Update
            };
            (kind, None, None, None)
        };
        Ok(Some(Record {
            exchange: self.exchange.clone(),
            market: self.market.clone(),
            feed: self.feed.clone(),
            symbol: self.symbol.clone(),
            kind,
            conn: meta.conn.ok_or_else(|| error("v2 line missing conn"))?,
            offset: Some(meta.offset.ok_or_else(|| error("v2 line missing offset"))?),
            recv_ns: meta
                .recv_time_ns
                .ok_or_else(|| error("v2 line missing recv_time_ns"))?,
            seq: 0,
            payload: if meta.raw.is_some() {
                String::new()
            } else {
                line.to_owned()
            },
            event,
            reason,
            detail,
            topic: Some(format!(
                "raw.{}.{}.{}.v1",
                self.exchange, self.market, self.feed
            )),
            partition: Some(self.partition),
        }))
    }
}
fn known_feed(feed: &str) -> bool {
    matches!(
        feed,
        "l1" | "l2" | "l3" | "trades" | "trades_agg" | "mark" | "liquidations" | "ticker"
    )
}

pub(crate) struct Source {
    reader: Box<dyn BufRead>,
    path: PathBuf,
    stream: StreamPath,
    open: bool,
    line: u64,
    previous_offset: Option<i64>,
    pub bytes: u64,
}
impl Source {
    pub fn new(path: &Path) -> Result<Self> {
        let stream = StreamPath::new(path)?;
        let file = File::open(path)?;
        let bytes = file.metadata()?.len();
        let capped = file.take(bytes);
        let reader: Box<dyn BufRead> = if path.to_string_lossy().ends_with(".gz") {
            Box::new(BufReader::new(MultiGzDecoder::new(capped)))
        } else {
            Box::new(BufReader::new(capped))
        };
        Ok(Self {
            reader,
            path: path.into(),
            stream,
            open: path.to_string_lossy().ends_with(".open"),
            line: 0,
            previous_offset: None,
            bytes,
        })
    }
    pub fn next(&mut self) -> Result<Option<Record>> {
        let mut text = String::new();
        loop {
            text.clear();
            if self.reader.read_line(&mut text)? == 0 {
                return Ok(None);
            }
            self.line += 1;
            if self.open && !text.ends_with('\n') {
                return Ok(None);
            }
            let record = self
                .stream
                .parse(&text)
                .map_err(|e| error(format!("{}:{}: {e}", self.path.display(), self.line)))?;
            let Some(record) = record else {
                continue;
            };
            if self
                .previous_offset
                .is_some_and(|offset| record.offset.unwrap() <= offset)
            {
                return Err(error(format!(
                    "{}:{}: non-increasing archived offsets",
                    self.path.display(),
                    self.line
                )));
            }
            self.previous_offset = record.offset;
            return Ok(Some(record));
        }
    }
}
struct PartitionSource {
    files: VecDeque<Source>,
    previous_ns: Option<i128>,
    previous_offset: Option<i64>,
    clock_steps: u64,
}
impl PartitionSource {
    fn next(&mut self) -> Result<Option<Record>> {
        while let Some(file) = self.files.front_mut() {
            if let Some(record) = file.next()? {
                if self
                    .previous_offset
                    .is_some_and(|offset| record.offset.unwrap() <= offset)
                {
                    return Err(error("non-increasing offsets across partition files"));
                }
                if self.previous_ns.is_some_and(|ns| record.recv_ns < ns) {
                    self.clock_steps += 1;
                }
                self.previous_ns = Some(record.recv_ns);
                self.previous_offset = record.offset;
                return Ok(Some(record));
            }
            self.files.pop_front();
        }
        Ok(None)
    }
}
struct Head {
    record: Record,
    source: usize,
}
impl Head {
    fn key(&self) -> (i128, &str, i32, i64, usize) {
        (
            self.record.recv_ns,
            self.record.topic.as_deref().unwrap(),
            self.record.partition.unwrap(),
            self.record.offset.unwrap(),
            self.source,
        )
    }
}
impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

/// Only each partition's current head participates. A clock step-back is exposed after
/// its predecessor is popped; it is never moved ahead of that predecessor.
pub struct Merge {
    sources: Vec<PartitionSource>,
    heap: BinaryHeap<Reverse<Head>>,
    pub snapshots: Vec<Snapshot>,
}
impl Merge {
    pub fn new(mut paths: Vec<PathBuf>) -> Result<Self> {
        paths.sort();
        let mut grouped = BTreeMap::<_, VecDeque<Source>>::new();
        let mut snapshots = Vec::new();
        for path in paths {
            let source = Source::new(&path)?;
            let stream = &source.stream;
            let key = (
                stream.exchange.clone(),
                stream.market.clone(),
                stream.feed.clone(),
                stream.partition,
            );
            snapshots.push(Snapshot {
                path,
                bytes: source.bytes,
            });
            grouped.entry(key).or_default().push_back(source);
        }
        let mut sources = Vec::new();
        let mut heap = BinaryHeap::new();
        for files in grouped.into_values() {
            let mut source = PartitionSource {
                files,
                previous_ns: None,
                previous_offset: None,
                clock_steps: 0,
            };
            if let Some(record) = source.next()? {
                heap.push(Reverse(Head {
                    record,
                    source: sources.len(),
                }));
            }
            sources.push(source);
        }
        Ok(Self {
            sources,
            heap,
            snapshots,
        })
    }
    pub fn peek_ns(&self) -> Option<i128> {
        self.heap.peek().map(|h| h.0.record.recv_ns)
    }
    pub fn pop(&mut self) -> Result<Option<Record>> {
        let Some(Reverse(head)) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(record) = self.sources[head.source].next()? {
            self.heap.push(Reverse(Head {
                record,
                source: head.source,
            }));
        }
        Ok(Some(head.record))
    }
    pub fn clock_steps(&self) -> u64 {
        self.sources.iter().map(|s| s.clock_steps).sum()
    }
}

pub use engine_core::features::asset;

/// Recover a bad archived envelope without losing its Kafka offset. The recorder
/// always appends the coordinates at the end, even if the source bytes were bad.
/// Missing recv_ns is eligible immediately, so this offset cannot block its queue.
pub fn recover_archived(line: &str, coordinates: Option<(&str, i32, i64)>) -> Result<Record> {
    if let Ok(record) = Record::parse(line) {
        return Ok(record);
    }
    let outer: serde_json::Value = serde_json::from_str(line).unwrap_or_default();
    let (topic, partition, offset) = if let Some((topic, partition, offset)) = coordinates {
        (topic.to_owned(), partition, offset)
    } else {
        let start = line
            .rfind(",\"topic\":")
            .ok_or_else(|| error("malformed archive lacks Kafka coordinates"))?;
        let tail: serde_json::Value = serde_json::from_str(&format!("{{{}", &line[start + 1..]))?;
        (
            tail["topic"]
                .as_str()
                .ok_or_else(|| error("bad archived topic"))?
                .to_owned(),
            tail["partition"]
                .as_i64()
                .ok_or_else(|| error("bad archived partition"))? as i32,
            tail["offset"]
                .as_i64()
                .ok_or_else(|| error("bad archived offset"))?,
        )
    };
    let parts = topic.split('.').collect::<Vec<_>>();
    if parts.len() != 5 || parts[0] != "raw" {
        return Err(error(
            "malformed archive topic is not raw.<exchange>.<market>.<feed>.v1",
        ));
    }
    let (exchange, market, feed) = (parts[1], parts[2], parts[3]);
    let asset = match partition {
        0 => "BTC",
        1 => "ETH",
        _ => return Err(error("unknown asset partition")),
    };
    let symbol = match (exchange, market, asset) {
        ("binance", _, "BTC") => "BTCUSDT",
        ("binance", _, _) => "ETHUSDT",
        ("okx", "spot", "BTC") => "BTC-USDT",
        ("okx", "spot", _) => "ETH-USDT",
        ("okx", "perp", "BTC") => "BTC-USDT-SWAP",
        ("okx", "perp", _) => "ETH-USDT-SWAP",
        ("kraken", "spot", "BTC") => "BTC/USD",
        ("kraken", "spot", _) => "ETH/USD",
        ("kraken", "perp", "BTC") => "PF_XBTUSD",
        ("kraken", "perp", _) => "PF_ETHUSD",
        _ => return Err(error("unknown raw stream in malformed archive")),
    };
    Ok(Record {
        exchange: exchange.into(),
        market: market.into(),
        feed: feed.into(),
        symbol: symbol.into(),
        kind: engine_core::Kind::Control,
        event: Some("drop".into()),
        reason: Some("malformed_envelope".into()),
        conn: outer["conn"].as_str().unwrap_or("__unknown__").into(),
        seq: outer["seq"].as_u64().unwrap_or(0),
        recv_ns: outer["recv_ns"]
            .as_i64()
            .map(i128::from)
            .or_else(|| outer["recv_ns"].as_u64().map(i128::from))
            .or_else(|| {
                line.split_once("\"recv_ns\":").and_then(|(_, tail)| {
                    let digits = tail
                        .trim_start()
                        .chars()
                        .take_while(|c| c.is_ascii_digit() || *c == '-')
                        .collect::<String>();
                    digits.parse::<i128>().ok()
                })
            })
            .unwrap_or(i128::MIN),
        payload: String::new(),
        detail: None,
        topic: Some(topic),
        partition: Some(partition),
        offset: Some(offset),
    })
}

pub fn discover(root: &Path, date: &str, allow_open: bool) -> Result<Vec<PathBuf>> {
    discover_filtered(root, date, allow_open, None, None)
}

pub fn discover_filtered(
    root: &Path,
    date: &str,
    allow_open: bool,
    exchange: Option<&str>,
    market: Option<&str>,
) -> Result<Vec<PathBuf>> {
    day_bounds(date)?;
    let base = if root.join("raw").is_dir() {
        root.join("raw")
    } else {
        root.to_owned()
    };
    let mut files = Vec::new();
    fn walk(path: &Path, depth: usize, files: &mut Vec<PathBuf>) -> Result<()> {
        if depth == 0 {
            return Ok(());
        }
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                walk(&entry.path(), depth - 1, files)?;
            } else if entry.file_type()?.is_file() {
                files.push(entry.path());
            }
        }
        Ok(())
    }
    walk(&base, 4, &mut files)?;
    let prefix = format!("{}_", date.replace('-', ""));
    let mut chosen = BTreeMap::<PathBuf, (u8, PathBuf)>::new();
    for path in files {
        let name = path.file_name().unwrap().to_string_lossy();
        if !name.starts_with(&prefix) {
            continue;
        }
        let (stem, priority) = if let Some(s) = name.strip_suffix(".jsonl") {
            (s, 0)
        } else if let Some(s) = name.strip_suffix(".jsonl.gz") {
            (s, 1)
        } else if allow_open && let Some(s) = name.strip_suffix(".jsonl.open") {
            (s, 2)
        } else {
            continue;
        };
        if !known_feed(&stem[prefix.len()..]) {
            eprintln!("warning: skipping unknown feed {}", path.display());
            continue;
        }
        let stream = StreamPath::new(&path)?;
        if exchange.is_some_and(|e| e != stream.exchange)
            || market.is_some_and(|m| m != stream.market)
        {
            continue;
        }
        let key = path.parent().unwrap().join(stem);
        if chosen.get(&key).is_none_or(|(p, _)| priority < *p) {
            chosen.insert(key, (priority, path));
        }
    }
    let paths = chosen.into_values().map(|(_, p)| p).collect::<Vec<_>>();
    if paths.is_empty() {
        return Err(error(
            "no v2 input streams found (use --allow-open for .jsonl.open)",
        ));
    }
    Ok(paths)
}

pub fn day_bounds(date: &str) -> Result<(i64, i64)> {
    let parts = date
        .split('-')
        .map(str::parse::<i64>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if parts.len() != 3
        || date.len() != 10
        || !(1970..=2200).contains(&parts[0])
        || !(1..=12).contains(&parts[1])
    {
        return Err(error("date must be YYYY-MM-DD (1970..2200)"));
    }
    let leap = |y: i64| y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let months = [
        31,
        if leap(parts[0]) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=months[parts[1] as usize - 1]).contains(&parts[2]) {
        return Err(error("invalid day"));
    }
    let days = (1970..parts[0])
        .map(|y| if leap(y) { 366 } else { 365 })
        .sum::<i64>()
        + months[..parts[1] as usize - 1].iter().sum::<i64>()
        + parts[2]
        - 1;
    let start = days * 86400 * SECOND;
    Ok((start, start + 86400 * SECOND))
}
