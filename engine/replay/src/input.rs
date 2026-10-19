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
/// Native pair and canonical exchange/market/feed inferred from a v3 path.
#[derive(Debug, Clone)]
pub struct StreamPath {
    pub exchange: String,
    pub market: String,
    pub feed: String,
    pub symbol: String,
    pub partition: i32,
    file: String,
    channel: String,
}
impl StreamPath {
    pub fn new(path: &Path) -> Result<Self> {
        let component = |p: &Path| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(str::to_owned)
                .ok_or_else(|| error("invalid raw path"))
        };
        let name = component(path)?;
        let stem = name
            .strip_suffix(".jsonl")
            .or_else(|| name.strip_suffix(".jsonl.gz"))
            .or_else(|| name.strip_suffix(".jsonl.open"))
            .ok_or_else(|| error("expected v3 .jsonl[.gz|.open] file"))?;
        let (date, channel) = stem
            .split_once('_')
            .ok_or_else(|| error("expected YYYYMMDD_channel filename"))?;
        if date.len() != 8 || !date.bytes().all(|b| b.is_ascii_digit()) {
            return Err(error("invalid v3 date"));
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
        let exchange = canonical_exchange(&component(exchange_dir)?)
            .ok_or_else(|| error("unknown exchange directory"))?
            .to_owned();
        let market = canonical_market(&component(market_dir)?)
            .ok_or_else(|| error("unknown market directory"))?
            .to_owned();
        let feed = channel_feed(&exchange, &market, channel)
            .ok_or_else(|| error("unknown v3 channel"))?
            .to_owned();
        let disk_symbol = component(symbol_dir)?;
        let symbol = if exchange == "kraken" && market == "spot" {
            disk_symbol.replace('-', "/")
        } else {
            disk_symbol
        };
        Ok(Self {
            exchange,
            market,
            feed,
            symbol,
            partition: 0,
            file: path.to_string_lossy().into_owned(),
            channel: channel.into(),
        })
    }
    /// Stateless convenience for explicit coordinates; Source maintains synthetic epochs.
    pub fn parse(&self, line: &str) -> Result<Option<Record>> {
        self.parse_at(line, 0, &mut 0, &mut None)
    }
    fn parse_at(
        &self,
        line: &str,
        index: i64,
        epochs: &mut u64,
        epoch: &mut Option<String>,
    ) -> Result<Option<Record>> {
        #[derive(Deserialize)]
        struct Meta<'a> {
            marker: Option<&'a str>,
            event: Option<&'a str>,
            conn: Option<&'a str>,
            offset: Option<i64>,
            recv_time_ns: Option<i128>,
            timestamp: Option<&'a str>,
            reason: Option<String>,
            raw: Option<&'a str>,
            #[serde(borrow)]
            data: Option<&'a serde_json::value::RawValue>,
        }
        let meta: Meta<'_> = serde_json::from_str(line)?;
        if matches!(meta.marker, Some("rollover" | "gap")) || meta.event == Some("gap") {
            return Ok(None);
        }
        // A user's marker conn is a recorder label reused on reconnect. Explicit
        // conn+offset coordinates are our globally unique connection epochs.
        if meta.marker == Some("ws_event") && meta.event == Some("connected") {
            *epochs += 1;
            *epoch = Some(if meta.offset.is_some() {
                meta.conn
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("{}#{}", self.file, epochs))
            } else {
                format!("{}#{}", meta.conn.unwrap_or(&self.file), epochs)
            });
        }
        let conn = if meta.marker.is_some() && meta.offset.is_none() {
            epoch.clone()
        } else {
            meta.conn.map(str::to_owned).or_else(|| epoch.clone())
        }
        .unwrap_or_else(|| format!("{}#pre", self.file));
        let (kind, event, reason, detail, payload) = if meta.raw.is_some() && meta.marker.is_none()
        {
            (
                Kind::Control,
                Some("drop".into()),
                Some("malformed_envelope".into()),
                None,
                String::new(),
            )
        } else if let Some(marker) = meta.marker {
            let event = match (marker, meta.event) {
                ("ws_event", Some("connected")) => "start",
                ("ws_event", Some("disconnected" | "rotating" | "shutdown")) => "stop",
                ("ws_event", Some("connection_error")) => "drop",
                ("ws_event", Some("corrupt_message")) => "corrupt_message",
                _ => {
                    return Err(error(format!(
                        "unknown v3 marker/event {marker}/{:?}",
                        meta.event
                    )));
                }
            };
            let mut detail: serde_json::Value = serde_json::from_str(line)?;
            if let Some(nested) = detail.get("detail").and_then(|v| v.as_object()).cloned() {
                detail.as_object_mut().unwrap().extend(nested);
            }
            (
                Kind::Control,
                Some(event.into()),
                meta.reason,
                Some(detail),
                String::new(),
            )
        } else {
            let native = if self.exchange == "kraken" && self.market == "spot" {
                line
            } else {
                meta.data
                    .ok_or_else(|| error("wrapped v3 line missing data"))?
                    .get()
            };
            #[derive(Deserialize)]
            struct Shape<'a> {
                #[serde(rename = "type")]
                kind: Option<&'a str>,
                action: Option<&'a str>,
                feed: Option<&'a str>,
            }
            let shape: Shape<'_> = if self.feed == "dvol" {
                Shape {
                    kind: None,
                    action: None,
                    feed: None,
                }
            } else {
                serde_json::from_str(native)?
            };
            let snapshot = self.channel == "snapshot"
                || shape.kind == Some("snapshot")
                || shape.action == Some("snapshot")
                || shape.feed.is_some_and(|f| f.ends_with("_snapshot"));
            let kind = if snapshot {
                Kind::Snapshot
            } else if matches!(self.feed.as_str(), "trades" | "trades_agg") {
                Kind::Trade
            } else {
                Kind::Update
            };
            let detail = (self.exchange == "binance" && self.channel == "snapshot")
                .then(|| serde_json::json!({"archive_snapshot":true}));
            (kind, None, None, detail, native.to_owned())
        };
        let recv_ns = meta
            .recv_time_ns
            .or_else(|| meta.timestamp.and_then(|t| iso_ns(t).ok()))
            .ok_or_else(|| error("v3 line missing receive time or valid marker timestamp"))?;
        Ok(Some(Record {
            exchange: self.exchange.clone(),
            market: self.market.clone(),
            feed: self.feed.clone(),
            symbol: self.symbol.clone(),
            kind,
            conn,
            offset: Some(meta.offset.unwrap_or(index)),
            recv_ns,
            seq: 0,
            payload,
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
fn canonical_exchange(value: &str) -> Option<&'static str> {
    let value = value.to_ascii_lowercase();
    ["binance", "kraken", "okx", "deribit"]
        .into_iter()
        .find(|prefix| value.starts_with(prefix))
}
fn canonical_market(value: &str) -> Option<&'static str> {
    let value = value.to_ascii_lowercase();
    if value.starts_with("spot") {
        Some("spot")
    } else if ["fut", "perp", "swap"].iter().any(|p| value.starts_with(p)) {
        Some("perp")
    } else if value.starts_with("index") {
        Some("index")
    } else {
        None
    }
}
fn channel_feed(exchange: &str, market: &str, channel: &str) -> Option<&'static str> {
    match (exchange, market, channel) {
        ("deribit", "index", "dvol") => Some("dvol"),
        ("binance", _, "depth" | "snapshot") | ("okx" | "kraken", _, "book") => Some("l2"),
        ("binance", _, "bookticker") | ("okx", _, "bbo") => Some("l1"),
        ("okx", _, "trades") | ("binance", "perp", "aggtrades") => Some("trades_agg"),
        ("binance" | "kraken", _, "trades") | ("okx", _, "tradesall") => Some("trades"),
        ("binance", "perp", "markprice") => Some("mark"),
        ("binance", "perp", "forceorder") => Some("liquidations"),
        ("kraken", "spot", "level3") => Some("l3"),
        ("kraken", "perp", "ticker") => Some("ticker"),
        _ => None,
    }
}
fn iso_ns(text: &str) -> Result<i128> {
    let utc = text
        .strip_suffix('Z')
        .or_else(|| text.strip_suffix("+00:00"))
        .ok_or_else(|| error("marker timestamp must be UTC"))?;
    let (date, time) = utc
        .split_once('T')
        .ok_or_else(|| error("invalid ISO timestamp"))?;
    let (start, _) = day_bounds(date)?;
    let (hms, fraction) = time.split_once('.').unwrap_or((time, ""));
    let fields = hms
        .split(':')
        .map(str::parse::<u32>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if fields.len() != 3
        || fields[0] > 23
        || fields[1] > 59
        || fields[2] > 59
        || fraction.len() > 9
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(error("invalid ISO timestamp"));
    }
    let nanos = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i128>()? * 10i128.pow(9 - fraction.len() as u32)
    };
    Ok(i128::from(start)
        + i128::from(fields[0] * 3600 + fields[1] * 60 + fields[2]) * i128::from(SECOND)
        + nanos)
}

pub(crate) struct Source {
    reader: Box<dyn BufRead>,
    path: PathBuf,
    stream: StreamPath,
    open: bool,
    line: u64,
    previous_offset: Option<i64>,
    epochs: u64,
    epoch: Option<String>,
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
            epochs: 0,
            epoch: None,
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
                .parse_at(
                    &text,
                    (self.line - 1) as i64,
                    &mut self.epochs,
                    &mut self.epoch,
                )
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
        let mut snapshots = Vec::new();
        let mut sources = Vec::new();
        let mut heap = BinaryHeap::new();
        let mut partitions = BTreeMap::<String, i32>::new();
        for path in paths {
            let mut file = Source::new(&path)?;
            let topic = format!(
                "raw.{}.{}.{}.v1",
                file.stream.exchange, file.stream.market, file.stream.feed
            );
            let partition = partitions.entry(topic).or_default();
            file.stream.partition = *partition;
            *partition += 1;
            snapshots.push(Snapshot {
                path,
                bytes: file.bytes,
            });
            let mut source = PartitionSource {
                files: VecDeque::from([file]),
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
    // Kafka partition numbers are transport coordinates, never asset identities.
    // Truly non-JSON bytes have no recoverable instrument; Engine handles the
    // resulting unknown-symbol control conservatively for the whole feed.
    let symbol = outer["symbol"].as_str().unwrap_or("__unknown__");
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
        let stream = match StreamPath::new(&path) {
            Ok(stream) => stream,
            Err(_) => {
                eprintln!("warning: skipping unknown v3 stream {}", path.display());
                continue;
            }
        };
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
            "no v3 input streams found (use --allow-open for .jsonl.open)",
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
