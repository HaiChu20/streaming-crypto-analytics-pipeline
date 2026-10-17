use crate::{Result, SECOND, error};
use engine_core::Record;
use flate2::read::MultiGzDecoder;
use serde::Serialize;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap},
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub path: PathBuf,
    pub bytes: u64,
}
struct Source {
    reader: Box<dyn BufRead>,
    path: PathBuf,
    open: bool,
    line: u64,
    previous_ns: Option<i128>,
    clock_steps: u64,
}
impl Source {
    fn next(&mut self) -> Result<Option<Record>> {
        let mut text = String::new();
        if self.reader.read_line(&mut text)? == 0 {
            return Ok(None);
        }
        self.line += 1;
        if self.open && !text.ends_with('\n') {
            return Ok(None);
        }
        let mut record = Record::parse(&text)
            .map_err(|e| error(format!("{}:{}: {e}", self.path.display(), self.line)))?;
        if self.previous_ns.is_some_and(|t| record.recv_ns < t) {
            self.clock_steps += 1;
        }
        self.previous_ns = Some(record.recv_ns);
        record.topic.get_or_insert_with(|| {
            format!(
                "raw.{}.{}.{}.v1",
                record.exchange, record.market, record.feed
            )
        });
        record
            .partition
            .get_or_insert(if asset(&record.symbol) == Some("ETH") {
                1
            } else {
                0
            });
        record.offset.get_or_insert(self.line as i64 - 1);
        Ok(Some(record))
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

/// Only each file's current head participates. A clock step-back is exposed after
/// its predecessor is popped; it is never moved ahead of that predecessor.
pub struct Merge {
    sources: Vec<Source>,
    heap: BinaryHeap<Reverse<Head>>,
    pub snapshots: Vec<Snapshot>,
}
impl Merge {
    pub fn new(mut paths: Vec<PathBuf>) -> Result<Self> {
        paths.sort();
        let mut sources = Vec::new();
        let mut snapshots = Vec::new();
        let mut heap = BinaryHeap::new();
        for path in paths {
            let file = File::open(&path)?;
            let bytes = file.metadata()?.len();
            let open = path.to_string_lossy().ends_with(".open");
            let capped = file.take(bytes); // Do not chase a concurrently growing open day.
            let reader: Box<dyn BufRead> = if path.to_string_lossy().ends_with(".gz") {
                Box::new(BufReader::new(MultiGzDecoder::new(capped)))
            } else {
                Box::new(BufReader::new(capped))
            };
            let mut source = Source {
                reader,
                path: path.clone(),
                open,
                line: 0,
                previous_ns: None,
                clock_steps: 0,
            };
            if let Some(record) = source.next()? {
                heap.push(Reverse(Head {
                    record,
                    source: sources.len(),
                }));
            }
            snapshots.push(Snapshot { path, bytes });
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

pub fn discover(root: &Path, date: &str, allow_open: bool) -> Result<Vec<PathBuf>> {
    let base = root.join("raw").join(format!("date={date}"));
    let mut files = Vec::new();
    fn walk(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                walk(&entry.path(), files)?;
            } else if entry.file_type()?.is_file() {
                files.push(entry.path());
            }
        }
        Ok(())
    }
    walk(&base, &mut files)?;
    let mut chosen = BTreeMap::<PathBuf, (u8, PathBuf)>::new();
    for path in files {
        let name = path.file_name().unwrap().to_string_lossy();
        let (stem, priority) = if let Some(s) = name.strip_suffix(".jsonl") {
            (s, 0)
        } else if let Some(s) = name.strip_suffix(".jsonl.gz") {
            (s, 1)
        } else if allow_open && let Some(s) = name.strip_suffix(".jsonl.open") {
            (s, 2)
        } else {
            continue;
        };
        let feed = path
            .parent()
            .and_then(|p| p.file_name())
            .unwrap()
            .to_string_lossy();
        if !matches!(
            feed.as_ref(),
            "feed=l1"
                | "feed=l2"
                | "feed=l3"
                | "feed=trades"
                | "feed=trades_agg"
                | "feed=mark"
                | "feed=liquidations"
                | "feed=ticker"
        ) {
            eprintln!("warning: skipping unknown feed {}", path.display());
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
            "no input streams found (use --allow-open for .jsonl.open)",
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
