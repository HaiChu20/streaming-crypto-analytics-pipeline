//! Replay recorded offset prefixes; recv_ns never decides membership in a tick.
use crate::{
    Result, error,
    input::{Source, discover},
    state_io,
};
use engine_core::{
    Record,
    session::{Checkpoint, Cutoff, partition_key},
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

struct Archive {
    reader: Source,
    head: Option<Record>,
    key: String,
}
impl Archive {
    fn new(path: &Path) -> Result<Self> {
        let mut source = Self {
            reader: Source::new(path)?,
            head: None,
            key: String::new(),
        };
        source.advance()?;
        if let Some(head) = &source.head {
            source.key = partition_key(head)?;
        }
        Ok(source)
    }
    fn advance(&mut self) -> Result<()> {
        self.head = self.reader.next()?;
        Ok(())
    }
}

pub struct Replay {
    pub state: Checkpoint,
    pub cutoffs: Vec<Cutoff>,
    pub late_records: u64,
    pub book_switches: Vec<BookSwitch>,
    sources: BTreeMap<String, Archive>,
    carry: BTreeMap<String, VecDeque<Record>>,
}
#[derive(Serialize)]
pub struct BookSwitch {
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub tick_ns: i64,
    pub was_valid: bool,
    pub is_valid: bool,
}
impl Replay {
    pub fn open(
        root: &Path,
        date: &str,
        from_state: Option<&Path>,
        allow_open: bool,
    ) -> Result<Self> {
        let directory = root.join("engine").join(format!("date={date}"));
        let state = state_io::load(from_state.unwrap_or(&directory.join("start_state.json")))?;
        let reader = BufReader::new(File::open(directory.join("cutoffs.jsonl"))?);
        let mut cutoffs = Vec::new();
        for line in reader.split(b'\n') {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let cutoff: Cutoff = serde_json::from_slice(&line)?;
            if cutoff.tick_ns >= state.next_tick_ns {
                cutoffs.push(cutoff);
            }
        }
        let mut sources = BTreeMap::new();
        for path in discover(root, date, allow_open)? {
            let mut source = Archive::new(&path)?;
            if source.head.is_none() {
                continue;
            }
            let key = source.key.clone();
            let Some(prev) = state.last_cutoff.processed.get(&key) else {
                continue;
            };
            while source
                .head
                .as_ref()
                .is_some_and(|r| r.offset.unwrap() <= *prev)
            {
                source.advance()?;
            }
            if sources.insert(key.clone(), source).is_some() {
                return Err(error(format!("multiple archives for {key}")));
            }
        }
        let mut carry: BTreeMap<String, Vec<Record>> = BTreeMap::new();
        let path = directory.join("carry_in.jsonl");
        if path.exists() {
            for line in BufReader::new(File::open(path)?).lines() {
                let record = crate::input::recover_archived(&line?, None)?;
                let key = partition_key(&record)?;
                if record.offset.is_none() {
                    return Err(error("carry-in missing offset"));
                }
                if state
                    .last_cutoff
                    .processed
                    .get(&key)
                    .is_some_and(|p| record.offset.unwrap() > *p)
                {
                    carry.entry(key).or_default().push(record);
                }
            }
        }
        let carry = carry
            .into_iter()
            .map(|(key, mut records)| {
                records.sort_by_key(|r| r.offset);
                records.dedup_by_key(|r| r.offset);
                (key, records.into())
            })
            .collect();
        Ok(Self {
            state,
            cutoffs,
            late_records: 0,
            book_switches: Vec::new(),
            sources,
            carry,
        })
    }
    pub fn tick(&mut self, cutoff: Cutoff) -> Result<Vec<engine_core::row::Row>> {
        let mut records = Vec::new();
        for (key, target) in &cutoff.processed {
            let previous = *self
                .state
                .last_cutoff
                .processed
                .get(key)
                .ok_or_else(|| error(format!("new partition {key} absent from checkpoint")))?;
            if *target < previous {
                return Err(error("cutoff moved backwards"));
            }
            let mut expected = previous + 1;
            while expected <= *target {
                let carry_offset = self
                    .carry
                    .get(key)
                    .and_then(|q| q.front())
                    .and_then(|r| r.offset);
                let file_offset = self
                    .sources
                    .get(key)
                    .and_then(|s| s.head.as_ref())
                    .and_then(|r| r.offset);
                let from_carry = carry_offset.is_some_and(|a| file_offset.is_none_or(|b| a <= b));
                let record = if from_carry {
                    self.carry.get_mut(key).unwrap().pop_front().unwrap()
                } else if let Some(source) = self.sources.get_mut(key).filter(|s| s.head.is_some())
                {
                    let record = source.head.as_ref().unwrap().clone();
                    source.advance()?;
                    record
                } else {
                    return Err(error(format!(
                        "archive not caught up: missing {key} offset {expected}"
                    )));
                };
                let offset = record.offset.unwrap();
                if offset < expected {
                    continue;
                } // carry and current-day archive may overlap after a clock step.
                if offset != expected {
                    return Err(error(format!(
                        "archive gap: {key} expected {expected}, got {offset}"
                    )));
                }
                if record.recv_ns <= i128::from(cutoff.tick_ns - engine_core::SECOND) {
                    self.late_records += 1;
                }
                records.push(record);
                expected += 1;
            }
        }
        let previous = self.state.engine.book_sources();
        let tick_ns = cutoff.tick_ns;
        let rows = self.state.apply(cutoff, records)?;
        for ((exchange, market, symbol), (conn, is_valid)) in self.state.engine.book_sources() {
            if let Some((old, was_valid)) =
                previous.get(&(exchange.clone(), market.clone(), symbol.clone()))
                && old != &conn
            {
                self.book_switches.push(BookSwitch {
                    exchange,
                    market,
                    symbol,
                    tick_ns,
                    was_valid: *was_valid,
                    is_valid,
                });
            }
        }
        Ok(rows)
    }
}
