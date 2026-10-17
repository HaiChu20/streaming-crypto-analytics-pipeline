//! Live cutoffs and checkpoints, shared with as_live replay. No clocks or I/O.
use crate::{ENGINE_VERSION, Error, Record, Result, SECOND, features::Engine, row::Row};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

pub const TICK_DELAY_NS: i64 = 250_000_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Cutoff {
    pub tick_ns: i64,
    pub engine_version: String,
    pub processed: BTreeMap<String, i64>,
}

#[derive(Serialize, Deserialize)]
pub struct Checkpoint {
    pub engine_version: String,
    pub next_tick_ns: i64,
    pub last_cutoff: Cutoff,
    pub engine: Engine,
}
impl Checkpoint {
    pub fn new(next_tick_ns: i64, processed: BTreeMap<String, i64>) -> Self {
        Self {
            engine_version: ENGINE_VERSION.into(),
            next_tick_ns,
            last_cutoff: Cutoff {
                tick_ns: next_tick_ns - SECOND,
                engine_version: ENGINE_VERSION.into(),
                processed,
            },
            engine: Engine::default(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        if self.engine_version != ENGINE_VERSION
            || self.last_cutoff.engine_version != ENGINE_VERSION
        {
            return Err(Error("checkpoint engine_version mismatch".into()));
        }
        if self.next_tick_ns != self.last_cutoff.tick_ns + SECOND {
            return Err(Error(
                "checkpoint tick is not immediately after its last cutoff".into(),
            ));
        }
        Ok(())
    }
    pub fn apply(&mut self, cutoff: Cutoff, mut records: Vec<Record>) -> Result<Vec<Row>> {
        self.validate()?;
        if cutoff.tick_ns != self.next_tick_ns || cutoff.engine_version != ENGINE_VERSION {
            return Err(Error(
                "nonconsecutive cutoff or engine_version mismatch".into(),
            ));
        }
        for (key, prev) in &self.last_cutoff.processed {
            if cutoff.processed.get(key).is_none_or(|next| next < prev) {
                return Err(Error(format!("cutoff moved backwards or omitted {key}")));
            }
        }
        sort_records(&mut records);
        for record in &records {
            self.engine.consume(record)?;
        }
        let rows = self.engine.tick(cutoff.tick_ns)?;
        self.next_tick_ns += SECOND;
        self.last_cutoff = cutoff;
        Ok(rows)
    }
}

pub fn partition_key(r: &Record) -> Result<String> {
    Ok(format!(
        "{}/{}",
        r.topic
            .as_ref()
            .ok_or_else(|| Error("missing topic".into()))?,
        r.partition
            .ok_or_else(|| Error("missing partition".into()))?
    ))
}
pub fn sort_records(records: &mut [Record]) {
    records.sort_by(|a, b| {
        (a.recv_ns, &a.topic, a.partition, a.offset).cmp(&(
            b.recv_ns,
            &b.topic,
            b.partition,
            b.offset,
        ))
    });
}
pub fn is_carry_in(record: &Record, day_start_ns: i64) -> bool {
    record.recv_ns < i128::from(day_start_ns)
}

/// The exact archived line, including Kafka coordinates, travels with its record.
pub struct Queued {
    pub record: Record,
    pub line: String,
}
#[derive(Default)]
pub struct Queues(BTreeMap<String, VecDeque<Queued>>);
impl Queues {
    pub fn push(&mut self, queued: Queued) -> Result<()> {
        let key = partition_key(&queued.record)?;
        let offset = queued
            .record
            .offset
            .ok_or_else(|| Error("missing offset".into()))?;
        let queue = self.0.entry(key).or_default();
        if queue
            .back()
            .is_some_and(|p| p.record.offset.unwrap() >= offset)
        {
            return Err(Error(
                "Kafka partition queue is not in increasing offset order".into(),
            ));
        }
        queue.push_back(queued);
        Ok(())
    }
    /// First select each partition's offset prefix, stopping at a future head even
    /// if later offsets have earlier recv_ns. Only then globally sort the batch.
    pub fn take(&mut self, tick: i64, processed: &mut BTreeMap<String, i64>) -> Vec<Queued> {
        let mut batch = Vec::new();
        for (key, queue) in &mut self.0 {
            while queue
                .front()
                .is_some_and(|q| q.record.recv_ns <= i128::from(tick))
            {
                let queued = queue.pop_front().unwrap();
                processed.insert(key.clone(), queued.record.offset.unwrap());
                batch.push(queued);
            }
        }
        batch.sort_by(|a, b| {
            (
                a.record.recv_ns,
                &a.record.topic,
                a.record.partition,
                a.record.offset,
            )
                .cmp(&(
                    b.record.recv_ns,
                    &b.record.topic,
                    b.record.partition,
                    b.record.offset,
                ))
        });
        batch
    }
}
