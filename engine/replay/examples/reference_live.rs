//! Offline live-queue reference from recorded Kafka v1 envelopes, for P10 parity.
//! Production replay readers accept only v2; this independent audit checks conversion.
use engine_core::{
    Record,
    session::{Cutoff, Queued, Queues, partition_key},
};
use engine_replay::{Result, error, state_io};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};
struct Source {
    reader: BufReader<File>,
    head: Option<Record>,
}
impl Source {
    fn next(&mut self) -> Result<()> {
        let mut text = String::new();
        self.head = if self.reader.read_line(&mut text)? == 0 || !text.ends_with('\n') {
            None
        } else {
            Some(Record::parse(&text)?)
        };
        Ok(())
    }
}
fn walk(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            walk(&entry.path(), files)?;
        } else if entry.file_name().to_string_lossy().ends_with(".jsonl.open")
            || entry.file_name().to_string_lossy().ends_with(".jsonl")
        {
            files.push(entry.path());
        }
    }
    Ok(())
}
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let mut state = state_io::load(Path::new(&args[2]))?;
    let mut files = Vec::new();
    walk(Path::new(&args[1]), &mut files)?;
    let mut sources = BTreeMap::new();
    for file in files {
        let mut source = Source {
            reader: BufReader::new(File::open(file)?),
            head: None,
        };
        source.next()?;
        let Some(head) = &source.head else {
            continue;
        };
        let key = partition_key(head)?;
        let Some(prev) = state.last_cutoff.processed.get(&key) else {
            continue;
        };
        while source
            .head
            .as_ref()
            .is_some_and(|r| r.offset.unwrap() <= *prev)
        {
            source.next()?;
        }
        sources.insert(key, source);
    }
    let mut output = std::io::BufWriter::new(File::create(&args[4])?);
    let mut queues = Queues::default();
    let mut rows = 0u64;
    for line in BufReader::new(File::open(&args[3])?).lines() {
        let cutoff: Cutoff = serde_json::from_str(&line?)?;
        if cutoff.tick_ns < state.next_tick_ns {
            continue;
        }
        for (key, target) in &cutoff.processed {
            let Some(source) = sources.get_mut(key) else {
                continue;
            };
            while source
                .head
                .as_ref()
                .is_some_and(|r| r.offset.unwrap() <= *target)
            {
                queues.push(Queued {
                    record: source.head.take().unwrap(),
                    line: String::new(),
                })?;
                source.next()?;
            }
        }
        let mut processed = state.last_cutoff.processed.clone();
        let records = queues
            .take(cutoff.tick_ns, &mut processed)
            .into_iter()
            .map(|q| q.record)
            .collect();
        if processed != cutoff.processed {
            return Err(error(
                "reference live queue did not reproduce recorded cutoff",
            ));
        }
        for row in state.apply(cutoff, records)? {
            serde_json::to_writer(&mut output, &row)?;
            writeln!(output)?;
            rows += 1;
        }
    }
    output.flush()?;
    println!(
        "{}",
        serde_json::json!({"rows":rows,"records":state.engine.records,"book_breaks":state.engine.book_breaks,"book_break_reasons":state.engine.book_break_reasons,"data_anomalies":state.engine.data_anomalies})
    );
    Ok(())
}
