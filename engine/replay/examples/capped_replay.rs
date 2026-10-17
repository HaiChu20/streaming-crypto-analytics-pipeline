//! Reproduce a prior v1 replay's exact input caps using the converted v2 tree.
use engine_core::Record;
use engine_replay::{
    Result, SECOND, error,
    features::Engine,
    input::{Merge, day_bounds, discover},
    output::Writer,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::Path,
};
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let report: Value = serde_json::from_reader(File::open(&args[3])?)?;
    let mut caps = BTreeMap::new();
    for input in report["inputs"]
        .as_array()
        .ok_or_else(|| error("missing prior input caps"))?
    {
        let bytes = input["bytes"].as_u64().unwrap();
        let mut file = File::open(input["path"].as_str().unwrap())?;
        let start = bytes.saturating_sub(1_048_576);
        file.seek(SeekFrom::Start(start))?;
        let mut reader = BufReader::new(file.take(bytes - start));
        let mut text = String::new();
        if start > 0 {
            reader.read_line(&mut text)?;
        }
        let mut last = None;
        loop {
            text.clear();
            if reader.read_line(&mut text)? == 0 {
                break;
            }
            if text.ends_with('\n') {
                last = Some(Record::parse(&text)?);
            }
        }
        let r = last.ok_or_else(|| error("no complete line within cap suffix"))?;
        caps.insert((r.topic.unwrap(), r.partition.unwrap()), r.offset.unwrap());
    }
    let (start, end) = day_bounds(&args[2])?;
    let mut merge = Merge::new(discover(Path::new(&args[1]), &args[2], true)?)?;
    let mut remaining = caps.clone();
    let mut engine = Engine::default();
    let mut writer = Writer::new(Path::new(&args[4]))?;
    for tick in (start..end).step_by(SECOND as usize) {
        while !remaining.is_empty() && merge.peek_ns().is_some_and(|ns| ns <= i128::from(tick)) {
            let r = merge.pop()?.unwrap();
            let key = (r.topic.clone().unwrap(), r.partition.unwrap());
            if caps.get(&key) == r.offset.as_ref() {
                remaining.remove(&key);
            }
            if r.recv_ns >= i128::from(start)
                && caps.get(&key).is_some_and(|cap| r.offset.unwrap() <= *cap)
            {
                engine.consume_at_tick(&r, tick)?;
            }
        }
        for row in engine.tick(tick)? {
            writer.push(row)?;
        }
    }
    println!(
        "{}",
        serde_json::json!({"records":engine.records,"rows":writer.finish()?,"book_breaks":engine.book_breaks,"book_break_reasons":engine.book_break_reasons,"data_anomalies":engine.data_anomalies,"offset_caps":caps.into_iter().map(|((t,p),o)| (format!("{t}/{p}"),o)).collect::<BTreeMap<_,_>>()})
    );
    Ok(())
}
