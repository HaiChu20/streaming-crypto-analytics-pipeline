//! Fixture ingestion throughput: gzip + v3 parsing + head merge + shared books/features.
//! Excludes full-day empty ticks and Parquet output; no services or external data.
use engine_replay::{
    Result,
    features::Engine,
    input::{Merge, discover},
};
use std::{path::PathBuf, time::Instant};
fn main() -> Result<()> {
    let root = PathBuf::from(std::env::args().nth(1).expect("fixture root"));
    for date in ["2026-09-15", "2026-10-08"] {
        let paths = discover(&root, date, false)?;
        for run in 0..4 {
            let started = Instant::now();
            let mut merge = Merge::new(paths.clone())?;
            let mut engine = Engine::default();
            while let Some(record) = merge.pop()? {
                engine.consume(&record)?;
            }
            let elapsed = started.elapsed().as_secs_f64();
            println!(
                "{}",
                serde_json::json!({"date":date,"run":run,"records":engine.records,"wall_s":elapsed,"records_per_s":engine.records as f64/elapsed,"books":engine.book_reports(),"anomalies":engine.data_anomalies})
            );
        }
    }
    Ok(())
}
