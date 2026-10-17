use engine_replay::{
    Result, SECOND, error,
    features::Engine,
    input::{Merge, day_bounds, discover},
    output::Writer,
};
use std::{collections::BTreeMap, path::PathBuf};
fn main() {
    if let Err(e) = run() {
        eprintln!("engine-replay: {e}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let mut root = PathBuf::from("data");
    let mut date = None;
    let mut out = None;
    let mut allow_open = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => {
                root = PathBuf::from(args.next().ok_or_else(|| error("--root requires a path"))?)
            }
            "--date" => {
                date = Some(
                    args.next()
                        .ok_or_else(|| error("--date requires YYYY-MM-DD"))?,
                )
            }
            "--out" => {
                out = Some(PathBuf::from(
                    args.next().ok_or_else(|| error("--out requires a path"))?,
                ))
            }
            "--allow-open" => allow_open = true,
            "--help" => {
                println!(
                    "engine-replay --root data --date YYYY-MM-DD [--out data/features] [--allow-open]"
                );
                return Ok(());
            }
            _ => return Err(error(format!("unknown argument {arg}"))),
        }
    }
    let date = date.ok_or_else(|| error("--date is required"))?;
    let (start, end) = day_bounds(&date)?;
    let out = out
        .unwrap_or_else(|| root.join("features"))
        .join(format!("date={date}"))
        .join("features.parquet");
    let mut merge = Merge::new(discover(&root, &date, allow_open)?)?;
    let mut writer = Writer::new(&out)?;
    let mut engine = Engine::default();
    let mut coverage = BTreeMap::<String, (u64, u64)>::new();
    let mut max_cross = BTreeMap::<String, f64>::new();
    let mut first_ns = None;
    let mut last_ns = None;
    let mut foreign = 0u64;
    let wall = std::time::Instant::now();
    for tick in (start..end).step_by(SECOND as usize) {
        while merge.peek_ns().is_some_and(|ns| ns <= i128::from(tick)) {
            let r = merge.pop()?.unwrap();
            if r.recv_ns < i128::from(start) {
                foreign += 1;
                continue;
            }
            first_ns = Some(first_ns.map_or(r.recv_ns, |n: i128| n.min(r.recv_ns)));
            last_ns = Some(last_ns.map_or(r.recv_ns, |n: i128| n.max(r.recv_ns)));
            engine.consume(&r)?;
        }
        for row in engine.tick(tick)? {
            if row.exchange != "cross" {
                let count = coverage
                    .entry(format!("{}/{}/{}", row.exchange, row.market, row.symbol))
                    .or_default();
                count.0 += 1;
                count.1 += u64::from(row.book_valid);
            } else {
                for (name, value) in [
                    ("basis_bps_binance", row.basis_bps_binance),
                    ("basis_bps_okx", row.basis_bps_okx),
                    ("basis_bps_kraken", row.basis_bps_kraken),
                    ("spot_dev_bps_okx", row.spot_dev_bps_okx),
                    ("spot_dev_bps_kraken", row.spot_dev_bps_kraken),
                ] {
                    if let Some(value) = value {
                        let v = max_cross
                            .entry(format!("{}/{}", row.symbol, name))
                            .or_default();
                        *v = v.max(value.abs());
                    }
                }
            }
            writer.push(row)?;
        }
    }
    let rows = writer.finish()?;
    let seconds = wall.elapsed().as_secs_f64();
    let coverage=coverage.into_iter().map(|(k,(ticks,valid))|(k,serde_json::json!({"ticks":ticks,"valid_ticks":valid,"valid_pct":100.0*valid as f64/ticks as f64}))).collect::<BTreeMap<_,_>>();
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"output":out,"records":engine.records,"rows":rows,"wall_s":seconds,"records_per_s":engine.records as f64/seconds,"first_recv_ns":first_ns,"last_recv_ns":last_ns,"book_breaks":engine.book_breaks,"book_break_reasons":engine.book_break_reasons,"clock_steps":merge.clock_steps(),"foreign_before_day":foreign,"coverage":coverage,"max_abs_cross_bps":max_cross,"inputs":merge.snapshots})
        )?
    );
    Ok(())
}
