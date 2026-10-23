use engine_replay::{
    Result, SECOND, error,
    features::Engine,
    input::{Merge, StreamPath, day_bounds, discover_filtered_with_dvol, marker_only_files},
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
    let mut exchange = None;
    let mut market = None;
    let mut exchange_clock_ms = None;
    let mut skip_marker_only_days = false;
    let mut include_dvol = false;
    let mut mode = "ideal".to_string();
    let mut from_state = None;
    let mut json_out = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--engine-version" => {
                println!("{}", engine_core::ENGINE_VERSION);
                return Ok(());
            }
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
            "--exchange" => {
                exchange = Some(
                    args.next()
                        .ok_or_else(|| error("--exchange requires name"))?,
                )
            }
            "--market" => {
                market = Some(args.next().ok_or_else(|| error("--market requires name"))?)
            }
            "--exchange-clock-ms" => {
                exchange_clock_ms = Some(
                    args.next()
                        .ok_or_else(|| {
                            error("--exchange-clock-ms requires non-negative integer milliseconds")
                        })?
                        .parse::<u64>()
                        .map_err(|_| {
                            error("--exchange-clock-ms requires non-negative integer milliseconds")
                        })?,
                );
            }
            "--include-dvol" => include_dvol = true,
            "--skip-marker-only-days" => skip_marker_only_days = true,
            "--allow-open" => allow_open = true,
            "--mode" => {
                mode = args
                    .next()
                    .ok_or_else(|| error("--mode requires ideal or as_live"))?
            }
            "--from-state" => {
                from_state = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| error("--from-state requires path"))?,
                ))
            }
            "--json-out" => {
                json_out = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| error("--json-out requires path"))?,
                ))
            }
            "--help" => {
                println!(
                    "engine-replay --root data --date YYYY-MM-DD [--out data/features] [--allow-open] [--exchange NAME] [--market NAME] [--exchange-clock-ms N] [--include-dvol] [--skip-marker-only-days]\nengine-replay --engine-version"
                );
                return Ok(());
            }
            _ => return Err(error(format!("unknown argument {arg}"))),
        }
    }
    if exchange_clock_ms.is_some()
        && (mode != "ideal"
            || exchange.as_deref() != Some("kraken")
            || market.as_deref() != Some("spot"))
    {
        return Err(error(
            "--exchange-clock-ms requires --mode ideal --exchange kraken --market spot",
        ));
    }
    if include_dvol && exchange.is_none() && market.is_none() {
        return Err(error(
            "--include-dvol requires an --exchange/--market filter",
        ));
    }
    let mut replay_mode =
        exchange_clock_ms.map_or_else(|| mode.clone(), |ms| format!("ideal+exchange_clock_{ms}ms"));
    if include_dvol {
        replay_mode.push_str("+dvol");
    }
    let date = date.ok_or_else(|| error("--date is required"))?;
    let (start, end) = day_bounds(&date)?;
    let out = out
        .unwrap_or_else(|| root.join("features"))
        .join(format!("date={date}"))
        .join("features.parquet");
    if mode == "as_live" {
        if exchange.is_some() || market.is_some() {
            return Err(error("--exchange/--market filters require ideal mode"));
        }
    } else if mode != "ideal" || from_state.is_some() || json_out.is_some() {
        return Err(error(
            "use --mode ideal or as_live; --from-state/--json-out require as_live",
        ));
    }
    if skip_marker_only_days {
        let mut paths = discover_filtered_with_dvol(
            &root,
            &date,
            allow_open,
            exchange.as_deref(),
            market.as_deref(),
            include_dvol,
        )?;
        if include_dvol {
            // Optional auxiliary streams must not decide whether the selected day is skipped.
            let mut selected = Vec::new();
            for path in paths {
                let stream = StreamPath::new(&path)?;
                if exchange.as_deref().is_none_or(|e| e == stream.exchange)
                    && market.as_deref().is_none_or(|m| m == stream.market)
                {
                    selected.push(path);
                }
            }
            paths = selected;
        }
        let files = marker_only_files(&paths, exchange_clock_ms)?;
        if !files.is_empty() {
            if out.exists() {
                return Err(error(
                    "refusing to skip a day with existing features; use a new output directory",
                ));
            }
            println!(
                "{}",
                serde_json::json!({
                    "status":"skipped", "reason":"marker_only_inputs", "files":files,
                    "date":date, "engine_version":engine_core::ENGINE_VERSION, "replay_mode":replay_mode,
                })
            );
            std::process::exit(3);
        }
    }
    if mode == "as_live" {
        use std::io::Write;
        let wall = std::time::Instant::now();
        let mut replay =
            engine_replay::as_live::Replay::open(&root, &date, from_state.as_deref(), allow_open)?;
        let initial_records = replay.state.engine.records;
        let mut writer = Writer::new_mode(&out, "as_live")?;
        let mut json = json_out
            .map(std::fs::File::create)
            .transpose()?
            .map(std::io::BufWriter::new);
        let cutoffs = std::mem::take(&mut replay.cutoffs);
        let ticks = cutoffs.len();
        for cutoff in cutoffs {
            for row in replay.tick(cutoff)? {
                if let Some(json) = json.as_mut() {
                    serde_json::to_writer(&mut *json, &row)?;
                    writeln!(json)?;
                }
                writer.push(row)?;
            }
        }
        if let Some(json) = json.as_mut() {
            json.flush()?;
        }
        let rows = writer.finish()?;
        println!(
            "{}",
            serde_json::json!({"mode":"as_live","output":out,"ticks":ticks,"rows":rows,"records":replay.state.engine.records-initial_records,"wall_s":wall.elapsed().as_secs_f64(),"book_breaks":replay.state.engine.book_breaks,"book_break_reasons":replay.state.engine.book_break_reasons,"data_anomalies":replay.state.engine.data_anomalies,"late_records":replay.late_records,"book_switches":replay.book_switches})
        );
        return Ok(());
    }
    let mut merge = Merge::with_exchange_clock(
        discover_filtered_with_dvol(
            &root,
            &date,
            allow_open,
            exchange.as_deref(),
            market.as_deref(),
            include_dvol,
        )?,
        exchange_clock_ms,
    )?;
    let mut writer = Writer::new_mode(&out, &replay_mode)?;
    let mut engine = Engine::default();
    let mut coverage = BTreeMap::<String, (u64, u64)>::new();
    let mut row_counts = BTreeMap::<String, u64>::new();
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
            engine.consume_at_tick(&r, tick)?;
        }
        for row in engine.tick(tick)? {
            *row_counts
                .entry(format!("{}/{}/{}", row.exchange, row.market, row.symbol))
                .or_default() += 1;
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
            &serde_json::json!({"mode":replay_mode,"engine_version":engine_core::ENGINE_VERSION,"output":out,"records":engine.records,"rows":rows,"wall_s":seconds,"records_per_s":engine.records as f64/seconds,"first_recv_ns":first_ns,"last_recv_ns":last_ns,"book_breaks":engine.book_breaks,"book_break_reasons":engine.book_break_reasons,"data_anomalies":engine.data_anomalies,"clock_steps":merge.clock_steps(),"foreign_before_day":foreign,"coverage":coverage,"max_abs_cross_bps":max_cross,"inputs":merge.snapshots,"books":engine.book_reports(),"row_counts":row_counts})
        )?
    );
    Ok(())
}
