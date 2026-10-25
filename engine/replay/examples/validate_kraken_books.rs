//! Read-only per-book checksum/coverage and simultaneous L3/L2 audit.
use engine_core::{Kind, Outcome, SECOND, Side, StreamBook};
use engine_replay::{
    Result,
    input::{Merge, day_bounds, discover_filtered},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};

#[derive(Default, Serialize)]
struct Audit {
    records: u64,
    crc_pass: u64,
    crc_fail: u64,
    valid_ticks: u64,
    observed_ticks: u64,
    observed_valid_ticks: u64,
    invalid_intervals: Vec<(i64, i64)>,
    connection_errors: Vec<Value>,
    breaks: Vec<Value>,
    all_l3_events: [u64; 3],
    top10_l3_events: [i64; 3],
    #[serde(skip)]
    book: Option<StreamBook>,
    #[serde(skip)]
    last_recv: i128,
    #[serde(skip)]
    first_recv: Option<i128>,
    #[serde(skip)]
    exchange_timestamp: Value,
}
fn interval(ranges: &mut Vec<(i64, i64)>, tick: i64) {
    if let Some((_, end)) = ranges.last_mut()
        && *end + SECOND == tick
    {
        *end = tick;
    } else {
        ranges.push((tick, tick));
    }
}
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let (start, end) = day_bounds(&args[2])?;
    let mut merge = Merge::new(discover_filtered(
        Path::new(&args[1]),
        &args[2],
        true,
        Some("kraken"),
        Some("spot"),
    )?)?;
    let mut audit = BTreeMap::<String, Audit>::new();
    let mut both_valid = 0u64;
    let mut bbo_agree = 0u64;
    let mut bbo_size_agree = 0u64;
    let mut depth_agree = 0u64;
    let mut levels_agree = 0u64;
    let mut samples = Vec::new();
    for tick in (start..end).step_by(SECOND as usize) {
        while merge.peek_ns().is_some_and(|ns| ns <= i128::from(tick)) {
            let r = merge.pop()?.unwrap();
            if !matches!(r.feed.as_str(), "l2" | "l3") {
                continue;
            }
            let a = audit.entry(r.feed.clone()).or_default();
            a.records += 1;
            a.first_recv.get_or_insert(r.recv_ns);
            a.last_recv = r.recv_ns;
            if r.event.as_deref() == Some("drop") {
                a.connection_errors.push(
                    json!({"tick_ns":tick,"recv_ns":r.recv_ns,"conn":r.conn,"reason":r.reason}),
                );
            }
            if r.kind != Kind::Control {
                let payload: Value = serde_json::from_str(&r.payload)?;
                a.exchange_timestamp = payload["data"][0]["timestamp"].clone();
                if r.feed == "l3" && r.kind != Kind::Snapshot {
                    for side in ["bids", "asks"] {
                        for order in payload["data"][0][side].as_array().into_iter().flatten() {
                            match order["event"].as_str() {
                                Some("add") => a.all_l3_events[0] += 1,
                                Some("modify") => a.all_l3_events[1] += 1,
                                Some("delete") => a.all_l3_events[2] += 1,
                                _ => {}
                            }
                        }
                    }
                }
            }
            if a.book.is_none() {
                a.book = Some(StreamBook::new(&r)?);
            }
            let book = a.book.as_mut().unwrap();
            let before = book.counters().checksums_verified;
            let outcome = book.consume(&r)?;
            a.crc_pass += book.counters().checksums_verified - before;
            if let Outcome::Break(reason) = &outcome {
                a.crc_fail += u64::from(matches!(
                    reason,
                    engine_core::BreakReason::ChecksumMismatch { .. }
                ));
                a.breaks
                    .push(json!({"tick_ns":tick,"offset":r.offset,"conn":r.conn,"reason":reason}));
            }
            if r.feed == "l3" && outcome == Outcome::Applied {
                for (total, count) in a
                    .top10_l3_events
                    .iter_mut()
                    .zip(book.l3_event_counts().unwrap())
                {
                    *total += count;
                }
            }
        }
        let valid = |a: &Audit| {
            a.book.as_ref().is_some_and(StreamBook::valid)
                && i128::from(tick) - a.last_recv <= 60 * i128::from(SECOND)
        };
        for a in audit.values_mut() {
            let is_valid = valid(a);
            a.valid_ticks += u64::from(is_valid);
            if !is_valid {
                interval(&mut a.invalid_intervals, tick);
            }
        }
        let (Some(l2), Some(l3)) = (audit.get("l2"), audit.get("l3")) else {
            continue;
        };
        if !valid(l2) || !valid(l3) {
            continue;
        }
        let b2 = l2.book.as_ref().unwrap().book().unwrap();
        let b3 = l3.book.as_ref().unwrap().book().unwrap();
        both_valid += 1;
        let same_bbo = b2.best_bid().map(|b| b.0) == b3.best_bid().map(|b| b.0)
            && b2.best_ask().map(|b| b.0) == b3.best_ask().map(|b| b.0);
        let same_bbo_size = b2.best_bid() == b3.best_bid() && b2.best_ask() == b3.best_ask();
        let mut same_depth = true;
        let mut same_levels = true;
        for side in [Side::Bid, Side::Ask] {
            let top2 = b2.top(side, 10);
            let top3 = b3.top(side, 10);
            same_depth &= top2.iter().map(|l| i128::from(l.1.0)).sum::<i128>()
                == top3.iter().map(|l| i128::from(l.1.0)).sum::<i128>();
            same_levels &= top2 == top3;
        }
        bbo_agree += u64::from(same_bbo);
        bbo_size_agree += u64::from(same_bbo_size);
        depth_agree += u64::from(same_depth);
        levels_agree += u64::from(same_levels);
        if (!same_bbo || !same_depth) && samples.len() < 20 {
            let top = |b: &engine_core::Book| json!({"bid":b.best_bid().map(|(p,q)| (p.0,q.0)),"ask":b.best_ask().map(|(p,q)| (p.0,q.0)),"bid_depth10":b.top(Side::Bid,10).iter().map(|l| i128::from(l.1.0)).sum::<i128>(),"ask_depth10":b.top(Side::Ask,10).iter().map(|l| i128::from(l.1.0)).sum::<i128>()});
            samples.push(json!({"tick_ns":tick,"l2":top(b2),"l3":top(b3),"l2_recv_ns":l2.last_recv,"l3_recv_ns":l3.last_recv,"l2_exchange_timestamp":l2.exchange_timestamp,"l3_exchange_timestamp":l3.exchange_timestamp}));
        }
    }
    for a in audit.values_mut() {
        let first = a.first_recv.unwrap();
        let first_tick =
            ((first + i128::from(SECOND) - 1) / i128::from(SECOND) * i128::from(SECOND)) as i64;
        let last_tick = (a.last_recv / i128::from(SECOND) * i128::from(SECOND)) as i64;
        a.observed_ticks = ((last_tick - first_tick) / SECOND + 1).max(0) as u64;
        let invalid = a
            .invalid_intervals
            .iter()
            .map(|(start, end)| {
                let start = (*start).max(first_tick);
                let end = (*end).min(last_tick);
                ((end - start) / SECOND + 1).max(0) as u64
            })
            .sum::<u64>();
        a.observed_valid_ticks = a.observed_ticks - invalid;
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"books":audit,"both_valid_ticks":both_valid,"bbo_agree":bbo_agree,"bbo_size_agree":bbo_size_agree,"top10_depth_agree":depth_agree,"top10_levels_agree":levels_agree,"disagreement_samples":samples,"clock_steps":merge.clock_steps()})
        )?
    );
    Ok(())
}
