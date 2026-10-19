//! Read-only Parquet audit; sample five valid observed Kraken BTC ticks using reservoir sampling.
use engine_replay::{Result, SECOND, features::STALE_S};
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs::File};
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let report: Value = serde_json::from_reader(File::open(&args[1])?)?;
    let end = report["last_recv_ns"].as_i64().unwrap();
    let reader = SerializedFileReader::new(File::open(report["output"].as_str().unwrap())?)?;
    let mut samples = Vec::new();
    let mut seen = 0u64;
    let mut random = 20261017u64;
    let mut previous = None;
    let age_column = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.name() == "book_age_ms")
        .unwrap();
    let mut counts = BTreeMap::<String, (u64, u64, u64)>::new();
    let mut stale_ranges = BTreeMap::<String, Vec<(i64, i64)>>::new();
    let mut tail = BTreeMap::<String, (u64, u64, u64)>::new();
    let mut total = 0u64;
    for row in reader.get_row_iter(None)? {
        let row = row?;
        let exchange = row.get_string(0)?;
        let market = row.get_string(1)?;
        let symbol = row.get_string(2)?;
        let tick = row.get_long(3)?;
        let key = (tick, exchange.clone(), market.clone(), symbol.clone());
        assert!(previous.as_ref().is_none_or(|p| p < &key));
        previous = Some(key);
        total += 1;
        if exchange != "cross" && tick <= end {
            let count = counts
                .entry(format!("{exchange}/{market}/{symbol}"))
                .or_default();
            count.0 += 1;
            count.1 += u64::from(row.get_bool(6)?);
            count.2 += u64::from(
                row.get_double(age_column)
                    .is_ok_and(|age| age > STALE_S as f64 * 1000.0),
            );
            if row
                .get_double(age_column)
                .is_ok_and(|age| age > STALE_S as f64 * 1000.0)
            {
                let ranges = stale_ranges
                    .entry(format!("{exchange}/{market}/{symbol}"))
                    .or_default();
                if let Some((_, last)) = ranges.last_mut()
                    && tick == *last + SECOND
                {
                    *last = tick;
                } else {
                    ranges.push((tick, tick));
                }
            }
        }
        if tick > end + STALE_S * SECOND {
            if exchange != "cross" {
                let count = tail
                    .entry(format!("{exchange}/{market}/{symbol}"))
                    .or_default();
                count.0 += 1;
                count.1 += u64::from(row.get_bool(6)?);
                count.2 += u64::from(
                    row.get_double(age_column)
                        .is_ok_and(|age| age > STALE_S as f64 * 1000.0),
                );
                assert!(!row.get_bool(6)?);
            }
            for (name, value) in row.get_column_iter() {
                if matches!(
                    name.as_str(),
                    "best_bid"
                        | "best_ask"
                        | "mid"
                        | "mid_e9"
                        | "microprice"
                        | "spread_bps"
                        | "bid_depth10"
                        | "ask_depth10"
                        | "imbalance10"
                        | "ret_1s"
                        | "ret_10s"
                        | "ret_60s"
                        | "rv_60s"
                        | "rv_300s"
                        | "funding_rate"
                        | "mark_price"
                        | "open_interest"
                        | "basis_bps_binance"
                        | "basis_bps_okx"
                        | "basis_bps_kraken"
                        | "spot_dev_bps_okx"
                        | "spot_dev_bps_kraken"
                ) || name.starts_with("ret_")
                    || name.starts_with("range_bps_")
                    || name == "microprice_dev_bps"
                {
                    // Historical OFI/volatility may remain populated until their
                    // window expires. Endpoint-based fields require a current mid.
                    assert!(
                        matches!(value, parquet::record::Field::Null),
                        "non-null frozen tail field {name}"
                    );
                }
            }
        }
        if exchange == "kraken"
            && market == "spot"
            && symbol == "BTC/USD"
            && tick <= end
            && row.get_bool(6)?
        {
            let Ok(mid) = row.get_long(8) else {
                continue;
            };
            seen += 1;
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let entry = json!({"tick_ns":tick,"mid_e9":mid});
            if samples.len() < 5 {
                samples.push(entry);
            } else {
                let index = random % seen;
                if index < 5 {
                    samples[index as usize] = entry;
                }
            }
        }
    }
    samples.sort_by_key(|s| s["tick_ns"].as_i64().unwrap());
    let observed=counts.into_iter().map(|(key,(ticks,valid,stale))|(key,json!({"ticks":ticks,"valid_ticks":valid,"valid_pct":100.0*valid as f64/ticks as f64,"stale_ticks":stale,"stale_pct":100.0*stale as f64/ticks as f64}))).collect::<BTreeMap<_,_>>();
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"rows":total,"observed_coverage":observed,"observed_stale_ranges":stale_ranges,"tail_after_60s":tail.into_iter().map(|(key,(ticks,valid,stale))|(key,json!({"ticks":ticks,"valid_ticks":valid,"stale_ticks":stale}))).collect::<BTreeMap<_,_>>(),"kraken_btc_samples":samples,"metadata":format!("{:?}",reader.metadata().file_metadata().key_value_metadata())})
        )?
    );
    Ok(())
}
