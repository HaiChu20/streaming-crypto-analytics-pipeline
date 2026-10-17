use engine_core::{Kind, Record};
use engine_replay::{
    ENGINE_VERSION, SECOND,
    features::Engine,
    input::{Merge, day_bounds, discover},
    output::Writer,
    row::Row,
};
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::{Field, RowAccessor},
};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
fn row_json(row: parquet::record::Row) -> Value {
    Value::Object(
        row.get_column_iter()
            .map(|(name, field)| {
                (
                    name.clone(),
                    match field {
                        Field::Null => Value::Null,
                        Field::Str(s) => json!(s),
                        Field::Long(n) => json!(n),
                        Field::Double(n) => json!(n),
                        Field::Bool(b) => json!(b),
                        _ => panic!("unexpected column type"),
                    },
                )
            })
            .collect(),
    )
}
static SERIAL: AtomicU64 = AtomicU64::new(0);
fn scratch() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
        "../target/replay-tests/{}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&p).unwrap();
    p
}
fn envelope(ex: &str, market: &str, feed: &str, kind: &str, time: i64, payload: Value) -> Value {
    json!({"exchange":ex,"market":market,"feed":feed,"symbol":if ex=="kraken"{"BTC/USD"}else{"BTCUSDT"},"kind":kind,"recv_ns":time,"conn":"A","seq":1,"topic":format!("raw.{ex}.{market}.{feed}.v1"),"partition":0,"offset":0,"payload":payload.to_string()})
}
fn rec(v: Value) -> Record {
    Record::parse(&v.to_string()).unwrap()
}
fn snap(time: i64, bid: &str, ask: &str) -> Value {
    envelope(
        "binance",
        "spot",
        "l2",
        "snapshot",
        time,
        json!({"lastUpdateId":1,"bids":[[bid,"2"]],"asks":[[ask,"1"]]}),
    )
}
fn trade(time: i64, id: i64, m: bool) -> Value {
    envelope(
        "binance",
        "spot",
        "trades",
        "trade",
        time,
        json!({"data":{"t":id,"q":"0.5","m":m}}),
    )
}
fn control(feed: &str, event: &str, time: i64, conn: &str) -> Value {
    let mut v = envelope("binance", "spot", feed, "control", time, Value::Null);
    v["event"] = json!(event);
    v["conn"] = json!(conn);
    v
}
fn row(engine: &mut Engine, tick: i64) -> Row {
    engine
        .tick(tick)
        .unwrap()
        .into_iter()
        .find(|r| r.exchange != "cross")
        .unwrap()
}
#[test]
fn merge_ties_topic_partition_offset_and_file_clock_steps() {
    let dir = scratch();
    let mut files = Vec::new();
    for (i, topic, partition, offset, time) in [
        (0, "b", 0, 0, 10),
        (1, "a", 1, 0, 10),
        (2, "a", 0, 2, 10),
        (3, "a", 0, 1, 10),
    ] {
        let mut v = trade(time, i, false);
        v["topic"] = json!(topic);
        v["partition"] = json!(partition);
        v["offset"] = json!(offset);
        let p = dir.join(format!("{i}.jsonl"));
        fs::write(&p, format!("{v}\n")).unwrap();
        files.push(p);
    }
    let mut merge = Merge::new(files).unwrap();
    let mut ids = Vec::new();
    while let Some(r) = merge.pop().unwrap() {
        ids.push(
            serde_json::from_str::<Value>(&r.payload).unwrap()["data"]["t"]
                .as_i64()
                .unwrap(),
        );
    }
    assert_eq!(ids, vec![3, 2, 1, 0]);
    let a = dir.join("step.jsonl");
    let b = dir.join("other.jsonl");
    fs::write(
        &a,
        format!("{}\n{}\n", trade(20, 1, false), trade(5, 2, false)),
    )
    .unwrap();
    fs::write(&b, format!("{}\n", trade(10, 3, false))).unwrap();
    let mut merge = Merge::new(vec![a, b]).unwrap();
    let mut times = Vec::new();
    while let Some(r) = merge.pop().unwrap() {
        times.push(r.recv_ns);
    }
    assert_eq!(times, vec![10, 20, 5]);
    assert_eq!(merge.clock_steps(), 1);
}
#[test]
fn tick_cutoff_includes_exact_tick_not_one_ns_later() {
    let path = scratch().join("trades.jsonl");
    fs::write(
        &path,
        format!(
            "{}\n{}\n",
            trade(SECOND, 1, false),
            trade(SECOND + 1, 2, true)
        ),
    )
    .unwrap();
    let mut merge = Merge::new(vec![path]).unwrap();
    let mut engine = Engine::default();
    while merge.peek_ns().is_some_and(|t| t <= i128::from(SECOND)) {
        engine.consume(&merge.pop().unwrap().unwrap()).unwrap();
    }
    let first = row(&mut engine, SECOND);
    assert_eq!(first.trade_count, Some(1));
    assert_eq!(first.taker_buy_base, Some(0.5));
    engine.consume(&merge.pop().unwrap().unwrap()).unwrap();
    let second = row(&mut engine, 2 * SECOND);
    assert_eq!(second.trade_count, Some(1));
    assert_eq!(second.taker_sell_base, Some(0.5));
}
#[test]
fn trade_overlap_dedup_switches_only_on_first_trade() {
    let mut e = Engine::default();
    e.consume(&rec(trade(SECOND, 1, false))).unwrap();
    e.consume(&rec(control("trades", "start", SECOND, "B")))
        .unwrap();
    e.consume(&rec(trade(SECOND, 2, true))).unwrap();
    let mut b = trade(SECOND, 2, true);
    b["conn"] = json!("B");
    e.consume(&rec(b)).unwrap();
    e.consume(&rec(trade(SECOND, 3, false))).unwrap();
    let mut b = trade(SECOND, 4, false);
    b["conn"] = json!("B");
    e.consume(&rec(b)).unwrap();
    let r = row(&mut e, SECOND);
    assert_eq!(r.trade_count, Some(3));
    assert_eq!(r.volume_base, Some(1.5));
    assert_eq!(r.taker_buy_base, Some(1.0));
    assert_eq!(r.taker_sell_base, Some(0.5));
}
#[test]
fn okx_ctval_scales_book_and_canonical_trades_per_connection() {
    let mut start = envelope("okx", "perp", "l2", "control", SECOND, Value::Null);
    start["event"] = json!("start");
    start["detail"] = json!({"instrument":{"ctVal":"0.01"}});
    let mut e = Engine::default();
    e.consume(&rec(start.clone())).unwrap();
    let snap = envelope(
        "okx",
        "perp",
        "l2",
        "snapshot",
        SECOND,
        json!({"data":[{"seqId":1,"checksum":0,"bids":[["100","5"]],"asks":[["102","10"]]}]}),
    );
    e.consume(&rec(snap)).unwrap();
    start["feed"] = json!("trades");
    e.consume(&rec(start)).unwrap();
    e.consume(&rec(envelope(
        "okx",
        "perp",
        "trades",
        "trade",
        SECOND,
        json!({"data":[{"tradeId":"1","sz":"3","side":"sell"}]}),
    )))
    .unwrap();
    let mut agg = envelope(
        "okx",
        "perp",
        "trades_agg",
        "trade",
        SECOND,
        json!({"data":[{"tradeId":"1","sz":"100","side":"buy"}]}),
    );
    agg["conn"] = json!("Z");
    e.consume(&rec(agg)).unwrap();
    let r = row(&mut e, SECOND);
    assert!(r.book_valid);
    assert_eq!(r.bid_depth10, Some(0.05));
    assert_eq!(r.ask_depth10, Some(0.1));
    assert_eq!(r.volume_base, Some(0.03));
    assert_eq!(r.taker_sell_base, Some(0.03));
    assert_eq!(r.mid_e9, Some(101000000000));
    assert!((r.microprice.unwrap() - 100.66666666666667).abs() < 1e-10);
}
#[test]
fn invalid_book_nulls_features_and_returns_warmup_and_vol_windows() {
    let mut e = Engine::default();
    e.consume(&rec(snap(SECOND, "100", "102"))).unwrap();
    for t in 1..=301 {
        if t > 1 && t % 60 == 0 {
            e.consume(&rec(snap(t * SECOND, "100", "102"))).unwrap();
        }
        let r = row(&mut e, t * SECOND);
        assert_eq!(r.warmup, t < 301);
        if t == 61 {
            assert_eq!(r.ret_60s, Some(0.0));
            assert_eq!(r.rv_60s, Some(0.0));
        }
        if t == 301 {
            assert_eq!(r.rv_300s, Some(0.0));
        }
    }
    e.consume(&rec(control("l2", "drop", 302 * SECOND, "A")))
        .unwrap();
    let r = row(&mut e, 302 * SECOND);
    assert!(!r.book_valid);
    assert!(
        r.mid.is_none()
            && r.mid_e9.is_none()
            && r.best_bid.is_none()
            && r.best_ask.is_none()
            && r.microprice.is_none()
            && r.bid_depth10.is_none()
            && r.imbalance10.is_none()
    );
    assert!(r.ret_1s.is_none() && r.rv_60s.is_none());
}
#[test]
fn cross_rows_math_uses_valid_same_exchange_and_asset() {
    let mut e = Engine::default();
    e.consume(&rec(snap(SECOND, "99", "101"))).unwrap();
    let mut perp = snap(SECOND, "100", "102");
    perp["market"] = json!("perp");
    e.consume(&rec(perp)).unwrap();
    let okx = envelope(
        "okx",
        "spot",
        "l2",
        "snapshot",
        SECOND,
        json!({"data":[{"seqId":1,"checksum":0,"bids":[["99.5","1"]],"asks":[["101.5","1"]]}]}),
    );
    e.consume(&rec(okx)).unwrap();
    let rows = e.tick(SECOND).unwrap();
    let cross = rows.iter().find(|r| r.exchange == "cross").unwrap();
    assert_eq!(cross.symbol, "BTC");
    assert!((cross.basis_bps_binance.unwrap() - 100.0).abs() < 1e-9);
    assert!((cross.spot_dev_bps_okx.unwrap() - 50.0).abs() < 1e-9);
    assert!(cross.basis_bps_kraken.is_none());
}
#[test]
fn gzip_open_complete_lines_discovery_and_bad_dates() {
    let dir = scratch();
    let parent = dir.join("raw/date=2026-10-17/exchange=binance/market=spot/feed=trades");
    fs::create_dir_all(&parent).unwrap();
    let open = parent.join("BTCUSDT.jsonl.open");
    fs::write(
        &open,
        format!("{}\n{{\"partial\":", trade(SECOND, 1, false)),
    )
    .unwrap();
    assert!(discover(&dir, "2026-10-17", false).is_err());
    let paths = discover(&dir, "2026-10-17", true).unwrap();
    let mut merge = Merge::new(paths).unwrap();
    assert!(merge.pop().unwrap().is_some());
    assert!(merge.pop().unwrap().is_none());
    let gz = parent.join("BTCUSDT.jsonl.gz");
    let mut encoder =
        flate2::write::GzEncoder::new(File::create(&gz).unwrap(), flate2::Compression::default());
    writeln!(encoder, "{}", trade(SECOND, 2, false)).unwrap();
    encoder.finish().unwrap();
    let paths = discover(&dir, "2026-10-17", true).unwrap();
    assert_eq!(paths, vec![gz]);
    let mut merge = Merge::new(paths).unwrap();
    assert_eq!(merge.pop().unwrap().unwrap().kind, Kind::Trade);
    assert_eq!(day_bounds("1970-01-01").unwrap().0, 0);
    assert!(day_bounds("2026-02-29").is_err());
    assert!(day_bounds("2024-02-29").is_ok());
    assert!(day_bounds("../../secret").is_err());
}
#[test]
fn parquet_optional_columns_metadata_and_atomic_publish() {
    let out = scratch().join("features.parquet");
    let mut writer = Writer::new(&out).unwrap();
    writer
        .push(Row {
            exchange: "test".into(),
            engine_version: ENGINE_VERSION.into(),
            ..Default::default()
        })
        .unwrap();
    assert!(!out.exists());
    assert_eq!(writer.finish().unwrap(), 1);
    let reader = SerializedFileReader::new(File::open(&out).unwrap()).unwrap();
    let metadata = reader.metadata().file_metadata();
    assert_eq!(metadata.num_rows(), 1);
    assert!(
        metadata
            .key_value_metadata()
            .unwrap()
            .iter()
            .any(|k| k.key == "mode" && k.value.as_deref() == Some("ideal"))
    );
    let r = reader.get_row_iter(None).unwrap().next().unwrap().unwrap();
    assert_eq!(r.get_string(0).unwrap(), "test");
    let v = row_json(r);
    assert!(v["mid"].is_null());
    assert!(v["mid_e9"].is_null());
}
#[test]
fn end_to_end_synthetic_day_directory() {
    let root = scratch();
    let day = "2026-10-17";
    let (_, end) = day_bounds(day).unwrap();
    let start = end - 3 * SECOND;
    let book = root.join(format!(
        "raw/date={day}/exchange=binance/market=spot/feed=l2/BTCUSDT.jsonl"
    ));
    fs::create_dir_all(book.parent().unwrap()).unwrap();
    fs::write(&book, format!("{}\n", snap(start, "100", "102"))).unwrap();
    let trades = root.join(format!(
        "raw/date={day}/exchange=binance/market=spot/feed=trades/BTCUSDT.jsonl.open"
    ));
    fs::create_dir_all(trades.parent().unwrap()).unwrap();
    fs::write(&trades, format!("{}\npartial", trade(start + 1, 1, true))).unwrap();
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_engine-replay"))
        .arg("--root")
        .arg(&root)
        .args(["--date", day, "--allow-open"])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let reader = SerializedFileReader::new(
        File::open(root.join(format!("features/date={day}/features.parquet"))).unwrap(),
    )
    .unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 6);
    let rows = reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| row_json(r.unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(rows[0]["available_ns"], start + 250_000_000);
    assert_eq!(rows[0]["mid_e9"], 101000000000i64);
    assert_eq!(rows[2]["trade_count"], 1);
    assert_eq!(rows[2]["taker_sell_base"], 0.5);
    assert_eq!(rows[0]["engine_version"], ENGINE_VERSION);
}

#[test]
fn kraken_l3_counts_only_events_within_top_ten() {
    use engine_core::kraken::L3Book;
    let bids = (90..100)
        .map(|p| json!({"order_id":format!("b{p}"),"limit_price":p.to_string(),"order_qty":"1.00"}))
        .chain(std::iter::once(
            json!({"order_id":"outside","limit_price":"89","order_qty":"1"}),
        ))
        .collect::<Vec<_>>();
    let mut data =
        json!({"bids":bids,"asks":[{"order_id":"a","limit_price":"101","order_qty":"1.00"}]});
    let mut reference = L3Book::new(100);
    reference.apply(&data, true).unwrap();
    data["checksum"] = json!(reference.checksum());
    let mut engine = Engine::default();
    engine
        .consume(&rec(envelope(
            "kraken",
            "spot",
            "l3",
            "snapshot",
            SECOND,
            json!({"data":[data]}),
        )))
        .unwrap();
    let initial = row(&mut engine, SECOND);
    assert_eq!(initial.l3_adds_1s, Some(0));
    let mut data = json!({"bids":[{"event":"add","order_id":"new","limit_price":"99","order_qty":"2.00"},{"event":"modify","order_id":"b99","limit_price":"99","order_qty":"0.5"},{"event":"delete","order_id":"outside","limit_price":"89"},{"event":"delete","order_id":"b98","limit_price":"98"}]});
    reference.apply(&data, false).unwrap();
    data["checksum"] = json!(reference.checksum());
    engine
        .consume(&rec(envelope(
            "kraken",
            "spot",
            "l3",
            "update",
            2 * SECOND,
            json!({"data":[data]}),
        )))
        .unwrap();
    let r = row(&mut engine, 2 * SECOND);
    assert_eq!(
        (r.l3_adds_1s, r.l3_modifies_1s, r.l3_deletes_1s),
        (Some(1), Some(1), Some(1))
    );
    assert_eq!(engine.book_breaks, 0);
}
#[test]
fn kraken_trade_sides_and_snapshots_and_perp_extras() {
    let mut e = Engine::default();
    e.consume(&rec(envelope(
        "kraken",
        "spot",
        "trades",
        "snapshot",
        SECOND,
        json!({"data":[{"trade_id":1,"side":"buy","qty":"100"}]}),
    )))
    .unwrap();
    e.consume(&rec(envelope("kraken","spot","trades","trade",SECOND,json!({"data":[{"trade_id":1,"side":"buy","qty":"100"},{"trade_id":2,"side":"sell","qty":"1.5"}]})))).unwrap();
    let r = row(&mut e, SECOND);
    assert_eq!(r.trade_count, Some(1));
    assert_eq!(r.taker_sell_base, Some(1.5));
    assert_eq!(r.taker_buy_base, Some(0.0));
    let mut e = Engine::default();
    e.consume(&rec(envelope(
        "kraken",
        "perp",
        "trades",
        "trade",
        SECOND,
        json!({"uid":"one","qty":"2.5","side":"buy"}),
    )))
    .unwrap();
    e.consume(&rec(envelope(
        "kraken",
        "perp",
        "ticker",
        "update",
        SECOND,
        json!({"funding_rate":0.000000000123,"markPrice":100.123456789,"openInterest":123}),
    )))
    .unwrap();
    let r = row(&mut e, SECOND);
    assert_eq!(r.taker_buy_base, Some(2.5));
    assert_eq!(r.funding_rate, Some(0.000000000123));
    assert_eq!(r.mark_price, Some(100.123456789));
    assert_eq!(r.open_interest, Some(123.0));
    let mut e = Engine::default();
    e.consume(&rec(envelope(
        "binance",
        "perp",
        "mark",
        "update",
        SECOND,
        json!({"data":{"r":"0.0001","p":"100.2"}}),
    )))
    .unwrap();
    e.consume(&rec(envelope(
        "binance",
        "perp",
        "liquidations",
        "update",
        SECOND,
        json!({"data":{"o":{"q":"10","z":"3"}}}),
    )))
    .unwrap();
    let r = row(&mut e, SECOND);
    assert_eq!(r.funding_rate, Some(0.0001));
    assert_eq!(r.mark_price, Some(100.2));
    assert_eq!(r.liq_count_1s, Some(1));
    assert_eq!(r.liq_base_1s, Some(3.0));
}

#[test]
fn volumes_accumulate_exactly_and_half_atom_mids_are_not_rounded() {
    let mut engine = Engine::default();
    for (i, qty) in ["0.1", "0.2"].into_iter().enumerate() {
        engine
            .consume(&rec(envelope(
                "binance",
                "spot",
                "trades",
                "trade",
                SECOND,
                json!({"data":{"t":i,"q":qty,"m":false}}),
            )))
            .unwrap();
    }
    let r = row(&mut engine, SECOND);
    assert_eq!(r.volume_base, Some(0.3));
    assert_eq!(r.taker_buy_base, Some(0.3));
    let mut engine = Engine::default();
    engine
        .consume(&rec(snap(SECOND, "0.00000001", "0.00000002")))
        .unwrap();
    let r = row(&mut engine, SECOND);
    assert!(r.book_valid);
    assert_eq!(r.mid, Some(0.000000015));
    assert_eq!(r.mid_e9, Some(15));
    assert!(r.ret_1s.is_none());
    for tick in 2..=301 {
        let r = row(&mut engine, tick * SECOND);
        assert_eq!(r.warmup, tick < 301);
    }
}

#[test]
fn l3_deletes_use_rank_at_each_event_not_initial_frame_rank() {
    use engine_core::kraken::L3Book;
    let bids = (89..100)
        .map(|p| json!({"order_id":p.to_string(),"limit_price":p.to_string(),"order_qty":"1"}))
        .collect::<Vec<_>>();
    let mut data = json!({"bids":bids,"asks":[]});
    let mut reference = L3Book::new(100);
    reference.apply(&data, true).unwrap();
    data["checksum"] = json!(reference.checksum());
    let mut engine = Engine::default();
    engine
        .consume(&rec(envelope(
            "kraken",
            "spot",
            "l3",
            "snapshot",
            SECOND,
            json!({"data":[data]}),
        )))
        .unwrap();
    let events = [99, 98, 89]
        .into_iter()
        .map(|p| json!({"event":"delete","order_id":p.to_string(),"limit_price":p.to_string()}))
        .collect::<Vec<_>>();
    let mut data = json!({"bids":events});
    reference.apply(&data, false).unwrap();
    data["checksum"] = json!(reference.checksum());
    engine
        .consume(&rec(envelope(
            "kraken",
            "spot",
            "l3",
            "update",
            SECOND,
            json!({"data":[data]}),
        )))
        .unwrap();
    assert_eq!(row(&mut engine, SECOND).l3_deletes_1s, Some(3));
}

#[test]
fn silent_book_expires_after_60_seconds_and_recovers_on_next_record() {
    let mut e = Engine::default();
    e.consume(&rec(snap(SECOND, "100", "102"))).unwrap();
    let first = row(&mut e, SECOND);
    assert!(first.book_valid);
    assert_eq!(first.book_age_ms, Some(0.0));
    let boundary = row(&mut e, 61 * SECOND);
    assert!(boundary.book_valid);
    assert_eq!(boundary.book_age_ms, Some(60_000.0));
    let stale = row(&mut e, 62 * SECOND);
    assert!(!stale.book_valid);
    assert_eq!(stale.book_age_ms, Some(61_000.0));
    assert!(
        stale.best_bid.is_none()
            && stale.best_ask.is_none()
            && stale.mid.is_none()
            && stale.mid_e9.is_none()
            && stale.microprice.is_none()
            && stale.spread_bps.is_none()
            && stale.bid_depth10.is_none()
            && stale.ask_depth10.is_none()
            && stale.imbalance10.is_none()
            && stale.ret_1s.is_none()
    );
    e.consume(&rec(envelope(
        "binance",
        "spot",
        "l2",
        "update",
        63 * SECOND,
        json!({"data":{"U":2,"u":2,"b":[],"a":[]}}),
    )))
    .unwrap();
    let recovered = row(&mut e, 63 * SECOND);
    assert!(recovered.book_valid);
    assert_eq!(recovered.book_age_ms, Some(0.0));
    assert!(recovered.ret_1s.is_none());
    assert_eq!(e.book_breaks, 0);
    assert_eq!(row(&mut e, 64 * SECOND).ret_1s, Some(0.0));
}
#[test]
fn extras_expire_independently_and_flows_remain_zero_when_silent() {
    for (exchange, feed, payload) in [
        ("binance", "mark", json!({"data":{"r":"0.001","p":"100"}})),
        (
            "kraken",
            "ticker",
            json!({"funding_rate":0.001,"markPrice":100,"openInterest":3}),
        ),
    ] {
        let mut e = Engine::default();
        e.consume(&rec(envelope(
            exchange,
            "perp",
            feed,
            "update",
            SECOND,
            payload.clone(),
        )))
        .unwrap();
        let initial = row(&mut e, SECOND);
        assert_eq!(initial.mark_price, Some(100.0));
        assert!(initial.book_age_ms.is_none());
        assert_eq!(row(&mut e, 61 * SECOND).funding_rate, Some(0.001));
        let mut activity = envelope(
            exchange,
            "perp",
            "trades",
            "control",
            62 * SECOND,
            Value::Null,
        );
        activity["event"] = json!("start");
        e.consume(&rec(activity)).unwrap();
        let expired = row(&mut e, 62 * SECOND);
        assert!(
            expired.funding_rate.is_none()
                && expired.mark_price.is_none()
                && expired.open_interest.is_none()
        );
        assert_eq!(expired.trade_count, Some(0));
        assert_eq!(expired.volume_base, Some(0.0));
        e.consume(&rec(envelope(
            exchange,
            "perp",
            feed,
            "update",
            63 * SECOND,
            payload,
        )))
        .unwrap();
        let recovered = row(&mut e, 63 * SECOND);
        assert_eq!(recovered.mark_price, Some(100.0));
        assert_eq!(recovered.funding_rate, Some(0.001));
        if exchange == "kraken" {
            assert_eq!(recovered.open_interest, Some(3.0));
        }
    }
}
#[test]
fn freshness_uses_controls_and_ignores_retired_book_records() {
    let mut e = Engine::default();
    e.consume(&rec(snap(SECOND, "100", "102"))).unwrap();
    e.consume(&rec(control("l2", "start", 61 * SECOND, "B")))
        .unwrap();
    let fresh = row(&mut e, 61 * SECOND);
    assert_eq!(fresh.book_age_ms, Some(0.0));
    assert!(fresh.book_valid);
    let mut replacement = snap(62 * SECOND, "100", "102");
    replacement["conn"] = json!("B");
    e.consume(&rec(replacement)).unwrap();
    e.consume(&rec(snap(123 * SECOND, "100", "102"))).unwrap();
    let stale = row(&mut e, 123 * SECOND);
    assert_eq!(stale.book_age_ms, Some(61_000.0));
    assert!(!stale.book_valid);
}

#[test]
fn collector_crash_trade_carrier_remains_eligible_and_dedup_survives() {
    let mut e = Engine::default();
    e.consume(&rec(trade(SECOND, 1, false))).unwrap();
    let mut crash = control("trades", "drop", SECOND, "B");
    crash["reason"] = json!("collector_crash");
    e.consume(&rec(crash)).unwrap();
    e.consume(&rec(trade(SECOND, 2, false))).unwrap(); // Previous crashed source is retired immediately.
    e.consume(&rec(control("trades", "start", SECOND, "B")))
        .unwrap();
    for (id, sell) in [(1, false), (2, true)] {
        let mut r = trade(SECOND, id, sell);
        r["conn"] = json!("B");
        e.consume(&rec(r)).unwrap();
    }
    let mut late = control("trades", "drop", SECOND, "A");
    late["reason"] = json!("collector_crash");
    e.consume(&rec(late)).unwrap();
    let mut r = trade(SECOND, 3, false);
    r["conn"] = json!("B");
    e.consume(&rec(r)).unwrap();
    let r = row(&mut e, SECOND);
    assert_eq!(r.trade_count, Some(3));
    assert_eq!(r.volume_base, Some(1.5));
    assert_eq!(r.taker_buy_base, Some(1.0));
    assert_eq!(r.taker_sell_base, Some(0.5));
    e.consume(&rec(control("trades", "drop", 2 * SECOND, "B")))
        .unwrap();
    let mut r = trade(2 * SECOND, 4, false);
    r["conn"] = json!("B");
    e.consume(&rec(r)).unwrap();
    assert_eq!(row(&mut e, 2 * SECOND).trade_count, Some(0)); // Normal drops still retire their carrier.
}
#[test]
fn collector_crash_book_features_accept_new_conn_metadata_and_updates() {
    let mut e = Engine::default();
    let start = |conn: &str| {
        let mut r = envelope("okx", "perp", "l2", "control", SECOND, Value::Null);
        r["event"] = json!("start");
        r["conn"] = json!(conn);
        r["detail"] = json!({"instrument":{"ctVal":"0.1"}});
        r
    };
    let snapshot = |conn: &str| {
        let mut r = envelope(
            "okx",
            "perp",
            "l2",
            "snapshot",
            SECOND,
            json!({"data":[{"seqId":10,"checksum":0,"bids":[["100","5"]],"asks":[["102","10"]]}]}),
        );
        r["conn"] = json!(conn);
        r
    };
    e.consume(&rec(start("A"))).unwrap();
    e.consume(&rec(snapshot("A"))).unwrap();
    let mut crash = start("B");
    crash["event"] = json!("drop");
    crash["reason"] = json!("collector_crash");
    e.consume(&rec(crash)).unwrap();
    assert!(!row(&mut e, SECOND).book_valid);
    e.consume(&rec(start("B"))).unwrap();
    e.consume(&rec(snapshot("B"))).unwrap();
    let mut update = envelope(
        "okx",
        "perp",
        "l2",
        "update",
        2 * SECOND,
        json!({"data":[{"seqId":11,"prevSeqId":10,"checksum":0,"bids":[["100","6"]],"asks":[]}]}),
    );
    update["conn"] = json!("B");
    e.consume(&rec(update)).unwrap();
    let r = row(&mut e, 2 * SECOND);
    assert!(r.book_valid);
    assert_eq!(r.bid_depth10, Some(0.6));
    assert_eq!(e.book_breaks, 1);
}
