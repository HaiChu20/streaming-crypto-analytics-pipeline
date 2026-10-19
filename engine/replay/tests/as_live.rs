use engine_core::{
    ENGINE_VERSION, Record, SECOND,
    session::{Checkpoint, Cutoff},
};
use engine_replay::{as_live::Replay, state_io};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
fn v3(r: &Record) -> String {
    let native: serde_json::Value = serde_json::from_str(&r.payload).unwrap();
    let mut payload = json!({"channel":"trade", "data":native.get("data").unwrap_or(&native)});
    payload["recv_time_ns"] = json!(r.recv_ns);
    payload["offset"] = json!(r.offset);
    payload["conn"] = json!(r.conn);
    payload.to_string()
}
static ID: AtomicU64 = AtomicU64::new(0);
#[test]
fn synthetic_live_session_replays_cutoffs_carry_and_late_records_exactly() {
    let root = PathBuf::from(
        std::env::var_os("ENGINE_TEST_SCRATCH").unwrap_or_else(|| std::env::temp_dir().into()),
    )
    .join(format!(
        "as-live-tests/{}-{}",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    let date = "2026-10-17";
    let start = engine_replay::input::day_bounds(date).unwrap().0;
    let key = "raw.binance.spot.trades.v1/0";
    let mut live = Checkpoint::new(start, BTreeMap::from([(key.into(), 99)]));
    let dir = root.join("engine").join(format!("date={date}"));
    fs::create_dir_all(&dir).unwrap();
    state_io::save(&dir.join("start_state.json"), &live).unwrap();
    let record = |ns, offset, id| {
        Record::parse(&json!({"exchange":"binance","market":"spot","symbol":"BTCUSDT","feed":"trades","kind":"trade","conn":"A","seq":offset,"recv_ns":ns,"topic":"raw.binance.spot.trades.v1","partition":0,"offset":offset,"payload":json!({"data":{"t":id,"q":"0.1","m":false}}).to_string()}).to_string()).unwrap()
    };
    let carry = record(start - 1, 100, 1);
    let late = record(start + 1, 101, 2);
    let future = record(start + SECOND + 1, 102, 3);
    fs::write(
        dir.join("carry_in.jsonl"),
        format!("{}\n", serde_json::to_string(&carry).unwrap()),
    )
    .unwrap();
    let raw = root.join("raw/binance/spot/BTCUSDT");
    fs::create_dir_all(&raw).unwrap();
    fs::write(
        raw.join("20261017_trades.jsonl"),
        format!("{}\n{}\n", v3(&late), v3(&future)),
    )
    .unwrap();
    let c0 = Cutoff {
        tick_ns: start,
        engine_version: ENGINE_VERSION.into(),
        processed: BTreeMap::from([(key.into(), 100)]),
    };
    let c1 = Cutoff {
        tick_ns: start + SECOND,
        engine_version: ENGINE_VERSION.into(),
        processed: BTreeMap::from([(key.into(), 100)]),
    };
    let c2 = Cutoff {
        tick_ns: start + 2 * SECOND,
        engine_version: ENGINE_VERSION.into(),
        processed: BTreeMap::from([(key.into(), 102)]),
    };
    fs::write(
        dir.join("cutoffs.jsonl"),
        [&c0, &c1, &c2]
            .into_iter()
            .map(|c| format!("{}\n", serde_json::to_string(c).unwrap()))
            .collect::<String>(),
    )
    .unwrap();
    let expected = [
        live.apply(c0, vec![carry]).unwrap(),
        live.apply(c1, vec![]).unwrap(),
        live.apply(c2, vec![late, future]).unwrap(),
    ];
    let mut replay = Replay::open(&root, date, None, false).unwrap();
    for (cutoff, expected) in std::mem::take(&mut replay.cutoffs)
        .into_iter()
        .zip(expected)
    {
        assert_eq!(
            serde_json::to_value(replay.tick(cutoff).unwrap()).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }
    assert_eq!(replay.state.engine.records, 3);
    assert_eq!(replay.late_records, 1);
}
#[test]
fn utc_dates_match_day_bounds_including_leap_day() {
    for date in ["1970-01-01", "2000-02-29", "2026-10-17", "2100-03-01"] {
        assert_eq!(
            state_io::utc_date(engine_replay::input::day_bounds(date).unwrap().0),
            date
        );
    }
}

#[test]
fn latest_start_without_okx_ctval_keeps_trade_count_and_nulls_unknown_base_volume() {
    use engine_core::features::Engine;
    let make = |kind: &str, conn: &str, ns: i64, payload: serde_json::Value| {
        let mut v = json!({"exchange":"okx","market":"perp","symbol":"BTC-USDT-SWAP","feed":"trades","kind":kind,"conn":conn,"seq":1,"recv_ns":ns,"topic":"raw.okx.perp.trades.v1","partition":0,"offset":1,"payload":payload.to_string()});
        if kind == "control" {
            v["event"] = json!("start");
            v["detail"] = json!({"instrument":{"ctVal":"0.01"}});
        }
        Record::parse(&v.to_string()).unwrap()
    };
    let mut engine = Engine::default();
    let unknown = make(
        "trade",
        "A",
        SECOND + 1,
        json!({"data":[{"tradeId":"1","sz":"2","side":"buy"}]}),
    );
    engine.consume(&unknown).unwrap();
    engine.consume(&unknown).unwrap();
    let start = make("control", "A", SECOND + 2, json!(null));
    engine.consume(&start).unwrap();
    let known = make(
        "trade",
        "A",
        SECOND + 3,
        json!({"data":[{"tradeId":"2","sz":"3","side":"sell"}]}),
    );
    engine.consume(&known).unwrap();
    let rows = engine.tick(2 * SECOND).unwrap();
    let row = rows.iter().find(|r| r.exchange == "okx").unwrap();
    assert_eq!(row.trade_count, Some(2));
    assert_eq!(
        (row.volume_base, row.taker_buy_base, row.taker_sell_base),
        (None, None, None)
    );
    let still_unknown = engine.tick(3 * SECOND).unwrap();
    let row = still_unknown.iter().find(|r| r.exchange == "okx").unwrap();
    assert_eq!(row.trade_count, Some(0));
    assert_eq!(row.volume_base, Some(0.0));
    let after = make(
        "trade",
        "A",
        3 * SECOND + 1,
        json!({"data":[{"tradeId":"3","sz":"4","side":"buy"}]}),
    );
    engine.consume(&after).unwrap();
    let rows = engine.tick(4 * SECOND).unwrap();
    let row = rows.iter().find(|r| r.exchange == "okx").unwrap();
    assert_eq!(row.trade_count, Some(1));
    assert!((row.volume_base.unwrap() - 0.04).abs() < 1e-12);
}

#[test]
fn malformed_book_frame_is_counted_and_next_snapshot_recovers() {
    use engine_core::features::Engine;
    let make = |kind: &str, conn: &str, payload: &str| {
        Record::parse(&json!({"exchange":"okx","market":"spot","symbol":"BTC-USDT","feed":"l2","kind":kind,"conn":conn,"seq":1,"recv_ns":SECOND,"topic":"raw.okx.spot.l2.v1","partition":0,"offset":1,"payload":payload}).to_string()).unwrap()
    };
    let mut engine = Engine::default();
    engine.consume(&make("snapshot", "A", "not JSON")).unwrap();
    assert_eq!(engine.data_anomalies.values().sum::<u64>(), 1);
    assert!(
        !engine
            .tick(SECOND)
            .unwrap()
            .iter()
            .find(|r| r.exchange == "okx")
            .unwrap()
            .book_valid
    );
    let payload=json!({"data":[{"seqId":1,"prevSeqId":0,"checksum":0,"bids":[["100","1"]],"asks":[["102","1"]]}]}).to_string();
    engine.consume(&make("snapshot", "B", &payload)).unwrap();
    assert!(
        engine
            .tick(2 * SECOND)
            .unwrap()
            .iter()
            .find(|r| r.exchange == "okx")
            .unwrap()
            .book_valid
    );
}

#[test]
fn malformed_trade_frame_nulls_only_its_flow_bucket_and_later_ticks_recover() {
    use engine_core::features::Engine;
    let mut engine = Engine::default();
    let trade = |ns: i64, payload: serde_json::Value| {
        Record::parse(&json!({"exchange":"binance","market":"spot","symbol":"BTCUSDT","feed":"trades","kind":"trade","conn":"A","seq":1,"recv_ns":ns,"topic":"raw.binance.spot.trades.v1","partition":0,"offset":1,"payload":payload.to_string()}).to_string()).unwrap()
    };
    engine
        .consume(&trade(SECOND + 1, json!({"data":{"q":"1","m":false}})))
        .unwrap();
    let first = engine.tick(2 * SECOND).unwrap();
    let row = first.iter().find(|r| r.exchange == "binance").unwrap();
    assert_eq!((row.trade_count, row.volume_base), (None, None));
    assert_eq!(engine.data_anomalies.values().sum::<u64>(), 1);
    engine
        .consume(&trade(
            2 * SECOND + 1,
            json!({"data":{"t":2,"q":"1","m":false}}),
        ))
        .unwrap();
    let second = engine.tick(3 * SECOND).unwrap();
    let row = second.iter().find(|r| r.exchange == "binance").unwrap();
    assert_eq!((row.trade_count, row.volume_base), (Some(1), Some(1.0)));
}

#[test]
fn unrepresentable_mid_is_counted_and_book_invalidated_without_tick_failure() {
    use engine_core::features::Engine;
    let snap = |conn: &str, bid: &str, ask: &str| {
        Record::parse(&json!({"exchange":"okx","market":"spot","symbol":"BTC-USDT","feed":"l2","kind":"snapshot","conn":conn,"seq":1,"recv_ns":SECOND,"topic":"raw.okx.spot.l2.v1","partition":0,"offset":1,"payload":json!({"data":[{"seqId":1,"prevSeqId":0,"checksum":0,"bids":[[bid,"1"]],"asks":[[ask,"1"]]}]}).to_string()}).to_string()).unwrap()
    };
    let mut engine = Engine::default();
    engine
        .consume(&snap("A", "10000000000", "10000000001"))
        .unwrap();
    let row = engine
        .tick(SECOND)
        .unwrap()
        .into_iter()
        .find(|r| r.exchange == "okx")
        .unwrap();
    assert!(!row.book_valid);
    assert_eq!(engine.data_anomalies.values().sum::<u64>(), 1);
    engine.consume(&snap("B", "100", "101")).unwrap();
    assert!(
        engine
            .tick(2 * SECOND)
            .unwrap()
            .into_iter()
            .find(|r| r.exchange == "okx")
            .unwrap()
            .book_valid
    );
}

#[test]
fn from_state_preserves_full_hour_windows_and_pending_flow() {
    let root = std::env::temp_dir().join(format!(
        "p18-state-{}-{}",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    let date = "2026-10-17";
    let start = engine_replay::input::day_bounds(date).unwrap().0;
    let directory = root.join("engine").join(format!("date={date}"));
    fs::create_dir_all(&directory).unwrap();
    let raw = root.join("raw/okx/spot/BTC-USDT");
    fs::create_dir_all(&raw).unwrap();
    fs::write(raw.join("20261017_book.jsonl"), "").unwrap();
    let mut state = Checkpoint::new(start, BTreeMap::new());
    let cutoff = |t| Cutoff {
        tick_ns: start + t * SECOND,
        engine_version: ENGINE_VERSION.into(),
        processed: BTreeMap::new(),
    };
    for t in 0..=3900 {
        let record = Record::parse(&json!({"exchange":"okx","market":"spot","symbol":"BTC-USDT","feed":"l2",
            "kind":if t==0 {"snapshot"} else {"update"}, "conn":"A","seq":t,"recv_ns":start+t*SECOND,
            "payload":json!({"data":[{"seqId":t,"prevSeqId":t-1,"checksum":0,
                "bids":[[format!("100.{t:08}"),(t+1).to_string()]],"asks":[["102","3"]]}]}).to_string()}).to_string()).unwrap();
        let rows = state.apply(cutoff(t), vec![record]).unwrap();
        if t == 3900 {
            let row = rows.iter().find(|r| r.exchange == "okx").unwrap();
            assert_eq!(row.memory.cov_3600s, Some(1.0));
            assert!(row.memory.ofi10_3600s.unwrap() > 0.0);
            assert!(row.memory.ret_3600s.unwrap() > 0.0);
            assert!(row.memory.rv_3600s.unwrap() > 0.0);
        }
    }
    let future = Record::parse(&json!({"exchange":"okx","market":"spot","symbol":"BTC-USDT","feed":"trades",
        "kind":"trade","conn":"T","seq":1,"recv_ns":start+3901*SECOND,
        "payload":json!({"data":[{"tradeId":"1","sz":"123456.12345678","side":"buy"}]}).to_string()}).to_string()).unwrap();
    state.engine.consume(&future).unwrap();
    let checkpoint = directory.join("hour_state.json");
    state_io::save(&checkpoint, &state).unwrap();
    let cutoffs = (3901..=3965).map(cutoff).collect::<Vec<_>>();
    fs::write(
        directory.join("cutoffs.jsonl"),
        cutoffs
            .iter()
            .map(|c| format!("{}\n", serde_json::to_string(c).unwrap()))
            .collect::<String>(),
    )
    .unwrap();
    let mut replay = Replay::open(&root, date, Some(&checkpoint), false).unwrap();
    let mut ideal: engine_core::features::Engine =
        serde_json::from_slice(&serde_json::to_vec(&state.engine).unwrap()).unwrap();
    for cutoff in cutoffs {
        let tick = cutoff.tick_ns;
        let live = state.apply(cutoff.clone(), vec![]).unwrap();
        assert_eq!(
            serde_json::to_value(&live).unwrap(),
            serde_json::to_value(replay.tick(cutoff).unwrap()).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&live).unwrap(),
            serde_json::to_value(ideal.tick(tick).unwrap()).unwrap()
        );
    }
}
