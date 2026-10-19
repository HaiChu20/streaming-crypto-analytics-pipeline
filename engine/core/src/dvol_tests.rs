use super::*;
use serde_json::json;

fn record(currency: &str, ts: i64, recv: i64, close: f64) -> Record {
    Record::parse(
        &json!({"exchange":"deribit","market":"index","feed":"dvol","symbol":currency,
        "kind":"update","conn":"D","seq":1,"recv_ns":recv,
        "payload":json!([ts / 1_000_000, close, close, close, close]).to_string()})
        .to_string(),
    )
    .unwrap()
}
fn cross(engine: &mut Engine, tick: i64, currency: &str) -> Row {
    engine
        .tick(tick)
        .unwrap()
        .into_iter()
        .find(|r| r.exchange == "cross" && r.asset == currency)
        .unwrap()
}
#[test]
fn availability_floor_late_receive_staleness_and_asset_isolation() {
    let mut live = Engine::default();
    let mut backfill = Engine::default();
    for engine in [&mut live, &mut backfill] {
        let mut start = record("BTC", 0, 0, 0.0);
        start.kind = Kind::Control;
        start.event = Some("start".into());
        engine.consume(&start).unwrap();
    }
    live.consume(&record("BTC", 0, 65 * SECOND, 52.3)).unwrap();
    assert!(cross(&mut live, 69 * SECOND, "BTC").dvol.is_none());
    backfill
        .consume(&record("BTC", 0, 70 * SECOND, 52.3))
        .unwrap();
    for t in [70, 180, 181] {
        let a = cross(&mut live, t * SECOND, "BTC");
        let b = cross(&mut backfill, t * SECOND, "BTC");
        assert_eq!(
            serde_json::to_value(&a).unwrap(),
            serde_json::to_value(&b).unwrap()
        );
        assert_eq!(a.dvol, (t <= 180).then_some(52.3));
    }
    let mut engine = Engine::default();
    engine
        .consume(&record("BTC", 0, 65 * SECOND, 50.0))
        .unwrap();
    engine
        .consume(&record("BTC", 60 * SECOND, 150 * SECOND, 51.0))
        .unwrap();
    engine
        .consume(&record("ETH", 60 * SECOND, 125 * SECOND, 60.0))
        .unwrap();
    assert_eq!(cross(&mut engine, 149 * SECOND, "BTC").dvol, Some(50.0));
    assert_eq!(cross(&mut engine, 150 * SECOND, "BTC").dvol, Some(51.0));
    assert_eq!(cross(&mut engine, 150 * SECOND, "ETH").dvol, Some(60.0));
    assert!(
        engine
            .tick(151 * SECOND)
            .unwrap()
            .iter()
            .all(|r| r.exchange == "cross")
    );
}
#[test]
fn change_windows_use_fresh_asof_endpoints_and_survive_checkpoint() {
    let mut engine = Engine::default();
    for ts in (0..=3600).step_by(60) {
        engine
            .consume(&record(
                "BTC",
                ts * SECOND,
                (ts + 65) * SECOND,
                50.0 + ts as f64 / 60.0,
            ))
            .unwrap();
        engine
            .consume(&record("BTC", ts * SECOND, (ts + 80) * SECOND, 999.0))
            .unwrap();
    }
    let row = cross(&mut engine, 3670 * SECOND, "BTC");
    assert_eq!(row.dvol, Some(110.0));
    assert_eq!(row.dvol_chg_300s, Some(5.0));
    assert_eq!(row.dvol_chg_900s, Some(15.0));
    assert_eq!(row.dvol_chg_3600s, Some(60.0));
    assert!(row.dvol_rv_gap.is_none());
    let mut restored: Engine =
        serde_json::from_slice(&serde_json::to_vec(&engine).unwrap()).unwrap();
    for t in [3671, 3780, 3781, 8000] {
        assert_eq!(
            serde_json::to_value(engine.tick(t * SECOND).unwrap()).unwrap(),
            serde_json::to_value(restored.tick(t * SECOND).unwrap()).unwrap()
        );
    }
    assert!(engine.dvol["BTC"].candles.is_empty());
    let mut engine = Engine::default();
    engine
        .consume(&record("BTC", 0, 65 * SECOND, 50.0))
        .unwrap();
    engine
        .consume(&record("BTC", 420 * SECOND, 485 * SECOND, 60.0))
        .unwrap();
    let row = cross(&mut engine, 490 * SECOND, "BTC");
    assert_eq!(row.dvol, Some(60.0));
    assert!(row.dvol_chg_300s.is_none() && row.dvol_chg_900s.is_none());
}
#[test]
fn rv_gap_uses_matching_binance_spot_hour_rv_in_vol_points() {
    let mut engine = Engine::default();
    for t in 0..=3700 {
        let mid = 10000 + t;
        let payload = if t == 0 {
            json!({"lastUpdateId":0,"bids":[[(mid-1).to_string(),"1"]],
                   "asks":[[(mid+1).to_string(),"1"]]})
        } else {
            json!({"U":t,"u":t,"b":[[(mid-2).to_string(),"0"],[(mid-1).to_string(),"1"]],
                   "a":[[mid.to_string(),"0"],[(mid+1).to_string(),"1"]]})
        };
        let book = Record::parse(
            &json!({"exchange":"binance","market":"spot","symbol":"BTCUSDT",
            "feed":"l2","kind":if t==0 {"snapshot"} else {"update"}, "conn":"B","seq":t,
            "recv_ns":t*SECOND,"payload":payload.to_string()})
            .to_string(),
        )
        .unwrap();
        engine.consume(&book).unwrap();
        if t == 3665 {
            engine
                .consume(&record("BTC", 3600 * SECOND, t * SECOND, 52.3))
                .unwrap();
            engine
                .consume(&record("ETH", 3600 * SECOND, t * SECOND, 60.0))
                .unwrap();
        }
        let rows = engine.tick(t * SECOND).unwrap();
        if t == 3700 {
            let market = rows.iter().find(|r| r.exchange == "binance").unwrap();
            let rv = market.memory.rv_3600s.unwrap();
            assert!(rv > 0.0 && market.dvol.is_none());
            let cross = rows
                .iter()
                .find(|r| r.exchange == "cross" && r.asset == "BTC")
                .unwrap();
            assert_eq!(
                cross.dvol_rv_gap,
                Some(52.3 - rv * (365.0_f64 * 24.0).sqrt() * 100.0)
            );
            assert!(
                rows.iter()
                    .find(|r| r.asset == "ETH")
                    .unwrap()
                    .dvol_rv_gap
                    .is_none()
            );
        }
    }
}
#[test]
fn rejects_unclosed_malformed_and_unknown_currency_without_market_rows() {
    let mut engine = Engine::default();
    engine
        .consume(&record("BTC", 0, 59 * SECOND, 52.0))
        .unwrap();
    let mut malformed = record("ETH", 0, 65 * SECOND, 52.0);
    malformed.payload = "[]".into();
    engine.consume(&malformed).unwrap();
    engine
        .consume(&record("SOL", 0, 65 * SECOND, 52.0))
        .unwrap();
    assert_eq!(engine.data_anomalies.values().sum::<u64>(), 3);
    assert!(
        engine
            .tick(70 * SECOND)
            .unwrap()
            .iter()
            .all(|r| r.dvol.is_none())
    );
}
