#[test]
fn carry_in_line_matches_recorder_and_preserves_envelope_bytes() {
    let value=br#"{"v":1,"exchange":"okx","market":"spot","feed":"l2","symbol":"BTC-USDT","kind":"control","event":"start","conn":"A","seq":0,"recv_ns":1}"#;
    let queued = engine_live::archived(value, "raw.okx.spot.l2.v1", 0, 42).unwrap();
    assert_eq!(
        queued.line,
        format!(
            "{},\"topic\":\"raw.okx.spot.l2.v1\",\"partition\":0,\"offset\":42}}\n",
            std::str::from_utf8(&value[..value.len() - 1]).unwrap()
        )
    );
    assert_eq!(queued.record.offset, Some(42));
}

#[test]
fn malformed_outer_envelope_keeps_offset_and_invalidates_without_exiting() {
    use engine_core::{SECOND, features::Engine};
    let bad = br#"{"exchange":"okx","market":"spot","feed":"l2","symbol":"BTC-USDT","kind":"bogus","conn":"A","recv_ns":1000000001}"#;
    let queued = engine_live::archived(bad, "raw.okx.spot.l2.v1", 0, 7).unwrap();
    assert_eq!(queued.record.reason.as_deref(), Some("malformed_envelope"));
    assert_eq!(queued.record.offset, Some(7));
    let recovered = engine_replay::input::recover_archived(&queued.line, None).unwrap();
    assert_eq!(recovered.reason, queued.record.reason);
    assert_eq!(recovered.recv_ns, queued.record.recv_ns);
    let mut engine = Engine::default();
    engine.consume(&recovered).unwrap();
    assert_eq!(engine.data_anomalies.values().sum::<u64>(), 1);
    assert!(
        !engine
            .tick(2 * SECOND)
            .unwrap()
            .iter()
            .find(|r| r.exchange == "okx")
            .unwrap()
            .book_valid
    );
}

#[test]
fn non_json_outer_envelope_still_advances_kafka_offset() {
    let queued = engine_live::archived(b"broken}", "raw.binance.spot.trades.v1", 1, 8).unwrap();
    assert_eq!(queued.record.reason.as_deref(), Some("malformed_envelope"));
    assert_eq!(queued.record.symbol, "ETHUSDT");
    assert_eq!(queued.record.offset, Some(8));
    let recovered = engine_replay::input::recover_archived(&queued.line, None).unwrap();
    assert_eq!(recovered.offset, queued.record.offset);
    let mut engine = engine_core::features::Engine::default();
    engine.consume(&recovered).unwrap();
    let row = engine
        .tick(engine_core::SECOND)
        .unwrap()
        .into_iter()
        .find(|r| r.exchange == "binance")
        .unwrap();
    assert_eq!((row.trade_count, row.volume_base), (None, None));
}
