use engine_core::{
    ENGINE_VERSION, Record, SECOND,
    session::{Checkpoint, Cutoff, Queued, Queues, is_carry_in},
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
fn record(ns: i64, offset: i64, feed: &str, kind: &str, payload: Value) -> Record {
    Record::parse(&json!({"exchange":"binance","market":"spot","symbol":"BTCUSDT","feed":feed,"kind":kind,"conn":"A","seq":offset,"recv_ns":ns,"payload":payload.to_string(),"topic":format!("raw.binance.spot.{feed}.v1"),"partition":0,"offset":offset}).to_string()).unwrap()
}
fn queued(ns: i64, offset: i64) -> Queued {
    Queued {
        record: record(ns, offset, "l1", "update", json!({})),
        line: String::new(),
    }
}
#[test]
fn head_of_line_prefix_blocks_clock_step_until_future_head_is_eligible() {
    let mut queues = Queues::default();
    queues.push(queued(SECOND, 0)).unwrap();
    queues.push(queued(3 * SECOND, 1)).unwrap();
    queues.push(queued(SECOND + 1, 2)).unwrap();
    let mut offsets = BTreeMap::new();
    assert_eq!(
        queues
            .take(2 * SECOND, &mut offsets)
            .iter()
            .map(|q| q.record.offset.unwrap())
            .collect::<Vec<_>>(),
        [0]
    );
    assert_eq!(offsets["raw.binance.spot.l1.v1/0"], 0);
    // Membership is offset-prefix; the selected batch is then sorted by receive time.
    assert_eq!(
        queues
            .take(3 * SECOND, &mut offsets)
            .iter()
            .map(|q| q.record.offset.unwrap())
            .collect::<Vec<_>>(),
        [2, 1]
    );
    assert_eq!(offsets["raw.binance.spot.l1.v1/0"], 2);
}
#[test]
fn cutoff_roundtrip_preserves_partition_offsets() {
    let cutoff = Cutoff {
        tick_ns: SECOND,
        engine_version: ENGINE_VERSION.into(),
        processed: BTreeMap::from([("raw.test/0".into(), -1), ("raw.test/1".into(), 123)]),
    };
    assert_eq!(
        cutoff,
        serde_json::from_str(&serde_json::to_string(&cutoff).unwrap()).unwrap()
    );
}
#[test]
fn carry_in_selection_uses_receive_day_not_delivery_time() {
    assert!(is_carry_in(&queued(SECOND - 1, 0).record, SECOND));
    assert!(!is_carry_in(&queued(SECOND, 0).record, SECOND));
    assert!(!is_carry_in(&queued(SECOND + 1, 0).record, SECOND));
}
#[test]
fn state_roundtrip_preserves_books_windows_pending_deltas_and_trade_ids() {
    let mut state = Checkpoint::new(SECOND, BTreeMap::new());
    let snap = record(
        1,
        0,
        "l2",
        "snapshot",
        json!({"lastUpdateId":1,"bids":[["99.00000001","2"]],"asks":[["101","3"]]}),
    );
    state.engine.consume(&snap).unwrap();
    for tick in 1..=310 {
        // Nontrivial floating-point returns exercise exact checkpoint float round-trip.
        let update = record(
            tick * SECOND,
            tick,
            "l2",
            "snapshot",
            json!({"lastUpdateId":tick,"bids":[["99.00000001","2"]],"asks":[[format!("101.{tick:08}"),"3"]]}),
        );
        let cutoff = Cutoff {
            tick_ns: tick * SECOND,
            engine_version: ENGINE_VERSION.into(),
            processed: BTreeMap::new(),
        };
        state.apply(cutoff, vec![update]).unwrap();
    }
    let trade = record(
        311 * SECOND,
        1,
        "trades",
        "trade",
        json!({"data":{"t":42,"q":"123456.12345678","m":false}}),
    );
    state.engine.consume(&trade).unwrap();
    let mut pending = record(
        311 * SECOND,
        99,
        "l2",
        "update",
        json!({"data":{"U":500,"u":500,"b":[],"a":[]}}),
    );
    pending.conn = "B".into();
    state.engine.consume(&pending).unwrap();
    let bytes = serde_json::to_vec(&state).unwrap();
    let mut restored: Checkpoint = serde_json::from_slice(&bytes).unwrap();
    restored.engine.consume(&trade).unwrap();
    state.engine.consume(&trade).unwrap();
    for tick in 311..=315 {
        let cutoff = Cutoff {
            tick_ns: tick * SECOND,
            engine_version: ENGINE_VERSION.into(),
            processed: BTreeMap::new(),
        };
        let a = state.apply(cutoff.clone(), vec![]).unwrap();
        let b = restored.apply(cutoff, vec![]).unwrap();
        assert_eq!(
            serde_json::to_value(a).unwrap(),
            serde_json::to_value(b).unwrap()
        );
    }
}
