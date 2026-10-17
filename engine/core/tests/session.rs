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
    // Only partition heads compete; the older timestamp stays behind its predecessor.
    assert_eq!(
        queues
            .take(3 * SECOND, &mut offsets)
            .iter()
            .map(|q| q.record.offset.unwrap())
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(offsets["raw.binance.spot.l1.v1/0"], 2);
}

#[test]
fn backward_step_keeps_book_continuity_and_late_flow_in_current_tick() {
    let mut state = Checkpoint::new(SECOND, BTreeMap::new());
    let book = |ns, offset, snapshot, seq, price| {
        let mut r = record(
            ns,
            offset,
            "l2",
            if snapshot { "snapshot" } else { "update" },
            if snapshot {
                json!({"seq":seq,"bids":[{"price":price,"qty":1}],"asks":[{"price":102,"qty":1}]})
            } else {
                json!({"seq":seq,"side":"buy","price":price,"qty":1})
            },
        );
        r.exchange = "kraken".into();
        r.market = "perp".into();
        r.symbol = "PF_XBTUSD".into();
        r.topic = Some("raw.kraken.perp.l2.v1".into());
        r
    };
    let cutoff = |tick_ns| Cutoff {
        tick_ns,
        engine_version: ENGINE_VERSION.into(),
        processed: BTreeMap::new(),
    };
    state
        .apply(cutoff(SECOND), vec![book(1, 0, true, 10, 99)])
        .unwrap();
    state.apply(cutoff(2 * SECOND), vec![]).unwrap();
    let trade = record(
        SECOND + 1,
        0,
        "trades",
        "trade",
        json!({"data":{"t":1,"q":"0.1","m":false}}),
    );
    let rows = state
        .apply(
            cutoff(3 * SECOND),
            vec![
                book(3 * SECOND - 1, 1, false, 11, 100),
                book(SECOND + 1, 2, false, 12, 101),
                trade,
            ],
        )
        .unwrap();
    let kraken = rows.iter().find(|r| r.exchange == "kraken").unwrap();
    assert!(kraken.book_valid);
    assert_eq!(kraken.best_bid, Some(101.0));
    assert_eq!(state.engine.book_breaks, 0);
    let binance = rows.iter().find(|r| r.exchange == "binance").unwrap();
    assert_eq!(binance.trade_count, Some(1));
    assert_eq!(binance.volume_base, Some(0.1));
}

#[test]
fn no_step_head_merge_equals_global_sort() {
    let mut queues = Queues::default();
    let mut expected = Vec::new();
    for (feed, partition, times) in [
        ("l1", 0, vec![1, 4, 4, 9]),
        ("l1", 1, vec![2, 4, 8]),
        ("trades", 0, vec![1, 4, 7]),
    ] {
        for (offset, ns) in times.into_iter().enumerate() {
            let mut q = queued(ns, offset as i64);
            q.record.feed = feed.into();
            q.record.topic = Some(format!("raw.binance.spot.{feed}.v1"));
            q.record.partition = Some(partition);
            expected.push((
                q.record.recv_ns,
                q.record.topic.clone(),
                q.record.partition,
                q.record.offset,
            ));
            queues.push(q).unwrap();
        }
    }
    expected.sort();
    let actual = queues
        .take(SECOND, &mut BTreeMap::new())
        .into_iter()
        .map(|q| {
            (
                q.record.recv_ns,
                q.record.topic,
                q.record.partition,
                q.record.offset,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
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
