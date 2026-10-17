use engine_core::{
    BreakReason, Fixed, Kind, Mid, Outcome, Record, Side, StreamBook, kraken::L3Book,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf};

fn fixture(exchange: &str, market: &str) -> String {
    std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../../tests/fixtures/{exchange}/{market}/l2.jsonl")),
    )
    .unwrap()
}
fn replay(exchange: &str, market: &str) {
    let text = fixture(exchange, market);
    let mut streams = BTreeMap::<String, StreamBook>::new();
    let mut messages = 0;
    let mut absent = 0;
    for (i, line) in text.lines().enumerate() {
        let record = Record::parse(line).unwrap();
        let stream = streams
            .entry(record.symbol.clone())
            .or_insert_with(|| StreamBook::new(&record).unwrap());
        let outcome = stream
            .consume(&record)
            .unwrap_or_else(|e| panic!("{exchange}/{market} line {}: {e}", i + 1));
        assert!(
            !matches!(outcome, Outcome::Break(_)),
            "{exchange}/{market} line {}: {outcome:?}",
            i + 1
        );
        if record.kind != Kind::Control {
            messages += 1;
            if exchange == "okx"
                && serde_json::from_str::<Value>(&record.payload).unwrap()["data"][0]["checksum"]
                    == 0
            {
                absent += 1;
            }
        }
        if stream.valid() {
            let book = stream.book().unwrap();
            if let (Some((bid, _)), Some((ask, _))) = (book.best_bid(), book.best_ask()) {
                assert!(
                    bid < ask,
                    "{exchange}/{market} line {}: crossed {bid} >= {ask}",
                    i + 1
                );
                assert_eq!(
                    book.mid(),
                    Some(Mid((i128::from(bid.0) + i128::from(ask.0)) * 5))
                );
            }
            let bids = book.top(Side::Bid, 10);
            let asks = book.top(Side::Ask, 10);
            assert!(bids.windows(2).all(|w| w[0].0 > w[1].0));
            assert!(asks.windows(2).all(|w| w[0].0 < w[1].0));
        }
    }
    assert_eq!(streams.len(), 2);
    assert!(
        streams
            .values()
            .all(|s| s.counters().breaks == 0 && s.counters().updates_applied > 0)
    );
    if exchange == "okx" || (exchange == "kraken" && market == "spot") {
        let verified = streams
            .values()
            .map(|s| s.counters().checksums_verified)
            .sum::<u64>();
        assert_eq!(verified + absent, messages);
        assert_eq!(
            streams
                .values()
                .map(|s| s.counters().okx_checksum_absent)
                .sum::<u64>(),
            absent
        );
        println!("{exchange}/{market}: {verified} checksums verified, {absent} absent");
    }
}
#[test]
fn binance_spot_fixture() {
    replay("binance", "spot");
}
#[test]
fn binance_perp_fixture() {
    replay("binance", "perp");
}
#[test]
fn okx_spot_fixture() {
    replay("okx", "spot");
}
#[test]
fn okx_perp_fixture() {
    replay("okx", "perp");
}
#[test]
fn kraken_spot_fixture() {
    replay("kraken", "spot");
}
#[test]
fn kraken_perp_fixture() {
    replay("kraken", "perp");
}

fn record(exchange: &str, market: &str, kind: &str, payload: Value) -> Record {
    Record::parse(&json!({"exchange":exchange,"market":market,"feed":"l2","symbol":"BTC","kind":kind,"conn":"A","seq":1,"recv_ns":1,"payload":payload.to_string()}).to_string()).unwrap()
}
fn spot_delta(first: i64, last: i64) -> Record {
    record(
        "binance",
        "spot",
        "update",
        json!({"data":{"U":first,"u":last,"b":[["10",last.to_string()]],"a":[]}}),
    )
}
fn snapshot(last: i64) -> Record {
    record(
        "binance",
        "spot",
        "snapshot",
        json!({"lastUpdateId":last,"bids":[["10","1"]],"asks":[["12","1"]]}),
    )
}
fn consume(stream: &mut StreamBook, r: &Record) -> Outcome {
    stream.consume(r).unwrap()
}
fn control(event: &str, conn: &str) -> Record {
    let mut r = snapshot(0);
    r.kind = Kind::Control;
    r.event = Some(event.into());
    r.conn = conn.into();
    r.payload.clear();
    r
}
#[test]
fn binance_spot_buffer_overlap_repeat_and_gap() {
    let mut stream = StreamBook::new(&snapshot(0)).unwrap();
    for (first, last) in [(1, 5), (6, 10), (11, 15)] {
        assert_eq!(
            consume(&mut stream, &spot_delta(first, last)),
            Outcome::Buffered
        );
    }
    assert_eq!(
        consume(&mut stream, &snapshot(7)),
        Outcome::Snapshot {
            buffered_updates: 2
        }
    );
    assert!(stream.valid());
    assert_eq!(
        stream.book().unwrap().best_bid().unwrap().1,
        "15".parse().unwrap()
    );
    for (first, last) in [(14, 22), (20, 22)] {
        assert_eq!(
            consume(&mut stream, &spot_delta(first, last)),
            Outcome::Applied
        );
    }
    assert_eq!(consume(&mut stream, &spot_delta(10, 12)), Outcome::Ignored);
    assert!(matches!(
        consume(&mut stream, &spot_delta(25, 30)),
        Outcome::Break(BreakReason::SequenceGap { .. })
    ));
    assert!(!stream.valid());
    assert_eq!(stream.connection_count(), 0);
    assert!(stream.book().is_none());
    assert_eq!(stream.counters().breaks, 1);
}
#[test]
fn binance_stale_snapshot_waits_for_retry() {
    let mut stream = StreamBook::new(&snapshot(0)).unwrap();
    consume(&mut stream, &spot_delta(6, 10));
    assert_eq!(consume(&mut stream, &snapshot(3)), Outcome::Stale);
    assert!(!stream.valid());
    assert_eq!(stream.counters().breaks, 0);
    assert_eq!(consume(&mut stream, &spot_delta(11, 12)), Outcome::Buffered);
    assert_eq!(
        consume(&mut stream, &snapshot(10)),
        Outcome::Snapshot {
            buffered_updates: 1
        }
    );
    assert!(stream.valid());
}
#[test]
fn binance_snapshot_before_delta() {
    let mut stream = StreamBook::new(&snapshot(100)).unwrap();
    consume(&mut stream, &snapshot(100));
    assert_eq!(consume(&mut stream, &spot_delta(90, 100)), Outcome::Ignored);
    assert_eq!(consume(&mut stream, &spot_delta(95, 105)), Outcome::Applied);
    assert_eq!(
        consume(&mut stream, &spot_delta(106, 110)),
        Outcome::Applied
    );
    let mut late = StreamBook::new(&snapshot(100)).unwrap();
    consume(&mut late, &snapshot(100));
    assert!(matches!(
        consume(&mut late, &spot_delta(102, 103)),
        Outcome::Break(_)
    ));
}
fn perp_delta(first: i64, last: i64, prev: i64) -> Record {
    record(
        "binance",
        "perp",
        "update",
        json!({"data":{"U":first,"u":last,"pu":prev,"b":[],"a":[]}}),
    )
}
fn perp_snapshot(last: i64) -> Record {
    let mut r = snapshot(last);
    r.market = "perp".into();
    r
}
#[test]
fn binance_perp_pu_chain_and_snapshot_equality() {
    let mut stream = StreamBook::new(&perp_snapshot(0)).unwrap();
    for (f, l, p) in [(1, 4, 0), (5, 9, 4), (10, 12, 9)] {
        consume(&mut stream, &perp_delta(f, l, p));
    }
    assert_eq!(
        consume(&mut stream, &perp_snapshot(7)),
        Outcome::Snapshot {
            buffered_updates: 2
        }
    );
    assert_eq!(
        consume(&mut stream, &perp_delta(13, 15, 12)),
        Outcome::Applied
    );
    assert!(matches!(
        consume(&mut stream, &perp_delta(18, 20, 17)),
        Outcome::Break(_)
    ));
    let mut equal = StreamBook::new(&perp_snapshot(9)).unwrap();
    consume(&mut equal, &perp_delta(5, 9, 4));
    assert_eq!(
        consume(&mut equal, &perp_snapshot(9)),
        Outcome::Snapshot {
            buffered_updates: 1
        }
    );
    assert_eq!(
        consume(&mut equal, &perp_delta(10, 12, 9)),
        Outcome::Applied
    );
}
#[test]
fn binance_perp_stale_and_first_event() {
    let mut stream = StreamBook::new(&perp_snapshot(0)).unwrap();
    consume(&mut stream, &perp_delta(6, 10, 5));
    assert_eq!(consume(&mut stream, &perp_snapshot(3)), Outcome::Stale);
    consume(&mut stream, &perp_delta(11, 12, 10));
    assert_eq!(
        consume(&mut stream, &perp_snapshot(11)),
        Outcome::Snapshot {
            buffered_updates: 1
        }
    );
    let mut empty = StreamBook::new(&perp_snapshot(10)).unwrap();
    consume(&mut empty, &perp_snapshot(10));
    assert_eq!(consume(&mut empty, &perp_delta(1, 9, 0)), Outcome::Ignored);
    assert_eq!(
        consume(&mut empty, &perp_delta(10, 11, 0)),
        Outcome::Applied
    );
}
fn okx(kind: &str, prev: i64, seq: i64) -> Record {
    record(
        "okx",
        "spot",
        kind,
        json!({"data":[{"bids":[],"asks":[],"prevSeqId":prev,"seqId":seq,"checksum":0}]}),
    )
}
#[test]
fn okx_gap_and_unchanged_sequence() {
    let mut stream = StreamBook::new(&okx("snapshot", -1, 10)).unwrap();
    consume(&mut stream, &okx("snapshot", -1, 10));
    assert_eq!(
        consume(&mut stream, &okx("update", 10, 11)),
        Outcome::Applied
    );
    assert_eq!(
        consume(&mut stream, &okx("update", 11, 11)),
        Outcome::Applied
    );
    assert!(matches!(
        consume(&mut stream, &okx("update", 12, 13)),
        Outcome::Break(_)
    ));
    assert!(!stream.valid());
}
#[test]
fn okx_checksum_corruption_invalidates() {
    let mut r = okx("snapshot", -1, 10);
    r.payload = r#"{"data":[{"seqId":10,"bids":[],"asks":[],"checksum":1}]}"#.into();
    let mut stream = StreamBook::new(&r).unwrap();
    assert!(matches!(
        consume(&mut stream, &r),
        Outcome::Break(BreakReason::ChecksumMismatch { .. })
    ));
    assert!(!stream.valid());
    assert_eq!(stream.connection_count(), 0);
    assert!(stream.book().is_none());
}
#[test]
fn futures_sequence_gap() {
    let snap = record(
        "kraken",
        "perp",
        "snapshot",
        json!({"seq":100,"bids":[{"price":10,"qty":1}],"asks":[{"price":12,"qty":1}]}),
    );
    let mut stream = StreamBook::new(&snap).unwrap();
    consume(&mut stream, &snap);
    let delta = |seq| {
        record(
            "kraken",
            "perp",
            "update",
            json!({"seq":seq,"side":"buy","price":10,"qty":0}),
        )
    };
    assert_eq!(consume(&mut stream, &delta(101)), Outcome::Applied);
    assert_eq!(stream.book().unwrap().best_bid(), None);
    assert!(matches!(
        consume(&mut stream, &delta(103)),
        Outcome::Break(_)
    ));
}
#[test]
fn drop_requires_start_and_snapshot() {
    let mut stream = StreamBook::new(&snapshot(1)).unwrap();
    consume(&mut stream, &snapshot(1));
    assert!(stream.valid());
    assert_eq!(stream.connection_count(), 1);
    consume(&mut stream, &control("drop", "A"));
    assert!(!stream.valid());
    assert_eq!(stream.connection_count(), 0);
    assert!(stream.book().is_none());
    assert_eq!(consume(&mut stream, &snapshot(1)), Outcome::Ignored);
    assert_eq!(
        consume(&mut stream, &control("start", "A")),
        Outcome::Ignored
    );
    assert_eq!(stream.connection_count(), 0);
    assert!(!stream.valid());
    consume(&mut stream, &control("start", "B"));
    assert!(!stream.valid());
    let mut snap = snapshot(2);
    snap.conn = "B".into();
    consume(&mut stream, &snap);
    assert!(stream.valid());
    assert_eq!(stream.connection_count(), 1);
    assert_eq!(stream.counters().breaks, 1);
}
#[test]
fn overlap_switches_at_snapshot_and_never_back() {
    let mut stream = StreamBook::new(&snapshot(1)).unwrap();
    consume(&mut stream, &snapshot(1));
    consume(&mut stream, &control("start", "B"));
    assert_eq!(stream.connection_count(), 2);
    assert!(stream.valid());
    assert_eq!(stream.active_connection(), Some("A"));
    let mut delta = spot_delta(2, 3);
    delta.conn = "B".into();
    assert_eq!(consume(&mut stream, &delta), Outcome::Buffered);
    assert_eq!(consume(&mut stream, &spot_delta(2, 2)), Outcome::Applied);
    let mut snap = snapshot(2);
    snap.conn = "B".into();
    consume(&mut stream, &snap);
    assert_eq!(stream.active_connection(), Some("B"));
    assert_eq!(stream.connection_count(), 1);
    assert!(stream.valid());
    for r in [
        spot_delta(3, 4),
        snapshot(100),
        control("drop", "A"),
        control("start", "A"),
    ] {
        assert_eq!(consume(&mut stream, &r), Outcome::Ignored);
        assert_eq!(stream.connection_count(), 1);
    }
    assert_eq!(stream.counters().breaks, 0);
    delta = spot_delta(4, 5);
    delta.conn = "B".into();
    assert_eq!(consume(&mut stream, &delta), Outcome::Applied);
}
fn docs(name: &str) -> Value {
    serde_json::from_str(
        &std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../../tests/fixtures/kraken_docs/{name}_checksum_example.json"
        )))
        .unwrap(),
    )
    .unwrap()
}
#[test]
fn kraken_l2_documented_checksum() {
    let mut r = record("kraken", "spot", "snapshot", docs("l2"));
    r.symbol = "BTC/USD".into();
    let mut stream = StreamBook::new(&r).unwrap();
    consume(&mut stream, &r);
    assert!(stream.valid());
    assert_eq!(stream.counters().checksums_verified, 1);
}
#[test]
fn kraken_l3_documented_checksum() {
    let data = docs("l3");
    let mut book = L3Book::new(100);
    book.apply(&data["data"][0], true).unwrap();
    assert_eq!(book.checksum(), 1063832831);
    let mut r = record("kraken", "spot", "snapshot", data);
    r.feed = "l3".into();
    let mut stream = StreamBook::new(&r).unwrap();
    consume(&mut stream, &r);
    assert!(stream.valid());
}
#[test]
fn kraken_l3_queue_modify_delete_and_truncate() {
    let mut book = L3Book::new(2);
    book.apply(&json!({"bids":[{"order_id":"a","limit_price":"10.0","order_qty":"1.0"},{"order_id":"b","limit_price":"10.0","order_qty":"2.0"},{"order_id":"c","limit_price":"9.0","order_qty":"1.0"}]}),true).unwrap();
    book.apply(&json!({"bids":[{"event":"modify","order_id":"a","order_qty":"0.5"},{"event":"add","order_id":"d","limit_price":"8.0","order_qty":"1.0"}]}),false).unwrap();
    let p = "10".parse().unwrap();
    let orders = book.orders_at(Side::Bid, p);
    assert_eq!(
        orders,
        vec![("a", "0.5".parse().unwrap()), ("b", "2".parse().unwrap())]
    );
    assert_eq!(book.book().unwrap().top(Side::Bid, 10).len(), 2);
    book.apply(
        &json!({"bids":[{"event":"delete","order_id":"c","limit_price":"9.0"}]}),
        false,
    )
    .unwrap();
    assert_eq!(book.book().unwrap().top(Side::Bid, 10).len(), 1);
    // Unknown modify is add, and delete uses the stored side/price.
    book.apply(&json!({"asks":[{"event":"modify","order_id":"e","limit_price":"11.0","order_qty":"1.0"},{"event":"delete","order_id":"a"}]}),false).unwrap();
    assert_eq!(
        book.orders_at(Side::Bid, p),
        vec![("b", "2".parse().unwrap())]
    );
    assert_eq!(
        book.book().unwrap().best_ask().unwrap().0,
        "11".parse().unwrap()
    );
}
#[test]
fn fixed_precision_overflow_and_exponents() {
    for (text, atoms) in [
        ("0", 0),
        ("1.23000000", 123000000),
        ("-0.00000001", -1),
        ("1e-8", 1),
        ("1.20e2", 12000000000),
        ("92233720368.54775807", i64::MAX),
        ("-92233720368.54775808", i64::MIN),
    ] {
        assert_eq!(text.parse::<Fixed>().unwrap(), Fixed(atoms));
    }
    for text in [
        "1.000000000",
        "0.000000001",
        "92233720368.54775808",
        "-92233720368.54775809",
        "NaN",
        "1e999999",
        "1.2.3",
        "",
        ".1",
        "1.",
    ] {
        assert!(text.parse::<Fixed>().is_err(), "{text}");
    }
}
#[test]
fn record_coordinates_and_timestamp_domains() {
    for recv in [i64::MIN as i128, u64::MAX as i128] {
        let line = format!(
            r#"{{"exchange":"kraken","market":"spot","feed":"l2","symbol":"BTC/USD","kind":"control","event":"drop","conn":"A","seq":1,"recv_ns":{recv},"reason":"disconnect","detail":{{"got":1}}}}"#
        );
        let rec = Record::parse(&line).unwrap();
        assert_eq!(rec.recv_ns, recv);
        assert!(rec.topic.is_none());
        assert!(rec.payload.is_empty());
        let mut v: Value = serde_json::from_str(&line).unwrap();
        v["topic"] = json!("raw.kraken.spot.l2.v1");
        v["partition"] = json!(0);
        v["offset"] = json!(123);
        let rec = Record::parse(&v.to_string()).unwrap();
        assert_eq!(rec.offset, Some(123));
    }
}
#[test]
fn exact_half_atom_midpoint() {
    let r = record(
        "binance",
        "spot",
        "snapshot",
        json!({"lastUpdateId":1,"bids":[["0.00000001","1"]],"asks":[["0.00000002","1"]]}),
    );
    let mut stream = StreamBook::new(&r).unwrap();
    consume(&mut stream, &r);
    assert_eq!(stream.book().unwrap().mid(), Some(Mid(15)));
}

/// End-to-end archived line parsing + book/checksum work; file reads are outside the timer.
#[test]
#[ignore = "release-mode timing: cargo test --release timing -- --ignored --nocapture"]
fn timing() {
    let n = std::env::var("ENGINE_BENCH_N")
        .ok()
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(100);
    assert!(n > 0);
    for exchange in ["binance", "kraken", "okx"] {
        if std::env::var("ENGINE_BENCH_EXCHANGE").is_ok_and(|selected| selected != exchange) {
            continue;
        }
        let inputs = [fixture(exchange, "spot"), fixture(exchange, "perp")];
        let count = inputs.iter().map(|s| s.lines().count()).sum::<usize>() * n;
        let start = std::time::Instant::now();
        let mut market_ns = [0u128; 2];
        for _ in 0..n {
            for (market, input) in inputs.iter().enumerate() {
                let market_start = std::time::Instant::now();
                let mut streams = BTreeMap::<String, StreamBook>::new();
                for line in input.lines() {
                    let r = Record::parse(line).unwrap();
                    let stream = streams
                        .entry(r.symbol.clone())
                        .or_insert_with(|| StreamBook::new(&r).unwrap());
                    let outcome = stream.consume(&r).unwrap();
                    assert!(!matches!(outcome, Outcome::Break(_)));
                }
                std::hint::black_box(streams);
                market_ns[market] += market_start.elapsed().as_nanos();
            }
        }
        for (i, market) in ["spot", "perp"].into_iter().enumerate() {
            let messages = inputs[i].lines().count() * n;
            println!(
                "{exchange}/{market}: {:.0} ns/msg",
                market_ns[i] as f64 / messages as f64
            );
        }
        let elapsed = start.elapsed().as_secs_f64();
        println!(
            "{exchange}: {count} msgs / {elapsed:.3}s = {:.0} msgs/s, {:.0} ns/msg (envelope + payload + book + checksum)",
            count as f64 / elapsed,
            elapsed * 1e9 / count as f64
        );
    }
}

#[test]
fn okx_signed_checksum_uneven_sides_and_delete() {
    // Reference text independently cross-checked with Python zlib.crc32.
    let checksum = crc32fast::hash(b"10:1:12:3:9:2") as i32;
    assert_eq!(checksum, 1542503640);
    let snap = record(
        "okx",
        "spot",
        "snapshot",
        json!({"data":[{"seqId":10,"checksum":checksum,"bids":[["9","2"],["10","1"]],"asks":[["12","3"]]}]}),
    );
    let mut stream = StreamBook::new(&snap).unwrap();
    consume(&mut stream, &snap);
    assert!(stream.valid());
    let update = record(
        "okx",
        "spot",
        "update",
        json!({"data":[{"prevSeqId":10,"seqId":11,"checksum":-1533969673i64,"bids":[["10","0"]],"asks":[]}]}),
    );
    assert_eq!(consume(&mut stream, &update), Outcome::Applied);
    assert_eq!(
        stream.book().unwrap().best_bid().unwrap().0,
        "9".parse().unwrap()
    );
    assert_eq!(stream.counters().checksums_verified, 2);
}
#[test]
fn okx_preserves_price_key_and_latest_size_text() {
    let snap = record(
        "okx",
        "spot",
        "snapshot",
        json!({"data":[{"seqId":10,"checksum":861703255,"bids":[["10.0","2.00"]],"asks":[["12.0","1.00"]]}]}),
    );
    let mut stream = StreamBook::new(&snap).unwrap();
    consume(&mut stream, &snap);
    let update = record(
        "okx",
        "spot",
        "update",
        json!({"data":[{"prevSeqId":10,"seqId":11,"checksum":1371390826,"bids":[["10.00","3.0"]],"asks":[]}]}),
    );
    assert_eq!(consume(&mut stream, &update), Outcome::Applied);
    assert!(stream.valid());
}
#[test]
fn okx_requires_snapshot_before_updates() {
    let r = okx("update", 10, 11);
    let mut stream = StreamBook::new(&r).unwrap();
    assert_eq!(
        consume(&mut stream, &r),
        Outcome::Break(BreakReason::MissingSnapshot)
    );
}
#[test]
fn malformed_decimal_invalidates_without_rounding() {
    let mut stream = StreamBook::new(&snapshot(1)).unwrap();
    consume(&mut stream, &snapshot(1));
    let r = record(
        "binance",
        "spot",
        "update",
        json!({"data":{"U":2,"u":2,"b":[["10","1.000000001"]],"a":[]}}),
    );
    assert!(stream.consume(&r).is_err());
    assert!(!stream.valid());
    assert_eq!(stream.connection_count(), 0);
    assert!(stream.book().is_none());
    assert!(matches!(
        stream.last_break(),
        Some(BreakReason::Malformed(_))
    ));
}
#[test]
fn kraken_checksum_corruption_invalidates() {
    let mut payload = docs("l2");
    payload["data"][0]["checksum"] = json!(1);
    let r = record("kraken", "spot", "snapshot", payload);
    let mut stream = StreamBook::new(&r).unwrap();
    assert!(matches!(
        consume(&mut stream, &r),
        Outcome::Break(BreakReason::ChecksumMismatch { .. })
    ));
    assert!(!stream.valid());
}
#[test]
fn binance_buffered_chain_gap_is_stale_without_break() {
    let mut stream = StreamBook::new(&snapshot(0)).unwrap();
    for (u, last) in [(6, 10), (12, 15)] {
        consume(&mut stream, &spot_delta(u, last));
    }
    assert_eq!(consume(&mut stream, &snapshot(7)), Outcome::Stale);
    assert_eq!(stream.counters().breaks, 0);
    consume(&mut stream, &spot_delta(16, 20));
    assert_eq!(
        consume(&mut stream, &snapshot(15)),
        Outcome::Snapshot {
            buffered_updates: 1
        }
    );
    assert!(stream.valid());
    let mut perp = StreamBook::new(&perp_snapshot(0)).unwrap();
    consume(&mut perp, &perp_delta(5, 9, 4));
    consume(&mut perp, &perp_delta(10, 12, 8));
    assert_eq!(consume(&mut perp, &perp_snapshot(7)), Outcome::Stale);
    assert_eq!(perp.counters().breaks, 0);
}

#[test]
fn okx_checksum_uses_top_25_levels_only() {
    // Both constants independently cross-checked with Python zlib.crc32.
    let fields = (0..25)
        .flat_map(|i| {
            [
                (30 - i).to_string(),
                "1".into(),
                (31 + i).to_string(),
                "2".into(),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        crc32fast::hash(fields.join(":").as_bytes()) as i32,
        1323080728
    );
    let bids = (1..=30)
        .map(|p| json!([p.to_string(), "1"]))
        .collect::<Vec<_>>();
    let asks = (31..=60)
        .rev()
        .map(|p| json!([p.to_string(), "2"]))
        .collect::<Vec<_>>();
    let snap = record(
        "okx",
        "spot",
        "snapshot",
        json!({"data":[{"seqId":10,"checksum":1323080728,"bids":bids,"asks":asks}]}),
    );
    let mut stream = StreamBook::new(&snap).unwrap();
    consume(&mut stream, &snap);
    assert!(stream.valid());
    let outside = record(
        "okx",
        "spot",
        "update",
        json!({"data":[{"prevSeqId":10,"seqId":11,"checksum":1323080728,"bids":[["1","9"]],"asks":[["60","9"]]}]}),
    );
    assert_eq!(consume(&mut stream, &outside), Outcome::Applied);
    let boundary = record(
        "okx",
        "spot",
        "update",
        json!({"data":[{"prevSeqId":11,"seqId":12,"checksum":51658003,"bids":[["6","3"]],"asks":[]}]}),
    );
    assert_eq!(consume(&mut stream, &boundary), Outcome::Applied);
    assert_eq!(stream.counters().checksums_verified, 3);
}
#[test]
fn okx_zero_checksum_is_absent_but_continuity_is_required() {
    let snap = record(
        "okx",
        "spot",
        "snapshot",
        json!({"data":[{"seqId":10,"checksum":0,"bids":[["10","1"]],"asks":[["12","1"]]}]}),
    );
    let mut stream = StreamBook::new(&snap).unwrap();
    consume(&mut stream, &snap);
    assert!(stream.valid());
    assert_eq!(stream.counters().okx_checksum_absent, 1);
    assert_eq!(stream.counters().checksums_verified, 0);
    assert_eq!(
        consume(&mut stream, &okx("update", 10, 11)),
        Outcome::Applied
    );
    assert_eq!(stream.counters().okx_checksum_absent, 2);
    assert!(matches!(
        consume(&mut stream, &okx("update", 12, 13)),
        Outcome::Break(BreakReason::SequenceGap { .. })
    ));
    assert_eq!(stream.counters().okx_checksum_absent, 2);
    assert!(!stream.valid());
}

#[test]
fn ended_connections_release_active_books_and_pending_deltas() {
    for event in ["stop", "drop"] {
        let mut stream = StreamBook::new(&snapshot(1)).unwrap();
        consume(&mut stream, &snapshot(1));
        let mut pending = spot_delta(2, 3);
        pending.conn = "B".into();
        assert_eq!(consume(&mut stream, &pending), Outcome::Buffered);
        assert_eq!(stream.connection_count(), 2);
        consume(&mut stream, &control(event, "B"));
        assert_eq!(stream.connection_count(), 1);
        assert!(stream.valid());
        assert_eq!(stream.active_connection(), Some("A"));
        assert_eq!(consume(&mut stream, &pending), Outcome::Ignored);
        assert_eq!(
            consume(&mut stream, &control("start", "B")),
            Outcome::Ignored
        );
        assert_eq!(stream.connection_count(), 1);
        consume(&mut stream, &control(event, "A"));
        assert_eq!(stream.connection_count(), 0);
        assert!(!stream.valid());
        assert!(stream.book().is_none());
        assert_eq!(consume(&mut stream, &snapshot(1)), Outcome::Ignored);
        assert_eq!(stream.connection_count(), 0);
    }
}
