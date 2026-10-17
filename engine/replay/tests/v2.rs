use engine_core::{Kind, Outcome, Side, StreamBook};
use engine_replay::input::{Merge, StreamPath, discover_filtered};
use std::path::PathBuf;

fn fixtures(variant: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/fixtures/v2/{variant}"))
}

#[test]
fn exact_decimal_strings_and_number_tokens_produce_identical_books_and_crcs() {
    let mut books = Vec::new();
    for variant in ["strings", "numbers"] {
        let mut merge = Merge::new(
            discover_filtered(&fixtures(variant), "2026-10-16", false, None, None).unwrap(),
        )
        .unwrap();
        let mut streams = std::collections::BTreeMap::new();
        while let Some(record) = merge.pop().unwrap() {
            let key = (
                record.exchange.clone(),
                record.market.clone(),
                record.feed.clone(),
            );
            let stream = streams
                .entry(key)
                .or_insert_with(|| StreamBook::new(&record).unwrap());
            let outcome = stream.consume(&record).unwrap();
            assert!(!matches!(outcome, Outcome::Break(_)), "{outcome:?}");
        }
        assert_eq!(streams.len(), 5);
        books.push(
            streams
                .into_iter()
                .map(|(key, stream)| {
                    assert!(stream.valid());
                    if key.0 == "kraken" && key.1 == "spot" {
                        assert_eq!(stream.counters().checksums_verified, 2);
                    }
                    let book = stream.book().unwrap();
                    (
                        key,
                        book.top(Side::Bid, 10),
                        book.top(Side::Ask, 10),
                        stream.counters().checksums_verified,
                    )
                })
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(books[0], books[1]);
}

#[test]
fn path_metadata_markers_kind_inference_and_v1_rejection() {
    let path = StreamPath::new(&PathBuf::from(
        "raw/okx/perp/ETH-USDT-SWAP/20261016_l2.jsonl",
    ))
    .unwrap();
    let record = path.parse(r#"{"marker":"ws_event","event":"connected","conn":"A","offset":0,"recv_time_ns":1,"instrument":{"ctVal":"0.01"},"ack":{"result":{"depth":10}}}"#).unwrap().unwrap();
    assert_eq!(record.kind, Kind::Control);
    assert_eq!(record.event.as_deref(), Some("start"));
    assert_eq!(record.partition, Some(1));
    assert_eq!(record.symbol, "ETH-USDT-SWAP");
    assert_eq!(record.topic.as_deref(), Some("raw.okx.perp.l2.v1"));
    assert_eq!(record.detail.unwrap()["instrument"]["ctVal"], "0.01");
    for (event, expected, reason) in [
        ("disconnected", "stop", None),
        ("connection_error", "drop", Some("disconnect")),
    ] {
        let mut v = serde_json::json!({"marker":"ws_event","event":event,"conn":"A","offset":1,"recv_time_ns":2});
        if event == "connection_error" {
            v["reason"] = serde_json::json!("disconnect");
        }
        let r = path.parse(&v.to_string()).unwrap().unwrap();
        assert_eq!(r.event.as_deref(), Some(expected));
        assert_eq!(r.reason.as_deref(), reason);
    }
    assert!(
        path.parse(r#"{"marker":"rollover","new_period":"20261017"}"#)
            .unwrap()
            .is_none()
    );
    assert!(
        path.parse(r#"{"marker":"gap","offset":2,"recv_time_ns":2}"#)
            .unwrap()
            .is_none()
    );
    assert!(
        path.parse(r#"{"exchange":"okx","recv_ns":1,"payload":"{}"}"#)
            .is_err()
    );
    assert!(
        path.parse(r#"{"_recv_time_ns":1,"_conn":"A","_offset":0,"data":[]}"#)
            .is_err()
    );
    let bad = path
        .parse(r#"{"recv_time_ns":1,"conn":"A","offset":2,"raw":"not JSON"}"#)
        .unwrap()
        .unwrap();
    assert_eq!(bad.kind, Kind::Control);
    assert_eq!(bad.event.as_deref(), Some("drop"));
    assert_eq!(bad.reason.as_deref(), Some("malformed_envelope"));
    assert_eq!(bad.offset, Some(2));
    assert_eq!(bad.payload, "");
    let spot =
        StreamPath::new(&PathBuf::from("raw/kraken/spot/BTC-USD/20261016_l2.jsonl")).unwrap();
    let fixture_text =
        std::fs::read_to_string(fixtures("strings").join("kraken/spot/BTC-USD/20261016_l2.jsonl"))
            .unwrap();
    let mut fixture = fixture_text.lines();
    let start = spot.parse(fixture.next().unwrap()).unwrap().unwrap();
    let snapshot = spot.parse(fixture.next().unwrap()).unwrap().unwrap();
    let mut book = StreamBook::new(&start).unwrap();
    book.consume(&start).unwrap();
    book.consume(&snapshot).unwrap();
    assert!(book.valid());
    let bad = spot
        .parse(r#"{"recv_time_ns":3,"conn":"A","offset":3,"raw":"[truncated"}"#)
        .unwrap()
        .unwrap();
    let mut engine = engine_replay::features::Engine::default();
    engine.consume(&start).unwrap();
    engine.consume(&snapshot).unwrap();
    engine.consume(&bad).unwrap();
    assert_eq!(engine.book_break_reasons["malformed"], 1);
    for (ex, market, symbol, feed, native, expected) in [
        (
            "kraken",
            "spot",
            "BTC-USD",
            "l2",
            r#""type":"snapshot""#,
            Kind::Snapshot,
        ),
        (
            "kraken",
            "spot",
            "BTC-USD",
            "trades",
            r#""type":"update""#,
            Kind::Trade,
        ),
        (
            "kraken",
            "perp",
            "PF_XBTUSD",
            "trades",
            r#""feed":"trade_snapshot""#,
            Kind::Snapshot,
        ),
        (
            "kraken",
            "perp",
            "PF_XBTUSD",
            "ticker",
            r#""feed":"ticker""#,
            Kind::Update,
        ),
        (
            "binance",
            "spot",
            "BTCUSDT",
            "l2",
            r#""data":{"e":"depthUpdate"}"#,
            Kind::Update,
        ),
        (
            "binance",
            "spot",
            "BTCUSDT",
            "l2",
            r#""type":"snapshot""#,
            Kind::Snapshot,
        ),
        (
            "okx",
            "spot",
            "BTC-USDT",
            "l2",
            r#""action":"snapshot""#,
            Kind::Snapshot,
        ),
    ] {
        let path = StreamPath::new(&PathBuf::from(format!(
            "raw/{ex}/{market}/{symbol}/20261016_{feed}.jsonl"
        )))
        .unwrap();
        let line = format!("{{{native},\"conn\":\"A\",\"offset\":0,\"recv_time_ns\":1}}");
        let r = path.parse(&line).unwrap().unwrap();
        assert_eq!(r.kind, expected);
        assert_eq!(r.payload, line); // Archive keys do not require payload serialization.
    }
    assert_eq!(
        discover_filtered(
            &fixtures("strings"),
            "2026-10-16",
            false,
            Some("kraken"),
            Some("spot")
        )
        .unwrap()
        .len(),
        2
    );
}

#[test]
fn backward_step_across_files_keeps_one_partition_in_offset_order() {
    let root = PathBuf::from(
        std::env::var_os("ENGINE_TEST_SCRATCH").unwrap_or_else(|| std::env::temp_dir().into()),
    )
    .join(format!("v2-multiday-{}", std::process::id()));
    let parent = root.join("raw/binance/spot/BTCUSDT");
    std::fs::create_dir_all(&parent).unwrap();
    let first = parent.join("20261016_trades.jsonl");
    let second = parent.join("20261017_trades.jsonl");
    std::fs::write(
        &first,
        "{\"conn\":\"A\",\"recv_time_ns\":20,\"offset\":0}\n",
    )
    .unwrap();
    std::fs::write(
        &second,
        "{\"conn\":\"A\",\"recv_time_ns\":5,\"offset\":1}\n",
    )
    .unwrap();
    let mut merge = Merge::new(vec![second, first]).unwrap();
    assert_eq!(merge.pop().unwrap().unwrap().offset, Some(0));
    assert_eq!(merge.pop().unwrap().unwrap().offset, Some(1));
    assert!(merge.pop().unwrap().is_none());
    assert_eq!(merge.clock_steps(), 1);
}
