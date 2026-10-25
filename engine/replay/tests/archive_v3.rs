//! Native v3 archive replay and exact-decimal fixture tests.
use engine_core::{Kind, Outcome, Side, StreamBook};
use engine_replay::input::{Merge, StreamPath, discover_filtered};
use serde_json::json;
use std::path::PathBuf;
fn fixtures(variant: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/fixtures/v3/{variant}"))
}
fn scratch(name: &str) -> PathBuf {
    PathBuf::from(
        std::env::var_os("ENGINE_TEST_SCRATCH").unwrap_or_else(|| std::env::temp_dir().into()),
    )
    .join(format!("v3-{name}-{}", std::process::id()))
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
fn native_shapes_channel_mapping_and_lenient_paths() {
    for (ex, market, symbol, channel, native, kind, feed) in [
        (
            "BiNaNcERaw",
            "Spotv6",
            "USDTUSD",
            "depth",
            json!({"U":1,"u":2}),
            Kind::Update,
            "l2",
        ),
        (
            "Binance",
            "Futures",
            "SOLFDUSD",
            "snapshot",
            json!({"lastUpdateId":2}),
            Kind::Snapshot,
            "l2",
        ),
        (
            "Binance",
            "Perpetual",
            "SOLUSDC",
            "bookticker",
            json!({}),
            Kind::Update,
            "l1",
        ),
        (
            "Binance",
            "Swap",
            "SOLUSD",
            "aggtrades",
            json!({}),
            Kind::Trade,
            "trades_agg",
        ),
        (
            "Binance",
            "Futures",
            "SOLBTC",
            "markprice",
            json!({}),
            Kind::Update,
            "mark",
        ),
        (
            "Binance",
            "Futures",
            "SOLETH",
            "forceorder",
            json!({}),
            Kind::Update,
            "liquidations",
        ),
        (
            "OKX",
            "Spot",
            "USDT-USD",
            "book",
            json!({"action":"snapshot","data":[]}),
            Kind::Snapshot,
            "l2",
        ),
        (
            "OKX",
            "Spot",
            "USDT-USD",
            "bbo",
            json!({"data":[]}),
            Kind::Update,
            "l1",
        ),
        (
            "OKX",
            "Spot",
            "USDT-USD",
            "trades",
            json!({"data":[]}),
            Kind::Trade,
            "trades_agg",
        ),
        (
            "OKX",
            "Spot",
            "USDT-USD",
            "tradesall",
            json!({"data":[]}),
            Kind::Trade,
            "trades",
        ),
        (
            "Kraken",
            "Spotv6",
            "USDT-USD",
            "book",
            json!({"channel":"book","type":"snapshot","data":[]}),
            Kind::Snapshot,
            "l2",
        ),
        (
            "Kraken",
            "Spotv6",
            "USDT-USD",
            "level3",
            json!({"channel":"level3","type":"snapshot","data":[]}),
            Kind::Snapshot,
            "l3",
        ),
        (
            "Kraken",
            "Spotv6",
            "USDT-USD",
            "trades",
            json!({"channel":"trade","type":"update","data":[]}),
            Kind::Trade,
            "trades",
        ),
        (
            "KrakenRaw",
            "Futurev6",
            "PF_XBTUSD",
            "book",
            json!({"feed":"book_snapshot"}),
            Kind::Snapshot,
            "l2",
        ),
        (
            "KrakenRaw",
            "Futurev6",
            "PF_XBTUSD",
            "trades",
            json!({"feed":"trade_snapshot"}),
            Kind::Snapshot,
            "trades",
        ),
        (
            "KrakenRaw",
            "Futurev6",
            "PF_XBTUSD",
            "ticker",
            json!({"feed":"ticker"}),
            Kind::Update,
            "ticker",
        ),
    ] {
        let path = StreamPath::new(&PathBuf::from(format!(
            "raw/{ex}/{market}/{symbol}/20261016_{channel}.jsonl"
        )))
        .unwrap();
        let spot = path.exchange == "kraken" && path.market == "spot";
        let mut line = if spot {
            native.clone()
        } else {
            json!({"data":native})
        };
        line["recv_time_ns"] = json!(1);
        line["conn"] = json!("explicit");
        line["offset"] = json!(7);
        let r = path.parse(&line.to_string()).unwrap().unwrap();
        assert_eq!(r.kind, kind);
        assert_eq!(r.feed, feed);
        assert_eq!(r.conn, "explicit");
        assert_eq!(r.offset, Some(7));
        let payload: serde_json::Value = serde_json::from_str(&r.payload).unwrap();
        if spot {
            assert_eq!(payload, line);
        } else {
            assert_eq!(payload, native);
        }
    }
    let path = StreamPath::new(&PathBuf::from(
        "raw/OKX/Swap/ETH-USDT-SWAP/20261016_book.jsonl",
    ))
    .unwrap();
    for (event, expected) in [
        ("connected", "start"),
        ("disconnected", "stop"),
        ("rotating", "stop"),
        ("shutdown", "stop"),
        ("connection_error", "drop"),
        ("corrupt_message", "corrupt_message"),
    ] {
        let line = json!({"marker":"ws_event","event":event,"conn":"A","offset":0,"timestamp":"2026-10-16T00:00:01.123456789Z","instrument":{"ctVal":"0.01"}});
        let r = path.parse(&line.to_string()).unwrap().unwrap();
        assert_eq!(r.event.as_deref(), Some(expected));
        assert_eq!(
            r.recv_ns,
            i128::from(engine_replay::input::day_bounds("2026-10-16").unwrap().0) + 1_123_456_789
        );
        assert_eq!(r.detail.unwrap()["instrument"]["ctVal"], "0.01");
    }
    for line in [r#"{"marker":"rollover"}"#, r#"{"marker":"gap"}"#] {
        assert!(path.parse(line).unwrap().is_none());
    }
    let raw = path
        .parse(r#"{"recv_time_ns":1,"raw":"broken"}"#)
        .unwrap()
        .unwrap();
    assert_eq!(raw.reason.as_deref(), Some("malformed_envelope"));
    for line in [
        r#"{"exchange":"okx","recv_ns":1,"payload":"{}"}"#,
        r#"{"_recv_time_ns":1,"data":[]}"#,
    ] {
        assert!(path.parse(line).is_err());
    }
}
#[test]
fn synthetic_epochs_line_offsets_marker_times_and_head_only_order() {
    let root = scratch("epochs");
    let parent = root.join("Kraken/Spotv6/USDT-USD");
    std::fs::create_dir_all(&parent).unwrap();
    let file = parent.join("20261016_trades.jsonl");
    let data = |time| json!({"recv_time_ns":time,"channel":"trade","data":[]}).to_string();
    let start = r#"{"marker":"ws_event","event":"connected","conn":"label","timestamp":"1970-01-01T00:00:00.000000020+00:00"}"#;
    std::fs::write(
        &file,
        format!(
            "{}\n{}\n{}\n{{\"marker\":\"gap\"}}\n{}\n{}\n",
            data(10),
            start,
            data(5),
            start,
            data(30)
        ),
    )
    .unwrap();
    let mut merge = Merge::new(vec![file.clone()]).unwrap();
    let mut records = Vec::new();
    while let Some(r) = merge.pop().unwrap() {
        records.push(r);
    }
    assert_eq!(
        records
            .iter()
            .map(|r| r.offset.unwrap())
            .collect::<Vec<_>>(),
        [0, 1, 2, 4, 5]
    );
    assert_eq!(records[0].conn, format!("{}#pre", file.display()));
    assert_eq!(records[1].conn, "label#1");
    assert_eq!(records[2].conn, "label#1");
    assert_eq!(records[3].conn, "label#2");
    assert_eq!(records[4].conn, "label#2");
    assert_eq!(
        records.iter().map(|r| r.recv_ns).collect::<Vec<_>>(),
        [10, 20, 5, 20, 30]
    );
    assert_eq!(merge.clock_steps(), 1);
    let second = parent.join("20261017_trades.jsonl");
    std::fs::write(&second, format!("{}\n", data(1))).unwrap();
    let mut merge = Merge::new(vec![file, second]).unwrap();
    assert_eq!(merge.pop().unwrap().unwrap().recv_ns, 1);
}
#[test]
fn assets_are_parsed_without_instrument_tables() {
    for (pair, base) in [
        ("USDTUSD", "USDT"),
        ("SOLUSDT", "SOL"),
        ("ADAUSDC", "ADA"),
        ("SOLFDUSD", "SOL"),
        ("ADABTC", "ADA"),
        ("SOLETH", "SOL"),
        ("XRPEUR", "XRP"),
        ("SOL-USDT-SWAP", "SOL"),
        ("USDT/USD", "USDT"),
        ("PF_XBTUSD", "BTC"),
        ("PF_SOLUSD", "SOL"),
    ] {
        assert_eq!(engine_core::features::asset(pair), Some(base));
    }
}

#[test]
fn every_ending_marker_requires_a_new_epoch_and_corrupt_message_is_counted() {
    let text = std::fs::read_to_string(
        fixtures("strings").join("kraken/spot/BTC-USD/20261016_book.jsonl"),
    )
    .unwrap();
    let mut snapshot: serde_json::Value =
        serde_json::from_str(text.lines().nth(1).unwrap()).unwrap();
    snapshot.as_object_mut().unwrap().remove("conn");
    snapshot.as_object_mut().unwrap().remove("offset");
    for end in ["connection_error", "disconnected", "rotating", "shutdown"] {
        let dir = scratch(end).join("Kraken/Spot/BTC-USD");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("20261016_book.jsonl");
        let marker =
            |event| json!({"marker":"ws_event","event":event,"conn":"same-label","recv_time_ns":1});
        let lines = [
            marker("connected"),
            snapshot.clone(),
            marker("corrupt_message"),
            marker(end),
            snapshot.clone(),
            marker("connected"),
            snapshot.clone(),
        ];
        std::fs::write(
            &path,
            lines.iter().map(|v| format!("{v}\n")).collect::<String>(),
        )
        .unwrap();
        let mut merge = Merge::new(vec![path]).unwrap();
        let mut book = None;
        let mut engine = engine_replay::features::Engine::default();
        for expected in [false, true, true, false, false, false, true] {
            let record = merge.pop().unwrap().unwrap();
            let state = book.get_or_insert_with(|| StreamBook::new(&record).unwrap());
            state.consume(&record).unwrap();
            engine.consume(&record).unwrap();
            assert_eq!(state.valid(), expected, "{end}: {record:?}");
        }
        assert_eq!(book.unwrap().counters().corrupt_messages, 1);
        assert_eq!(engine.data_anomalies.values().sum::<u64>(), 1);
    }
}
