use engine_replay::input::{Merge, discover};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use serde_json::Value;
use std::{collections::BTreeSet, fs::File, path::PathBuf};

#[test]
fn archived_kraken_l3_july_delete_quantities_replay_without_breaks() {
    use engine_core::{Fixed, Kind, features::Engine};

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/archive_kraken_l3_2026_07");
    let mut merge = Merge::new(discover(&root, "2026-07-15", false).unwrap()).unwrap();
    let mut engine = Engine::default();
    let mut zero_qty_deletes = 0;
    let mut nonzero_qty_deletes = 0;
    while let Some(record) = merge.pop().unwrap() {
        if record.feed == "l3" && record.kind == Kind::Update {
            let payload: Value = serde_json::from_str(&record.payload).unwrap();
            for data in payload["data"].as_array().unwrap() {
                for side in ["bids", "asks"] {
                    if let Some(orders) = data[side].as_array() {
                        for order in orders {
                            if order["event"] == "delete" {
                                let qty: Fixed =
                                    order["order_qty"].as_str().unwrap().parse().unwrap();
                                if qty.0 == 0 {
                                    zero_qty_deletes += 1;
                                } else {
                                    nonzero_qty_deletes += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        engine.consume(&record).unwrap();
    }
    assert!(
        zero_qty_deletes > 0,
        "fixture must contain zero-qty deletes"
    );
    assert!(
        nonzero_qty_deletes > 0,
        "fixture must contain non-zero-qty deletes"
    );
    let reports = engine.book_reports();
    let book = &reports["kraken/spot/BTC/USD/l3"];
    assert_eq!(book.counters.checksum_failures, 0, "{book:?}");
    assert_eq!(book.counters.breaks, 0, "{book:?}");
    assert!(book.counters.checksums_verified > 0, "{book:?}");
    assert!(book.valid, "{book:?}");
}

#[test]
fn users_v3_archive_replays_every_pair_and_validates_all_books() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/archive");
    let scratch = PathBuf::from(
        std::env::var_os("ENGINE_TEST_SCRATCH").unwrap_or_else(|| std::env::temp_dir().into()),
    )
    .join(format!("archive-e2e-{}", std::process::id()));
    for (date, expected_pairs, assets) in [
        (
            "2026-09-15",
            vec![
                "binance/spot/BTCUSDT",
                "binance/spot/USDTUSD",
                "okx/spot/BTC-USDT",
                "okx/spot/USDT-USD",
                "kraken/spot/BTC/USDT",
                "kraken/spot/USDT/USD",
            ],
            vec!["BTC", "USDT"],
        ),
        ("2026-10-08", vec!["kraken/perp/PF_XBTUSD"], vec!["BTC"]),
    ] {
        let result = std::process::Command::new(env!("CARGO_BIN_EXE_engine-replay"))
            .arg("--root")
            .arg(&root)
            .args(["--date", date, "--out"])
            .arg(&scratch)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let report: Value = serde_json::from_slice(&result.stdout).unwrap();
        std::fs::write(scratch.join(format!("{date}-report.json")), &result.stdout).unwrap();
        assert_eq!(report["book_breaks"], 0, "{report}");
        assert_eq!(report["data_anomalies"], serde_json::json!({}), "{report}");
        let books = report["books"].as_object().unwrap();
        assert_eq!(books.len(), if date == "2026-09-15" { 8 } else { 1 });
        for (key, book) in books {
            assert_eq!(book["valid"], true, "{key}: {book}");
            assert_eq!(book["checksum_failures"], 0, "{key}: {book}");
            assert_eq!(book["sequence_breaks"], 0, "{key}: {book}");
            assert_eq!(book["pending_evicted"], 0, "{key}: {book}");
            if key.starts_with("kraken/spot/") {
                assert!(
                    book["checksums_verified"].as_u64().unwrap() > 0,
                    "{key}: {book}"
                );
            }
        }
        for pair in &expected_pairs {
            assert!(report["row_counts"][pair].as_u64().unwrap() > 0);
            assert!(
                report["coverage"][pair]["valid_ticks"].as_u64().unwrap() > 0,
                "{pair}: {report}"
            );
        }
        let cross = report["row_counts"]
            .as_object()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with("cross/"))
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            cross,
            assets
                .iter()
                .map(|base| format!("cross/cross/{base}"))
                .collect()
        );
        let reader =
            SerializedFileReader::new(File::open(report["output"].as_str().unwrap()).unwrap())
                .unwrap();
        assert_eq!(
            reader.metadata().file_metadata().num_rows() as u64,
            report["rows"].as_u64().unwrap()
        );
        assert!(
            reader
                .metadata()
                .file_metadata()
                .schema_descr()
                .columns()
                .iter()
                .any(|column| column.name() == "asset")
        );
        let mut seen = BTreeSet::new();
        let asset_column = reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .position(|c| c.name() == "asset")
            .unwrap();
        for row in reader.get_row_iter(None).unwrap() {
            let row = row.unwrap();
            let (exchange, market, symbol) = (
                row.get_string(0).unwrap(),
                row.get_string(1).unwrap(),
                row.get_string(2).unwrap(),
            );
            let expected_asset = if exchange == "cross" {
                symbol.as_str()
            } else {
                engine_replay::input::asset(symbol).unwrap()
            };
            assert_eq!(row.get_string(asset_column).unwrap(), expected_asset);
            seen.insert(format!("{exchange}/{market}/{symbol}"));
            if seen.len() == expected_pairs.len() + assets.len() {
                break;
            }
        }
        assert_eq!(seen.len(), expected_pairs.len() + assets.len());
        // Assert the delayed USDTUSD anchor has depth waiting before it arrives.
        if date == "2026-09-15" {
            let mut merge = Merge::new(discover(&root, date, false).unwrap()).unwrap();
            let mut first_depth = None;
            let mut anchor = None;
            while let Some(r) = merge.pop().unwrap() {
                if r.exchange == "binance" && r.symbol == "USDTUSD" && r.feed == "l2" {
                    if r.kind == engine_core::Kind::Update {
                        first_depth.get_or_insert(r.recv_ns);
                    }
                    if r.kind == engine_core::Kind::Snapshot {
                        anchor = Some(r.recv_ns);
                    }
                }
            }
            assert!(anchor.unwrap() - first_depth.unwrap() > 170_000_000_000);
        }
        println!("fixture {date}: {report}");
    }
}

#[test]
fn fixture_ofi_windows_cover_all_four_market_types_and_restart_identically() {
    use engine_core::{SECOND, features::Engine};
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/archive");
    let mut seen = BTreeSet::new();
    for date in ["2026-09-15", "2026-10-08"] {
        let mut merge = Merge::new(discover(&root, date, false).unwrap()).unwrap();
        let mut engine = Engine::default();
        let mut head = merge.pop().unwrap();
        let first = head.as_ref().unwrap().recv_ns as i64;
        let mut tick = first.div_euclid(SECOND) * SECOND;
        // These archives are shorter than the 300-second warmup. Prime only
        // with each fixture's initial L2 snapshot; snapshots add no OFI.
        let mut initial = std::collections::BTreeMap::new();
        let mut scan = Merge::new(discover(&root, date, false).unwrap()).unwrap();
        while let Some(record) = scan.pop().unwrap() {
            if record.feed == "l2" && record.kind == engine_core::Kind::Snapshot {
                initial
                    .entry((
                        record.exchange.clone(),
                        record.market.clone(),
                        record.symbol.clone(),
                    ))
                    .or_insert(record);
            }
        }
        for prime in (tick - 300 * SECOND..tick).step_by(SECOND as usize) {
            for snapshot in initial.values() {
                let mut snapshot = snapshot.clone();
                snapshot.recv_ns = i128::from(prime);
                engine.consume(&snapshot).unwrap();
            }
            engine.tick(prime).unwrap();
        }
        let mut restarted = None;
        while head.is_some() {
            while head.as_ref().is_some_and(|r| r.recv_ns <= i128::from(tick)) {
                let record = head.take().unwrap();
                engine.consume(&record).unwrap();
                if let Some(restored) = &mut restarted {
                    Engine::consume(restored, &record).unwrap();
                }
                head = merge.pop().unwrap();
            }
            let rows = engine.tick(tick).unwrap();
            if let Some(restored) = &mut restarted {
                assert_eq!(
                    serde_json::to_value(&rows).unwrap(),
                    serde_json::to_value(restored.tick(tick).unwrap()).unwrap()
                );
            }
            for row in rows {
                if row.memory.ofi1_10s.is_some() && row.memory.ofi10_10s.is_some() {
                    seen.insert(format!("{}/{}", row.exchange, row.market));
                }
            }
            if tick == first.div_euclid(SECOND) * SECOND + 10 * SECOND {
                restarted = Some(
                    serde_json::from_slice::<Engine>(&serde_json::to_vec(&engine).unwrap())
                        .unwrap(),
                );
            }
            tick += SECOND;
        }
    }
    assert_eq!(
        seen,
        ["binance/spot", "okx/spot", "kraken/spot", "kraken/perp"]
            .into_iter()
            .map(String::from)
            .collect()
    );
}

#[test]
fn archived_kraken_ticker_funding_is_an_epoch_millisecond_timestamp() {
    use engine_core::{SECOND, features::Engine};
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/archive");
    let mut merge = Merge::new(discover(&root, "2026-10-08", false).unwrap()).unwrap();
    while let Some(record) = merge.pop().unwrap() {
        if record.feed != "ticker" || record.kind == engine_core::Kind::Control {
            continue;
        }
        let payload: Value = serde_json::from_str(&record.payload).unwrap();
        assert_eq!(payload["next_funding_rate_time"], 1791471600000i64);
        let mut engine = Engine::default();
        engine.consume(&record).unwrap();
        let tick = (record.recv_ns as i64 / SECOND + 1) * SECOND;
        let row = engine
            .tick(tick)
            .unwrap()
            .into_iter()
            .find(|r| r.exchange == "kraken")
            .unwrap();
        let expected = (1791471600000.0 - tick as f64 / 1_000_000.0) / 60000.0;
        assert_eq!(row.funding_min_to_next, Some(expected));
        assert!((0.0..60.0).contains(&expected));
        let relative: f64 = payload["relative_funding_rate"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(row.funding_rate, Some(relative));
        let mark: f64 = payload["markPrice"].as_str().unwrap().parse().unwrap();
        let index: f64 = payload["index"].as_str().unwrap().parse().unwrap();
        assert_eq!(row.mark_index_bps, Some((mark - index) / index * 10000.0));
        return;
    }
    panic!("fixture has no ticker");
}
