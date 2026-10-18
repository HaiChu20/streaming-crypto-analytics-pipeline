use engine_replay::input::{Merge, discover};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use serde_json::Value;
use std::{collections::BTreeSet, fs::File, path::PathBuf};

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
            .num_columns()
            - 1;
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
