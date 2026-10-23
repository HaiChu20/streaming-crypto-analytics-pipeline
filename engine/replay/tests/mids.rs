use engine_replay::input::day_bounds;
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::Field,
};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

static SERIAL: AtomicU64 = AtomicU64::new(0);
fn scratch() -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../target/mid-tests")
        .join(format!(
            "{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
    fs::create_dir_all(&path).unwrap();
    path
}
fn cli(root: &Path, out: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_engine-replay"))
        .args([
            "--root",
            root.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--date",
            "2026-09-15",
        ])
        .args(args)
        .output()
        .unwrap()
}
fn success(output: std::process::Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn exact_grid_includes_equal_time_events_masks_warmup_invalid_and_stale_books() {
    let root = scratch();
    let pair = root.join("raw/Binance/Spot/BTCUSDT");
    fs::create_dir_all(&pair).unwrap();
    let (start, end) = day_bounds("2026-09-15").unwrap();
    fs::write(pair.join("20260915_snapshot.jsonl"), format!("{}\n", json!({
        "recv_time_ns":start,"conn":"A", "data":{"lastUpdateId":1,"bids":[["99","1"]],"asks":[["101","1"]]}
    }))).unwrap();
    let rows = [
        json!({"recv_time_ns":start+299_950_000_000i64,"conn":"A","data":{"U":2,"u":2,"b":[["99","0"],["100","1"]],"a":[["101","0"],["102","1"]]}}),
        json!({"recv_time_ns":start+300_100_000_000i64,"conn":"A","data":{"U":3,"u":3,"b":[["100","0"],["101","1"]],"a":[["102","0"],["103","1"]]}}),
        json!({"recv_time_ns":start+300_100_000_000i64,"conn":"A","data":{"U":4,"u":4,"b":[["101","0"],["102","1"]],"a":[["103","0"],["104","1"]]}}),
        json!({"recv_time_ns":start+300_200_000_001i64,"marker":"ws_event","event":"disconnected","conn":"A","offset":99}),
    ];
    fs::write(
        pair.join("20260915_depth.jsonl"),
        rows.iter().map(|v| format!("{v}\n")).collect::<String>(),
    )
    .unwrap();
    // A discovered book with no records is present as an all-null column.
    let absent = root.join("raw/Kraken/Spot/ETH-USD");
    fs::create_dir_all(&absent).unwrap();
    fs::write(absent.join("20260915_book.jsonl"), "").unwrap();
    let off = root.join("off");
    success(cli(&root, &off, &[]));
    let on = root.join("on");
    let mids = root.join("mids.parquet");
    success(cli(
        &root,
        &on,
        &["--mid-grid-ms", "100", "--mids-out", mids.to_str().unwrap()],
    ));
    // The opt-in sampler is read-only; even enabled it preserves feature bytes.
    assert_eq!(
        fs::read(off.join("date=2026-09-15/features.parquet")).unwrap(),
        fs::read(on.join("date=2026-09-15/features.parquet")).unwrap()
    );
    let reader = SerializedFileReader::new(fs::File::open(mids).unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 864_000);
    assert!(reader.metadata().num_row_groups() > 1);
    let mut count = 0;
    for row in reader.get_row_iter(None).unwrap() {
        let row = row.unwrap();
        let columns: Vec<_> = row.get_column_iter().collect();
        assert_eq!(columns[0].1, &Field::Long(start + count * 100_000_000));
        assert_eq!(columns[1].0, "binance_spot_BTCUSDT_mid_e9");
        assert_eq!(columns[2].0, "kraken_spot_ETH_USD_mid_e9");
        assert_eq!(columns[2].1, &Field::Null);
        let expected = match count {
            3000 => Field::Long(101_000_000_000),
            3001 | 3002 => Field::Long(103_000_000_000),
            _ => Field::Null,
        };
        assert_eq!(columns[1].1, &expected, "grid row {count}");
        count += 1;
    }
    assert_eq!(start + (count - 1) * 100_000_000, end - 100_000_000);
}

#[test]
fn filters_dvol_and_exchange_clock_keep_only_book_columns_and_epoch_multiples() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/archive_v2");
    let out = scratch();
    let mids = out.join("mids.parquet");
    let output = Command::new(env!("CARGO_BIN_EXE_engine-replay"))
        .args([
            "--root",
            root.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--date",
            "2025-11-08",
            "--exchange",
            "kraken",
            "--market",
            "spot",
            "--include-dvol",
            "--exchange-clock-ms",
            "35",
            "--mid-grid-ms",
            "700",
            "--mids-out",
            mids.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    success(output);
    let reader = SerializedFileReader::new(fs::File::open(mids).unwrap()).unwrap();
    let schema = reader.metadata().file_metadata().schema_descr();
    assert!(
        schema
            .columns()
            .iter()
            .skip(1)
            .all(|c| c.name().starts_with("kraken_spot_"))
    );
    assert!(schema.num_columns() > 1);
    let (start, end) = day_bounds("2025-11-08").unwrap();
    let first = start + (700_000_000 - start % 700_000_000) % 700_000_000;
    assert_eq!(
        reader.metadata().file_metadata().num_rows(),
        (end - 1 - first) / 700_000_000 + 1
    );
    let row = reader.get_row_iter(None).unwrap().next().unwrap().unwrap();
    assert_eq!(row.get_column_iter().next().unwrap().1, &Field::Long(first));
}

#[test]
fn grid_arguments_reject_zero_unpaired_overflow_and_as_live() {
    for args in [
        vec!["--mid-grid-ms", "0", "--mids-out", "unused"],
        vec!["--mid-grid-ms", "100"],
        vec!["--mids-out", "unused"],
        vec!["--mid-grid-ms", "-1", "--mids-out", "unused"],
        vec![
            "--mid-grid-ms",
            "18446744073709551615",
            "--mids-out",
            "unused",
        ],
        vec![
            "--mode",
            "as_live",
            "--mid-grid-ms",
            "100",
            "--mids-out",
            "unused",
        ],
    ] {
        let root = scratch();
        assert!(!cli(&root, &root.join("out"), &args).status.success());
        assert!(!root.join("out").exists());
    }
}

#[test]
fn sampling_uses_feature_warmup_and_exact_staleness_boundary_without_mutation() {
    use engine_core::Record;
    use engine_replay::{SECOND, features::Engine};
    let key = ("binance".into(), "spot".into(), "BTCUSDT".into());
    let snapshot = |ns| {
        Record::parse(&json!({
        "exchange":"binance","market":"spot","feed":"l2","symbol":"BTCUSDT",
        "kind":"snapshot","conn":"A","seq":1,"recv_ns":ns,
        "payload":json!({"lastUpdateId":1,"bids":[["99","1"]],"asks":[["101","1"]]}).to_string()
    }).to_string()).unwrap()
    };
    let mut engine = Engine::default();
    engine.consume(&snapshot(0)).unwrap();
    engine.tick(0).unwrap();
    engine.consume(&snapshot(300 * SECOND)).unwrap();
    assert_eq!(engine.mid_e9(&key, 300 * SECOND - 1), None);
    assert_eq!(engine.mid_e9(&key, 300 * SECOND), Some(100_000_000_000));
    assert_eq!(engine.mid_e9(&key, 360 * SECOND), Some(100_000_000_000));
    assert_eq!(engine.mid_e9(&key, 360 * SECOND + 1), None);
    assert_eq!(
        engine.mid_e9(&("other".into(), "spot".into(), "BTC".into()), 300 * SECOND),
        None
    );
    assert_eq!(engine.records, 2);
}
