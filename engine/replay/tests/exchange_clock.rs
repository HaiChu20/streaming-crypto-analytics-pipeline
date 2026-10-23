use engine_core::Kind;
use engine_replay::{
    ENGINE_VERSION,
    input::{Merge, day_bounds, discover, discover_filtered, marker_only_files},
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

static SERIAL: AtomicU64 = AtomicU64::new(0);
fn scratch() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../target/k1-tests")
        .join(format!(
            "{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
    fs::create_dir_all(&root).unwrap();
    root
}
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/archive_v2")
}
fn file(root: &std::path::Path, values: &[Value]) -> PathBuf {
    let path = root.join("Kraken/Spotv2/BTC-USD/20251108_book.jsonl");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        values.iter().map(|v| format!("{v}\n")).collect::<String>(),
    )
    .unwrap();
    path
}
fn cli(
    root: &std::path::Path,
    out: &std::path::Path,
    date: &str,
    args: &[&str],
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_engine-replay"))
        .args([
            "--root",
            root.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--date",
            date,
        ])
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn native_v2_excerpts_have_monotonic_clocks_and_l2_snapshot_carry() {
    let paths = discover(&fixtures(), "2025-11-08", false).unwrap();
    assert_eq!(paths.len(), 3);
    assert!(
        Merge::new(paths.clone())
            .err()
            .unwrap()
            .to_string()
            .contains("missing receive time")
    );
    assert!(marker_only_files(&paths, Some(35)).unwrap().is_empty());
    let base = i128::from(day_bounds("2025-11-08").unwrap().0);
    for path in paths {
        let mut merge = Merge::with_exchange_clock(vec![path.clone()], Some(35)).unwrap();
        let mut previous = None;
        let mut snapshot = false;
        let mut count = 0;
        while let Some(record) = merge.pop().unwrap() {
            assert!(previous.is_none_or(|p| record.recv_ns >= p));
            if count == 0 && record.feed == "l2" {
                assert_eq!(record.recv_ns, base + 346_298_000);
            }
            if count == 2 && record.feed == "l3" {
                assert_eq!(record.recv_ns, base + 2_934_662_726);
            }
            if record.feed == "l2" && record.kind == Kind::Snapshot {
                assert_eq!(Some(record.recv_ns), previous);
                snapshot = true;
            }
            previous = Some(record.recv_ns);
            count += 1;
        }
        assert_eq!(merge.clock_steps(), 0);
        if path.to_string_lossy().contains("_book.") {
            assert!(snapshot);
        }
    }
}

#[test]
fn exchange_clock_ignores_receive_times_and_carries_rollover_time() {
    let root = scratch();
    let path = file(
        &root,
        &[
            json!({"channel":"book","type":"update","recv_time_ns":1,"data":[{"timestamp":"2025-11-08T00:00:02.123456789Z"}]}),
            json!({"channel":"book","type":"update","recv_time_ns":999999999999999999i64,"data":[{"timestamp":"2025-11-08T00:00:01Z"}]}),
            json!({"marker":"rollover","timestamp":"2025-11-08T00:00:03+00:00"}),
            json!({"channel":"book","type":"snapshot","recv_time_ns":1,"data":[{}]}),
            json!({"marker":"ws_event","event":"connected","recv_time_ns":1,"timestamp":"2025-11-08T00:00:02+00:00"}),
        ],
    );
    let base = i128::from(day_bounds("2025-11-08").unwrap().0);
    let mut merge = Merge::with_exchange_clock(vec![path], Some(35)).unwrap();
    let mut times = Vec::new();
    while let Some(r) = merge.pop().unwrap() {
        times.push(r.recv_ns - base);
    }
    assert_eq!(
        times,
        vec![2_158_456_789, 2_158_456_789, 3_000_000_000, 3_000_000_000]
    );
}

#[test]
fn initial_untimestamped_snapshot_requires_a_previous_clock_in_its_own_file() {
    let path = file(
        &scratch(),
        &[json!({"channel":"book","type":"snapshot","recv_time_ns":1,"data":[{}]})],
    );
    assert!(
        Merge::with_exchange_clock(vec![path], Some(35))
            .err()
            .unwrap()
            .to_string()
            .contains("neither exchange timestamp nor previous clock")
    );
}

#[test]
fn exchange_clock_flag_requires_explicit_kraken_spot_and_ideal_mode() {
    for args in [
        vec!["--exchange-clock-ms", "35"],
        vec!["--exchange-clock-ms", "35", "--exchange", "kraken"],
        vec![
            "--exchange-clock-ms",
            "35",
            "--exchange",
            "binance",
            "--market",
            "spot",
        ],
        vec![
            "--exchange-clock-ms",
            "35",
            "--exchange",
            "kraken",
            "--market",
            "spot",
            "--mode",
            "as_live",
        ],
        vec!["--exchange-clock-ms", "-1"],
    ] {
        let result = cli(&fixtures(), &scratch(), "2025-11-08", &args);
        assert_eq!(result.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&result.stderr).contains("--exchange-clock-ms requires"));
    }
}

#[test]
fn marker_only_day_has_distinct_status_files_and_no_features() {
    let out = scratch();
    let result = cli(
        &fixtures(),
        &out,
        "2025-11-11",
        &[
            "--exchange",
            "kraken",
            "--market",
            "spot",
            "--exchange-clock-ms",
            "35",
            "--skip-marker-only-days",
        ],
    );
    assert_eq!(
        result.status.code(),
        Some(3),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["reason"], "marker_only_inputs");
    assert_eq!(report["engine_version"], ENGINE_VERSION);
    assert_eq!(report["replay_mode"], "ideal+exchange_clock_35ms");
    assert_eq!(report["files"].as_array().unwrap().len(), 1);
    assert!(
        report["files"][0]
            .as_str()
            .unwrap()
            .ends_with("20251111_level3.jsonl.gz")
    );
    assert!(!out.join("date=2025-11-11").exists());
    // Opt-out still replays the marker-only channel and writes a full day.
    let result = cli(
        &fixtures(),
        &out,
        "2025-11-11",
        &[
            "--exchange",
            "kraken",
            "--market",
            "spot",
            "--exchange-clock-ms",
            "35",
        ],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["mode"], "ideal+exchange_clock_35ms");
    let reader = SerializedFileReader::new(
        fs::File::open(out.join("date=2025-11-11/features.parquet")).unwrap(),
    )
    .unwrap();
    assert!(
        reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .any(|kv| kv.key == "mode" && kv.value.as_deref() == Some("ideal+exchange_clock_35ms"))
    );
}

#[test]
fn duplicate_archive_directories_error_names_both_paths_but_extensions_keep_priority() {
    let root = scratch();
    let original = file(&root, &[json!({})]);
    let compressed = original.with_extension("jsonl.gz");
    fs::write(&compressed, b"representation not selected").unwrap();
    assert_eq!(
        discover(&root, "2025-11-08", false).unwrap(),
        vec![original.clone()]
    );
    let other = root.join("KrakenTest/Spot/BTC-USD/20251108_book.jsonl");
    fs::create_dir_all(other.parent().unwrap()).unwrap();
    fs::write(&other, b"{}\n").unwrap();
    let error = discover_filtered(&root, "2025-11-08", false, Some("kraken"), Some("spot"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("duplicate archive stream"));
    assert!(error.contains(original.to_str().unwrap()) && error.contains(other.to_str().unwrap()));
}

#[test]
fn default_v3_feature_bytes_match_pre_exchange_clock_output() {
    // FNV-1a of the complete Parquet captured from the pre-K1 release binary.
    // This freezes rows, ordering, schema and metadata without a new dependency.
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/archive");
    let out = scratch();
    let result = cli(&root, &out, "2026-09-15", &[]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let bytes = fs::read(out.join("date=2026-09-15/features.parquet")).unwrap();
    let hash = bytes.iter().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
    });
    assert_eq!(bytes.len(), 5_695_854);
    assert_eq!(hash, 0xb8443ba18016e54b);
}
