use engine_core::Kind;
use engine_replay::{
    SECOND,
    input::{Merge, day_bounds, discover_filtered_with_dvol, marker_only_files},
};
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::{Field, Row},
};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

fn row_json(row: Row) -> Value {
    Value::Object(
        row.get_column_iter()
            .map(|(name, field)| {
                let value = match field {
                    Field::Null => Value::Null,
                    Field::Str(s) => Value::from(s.clone()),
                    Field::Long(n) => Value::from(*n),
                    Field::Double(n) => Value::from(*n),
                    Field::Bool(b) => Value::from(*b),
                    _ => panic!("unexpected column type"),
                };
                (name.clone(), value)
            })
            .collect(),
    )
}

static SERIAL: AtomicU64 = AtomicU64::new(0);
fn scratch() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../target/k2-tests")
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
fn cli(root: &Path, out: &Path, date: &str, args: &[&str]) -> std::process::Output {
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
fn mixed_clocks_merge_by_effective_receive_time() {
    let paths = discover_filtered_with_dvol(
        &fixtures(),
        "2025-11-08",
        false,
        Some("kraken"),
        Some("spot"),
        true,
    )
    .unwrap();
    assert_eq!(paths.len(), 4);
    assert!(marker_only_files(&paths, Some(35)).unwrap().is_empty());
    let mut merge = Merge::with_exchange_clock(paths, Some(35)).unwrap();
    let start = i128::from(day_bounds("2025-11-08").unwrap().0);
    let mut previous = i128::MIN;
    let mut dvol = Vec::new();
    let mut kraken_before = false;
    let mut kraken_after = false;
    while let Some(record) = merge.pop().unwrap() {
        assert!(record.recv_ns >= previous);
        previous = record.recv_ns;
        if record.exchange == "deribit" && record.kind != Kind::Control {
            dvol.push(record.recv_ns - start);
            assert_eq!(record.conn, "backfill");
        } else if record.exchange == "kraken" {
            kraken_before |= record.recv_ns < start + 75 * i128::from(SECOND);
            kraken_after |= record.recv_ns > start + 75 * i128::from(SECOND);
        }
    }
    assert!(kraken_before && kraken_after);
    assert_eq!(dvol, [75, 2775, 3375, 3675].map(|s| s * i128::from(SECOND)));
}

#[test]
fn filtered_v2_parquet_has_dvol_only_when_requested_and_preserves_asof() {
    let start = day_bounds("2025-11-08").unwrap().0;
    for include in [false, true] {
        let out = scratch();
        let mut args = vec![
            "--exchange",
            "kraken",
            "--market",
            "spot",
            "--exchange-clock-ms",
            "35",
        ];
        if include {
            args.push("--include-dvol");
        }
        let result = cli(&fixtures(), &out, "2025-11-08", &args);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let mode = if include {
            "ideal+exchange_clock_35ms+dvol"
        } else {
            "ideal+exchange_clock_35ms"
        };
        let report: Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(report["mode"], mode);
        let reader = SerializedFileReader::new(
            fs::File::open(out.join("date=2025-11-08/features.parquet")).unwrap(),
        )
        .unwrap();
        assert!(
            reader
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .unwrap()
                .iter()
                .any(|kv| kv.key == "mode" && kv.value.as_deref() == Some(mode))
        );
        let mut checked = 0;
        for row in reader.get_row_iter(None).unwrap() {
            let row = row_json(row.unwrap());
            if row["exchange"] != "cross" || row["asset"] != "BTC" {
                continue;
            }
            let sec = (row["tick_ns"].as_i64().unwrap() - start) / SECOND;
            if !include {
                for name in ["dvol", "dvol_chg_300s", "dvol_chg_900s", "dvol_chg_3600s"] {
                    assert!(row[name].is_null());
                }
            }
            if [74, 75, 3675].contains(&sec) {
                checked += 1;
                if include && sec == 75 {
                    assert_eq!(row["dvol"], 50.0);
                }
                if sec == 74 {
                    assert!(row["dvol"].is_null());
                }
                if include && sec == 3675 {
                    assert_eq!(row["dvol"], 110.0);
                    assert_eq!(row["dvol_chg_300s"], 5.0);
                    assert_eq!(row["dvol_chg_900s"], 15.0);
                    assert_eq!(row["dvol_chg_3600s"], 60.0);
                }
            }
        }
        assert_eq!(checked, 3);
    }
}

#[test]
fn flag_requires_filter_and_ideal_mode_and_supports_either_filter_alone() {
    for args in [
        vec!["--include-dvol"],
        vec!["--include-dvol", "--mode", "as_live"],
        vec![
            "--include-dvol",
            "--exchange",
            "deribit",
            "--mode",
            "as_live",
        ],
    ] {
        let result = cli(&fixtures(), &scratch(), "2025-11-08", &args);
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("filter"));
    }
    // These filters select only the synthetic DVOL stream, with no exchange clock.
    for filter in ["--exchange", "--market"] {
        let value = if filter == "--exchange" {
            "deribit"
        } else {
            "index"
        };
        let result = cli(
            &fixtures(),
            &scratch(),
            "2025-11-08",
            &[filter, value, "--include-dvol"],
        );
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let report: Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(report["mode"], "ideal+dvol");
    }
}

#[test]
fn missing_dvol_is_optional_and_skipped_day_keeps_full_mode() {
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
            "--include-dvol",
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
    assert_eq!(report["replay_mode"], "ideal+exchange_clock_35ms+dvol");
    assert!(!out.join("date=2025-11-11/features.parquet").exists());
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
            "--include-dvol",
        ],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let reader = SerializedFileReader::new(
        fs::File::open(out.join("date=2025-11-11/features.parquet")).unwrap(),
    )
    .unwrap();
    for row in reader.get_row_iter(None).unwrap() {
        assert!(row_json(row.unwrap())["dvol"].is_null());
    }
}

#[test]
fn auxiliary_dvol_duplicate_directories_are_errors() {
    let root = scratch();
    let mut paths = Vec::new();
    for dir in ["Deribit/Index/BTC", "DeribitRaw/IndexV2/BTC"] {
        let path = root.join(dir).join("20251108_dvol.jsonl");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"{}\n").unwrap();
        paths.push(path);
    }
    let error = discover_filtered_with_dvol(
        &root,
        "2025-11-08",
        false,
        Some("kraken"),
        Some("spot"),
        true,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("duplicate archive stream"));
    assert!(
        paths
            .iter()
            .all(|path| error.contains(path.to_str().unwrap()))
    );
}

#[test]
fn empty_or_marker_only_auxiliary_dvol_does_not_skip_valid_kraken_day() {
    for content in [
        "",
        "{\"marker\":\"rollover\",\"new_period\":\"20251109\",\"timestamp\":\"2025-11-09T00:00:00+00:00\"}\n",
    ] {
        let root = scratch();
        let fixture_root = fixtures();
        for source in discover_filtered_with_dvol(
            &fixture_root,
            "2025-11-08",
            false,
            Some("kraken"),
            Some("spot"),
            false,
        )
        .unwrap()
        {
            let target = root.join(source.strip_prefix(&fixture_root).unwrap());
            fs::create_dir_all(target.parent().unwrap()).unwrap();
            fs::copy(source, target).unwrap();
        }
        let dvol = root.join("Deribit/Index/BTC/20251108_dvol.jsonl");
        fs::create_dir_all(dvol.parent().unwrap()).unwrap();
        fs::write(dvol, content).unwrap();
        let out = scratch();
        let result = cli(
            &root,
            &out,
            "2025-11-08",
            &[
                "--exchange",
                "kraken",
                "--market",
                "spot",
                "--exchange-clock-ms",
                "35",
                "--include-dvol",
                "--skip-marker-only-days",
            ],
        );
        assert!(
            result.status.success(),
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        let report: Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(report["mode"], "ideal+exchange_clock_35ms+dvol");
        let reader = SerializedFileReader::new(
            fs::File::open(out.join("date=2025-11-08/features.parquet")).unwrap(),
        )
        .unwrap();
        let mut kraken_rows = 0;
        for row in reader.get_row_iter(None).unwrap() {
            let row = row_json(row.unwrap());
            if row["exchange"] == "kraken" {
                kraken_rows += 1;
            }
            for name in [
                "dvol",
                "dvol_chg_300s",
                "dvol_chg_900s",
                "dvol_chg_3600s",
                "dvol_rv_gap",
            ] {
                assert!(row[name].is_null());
            }
        }
        assert!(kraken_rows > 0);
    }
}
