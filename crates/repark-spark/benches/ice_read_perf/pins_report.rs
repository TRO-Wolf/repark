use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use repark_iceberg::catalog::{IcebergIoStats, ParquetFooterCacheStats};
use serde_json::Value;
use tempfile::TempDir;

use crate::r3::StepSummary;
use crate::report::{IoDelta, QueryRecord, markdown};
use crate::run::{Mode, QuerySpec, Timing};
use crate::{cli, local_run, query, run_mode, strings, text, tiny_bed};

#[test]
fn the_parser_accepts_a_bare_baseline_flag_and_defaults_it_off() {
    assert!(cli::USAGE.contains("--baseline"));
    assert_eq!(
        cli::parse(&strings(&["run", "--mode", "warm", "--warehouse", "/w"])).unwrap(),
        crate::cli::Command::Run(local_run(Mode::Warm, Path::new("/w")))
    );
    let flagged = cli::parse(&strings(&[
        "run",
        "--mode",
        "warm",
        "--warehouse",
        "/w",
        "--baseline",
    ]))
    .unwrap();
    let crate::cli::Command::Run(options) = flagged else {
        panic!("--baseline must stay a run flag");
    };
    assert!(options.baseline);
    assert_eq!(options.mode, Mode::Warm);
    assert!(
        cli::parse(&strings(&[
            "run",
            "--mode",
            "warm",
            "--warehouse",
            "/w",
            "--baseline",
            "false"
        ]))
        .is_err()
    );
    assert!(cli::parse(&strings(&["setup", "--warehouse", "/w", "--baseline"])).is_err());
}

#[test]
fn the_report_carries_metadata_cache_evictions() {
    let spec = QuerySpec {
        name: "Q1",
        label: "count",
        sql: "SELECT 1".to_string(),
    };
    let delta = IoDelta {
        io: IcebergIoStats::default(),
        cache_hits: 5,
        cache_misses: 3,
        cache_body_fetches: 3,
        cache_evictions: 2,
        footer: None,
        peak_rss_kib: None,
        rss_at_reset_kib: None,
        peak_rss_reset: false,
    };
    let timing = Timing {
        planning: Duration::ZERO,
        first_batch: None,
        total: Duration::ZERO,
        rows: 1,
    };
    let record = QueryRecord::new(&spec, timing, Some(&delta));
    let json = record.to_json();
    assert_eq!(json["metadata_cache"]["evictions"], 2);
    assert_eq!(json["metadata_cache"]["hits"], 5);
    let table = markdown("warm", false, std::slice::from_ref(&spec), &[record], &[]);
    assert!(table.contains("| cache hit/miss/evict |"), "{table}");
    assert!(table.contains("| 5/3/2 |"), "{table}");
}

#[test]
fn every_measured_sample_reports_its_evictions() {
    let dir = TempDir::new().unwrap();
    let warehouse = tiny_bed(&dir);
    for mode in ["cold", "warm"] {
        let document = run_mode(&dir, &warehouse, mode, "1");
        for name in ["Q1", "Q2", "Q3"] {
            let cache = &query(&document, name)["samples"][0]["metadata_cache"];
            assert!(cache["evictions"].is_u64(), "{mode} {name} {cache}");
        }
    }
}

#[test]
fn the_report_carries_footer_cache_hits_and_misses_beside_the_metadata_cache() {
    let spec = QuerySpec {
        name: "Q2",
        label: "full",
        sql: "SELECT 1".to_string(),
    };
    let footer = ParquetFooterCacheStats {
        hits: 7,
        misses: 4,
        fetches: 4,
        upgrades: 1,
        evictions: 0,
    };
    let delta = IoDelta {
        io: IcebergIoStats::default(),
        cache_hits: 5,
        cache_misses: 3,
        cache_body_fetches: 3,
        cache_evictions: 2,
        footer: Some(footer),
        peak_rss_kib: None,
        rss_at_reset_kib: None,
        peak_rss_reset: false,
    };
    let timing = Timing {
        planning: Duration::ZERO,
        first_batch: None,
        total: Duration::ZERO,
        rows: 1,
    };
    let record = QueryRecord::new(&spec, timing, Some(&delta));
    let json = record.to_json();
    assert_eq!(json["footer_cache"]["hits"], 7);
    assert_eq!(json["footer_cache"]["misses"], 4);
    assert_eq!(json["footer_cache"]["fetches"], 4);
    assert_eq!(json["footer_cache"]["upgrades"], 1);
    assert_eq!(json["footer_cache"]["evictions"], 0);
    assert_eq!(json["metadata_cache"]["hits"], 5);
    let table = markdown("warm", false, std::slice::from_ref(&spec), &[record], &[]);
    assert!(
        table.contains("| cache hit/miss/evict | footer cache hit/miss |"),
        "{table}"
    );
    assert!(table.contains("| 5/3/2 | 7/4 |"), "{table}");
    let off = IoDelta {
        footer: None,
        ..delta
    };
    let record = QueryRecord::new(&spec, timing, Some(&off));
    assert!(record.to_json()["footer_cache"].is_null());
    let table = markdown("warm", false, std::slice::from_ref(&spec), &[record], &[]);
    assert!(table.contains("| 5/3/2 | off |"), "{table}");
}

fn warm_run(dir: &TempDir, warehouse: &Path, name: &str, extra: &[&str]) -> Value {
    let out = dir.path().join(format!("{name}.json"));
    let mut argv = vec![
        "run",
        "--mode",
        "warm",
        "--warehouse",
        text(warehouse),
        "--out",
        text(&out),
    ];
    argv.extend(extra.iter().copied());
    let code = cli::main_with(&strings(&argv), &StepSummary::default());
    assert_eq!(code, ExitCode::SUCCESS, "args {argv:?}");
    serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap()
}

fn three_file_bed(dir: &TempDir, name: &str, files: &str, rows_per_file: &str) -> PathBuf {
    let warehouse = dir.path().join(name);
    let code = cli::main_with(
        &strings(&[
            "setup",
            "--warehouse",
            text(&warehouse),
            "--files",
            files,
            "--rows-per-file",
            rows_per_file,
        ]),
        &StepSummary::default(),
    );
    assert_eq!(code, ExitCode::SUCCESS);
    warehouse
}

#[test]
fn a_warm_baseline_run_reads_every_footer_on_its_second_sample() {
    let dir = TempDir::new().unwrap();
    let warehouse = tiny_bed(&dir);
    let plain = warm_run(
        &dir,
        &warehouse,
        "plain-q2",
        &["--repeat", "2", "--query", "Q2"],
    );
    let baseline = warm_run(
        &dir,
        &warehouse,
        "baseline-q2",
        &["--repeat", "2", "--query", "Q2", "--baseline"],
    );
    assert_eq!(plain["baseline"], false);
    assert!(plain["baseline_switches"].is_null());
    assert_eq!(baseline["baseline"], true);
    assert_eq!(
        baseline["baseline_switches"],
        serde_json::json!({
            "row_selection_enabled": false,
            "metadata_cache": false,
            "manifest_cache_bytes": 0,
            "footer_cache_bytes": 0,
        })
    );
    for index in [0usize, 1] {
        let footer = &query(&baseline, "Q2")["samples"][index]["io"]["data_file_ranged"]["footer"];
        assert_eq!(footer["requests"], 3, "sample {index}");
        assert!(
            query(&baseline, "Q2")["samples"][index]["footer_cache"].is_null(),
            "sample {index}"
        );
        let footer = &query(&plain, "Q2")["samples"][index]["io"]["data_file_ranged"]["footer"];
        assert_eq!(footer["requests"], 0, "sample {index}");
    }
    assert_eq!(query(&plain, "Q2")["samples"][1]["footer_cache"]["hits"], 3);
}

#[test]
fn a_warm_baseline_run_records_no_metadata_cache_hits() {
    let dir = TempDir::new().unwrap();
    let warehouse = tiny_bed(&dir);
    let baseline = warm_run(
        &dir,
        &warehouse,
        "baseline-all",
        &["--repeat", "1", "--baseline"],
    );
    for entry in baseline["queries"].as_array().unwrap() {
        for sample in entry["samples"].as_array().unwrap() {
            let cache = &sample["metadata_cache"];
            assert_eq!(cache["hits"], 0, "{}", entry["name"]);
            assert_eq!(cache["misses"], 0, "{}", entry["name"]);
            assert_eq!(cache["body_fetches"], 0, "{}", entry["name"]);
            assert_eq!(cache["evictions"], 0, "{}", entry["name"]);
        }
    }
    let plain = warm_run(&dir, &warehouse, "plain-all", &["--repeat", "1"]);
    assert!(
        query(&plain, "Q1")["samples"][0]["metadata_cache"]["hits"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[test]
fn page_selection_off_reads_more_page_bytes_on_a_selective_id_range() {
    let dir = TempDir::new().unwrap();
    let warehouse = three_file_bed(&dir, "big", "3", "50000");
    let plain = warm_run(
        &dir,
        &warehouse,
        "plain-q7",
        &["--repeat", "1", "--query", "Q7"],
    );
    let baseline = warm_run(
        &dir,
        &warehouse,
        "baseline-q7",
        &["--repeat", "1", "--query", "Q7", "--baseline"],
    );
    let rows = |document: &Value| document["queries"][0]["rows"].as_u64().unwrap();
    assert_eq!(rows(&plain), 1500);
    assert_eq!(rows(&plain), rows(&baseline));
    let page_bytes = |document: &Value| {
        document["queries"][0]["samples"][0]["io"]["data_file_ranged"]["page"]["bytes"]
            .as_u64()
            .unwrap()
    };
    let plain_bytes = page_bytes(&plain);
    let baseline_bytes = page_bytes(&baseline);
    assert!(baseline_bytes > 0);
    assert!(
        plain_bytes < baseline_bytes,
        "plain {plain_bytes} baseline {baseline_bytes}"
    );
}

#[test]
fn the_human_table_names_the_baseline_in_its_header_line() {
    let spec = QuerySpec {
        name: "Q1",
        label: "count",
        sql: "SELECT 1".to_string(),
    };
    let records: Vec<QueryRecord> = Vec::new();
    let table = markdown("warm", true, std::slice::from_ref(&spec), &records, &[]);
    assert!(
        table.starts_with("ice-read-perf mode=warm baseline=true\n"),
        "{table}"
    );
    let table = markdown("warm", false, std::slice::from_ref(&spec), &records, &[]);
    assert!(
        table.starts_with("ice-read-perf mode=warm baseline=false\n"),
        "{table}"
    );
}

#[test]
fn a_warm_sample_hits_the_footer_cache_and_a_cold_one_misses_it() {
    let dir = TempDir::new().unwrap();
    let warehouse = tiny_bed(&dir);
    for mode in ["cold", "warm"] {
        let document = run_mode(&dir, &warehouse, mode, "1");
        let folded = &query(&document, "Q1")["samples"][0]["footer_cache"];
        assert_eq!(folded["hits"].as_u64().unwrap(), 0, "{mode} Q1 {folded}");
        assert_eq!(folded["misses"].as_u64().unwrap(), 0, "{mode} Q1 {folded}");
        for name in ["Q2", "Q3"] {
            let footer = &query(&document, name)["samples"][0]["footer_cache"];
            let hits = footer["hits"].as_u64().unwrap();
            let misses = footer["misses"].as_u64().unwrap();
            if mode == "cold" {
                assert_eq!(hits, 0, "{mode} {name} {footer}");
                assert!(misses > 0, "{mode} {name} {footer}");
            } else {
                assert!(hits > 0, "{mode} {name} {footer}");
                assert_eq!(misses, 0, "{mode} {name} {footer}");
            }
        }
    }
}
