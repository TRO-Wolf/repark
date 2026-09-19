use std::path::Path;
use std::time::Duration;

use repark_iceberg::catalog::{IcebergIoStats, ParquetFooterCacheStats};
use tempfile::TempDir;

use crate::report::{IoDelta, QueryRecord, markdown};
use crate::run::{Mode, QuerySpec, Timing};
use crate::{cli, local_run, query, run_mode, strings, tiny_bed};

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
    let table = markdown("warm", std::slice::from_ref(&spec), &[record], &[]);
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
    let table = markdown("warm", std::slice::from_ref(&spec), &[record], &[]);
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
    let table = markdown("warm", std::slice::from_ref(&spec), &[record], &[]);
    assert!(table.contains("| 5/3/2 | off |"), "{table}");
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
