use std::time::Duration;

use repark_iceberg::catalog::IcebergIoStats;
use tempfile::TempDir;

use crate::report::{IoDelta, QueryRecord, markdown};
use crate::run::{QuerySpec, Timing};
use crate::{query, run_mode, tiny_bed};

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
