use std::collections::BTreeMap;

use super::{IncrementalWindow, incremental_spec};
use crate::time_travel::TimeTravelSpec;

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

#[test]
fn from_options_returns_none_without_a_window_key() {
    let parsed = IncrementalWindow::from_options(&map(&[("snapshot-id", "7")])).expect("parses");
    assert!(parsed.is_none());
}

#[test]
fn from_options_reads_all_four_names_case_insensitively() {
    let parsed = IncrementalWindow::from_options(&map(&[
        ("Start-Snapshot-Id", "1"),
        ("END-SNAPSHOT-ID", "2"),
        ("start-timestamp", "3"),
        ("end-timestamp", "4"),
    ]))
    .expect("parses")
    .expect("a window");
    assert_eq!(parsed.start_snapshot_id, Some(1));
    assert_eq!(parsed.end_snapshot_id, Some(2));
    assert_eq!(parsed.start_timestamp_ms, Some(3));
    assert_eq!(parsed.end_timestamp_ms, Some(4));
}

#[test]
fn a_non_numeric_bound_is_refused() {
    let error =
        IncrementalWindow::from_options(&map(&[("start-snapshot-id", "x")])).expect_err("refuses");
    assert!(error.to_string().contains("start-snapshot-id"), "{error}");
}

#[test]
fn append_boundaries_carry_the_exclusive_start_and_inclusive_end() {
    let window = IncrementalWindow::from_options(&map(&[
        ("start-snapshot-id", "11"),
        ("end-snapshot-id", "22"),
    ]))
    .expect("parses")
    .expect("a window");
    let bounds = window.append_boundaries().expect("bounds");
    assert_eq!(bounds.from_exclusive, Some(11));
    assert_eq!(bounds.to_inclusive, Some(22));
}

#[test]
fn append_boundaries_refuse_an_end_without_a_start() {
    let window = IncrementalWindow::from_options(&map(&[("end-snapshot-id", "22")]))
        .expect("parses")
        .expect("a window");
    let error = window.append_boundaries().expect_err("refuses");
    assert_eq!(
        error.to_string().replace("External error: ", ""),
        "Cannot set only `end-snapshot-id` for incremental scans. Please, set `start-snapshot-id` too."
    );
}

#[test]
fn append_boundaries_refuse_a_timestamp_window() {
    let window = IncrementalWindow::from_options(&map(&[("start-timestamp", "5")]))
        .expect("parses")
        .expect("a window");
    let error = window.append_boundaries().expect_err("refuses");
    assert_eq!(
        error.to_string().replace("External error: ", ""),
        "Only changelog scans support `start-timestamp` and `end-timestamp`. Use `start-snapshot-id` and `end-snapshot-id` for incremental scans."
    );
}

#[test]
fn a_time_travel_pin_beside_a_window_is_refused() {
    let error = incremental_spec(
        &map(&[("start-snapshot-id", "1")]),
        &map(&[("version_as_of", "2")]),
    )
    .expect_err("refuses");
    assert_eq!(
        error.to_string().replace("External error: ", ""),
        "Cannot use time travel in incremental scan"
    );
}

#[test]
fn a_legacy_snapshot_pin_beside_a_window_keeps_sparks_legacy_message() {
    let error = incremental_spec(
        &map(&[("start-snapshot-id", "1")]),
        &map(&[("snapshot_id", "2")]),
    )
    .expect_err("refuses");
    assert_eq!(
        error.to_string().replace("External error: ", ""),
        "Time travel option `snapshot-id` is no longer supported, use Spark built-in `versionAsOf` instead"
    );
}

#[test]
fn a_window_without_travel_becomes_the_incremental_spec() {
    let spec =
        incremental_spec(&map(&[("start-snapshot-id", "9")]), &BTreeMap::new()).expect("a spec");
    assert_eq!(
        spec,
        TimeTravelSpec::Incremental {
            from: Some(9),
            to: None
        }
    );
}
