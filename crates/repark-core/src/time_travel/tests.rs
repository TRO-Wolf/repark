//! Direct tests of the hoisted time-travel pins (bodies byte-faithful to v1 at the pin).
//!
//! The token-scan / SQL-rewrite tests of v1's module (`detects_spark_and_system_spellings`,
//! `find_spans_*`, `comments_do_not_false_positive_time_travel`,
//! `system_version_string_ref_span`) are DEFERRED with the phase-2 statement router — see the
//! deferred-test manifest.

use super::*;
use crate::{IllegalArgumentMarker, engine_err};
use iceberg::{Error as IcebergError, ErrorKind};

#[test]
fn parse_version_integer_and_ref() {
    assert_eq!(
        parse_version_value("42").unwrap(),
        TimeTravelSpec::SnapshotId(42)
    );
    assert_eq!(
        parse_version_value(" 99 ").unwrap(),
        TimeTravelSpec::SnapshotId(99)
    );
    assert_eq!(
        parse_version_value("audit").unwrap(),
        TimeTravelSpec::VersionRef("audit".into())
    );
    assert_eq!(
        parse_version_value("s1").unwrap(),
        TimeTravelSpec::VersionRef("s1".into())
    );
    assert!(parse_version_value("").is_err());
}

#[test]
fn parse_timestamp_ms_and_strings() {
    assert_eq!(
        parse_timestamp_to_ms("1500000000000").unwrap(),
        1_500_000_000_000
    );
    let date_only = parse_timestamp_to_ms("2020-01-15").unwrap();
    // 2020-01-15 00:00:00 UTC
    assert_eq!(date_only, 1_579_046_400_000);
    let with_time = parse_timestamp_to_ms("2020-01-15 12:30:00").unwrap();
    assert!(with_time > date_only);
    assert!(parse_timestamp_to_ms("not-a-time").is_err());
    // RFC3339 / Zulu — must equal naive UTC midnight.
    assert_eq!(
        parse_timestamp_to_ms("2020-01-15T00:00:00Z").unwrap(),
        date_only
    );
    assert_eq!(
        parse_timestamp_to_ms("2020-01-15T00:00:00+00:00").unwrap(),
        date_only
    );
    // Non-UTC offset must shift the epoch ms (not silently treat as naive UTC).
    let plus_one = parse_timestamp_to_ms("2020-01-15T01:00:00+01:00").unwrap();
    assert_eq!(plus_one, date_only);
}

#[test]
fn incremental_datainvalid_refusal_maps_to_illegal_argument() {
    let expected =
        "DataInvalid => Starting snapshot (exclusive) 1 is not a parent ancestor of end snapshot 1";
    let refusal = DataFusionError::External(Box::new(IcebergError::new(
        ErrorKind::DataInvalid,
        "Starting snapshot (exclusive) 1 is not a parent ancestor of end snapshot 1",
    )));
    let mapped = incremental_window_refusal(refusal);
    let DataFusionError::External(inner) = &mapped else {
        panic!("expected an External marker, got {mapped:?}");
    };
    let marker = inner
        .downcast_ref::<IllegalArgumentMarker>()
        .expect("expected an IllegalArgumentMarker");
    assert_eq!(marker.0, expected);
    match engine_err(mapped) {
        Error::IllegalArgument(message) => assert_eq!(message, expected),
        other => panic!("expected IllegalArgument, got {other:?}"),
    }
}

#[test]
fn incremental_other_failures_keep_their_error() {
    let refusal = DataFusionError::External(Box::new(IcebergError::new(
        ErrorKind::FeatureUnsupported,
        "Delete files are currently not supported in changelog scans",
    )));
    let mapped = incremental_window_refusal(refusal);
    let DataFusionError::External(inner) = &mapped else {
        panic!("expected the External error untouched, got {mapped:?}");
    };
    let kept = inner
        .downcast_ref::<IcebergError>()
        .expect("expected the live iceberg error");
    assert_eq!(kept.kind(), ErrorKind::FeatureUnsupported);
    let planned = DataFusionError::Plan("boom".to_string());
    let mapped = incremental_window_refusal(planned);
    assert!(matches!(mapped, DataFusionError::Plan(_)));
}
