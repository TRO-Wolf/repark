//! Partition-transform parsing + spec-building tests. Every transform name, every arity branch,
//! every bound, and the schema-resolution failure all get a row — a partition spec that is wrong
//! is not recoverable after the table exists.

use super::*;

const FORM: &str = "CREATE TABLE";

// === Transform parsing (the pure function) ==================================================

/// Every supported transform spelling maps to the right Iceberg transform and Java field name.
#[test]
fn every_supported_transform_parses() {
    let cases: &[(&str, PartitionTransform, Transform, &str)] = &[
        (
            "ts",
            PartitionTransform::Identity("ts".to_string()),
            Transform::Identity,
            "ts",
        ),
        (
            "identity(ts)",
            PartitionTransform::Identity("ts".to_string()),
            Transform::Identity,
            "ts",
        ),
        (
            "year(ts)",
            PartitionTransform::Temporal {
                column: "ts".to_string(),
                unit: TemporalUnit::Year,
            },
            Transform::Year,
            "ts_year",
        ),
        (
            "month(ts)",
            PartitionTransform::Temporal {
                column: "ts".to_string(),
                unit: TemporalUnit::Month,
            },
            Transform::Month,
            "ts_month",
        ),
        (
            "day(ts)",
            PartitionTransform::Temporal {
                column: "ts".to_string(),
                unit: TemporalUnit::Day,
            },
            Transform::Day,
            "ts_day",
        ),
        (
            "hour(ts)",
            PartitionTransform::Temporal {
                column: "ts".to_string(),
                unit: TemporalUnit::Hour,
            },
            Transform::Hour,
            "ts_hour",
        ),
        (
            "bucket(16, id)",
            PartitionTransform::Bucket {
                column: "id".to_string(),
                buckets: 16,
            },
            Transform::Bucket(16),
            "id_bucket",
        ),
        (
            "truncate(4, name)",
            PartitionTransform::Truncate {
                column: "name".to_string(),
                width: 4,
            },
            Transform::Truncate(4),
            "name_trunc",
        ),
    ];
    for (spelling, expected, transform, field_name) in cases {
        let parsed = parse_transform(spelling, FORM)
            .unwrap_or_else(|err| panic!("`{spelling}` must parse: {err}"));
        assert_eq!(&parsed, expected, "`{spelling}` shape");
        assert_eq!(&parsed.transform(), transform, "`{spelling}` transform");
        assert_eq!(&parsed.field_name(), field_name, "`{spelling}` field name");
    }
}

/// Transform names are case-insensitive; whitespace inside the call is tolerated.
#[test]
fn transform_parsing_is_case_and_whitespace_tolerant() {
    assert_eq!(
        parse_transform("BUCKET( 16 , id )", FORM).expect("tolerant parse"),
        PartitionTransform::Bucket {
            column: "id".to_string(),
            buckets: 16
        }
    );
}

/// Argument COUNTS and numeric BOUNDS are validated together — the pin the surface matrix
/// names for `PARTITION_TRANSFORM_VALIDATION`. Both classes live in one test because they are
/// one contract: a transform call is accepted only when it has the right number of arguments
/// AND its width is a positive, in-range integer.
#[test]
fn transform_arg_counts_and_bounds_validated() {
    for (spelling, want) in [
        ("bucket(16)", "(count, column)"),
        ("bucket(16, id, extra)", "(count, column)"),
        ("truncate(name)", "(width, column)"),
        ("month(a, b)", "a single (column)"),
    ] {
        let err = parse_transform(spelling, FORM).unwrap_err().to_string();
        assert!(err.contains(want), "`{spelling}` must want {want}: {err}");
    }
    // Bounds: non-positive, non-integer, and overflowing widths all refuse.
    for spelling in ["bucket(0, id)", "bucket(-1, id)", "truncate(0, name)"] {
        let err = parse_transform(spelling, FORM).unwrap_err().to_string();
        assert!(err.contains("> 0"), "`{spelling}` must require > 0: {err}");
    }
    let non_integer = parse_transform("bucket(x, id)", FORM)
        .unwrap_err()
        .to_string();
    assert!(
        non_integer.contains("integer"),
        "must require an integer: {non_integer}"
    );
    let overflow = parse_transform("bucket(5000000000, id)", FORM)
        .unwrap_err()
        .to_string();
    assert!(
        overflow.contains("too large"),
        "must refuse overflow: {overflow}"
    );
}

/// A non-positive or non-integer width refuses (Spark/Iceberg reject these as analysis errors).
#[test]
fn transform_width_must_be_a_positive_integer() {
    for spelling in ["bucket(0, id)", "bucket(-1, id)", "truncate(0, name)"] {
        let err = parse_transform(spelling, FORM).unwrap_err().to_string();
        assert!(err.contains("> 0"), "`{spelling}` must require > 0: {err}");
    }
    let err = parse_transform("bucket(x, id)", FORM)
        .unwrap_err()
        .to_string();
    assert!(err.contains("integer"), "must require an integer: {err}");
}

/// A width beyond `u32` refuses rather than wrapping.
#[test]
fn transform_width_overflow_refuses() {
    let err = parse_transform("bucket(5000000000, id)", FORM)
        .unwrap_err()
        .to_string();
    assert!(err.contains("too large"), "must name the class: {err}");
}

/// An unknown transform refuses, listing the supported set.
#[test]
fn unknown_transform_refuses_listing_support() {
    let err = parse_transform("void(a)", FORM).unwrap_err().to_string();
    assert!(err.contains("void"), "must name it: {err}");
    assert!(err.contains("bucket"), "must list support: {err}");
}

/// Malformed and empty spellings refuse.
#[test]
fn malformed_transform_refuses() {
    assert!(parse_transform("bucket(16, id", FORM).is_err());
    assert!(parse_transform("   ", FORM).is_err());
}

// === Spec building ==========================================================================

fn schema_with(columns: &[(&str, iceberg::spec::PrimitiveType)]) -> iceberg::spec::Schema {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, Schema, Type};

    let fields = columns
        .iter()
        .enumerate()
        .map(|(index, (name, primitive))| {
            Arc::new(NestedField::optional(
                i32::try_from(index).expect("small fixture") + 1,
                *name,
                Type::Primitive(primitive.clone()),
            ))
        })
        .collect::<Vec<_>>();
    Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .expect("fixture schema")
}

/// No transforms → an unpartitioned table (`None`, not an empty spec).
#[test]
fn empty_transforms_build_no_spec() {
    let schema = schema_with(&[("id", iceberg::spec::PrimitiveType::Int)]);
    assert!(
        build_partition_spec(&schema, &[]).expect("build").is_none(),
        "no transforms must mean unpartitioned"
    );
}

/// Transforms resolve against the schema, keeping clause order and Java field names.
#[test]
fn transforms_resolve_against_the_schema() {
    let schema = schema_with(&[
        ("id", iceberg::spec::PrimitiveType::Int),
        ("ts", iceberg::spec::PrimitiveType::Timestamp),
    ]);
    let transforms = vec![
        parse_transform("month(ts)", FORM).expect("parse"),
        parse_transform("bucket(16, id)", FORM).expect("parse"),
    ];
    let spec = build_partition_spec(&schema, &transforms)
        .expect("build")
        .expect("partitioned");
    let names: Vec<&str> = spec.fields().iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, vec!["ts_month", "id_bucket"], "clause order + names");
}

/// A transform on a column the table does not have refuses, LISTING the available columns.
#[test]
fn unknown_partition_column_refuses_listing_columns() {
    let schema = schema_with(&[("id", iceberg::spec::PrimitiveType::Int)]);
    let transforms = vec![parse_transform("month(ts)", FORM).expect("parse")];
    let err = build_partition_spec(&schema, &transforms)
        .unwrap_err()
        .to_string();
    assert!(err.contains("`ts`"), "must name the missing column: {err}");
    assert!(err.contains("[id]"), "must list what IS there: {err}");
}
