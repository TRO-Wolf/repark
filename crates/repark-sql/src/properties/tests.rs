//! `WITH ( … )` vocabulary tests. Every accepted key, every refusal class, and every transform
//! validation branch has a row here — the refusals especially, since a refusal that stops firing
//! is a silent behavior change.

use datafusion::sql::sqlparser::ast::{CreateTableOptions, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::*;

const FORM: &str = "CREATE TABLE";

/// Parse `CREATE TABLE … WITH (…)` and hand back the option list (the door's real input shape).
fn options_of(with_clause: &str) -> Vec<SqlOption> {
    let sql = format!("CREATE TABLE c.s.t WITH ({with_clause}) AS SELECT 1 AS a");
    let mut statements = Parser::parse_sql(&GenericDialect {}, &sql)
        .unwrap_or_else(|err| panic!("fixture must parse (`{sql}`): {err}"));
    let Statement::CreateTable(create) = statements.remove(0) else {
        panic!("fixture must be a CREATE TABLE");
    };
    match create.table_options {
        CreateTableOptions::With(options) => options,
        other => panic!("fixture must carry a WITH clause, got {other:?}"),
    }
}

fn parse(with_clause: &str) -> Result<TableProperties> {
    parse_with_options(&options_of(with_clause), FORM)
}

// === Accepted vocabulary ====================================================================

/// An empty WITH clause is the default table.
#[test]
fn no_properties_is_the_default() {
    let properties = parse_with_options(&[], FORM).expect("no options must pass");
    assert_eq!(properties, TableProperties::default());
}

/// `format = 'PARQUET'` is accepted, case-insensitively.
#[test]
fn format_parquet_is_accepted() {
    for spelling in ["'PARQUET'", "'parquet'", "'Parquet'"] {
        parse(&format!("format = {spelling}"))
            .unwrap_or_else(|err| panic!("{spelling} must be accepted: {err}"));
    }
}

/// `format_version = 2` is accepted as a number AND as a string (both spellings occur in the
/// wild), and is CONSUMED — it never leaks into the table's plain properties, because Iceberg
/// rejects it there.
/// pins: v3-2-create-v3-opt-in/C-003
#[test]
fn format_version_two_is_accepted_and_consumed() {
    for spelling in ["2", "'2'"] {
        let properties = parse(&format!("format_version = {spelling}"))
            .unwrap_or_else(|err| panic!("{spelling} must be accepted: {err}"));
        assert!(
            properties.extra_properties.is_empty(),
            "format_version must not become a table property"
        );
        assert_eq!(properties.format_version.as_deref(), Some("2"));
    }
}

/// `format_version = 3` is accepted at parse (execute still needs the session opt-in).
/// pins: v3-2-create-v3-opt-in/C-006
#[test]
fn format_version_three_is_accepted_at_parse() {
    for spelling in ["3", "'3'"] {
        let properties = parse(&format!("format_version = {spelling}"))
            .unwrap_or_else(|err| panic!("{spelling} must be accepted: {err}"));
        assert!(
            properties.extra_properties.is_empty(),
            "format_version must not become a table property"
        );
        assert_eq!(properties.format_version.as_deref(), Some("3"));
    }
}

/// `location` is captured verbatim.
#[test]
fn location_is_captured() {
    let properties = parse("location = 's3://bucket/warehouse/t'").expect("location accepted");
    assert_eq!(
        properties.location.as_deref(),
        Some("s3://bucket/warehouse/t")
    );
}

/// `extra_properties` carries RAW dotted Iceberg keys through untouched — the G4 hatch, and the
/// concrete reason it exists: merge-on-read table creation.
#[test]
fn extra_properties_carries_raw_iceberg_keys() {
    let properties = parse(
        "extra_properties = MAP(ARRAY['write.merge.mode', 'write.target-file-size-bytes'], \
         ARRAY['merge-on-read', '134217728'])",
    )
    .expect("extra_properties accepted");
    assert_eq!(
        properties
            .extra_properties
            .get("write.merge.mode")
            .map(String::as_str),
        Some("merge-on-read")
    );
    assert_eq!(
        properties
            .extra_properties
            .get("write.target-file-size-bytes")
            .map(String::as_str),
        Some("134217728")
    );
}

/// An empty MAP is legal (it just sets nothing).
#[test]
fn empty_extra_properties_is_legal() {
    let properties = parse("extra_properties = MAP(ARRAY[], ARRAY[])").expect("empty map accepted");
    assert!(properties.extra_properties.is_empty());
}

/// Several curated keys compose in one clause.
#[test]
fn curated_keys_compose() {
    let properties = parse(
        "format = 'PARQUET', format_version = 2, location = 'file:///w/t', \
         partitioning = ARRAY['month(ts)'], \
         extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['merge-on-read'])",
    )
    .expect("composed clause accepted");
    assert_eq!(properties.location.as_deref(), Some("file:///w/t"));
    assert_eq!(properties.partitioning.len(), 1);
    assert_eq!(properties.extra_properties.len(), 1);
}

// === Refusal classes ========================================================================

/// The typo guard: an unknown bare key refuses and LISTS the curated set.
#[test]
fn unknown_bare_key_refuses_listing_the_set() {
    let err = parse("formatt = 'PARQUET'").unwrap_err().to_string();
    assert!(
        err.contains("formatt"),
        "must name the offending key: {err}"
    );
    for key in CURATED_KEYS {
        assert!(err.contains(key), "must list `{key}`: {err}");
    }
    assert!(
        err.contains("extra_properties = MAP"),
        "must point dotted keys at the hatch: {err}"
    );
}

/// G9: `sorted_by` refuses loud AND names its trigger.
#[test]
fn sorted_by_refuses_with_a_named_trigger() {
    let err = parse("sorted_by = ARRAY['a']").unwrap_err().to_string();
    assert!(err.contains("sorted_by"), "must name the property: {err}");
    assert!(err.contains("TRIGGER"), "must name the trigger: {err}");
    assert!(
        err.contains("rewrite_data_files") || err.contains("sorted writer"),
        "the trigger must be concrete: {err}"
    );
}

/// G9: ORC and AVRO refuse loud, each naming the format and its trigger.
#[test]
fn orc_and_avro_refuse_with_a_named_trigger() {
    for format in ["ORC", "AVRO"] {
        let err = parse(&format!("format = '{format}'"))
            .unwrap_err()
            .to_string();
        assert!(err.contains(format), "must name the format: {err}");
        assert!(err.contains("TRIGGER"), "must name the trigger: {err}");
        assert!(
            err.contains("PARQUET"),
            "must steer to the supported format: {err}"
        );
    }
}

/// A format outside the Iceberg vocabulary is a plain refusal, not a reserved one.
#[test]
fn unknown_format_refuses() {
    let err = parse("format = 'CSV'").unwrap_err().to_string();
    assert!(err.contains("CSV"), "must name the value: {err}");
    assert!(err.contains("'PARQUET'"), "must list the support: {err}");
}

/// A format version other than 2 or 3 refuses rather than being silently ignored.
/// pins: v3-2-create-v3-opt-in/C-007
#[test]
fn non_v2_format_version_refuses() {
    for spelling in ["1", "4"] {
        let err = parse(&format!("format_version = {spelling}"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("format_version"), "must name the key: {err}");
        assert!(err.contains("v2"), "must state what IS created: {err}");
    }
}

/// A duplicated key refuses (rather than last-write-wins, which hides a mistake).
#[test]
fn duplicate_key_refuses() {
    let err = parse("format = 'PARQUET', format = 'PARQUET'")
        .unwrap_err()
        .to_string();
    assert!(err.contains("more than once"), "must name the class: {err}");
}

/// A property whose value has the wrong shape refuses with the expected shape spelled out.
#[test]
fn wrong_value_shape_refuses() {
    let err = parse("location = 42").unwrap_err().to_string();
    assert!(
        err.contains("string literal"),
        "must say what is wanted: {err}"
    );
}

/// `partitioning` must be an ARRAY, and its elements must be strings.
#[test]
fn partitioning_shape_refuses() {
    let err = parse("partitioning = 'month(ts)'").unwrap_err().to_string();
    assert!(err.contains("ARRAY"), "must show the ARRAY spelling: {err}");

    let err = parse("partitioning = ARRAY[1]").unwrap_err().to_string();
    assert!(
        err.contains("string literal"),
        "must say what is wanted: {err}"
    );
}

/// The reserved Iceberg key cannot sneak in through the hatch.
#[test]
fn reserved_format_version_through_the_hatch_refuses() {
    let err = parse("extra_properties = MAP(ARRAY['format-version'], ARRAY['2'])")
        .unwrap_err()
        .to_string();
    assert!(err.contains("reserved"), "must name the class: {err}");
    assert!(
        err.contains("format_version"),
        "must point at the curated key: {err}"
    );
}

/// Mismatched hatch arrays refuse with both lengths named.
#[test]
fn mismatched_extra_properties_arrays_refuse() {
    let err = parse("extra_properties = MAP(ARRAY['a', 'b'], ARRAY['1'])")
        .unwrap_err()
        .to_string();
    assert!(err.contains("same length"), "must name the class: {err}");
    assert!(
        err.contains('2') && err.contains('1'),
        "must show both: {err}"
    );
}

/// A non-MAP `extra_properties` value refuses with the expected spelling shown.
#[test]
fn non_map_extra_properties_refuses() {
    for spelling in [
        "extra_properties = 'write.merge.mode'",
        "extra_properties = ARRAY['a']",
        "extra_properties = OTHER(ARRAY['a'], ARRAY['b'])",
    ] {
        let err = parse(spelling).unwrap_err().to_string();
        assert!(
            err.contains("MAP(ARRAY["),
            "`{spelling}` must show the shape: {err}"
        );
    }
}

/// An empty hatch key refuses.
#[test]
fn empty_extra_property_key_refuses() {
    let err = parse("extra_properties = MAP(ARRAY[''], ARRAY['v'])")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("must not be empty"),
        "must name the class: {err}"
    );
}

/// A duplicated hatch key refuses.
#[test]
fn duplicate_extra_property_key_refuses() {
    let err = parse("extra_properties = MAP(ARRAY['a', 'a'], ARRAY['1', '2'])")
        .unwrap_err()
        .to_string();
    assert!(err.contains("more than once"), "must name the class: {err}");
}
