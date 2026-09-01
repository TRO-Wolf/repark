//! `CREATE TABLE` clause-refusal unit tests.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::*;

/// Parse a `CREATE TABLE` fixture into the AST node the handler receives.
fn create_of(sql: &str) -> CreateTable {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|err| panic!("fixture must parse (`{sql}`): {err}"));
    match statements.remove(0) {
        datafusion::sql::sqlparser::ast::Statement::CreateTable(create) => create,
        other => panic!("fixture must be a CREATE TABLE, got {other:?}"),
    }
}

fn form_of(create: &CreateTable) -> &'static str {
    if create.query.is_some() {
        "CREATE TABLE AS SELECT"
    } else {
        "CREATE TABLE"
    }
}

/// Every clause that would otherwise be dropped refuses, naming itself.
#[test]
fn silently_droppable_clauses_all_refuse() {
    let cases: &[(&str, &str)] = &[
        ("CREATE TEMPORARY TABLE c.s.t AS SELECT 1 AS a", "TEMPORARY"),
        (
            "CREATE EXTERNAL TABLE c.s.t (a INT) STORED AS PARQUET LOCATION 'file:///w'",
            "EXTERNAL",
        ),
        ("CREATE TABLE c.s.t (a INT) COMMENT 'hi'", "COMMENT"),
        (
            "CREATE TABLE c.s.t (a INT) LOCATION 'file:///w/t'",
            "LOCATION",
        ),
    ];
    for (sql, token) in cases {
        let create = create_of(sql);
        let err = refuse_unsupported_clauses(&create, form_of(&create))
            .unwrap_err()
            .to_string();
        assert!(err.contains(token), "`{sql}` must name `{token}`: {err}");
    }
}

/// The bare `LOCATION` refusal points at the property spelling this door accepts.
#[test]
fn bare_location_refusal_offers_the_property_spelling() {
    let create = create_of("CREATE TABLE c.s.t (a INT) LOCATION 'file:///w/t'");
    let err = refuse_unsupported_clauses(&create, "CREATE TABLE")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("WITH (location = "),
        "must offer the WITH spelling: {err}"
    );
}

/// A column list with `AS SELECT`, and a create with neither, both refuse because the schema source is ambiguous.
#[test]
fn schema_source_must_be_unambiguous() {
    let both = create_of("CREATE TABLE c.s.t (a INT) AS SELECT 1 AS a");
    let err = refuse_unsupported_clauses(&both, "CREATE TABLE AS SELECT")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("may not be combined with AS SELECT"),
        "must name the class: {err}"
    );

    let neither = create_of("CREATE TABLE c.s.t");
    let err = refuse_unsupported_clauses(&neither, "CREATE TABLE")
        .unwrap_err()
        .to_string();
    assert!(err.contains("column list"), "must name the class: {err}");
}

/// A well-formed create passes the clause gate, proving the gate is selective.
#[test]
fn well_formed_creates_pass_the_clause_gate() {
    for sql in [
        "CREATE TABLE c.s.t AS SELECT 1 AS a",
        "CREATE OR REPLACE TABLE c.s.t WITH (format = 'PARQUET') AS SELECT 1 AS a",
        "CREATE TABLE IF NOT EXISTS c.s.t (a INT, b VARCHAR)",
    ] {
        let create = create_of(sql);
        refuse_unsupported_clauses(&create, form_of(&create))
            .unwrap_or_else(|err| panic!("`{sql}` must pass: {err}"));
    }
}

/// Only `WITH (…)` is read as properties; Spark's `TBLPROPERTIES` refuses with a steer.
#[test]
fn only_the_with_option_syntax_is_accepted() {
    let spark = create_of("CREATE TABLE c.s.t (a INT) TBLPROPERTIES ('k' = 'v')");
    let err = with_options(&spark, "CREATE TABLE")
        .unwrap_err()
        .to_string();
    assert!(err.contains("TBLPROPERTIES"), "must name it: {err}");
    assert!(err.contains("WITH (…)"), "must steer: {err}");

    let ansi = create_of("CREATE TABLE c.s.t WITH (format = 'PARQUET') AS SELECT 1 AS a");
    assert_eq!(
        with_options(&ansi, "CREATE TABLE AS SELECT")
            .expect("WITH is accepted")
            .len(),
        1
    );

    let none = create_of("CREATE TABLE c.s.t (a INT)");
    assert!(
        with_options(&none, "CREATE TABLE")
            .expect("no options is fine")
            .is_empty()
    );
}

/// A11: the DDL-time refuse names the column, nanosecond precision 9, and TIMESTAMP(6).
#[test]
fn nanosecond_timestamp_columns_refuse_with_column_and_precision() {
    let schema = Schema::new(vec![
        Field::new("ok", DataType::Int64, true),
        Field::new(
            "event_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]);
    let err = refuse_nanosecond_timestamp_columns(&schema, "CREATE TABLE", &[])
        .expect_err("ns timestamp must refuse")
        .to_string();
    assert!(err.contains("`event_at`"), "must name the column: {err}");
    assert!(
        err.contains("nanosecond") && err.contains("(9)"),
        "must name precision 9: {err}"
    );
    assert!(
        err.contains("microsecond") && err.contains("TIMESTAMP(6)"),
        "must name the supported spelling: {err}"
    );
    assert!(
        !err.contains("`ok`"),
        "must not blame a non-timestamp sibling: {err}"
    );
}

/// A zoned nanosecond timestamp is the same refuse (`timestamptz_ns` is also v2-illegal).
#[test]
fn nanosecond_timestamptz_columns_refuse() {
    let schema = Schema::new(vec![Field::new(
        "when_tz",
        DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
        true,
    )]);
    let err = refuse_nanosecond_timestamp_columns(&schema, "CREATE TABLE", &[])
        .expect_err("ns timestamptz must refuse")
        .to_string();
    assert!(err.contains("`when_tz`"), "must name the column: {err}");
    assert!(err.contains("nanosecond"), "must name the unit: {err}");
}

/// Positive control: microsecond (and non-timestamp) schemas pass the gate.
#[test]
fn microsecond_timestamp_columns_pass_the_ns_gate() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "event_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "when_tz",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        ),
    ]);
    refuse_nanosecond_timestamp_columns(&schema, "CREATE TABLE", &[])
        .unwrap_or_else(|err| panic!("µs timestamps must pass: {err}"));
}
