//! `CREATE TABLE` clause-refusal unit tests.
//!
//! The end-to-end behavior lives in `crate::tests` (native session, Arrow path). What is pinned
//! here is the set of clauses that must never be SILENTLY DROPPED: each one, if ignored, would
//! produce a table that exists but does not match what the user asked for — the failure mode
//! worth the most refusals.

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

/// The bare `LOCATION` refusal points at the property spelling this door DOES accept — a refusal
/// that only says "no" sends the user hunting.
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

/// A column list together with `AS SELECT`, and a create with neither, both refuse: the table's
/// schema must have exactly one unambiguous source.
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

/// A well-formed create passes the clause gate untouched — the gate is not simply refusing
/// everything.
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

/// Only the `WITH (…)` option syntax is read as properties; Spark's `TBLPROPERTIES` refuses with
/// a steer rather than being interpreted.
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
