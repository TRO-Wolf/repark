//! Parser-production pins for the ANSI door's stock-parser and pre-parse paths.

use datafusion::sql::sqlparser::ast::{CreateTableOptions, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

fn parse(sql: &str) -> Result<Vec<Statement>, String> {
    Parser::parse_sql(&GenericDialect {}, sql).map_err(|err| err.to_string())
}

/// Every production used by the door parses on the stock Generic dialect.
#[test]
fn m1_productions_parse_on_the_stock_generic_dialect() {
    for sql in [
        "CREATE SCHEMA c.s WITH (location = 's3://bucket/s')",
        "CREATE SCHEMA IF NOT EXISTS c.s WITH (location = 's3://bucket/s')",
        "DROP SCHEMA c.s",
        "DROP SCHEMA IF EXISTS c.s CASCADE",
        "DROP TABLE IF EXISTS c.s.t",
        "CREATE TABLE c.s.t WITH (format = 'PARQUET') AS SELECT 1 AS a",
        "CREATE OR REPLACE TABLE c.s.t WITH (format_version = 2) AS SELECT 1 AS a",
        "CREATE TABLE c.s.t WITH (partitioning = ARRAY['month(ts)', 'bucket(16, id)']) \
         AS SELECT 1 AS a",
        "CREATE TABLE c.s.t WITH (extra_properties = MAP(ARRAY['write.merge.mode'], \
         ARRAY['merge-on-read'])) AS SELECT 1 AS a",
        "CREATE TABLE c.s.t (a integer, b varchar) WITH (location = 's3://b/t')",
    ] {
        parse(sql).unwrap_or_else(|err| panic!("M1 depends on `{sql}` parsing: {err}"));
    }
}

/// The `WITH (…)` clause arrives as `CreateTableOptions::With`, the variant the door handles.
#[test]
fn with_clause_lands_in_the_with_variant() {
    let mut statements =
        parse("CREATE TABLE c.s.t WITH (format = 'PARQUET') AS SELECT 1 AS a").expect("parses");
    let Statement::CreateTable(create) = statements.remove(0) else {
        panic!("must be a CREATE TABLE");
    };
    assert!(
        matches!(create.table_options, CreateTableOptions::With(ref options) if options.len() == 1),
        "the WITH clause must land in CreateTableOptions::With, got {:?}",
        create.table_options
    );
}

/// These productions need the door's pre-parse recognizer because stock parsing cannot reach them.
#[test]
fn m2_productions_still_need_a_pre_parse_recognizer() {
    for (sql, obligation) in [
        (
            "ALTER TABLE c.s.t SET PROPERTIES ('a' = 'b')",
            "Trino SET PROPERTIES — PR-6 property mutation (design §6 R1)",
        ),
        (
            "ALTER TABLE c.s.t EXECUTE optimize",
            "ALTER … EXECUTE — PR-6 maintenance refuse (design §2 Q7)",
        ),
        (
            "SELECT * FROM c.s.t FOR VERSION AS OF 5",
            "FOR … AS OF — PR-6 time-travel scanner (design §2 Q5)",
        ),
    ] {
        assert!(
            parse(sql).is_err(),
            "`{sql}` now parses on the stock dialect — revisit the PR-6 plan for: {obligation}"
        );
    }
}
