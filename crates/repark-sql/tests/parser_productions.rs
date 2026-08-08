//! Parser-production pins — the R1 spike, kept as a test.
//!
//! Design §6 R1 asked whether the DataFusion-re-exported sqlparser can reach the productions the
//! two ANSI milestones need, with a ~50-LOC pre-parse recognizer as the fallback. The spike was
//! run on day 1 of PR-5 and the answer is recorded in `task/p2f-ansi-m1-ledger.md`. It lives on
//! here as assertions rather than as a note, because the answer is a **dependency**: the door
//! ships no recognizer for the M1 forms precisely because the stock parser handles them, and an
//! upstream parser change that quietly broke one of them would otherwise surface as a confusing
//! user-facing parse error rather than as a red test.
//!
//! The negative half matters just as much. The three productions that do NOT parse are the ones
//! PR-6 must carry a recognizer for; pinning them keeps that obligation honest, and turns "the
//! parser learned this form upstream" into a visible signal rather than a silent redundancy.

use datafusion::sql::sqlparser::ast::{CreateTableOptions, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

fn parse(sql: &str) -> Result<Vec<Statement>, String> {
    Parser::parse_sql(&GenericDialect {}, sql).map_err(|err| err.to_string())
}

/// Every production the M1 door depends on parses on the stock Generic dialect. If any of these
/// stops parsing, the corresponding handler becomes unreachable.
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

/// The `WITH (…)` clause arrives as `CreateTableOptions::With`, which is the variant the door
/// reads. Any other variant would make the whole property vocabulary silently unreachable.
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

/// The three productions the stock parser CANNOT reach — PR-6's recognizer obligations
/// (design §6 R1, and §2 Q5/Q7 for the last two).
///
/// If one of these starts parsing, that is good news, not a failure — but it must be noticed and
/// the PR-6 plan adjusted, which is exactly what a red test here forces.
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
