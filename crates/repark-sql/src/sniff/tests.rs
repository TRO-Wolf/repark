//! Wrong-door sniff tests. Every recognized token gets a row (the message contract is the
//! product here), plus the three properties the error-path placement is chosen FOR: the original
//! error survives, non-Spark SQL is untouched, and literals/comments cannot trigger it.

use super::*;

fn original() -> DataFusionError {
    DataFusionError::Plan("sql parser error: Expected: AS, found: USING".to_string())
}

fn upgraded(sql: &str) -> String {
    upgrade_error(sql, original()).to_string()
}

/// The three-part contract: the token, the native equivalent, and the Spark door.
fn assert_steer(message: &str, token: &str, equivalent_fragment: &str) {
    assert!(message.contains(token), "must name the token: {message}");
    assert!(
        message.contains(equivalent_fragment),
        "must give the native equivalent (`{equivalent_fragment}`): {message}"
    );
    assert!(
        message.contains("SparkDialect"),
        "must name the Spark door: {message}"
    );
}

/// The ORIGINAL parser error is preserved — the upgrade adds context, never replaces the
/// diagnostic (a user who knows what they are doing must still see the real error).
#[test]
fn original_error_is_preserved() {
    let message = upgraded("CREATE TABLE t USING iceberg AS SELECT 1");
    assert!(
        message.contains("Expected: AS, found: USING"),
        "the original error must survive: {message}"
    );
}

/// Ordinary SQL that merely failed to parse is returned untouched — no speculation.
#[test]
fn non_spark_errors_are_untouched() {
    for sql in [
        "SELECT * FROM",
        "CREATE TABLE c.s.t AS SELECT 1",
        "SELCT 1",
        "DROP TABLE c.s.t",
    ] {
        let message = upgrade_error(sql, original()).to_string();
        assert_eq!(
            message,
            original().to_string(),
            "`{sql}` must not be upgraded"
        );
    }
}

/// A Spark-ism inside a string literal or a comment must NOT trigger the sniff — this is the
/// property that makes an error-path scan safe to ship.
#[test]
fn literals_and_comments_do_not_trigger_the_sniff() {
    for sql in [
        "SELECT 'USING' AS x FROM",
        "SELECT 'TBLPROPERTIES' FROM",
        "SELECT 1 -- PARTITIONED BY (a)\nFROM",
        "SELECT /* LATERAL VIEW */ 1 FROM",
        r#"SELECT "TBLPROPERTIES" FROM"#,
    ] {
        assert_eq!(
            upgrade_error(sql, original()).to_string(),
            original().to_string(),
            "`{sql}` must not be sniffed"
        );
    }
}

/// Backticks are recognized as Spark quoting and steered to ANSI double quotes.
#[test]
fn backticks_are_recognized() {
    let message = upgraded("SELECT * FROM `my db`.`my table`");
    assert_steer(&message, "backtick", "double quotes");
}

/// The headline behavior: a failed parse carrying a Spark-ism comes back naming the token, the
/// native equivalent, and the Spark door. Several isms in one test because the CONTRACT is
/// per-message, and it must hold for every recognized token, not one lucky one.
#[test]
fn spark_isms_upgrade_the_parse_error() {
    let cases: &[(&str, &str, &str)] = &[
        (
            "CREATE TABLE c.s.t USING iceberg AS SELECT 1 AS a",
            "USING",
            "no USING clause",
        ),
        (
            "CREATE TABLE c.s.t TBLPROPERTIES ('k' = 'v') AS SELECT 1 AS a",
            "TBLPROPERTIES",
            "WITH (format = 'PARQUET'",
        ),
        (
            "CREATE TABLE c.s.t PARTITIONED BY (months(ts)) AS SELECT 1 AS a",
            "PARTITIONED BY",
            "partitioning = ",
        ),
        (
            "INSERT OVERWRITE c.s.t SELECT 1",
            "INSERT OVERWRITE",
            "MERGE INTO",
        ),
        ("SELECT * FROM `t`", "backtick", "double quotes"),
    ];
    for (sql, token, equivalent) in cases {
        let message = upgraded(sql);
        assert_steer(&message, token, equivalent);
    }
}

/// `TBLPROPERTIES` steers to the WITH clause.
#[test]
fn tblproperties_is_recognized() {
    let message = upgraded(
        "CREATE TABLE c.s.t TBLPROPERTIES ('write.merge.mode' = 'merge-on-read') AS SELECT 1 AS a",
    );
    assert_steer(&message, "TBLPROPERTIES", "WITH (format = 'PARQUET'");
}

/// `PARTITIONED BY` steers to `partitioning = ARRAY[…]`.
#[test]
fn partitioned_by_is_recognized() {
    let message = upgraded("CREATE TABLE c.s.t PARTITIONED BY (months(ts)) AS SELECT 1 AS a");
    assert_steer(&message, "PARTITIONED BY", "partitioning = ");
}

/// `INSERT OVERWRITE` steers to the three Q9-sanctioned alternatives.
#[test]
fn insert_overwrite_is_recognized() {
    let message = upgraded("INSERT OVERWRITE c.s.t SELECT 1");
    assert_steer(&message, "INSERT OVERWRITE", "CREATE OR REPLACE TABLE");
    assert!(
        message.contains("MERGE INTO"),
        "must offer MERGE: {message}"
    );
}

/// The Spark `SYSTEM_*` time-travel spellings steer to the ANSI ones.
#[test]
fn system_time_travel_spellings_are_recognized() {
    let version = upgraded("SELECT * FROM c.s.t FOR SYSTEM_VERSION AS OF 12345");
    assert_steer(&version, "SYSTEM_VERSION", "FOR VERSION AS OF");

    let time = upgraded("SELECT * FROM c.s.t FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01'");
    assert_steer(&time, "SYSTEM_TIME", "FOR TIMESTAMP AS OF");
}

/// Bare `VERSION AS OF` (Spark's optional-FOR form) steers to the mandatory-FOR spelling.
#[test]
fn bare_as_of_without_for_is_recognized() {
    let message = upgraded("SELECT * FROM c.s.t VERSION AS OF 12345");
    assert_steer(&message, "without FOR", "FOR VERSION AS OF 12345");
}

/// …but the correct `FOR VERSION AS OF` spelling is NOT flagged as a Spark-ism (that would be an
/// insulting error for someone who wrote the right thing and hit an unrelated failure).
#[test]
fn correct_for_spelling_is_not_flagged_as_spark() {
    let message = upgrade_error(
        "SELECT * FROM c.s.t FOR VERSION AS OF 12345",
        DataFusionError::Plan("boom".to_string()),
    )
    .to_string();
    assert!(
        !message.contains("without FOR"),
        "the correct spelling must not be flagged: {message}"
    );
}

/// `NAMESPACE` / `DATABASE` / `DBPROPERTIES` steer to `SCHEMA` + `WITH`.
#[test]
fn namespace_spellings_are_recognized() {
    for sql in [
        "CREATE NAMESPACE ice.sales LOCATION 'file:///w/sales'",
        "CREATE DATABASE ice.sales",
        "CREATE SCHEMA ice.sales WITH DBPROPERTIES ('location' = 'file:///w')",
    ] {
        let message = upgraded(sql);
        assert!(
            message.contains("CREATE SCHEMA c.s WITH (location"),
            "`{sql}` must steer to CREATE SCHEMA … WITH: {message}"
        );
        assert!(
            message.contains("SparkDialect"),
            "`{sql}` must name the Spark door: {message}"
        );
    }
}

/// `LATERAL VIEW` steers to `UNNEST`.
#[test]
fn lateral_view_is_recognized() {
    let message = upgraded("SELECT x FROM t LATERAL VIEW explode(arr) AS x");
    assert_steer(&message, "LATERAL VIEW", "UNNEST");
}

/// Top-level branch/tag DDL steers to the ALTER-scoped spelling.
#[test]
fn top_level_branch_ddl_is_recognized() {
    let message = upgraded("CREATE BRANCH audit IN ice.sales.orders");
    assert_steer(&message, "BRANCH", "ALTER TABLE t CREATE BRANCH");
}

/// `CALL cat.system.<proc>` steers to the callable operation.
#[test]
fn call_system_procedure_is_recognized() {
    let message = upgraded("CALL ice.system.expire_snapshots(table => 'sales.orders')");
    assert_steer(&message, "CALL", "callable operation");
}

/// The scoping property, stated as its counterexamples: ANSI-LEGAL SQL that merely failed for an
/// unrelated reason (a missing table) is never answered with "this looks like Spark SQL".
///
/// Each row here is a real false positive the unscoped token table produced: `USING` is the ANSI
/// join clause, and `tag` / `branch` / `namespace` / `database` / `call` are ordinary column
/// names. The doc-comment claim ("bounded false positives") is only worth what these pin.
#[test]
fn ansi_legal_statements_are_never_steered_to_the_spark_door() {
    for sql in [
        "SELECT * FROM a JOIN b USING (id)",
        "SELECT a.id FROM a INNER JOIN b USING (id) WHERE a.id > 1",
        "SELECT tag FROM t",
        "SELECT branch, tag FROM t ORDER BY branch",
        "SELECT namespace FROM t",
        "SELECT database FROM t",
        "SELECT call FROM system",
        "DELETE FROM t WHERE tag = 1",
    ] {
        assert_eq!(
            upgrade_error(sql, original()).to_string(),
            original().to_string(),
            "`{sql}` is ANSI-legal and must be returned untouched"
        );
    }
}

/// …and the scoping does not disarm the rules: the same tokens, in the statement shapes that
/// really ARE Spark-isms, still steer.
#[test]
fn scoped_rules_still_fire_in_their_own_statement_shapes() {
    assert_steer(
        &upgraded("CREATE TABLE c.s.t USING iceberg AS SELECT 1 AS a"),
        "USING",
        "there is no USING clause",
    );
    assert_steer(
        &upgraded("DROP NAMESPACE ice.sales"),
        "NAMESPACE",
        "CREATE SCHEMA c.s WITH (location",
    );
    assert_steer(
        &upgraded("DROP BRANCH audit IN ice.sales.orders"),
        "BRANCH",
        "ALTER TABLE t CREATE BRANCH",
    );
}

/// Token matching is case-insensitive but boundary-aware — `using_col` is not `USING`.
#[test]
fn matching_is_case_insensitive_and_boundary_aware() {
    let lower = upgraded("create table c.s.t using iceberg as select 1 as a");
    assert!(lower.contains("USING"), "lower case must match: {lower}");

    let identifier = upgrade_error("SELECT using_col FROM", original()).to_string();
    assert_eq!(
        identifier,
        original().to_string(),
        "`using_col` must not match `USING`"
    );
}
