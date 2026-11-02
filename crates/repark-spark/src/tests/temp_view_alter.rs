use super::common::*;

use std::collections::HashSet;

use datafusion::prelude::DataFrame;

use crate::write_options::StatementWriteOptions;

struct NamedViews {
    names: Vec<&'static str>,
}

impl repark_core::TempViewSession for NamedViews {
    fn create_or_replace_temp_view_from(
        &self,
        _name: &str,
        _frame: &DataFrame,
    ) -> repark_common::Result<()> {
        Ok(())
    }

    fn resolve_temp_view_home_ref(&self, name: &str) -> repark_common::Result<Option<Vec<String>>> {
        let name = name.trim_matches('"').to_lowercase();
        Ok(self
            .names
            .contains(&name.as_str())
            .then(|| vec!["datafusion".to_string(), "public".to_string(), name.clone()]))
    }

    fn temp_view_home(&self) -> repark_common::Result<Vec<String>> {
        Ok(vec!["datafusion".to_string(), "public".to_string()])
    }

    fn list_temp_view_names(&self) -> repark_common::Result<Vec<String>> {
        Ok(self.names.iter().map(ToString::to_string).collect())
    }

    fn drop_temp_view(&self, _name: &str) -> repark_common::Result<bool> {
        Ok(false)
    }
}

const NO_ALTERNATIVE: [(&str, &str); 22] = [
    ("ALTER TABLE t ADD COLUMN z INT", "ADD COLUMN"),
    ("ALTER TABLE t ADD COLUMN s.x INT", "ADD COLUMN"),
    ("ALTER TABLE t ADD z INT", "ADD COLUMN"),
    ("ALTER TABLE t ADD COLUMNS (z INT)", "ADD COLUMNS"),
    ("ALTER TABLE t DROP COLUMN v", "DROP COLUMNS"),
    ("ALTER TABLE t DROP COLUMNS IF EXISTS (v)", "DROP COLUMNS"),
    ("ALTER TABLE IF EXISTS t DROP v", "DROP COLUMNS"),
    ("ALTER TABLE t RENAME COLUMN v TO v2", "RENAME COLUMN"),
    ("ALTER TABLE t ALTER COLUMN id TYPE BIGINT", "ALTER COLUMN"),
    ("ALTER TABLE t ALTER v COMMENT 'c'", "ALTER COLUMN"),
    ("ALTER TABLE t CHANGE COLUMN v COMMENT 'c'", "CHANGE COLUMN"),
    ("ALTER TABLE t CHANGE v v STRING", "CHANGE COLUMN"),
    ("ALTER TABLE t REPLACE COLUMNS (id INT)", "REPLACE COLUMNS"),
    ("ALTER TABLE t SET LOCATION 'relocated'", "SET LOCATION ..."),
    (
        "ALTER TABLE t PARTITION (id = (1)) SET LOCATION 'relocated'",
        "SET LOCATION ...",
    ),
    (
        "ALTER TABLE t ADD IF NOT EXISTS PARTITION (id=1)",
        "ADD PARTITION ...",
    ),
    ("ALTER TABLE t DROP PARTITION (id=1)", "DROP PARTITION ..."),
    (
        "ALTER TABLE t PARTITION (id=1) RENAME TO PARTITION (id=2)",
        "RENAME TO PARTITION",
    ),
    ("ALTER TABLE t CLUSTER BY (id)", "CLUSTER BY"),
    ("ALTER TABLE t RECOVER PARTITIONS", "RECOVER PARTITIONS"),
    (
        "ALTER TABLE t ADD CONSTRAINT c1 CHECK (id > 0)",
        "ADD CONSTRAINT",
    ),
    ("ALTER TABLE t DROP CONSTRAINT c1", "DROP CONSTRAINT"),
];

const USE_ALTER_VIEW: [(&str, &str); 4] = [
    (
        "ALTER TABLE t SET TBLPROPERTIES ('k1'='x')",
        "SET TBLPROPERTIES",
    ),
    (
        "ALTER TABLE t UNSET TBLPROPERTIES ('k0')",
        "UNSET TBLPROPERTIES",
    ),
    (
        "ALTER TABLE t SET SERDEPROPERTIES ('a'='b')",
        "SET [SERDE|SERDEPROPERTIES]",
    ),
    ("ALTER TABLE t SET SERDE 'x'", "SET [SERDE|SERDEPROPERTIES]"),
];

const EXTENSIONS: [&str; 12] = [
    "ALTER TABLE t ADD PARTITION FIELD id",
    "ALTER TABLE t DROP PARTITION FIELD id",
    "ALTER TABLE t REPLACE PARTITION FIELD id WITH bucket(2, id)",
    "ALTER TABLE t WRITE ORDERED BY id",
    "ALTER TABLE t WRITE DISTRIBUTED BY PARTITION",
    "ALTER TABLE t SET IDENTIFIER FIELDS id",
    "ALTER TABLE t DROP IDENTIFIER FIELDS id",
    "ALTER TABLE t CREATE BRANCH b1",
    "ALTER TABLE t CREATE OR REPLACE TAG g1",
    "ALTER TABLE t REPLACE BRANCH b1",
    "ALTER TABLE t DROP BRANCH b1",
    "ALTER TABLE t DROP TAG g1",
];

async fn outcome(views: &dyn repark_core::TempViewSession, sql: &str) -> String {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    match crate::router::execute_in_session(
        &ctx,
        &catalogs,
        sql,
        &HashSet::new(),
        &StatementWriteOptions::empty(),
        Some(views),
    )
    .await
    {
        Ok(frame) => match frame.collect().await {
            Ok(_) => "ok".to_string(),
            Err(error) => error.to_string(),
        },
        Err(error) => error.to_string(),
    }
}

fn view_t() -> NamedViews {
    NamedViews { names: vec!["t"] }
}

#[tokio::test]
async fn every_alter_operation_through_a_view_name_refuses_with_spark_text() {
    for (sql, operation) in NO_ALTERNATIVE {
        assert_eq!(
            outcome(&view_t(), sql).await,
            format!(
                "Error during planning: [EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'ALTER TABLE ... \
                 {operation}' expects a table but `t` is a view.  SQLSTATE: 42809"
            ),
            "{sql}"
        );
    }
    for (sql, operation) in USE_ALTER_VIEW {
        assert_eq!(
            outcome(&view_t(), sql).await,
            format!(
                "Error during planning: [EXPECT_TABLE_NOT_VIEW.USE_ALTER_VIEW] 'ALTER TABLE ... \
                 {operation}' expects a table but `t` is a view. Please use ALTER VIEW instead. \
                 SQLSTATE: 42809"
            ),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn the_refusal_shows_the_written_name() {
    assert_eq!(
        outcome(&view_t(), "ALTER TABLE `T` DROP COLUMN v").await,
        "Error during planning: [EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'ALTER TABLE ... DROP \
         COLUMNS' expects a table but `T` is a view.  SQLSTATE: 42809"
    );
}

#[tokio::test]
async fn iceberg_extension_alters_pass_the_view_check() {
    for sql in EXTENSIONS {
        let outcome = outcome(&view_t(), sql).await;
        assert!(
            !outcome.contains("EXPECT_TABLE_NOT_VIEW"),
            "{sql}: {outcome}"
        );
    }
}

#[tokio::test]
async fn an_alter_with_no_view_of_that_name_passes_the_view_check() {
    let views = NamedViews { names: vec!["w"] };
    for (sql, _) in NO_ALTERNATIVE.iter().chain(USE_ALTER_VIEW.iter()) {
        let outcome = outcome(&views, sql).await;
        assert!(
            !outcome.contains("EXPECT_TABLE_NOT_VIEW"),
            "{sql}: {outcome}"
        );
    }
}

#[tokio::test]
async fn rename_to_a_qualified_or_malformed_destination_refuses_with_spark_text() {
    for (sql, expected) in [
        (
            "ALTER TABLE t RENAME TO a.b.t2",
            "Error during planning: [IDENTIFIER_TOO_MANY_NAME_PARTS] `a`.`b`.`t2` is not a valid \
             identifier as it has more than 2 name parts. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE t RENAME TO ns.t2",
            "Error during planning: RENAME TABLE source and destination databases do not match: \
             'default' != 'ns'.",
        ),
        (
            "ALTER TABLE t RENAME TO default.t2",
            "Error during planning: RENAME TEMPORARY VIEW from '`t`' to '`default`.`t2`': cannot \
             specify database name 'default' in the destination table.",
        ),
        (
            "ALTER TABLE t RENAME TO w",
            "Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view \
             `w` because it already exists.\nChoose a different name, drop or replace the \
             existing object, or add the IF NOT EXISTS clause to tolerate pre-existing objects. \
             SQLSTATE: 42P07",
        ),
    ] {
        let views = NamedViews {
            names: vec!["t", "w"],
        };
        assert_eq!(outcome(&views, sql).await, expected, "{sql}");
    }
    let trailing = outcome(&view_t(), "ALTER TABLE t RENAME TO t2, ADD COLUMN z INT").await;
    assert!(
        trailing.contains("[PARSE_SYNTAX_ERROR] Syntax error at or near ','"),
        "{trailing}"
    );
}

const PREFIXED: [(&str, &str); 12] = [
    ("ALTER TABLE ONLY t DROP COLUMN v", "DROP COLUMNS"),
    (
        "ALTER TABLE IF EXISTS ONLY t ADD COLUMN z INT",
        "ADD COLUMN",
    ),
    (
        "ALTER TABLE t ON CLUSTER c1 RENAME COLUMN v TO v2",
        "RENAME COLUMN",
    ),
    (
        "ALTER ICEBERG TABLE t ALTER COLUMN id TYPE BIGINT",
        "ALTER COLUMN",
    ),
    ("ALTER TABLE ONLY t RENAME TO t2", "RENAME"),
    ("ALTER TABLE t ON CLUSTER c1 RENAME TO t2", "RENAME"),
    ("ALTER TABLE IF EXISTS t RENAME TO t2", "RENAME"),
    ("ALTER ICEBERG TABLE t ADD PARTITION FIELD id", "ADD"),
    ("ALTER TABLE ONLY t WRITE ORDERED BY id", "WRITE"),
    ("ALTER TABLE t SWAP WITH w", "SWAP"),
    ("ALTER TABLE t SET OWNER TO someone", "SET"),
    (
        "ALTER TABLE t DEFAULT COLLATION UTF8_LCASE",
        "DEFAULT COLLATION",
    ),
];

#[tokio::test]
async fn every_prefix_and_unknown_head_through_a_view_name_refuses() {
    for (sql, operation) in PREFIXED {
        assert_eq!(
            outcome(&view_t(), sql).await,
            format!(
                "Error during planning: [EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'ALTER TABLE ... \
                 {operation}' expects a table but `t` is a view.  SQLSTATE: 42809"
            ),
            "{sql}"
        );
    }
    assert_eq!(
        outcome(&view_t(), "ALTER TABLE ONLY t UNSET TBLPROPERTIES ('k0')").await,
        "Error during planning: [EXPECT_TABLE_NOT_VIEW.USE_ALTER_VIEW] 'ALTER TABLE ... UNSET \
         TBLPROPERTIES' expects a table but `t` is a view. Please use ALTER VIEW instead. \
         SQLSTATE: 42809"
    );
}

#[tokio::test]
async fn a_prefixed_alter_with_no_view_of_that_name_passes_the_view_check() {
    let views = NamedViews { names: vec!["w"] };
    for (sql, _) in PREFIXED {
        let outcome = outcome(&views, sql).await;
        assert!(
            !outcome.contains("EXPECT_TABLE_NOT_VIEW"),
            "{sql}: {outcome}"
        );
    }
}

#[tokio::test]
async fn a_leading_semicolon_refuses_before_every_temp_view_check() {
    for sql in [
        ";ALTER TABLE t DROP COLUMN v",
        "; ALTER TABLE t RENAME TO t2",
        ";;ALTER ICEBERG TABLE t DROP COLUMN v",
        ";\nALTER TABLE ONLY t ADD COLUMN z INT",
        "/* c */;ALTER TABLE t SET TBLPROPERTIES ('k1'='x')",
        "-- c\n;ALTER TABLE t DROP COLUMN v",
        ";DROP TABLE t",
        ";INSERT INTO t VALUES (1)",
        ";TRUNCATE TABLE t",
    ] {
        let outcome = outcome(&view_t(), sql).await;
        assert!(
            outcome.contains("[PARSE_SYNTAX_ERROR] Syntax error at or near ';'. SQLSTATE: 42601"),
            "{sql}: {outcome}"
        );
    }
}

#[tokio::test]
async fn a_bare_identifier_word_after_drop_is_a_column_and_a_double_quoted_target_refuses() {
    for (sql, operation) in [
        ("ALTER TABLE t DROP identifier", "DROP COLUMNS"),
        ("ALTER TABLE t DROP `IDENTIFIER`", "DROP COLUMNS"),
        ("ALTER TABLE t DROP tag, v", "DROP COLUMNS"),
        ("ALTER TABLE t DROP `branch`", "DROP COLUMNS"),
        ("ALTER TABLE t SET identifier", "SET"),
        ("ALTER TABLE \"t\" RENAME TO t2", "RENAME"),
        ("ALTER TABLE \"t\" WRITE ORDERED BY id", "WRITE"),
    ] {
        assert_eq!(
            outcome(&view_t(), sql).await,
            format!(
                "Error during planning: [EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'ALTER TABLE ... \
                 {operation}' expects a table but `t` is a view.  SQLSTATE: 42809"
            ),
            "{sql}"
        );
    }
}

const UNDECIDED: &str = "the temporary view lookup cannot decide this name";

struct UndecidedViews;

impl repark_core::TempViewSession for UndecidedViews {
    fn create_or_replace_temp_view_from(
        &self,
        _name: &str,
        _frame: &DataFrame,
    ) -> repark_common::Result<()> {
        Ok(())
    }

    fn resolve_temp_view_home_ref(
        &self,
        _name: &str,
    ) -> repark_common::Result<Option<Vec<String>>> {
        Err(repark_common::Error::Analysis(UNDECIDED.to_string()))
    }

    fn temp_view_home(&self) -> repark_common::Result<Vec<String>> {
        Ok(vec!["datafusion".to_string(), "public".to_string()])
    }

    fn list_temp_view_names(&self) -> repark_common::Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn drop_temp_view(&self, _name: &str) -> repark_common::Result<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn an_undecided_view_lookup_refuses_every_head_that_consults_it() {
    for sql in [
        "ALTER TABLE t DROP COLUMN v",
        "ALTER TABLE 1t RENAME TO t2",
        "INSERT INTO t VALUES (1)",
        "DELETE FROM t WHERE id = 1",
        "TRUNCATE TABLE t",
        "DROP TABLE t",
    ] {
        assert_eq!(
            outcome(&UndecidedViews, sql).await,
            format!("Error during planning: {UNDECIDED}"),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn a_double_quoted_drop_or_truncate_target_naming_a_view_refuses() {
    for (sql, near) in [
        ("DROP TABLE \"t\"", "'\"t\"'"),
        ("TRUNCATE TABLE \"t\"", "'\"t\"'"),
    ] {
        let outcome = outcome(&view_t(), sql).await;
        assert!(
            outcome.contains(&format!(
                "[PARSE_SYNTAX_ERROR] Syntax error at or near {near}. SQLSTATE: 42601"
            )),
            "{sql}: {outcome}"
        );
    }
}
