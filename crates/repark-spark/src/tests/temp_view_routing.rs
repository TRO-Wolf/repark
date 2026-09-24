use super::super::*;
use super::common::*;

use datafusion::sql::sqlparser::parser::ParserError;

use crate::view_ddl::execute::NO_TEMP_VIEW_HOME;
use crate::view_ddl::parse::{
    CreateTempViewStatement, GLOBAL_TEMP_VIEW_REFUSAL, try_parse_create_temp_view,
};

fn parsed(sql: &str) -> CreateTempViewStatement {
    try_parse_create_temp_view(sql)
        .unwrap_or_else(|| panic!("must match: {sql}"))
        .unwrap_or_else(|error| panic!("must parse: {sql}: {error}"))
}

fn refusal(sql: &str) -> DataFusionError {
    match try_parse_create_temp_view(sql) {
        Some(Err(error)) => error,
        Some(Ok(_)) => panic!("must refuse: {sql}"),
        None => panic!("must match: {sql}"),
    }
}

fn parse_exception_text(sql: &str) -> String {
    let error = refusal(sql);
    let DataFusionError::SQL(inner, None) = error else {
        panic!("expected a ParseException-class SQL error for {sql}, got {error:?}");
    };
    let ParserError::ParserError(message) = *inner else {
        panic!("expected ParserError::ParserError for {sql}");
    };
    message
}

#[test]
fn temp_view_heads_parse_with_the_body_verbatim() {
    let statement = parsed("CREATE TEMPORARY VIEW v AS SELECT * FROM sc.ns.t WHERE id > 0");
    assert!(!statement.or_replace);
    assert_eq!(statement.name.value, "v");
    assert_eq!(statement.name.quote_style, None);
    assert!(statement.aliases.is_empty());
    assert_eq!(statement.body_sql, "SELECT * FROM sc.ns.t WHERE id > 0");
    let statement = parsed("CREATE OR REPLACE TEMP VIEW `V w` AS SELECT 1 AS id;");
    assert!(statement.or_replace);
    assert_eq!(statement.name.value, "V w");
    assert_eq!(statement.name.quote_style, Some('`'));
    assert_eq!(statement.body_sql, "SELECT 1 AS id");
}

#[test]
fn temp_view_aliases_and_comment_parse() {
    let statement = parsed(
        "CREATE TEMPORARY VIEW va (i COMMENT 'the id', d) COMMENT 'doc' AS SELECT id, data FROM t",
    );
    assert_eq!(
        statement.aliases,
        vec![
            ("i".to_string(), Some("the id".to_string())),
            ("d".to_string(), None)
        ]
    );
    assert_eq!(statement.body_sql, "SELECT id, data FROM t");
}

#[test]
fn temp_view_bodies_opening_a_query_parse() {
    for (sql, body) in [
        (
            "CREATE TEMP VIEW v AS WITH c AS (SELECT 1 AS id) SELECT * FROM c",
            "WITH c AS (SELECT 1 AS id) SELECT * FROM c",
        ),
        ("CREATE TEMP VIEW v AS VALUES (1), (2)", "VALUES (1), (2)"),
        ("CREATE TEMP VIEW v AS (SELECT 1 AS id)", "(SELECT 1 AS id)"),
        ("CREATE TEMP VIEW v AS TABLE sc.ns.t", "TABLE sc.ns.t"),
        (
            "CREATE TEMP VIEW v AS FROM sc.ns.t SELECT id",
            "FROM sc.ns.t SELECT id",
        ),
    ] {
        assert_eq!(parsed(sql).body_sql, body, "{sql}");
    }
}

#[test]
fn near_miss_statements_do_not_match_the_temp_view_head() {
    for sql in [
        "CREATE TEMPORARY TABLE tt (id INT)",
        "CREATE TEMPORARY TABLE tt AS SELECT 1 AS id",
        "DROP TEMPORARY FUNCTION IF EXISTS nofn",
        "CREATE TEMPORARY FUNCTION f AS 'com.example.F'",
        "CREATE VIEW sc.ns.v AS SELECT 1 AS id",
        "CREATE OR REPLACE VIEW sc.ns.v AS SELECT 1 AS id",
        "CREATE TABLE t (id INT)",
        "DROP VIEW v",
        "SELECT 1",
    ] {
        assert!(try_parse_create_temp_view(sql).is_none(), "{sql}");
    }
}

#[test]
fn global_temporary_view_refuses_as_unsupported() {
    for sql in [
        "CREATE GLOBAL TEMPORARY VIEW g AS SELECT 1 AS id",
        "CREATE OR REPLACE GLOBAL TEMP VIEW g2 AS SELECT 1 AS id",
    ] {
        let error = refusal(sql);
        let DataFusionError::NotImplemented(message) = error else {
            panic!("expected NotImplemented for {sql}, got {error:?}");
        };
        assert_eq!(message, GLOBAL_TEMP_VIEW_REFUSAL, "{sql}");
    }
}

#[test]
fn temp_view_parse_refusals_carry_the_measured_spark_text() {
    for (sql, expected) in [
        (
            "CREATE TEMPORARY VIEW IF NOT EXISTS v AS SELECT 1 AS id",
            "It is not allowed to define a TEMPORARY view with IF NOT EXISTS.",
        ),
        (
            "CREATE OR REPLACE TEMPORARY VIEW IF NOT EXISTS v AS SELECT 1 AS id",
            "CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed.",
        ),
        (
            "CREATE TEMPORARY VIEW vp TBLPROPERTIES ('k'='v') AS SELECT 1 AS id",
            "Operation not allowed: TBLPROPERTIES can't coexist with CREATE TEMPORARY VIEW.",
        ),
        (
            "CREATE TEMPORARY VIEW a.b AS SELECT 1 AS id",
            "[TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS] CREATE TEMPORARY VIEW or the corresponding Dataset APIs only accept single-part view names, but got: `a`.`b`. SQLSTATE: 428EK",
        ),
        (
            "CREATE OR REPLACE TEMPORARY VIEW a.b AS SELECT 1 AS id",
            "[TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS] CREATE TEMPORARY VIEW or the corresponding Dataset APIs only accept single-part view names, but got: `a`.`b`. SQLSTATE: 428EK",
        ),
        (
            "CREATE TEMPORARY VIEW sc.ns.q AS SELECT 1 AS id",
            "[IDENTIFIER_TOO_MANY_NAME_PARTS] `sc`.`ns`.`q` is not a valid identifier as it has more than 2 name parts. SQLSTATE: 42601",
        ),
        (
            "CREATE TEMPORARY VIEW vx AS INSERT INTO sc.ns.t VALUES (9, 'x', 'y')",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT'. SQLSTATE: 42601",
        ),
    ] {
        assert_eq!(parse_exception_text(sql), expected, "{sql}");
    }
}

#[tokio::test]
async fn temp_view_without_a_session_home_refuses_loud() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let error = execute(&ctx, &catalogs, "CREATE TEMPORARY VIEW v AS SELECT 1 AS id")
        .await
        .expect_err("a session-less door has no temp-view home");
    let DataFusionError::NotImplemented(message) = error else {
        panic!("expected NotImplemented, got {error:?}");
    };
    assert_eq!(message, NO_TEMP_VIEW_HOME);
}

#[tokio::test]
async fn temp_view_parse_refusal_wins_before_the_session_check() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TEMPORARY VIEW a.b AS SELECT 1 AS id",
    )
    .await
    .expect_err("a two-part temp view name refuses");
    assert!(
        matches!(error, DataFusionError::SQL(_, None)),
        "expected a ParseException-class error, got {error:?}"
    );
    assert_eq!(
        error.to_string(),
        "SQL error: ParserError(\"[TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS] CREATE TEMPORARY VIEW or the corresponding Dataset APIs only accept single-part view names, but got: `a`.`b`. SQLSTATE: 428EK\")"
    );
}

struct StubTempViews {
    existing: Vec<String>,
    registered: std::sync::Mutex<Vec<(String, datafusion::prelude::DataFrame)>>,
}

impl StubTempViews {
    fn with_existing(existing: &[&str]) -> Self {
        Self {
            existing: existing.iter().map(|name| (*name).to_string()).collect(),
            registered: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn registered(&self) -> Vec<(String, datafusion::prelude::DataFrame)> {
        self.registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl repark_core::TempViewSession for StubTempViews {
    fn create_or_replace_temp_view_from(
        &self,
        name: &str,
        frame: &datafusion::prelude::DataFrame,
    ) -> repark_common::Result<()> {
        self.registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((name.to_string(), frame.clone()));
        Ok(())
    }

    fn resolve_temp_view_home_ref(&self, name: &str) -> repark_common::Result<Option<Vec<String>>> {
        Ok(self
            .existing
            .iter()
            .any(|existing| existing == name)
            .then(|| {
                vec![
                    "datafusion".to_string(),
                    "public".to_string(),
                    name.to_string(),
                ]
            }))
    }
}

async fn run_with_session(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    stub: &StubTempViews,
) -> datafusion::error::Result<datafusion::prelude::DataFrame> {
    crate::router::execute_in_session(
        ctx,
        catalogs,
        sql,
        &std::collections::HashSet::<String>::new(),
        &crate::write_options::StatementWriteOptions::empty(),
        Some(stub),
    )
    .await
}

async fn id_name_rows(frame: datafusion::prelude::DataFrame) -> Vec<(i32, String)> {
    let batches = frame.collect().await.expect("collect temp view frame");
    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("ids");
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("names");
            (0..batch.num_rows())
                .map(move |index| (ids.value(index), names.value(index).to_string()))
        })
        .collect()
}

#[tokio::test]
async fn temp_view_body_resolves_a_bare_temp_name_before_the_current_namespace() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.src AS SELECT CAST(7 AS INT) AS id, 'z' AS name",
    )
    .await
    .expect("catalog table named like the temp view")
    .collect()
    .await
    .expect("collect ctas");
    crate::use_ddl::set_session_defaults(&ctx, &catalogs, "ice", "sales");
    let stub = StubTempViews::with_existing(&["src"]);
    run_with_session(
        &ctx,
        &catalogs,
        "CREATE TEMPORARY VIEW v AS SELECT id, name FROM src ORDER BY id",
        &stub,
    )
    .await
    .expect("the temp view registers");
    let registered = stub.registered();
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].0, "v");
    assert_eq!(
        id_name_rows(registered[0].1.clone()).await,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string())
        ]
    );
    run_with_session(
        &ctx,
        &catalogs,
        "CREATE TEMPORARY VIEW w AS SELECT id, name FROM sales.src",
        &stub,
    )
    .await
    .expect("a two-part name reads the catalog table");
    let registered = stub.registered();
    assert_eq!(registered[1].0, "w");
    assert_eq!(
        id_name_rows(registered[1].1.clone()).await,
        vec![(7, "z".to_string())]
    );
}

#[tokio::test]
async fn temp_view_existing_name_refuses_and_or_replace_registers() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let stub = StubTempViews::with_existing(&["src"]);
    let error = run_with_session(
        &ctx,
        &catalogs,
        "CREATE TEMPORARY VIEW src AS SELECT 1 AS id",
        &stub,
    )
    .await
    .expect_err("an existing temp view name refuses a plain CREATE");
    let DataFusionError::Plan(message) = error else {
        panic!("expected Plan, got {error:?}");
    };
    assert_eq!(
        message,
        "[TEMP_TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create the temporary view `src` because it already exists.\nChoose a different name, drop or replace the existing view. SQLSTATE: 42P07"
    );
    assert!(stub.registered().is_empty());
    run_with_session(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TEMPORARY VIEW src AS SELECT CAST(9 AS INT) AS id, 'q' AS name",
        &stub,
    )
    .await
    .expect("OR REPLACE registers over an existing name");
    let registered = stub.registered();
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].0, "src");
    assert_eq!(
        id_name_rows(registered[0].1.clone()).await,
        vec![(9, "q".to_string())]
    );
}

#[tokio::test]
async fn temp_view_quoted_name_reaches_the_session_quoted() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let stub = StubTempViews::with_existing(&[]);
    run_with_session(
        &ctx,
        &catalogs,
        "CREATE TEMPORARY VIEW `My View` AS SELECT CAST(1 AS INT) AS id, 'a' AS name",
        &stub,
    )
    .await
    .expect("a quoted temp view name registers");
    assert_eq!(stub.registered()[0].0, "\"My View\"");
}

#[tokio::test]
async fn temp_view_write_body_refuses_before_registering() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let stub = StubTempViews::with_existing(&[]);
    let error = run_with_session(
        &ctx,
        &catalogs,
        "CREATE TEMPORARY VIEW w AS WITH s AS (SELECT 9 AS id) INSERT INTO ice.sales.t SELECT id FROM s",
        &stub,
    )
    .await
    .expect_err("a WITH ... INSERT body is a write, not a view");
    let DataFusionError::Plan(message) = error else {
        panic!("expected Plan, got {error:?}");
    };
    assert_eq!(message, crate::view_ddl::read::TEMP_VIEW_WRITE_BODY_REFUSAL);
    assert!(stub.registered().is_empty());
}
