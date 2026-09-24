use super::super::*;
use super::common::*;

use datafusion::sql::sqlparser::parser::ParserError;

use crate::view_ddl::execute::NO_TEMP_VIEW_HOME;
use crate::view_ddl::temp_parse::{
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
            "CREATE TEMPORARY VIEW a.b.c.d AS SELECT 1 AS id",
            "[IDENTIFIER_TOO_MANY_NAME_PARTS] `a`.`b`.`c`.`d` is not a valid identifier as it has more than 2 name parts. SQLSTATE: 42601",
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
    dropped: std::sync::Mutex<Vec<String>>,
    registered: std::sync::Mutex<Vec<(String, datafusion::prelude::DataFrame)>>,
}

impl StubTempViews {
    fn with_existing(existing: &[&str]) -> Self {
        Self {
            existing: existing.iter().map(|name| (*name).to_string()).collect(),
            dropped: std::sync::Mutex::new(Vec::new()),
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

    fn temp_view_home(&self) -> repark_common::Result<Vec<String>> {
        Ok(vec!["datafusion".to_string(), "public".to_string()])
    }

    fn list_temp_view_names(&self) -> repark_common::Result<Vec<String>> {
        Ok(self.existing.clone())
    }

    fn drop_temp_view(&self, name: &str) -> repark_common::Result<bool> {
        self.dropped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(name.to_string());
        Ok(self.existing.iter().any(|existing| existing == name))
    }

    fn resolve_temp_view_home_ref(&self, name: &str) -> repark_common::Result<Option<Vec<String>>> {
        let name = name.trim_matches('"');
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
    for (body, verb) in [
        ("INSERT INTO ice.sales.t SELECT id FROM s", "INSERT"),
        ("UPDATE ice.sales.t SET id = 1", "UPDATE"),
        ("DELETE FROM ice.sales.t", "DELETE"),
        (
            "MERGE INTO ice.sales.t USING s ON ice.sales.t.id = s.id WHEN MATCHED THEN DELETE",
            "MERGE",
        ),
    ] {
        let sql = format!("CREATE TEMPORARY VIEW w AS WITH s AS (SELECT 9 AS id) {body}");
        let error = run_with_session(&ctx, &catalogs, &sql, &stub)
            .await
            .expect_err("a WITH ... write body is a write, not a view");
        let DataFusionError::SQL(inner, None) = error else {
            panic!("expected a ParseException-class SQL error for {verb}, got {error:?}");
        };
        let ParserError::ParserError(message) = *inner else {
            panic!("expected ParserError::ParserError for {verb}");
        };
        assert_eq!(
            message,
            format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{verb}'. SQLSTATE: 42601")
        );
    }
    assert!(stub.registered().is_empty());
}

impl StubTempViews {
    fn dropped(&self) -> Vec<String> {
        self.dropped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

async fn string_rows(frame: datafusion::prelude::DataFrame) -> Vec<Vec<Option<String>>> {
    let batches = frame.collect().await.expect("collect frame");
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cells = batch
                .columns()
                .iter()
                .map(|column| {
                    if column.is_null(row) {
                        return None;
                    }
                    Some(
                        datafusion::arrow::util::display::array_value_to_string(column, row)
                            .expect("render cell"),
                    )
                })
                .collect();
            rows.push(cells);
        }
    }
    rows
}

#[tokio::test]
async fn bare_drop_view_of_a_temp_name_drops_the_temp_view() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let stub = StubTempViews::with_existing(&["src"]);
    run_with_session(&ctx, &catalogs, "DROP VIEW src", &stub)
        .await
        .expect("a temp view drop succeeds");
    assert_eq!(stub.dropped(), vec!["\"src\"".to_string()]);
    run_with_session(
        &ctx,
        &catalogs,
        "DROP VIEW `datafusion`.`public`.`src`",
        &stub,
    )
    .await
    .expect("the home-qualified spelling drops the temp view");
    assert_eq!(stub.dropped().len(), 2);
}

#[tokio::test]
async fn qualified_drop_view_falls_through_to_the_catalog() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let stub = StubTempViews::with_existing(&["src"]);
    let error = run_with_session(&ctx, &catalogs, "DROP VIEW ice.sales.src", &stub)
        .await
        .expect_err("the catalog has no view src");
    let DataFusionError::Plan(message) = error else {
        panic!("expected Plan, got {error:?}");
    };
    assert!(message.starts_with("[VIEW_NOT_FOUND] The view sales.src cannot be found."));
    assert!(stub.dropped().is_empty());
    let error = run_with_session(&ctx, &catalogs, "DROP VIEW missing_view", &stub)
        .await
        .expect_err("a bare non-temp name is the catalog drop");
    assert!(matches!(error, DataFusionError::Plan(_)), "{error:?}");
    assert!(stub.dropped().is_empty());
}

#[tokio::test]
async fn describe_of_a_temp_view_answers_null_comments() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let stub = StubTempViews::with_existing(&["src"]);
    for sql in ["DESCRIBE src", "DESCRIBE TABLE EXTENDED src"] {
        let frame = run_with_session(&ctx, &catalogs, sql, &stub)
            .await
            .expect("DESCRIBE of a temp view answers");
        let schema = frame.schema().as_arrow().clone();
        assert!(schema.field(2).is_nullable(), "{sql}");
        assert_eq!(
            string_rows(frame).await,
            vec![
                vec![Some("id".to_string()), Some("int".to_string()), None],
                vec![Some("name".to_string()), Some("string".to_string()), None],
            ],
            "{sql}"
        );
    }
}

#[tokio::test]
async fn show_views_appends_temp_rows_after_catalog_rows() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.cv AS SELECT * FROM src",
    )
    .await
    .expect("catalog view")
    .collect()
    .await
    .expect("collect create view");
    let stub = StubTempViews::with_existing(&["tv", "atv"]);
    let frame = run_with_session(&ctx, &catalogs, "SHOW VIEWS IN ice.sales", &stub)
        .await
        .expect("SHOW VIEWS answers");
    assert_eq!(
        string_rows(frame).await,
        vec![
            vec![
                Some("sales".into()),
                Some("cv".into()),
                Some("false".into())
            ],
            vec![Some(String::new()), Some("atv".into()), Some("true".into())],
            vec![Some(String::new()), Some("tv".into()), Some("true".into())],
        ]
    );
    let frame = run_with_session(&ctx, &catalogs, "SHOW VIEWS LIKE 't*'", &stub)
        .await
        .expect("session-scope SHOW VIEWS answers");
    assert_eq!(
        string_rows(frame).await,
        vec![vec![
            Some(String::new()),
            Some("tv".into()),
            Some("true".into())
        ]]
    );
}

#[tokio::test]
async fn bare_drop_table_of_a_temp_name_drops_the_temp_view() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let stub = StubTempViews::with_existing(&["src"]);
    for sql in [
        "DROP TABLE src",
        "DROP TABLE src PURGE",
        "DROP TABLE IF EXISTS src",
        "DROP TABLE IF EXISTS src PURGE",
        "DROP TABLE `datafusion`.`public`.`src`",
        "DROP VIEW IF EXISTS src",
    ] {
        run_with_session(&ctx, &catalogs, sql, &stub)
            .await
            .unwrap_or_else(|error| panic!("{sql} must drop the temp view: {error}"));
    }
    assert_eq!(stub.dropped(), vec!["\"src\"".to_string(); 6]);
}

#[tokio::test]
async fn qualified_drop_table_falls_through_to_the_catalog() {
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
    let stub = StubTempViews::with_existing(&["src"]);
    run_with_session(&ctx, &catalogs, "DROP TABLE ice.sales.src", &stub)
        .await
        .expect("the qualified DROP TABLE is the catalog drop");
    assert!(stub.dropped().is_empty());
    let exists = catalogs["ice"]
        .table_exists(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "src".to_string(),
        ))
        .await
        .expect("table existence");
    assert!(!exists);
}

const BROKEN_HOME: &str = "no session-local temp-view home";

struct BrokenHomeTempViews;

impl repark_core::TempViewSession for BrokenHomeTempViews {
    fn create_or_replace_temp_view_from(
        &self,
        _name: &str,
        _frame: &datafusion::prelude::DataFrame,
    ) -> repark_common::Result<()> {
        Err(repark_common::Error::Analysis(BROKEN_HOME.to_string()))
    }

    fn resolve_temp_view_home_ref(
        &self,
        _name: &str,
    ) -> repark_common::Result<Option<Vec<String>>> {
        Err(repark_common::Error::Analysis(BROKEN_HOME.to_string()))
    }

    fn temp_view_home(&self) -> repark_common::Result<Vec<String>> {
        Err(repark_common::Error::Analysis(BROKEN_HOME.to_string()))
    }

    fn list_temp_view_names(&self) -> repark_common::Result<Vec<String>> {
        Err(repark_common::Error::Analysis(BROKEN_HOME.to_string()))
    }

    fn drop_temp_view(&self, _name: &str) -> repark_common::Result<bool> {
        Err(repark_common::Error::Analysis(BROKEN_HOME.to_string()))
    }
}

async fn session_error(sql: &str, stub: &dyn repark_core::TempViewSession) -> DataFusionError {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    crate::router::execute_in_session(
        &ctx,
        &catalogs,
        sql,
        &std::collections::HashSet::<String>::new(),
        &crate::write_options::StatementWriteOptions::empty(),
        Some(stub),
    )
    .await
    .err()
    .unwrap_or_else(|| panic!("{sql} must refuse"))
}

#[tokio::test]
async fn a_replaced_temp_view_home_refuses_every_temp_statement_as_analysis() {
    for sql in [
        "DROP VIEW src",
        "DROP TABLE src",
        "DROP VIEW `datafusion`.`public`.`src`",
        "DESCRIBE src",
        "SHOW VIEWS",
        "CREATE TEMPORARY VIEW v AS SELECT 1 AS id",
    ] {
        let error = session_error(sql, &BrokenHomeTempViews).await;
        assert!(
            matches!(error, DataFusionError::Plan(_)),
            "{sql}: {error:?}"
        );
        assert_eq!(
            error.to_string(),
            format!("Error during planning: {BROKEN_HOME}"),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn malformed_temp_view_heads_refuse_with_the_full_plan_text() {
    let stub = StubTempViews::with_existing(&[]);
    for (sql, expected) in [
        (
            "(CREATE TEMPORARY VIEW v AS SELECT 1 AS id",
            "Error during planning: could not parse `CREATE TEMPORARY VIEW`: expected CREATE",
        ),
        (
            "CREATE OR REPLACE \"x\" TEMPORARY VIEW v AS SELECT 1 AS id",
            "Error during planning: could not parse `CREATE TEMPORARY VIEW`: expected TEMPORARY",
        ),
        (
            "CREATE TEMPORARY ( VIEW v AS SELECT 1 AS id",
            "Error during planning: could not parse `CREATE TEMPORARY VIEW`: expected VIEW",
        ),
        (
            "CREATE TEMPORARY VIEW v SELECT 1",
            "Error during planning: could not parse `CREATE TEMPORARY VIEW`: expected AS with the view query, got `CREATE TEMPORARY VIEW v SELECT 1`",
        ),
        (
            "CREATE TEMPORARY VIEW v AS",
            "Error during planning: could not parse `CREATE TEMPORARY VIEW`: the view query after AS is empty",
        ),
        (
            "CREATE TEMPORARY VIEW v AS ;",
            "Error during planning: could not parse `CREATE TEMPORARY VIEW`: the view query after AS is empty",
        ),
    ] {
        let error = session_error(sql, &stub).await;
        assert!(
            matches!(error, DataFusionError::Plan(_)),
            "{sql}: {error:?}"
        );
        assert_eq!(error.to_string(), expected, "{sql}");
    }
    assert!(stub.registered().is_empty());
}

#[tokio::test]
async fn malformed_temp_view_clauses_refuse_with_the_full_plan_text() {
    let stub = StubTempViews::with_existing(&[]);
    for (sql, expected) in [
        (
            "CREATE TEMPORARY VIEW AS SELECT 1 AS id",
            "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: identifier, found: EOF",
        ),
        (
            "CREATE TEMPORARY VIEW v (, i) AS SELECT 1 AS id",
            "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: identifier, found: ,",
        ),
        (
            "CREATE TEMPORARY VIEW v (i AS SELECT 1 AS id",
            "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: ), found: EOF",
        ),
        (
            "CREATE TEMPORARY VIEW v COMMENT 5 AS SELECT 1 AS id",
            "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: literal string, found: 5",
        ),
        (
            "CREATE TEMPORARY VIEW v TBLPROPERTIES ('k') AS SELECT 1 AS id",
            "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: =, found: )",
        ),
        (
            "CREATE TEMPORARY VIEW v extra AS SELECT 1 AS id",
            "Error during planning: could not parse `CREATE TEMPORARY VIEW` at `extra`",
        ),
    ] {
        let error = session_error(sql, &stub).await;
        assert!(
            matches!(error, DataFusionError::Plan(_)),
            "{sql}: {error:?}"
        );
        assert_eq!(error.to_string(), expected, "{sql}");
    }
    assert!(stub.registered().is_empty());
}

#[tokio::test]
async fn malformed_show_views_refuses_through_the_session_door() {
    let stub = StubTempViews::with_existing(&["tv"]);
    for (sql, expected) in [
        (
            "SHOW VIEWS LIKE",
            "Error during planning: SHOW VIEWS … LIKE needs a quoted pattern (e.g. SHOW VIEWS IN cat.ns LIKE 'v*')",
        ),
        (
            "SHOW VIEWS IN a.b.c",
            "Error during planning: expected a two-part `IN <catalog.namespace>` name, got `a.b.c`",
        ),
        (
            "SHOW VIEWS IN ice.sales extra",
            "Error during planning: could not parse `SHOW VIEWS` at `extra` — the supported form is SHOW VIEWS IN <catalog.namespace> [LIKE] ['pattern']",
        ),
        (
            "SHOW VIEWS IN ice.",
            "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: identifier, found: EOF",
        ),
    ] {
        let error = session_error(sql, &stub).await;
        assert!(
            matches!(error, DataFusionError::Plan(_)),
            "{sql}: {error:?}"
        );
        assert_eq!(error.to_string(), expected, "{sql}");
    }
}

#[tokio::test]
async fn temp_view_statements_refuse_statement_write_options() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let stub = StubTempViews::with_existing(&["src"]);
    let options = crate::write_options::StatementWriteOptions {
        raw: vec![("write-format".to_string(), "parquet".to_string())],
        ..crate::write_options::StatementWriteOptions::empty()
    };
    for (sql, context) in [
        ("DROP VIEW src", "DROP VIEW"),
        ("DROP TABLE src", "DROP TABLE"),
        ("DESCRIBE src", "DESCRIBE TABLE"),
        (
            "CREATE OR REPLACE TEMPORARY VIEW v2 AS SELECT 1 AS id",
            "CREATE TEMPORARY VIEW",
        ),
    ] {
        let error = crate::router::execute_in_session(
            &ctx,
            &catalogs,
            sql,
            &std::collections::HashSet::<String>::new(),
            &options,
            Some(&stub),
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} must refuse write options"));
        assert!(
            matches!(error, DataFusionError::Plan(_)),
            "{sql}: {error:?}"
        );
        assert_eq!(
            error.to_string(),
            format!(
                "Error during planning: {context} does not support write options (write-format); they are only honoured on Iceberg table writes (ICE-WRITE-OPTIONS-1)"
            ),
            "{sql}"
        );
    }
    assert!(stub.dropped().is_empty());
    assert!(stub.registered().is_empty());
}

fn assert_parse_syntax_error(error: &DataFusionError, sql: &str) {
    let DataFusionError::SQL(inner, _) = error else {
        panic!("{sql}: expected a SQL parse error, got {error:?}");
    };
    let ParserError::ParserError(message) = inner.as_ref() else {
        panic!("{sql}: expected ParserError, got {inner:?}");
    };
    assert_eq!(message, MULTI_STATEMENT_MESSAGE, "{sql}");
}

const MULTI_STATEMENT_MESSAGE: &str = "[PARSE_SYNTAX_ERROR] Syntax error: multiple SQL statements in one call are not supported (Spark parity). Only a single statement is accepted; a trailing semicolon, whitespace, or comment after that statement is allowed. SQLSTATE: 42601";

#[tokio::test]
async fn temp_view_statements_with_a_trailing_statement_refuse_as_the_catalog_does() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT CAST(1 AS INT) AS id",
    )
    .await
    .expect("catalog table")
    .collect()
    .await
    .expect("collect create table");
    let stub = StubTempViews::with_existing(&["src"]);
    for sql in [
        "DESCRIBE ice.sales.t; SELECT 1",
        "DESCRIBE src; SELECT 1",
        "DESC TABLE src; SELECT 1",
        "DESCRIBE EXTENDED src; SELECT 1",
        "SHOW VIEWS; SELECT 1",
        "SHOW VIEWS IN ice.sales; SELECT 1",
        "CREATE VIEW ice.sales.d1; DROP TABLE ice.sales.t AS SELECT 1 AS id",
        "CREATE TEMPORARY VIEW h1; DROP TABLE ice.sales.t AS SELECT 1 AS id",
        "CREATE TEMPORARY VIEW h2 (a); SELECT 1 AS id",
    ] {
        let error = run_with_session(&ctx, &catalogs, sql, &stub)
            .await
            .expect_err("a trailing statement must not be dropped");
        assert_parse_syntax_error(&error, sql);
    }
    for sql in ["DROP VIEW src; SELECT 1", "DROP TABLE src; SELECT 1"] {
        let temp = run_with_session(&ctx, &catalogs, sql, &stub)
            .await
            .expect_err("a trailing statement must not be dropped");
        let catalog_sql = sql.replace("src", "ice.sales.t");
        let catalog = run_with_session(&ctx, &catalogs, &catalog_sql, &stub)
            .await
            .expect_err("the catalog drop refuses the same statement");
        assert_eq!(
            std::mem::discriminant(&temp),
            std::mem::discriminant(&catalog),
            "{sql}: {temp:?} vs {catalog:?}"
        );
        assert_parse_syntax_error(&temp, sql);
        assert_parse_syntax_error(&catalog, &catalog_sql);
    }
    assert!(stub.dropped().is_empty());
    for sql in [
        "DESCRIBE src",
        "DESCRIBE src;",
        "DESCRIBE src ;",
        "DESCRIBE src;;",
        "DESC TABLE src;",
        "DESCRIBE EXTENDED src;",
    ] {
        let frame = run_with_session(&ctx, &catalogs, sql, &stub)
            .await
            .unwrap_or_else(|error| panic!("{sql} answers: {error}"));
        assert_eq!(
            string_rows(frame).await,
            vec![
                vec![Some("id".to_string()), Some("int".to_string()), None],
                vec![Some("name".to_string()), Some("string".to_string()), None],
            ],
            "{sql}"
        );
    }
    for sql in ["SHOW VIEWS;", "SHOW VIEWS IN ice.sales;"] {
        let frame = run_with_session(&ctx, &catalogs, sql, &stub)
            .await
            .unwrap_or_else(|error| panic!("{sql} answers: {error}"));
        assert_eq!(
            string_rows(frame).await,
            vec![vec![
                Some(String::new()),
                Some("src".into()),
                Some("true".into())
            ]],
            "{sql}"
        );
    }
    run_with_session(&ctx, &catalogs, "DROP VIEW src;", &stub)
        .await
        .expect("a trailing semicolon still drops the temp view");
    assert_eq!(stub.dropped(), vec!["\"src\"".to_string()]);
}
