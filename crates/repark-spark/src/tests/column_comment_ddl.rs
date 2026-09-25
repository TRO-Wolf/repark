use super::super::*;
use super::common::*;
use tempfile::TempDir;

const COMMENT_CREATE: &str = "CREATE TABLE ice.sales.ac (id BIGINT, data STRING, cat STRING, \
     st STRUCT<x: INT>, m MAP<STRING, INT>, arr ARRAY<INT>) USING iceberg";

async fn docs(catalogs: &CatalogRegistry) -> Vec<(String, Option<String>)> {
    let table = load_sales_table(catalogs, "ac").await;
    let schema = table.metadata().current_schema();
    ["id", "data", "cat", "st", "st.x", "m.value", "arr.element"]
        .iter()
        .map(|name| {
            let field = schema.field_by_name(name).unwrap();
            ((*name).to_string(), field.doc.clone())
        })
        .collect()
}

async fn schema_count(catalogs: &CatalogRegistry) -> usize {
    load_sales_table(catalogs, "ac")
        .await
        .metadata()
        .schemas_iter()
        .count()
}

async fn refusal(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    execute(ctx, catalogs, sql)
        .await
        .expect_err(sql)
        .to_string()
}

async fn parse_refusal(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    let mapped = repark_core::engine_err(execute(ctx, catalogs, sql).await.expect_err(sql));
    let repark_common::Error::Parse(message) = &mapped else {
        panic!("{sql}: expected a Parse error, got {mapped:?}");
    };
    message.clone()
}

#[tokio::test]
async fn alter_column_comment_sets_the_iceberg_doc_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    for sql in [
        "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'new doc'",
        "ALTER TABLE ice.sales.ac ALTER COLUMN st.x COMMENT 'nested doc'",
        "ALTER TABLE ice.sales.ac ALTER COLUMN id COMMENT ''",
        "ALTER TABLE ice.sales.ac CHANGE COLUMN cat COMMENT 'via change'",
        "ALTER TABLE ice.sales.ac ALTER COLUMN DATA COMMENT \"dq doc\"",
    ] {
        run(&ctx, &catalogs, sql).await;
    }
    let schemas = schema_count(&catalogs).await;
    for sql in [
        "ALTER TABLE ice.sales.ac ALTER COLUMN m.value COMMENT 'mv'",
        "ALTER TABLE ice.sales.ac ALTER COLUMN arr.element COMMENT 'el'",
        "ALTER TABLE ice.sales.ac ALTER COLUMN m.value COMMENT 'mv', arr.element COMMENT 'el'",
    ] {
        run(&ctx, &catalogs, sql).await;
        assert_eq!(schema_count(&catalogs).await, schemas, "{sql}");
    }
    assert_eq!(
        docs(&catalogs).await,
        vec![
            ("id".to_string(), Some(String::new())),
            ("data".to_string(), Some("dq doc".to_string())),
            ("cat".to_string(), Some("via change".to_string())),
            ("st".to_string(), None),
            ("st.x".to_string(), Some("nested doc".to_string())),
            ("m.value".to_string(), None),
            ("arr.element".to_string(), None),
        ]
    );
}

#[tokio::test]
async fn alter_column_comment_takes_bare_keywords_and_a_spec_list_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    for sql in [
        "ALTER TABLE ice.sales.ac CHANGE id COMMENT 'bare change'",
        "ALTER TABLE ice.sales.ac ALTER st COMMENT 'bare alter'",
    ] {
        run(&ctx, &catalogs, sql).await;
    }
    let schemas = schema_count(&catalogs).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'a', cat COMMENT 'b'",
    )
    .await;
    assert_eq!(schema_count(&catalogs).await, schemas + 1);
    let documented = docs(&catalogs).await;
    assert_eq!(
        documented[..4].to_vec(),
        vec![
            ("id".to_string(), Some("bare change".to_string())),
            ("data".to_string(), Some("a".to_string())),
            ("cat".to_string(), Some("b".to_string())),
            ("st".to_string(), Some("bare alter".to_string())),
        ]
    );
}

#[tokio::test]
async fn alter_column_comment_takes_a_backslash_escaped_quote() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    run(
        &ctx,
        &catalogs,
        "alter table ice.sales.ac change column data comment 'it\\'s';",
    )
    .await;
    assert_eq!(docs(&catalogs).await[1].1.as_deref(), Some("it's"));
}

#[tokio::test]
async fn alter_column_comment_refuses_unresolved_columns_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ac (id BIGINT, data STRING, cat STRING, st STRUCT<x: INT>) \
         USING iceberg",
    )
    .await;
    for (column, rendered) in [("nope", "`nope`"), ("st.nope", "`st`.`nope`")] {
        let sql = format!("ALTER TABLE ice.sales.ac ALTER COLUMN {column} COMMENT 'x'");
        assert_eq!(
            refusal(&ctx, &catalogs, &sql).await,
            format!(
                "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, \
                 or function parameter with name {rendered} cannot be resolved. Did you mean one \
                 of the following? [`id`, `data`, `cat`, `st`]. SQLSTATE: 42703"
            ),
            "{sql}"
        );
    }
    assert_eq!(
        refusal(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.ac ALTER COLUMN id.x COMMENT 'x'"
        )
        .await,
        "Error during planning: [INVALID_FIELD_NAME] Field name `id`.`x` is invalid: `id` is \
         not a struct. SQLSTATE: 42000"
    );
    assert_eq!(
        refusal(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.nope ALTER COLUMN data COMMENT 'x'"
        )
        .await,
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view \
         `ice`.`sales`.`nope` cannot be found. Verify the spelling and correctness of the \
         schema and catalog. If you did not qualify the name with a schema, verify the \
         current_schema() output, or qualify the name with the correct schema and catalog. \
         To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. \
         SQLSTATE: 42P01"
    );
    let table = load_sales_table(&catalogs, "ac").await;
    assert_eq!(table.metadata().schemas_iter().count(), 1);
}

#[tokio::test]
async fn alter_column_comment_refuses_malformed_forms_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    for (sql, near) in [
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN cat COMMENT NULL",
            "NULL",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data TYPE STRING COMMENT 'x'",
            "COMMENT",
        ),
        ("ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 5", "5"),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN \"data\" COMMENT 'dq'",
            "\"data\"",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data SET NOT NULL COMMENT 'x'",
            "COMMENT",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data DROP NOT NULL COMMENT 'x'",
            "COMMENT",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data SET DEFAULT 'a' COMMENT 'x'",
            "COMMENT",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data DROP DEFAULT COMMENT 'x'",
            "COMMENT",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN id SET DEFAULT 1 + 2 COMMENT 'x'",
            "COMMENT",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN st.x SET NOT NULL COMMENT 'x'",
            "COMMENT",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER data SET NOT NULL COMMENT 'x'",
            "COMMENT",
        ),
        (
            "ALTER TABLE ice.sales.ac CHANGE data SET DEFAULT 'a' COMMENT 'x'",
            "COMMENT",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data FIRST COMMENT 'x'",
            "COMMENT",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data AFTER id COMMENT 'x'",
            "COMMENT",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'a', 5 COMMENT 'b'",
            "5",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN 'data' COMMENT 'x'",
            "'data'",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'a', 'cat' COMMENT 'b'",
            "'cat'",
        ),
        (
            "ALTER TABLE ice.sales.ac CHANGE 'data' COMMENT 'x'",
            "'data'",
        ),
    ] {
        assert_eq!(
            parse_refusal(&ctx, &catalogs, sql).await,
            format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601"),
            "{sql}"
        );
    }
    assert_eq!(schema_count(&catalogs).await, 1);
}

#[tokio::test]
async fn alter_column_comment_refuses_malformed_spec_lists_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    assert_eq!(
        parse_refusal(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.ac ALTER COLUMN st.x COMMENT 'x' FIRST"
        )
        .await,
        "[PARSE_SYNTAX_ERROR] Syntax error at or near 'FIRST': extra input 'FIRST'. \
         SQLSTATE: 42601"
    );
    for (tail, extra) in [
        ("cat garbage", "garbage"),
        ("cat garbage;", "garbage"),
        ("cat 'x'", "'x'"),
        ("cat)", ")"),
        ("st.x garbage", "garbage"),
    ] {
        let sql = format!("ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'a', {tail}");
        assert_eq!(
            parse_refusal(&ctx, &catalogs, &sql).await,
            format!(
                "[PARSE_SYNTAX_ERROR] Syntax error at or near '{extra}': extra input '{extra}'. \
                 SQLSTATE: 42601"
            ),
            "{sql}"
        );
    }
    for tail in ["cat", "cat;", "cat, id COMMENT 'b'", "st.x"] {
        let sql = format!("ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'a', {tail}");
        assert_eq!(
            parse_refusal(&ctx, &catalogs, &sql).await,
            "SQL error: ParserError(\"Operation not allowed: ALTER TABLE table ALTER COLUMN \
             requires a TYPE, a SET/DROP, a COMMENT, or a FIRST/AFTER.\")",
            "{sql}"
        );
    }
    for sql in [
        "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT",
        "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'a',",
    ] {
        assert_eq!(
            parse_refusal(&ctx, &catalogs, sql).await,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
            "{sql}"
        );
    }
    for (tail, column) in [
        ("cat TYPE STRING", "cat"),
        ("cat FIRST", "cat"),
        ("cat AFTER id", "cat"),
        ("cat DROP NOT NULL", "cat"),
        ("st.x TYPE BIGINT", "st.x"),
    ] {
        let sql = format!("ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'a', {tail}");
        assert_eq!(
            refusal(&ctx, &catalogs, &sql).await,
            format!(
                "This feature is not implemented: ALTER TABLE … ALTER COLUMN mixes COMMENT with \
                 another change for `{column}` in one column list; only a list of COMMENT changes \
                 is supported, so split the statement"
            ),
            "{sql}"
        );
    }
    assert_eq!(
        refusal(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.ac ALTER COLUMN m.key COMMENT 'mk'"
        )
        .await,
        "Execution error: Unsupported table change: Cannot update map keys: map<string, int>"
    );
    assert_eq!(
        docs(&catalogs)
            .await
            .iter()
            .filter(|(_, doc)| doc.is_some())
            .count(),
        0
    );
    assert_eq!(schema_count(&catalogs).await, 1);
}

#[tokio::test]
async fn a_comment_list_after_another_change_is_the_mixed_list_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    for (clause, column) in [
        ("ALTER COLUMN id DROP NOT NULL, data COMMENT 'dn'", "id"),
        ("ALTER COLUMN id SET NOT NULL, data COMMENT 'sn'", "id"),
        ("ALTER COLUMN cat SET DEFAULT 'x', data COMMENT 'sd'", "cat"),
        ("ALTER COLUMN cat DROP DEFAULT, data COMMENT 'dd'", "cat"),
        ("ALTER COLUMN cat FIRST, data COMMENT 'ff'", "cat"),
        ("ALTER COLUMN cat AFTER id, data COMMENT 'af'", "cat"),
        ("ALTER COLUMN cat TYPE STRING, data COMMENT 'tf'", "cat"),
        ("ALTER COLUMN st.x TYPE BIGINT, data COMMENT 'nt'", "st.x"),
        (
            "ALTER COLUMN id DROP NOT NULL, cat TYPE STRING, data COMMENT 'three'",
            "id",
        ),
        ("ALTER COLUMN id DROP NOT NULL, st.x COMMENT 'nx'", "id"),
        ("ALTER id DROP NOT NULL, data COMMENT 'bare'", "id"),
        ("ALTER id TYPE BIGINT, data COMMENT 'bt'", "id"),
        ("CHANGE id DROP NOT NULL, data COMMENT 'bd'", "id"),
    ] {
        let sql = format!("ALTER TABLE ice.sales.ac {clause}");
        assert_eq!(
            refusal(&ctx, &catalogs, &sql).await,
            format!(
                "This feature is not implemented: ALTER TABLE … ALTER COLUMN mixes COMMENT with \
                 another change for `{column}` in one column list; only a list of COMMENT changes \
                 is supported, so split the statement"
            ),
            "{sql}"
        );
    }
    assert_eq!(schema_count(&catalogs).await, 1);
}

#[tokio::test]
async fn wrapped_and_malformed_comment_lists_are_parse_errors_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    for (sql, message) in [
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN id DROP NOT NULL, data COMMENT",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN cat TYPE STRING, data COMMENT",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN id DROP NOT NULL, data COMMENT 'x' FIRST",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'FIRST': extra input 'FIRST'. \
             SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN id DROP NOT NULL, 'data' COMMENT 'q'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near ''data''. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN id foo, data COMMENT 'b2'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'foo'. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN id NOT NULL, data COMMENT 'nn'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'NOT'. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN id, data COMMENT 'b1'",
            "SQL error: ParserError(\"Operation not allowed: ALTER TABLE table ALTER COLUMN \
             requires a TYPE, a SET/DROP, a COMMENT, or a FIRST/AFTER.\")",
        ),
        (
            "ALTER TABLE IF EXISTS ice.sales.ac ALTER COLUMN id COMMENT 'ie'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXISTS'. SQLSTATE: 42601",
        ),
        (
            "alter table if exists ice.sales.ac alter column id comment 'lw'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'exists'. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE IF EXISTS ice.sales.ac CHANGE COLUMN id COMMENT 'ie2'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXISTS'. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE IF EXISTS ice.sales.nope ALTER COLUMN st.x COMMENT 'ie3'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXISTS'. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE IF EXISTS ice.sales.ac ALTER COLUMN id DROP NOT NULL, data COMMENT 'x'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXISTS'. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE IF EXISTS ice.sales.ac PARTITION (id=1) ALTER COLUMN id COMMENT 'x'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXISTS'. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE ice.sales.ac PARTITION (id=1) ALTER COLUMN id COMMENT 'pp'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'ALTER'. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE ice.sales.ac PARTITION (id=1) ALTER id COMMENT 'pa'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'ALTER'. SQLSTATE: 42601",
        ),
        (
            "ALTER TABLE ice.sales.ac PARTITION (id=1) ALTER COLUMN st.x COMMENT 'pnest'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'ALTER'. SQLSTATE: 42601",
        ),
        (
            "alter table ice.sales.ac partition (id=1) alter column id comment 'pl'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'alter'. SQLSTATE: 42601",
        ),
    ] {
        assert_eq!(parse_refusal(&ctx, &catalogs, sql).await, message, "{sql}");
    }
    assert_eq!(schema_count(&catalogs).await, 1);
}

#[tokio::test]
async fn other_column_comment_statements_answer_the_residual_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    for sql in [
        "ALTER TABLE ice.sales.ac ADD COLUMNS (z2 INT), ALTER COLUMN id COMMENT 'aa'",
        "ALTER TABLE ice.sales.ac DROP COLUMN cat, ALTER COLUMN id COMMENT 'da'",
        "ALTER TABLE ice.sales.ac ALTER COLUMN id DROP NOT NULL foo COMMENT 'x'",
        "ALTER TABLE ice.sales.ac ALTER COLUMN id TYPE, data COMMENT 'x'",
    ] {
        assert_eq!(
            refusal(&ctx, &catalogs, sql).await,
            "This feature is not implemented: ALTER TABLE … ALTER COLUMN … COMMENT is supported \
             only as ALTER TABLE <table> ALTER COLUMN <column> COMMENT '<doc>' or a list of such \
             COMMENT specs; this statement shape is not supported",
            "{sql}"
        );
    }
    for sql in [
        "ALTER TABLE ice.sales.ac ALTER COLUMN comment TYPE BIGINT",
        "ALTER TABLE ice.sales.ac ADD COLUMN z INT COMMENT 'x'",
        "ALTER TABLE ice.sales.ac CHANGE COLUMN data data STRING COMMENT 'x'",
    ] {
        assert!(
            crate::nested_column_ddl::residual_column_comment_refusal(sql).is_none(),
            "{sql}"
        );
    }
    for sql in [
        "ALTER TABLE ice.sales.ac ALTER COLUMN id DROP NOT NULL, cat DROP NOT NULL",
        "ALTER TABLE ice.sales.ac CHANGE data data STRING COMMENT 'x'",
        "ALTER TABLE IF EXISTS ice.sales.ac ADD COLUMN z INT COMMENT 'x'",
        "ALTER TABLE IF EXISTS ice.sales.ac ALTER COLUMN st.x TYPE BIGINT",
        "ALTER TABLE ice.sales.ac PARTITION (id=1) CHANGE COLUMN id COMMENT 'x'",
        "ALTER TABLE ice.sales.ac PARTITION (id=1) ALTER COLUMN id TYPE BIGINT",
    ] {
        assert!(
            crate::nested_column_ddl::try_parse_nested_column_ddl(sql).is_none(),
            "{sql}"
        );
    }
    assert_eq!(
        docs(&catalogs)
            .await
            .iter()
            .filter(|(_, doc)| doc.is_some())
            .count(),
        0
    );
    assert_eq!(schema_count(&catalogs).await, 1);
}

#[tokio::test]
async fn alter_column_comment_tails_follow_spark_token_recovery() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    for (sql, near) in [
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x' TYPE STRING",
            "TYPE",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x' SET NOT NULL",
            "SET",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x' DROP NOT NULL",
            "DROP",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x' AFTER id",
            "AFTER",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x' SET DEFAULT 'a'",
            "SET",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x' DROP DEFAULT",
            "DROP",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x' COMMENT 'y'",
            "COMMENT",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x' NOT NULL",
            "NOT",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x' FIRST foo",
            "FIRST",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x' foo bar",
            "foo",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x', cat COMMENT 'y' TYPE STRING",
            "TYPE",
        ),
        (
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'a', cat garbage more",
            "garbage",
        ),
    ] {
        assert_eq!(
            parse_refusal(&ctx, &catalogs, sql).await,
            format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601"),
            "{sql}"
        );
    }
    for (tail, extra) in [
        ("TYPE", "TYPE"),
        ("SET", "SET"),
        ("DROP", "DROP"),
        ("AFTER", "AFTER"),
        ("NOT", "NOT"),
        ("foo", "foo"),
        ("foo;", "foo"),
        ("5", "5"),
        (")", ")"),
    ] {
        let sql = format!("ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'x' {tail}");
        assert_eq!(
            parse_refusal(&ctx, &catalogs, &sql).await,
            format!(
                "[PARSE_SYNTAX_ERROR] Syntax error at or near '{extra}': extra input '{extra}'. \
                 SQLSTATE: 42601"
            ),
            "{sql}"
        );
    }
    assert_eq!(schema_count(&catalogs).await, 1);
}

#[tokio::test]
async fn alter_column_comment_refuses_a_repeated_column_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    for (list, column) in [
        (
            "ALTER COLUMN data COMMENT 'dup', data COMMENT 'dup2'",
            "`data`",
        ),
        (
            "ALTER COLUMN Data COMMENT 'c1', data COMMENT 'c2'",
            "`data`",
        ),
        (
            "ALTER COLUMN data COMMENT 'a', `data` COMMENT 'b'",
            "`data`",
        ),
        ("ALTER COLUMN st COMMENT 'p', st.x COMMENT 'c'", "`st`"),
        ("ALTER COLUMN ST.X COMMENT 'c', st COMMENT 'p'", "`st`"),
        (
            "ALTER COLUMN st.x COMMENT 'c', st.X COMMENT 'd'",
            "`st`.`x`",
        ),
        (
            "ALTER COLUMN id COMMENT 'i', data COMMENT 'd', id COMMENT 'i2'",
            "`id`",
        ),
        (
            "ALTER COLUMN arr COMMENT 'p', arr.element COMMENT 'c'",
            "`arr`",
        ),
        ("ALTER COLUMN m COMMENT 'p', m.value COMMENT 'c'", "`m`"),
        ("CHANGE COLUMN data COMMENT 'a', data COMMENT 'b'", "`data`"),
        ("ALTER data COMMENT 'a', data COMMENT 'b'", "`data`"),
    ] {
        let sql = format!("ALTER TABLE ice.sales.ac {list}");
        assert_eq!(
            refusal(&ctx, &catalogs, &sql).await,
            format!(
                "Error during planning: [NOT_SUPPORTED_CHANGE_SAME_COLUMN] ALTER TABLE \
                 ALTER/CHANGE COLUMN is not supported for changing `ice`.`sales`.`ac`'s column \
                 {column} including its nested fields multiple times in the same command. \
                 SQLSTATE: 0A000"
            ),
            "{sql}"
        );
    }
    assert_eq!(
        refusal(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.ac ALTER COLUMN nope COMMENT 'a', data COMMENT 'b', \
             data COMMENT 'c'"
        )
        .await,
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
         function parameter with name `nope` cannot be resolved. Did you mean one of the \
         following? [`id`, `data`, `cat`, `st`, `m`, `arr`]. SQLSTATE: 42703"
    );
    assert_eq!(schema_count(&catalogs).await, 1);
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ac ALTER COLUMN id COMMENT 'i3', data COMMENT 'd3', \
         cat COMMENT 'c3', st.x COMMENT 'sx'",
    )
    .await;
    assert_eq!(schema_count(&catalogs).await, 2);
    assert_eq!(
        docs(&catalogs).await[..5].to_vec(),
        vec![
            ("id".to_string(), Some("i3".to_string())),
            ("data".to_string(), Some("d3".to_string())),
            ("cat".to_string(), Some("c3".to_string())),
            ("st".to_string(), None),
            ("st.x".to_string(), Some("sx".to_string())),
        ]
    );
}

#[tokio::test]
async fn single_quoted_column_names_are_parse_errors_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    for (clause, near) in [
        ("ADD COLUMN st.'z' INT", "."),
        ("ADD COLUMNS ('st'.z INT)", "'st'"),
        ("DROP COLUMN 'st'.x", "'st'"),
        ("RENAME COLUMN st.'x' TO q", "."),
        ("ALTER COLUMN 'st'.x TYPE BIGINT", "'st'"),
    ] {
        let sql = format!("ALTER TABLE ice.sales.ac {clause}");
        assert_eq!(
            parse_refusal(&ctx, &catalogs, &sql).await,
            format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601"),
            "{sql}"
        );
    }
    assert_eq!(schema_count(&catalogs).await, 1);
}

#[tokio::test]
async fn alter_column_comment_leaves_type_and_hive_change_forms_unchanged() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ac (id INT, data STRING, cat STRING, st STRUCT<x: INT>) \
         USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ac ALTER COLUMN id TYPE BIGINT",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ac CHANGE COLUMN data data STRING COMMENT 'hive-style'",
    )
    .await;
    let table = load_sales_table(&catalogs, "ac").await;
    let schema = table.metadata().current_schema();
    assert_eq!(
        schema.field_by_name("id").unwrap().field_type.to_string(),
        "long"
    );
    assert_eq!(
        schema.field_by_name("data").unwrap().doc.as_deref(),
        Some("hive-style")
    );
    for sql in [
        "ALTER TABLE ice.sales.ac CHANGE COLUMN data data STRING COMMENT 'x'",
        "ALTER TABLE ice.sales.ac CHANGE COLUMN data payload STRING",
        "ALTER TABLE ice.sales.ac ALTER COLUMN id TYPE BIGINT",
        "ALTER TABLE ice.sales.ac ALTER COLUMN id DROP NOT NULL",
        "ALTER TABLE ice.sales.ac ALTER COLUMN cat FIRST",
    ] {
        assert!(
            crate::nested_column_ddl::try_parse_nested_column_ddl(sql).is_none(),
            "{sql}"
        );
    }
}

const MAP_KEY_CREATE: &str = "CREATE TABLE ice.sales.ac (id BIGINT, data STRING, \
     m MAP<STRING, STRUCT<z: INT>>, mm MAP<STRUCT<k: INT>, INT>) USING iceberg";

#[tokio::test]
async fn alter_column_comment_refuses_map_keys_in_spark_order() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, MAP_KEY_CREATE).await;
    let table = load_sales_table(&catalogs, "ac").await;
    let schema = table.metadata().current_schema();
    let k_id = schema.field_by_name("mm.key.k").unwrap().id;
    let z_id = schema.field_by_name("m.value.z").unwrap().id;
    let alter_mm = format!(
        "Unsupported table change: Cannot alter map keys: map<struct<{k_id}: k: optional int>, int>"
    );
    let update_m = format!(
        "Unsupported table change: Cannot update map keys: map<string, struct<{z_id}: z: optional int>>"
    );
    let same_column = |column: &str| {
        format!(
            "Error during planning: [NOT_SUPPORTED_CHANGE_SAME_COLUMN] ALTER TABLE ALTER/CHANGE \
             COLUMN is not supported for changing `ice`.`sales`.`ac`'s column {column} including \
             its nested fields multiple times in the same command. SQLSTATE: 0A000"
        )
    };
    let unresolved = "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, \
         variable, or function parameter with name `nope` cannot be resolved. Did you mean one \
         of the following? [`id`, `data`, `m`, `mm`]. SQLSTATE: 42703"
        .to_string();
    for (clause, expected) in [
        (
            "ALTER COLUMN mm.key.k COMMENT 'k'",
            format!("Execution error: {alter_mm}"),
        ),
        (
            "ALTER COLUMN data COMMENT 'd2', mm.key.k COMMENT 'k'",
            format!("Execution error: {alter_mm}"),
        ),
        (
            "ALTER COLUMN mm.key.k COMMENT 'y', m.key COMMENT 'x'",
            format!("Execution error: {update_m}"),
        ),
        (
            "ALTER COLUMN m.key COMMENT 'x', mm.key.k COMMENT 'y'",
            format!("Execution error: {update_m}"),
        ),
        (
            "ALTER COLUMN m.key COMMENT 'x', m.value COMMENT 'y'",
            format!("Execution error: {update_m}"),
        ),
        (
            "ALTER COLUMN m.key COMMENT 'x', m.key COMMENT 'y'",
            same_column("`m`.`key`"),
        ),
        (
            "ALTER COLUMN mm.key.k COMMENT 'a', mm.key COMMENT 'b'",
            same_column("`mm`.`key`"),
        ),
        (
            "ALTER COLUMN m.key COMMENT 'x', nope COMMENT 'y'",
            unresolved.clone(),
        ),
        (
            "ALTER COLUMN mm.key.k COMMENT 'a', nope COMMENT 'b'",
            unresolved.clone(),
        ),
        (
            "ALTER COLUMN mm.key.k TYPE BIGINT",
            format!("Execution error: {alter_mm}"),
        ),
    ] {
        let sql = format!("ALTER TABLE ice.sales.ac {clause}");
        assert_eq!(refusal(&ctx, &catalogs, &sql).await, expected, "{sql}");
    }
    assert_eq!(schema_count(&catalogs).await, 1);
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ac ALTER COLUMN mm.key.k TYPE INT",
    )
    .await;
    assert_eq!(schema_count(&catalogs).await, 1);
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ac ALTER COLUMN m.value COMMENT 'v', data COMMENT 'dv'",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ac ALTER COLUMN m.value.z COMMENT 'vz'",
    )
    .await;
    assert_eq!(schema_count(&catalogs).await, 3);
    let table = load_sales_table(&catalogs, "ac").await;
    let schema = table.metadata().current_schema();
    let doc = |name: &str| schema.field_by_name(name).unwrap().doc.clone();
    assert_eq!(doc("data").as_deref(), Some("dv"));
    assert_eq!(doc("m.value"), None);
    assert_eq!(doc("m.value.z").as_deref(), Some("vz"));
    assert_eq!(doc("mm.key.k"), None);
}

#[tokio::test]
async fn unresolved_columns_render_backquoted_parts_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ac (id BIGINT, st STRUCT<x: INT>, `p.q` INT) USING iceberg",
    )
    .await;
    for (clause, rendered) in [
        ("ALTER COLUMN `st.x` COMMENT 'a'", "`st.x`"),
        ("ALTER COLUMN st.`x.y` COMMENT 'a'", "`st`.`x.y`"),
        ("ALTER COLUMN st.nope TYPE BIGINT", "`st`.`nope`"),
        ("ALTER COLUMN st.`x.y` TYPE BIGINT", "`st`.`x.y`"),
        ("ADD COLUMN nope.z INT", "`nope`"),
    ] {
        let sql = format!("ALTER TABLE ice.sales.ac {clause}");
        assert_eq!(
            refusal(&ctx, &catalogs, &sql).await,
            format!(
                "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, \
                 or function parameter with name {rendered} cannot be resolved. Did you mean one \
                 of the following? [`id`, `st`, `p`.`q`]. SQLSTATE: 42703"
            ),
            "{sql}"
        );
    }
    assert_eq!(schema_count(&catalogs).await, 1);
}

#[tokio::test]
async fn a_repeated_column_after_use_renders_the_three_part_table_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, COMMENT_CREATE).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    for table in ["ac", "sales.ac"] {
        let sql = format!("ALTER TABLE {table} ALTER COLUMN data COMMENT 'a', data COMMENT 'b'");
        assert_eq!(
            refusal(&ctx, &catalogs, &sql).await,
            "Error during planning: [NOT_SUPPORTED_CHANGE_SAME_COLUMN] ALTER TABLE ALTER/CHANGE \
             COLUMN is not supported for changing `ice`.`sales`.`ac`'s column `data` including \
             its nested fields multiple times in the same command. SQLSTATE: 0A000",
            "{sql}"
        );
    }
    assert_eq!(schema_count(&catalogs).await, 1);
}
