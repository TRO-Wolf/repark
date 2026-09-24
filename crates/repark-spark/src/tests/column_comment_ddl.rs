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
        "ALTER TABLE ice.sales.ac ALTER COLUMN m.value COMMENT 'mv'",
        "ALTER TABLE ice.sales.ac ALTER COLUMN arr.element COMMENT 'el'",
    ] {
        run(&ctx, &catalogs, sql).await;
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
        "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'a', cat COMMENT 'b'",
    ] {
        run(&ctx, &catalogs, sql).await;
    }
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
    ] {
        assert_eq!(
            parse_refusal(&ctx, &catalogs, sql).await,
            format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601"),
            "{sql}"
        );
    }
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
    assert_eq!(
        parse_refusal(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT"
        )
        .await,
        "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601"
    );
    assert_eq!(
        refusal(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.ac ALTER COLUMN data COMMENT 'a', cat TYPE STRING"
        )
        .await,
        "This feature is not implemented: ALTER TABLE … ALTER COLUMN mixes COMMENT with another \
         change for `cat` in one column list; only a list of COMMENT changes is supported, so \
         split the statement"
    );
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
