use super::super::*;
use super::accept_any_refusals::{refusal, sorted_rows};
use super::common::*;

pub(super) const PARTITIONED: &str = "CREATE TABLE ice.sales.t (id BIGINT, data STRING, cat STRING) \
                                      USING iceberg PARTITIONED BY (cat)";
const UNPARTITIONED: &str =
    "CREATE TABLE ice.sales.t (id BIGINT, data STRING, cat STRING) USING iceberg";
pub(super) const SEED: &str =
    "INSERT INTO ice.sales.t VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')";

pub(super) async fn seeded(wh: &TempDir, create: &str) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    run(&ctx, &catalogs, create).await;
    run(&ctx, &catalogs, SEED).await;
    (ctx, catalogs)
}

pub(super) fn row(id: i64, data: &str, cat: &str) -> String {
    format!("| {id:<2} | {data:<4} | {cat:<3} |")
}

async fn latest_summary(catalogs: &CatalogRegistry) -> (String, HashMap<String, String>) {
    let table = load_sales_table(catalogs, "t").await;
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("a current snapshot");
    (
        snapshot.summary().operation.as_str().to_string(),
        snapshot.summary().additional_properties.clone(),
    )
}

async fn snapshot_count(catalogs: &CatalogRegistry) -> usize {
    load_sales_table(catalogs, "t")
        .await
        .metadata()
        .snapshots()
        .count()
}

fn counts(summary: &HashMap<String, String>) -> [Option<&str>; 4] {
    [
        "added-records",
        "deleted-records",
        "total-records",
        "deleted-data-files",
    ]
    .map(|key| summary.get(key).map(String::as_str))
}

async fn assert_refusal(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str, text: &str) {
    let mapped = refusal(ctx, catalogs, sql).await;
    assert_eq!(mapped.to_string(), text, "{sql}");
    assert_eq!(snapshot_count(catalogs).await, 1, "{sql} must not commit");
}

#[tokio::test]
async fn replace_where_on_a_partition_replaces_its_rows_with_the_query() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
    )
    .await;
    assert_eq!(
        sorted_rows(&ctx, &catalogs).await,
        vec![row(2, "b", "y"), row(9, "z", "x")]
    );
    let (operation, summary) = latest_summary(&catalogs).await;
    assert_eq!(operation, "overwrite");
    assert_eq!(
        counts(&summary),
        [Some("1"), Some("2"), Some("2"), Some("1")]
    );
}

#[tokio::test]
async fn replace_where_accepts_the_spellings_spark_accepts() {
    let statements = [
        "INSERT INTO TABLE ice.sales.t REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
        "insert into ice.sales.t replace where cat = 'x' select 9, 'z', 'x'",
        "INSERT INTO ice.sales.t REPLACE /* c */ WHERE cat = 'x' SELECT 9, 'z', 'x'",
        "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' VALUES (9, 'z', 'x')",
        "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' (SELECT 9, 'z', 'x')",
        "INSERT INTO ice.sales.t REPLACE WHERE (cat = 'x') SELECT 9, 'z', 'x'",
        "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' \
         WITH s AS (SELECT 9 AS a, 'z' AS b, 'x' AS c) SELECT * FROM s",
        "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' SELECT CAST(9 AS INT), 'z', 'x'",
        "INSERT INTO ice.sales.t REPLACE WHERE cat IN ('x') SELECT 9, 'z', 'x'",
        "INSERT INTO ice.sales.t REPLACE WHERE cat != 'y' SELECT 9, 'z', 'x'",
        "INSERT INTO ice.sales.t REPLACE WHERE cat NOT IN ('y') SELECT 9, 'z', 'x'",
        "INSERT INTO ice.sales.t REPLACE WHERE cat LIKE 'x%' SELECT 9, 'z', 'x'",
    ];
    for sql in statements {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
        run(&ctx, &catalogs, sql).await;
        assert_eq!(
            sorted_rows(&ctx, &catalogs).await,
            vec![row(2, "b", "y"), row(9, "z", "x")],
            "{sql}"
        );
    }
}

#[tokio::test]
async fn replace_where_reads_the_target_before_it_replaces_it() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' \
         SELECT id + 10, data, cat FROM ice.sales.t WHERE cat = 'x'",
    )
    .await;
    assert_eq!(
        sorted_rows(&ctx, &catalogs).await,
        vec![row(11, "a", "x"), row(13, "c", "x"), row(2, "b", "y")]
    );
}

#[tokio::test]
async fn replace_where_writes_rows_outside_the_predicate_unchecked() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' SELECT 9, 'z', 'y'",
    )
    .await;
    assert_eq!(
        sorted_rows(&ctx, &catalogs).await,
        vec![row(2, "b", "y"), row(9, "z", "y")]
    );
}

#[tokio::test]
async fn replace_where_snapshot_operations_follow_what_the_commit_changes() {
    let cases = [
        (
            "INSERT INTO ice.sales.t REPLACE WHERE cat = 'nope' SELECT 9, 'z', 'nope'",
            "overwrite",
            [Some("1"), None, Some("4"), None],
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x' WHERE false",
            "delete",
            [None, Some("2"), Some("1"), Some("1")],
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE cat = 'nope' SELECT 9, 'z', 'x' WHERE false",
            "delete",
            [None, None, Some("3"), None],
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE true SELECT 9, 'z', 'x'",
            "overwrite",
            [Some("1"), Some("3"), Some("1"), Some("2")],
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE false SELECT 9, 'z', 'x'",
            "append",
            [Some("1"), None, Some("4"), None],
        ),
    ];
    for (sql, operation, expected) in cases {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
        run(&ctx, &catalogs, sql).await;
        let (actual, summary) = latest_summary(&catalogs).await;
        assert_eq!(actual, operation, "{sql}");
        assert_eq!(counts(&summary), expected, "{sql}");
        assert_eq!(snapshot_count(&catalogs).await, 2, "{sql}");
    }
}

#[tokio::test]
async fn replace_where_on_an_unpartitioned_table_replaces_whole_files_only() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, UNPARTITIONED).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t REPLACE WHERE id >= 1 SELECT 9, 'z', 'x'",
    )
    .await;
    assert_eq!(sorted_rows(&ctx, &catalogs).await, vec![row(9, "z", "x")]);
    let (operation, summary) = latest_summary(&catalogs).await;
    assert_eq!(operation, "overwrite");
    assert_eq!(
        counts(&summary),
        [Some("1"), Some("3"), Some("1"), Some("1")]
    );
}

#[tokio::test]
async fn replace_where_writes_to_a_named_branch_only() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH b1").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t.branch_b1 REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
    )
    .await;
    assert_eq!(
        sorted_rows(&ctx, &catalogs).await,
        vec![row(1, "a", "x"), row(2, "b", "y"), row(3, "c", "x")]
    );
    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.t.branch_b1 ORDER BY id",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, vec![2, 9]);
}

#[tokio::test]
async fn replace_where_refusals_carry_spark_text_and_commit_nothing() {
    let cases = [
        (
            "INSERT INTO ice.sales.t REPLACE WHERE id = 2 SELECT 9, 'z', 'y'",
            "DataInvalid => Cannot delete file where some, but not all, rows match filter id = 2: ",
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE upper(cat) = 'X' SELECT 9, 'z', 'x'",
            "Cannot convert Spark predicate to Iceberg expression: upper(cat) = 'X'",
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE cat = NULL SELECT 9, 'z', 'x'",
            "Cannot convert Spark predicate to Iceberg expression: null",
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE id IN (SELECT 1) SELECT 9, 'z', 'x'",
            "Error during planning: [UNSUPPORTED_FEATURE.OVERWRITE_BY_SUBQUERY] The feature is not supported: INSERT \
             OVERWRITE with a subquery condition. SQLSTATE: 0A000",
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE rand() < 2 SELECT 9, 'z', 'x'",
            "Error during planning: [INVALID_NON_DETERMINISTIC_EXPRESSIONS] The operator expects a deterministic \
             expression, but the actual expression is \"(rand() < 2)\". SQLSTATE: 42K0E",
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE CAT = 'x' SELECT 9, 'z', 'x'",
            "Error during planning: Cannot find field 'CAT' in struct: struct<1: id: optional \
             long, 2: data: optional string, 3: cat: optional string>",
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' SELECT 9, 'z'",
            "Error during planning: [INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] \
             Cannot write to `ice`.`sales`.`t`, the reason is not enough data columns:\nTable \
             columns: `id`, `data`, `cat`.\nData columns: `9`, `z`. SQLSTATE: 21S01",
        ),
    ];
    for (sql, text) in cases {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
        let mapped = refusal(&ctx, &catalogs, sql).await.to_string();
        assert!(mapped.starts_with(text), "{sql}: got {mapped}");
        assert_eq!(snapshot_count(&catalogs).await, 1, "{sql} must not commit");
    }
}

#[tokio::test]
async fn misplaced_replace_where_is_a_spark_parse_error() {
    let cases = [
        (
            "INSERT OVERWRITE ice.sales.t REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
            "'REPLACE'",
        ),
        (
            "INSERT OVERWRITE TABLE ice.sales.t REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
            "'REPLACE'",
        ),
        (
            "INSERT INTO ice.sales.t (id, data, cat) REPLACE WHERE cat = 'x' SELECT 9, 'z', 'x'",
            "'REPLACE'",
        ),
        (
            "INSERT INTO ice.sales.t BY NAME REPLACE WHERE cat = 'x' \
             SELECT 9 AS id, 'z' AS data, 'x' AS cat",
            "'REPLACE'",
        ),
        (
            "INSERT INTO ice.sales.t PARTITION (cat = 'x') REPLACE WHERE cat = 'x' SELECT 9, 'z'",
            "'REPLACE'",
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x' BY NAME \
             SELECT 9 AS id, 'z' AS data, 'x' AS cat",
            "'BY'",
        ),
        (
            "INSERT INTO ice.sales.t REPLACE WHERE cat = 'x'",
            "end of input",
        ),
    ];
    for (sql, near) in cases {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
        assert_refusal(
            &ctx,
            &catalogs,
            sql,
            &format!("[PARSE_SYNTAX_ERROR] Syntax error at or near {near}. SQLSTATE: 42601"),
        )
        .await;
    }
}

#[tokio::test]
async fn a_where_inside_the_query_is_not_replace_where() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PARTITIONED).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT id + 10, data, cat FROM ice.sales.t AS replace WHERE cat = 'y'",
    )
    .await;
    assert_eq!(
        sorted_rows(&ctx, &catalogs).await,
        vec![
            row(1, "a", "x"),
            row(12, "b", "y"),
            row(2, "b", "y"),
            row(3, "c", "x")
        ]
    );
    let (operation, _) = latest_summary(&catalogs).await;
    assert_eq!(operation, "append");
}
