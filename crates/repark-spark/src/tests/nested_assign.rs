use super::super::*;
use super::common::*;

async fn struct_table(rows: &str) -> (TempDir, SessionContext, CatalogRegistry) {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id BIGINT, st STRUCT<a: INT, b: STRING>) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.t VALUES {rows}"),
    )
    .await;
    (wh, ctx, catalogs)
}

async fn text(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<String> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
        .lines()
        .filter(|line| line.starts_with("| "))
        .skip(1)
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect()
}

async fn last_summary(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<String> {
    text(
        ctx,
        catalogs,
        "SELECT operation, summary['added-records'], summary['deleted-records'], \
         summary['total-records'], summary['added-data-files'], summary['deleted-data-files'] \
         FROM ice.sales.t.snapshots ORDER BY committed_at DESC LIMIT 1",
    )
    .await
}

async fn columns(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<String> {
    let frame = execute(ctx, catalogs, "SELECT * FROM ice.sales.t")
        .await
        .unwrap();
    frame
        .schema()
        .fields()
        .iter()
        .map(|field| format!("{}: {}", field.name(), field.data_type()))
        .collect()
}

#[tokio::test]
async fn update_sets_one_struct_field_and_keeps_its_siblings() {
    let (_wh, ctx, catalogs) = struct_table("(1, named_struct('a', 1, 'b', 'p'))").await;
    let before = columns(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.t SET st.a = 99 WHERE id = 1",
    )
    .await;
    assert_eq!(
        text(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        ["| 1 | {a: 99, b: p} |"]
    );
    assert_eq!(columns(&ctx, &catalogs).await, before);
    assert_eq!(
        last_summary(&ctx, &catalogs).await,
        ["| overwrite | 1 | 1 | 1 | 1 | 1 |"]
    );
}

#[tokio::test]
async fn merge_sets_one_struct_field_from_the_source() {
    let (_wh, ctx, catalogs) =
        struct_table("(1, named_struct('a', 1, 'b', 'p')), (2, named_struct('a', 2, 'b', 'q'))")
            .await;
    let before = columns(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t t USING (SELECT 2 AS id, 20 AS na) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.st.a = s.na",
    )
    .await;
    assert_eq!(
        text(&ctx, &catalogs, "SELECT * FROM ice.sales.t ORDER BY id").await,
        ["| 1 | {a: 1, b: p} |", "| 2 | {a: 20, b: q} |"]
    );
    assert_eq!(columns(&ctx, &catalogs).await, before);
    assert_eq!(
        last_summary(&ctx, &catalogs).await,
        ["| overwrite | 2 | 2 | 2 | 1 | 1 |"]
    );
}

#[tokio::test]
async fn whole_struct_merge_assignments_pass_the_store_assignment_gate() {
    let (_wh, ctx, catalogs) =
        struct_table("(1, named_struct('a', 1, 'b', 'p')), (2, named_struct('a', 2, 'b', 'q'))")
            .await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t t USING (SELECT 2 AS id, named_struct('a', 22, 'b', 'w') AS st) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET * \
         WHEN NOT MATCHED THEN INSERT *",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t t USING (SELECT 3 AS id, 30 AS na) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, named_struct('a', s.na, 'b', 'n'))",
    )
    .await;
    assert_eq!(
        text(&ctx, &catalogs, "SELECT * FROM ice.sales.t ORDER BY id").await,
        [
            "| 1 | {a: 1, b: p} |",
            "| 2 | {a: 22, b: w} |",
            "| 3 | {a: 30, b: n} |"
        ]
    );
}
