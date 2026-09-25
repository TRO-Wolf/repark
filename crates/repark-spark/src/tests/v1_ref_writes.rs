use super::super::*;
use super::accept_any_refusals::refusal;
use super::common::*;
use super::ref_branch_on_empty::{metadata_file_count, v1_ref_refusal};
use super::wap_branch::set_wap;

const V1_WAP_DDL: &str = "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg \
     TBLPROPERTIES ('format-version'='1', 'write.wap.enabled'='true')";

async fn v1_table(ctx: &SessionContext, catalogs: &CatalogRegistry, seeded: bool) {
    run(ctx, catalogs, V1_WAP_DDL).await;
    if seeded {
        run(ctx, catalogs, "INSERT INTO ice.sales.t VALUES (1, 'a')").await;
    }
}

async fn assert_refused_with_nothing_written(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    ref_name: &str,
) {
    let expected = v1_ref_refusal("BRANCH", "sales.t");
    assert_text_refused_with_nothing_written(ctx, catalogs, sql, ref_name, &expected).await;
}

async fn assert_text_refused_with_nothing_written(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    ref_name: &str,
    expected: &str,
) {
    let before = load_sales_table(catalogs, "t").await;
    let files = metadata_file_count(&before);
    let snapshots = before.metadata().snapshots().count();
    let error = refusal(ctx, catalogs, sql).await;
    assert_eq!(error.to_string(), expected, "{sql}");
    let after = load_sales_table(catalogs, "t").await;
    assert_eq!(metadata_file_count(&after), files, "{sql}");
    assert_eq!(after.metadata().snapshots().count(), snapshots, "{sql}");
    assert!(
        after.metadata().snapshot_for_ref(ref_name).is_none(),
        "{sql}"
    );
}

#[tokio::test]
async fn a_wap_branch_write_on_a_seeded_v1_table_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    v1_table(&ctx, &catalogs, true).await;
    set_wap(&ctx, Some("w1"), None);
    assert_refused_with_nothing_written(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (2, 'b')",
        "w1",
    )
    .await;
    set_wap(&ctx, None, None);
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 1);
}

#[tokio::test]
async fn a_wap_branch_write_on_an_empty_v1_table_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    v1_table(&ctx, &catalogs, false).await;
    set_wap(&ctx, Some("w1"), None);
    assert_refused_with_nothing_written(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (2, 'b')",
        "w1",
    )
    .await;
}

#[tokio::test]
async fn writes_into_a_missing_branch_on_a_v1_table_answer_the_missing_branch_text() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    v1_table(&ctx, &catalogs, true).await;
    for sql in [
        "INSERT INTO ice.sales.t.branch_b1 VALUES (2, 'b')",
        "DELETE FROM ice.sales.t.branch_b1 WHERE id = 1",
        "MERGE INTO ice.sales.t.branch_b1 t USING (SELECT 1 AS id, 'q' AS name) u ON t.id = u.id \
         WHEN MATCHED THEN UPDATE SET name = u.name",
        "INSERT OVERWRITE ice.sales.t.branch_b1 VALUES (3, 'c')",
    ] {
        assert_text_refused_with_nothing_written(
            &ctx,
            &catalogs,
            sql,
            "b1",
            "Error during planning: Cannot use branch (does not exist): b1",
        )
        .await;
    }
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 1);
}

#[tokio::test]
async fn branch_procedures_on_a_v1_table_refuse_a_new_ref() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    v1_table(&ctx, &catalogs, true).await;
    assert_refused_with_nothing_written(
        &ctx,
        &catalogs,
        "CALL ice.system.fast_forward('sales.t', 'nb', 'main')",
        "nb",
    )
    .await;
    assert_refused_with_nothing_written(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.t', branch => 'b1')",
        "b1",
    )
    .await;
}

#[tokio::test]
async fn main_only_procedures_on_a_v1_table_keep_working() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    v1_table(&ctx, &catalogs, true).await;
    let first = load_sales_table(&catalogs, "t")
        .await
        .metadata()
        .current_snapshot_id()
        .unwrap();
    run(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (2, 'b')").await;
    run(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.set_current_snapshot('sales.t', {first})"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CALL ice.system.fast_forward('sales.t', 'main', 'main')",
    )
    .await;
    let table = load_sales_table(&catalogs, "t").await;
    assert_eq!(table.metadata().current_snapshot_id(), Some(first));
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 1);
}
