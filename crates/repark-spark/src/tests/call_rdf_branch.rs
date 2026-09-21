use super::super::*;
use super::call::call_count;
use super::common::*;

fn column_names(batch: &datafusion::arrow::array::RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

async fn seed_mk3(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id BIGINT, data STRING, cat STRING) USING iceberg \
             PARTITIONED BY (cat)"
        ),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!(
            "INSERT INTO ice.sales.{table} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (6, 'f', 'x')"
        ),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (3, 'c', 'x'), (7, 'g', 'x')"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!(
            "INSERT INTO ice.sales.{table} VALUES (4, 'd', 'x'), (5, 'e', 'y'), (8, 'h', 'x')"
        ),
    )
    .await;
}

async fn main_snapshot_id(catalogs: &CatalogRegistry, table: &str) -> i64 {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .current_snapshot_id()
        .expect("main head")
}

async fn ref_snapshot_id(catalogs: &CatalogRegistry, table: &str, name: &str) -> i64 {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .snapshot_for_ref(name)
        .expect("named ref")
        .snapshot_id()
}

fn rewrite_columns() -> Vec<String> {
    vec![
        "rewritten_data_files_count".to_string(),
        "added_data_files_count".to_string(),
        "rewritten_bytes_count".to_string(),
        "failed_data_files_count".to_string(),
        "removed_delete_files_count".to_string(),
    ]
}

#[tokio::test]
async fn call_rdf_branch_rewrites_only_the_named_branch() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mk3(&ctx, &catalogs, "rwb").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rwb CREATE BRANCH b1",
    )
    .await;
    let main_before = main_snapshot_id(&catalogs, "rwb").await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.rwb', branch => 'b1', options => \
         map('rewrite-all', 'true'))",
    )
    .await
    .expect("branch rewrite must run");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(column_names(&batches[0]), rewrite_columns());
    assert_eq!(call_count(&batches[0], "rewritten_data_files_count"), 5);
    assert_eq!(call_count(&batches[0], "added_data_files_count"), 2);
    assert!(call_count(&batches[0], "rewritten_bytes_count") > 0);
    assert_eq!(call_count(&batches[0], "failed_data_files_count"), 0);
    assert_eq!(call_count(&batches[0], "removed_delete_files_count"), 0);
    assert_eq!(main_snapshot_id(&catalogs, "rwb").await, main_before);
    assert_ne!(ref_snapshot_id(&catalogs, "rwb", "b1").await, main_before);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rwb").await,
        8
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rwb.branch_b1").await,
        8
    );
}

#[tokio::test]
async fn call_rdf_branch_unknown_ref_pins_the_fork_message() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mk3(&ctx, &catalogs, "rbg").await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.rbg', branch => 'ghost')",
    )
    .await
    .expect_err("unknown branch must refuse");
    assert_eq!(
        error.to_string(),
        "External error: snapshot ref 'ghost' not found"
    );
}

#[tokio::test]
async fn call_rdf_branch_with_dangling_deletes_refuses_both_spellings() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mk3(&ctx, &catalogs, "rbd").await;
    for sql in [
        "CALL ice.system.rewrite_data_files(table => 'sales.rbd', branch => 'b1', \
         'remove-dangling-deletes' => true)",
        "CALL ice.system.rewrite_data_files(table => 'sales.rbd', branch => 'b1', options => \
         map('remove-dangling-deletes', 'true'))",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("branch plus dangling deletes must refuse");
        let datafusion::error::DataFusionError::NotImplemented(message) = error else {
            panic!("expected a NotImplemented refusal, got {error}");
        };
        assert_eq!(
            message,
            "CALL rewrite_data_files 'branch' cannot be combined with 'remove-dangling-deletes' \
             yet -- the fork's dangling-delete pass reads main's head, not the named branch"
        );
    }
}

#[tokio::test]
async fn call_rdf_branch_null_binds_as_unset() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mk3(&ctx, &catalogs, "rbn").await;
    let main_before = main_snapshot_id(&catalogs, "rbn").await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.rbn', branch => NULL, options => \
         map('rewrite-all', 'true'))",
    )
    .await
    .expect("NULL branch must mean unset");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(column_names(&batches[0]), rewrite_columns());
    assert!(call_count(&batches[0], "rewritten_data_files_count") > 0);
    assert_ne!(main_snapshot_id(&catalogs, "rbn").await, main_before);
}

#[tokio::test]
async fn call_rdf_branch_main_behaves_like_the_default() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mk3(&ctx, &catalogs, "rbm").await;
    let main_before = main_snapshot_id(&catalogs, "rbm").await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.rbm', branch => 'main', options => \
         map('rewrite-all', 'true'))",
    )
    .await
    .expect("explicit main must rewrite");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(column_names(&batches[0]), rewrite_columns());
    assert!(call_count(&batches[0], "rewritten_data_files_count") > 0);
    assert_ne!(main_snapshot_id(&catalogs, "rbm").await, main_before);
}
