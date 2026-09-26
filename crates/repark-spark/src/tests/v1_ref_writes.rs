use datafusion::arrow::array::AsArray;

use super::super::*;
use super::accept_any_refusals::refusal;
use super::call::call_count;
use super::common::*;
use super::ref_branch_on_empty::metadata_file_count;
use super::wap_branch::set_wap;
use super::wap_id::ref_heads;

const V1_WAP_DDL: &str = "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg \
     TBLPROPERTIES ('format-version'='1', 'write.wap.enabled'='true')";

async fn v1_table(ctx: &SessionContext, catalogs: &CatalogRegistry, seeded: bool) {
    run(ctx, catalogs, V1_WAP_DDL).await;
    if seeded {
        run(ctx, catalogs, "INSERT INTO ice.sales.t VALUES (1, 'a')").await;
    }
}

async fn ids(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<i32> {
    let mut found = time_travel_id_multiset(ctx, catalogs, sql).await;
    found.sort_unstable();
    found
}

async fn ref_names_and_kinds(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) -> Vec<(String, String)> {
    ref_heads(ctx, catalogs)
        .await
        .into_iter()
        .map(|(name, kind, _)| (name, kind))
        .collect()
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
async fn a_wap_branch_write_on_a_seeded_v1_table_commits_on_the_branch() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    v1_table(&ctx, &catalogs, true).await;
    set_wap(&ctx, Some("w1"), None);
    run(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (2, 'b')").await;
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2]
    );
    set_wap(&ctx, None, None);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1]
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_w1").await,
        vec![1, 2]
    );
    assert_eq!(
        ref_names_and_kinds(&ctx, &catalogs).await,
        vec![
            ("main".to_string(), "BRANCH".to_string()),
            ("w1".to_string(), "BRANCH".to_string()),
        ]
    );
    let table = load_sales_table(&catalogs, "t").await;
    assert_eq!(
        table.metadata().format_version(),
        iceberg::spec::FormatVersion::V1
    );
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.fast_forward('sales.t', 'main', 'w1')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "main");
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn a_wap_branch_write_on_an_empty_v1_table_commits_on_the_branch() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    v1_table(&ctx, &catalogs, false).await;
    set_wap(&ctx, Some("w1"), None);
    run(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (2, 'b')").await;
    set_wap(&ctx, None, None);
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_w1").await,
        vec![2]
    );
    let table = load_sales_table(&catalogs, "t").await;
    assert!(table.metadata().snapshot_for_ref("w1").is_some());
    assert!(table.metadata().snapshot_for_ref("main").is_none());
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
async fn branch_tag_branch_write_and_fast_forward_on_a_v1_table_commit_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    v1_table(&ctx, &catalogs, true).await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH b1").await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE TAG t1").await;
    assert_eq!(
        ref_names_and_kinds(&ctx, &catalogs).await,
        vec![
            ("b1".to_string(), "BRANCH".to_string()),
            ("main".to_string(), "BRANCH".to_string()),
            ("t1".to_string(), "TAG".to_string()),
        ]
    );
    let seed_head = load_sales_table(&catalogs, "t")
        .await
        .metadata()
        .current_snapshot_id()
        .unwrap();
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t.branch_b1 VALUES (2, 'b')",
    )
    .await;
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_b1").await,
        vec![1, 2]
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1]
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t.tag_t1").await,
        vec![1]
    );
    let branch_head = load_sales_table(&catalogs, "t")
        .await
        .metadata()
        .snapshot_for_ref("b1")
        .unwrap()
        .snapshot_id();
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.fast_forward('sales.t', 'main', 'b1')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "main");
    let previous = batches[0]
        .column(1)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0);
    let updated = batches[0]
        .column(2)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0);
    assert_eq!(previous, seed_head);
    assert_eq!(updated, branch_head);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn branch_procedures_on_a_v1_table_commit_a_new_ref() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    v1_table(&ctx, &catalogs, true).await;
    run(
        &ctx,
        &catalogs,
        "CALL ice.system.fast_forward('sales.t', 'nb', 'main')",
    )
    .await;
    let table = load_sales_table(&catalogs, "t").await;
    let main_head = table.metadata().current_snapshot_id().unwrap();
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref("nb")
            .map(|snapshot| snapshot.snapshot_id()),
        Some(main_head)
    );
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH b1").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t.branch_b1 VALUES (2, 'b')",
    )
    .await;
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.t', branch => 'b1', options => \
         map('min-input-files', '1'))",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(call_count(&batches[0], "rewritten_data_files_count"), 2);
    assert_eq!(call_count(&batches[0], "added_data_files_count"), 1);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1]
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_b1").await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn rewrite_data_files_on_a_v1_table_rewrites_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    v1_table(&ctx, &catalogs, true).await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (2, 'b')").await;
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.t', options => \
         map('min-input-files', '1'))",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(call_count(&batches[0], "rewritten_data_files_count"), 2);
    assert_eq!(call_count(&batches[0], "added_data_files_count"), 1);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn retained_branch_on_a_v1_table_lands_in_metadata_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    v1_table(&ctx, &catalogs, true).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH b2 RETAIN 7 DAYS WITH SNAPSHOT RETENTION 2 \
         SNAPSHOTS",
    )
    .await;
    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT name, type, max_reference_age_in_ms, min_snapshots_to_keep FROM ice.sales.t.refs \
         WHERE name = 'b2'",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "b2");
    assert_eq!(batches[0].column(1).as_string::<i32>().value(0), "BRANCH");
    assert_eq!(
        batches[0]
            .column(2)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
            .value(0),
        604_800_000
    );
    assert_eq!(
        batches[0]
            .column(3)
            .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
            .value(0),
        2
    );
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
