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

fn plan_message(error: datafusion::error::DataFusionError) -> String {
    let datafusion::error::DataFusionError::Plan(message) = error else {
        panic!("expected a Plan error, got {error}");
    };
    message
}

async fn seed_six_files(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} AS SELECT 1 AS id, 'a' AS name"),
    )
    .await;
    for index in 2..=6 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} SELECT {index} AS id, 'x' AS name"),
        )
        .await;
    }
}

#[tokio::test]
async fn call_rdf_four_positional_form_binds_in_declared_order() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_six_files(&ctx, &catalogs, "b4").await;
    seed_six_files(&ctx, &catalogs, "b4high").await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files('sales.b4', 'binpack', NULL, \
         map('min-input-files', '1'))",
    )
    .await
    .expect("four positional args must bind");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(
        column_names(&batches[0]),
        vec![
            "rewritten_data_files_count",
            "added_data_files_count",
            "rewritten_bytes_count",
            "failed_data_files_count",
            "removed_delete_files_count",
        ]
    );
    assert!(
        call_count(&batches[0], "rewritten_data_files_count") >= 2,
        "binpack must rewrite with min-input-files 1"
    );
    assert!(call_count(&batches[0], "added_data_files_count") >= 1);
    assert_eq!(call_count(&batches[0], "failed_data_files_count"), 0);
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT CAST(id AS INT) FROM ice.sales.b4").await,
        vec![1, 2, 3, 4, 5, 6]
    );
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files('sales.b4high', 'binpack', NULL, \
         map('min-input-files', '7'))",
    )
    .await
    .expect("four positional args must bind");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(call_count(&batches[0], "rewritten_data_files_count"), 0);
    assert_eq!(call_count(&batches[0], "added_data_files_count"), 0);
}

#[tokio::test]
async fn call_rpd_two_positional_options_bind() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.p2 AS SELECT * FROM src",
    )
    .await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files('sales.p2', map('rewrite-all', 'true'))",
    )
    .await
    .expect("two positional args must bind");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(
        column_names(&batches[0]),
        vec![
            "rewritten_delete_files_count",
            "added_delete_files_count",
            "rewritten_bytes_count",
            "added_bytes_count",
        ]
    );
    assert_eq!(call_count(&batches[0], "rewritten_delete_files_count"), 0);
    assert_eq!(call_count(&batches[0], "added_delete_files_count"), 0);
    assert_eq!(call_count(&batches[0], "rewritten_bytes_count"), 0);
    assert_eq!(call_count(&batches[0], "added_bytes_count"), 0);
}

#[tokio::test]
async fn call_rollback_mixed_positional_and_named_binds() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rmx AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "rmx".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let first = table.metadata().current_snapshot_id().expect("s1");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.rmx SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let second = table.metadata().current_snapshot_id().expect("s2");
    let frame = execute(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.rollback_to_snapshot('sales.rmx', snapshot_id => {first})"),
    )
    .await
    .expect("mixed args must bind");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(
        column_names(&batches[0]),
        vec!["previous_snapshot_id", "current_snapshot_id"]
    );
    assert_eq!(call_count(&batches[0], "previous_snapshot_id"), second);
    assert_eq!(call_count(&batches[0], "current_snapshot_id"), first);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rmx").await,
        3
    );
}

#[tokio::test]
async fn call_bind_duplicate_positional_and_named_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files('sales.dup', 'binpack', strategy => 'sort')",
    )
    .await
    .expect_err("duplicate binding must refuse");
    assert_eq!(
        plan_message(error),
        "CALL argument `strategy` is bound twice (positionally and by name)"
    );
}

#[tokio::test]
async fn call_bind_unknown_argument_names_allowed() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.x', bogus => 1)",
    )
    .await
    .expect_err("unknown argument must refuse");
    assert_eq!(
        plan_message(error),
        "unknown CALL argument `bogus`; allowed: table, strategy, sort_order, options, where, \
         branch, remove-dangling-deletes"
    );
}

#[tokio::test]
async fn call_bind_missing_required_names_position() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(strategy => 'binpack')",
    )
    .await
    .expect_err("missing required must refuse");
    assert_eq!(
        plan_message(error),
        "CALL argument `table` is required (named `table => …` or positional #0)"
    );
}

#[tokio::test]
async fn call_bind_excess_positional_names_arity() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files('a', 'b', 'c', map('k', 'v'), 'd', 'e', 'f')",
    )
    .await
    .expect_err("excess positional must refuse");
    assert_eq!(
        plan_message(error),
        "CALL accepts at most 6 positional argument(s); got 7"
    );
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files('a', NULL, NULL, NULL)",
    )
    .await
    .expect_err("excess positional must refuse");
    assert_eq!(
        plan_message(error),
        "CALL accepts at most 3 positional argument(s); got 4"
    );
}

#[tokio::test]
async fn call_rpd_positional_where_binds() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.w AS SELECT * FROM src",
    )
    .await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files('sales.w', NULL, 'id = 1')",
    )
    .await
    .expect("positional where must bind");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(
        column_names(&batches[0]),
        vec![
            "rewritten_delete_files_count",
            "added_delete_files_count",
            "rewritten_bytes_count",
            "added_bytes_count",
        ]
    );
    assert_eq!(call_count(&batches[0], "rewritten_delete_files_count"), 0);
    assert_eq!(call_count(&batches[0], "added_delete_files_count"), 0);
}

#[tokio::test]
async fn call_rdf_named_null_is_unset() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nn AS SELECT * FROM src",
    )
    .await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.nn', strategy => 'binpack', \
         sort_order => NULL)",
    )
    .await
    .expect("named NULL must mean unset");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(
        column_names(&batches[0]),
        vec![
            "rewritten_data_files_count",
            "added_data_files_count",
            "rewritten_bytes_count",
            "failed_data_files_count",
            "removed_delete_files_count",
        ]
    );
}
