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

async fn seed_partitioned_mor_with_deletes(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, cat STRING) USING iceberg PARTITIONED BY \
             (cat) TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read', \
             'write.merge.mode' = 'merge-on-read')"
        ),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (1, 'x'), (2, 'x')"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (3, 'y'), (4, 'y')"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("DELETE FROM ice.sales.{table} WHERE id = 1"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("DELETE FROM ice.sales.{table} WHERE id = 3"),
    )
    .await;
}

#[tokio::test]
async fn call_rpd_where_restricts_to_matching_partition() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_partitioned_mor_with_deletes(&ctx, &catalogs, "rwf").await;
    seed_partitioned_mor_with_deletes(&ctx, &catalogs, "rwu").await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.rwf', where => 'cat \
         = \"x\"', options => map('rewrite-all', 'true'))",
    )
    .await
    .expect("filtered rewrite must run");
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
    assert_eq!(call_count(&batches[0], "rewritten_delete_files_count"), 1);
    assert_eq!(call_count(&batches[0], "added_delete_files_count"), 1);
    assert!(call_count(&batches[0], "rewritten_bytes_count") > 0);
    assert!(call_count(&batches[0], "added_bytes_count") > 0);
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.rwu', options => \
         map('rewrite-all', 'true'))",
    )
    .await
    .expect("unfiltered rewrite must run");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(call_count(&batches[0], "rewritten_delete_files_count"), 2);
    assert_eq!(call_count(&batches[0], "added_delete_files_count"), 2);
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.rwf").await,
        vec![2, 4]
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.rwu").await,
        vec![2, 4]
    );
}

async fn snapshot_ids_ordered(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<i64> {
    let batches = execute(
        ctx,
        catalogs,
        &format!(
            "SELECT snapshot_id FROM ice.sales.{table}.snapshots ORDER BY committed_at, snapshot_id"
        ),
    )
    .await
    .expect("read snapshot log")
    .collect()
    .await
    .expect("collect snapshot log");
    let mut ids = Vec::new();
    for batch in &batches {
        let values = batch
            .column_by_name("snapshot_id")
            .expect("snapshot_id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot_id is bigint");
        for index in 0..values.len() {
            ids.push(values.value(index));
        }
    }
    ids
}

fn expire_row(batch: &datafusion::arrow::array::RecordBatch) -> Vec<i64> {
    vec![
        call_count(batch, "deleted_data_files_count"),
        call_count(batch, "deleted_position_delete_files_count"),
        call_count(batch, "deleted_equality_delete_files_count"),
        call_count(batch, "deleted_manifest_files_count"),
        call_count(batch, "deleted_manifest_lists_count"),
        call_count(batch, "deleted_statistics_files_count"),
    ]
}

fn expire_columns() -> Vec<String> {
    vec![
        "deleted_data_files_count".to_string(),
        "deleted_position_delete_files_count".to_string(),
        "deleted_equality_delete_files_count".to_string(),
        "deleted_manifest_files_count".to_string(),
        "deleted_manifest_lists_count".to_string(),
        "deleted_statistics_files_count".to_string(),
    ]
}

async fn seed_four_snapshots(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg"),
    )
    .await;
    for index in 1..=3 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} VALUES ({index}, 'v{index}')"),
        )
        .await;
    }
    run(
        ctx,
        catalogs,
        &format!("DELETE FROM ice.sales.{table} WHERE id = 1"),
    )
    .await;
}

#[tokio::test]
async fn call_expire_snapshot_ids_expires_exactly_those() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_four_snapshots(&ctx, &catalogs, "exn").await;
    seed_four_snapshots(&ctx, &catalogs, "exp").await;
    let named_ids = snapshot_ids_ordered(&ctx, &catalogs, "exn").await;
    assert_eq!(named_ids.len(), 4);
    let frame = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.expire_snapshots(table => 'sales.exn', snapshot_ids => array({}))",
            named_ids[0]
        ),
    )
    .await
    .expect("snapshot_ids must expire");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(column_names(&batches[0]), expire_columns());
    assert_eq!(expire_row(&batches[0]), vec![0, 0, 0, 0, 1, 0]);
    let remaining = snapshot_ids_ordered(&ctx, &catalogs, "exn").await;
    assert_eq!(remaining, named_ids[1..]);
    let positional_ids = snapshot_ids_ordered(&ctx, &catalogs, "exp").await;
    let frame = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.expire_snapshots('sales.exp', NULL, NULL, NULL, NULL, array({}))",
            positional_ids[0]
        ),
    )
    .await
    .expect("positional snapshot_ids must bind in declared order");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(expire_row(&batches[0]), vec![0, 0, 0, 0, 1, 0]);
    let remaining = snapshot_ids_ordered(&ctx, &catalogs, "exp").await;
    assert_eq!(remaining, positional_ids[1..]);
}

#[tokio::test]
async fn call_expire_accept_and_ignore_trio_equals_plain() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_four_snapshots(&ctx, &catalogs, "ex0").await;
    seed_four_snapshots(&ctx, &catalogs, "ex1").await;
    seed_four_snapshots(&ctx, &catalogs, "ex2").await;
    seed_four_snapshots(&ctx, &catalogs, "ex3").await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.expire_snapshots(table => 'sales.ex0', older_than => TIMESTAMP '2999-01-01 00:00:00')",
    )
    .await
    .expect("plain expiry must run");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(column_names(&batches[0]), expire_columns());
    let plain = expire_row(&batches[0]);
    for (table, argument) in [
        ("ex1", "stream_results => true"),
        ("ex2", "max_concurrent_deletes => 2"),
        ("ex3", "clean_expired_metadata => true"),
    ] {
        let frame = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.expire_snapshots(table => 'sales.{table}', older_than => \
                 TIMESTAMP '2999-01-01 00:00:00', {argument})"
            ),
        )
        .await
        .expect("ignored argument must be accepted");
        let batches = frame.collect().await.expect("collect");
        assert_eq!(column_names(&batches[0]), expire_columns());
        assert_eq!(expire_row(&batches[0]), plain);
        assert_eq!(snapshot_ids_ordered(&ctx, &catalogs, table).await.len(), 1);
    }
    assert_eq!(snapshot_ids_ordered(&ctx, &catalogs, "ex0").await.len(), 1);
}

#[tokio::test]
async fn call_expire_ignored_args_type_check() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ext AS SELECT * FROM src",
    )
    .await;
    for (argument, needle) in [
        ("stream_results => 1", "must be a boolean literal"),
        (
            "clean_expired_metadata => 'yes'",
            "must be a boolean literal",
        ),
        ("max_concurrent_deletes => 'many'", "is not an integer"),
        ("snapshot_ids => 1", "must be an array of integer literals"),
    ] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!("CALL ice.system.expire_snapshots(table => 'sales.ext', {argument})"),
        )
        .await
        .expect_err("mistyped argument must refuse");
        assert!(
            plan_message(error).contains(needle),
            "mistyped {argument} must name its type"
        );
    }
}

#[tokio::test]
async fn call_rm_sort_by_stays_a_loud_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.x', sort_by => array('id'))",
    )
    .await
    .expect_err("sort_by must refuse");
    assert_eq!(
        plan_message(error),
        "unknown CALL argument `sort_by`; allowed: table, use_caching, spec_id"
    );
}

#[tokio::test]
async fn call_rpd_where_malformed_refuses_with_parse_text() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.wm AS SELECT * FROM src",
    )
    .await;
    for where_sql in ["id =", "nope = 1"] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.rewrite_position_delete_files(table => 'sales.wm', where => \
                 '{where_sql}')"
            ),
        )
        .await
        .expect_err("malformed where must refuse");
        assert_eq!(
            plan_message(error),
            format!("Cannot parse predicates in where option: {where_sql}")
        );
    }
}
