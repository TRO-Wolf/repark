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
