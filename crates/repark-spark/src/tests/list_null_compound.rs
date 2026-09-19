use super::super::*;
use super::common::*;

async fn surviving_ids(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) -> Vec<i64> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id FROM {table} ORDER BY id"),
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch.column(0);
        if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
            ids.extend((0..values.len()).map(|row| i64::from(values.value(row))));
        } else if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            ids.extend((0..values.len()).map(|row| values.value(row)));
        } else {
            panic!("id must read back as Int32 or Int64");
        }
    }
    ids
}

#[tokio::test]
async fn cow_delete_and_over_list_null_keeps_spark_ids() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.list_null (id INT, xs ARRAY<INT>) USING iceberg \
         TBLPROPERTIES('write.delete.mode' = 'copy-on-write')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.list_null VALUES (1, make_array(1, 2)), (2, NULL), (3, make_array(3)), \
         (4, make_array(4, 5))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.list_null WHERE id > 1 AND xs IS NULL",
    )
    .await;
    assert_eq!(
        surviving_ids(&ctx, &catalogs, "ice.sales.list_null").await,
        vec![1, 3, 4]
    );
}

#[tokio::test]
async fn cow_delete_or_over_list_null_keeps_spark_ids() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.list_null_or (id INT, xs ARRAY<INT>) USING iceberg \
         TBLPROPERTIES('write.delete.mode' = 'copy-on-write')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.list_null_or VALUES (1, make_array(1, 2)), (2, NULL), (3, make_array(3)), \
         (4, make_array(4, 5))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.list_null_or WHERE xs IS NULL OR id = 1",
    )
    .await;
    assert_eq!(
        surviving_ids(&ctx, &catalogs, "ice.sales.list_null_or").await,
        vec![3, 4]
    );
}

#[tokio::test]
async fn cow_delete_and_over_map_null_keeps_spark_ids() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.map_null (id INT, xs MAP<STRING, INT>) USING iceberg \
         TBLPROPERTIES('write.delete.mode' = 'copy-on-write')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.map_null VALUES (1, map(['k'], [1])), (2, NULL), (3, map(['a'], [3])), \
         (4, map(['b'], [4]))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.map_null WHERE id > 1 AND xs IS NULL",
    )
    .await;
    assert_eq!(
        surviving_ids(&ctx, &catalogs, "ice.sales.map_null").await,
        vec![1, 3, 4]
    );
}

#[tokio::test]
async fn cow_delete_or_over_map_null_keeps_spark_ids() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.map_null_or (id INT, xs MAP<STRING, INT>) USING iceberg \
         TBLPROPERTIES('write.delete.mode' = 'copy-on-write')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.map_null_or VALUES (1, map(['k'], [1])), (2, NULL), \
         (3, map(['a'], [3])), (4, map(['b'], [4]))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.map_null_or WHERE xs IS NULL OR id = 1",
    )
    .await;
    assert_eq!(
        surviving_ids(&ctx, &catalogs, "ice.sales.map_null_or").await,
        vec![3, 4]
    );
}

#[tokio::test]
async fn cow_delete_and_over_struct_null_keeps_spark_ids() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.struct_null (id INT, xs STRUCT<a: INT>) USING iceberg \
         TBLPROPERTIES('write.delete.mode' = 'copy-on-write')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.struct_null VALUES (1, NAMED_STRUCT('a', 1)), (2, NULL), \
         (3, NAMED_STRUCT('a', 3)), (4, NAMED_STRUCT('a', 4))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.struct_null WHERE id > 1 AND xs IS NULL",
    )
    .await;
    assert_eq!(
        surviving_ids(&ctx, &catalogs, "ice.sales.struct_null").await,
        vec![1, 3, 4]
    );
}

#[tokio::test]
async fn cow_delete_or_over_struct_null_keeps_spark_ids() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.struct_null_or (id INT, xs STRUCT<a: INT>) USING iceberg \
         TBLPROPERTIES('write.delete.mode' = 'copy-on-write')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.struct_null_or VALUES (1, NAMED_STRUCT('a', 1)), (2, NULL), \
         (3, NAMED_STRUCT('a', 3)), (4, NAMED_STRUCT('a', 4))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.struct_null_or WHERE xs IS NULL OR id = 1",
    )
    .await;
    assert_eq!(
        surviving_ids(&ctx, &catalogs, "ice.sales.struct_null_or").await,
        vec![3, 4]
    );
}
