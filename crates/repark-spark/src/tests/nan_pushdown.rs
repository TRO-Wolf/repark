use super::super::*;
use super::common::*;

use datafusion::arrow::array::Int32Array;

async fn nan_ids(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<i32> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            ids.push(column.value(index));
        }
    }
    ids.sort();
    ids
}

async fn nan_seed(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.t (id INT, d DOUBLE, f FLOAT) USING iceberg",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t VALUES (1, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)), (2, CAST(1.0 AS DOUBLE), CAST(1.0 AS FLOAT)), (3, NULL, NULL)",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t VALUES (4, CAST(0.5 AS DOUBLE), CAST(0.5 AS FLOAT)), (5, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)), (6, CAST(-2.0 AS DOUBLE), CAST(-2.0 AS FLOAT))",
    )
    .await;
}

#[tokio::test]
async fn nan_equality_answers_the_nan_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    nan_seed(&ctx, &catalogs).await;
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d = CAST('NaN' AS DOUBLE) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE CAST('NaN' AS DOUBLE) = d ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d <=> CAST('NaN' AS DOUBLE) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE f = CAST('NaN' AS FLOAT) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
}

#[tokio::test]
async fn nan_in_and_inequality_answer_the_oracle_sets() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    nan_seed(&ctx, &catalogs).await;
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d IN (CAST('NaN' AS DOUBLE)) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d IN (CAST('NaN' AS DOUBLE), CAST(1.0 AS DOUBLE)) ORDER BY id"
        )
        .await,
        vec![1, 2, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d != CAST('NaN' AS DOUBLE) ORDER BY id"
        )
        .await,
        vec![2, 4, 6]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d NOT IN (CAST('NaN' AS DOUBLE)) ORDER BY id"
        )
        .await,
        vec![2, 4, 6]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d < CAST('NaN' AS DOUBLE) ORDER BY id"
        )
        .await,
        vec![2, 4, 6]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d >= CAST('NaN' AS DOUBLE) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE NOT (d = CAST('NaN' AS DOUBLE)) ORDER BY id"
        )
        .await,
        vec![2, 4, 6]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE isnan(d) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
}
