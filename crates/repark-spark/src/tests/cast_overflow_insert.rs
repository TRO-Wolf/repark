use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use tempfile::TempDir;

use super::super::*;
use super::common::{run, setup};

async fn door() -> (TempDir, SessionContext, CatalogRegistry) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    ctx.add_analyzer_rule(Arc::new(repark_iceberg::StoreOverflowCast));
    repark_iceberg::write::store_cast::register_store_cast_udfs(&ctx);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.cov (id BIGINT, v BIGINT) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.cov VALUES (1, 0)").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.srcd (id BIGINT, d DOUBLE) USING iceberg",
    )
    .await;
    (warehouse, ctx, catalogs)
}

async fn refusal(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    match execute(ctx, catalogs, sql).await {
        Ok(frame) => match frame.collect().await {
            Ok(_) => panic!("`{sql}` must refuse"),
            Err(error) => error.to_string(),
        },
        Err(error) => error.to_string(),
    }
}

fn overflow_head(message: &str, from: &str, to: &str, column: &str) {
    assert!(
        message.contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
        "{message}"
    );
    assert!(message.contains(&format!("\"{from}\" type")), "{message}");
    assert!(message.contains(&format!("\"{to}\" type")), "{message}");
    assert!(message.contains(&format!("`{column}`")), "{message}");
    assert!(message.contains("SQLSTATE: 22003"), "{message}");
}

async fn values_of(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<i64> {
    let batches = execute(ctx, catalogs, "SELECT v FROM ice.sales.cov ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches
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
        .collect()
}

#[tokio::test]
async fn insert_values_and_select_refuse_with_sparks_text() {
    let (_warehouse, ctx, catalogs) = door().await;
    for sql in [
        "INSERT INTO ice.sales.cov VALUES (10, CAST(1e19 AS DOUBLE))",
        "INSERT INTO ice.sales.cov SELECT 10, CAST(1e19 AS DOUBLE)",
        "INSERT INTO ice.sales.cov SELECT 10, CAST('NaN' AS DOUBLE)",
        "INSERT INTO ice.sales.cov SELECT 10, CAST('Infinity' AS DOUBLE)",
        "INSERT INTO ice.sales.cov SELECT 10, CAST(1e30 AS DECIMAL(38,0))",
    ] {
        let message = refusal(&ctx, &catalogs, sql).await;
        let from = if sql.contains("DECIMAL") {
            "DECIMAL(38,0)"
        } else {
            "DOUBLE"
        };
        overflow_head(&message, from, "BIGINT", "v");
    }
    assert_eq!(values_of(&ctx, &catalogs).await, vec![0]);
}

#[tokio::test]
async fn insert_select_column_and_division_refuse() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.srcd VALUES (1, CAST(1e19 AS DOUBLE)), (2, 42.0)",
    )
    .await;
    let message = refusal(&ctx, &catalogs, "INSERT INTO ice.sales.cov SELECT id, d FROM ice.sales.srcd").await;
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
    for sql in [
        "INSERT INTO ice.sales.cov SELECT 10, 0/0",
        "INSERT INTO ice.sales.cov SELECT id, (id - id) / (id - id) FROM ice.sales.cov",
    ] {
        let message = refusal(&ctx, &catalogs, sql).await;
        overflow_head(&message, "DOUBLE", "BIGINT", "v");
    }
    assert_eq!(values_of(&ctx, &catalogs).await, vec![0]);
}

#[tokio::test]
async fn in_range_and_boundary_values_store() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.cov VALUES (10, CAST(42.0 AS DOUBLE))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.cov SELECT 11, CAST(1.5 AS DOUBLE)",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.cov SELECT 12, CAST(9.223372036854776e18 AS DOUBLE)",
    )
    .await;
    assert_eq!(
        values_of(&ctx, &catalogs).await,
        vec![0, 42, 1, i64::MAX]
    );
}

#[tokio::test]
async fn update_refuses_per_row_and_stays_lazy_on_empty_match() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.cov SET v = CAST(1e19 AS DOUBLE) WHERE false",
    )
    .await;
    let message = refusal(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.cov SET v = CAST(1e19 AS DOUBLE)",
    )
    .await;
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
    let message = refusal(&ctx, &catalogs, "UPDATE ice.sales.cov SET v = 0/0").await;
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
    let message = refusal(&ctx, &catalogs, "UPDATE ice.sales.cov SET v = 1/0.0").await;
    overflow_head(&message, "DECIMAL(27,6)", "BIGINT", "v");
    assert_eq!(values_of(&ctx, &catalogs).await, vec![0]);
}

#[tokio::test]
async fn merge_arms_refuse_with_sparks_text() {
    let (_warehouse, ctx, catalogs) = door().await;
    let message = refusal(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.cov x USING (SELECT 1 AS id, CAST(1e19 AS DOUBLE) AS v) s ON x.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v",
    )
    .await;
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
    let message = refusal(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.cov x USING (SELECT 2 AS id, CAST(1e19 AS DOUBLE) AS v) s ON x.id = s.id WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
    )
    .await;
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
    let message = refusal(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.cov x USING (SELECT 2 AS id, 5.0 AS v) s ON x.id = s.id WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, 0/0)",
    )
    .await;
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
    assert_eq!(values_of(&ctx, &catalogs).await, vec![0]);
}

#[tokio::test]
async fn overwrite_and_by_name_refuse() {
    let (_warehouse, ctx, catalogs) = door().await;
    let message = refusal(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.cov SELECT id, CAST(1e19 AS DOUBLE) FROM ice.sales.cov",
    )
    .await;
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
    let message = refusal(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.cov BY NAME SELECT 10 AS id, CAST(1e19 AS DOUBLE) AS v",
    )
    .await;
    overflow_head(&message, "DOUBLE", "BIGINT", "v");
    assert_eq!(values_of(&ctx, &catalogs).await, vec![0]);
}

#[tokio::test]
async fn int_and_string_stores_keep_their_old_text() {
    let (_warehouse, ctx, catalogs) = door().await;
    let message = refusal(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.cov SELECT 10, CAST('abc' AS INT)",
    )
    .await;
    assert!(
        !message.contains("CAST_OVERFLOW_IN_TABLE_INSERT"),
        "{message}"
    );
    let message = refusal(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.cov SET v = CAST('abc' AS INT)",
    )
    .await;
    assert!(
        !message.contains("CAST_OVERFLOW_IN_TABLE_INSERT"),
        "{message}"
    );
}
