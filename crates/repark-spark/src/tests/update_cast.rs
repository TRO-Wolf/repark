use super::super::*;
use super::common::{run, setup};
use tempfile::TempDir;

async fn door() -> (TempDir, SessionContext, CatalogRegistry) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id BIGINT, data STRING) USING iceberg",
    )
    .await;
    (warehouse, ctx, catalogs)
}

async fn plan_err(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    match execute(ctx, catalogs, sql).await {
        Ok(_) => panic!("`{sql}` must refuse"),
        Err(DataFusionError::Plan(inner)) => inner,
        Err(err) => panic!("`{sql}` must fail as a plan error: {err}"),
    }
}

fn assert_cannot_safely_cast(err: &str) {
    assert!(
        err.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"),
        "{err}"
    );
    assert!(err.contains("SQLSTATE: KD000"), "{err}");
    assert!(
        err.contains("Cannot safely cast `id` \"STRING\" to \"BIGINT\""),
        "{err}"
    );
}

#[tokio::test]
async fn update_string_literal_into_bigint_stamps_cannot_safely_cast() {
    let (_warehouse, ctx, catalogs) = door().await;
    let err = plan_err(&ctx, &catalogs, "UPDATE ice.sales.t SET id = 'notanumber'").await;
    assert_cannot_safely_cast(&err);
}

#[tokio::test]
async fn update_int_literal_into_bigint_succeeds() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(&ctx, &catalogs, "UPDATE ice.sales.t SET id = 7").await;
}

#[tokio::test]
async fn update_string_literal_into_string_succeeds() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(&ctx, &catalogs, "UPDATE ice.sales.t SET data = 'z'").await;
}

#[tokio::test]
async fn update_string_column_into_bigint_stamps_cannot_safely_cast() {
    let (_warehouse, ctx, catalogs) = door().await;
    let err = plan_err(&ctx, &catalogs, "UPDATE ice.sales.t SET id = data").await;
    assert_cannot_safely_cast(&err);
}

#[tokio::test]
async fn update_cast_to_bigint_succeeds() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.t SET id = CAST('1' AS BIGINT)",
    )
    .await;
}

#[tokio::test]
async fn update_missing_table_keeps_its_own_error() {
    let (_warehouse, ctx, catalogs) = door().await;
    let err = execute(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.missing SET id = 'notanumber'",
    )
    .await
    .expect_err("missing table must still fail")
    .to_string();
    assert!(!err.contains("CANNOT_SAFELY_CAST"), "{err}");
}

#[tokio::test]
async fn update_stacked_sign_int_into_timestamp_stamps_cannot_safely_cast() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.l (id INT, c TIMESTAMP) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (140, TIMESTAMP '2024-01-01 00:00:00')",
    )
    .await;
    let err = plan_err(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.l SET c = - -1 WHERE id = 140",
    )
    .await;
    assert!(
        err.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"),
        "{err}"
    );
    assert!(
        err.contains("Cannot safely cast `c` \"INT\" to \"TIMESTAMP\""),
        "{err}"
    );
    assert!(err.contains("SQLSTATE: KD000"), "{err}");
    let walls = execute(
        &ctx,
        &catalogs,
        "SELECT CAST(c AS STRING) AS c FROM ice.sales.l WHERE id = 140",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let rendered = walls[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(rendered.value(0), "2024-01-01 00:00:00");
}

#[tokio::test]
async fn insert_string_into_bigint_keeps_the_insert_path() {
    let (_warehouse, ctx, catalogs) = door().await;
    let text = match execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES ('notanumber', 'z')",
    )
    .await
    {
        Ok(frame) => match frame.collect().await {
            Ok(_) => panic!("INSERT string into BIGINT must fail on the insert path"),
            Err(err) => err.to_string(),
        },
        Err(err) => err.to_string(),
    };
    assert!(text.contains("Cannot cast string 'notanumber'"), "{text}");
    assert!(
        !text.contains("Cannot write incompatible data for the table"),
        "{text}"
    );
}

#[tokio::test]
async fn update_marks_user_casts_only_when_spark_cast_seam_is_installed() {
    use std::sync::Arc;

    use repark_core::ReparkSession;

    let warehouse = TempDir::new().unwrap();
    let session = ReparkSession::builder()
        .with_extension(Arc::new(crate::SparkExtension))
        .with_sql_dialect(Arc::new(crate::SparkDialect))
        .config("spark.sql.ansi.enabled", "false")
        .build()
        .unwrap();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().unwrap())
        .await
        .unwrap();
    session
        .sql("CREATE NAMESPACE ice.sales")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    session
        .sql("CREATE TABLE ice.sales.narrow_u (id INT, n INT, v BIGINT) USING iceberg")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    session
        .sql("INSERT INTO ice.sales.narrow_u VALUES (1, 0, 0), (2, 0, 0)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    session
        .sql("UPDATE ice.sales.narrow_u SET n = CAST(4294967297 AS INT) WHERE id = 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    session
        .sql("UPDATE ice.sales.narrow_u SET n = CAST(' 7 ' AS INT) WHERE id = 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batches = session
        .sql("SELECT id, n FROM ice.sales.narrow_u ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values: Vec<(i32, i32)> = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap();
            let ns = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap();
            (0..batch.num_rows()).map(move |row| (ids.value(row), ns.value(row)))
        })
        .collect();
    assert_eq!(values, vec![(1, 1), (2, 7)]);
    let err = match session
        .sql("UPDATE ice.sales.narrow_u SET v = 1e19 WHERE id = 1")
        .await
    {
        Ok(frame) => frame.collect().await.unwrap_err().to_string(),
        Err(error) => error.to_string(),
    };
    assert!(err.contains("CAST_OVERFLOW_IN_TABLE_INSERT"), "{err}");
}
