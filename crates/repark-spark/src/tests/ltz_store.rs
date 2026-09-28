use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use tempfile::TempDir;

use super::super::*;
use super::common::{load_sales_table, run, setup};

async fn door() -> (TempDir, SessionContext, CatalogRegistry) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    ctx.add_analyzer_rule(Arc::new(repark_iceberg::InsertStoreAssignment));
    ctx.register_udf(
        repark_functions::timestamp_ntz_cast::timestamp_ntz_literal_udf()
            .as_ref()
            .clone(),
    );
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.l (id INT, c TIMESTAMP) USING iceberg",
    )
    .await;
    let field_type = load_sales_table(&catalogs, "l")
        .await
        .metadata()
        .current_schema()
        .as_struct()
        .fields()[1]
        .field_type
        .to_string();
    assert_eq!(field_type, "timestamptz", "TIMESTAMP means LTZ here");
    (warehouse, ctx, catalogs)
}

async fn plan_err(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    match execute(ctx, catalogs, sql).await {
        Ok(_) => panic!("`{sql}` must refuse"),
        Err(DataFusionError::Plan(inner)) => inner,
        Err(err) => err.to_string(),
    }
}

async fn id_and_c(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<(i32, Option<String>)> {
    let batches = execute(
        ctx,
        catalogs,
        "SELECT id, CAST(c AS STRING) AS c FROM ice.sales.l ORDER BY id",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let rendered = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            let text = if rendered.is_null(index) {
                None
            } else {
                Some(rendered.value(index).to_string())
            };
            rows.push((ids.value(index), text));
        }
    }
    rows
}

#[tokio::test]
async fn int_into_timestamp_refuses_on_every_door() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (0, TIMESTAMP '2024-01-01 00:00:00')",
    )
    .await;
    assert_eq!(
        plan_err(&ctx, &catalogs, "INSERT INTO ice.sales.l VALUES (0, 1)").await,
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table `ice`.`sales`.`l`: Cannot safely cast `c` \"INT\" to \"TIMESTAMP\". SQLSTATE: \
         KD000"
    );
    assert_eq!(
        plan_err(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.l (id, c) VALUES (0, 1)"
        )
        .await,
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table `ice`.`sales`.`l`: Cannot safely cast `c` \"INT\" to \"TIMESTAMP\". SQLSTATE: \
         KD000"
    );
    assert_eq!(
        plan_err(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.l (c, id) VALUES (1, 0)"
        )
        .await,
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table `ice`.`sales`.`l`: Cannot safely cast `c` \"INT\" to \"TIMESTAMP\". SQLSTATE: \
         KD000"
    );
    assert_eq!(
        plan_err(&ctx, &catalogs, "INSERT INTO ice.sales.l VALUES (0, -1)").await,
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table `ice`.`sales`.`l`: Cannot safely cast `c` \"INT\" to \"TIMESTAMP\". SQLSTATE: \
         KD000"
    );
    assert_eq!(
        plan_err(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.l VALUES (0, CAST(1 AS BIGINT))"
        )
        .await,
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table `ice`.`sales`.`l`: Cannot safely cast `c` \"BIGINT\" to \"TIMESTAMP\". \
         SQLSTATE: KD000"
    );
    let select = plan_err(&ctx, &catalogs, "INSERT INTO ice.sales.l SELECT 0, 1").await;
    assert!(
        select.contains("INSERT INTO cannot store-assign column `c`"),
        "{select}"
    );
    assert!(select.contains("not ANSI-store-assignable"), "{select}");
    assert!(
        select.contains("INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST"),
        "{select}"
    );
    let update = plan_err(&ctx, &catalogs, "UPDATE ice.sales.l SET c = 1").await;
    assert!(
        update.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"),
        "{update}"
    );
    assert!(update.contains("Cannot safely cast `c`"), "{update}");
    assert!(update.contains("SQLSTATE: KD000"), "{update}");
    let merge = plan_err(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.l AS t USING (SELECT 0 AS id, 1 AS v) AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET c = s.v",
    )
    .await;
    assert!(merge.contains("not ANSI-store-assignable"), "{merge}");
    assert!(merge.contains("INCOMPATIBLE_DATA_FOR_TABLE"), "{merge}");
    assert_eq!(
        id_and_c(&ctx, &catalogs).await,
        vec![(0, Some("2024-01-01 00:00:00".to_string()))]
    );
}

#[tokio::test]
async fn legal_sources_into_timestamp_still_store() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.l VALUES (0, NULL)").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (1, DATE '2024-01-04')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (2, TIMESTAMP '2024-01-01 00:00:00')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (3, TIMESTAMP_NTZ '2024-01-01 00:00:00')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (4, CAST(1 AS TIMESTAMP))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l (id, c) VALUES (5, TIMESTAMP '2023-05-05 05:05:05')",
    )
    .await;
    assert_eq!(
        id_and_c(&ctx, &catalogs).await,
        vec![
            (0, None),
            (1, Some("2024-01-04 00:00:00".to_string())),
            (2, Some("2024-01-01 00:00:00".to_string())),
            (3, Some("2024-01-01 00:00:00".to_string())),
            (4, Some("1970-01-01 00:00:01".to_string())),
            (5, Some("2023-05-05 05:05:05".to_string())),
        ]
    );
}

#[tokio::test]
async fn typed_numeric_values_into_timestamp_refuse() {
    let (_warehouse, ctx, catalogs) = door().await;
    for target in [
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL(10,2)",
    ] {
        let sql = format!("INSERT INTO ice.sales.l VALUES (0, CAST(1 AS {target}))");
        let refused = plan_err(&ctx, &catalogs, &sql).await;
        assert!(
            refused.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"),
            "{sql}: {refused}"
        );
        assert!(refused.contains("SQLSTATE: KD000"), "{sql}: {refused}");
    }
    for cell in ["DECIMAL '1.5'", "1L"] {
        let sql = format!("INSERT INTO ice.sales.l VALUES (0, {cell})");
        let refused = plan_err(&ctx, &catalogs, &sql).await;
        assert!(
            refused.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"),
            "{sql}: {refused}"
        );
        assert!(refused.contains("SQLSTATE: KD000"), "{sql}: {refused}");
    }
    assert_eq!(
        plan_err(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.l VALUES (1, CAST(NULL AS INT))"
        )
        .await,
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table `ice`.`sales`.`l`: Cannot safely cast `c` \"INT\" to \"TIMESTAMP\". SQLSTATE: \
         KD000"
    );
    assert_eq!(
        plan_err(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.l VALUES (1, CAST(NULL AS DECIMAL(10,2)))"
        )
        .await,
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table `ice`.`sales`.`l`: Cannot safely cast `c` \"DECIMAL(10,2)\" to \"TIMESTAMP\". \
         SQLSTATE: KD000"
    );
    assert_eq!(id_and_c(&ctx, &catalogs).await, vec![]);
}

#[tokio::test]
async fn mixed_timestamp_and_int_rows_refuse_without_writing() {
    let (_warehouse, ctx, catalogs) = door().await;
    let mixed = plan_err(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (3, TIMESTAMP '2024-03-03 00:00:00'), (4, 5)",
    )
    .await;
    assert!(
        mixed.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"),
        "{mixed}"
    );
    assert!(mixed.contains("SQLSTATE: KD000"), "{mixed}");
    assert_eq!(id_and_c(&ctx, &catalogs).await, vec![]);
}

#[tokio::test]
async fn fractional_and_large_integer_literals_name_spark_types() {
    let (_warehouse, ctx, catalogs) = door().await;
    assert_eq!(
        plan_err(&ctx, &catalogs, "INSERT INTO ice.sales.l VALUES (5, 1.5)").await,
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table `ice`.`sales`.`l`: Cannot safely cast `c` \"DECIMAL(2,1)\" to \"TIMESTAMP\". \
         SQLSTATE: KD000"
    );
    assert_eq!(
        plan_err(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.l VALUES (6, 12345678901)"
        )
        .await,
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table `ice`.`sales`.`l`: Cannot safely cast `c` \"BIGINT\" to \"TIMESTAMP\". \
         SQLSTATE: KD000"
    );
    assert_eq!(id_and_c(&ctx, &catalogs).await, vec![]);
}

#[tokio::test]
async fn nvl_and_ifnull_over_temporal_values_store() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (0, nvl(NULL, DATE '2024-01-01'))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (1, ifnull(NULL, DATE '2024-01-01'))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (2, nvl(NULL, TIMESTAMP '2024-01-01 00:00:00'))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (3, ifnull(NULL, TIMESTAMP '2024-01-01 00:00:00'))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (4, nvl(DATE '2024-01-02', NULL))",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.l VALUES (5, coalesce(NULL, DATE '2024-01-01'))",
    )
    .await;
    assert_eq!(
        id_and_c(&ctx, &catalogs).await,
        vec![
            (0, Some("2024-01-01 00:00:00".to_string())),
            (1, Some("2024-01-01 00:00:00".to_string())),
            (2, Some("2024-01-01 00:00:00".to_string())),
            (3, Some("2024-01-01 00:00:00".to_string())),
            (4, Some("2024-01-02 00:00:00".to_string())),
            (5, Some("2024-01-01 00:00:00".to_string())),
        ]
    );
}
