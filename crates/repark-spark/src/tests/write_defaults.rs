use super::super::*;
use super::common::*;

use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema as IcebergSchema, Type};

async fn defaulted_door(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    let schema = IcebergSchema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Int))
                .with_write_default(Literal::int(5))
                .into(),
        ])
        .build()
        .unwrap();
    let creation = TableCreation::builder()
        .name("d".to_string())
        .schema(schema)
        .properties(HashMap::new())
        .build();
    let catalog = Arc::clone(catalog_handle(&catalogs, "ice").unwrap());
    catalog
        .create_table(&NamespaceIdent::new("sales".into()), creation)
        .await
        .unwrap();
    crate::catalog_ops::reregister(&ctx, catalog, "ice", "sales")
        .await
        .unwrap();
    (ctx, catalogs)
}

async fn single_row(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) -> (i32, String, Option<i32>) {
    let batches = execute(ctx, catalogs, "SELECT id, name, c FROM ice.sales.d")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1, "expected one row, got {total}");
    let batch = &batches[0];
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let fills = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let fill = (!fills.is_null(0)).then(|| fills.value(0));
    (ids.value(0), names.value(0).to_string(), fill)
}

async fn refusal(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    let outcome = match execute(ctx, catalogs, sql).await {
        Ok(frame) => frame.collect().await.map(|_| ()),
        Err(err) => Err(err),
    };
    match outcome {
        Ok(()) => panic!("`{sql}` must refuse"),
        Err(err) => err.to_string(),
    }
}

#[tokio::test]
async fn spark_overwrite_default_keyword_fills_write_default() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = defaulted_door(&wh).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.d VALUES (15, 'o', DEFAULT)",
    )
    .await;
    assert_eq!(
        single_row(&ctx, &catalogs).await,
        (15, "o".to_string(), Some(5))
    );
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.d (id, name, c) SELECT 18, 'r', DEFAULT",
    )
    .await;
    assert_eq!(
        single_row(&ctx, &catalogs).await,
        (18, "r".to_string(), Some(5))
    );
}

#[tokio::test]
async fn spark_default_in_outer_select_under_with_refuses_unresolved() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = defaulted_door(&wh).await;
    for verb in ["INSERT OVERWRITE", "INSERT INTO"] {
        let sql = format!(
            "{verb} ice.sales.d WITH x AS (SELECT 20 AS id, 'z' AS name) \
             SELECT id, name, DEFAULT FROM x"
        );
        let err = refusal(&ctx, &catalogs, &sql).await;
        assert!(
            err.contains("UNRESOLVED_COLUMN") && err.contains("`DEFAULT`") && err.contains("42703"),
            "{verb}: {err}"
        );
    }
    let empty = rows(&ctx, &catalogs, "SELECT * FROM ice.sales.d").await;
    assert_eq!(empty, 0);
}

#[tokio::test]
async fn spark_default_inside_cte_body_or_subquery_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = defaulted_door(&wh).await;
    for sql in [
        "INSERT OVERWRITE ice.sales.d WITH x AS (SELECT 18 AS id, 'r' AS name, DEFAULT AS c) \
         SELECT * FROM x",
        "INSERT OVERWRITE ice.sales.d SELECT * FROM (SELECT 19 AS id, 'q' AS name, DEFAULT AS c)",
    ] {
        let err = refusal(&ctx, &catalogs, sql).await;
        assert!(err.to_ascii_lowercase().contains("default"), "{sql}: {err}");
    }
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.d").await, 0);
}
