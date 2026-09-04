use datafusion::arrow::array::{Array, BinaryArray};
use datafusion::arrow::compute;

use super::super::*;
use super::common::*;

async fn view_typed_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(i32, Option<String>, Option<Vec<u8>>)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id, name, payload FROM {table} ORDER BY id"),
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = compute::cast(batch.column(0), &DataType::Int32).unwrap();
        let ids = ids.as_any().downcast_ref::<Int32Array>().unwrap();
        let names = compute::cast(batch.column(1), &DataType::Utf8).unwrap();
        let names = names.as_any().downcast_ref::<StringArray>().unwrap();
        let payloads = compute::cast(batch.column(2), &DataType::Binary).unwrap();
        let payloads = payloads.as_any().downcast_ref::<BinaryArray>().unwrap();
        for index in 0..batch.num_rows() {
            let name = if names.is_null(index) {
                None
            } else {
                Some(names.value(index).to_string())
            };
            let payload = if payloads.is_null(index) {
                None
            } else {
                Some(payloads.value(index).to_vec())
            };
            rows.push((ids.value(index), name, payload));
        }
    }
    rows
}

fn expected_view_typed_rows() -> Vec<(i32, Option<String>, Option<Vec<u8>>)> {
    vec![
        (1, Some("a".to_string()), Some(b"x".to_vec())),
        (2, None, Some(b"y".to_vec())),
        (3, Some("c".to_string()), None),
    ]
}

#[tokio::test]
async fn unpartitioned_ctas_from_view_typed_batches_round_trips() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_view_typed_source(&ctx, "viewsrc");
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.viewtyped USING iceberg AS SELECT * FROM viewsrc",
    )
    .await
    .expect("unpartitioned CTAS from Utf8View/BinaryView batches must commit");
    assert_eq!(
        view_typed_rows(&ctx, &catalogs, "ice.sales.viewtyped").await,
        expected_view_typed_rows(),
    );
}

#[tokio::test]
async fn partitioned_ctas_from_view_typed_batches_still_round_trips() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_view_typed_source(&ctx, "viewsrc");
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.viewpart USING iceberg PARTITIONED BY (id) AS \
             SELECT * FROM viewsrc",
    )
    .await
    .expect("partitioned CTAS from the same view-typed batches must still commit");
    assert_eq!(
        view_typed_rows(&ctx, &catalogs, "ice.sales.viewpart").await,
        expected_view_typed_rows(),
    );
}
