use std::collections::HashMap;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableCreation};
use repark_iceberg::catalog::{IcebergFileClass, IcebergIoCount, IcebergIoOp};
use tempfile::TempDir;

use super::super::*;

#[tokio::test]
async fn the_session_counts_its_iceberg_reads_and_resets_them() {
    let warehouse = TempDir::new().unwrap();
    let root = warehouse.path().to_str().unwrap();
    let session = ReparkSession::builder().build().unwrap();
    session.register_memory_catalog("ice", root).await.unwrap();
    let handle = session.catalogs_snapshot().get("ice").cloned().unwrap();
    let sales = NamespaceIdent::new("sales".to_string());
    handle
        .create_namespace(&sales, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap();
    let creation = TableCreation::builder()
        .name("orders".to_string())
        .location(format!("{root}/sales/orders"))
        .schema(schema)
        .properties(HashMap::new())
        .build();
    handle.create_table(&sales, creation).await.unwrap();
    session.refresh_catalog_provider("ice").await.unwrap();
    session
        .sql("INSERT INTO ice.sales.orders VALUES (1), (2), (3)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(
        session
            .iceberg_io_stats()
            .by_op(IcebergIoOp::Write)
            .requests
            > 0
    );

    session.reset_iceberg_io_stats();
    assert_eq!(
        session.iceberg_io_stats().total(),
        IcebergIoCount::default()
    );
    let rows: usize = session
        .sql("SELECT id FROM ice.sales.orders")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(datafusion::arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 3);
    let stats = session.iceberg_io_stats();
    let data = stats.get(IcebergIoOp::RangedRead, IcebergFileClass::DataFile);
    assert!(data.requests > 0 && data.bytes > 0, "stats {stats:?}");
    assert_eq!(stats.by_op(IcebergIoOp::Write), IcebergIoCount::default());
}
