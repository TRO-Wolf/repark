use std::collections::HashMap;

use datafusion::arrow::array::RecordBatch;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use repark_iceberg::catalog::{IcebergFileClass, IcebergIoOp, ParquetFooterCacheStats};
use tempfile::TempDir;

use super::super::iceberg_caches::caches_of;
use super::super::*;

async fn with_orders(session: &ReparkSession, root: &str) {
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
}

async fn footer_reads(session: &ReparkSession) -> u64 {
    session.reset_iceberg_io_stats();
    let rows: usize = session
        .sql("SELECT id FROM ice.sales.orders")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 3);
    session
        .iceberg_io_stats()
        .get(IcebergIoOp::FooterRead, IcebergFileClass::DataFile)
        .requests
}

#[tokio::test]
async fn the_session_reports_its_footer_cache_and_a_warm_scan_reads_no_footer() {
    let warehouse = TempDir::new().unwrap();
    let root = warehouse.path().to_str().unwrap();
    let session = ReparkSession::builder().build().unwrap();
    assert_eq!(
        session.iceberg_footer_cache_stats(),
        Some(ParquetFooterCacheStats::default())
    );
    with_orders(&session, root).await;
    let cold = footer_reads(&session).await;
    let warm = footer_reads(&session).await;
    assert!(cold > 0);
    assert_eq!(warm, 0);
    let stats = session.iceberg_footer_cache_stats().unwrap();
    assert!(stats.hits > 0, "{stats:?}");
    assert!(stats.misses > 0, "{stats:?}");
    assert_eq!(stats.fetches, cold, "{stats:?}");
}

#[tokio::test]
async fn a_zero_budget_session_reports_nothing_and_rereads_every_footer() {
    let warehouse = TempDir::new().unwrap();
    let root = warehouse.path().to_str().unwrap();
    let session = ReparkSession::builder()
        .config("repark.iceberg.footerCacheBytes", "0")
        .build()
        .unwrap();
    assert_eq!(session.iceberg_footer_cache_stats(), None);
    with_orders(&session, root).await;
    let cold = footer_reads(&session).await;
    assert!(cold > 0);
    assert_eq!(footer_reads(&session).await, cold);
    assert_eq!(session.iceberg_footer_cache_stats(), None);
}

#[test]
fn a_bad_footer_budget_fails_the_build_naming_the_key() {
    let error = ReparkSession::builder()
        .config("repark.iceberg.footer_cache_bytes", "lots")
        .build()
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("repark.iceberg.footer_cache_bytes"),
        "{error}"
    );
    assert!(error.contains("repark.iceberg.footerCacheBytes"), "{error}");
}

#[tokio::test]
async fn two_sessions_never_share_a_footer_cache() {
    let one = ReparkSession::builder().build().unwrap();
    let two = ReparkSession::builder().build().unwrap();
    assert!(!Arc::ptr_eq(
        &caches_of(&one.catalogs).footer_cache().unwrap(),
        &caches_of(&two.catalogs).footer_cache().unwrap()
    ));
    let warehouse = TempDir::new().unwrap();
    let root = warehouse.path().to_str().unwrap();
    with_orders(&one, root).await;
    footer_reads(&one).await;
    assert_eq!(footer_reads(&one).await, 0);
    let orders = TableIdent::from_strs(["sales", "orders"]).unwrap();
    let location = one
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .unwrap()
        .load_table(&orders)
        .await
        .unwrap()
        .metadata_location()
        .unwrap()
        .to_string();
    two.register_memory_catalog("ice", root).await.unwrap();
    let handle = two.catalogs_snapshot().get("ice").cloned().unwrap();
    handle
        .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
        .await
        .unwrap();
    handle.register_table(&orders, location).await.unwrap();
    two.refresh_catalog_provider("ice").await.unwrap();
    assert!(footer_reads(&two).await > 0);
    let stats = two.iceberg_footer_cache_stats().unwrap();
    assert_eq!(stats.hits, 0, "{stats:?}");
}
