use std::collections::HashMap;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::super::iceberg_caches::caches_of;
use super::super::*;

fn schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap()
}

#[tokio::test]
async fn the_report_carries_evictions_beside_the_legacy_triple() {
    let warehouse = TempDir::new().unwrap();
    let root = warehouse.path().to_str().unwrap();
    let session = ReparkSession::builder()
        .config("repark.iceberg.metadataCacheEntries", "1")
        .build()
        .unwrap();
    session.register_memory_catalog("ice", root).await.unwrap();
    let handle = session.catalogs_snapshot().get("ice").cloned().unwrap();
    let sales = NamespaceIdent::new("sales".to_string());
    handle
        .create_namespace(&sales, HashMap::new())
        .await
        .unwrap();
    let names = [("a", 'a'), ("b", 'b'), ("c", 'c')];
    for (name, fill) in names {
        let creation = TableCreation::builder()
            .name(name.to_string())
            .location(format!("{root}/sales/{name}"))
            .schema(schema())
            .properties(HashMap::from([(
                "fill".to_string(),
                std::iter::repeat_n(fill, 40 * 1024).collect(),
            )]))
            .build();
        handle.create_table(&sales, creation).await.unwrap();
    }
    let metadata = caches_of(&session.catalogs).metadata_cache().unwrap();
    for (name, fill) in names.iter().chain(names.iter()) {
        let table = handle
            .load_table(&TableIdent::from_strs(["sales", name]).unwrap())
            .await
            .unwrap();
        let value = table.metadata().properties().get("fill").unwrap();
        assert!(value.chars().all(|c| c == *fill), "{name}");
        metadata.run_pending_tasks().await;
    }
    let report = session.iceberg_metadata_cache_report().unwrap();
    assert!(report.evictions > 0, "{report:?}");
    assert_eq!(
        session.iceberg_metadata_cache_stats(),
        Some((report.hits, report.misses, report.body_fetches))
    );
}

#[test]
fn a_disabled_cache_reports_nothing_on_either_surface() {
    let session = ReparkSession::builder()
        .config("repark.iceberg.metadataCache", "false")
        .build()
        .unwrap();
    assert_eq!(session.iceberg_metadata_cache_report(), None);
    assert_eq!(session.iceberg_metadata_cache_stats(), None);
}

#[test]
fn two_sessions_never_share_a_metadata_cache() {
    let one = ReparkSession::builder().build().unwrap();
    let two = ReparkSession::builder().build().unwrap();
    let one = caches_of(&one.catalogs).metadata_cache().unwrap();
    let two = caches_of(&two.catalogs).metadata_cache().unwrap();
    assert!(!Arc::ptr_eq(&one, &two));
}

#[test]
fn configured_aws_catalogs_are_built_with_the_session_caches() {
    let source = include_str!("../../session.rs");
    let start = source.find("async fn register_catalog_spec(").unwrap();
    let body = &source[start..start + source[start..].find("\n    }\n").unwrap()];
    for builder in ["glue_catalog_counted(", "s3tables_catalog_counted("] {
        let call = &body[body.find(builder).unwrap()..];
        let args = &call[..call.find(".await").unwrap()];
        assert!(
            args.contains("&iceberg_caches::caches_of(&self.catalogs)"),
            "{builder} {args}"
        );
    }
    assert!(!body.contains("iceberg_io_counters()"));
}
