use super::super::*;

use std::collections::HashMap;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use iceberg::arrow::ParquetFooterCache;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent, TableMetadataCache};
use iceberg_catalog_s3tables::S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN;
use tempfile::TempDir;

use crate::catalog::builders::memory_catalog_wired;
use crate::catalog::cache_wiring::{CacheWiredBuilder, cache_credential_context, wire_caches};

const SECRET: &str = "wJalrXUtnFEMI-NEVER-IN-A-SCOPE";

#[derive(Debug, Default)]
struct Recorder {
    metadata: Option<Arc<TableMetadataCache>>,
    manifest_bytes: Option<u64>,
    footer: Option<Arc<ParquetFooterCache>>,
    context: Option<String>,
}

impl CacheWiredBuilder for Recorder {
    fn wire_metadata_cache(mut self, cache: Arc<TableMetadataCache>) -> Self {
        self.metadata = Some(cache);
        self
    }

    fn wire_manifest_cache_bytes(mut self, bytes: u64) -> Self {
        self.manifest_bytes = Some(bytes);
        self
    }

    fn wire_footer_cache(mut self, cache: Arc<ParquetFooterCache>) -> Self {
        self.footer = Some(cache);
        self
    }

    fn wire_credential_context(mut self, context: String) -> Self {
        self.context = Some(context);
        self
    }
}

fn props_of(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

fn keyed(access_key: &str) -> HashMap<String, String> {
    props_of(&[
        ("aws_access_key_id", access_key),
        ("aws_secret_access_key", SECRET),
        ("aws_session_token", SECRET),
        ("region_name", "us-east-1"),
    ])
}

#[test]
fn the_session_handles_reach_the_builder() {
    let caches = CatalogCaches::default();
    let wired = wire_caches(Recorder::default(), &caches, &keyed("AKIAONE"));
    assert!(Arc::ptr_eq(
        wired.metadata.as_ref().unwrap(),
        &caches.metadata_cache().unwrap()
    ));
    assert_eq!(wired.manifest_bytes, Some(DEFAULT_MANIFEST_CACHE_BYTES));
    assert!(Arc::ptr_eq(
        wired.footer.as_ref().unwrap(),
        &caches.footer_cache().unwrap()
    ));
    assert_eq!(wired.context.as_deref(), Some("aws_access_key_id=AKIAONE"));
}

#[test]
fn disabled_caches_wire_no_handle_and_no_context() {
    let wired = wire_caches(
        Recorder::default(),
        &CatalogCaches::disabled(),
        &keyed("AKIAONE"),
    );
    assert!(wired.metadata.is_none());
    assert_eq!(wired.manifest_bytes, None);
    assert!(wired.footer.is_none());
    assert_eq!(wired.context, None);
}

#[test]
fn each_switch_is_honoured_alone() {
    let metadata_only = CatalogCaches::new(IcebergCacheSettings {
        manifest_cache_bytes: 0,
        ..IcebergCacheSettings::default()
    });
    let wired = wire_caches(Recorder::default(), &metadata_only, &keyed("AKIAONE"));
    assert!(wired.metadata.is_some());
    assert_eq!(wired.manifest_bytes, None);
    assert!(wired.footer.is_some());
    let manifest_only = CatalogCaches::new(IcebergCacheSettings {
        metadata_cache: false,
        manifest_cache_bytes: 4096,
        footer_cache_bytes: 0,
        ..IcebergCacheSettings::default()
    });
    let wired = wire_caches(Recorder::default(), &manifest_only, &keyed("AKIAONE"));
    assert!(wired.metadata.is_none());
    assert_eq!(wired.manifest_bytes, Some(4096));
    assert!(wired.footer.is_none());
    assert_eq!(wired.context, None);
    let footer_only = CatalogCaches::new(IcebergCacheSettings {
        metadata_cache: false,
        manifest_cache_bytes: 0,
        footer_cache_bytes: 4096,
        ..IcebergCacheSettings::default()
    });
    let wired = wire_caches(Recorder::default(), &footer_only, &keyed("AKIAONE"));
    assert!(wired.metadata.is_none());
    assert_eq!(wired.manifest_bytes, None);
    assert!(Arc::ptr_eq(
        wired.footer.as_ref().unwrap(),
        &footer_only.footer_cache().unwrap()
    ));
    assert_eq!(wired.context.as_deref(), Some("aws_access_key_id=AKIAONE"));
    let footer_off = CatalogCaches::new(IcebergCacheSettings {
        footer_cache_bytes: 0,
        ..IcebergCacheSettings::default()
    });
    let wired = wire_caches(Recorder::default(), &footer_off, &keyed("AKIAONE"));
    assert!(wired.metadata.is_some());
    assert!(wired.footer.is_none());
}

#[test]
fn only_credential_selectors_name_a_context_and_never_a_secret() {
    assert_eq!(
        cache_credential_context(&props_of(&[("region_name", "us-east-1")])),
        None
    );
    assert_eq!(
        cache_credential_context(&HashMap::<String, String>::new()),
        None
    );
    assert_ne!(
        cache_credential_context(&keyed("AKIAONE")),
        cache_credential_context(&keyed("AKIATWO"))
    );
    let profiles = [
        cache_credential_context(&props_of(&[("profile_name", "dev")])),
        cache_credential_context(&props_of(&[("profile_name", "prod")])),
        cache_credential_context(&props_of(&[(
            "client.assume-role.arn",
            "arn:aws:iam::1:role/a",
        )])),
        cache_credential_context(&props_of(&[(
            "client.assume-role.arn",
            "arn:aws:iam::1:role/b",
        )])),
    ];
    for (i, left) in profiles.iter().enumerate() {
        assert!(left.is_some());
        for right in &profiles[i + 1..] {
            assert_ne!(left, right);
        }
    }
    let context = cache_credential_context(&keyed("AKIAONE")).unwrap();
    assert!(!context.contains(SECRET), "{context}");
    let wired = wire_caches(
        Recorder::default(),
        &CatalogCaches::default(),
        &keyed("AKIAONE"),
    );
    assert!(!format!("{wired:?}").contains(SECRET));
}

async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_mins(1), future)
        .await
        .expect("an offline catalog build never waits on the network")
}

#[tokio::test]
async fn glue_and_s3tables_catalogs_hold_the_session_metadata_cache() {
    let caches = CatalogCaches::default();
    let handle = caches.metadata_cache().unwrap();
    let before = Arc::strong_count(&handle);
    let mut glue_props = keyed("AKIAONE");
    glue_props.insert(
        GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
        "s3://w/".to_string(),
    );
    let glue = within(Box::pin(glue_catalog_counted(&glue_props, &caches)))
        .await
        .unwrap();
    assert_eq!(Arc::strong_count(&handle), before + 1);
    let mut s3tables_props = keyed("AKIAONE");
    s3tables_props.insert(
        S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
        "arn:aws:s3tables:us-east-1:1:bucket/b".to_string(),
    );
    let s3tables = within(Box::pin(s3tables_catalog_counted(&s3tables_props, &caches)))
        .await
        .unwrap();
    assert_eq!(Arc::strong_count(&handle), before + 2);
    for catalog in [&glue, &s3tables] {
        let debug = format!("{catalog:?}");
        assert!(!debug.contains(SECRET), "{debug}");
    }
    drop((glue, s3tables));
    assert_eq!(Arc::strong_count(&handle), before);
}

#[tokio::test]
async fn every_builder_holds_the_session_footer_cache() {
    let dir = TempDir::new().unwrap();
    let caches = CatalogCaches::default();
    let handle = caches.footer_cache().unwrap();
    let before = Arc::strong_count(&handle);
    let memory = memory_catalog_cached(dir.path().to_str().unwrap(), &caches)
        .await
        .unwrap();
    assert_eq!(Arc::strong_count(&handle), before + 1);
    let mut glue_props = keyed("AKIAONE");
    glue_props.insert(
        GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
        "s3://w/".to_string(),
    );
    let glue = within(Box::pin(glue_catalog_counted(&glue_props, &caches)))
        .await
        .unwrap();
    assert_eq!(Arc::strong_count(&handle), before + 2);
    let mut s3tables_props = keyed("AKIAONE");
    s3tables_props.insert(
        S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
        "arn:aws:s3tables:us-east-1:1:bucket/b".to_string(),
    );
    let s3tables = within(Box::pin(s3tables_catalog_counted(&s3tables_props, &caches)))
        .await
        .unwrap();
    assert_eq!(Arc::strong_count(&handle), before + 3);
    drop((memory, glue, s3tables));
    assert_eq!(Arc::strong_count(&handle), before);
}

#[tokio::test]
async fn disabled_glue_and_s3tables_catalogs_build_without_a_handle() {
    let caches = CatalogCaches::disabled();
    assert!(caches.metadata_cache().is_none());
    let mut glue_props = keyed("AKIAONE");
    glue_props.insert(
        GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
        "s3://w/".to_string(),
    );
    within(Box::pin(glue_catalog_counted(&glue_props, &caches)))
        .await
        .unwrap();
    let mut s3tables_props = keyed("AKIAONE");
    s3tables_props.insert(
        S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
        "arn:aws:s3tables:us-east-1:1:bucket/b".to_string(),
    );
    within(Box::pin(s3tables_catalog_counted(&s3tables_props, &caches)))
        .await
        .unwrap();
}

fn schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap()
}

fn sales() -> NamespaceIdent {
    NamespaceIdent::new("sales".to_string())
}

fn orders() -> TableIdent {
    TableIdent::from_strs(["sales", "orders"]).unwrap()
}

async fn with_orders(catalog: &Arc<dyn Catalog>, warehouse: &str) -> String {
    catalog
        .create_namespace(&sales(), HashMap::new())
        .await
        .unwrap();
    let creation = TableCreation::builder()
        .name("orders".to_string())
        .location(format!("{warehouse}/sales/orders"))
        .schema(schema())
        .properties(HashMap::new())
        .build();
    let table = catalog.create_table(&sales(), creation).await.unwrap();
    table.metadata_location().unwrap().to_string()
}

async fn adopting(catalog: &Arc<dyn Catalog>, location: &str) {
    catalog
        .create_namespace(&sales(), HashMap::new())
        .await
        .unwrap();
    catalog
        .register_table(&orders(), location.to_string())
        .await
        .unwrap();
}

async fn loaded(catalog: &Arc<dyn Catalog>) -> iceberg::spec::TableMetadataRef {
    catalog.load_table(&orders()).await.unwrap().metadata_ref()
}

#[tokio::test]
async fn two_credential_contexts_never_share_an_entry() {
    let dir = TempDir::new().unwrap();
    let warehouse = dir.path().to_str().unwrap();
    let caches = CatalogCaches::default();
    let one = memory_catalog_wired(warehouse, &caches, keyed("AKIAONE"))
        .await
        .unwrap();
    let location = with_orders(&one, warehouse).await;
    let two = memory_catalog_wired(warehouse, &caches, keyed("AKIATWO"))
        .await
        .unwrap();
    adopting(&two, &location).await;
    let metadata = caches.metadata_cache().unwrap();
    metadata.run_pending_tasks().await;
    assert_eq!(caches.metadata_len(), 2);
    assert!(!Arc::ptr_eq(&loaded(&one).await, &loaded(&two).await));

    let same = memory_catalog_wired(warehouse, &caches, keyed("AKIAONE"))
        .await
        .unwrap();
    adopting(&same, &location).await;
    metadata.run_pending_tasks().await;
    assert_eq!(caches.metadata_len(), 2);
    assert!(Arc::ptr_eq(&loaded(&one).await, &loaded(&same).await));
    assert!(!Arc::ptr_eq(&loaded(&two).await, &loaded(&same).await));
}

#[tokio::test]
async fn without_a_credential_selector_each_instance_is_its_own_scope() {
    let dir = TempDir::new().unwrap();
    let warehouse = dir.path().to_str().unwrap();
    let caches = CatalogCaches::default();
    let one = memory_catalog_cached(warehouse, &caches).await.unwrap();
    let location = with_orders(&one, warehouse).await;
    let two = memory_catalog_cached(warehouse, &caches).await.unwrap();
    adopting(&two, &location).await;
    caches.metadata_cache().unwrap().run_pending_tasks().await;
    assert_eq!(caches.metadata_len(), 2);
    assert!(!Arc::ptr_eq(&loaded(&one).await, &loaded(&two).await));
}

#[tokio::test]
async fn a_second_handles_commit_is_seen_and_never_leaks_into_a_sibling_pointer() {
    let dir = TempDir::new().unwrap();
    let warehouse = dir.path().to_str().unwrap();
    let caches = CatalogCaches::default();
    let one = memory_catalog_wired(warehouse, &caches, keyed("AKIAONE"))
        .await
        .unwrap();
    let location = with_orders(&one, warehouse).await;
    let first_handle = Arc::clone(&one);
    let second_handle = Arc::clone(&one);
    loaded(&first_handle).await;
    let table = second_handle.load_table(&orders()).await.unwrap();
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set("committed.by".to_string(), "second".to_string())
        .apply(tx)
        .unwrap();
    tx.commit(second_handle.as_ref()).await.unwrap();
    let seen = loaded(&first_handle).await;
    assert_eq!(
        seen.properties().get("committed.by").map(String::as_str),
        Some("second")
    );

    let sibling = memory_catalog_wired(warehouse, &caches, keyed("AKIAONE"))
        .await
        .unwrap();
    adopting(&sibling, &location).await;
    assert!(
        !loaded(&sibling)
            .await
            .properties()
            .contains_key("committed.by")
    );
    assert_eq!(
        loaded(&first_handle)
            .await
            .properties()
            .get("committed.by")
            .map(String::as_str),
        Some("second")
    );
}

async fn big_table(catalog: &Arc<dyn Catalog>, warehouse: &str, name: &str, fill: char) {
    let creation = TableCreation::builder()
        .name(name.to_string())
        .location(format!("{warehouse}/sales/{name}"))
        .schema(schema())
        .properties(HashMap::from([(
            "fill".to_string(),
            std::iter::repeat_n(fill, 40 * 1024).collect(),
        )]))
        .build();
    catalog.create_table(&sales(), creation).await.unwrap();
}

#[tokio::test]
async fn evictions_reach_the_stats_and_never_serve_a_sibling() {
    let dir = TempDir::new().unwrap();
    let warehouse = dir.path().to_str().unwrap();
    let caches = CatalogCaches::new(IcebergCacheSettings {
        metadata_cache_entries: 1,
        ..IcebergCacheSettings::default()
    });
    let catalog = memory_catalog_cached(warehouse, &caches).await.unwrap();
    catalog
        .create_namespace(&sales(), HashMap::new())
        .await
        .unwrap();
    let names = [("a", 'a'), ("b", 'b'), ("c", 'c')];
    for (name, fill) in names {
        big_table(&catalog, warehouse, name, fill).await;
    }
    let metadata = caches.metadata_cache().unwrap();
    for _ in 0..2 {
        for (name, fill) in names {
            let table = catalog
                .load_table(&TableIdent::from_strs(["sales", name]).unwrap())
                .await
                .unwrap();
            let value = table.metadata().properties().get("fill").unwrap();
            assert!(value.chars().all(|c| c == fill), "{name}");
            metadata.run_pending_tasks().await;
        }
    }
    let stats = caches.metadata_stats().unwrap();
    assert!(stats.evictions > 0, "{stats:?}");
    assert!(metadata.weighted_size() <= 64 * 1024, "{stats:?}");
}

async fn scan_counts(caches: &CatalogCaches) -> [IcebergIoStats; 2] {
    let dir = TempDir::new().unwrap();
    let warehouse = dir.path().to_str().unwrap();
    let writer = memory_catalog_cached(warehouse, &CatalogCaches::disabled())
        .await
        .unwrap();
    let location = with_orders(&writer, warehouse).await;
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "w", Arc::clone(&writer))
        .await
        .unwrap();
    ctx.sql("INSERT INTO w.sales.orders VALUES (1), (2), (3)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let written = writer.load_table(&orders()).await.unwrap();
    let current = written.metadata_location().unwrap().to_string();
    assert_ne!(current, location);

    let reader = memory_catalog_cached(warehouse, caches).await.unwrap();
    adopting(&reader, &current).await;
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "r", Arc::clone(&reader))
        .await
        .unwrap();
    let mut rounds = Vec::new();
    for _ in 0..2 {
        caches.reset_io_stats();
        let rows: usize = ctx
            .sql("SELECT id FROM r.sales.orders")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 3);
        rounds.push(caches.io_stats());
    }
    [rounds[0], rounds[1]]
}

#[tokio::test]
async fn the_counter_sees_every_request_with_the_caches_on() {
    let [cold_off, warm_off] = scan_counts(&CatalogCaches::disabled()).await;
    let [cold_on, warm_on] = scan_counts(&CatalogCaches::default()).await;
    for class in [
        IcebergFileClass::ManifestList,
        IcebergFileClass::Manifest,
        IcebergFileClass::DataFile,
    ] {
        assert!(
            cold_on.by_class(class).requests > 0,
            "{class:?} {cold_on:?}"
        );
        assert_eq!(
            cold_on.by_class(class).requests,
            cold_off.by_class(class).requests,
            "{class:?}"
        );
    }
    assert_eq!(
        warm_on.get(IcebergIoOp::RangedRead, IcebergFileClass::DataFile),
        warm_off.get(IcebergIoOp::RangedRead, IcebergFileClass::DataFile)
    );
    assert!(
        warm_off
            .get(IcebergIoOp::FooterRead, IcebergFileClass::DataFile)
            .requests
            > 0
    );
    assert_eq!(
        warm_on
            .get(IcebergIoOp::FooterRead, IcebergFileClass::DataFile)
            .requests,
        0
    );
    assert!(
        warm_on.by_class(IcebergFileClass::Manifest).requests
            < warm_off.by_class(IcebergFileClass::Manifest).requests,
        "on {warm_on:?} off {warm_off:?}"
    );
}

#[tokio::test]
async fn the_door_trim_settles_before_it_reads_the_high_water_mark() {
    let dir = TempDir::new().unwrap();
    let warehouse = dir.path().to_str().unwrap();
    let caches = CatalogCaches::new(IcebergCacheSettings {
        metadata_cache_entries: 1,
        ..IcebergCacheSettings::default()
    });
    let catalog = memory_catalog_cached(warehouse, &caches).await.unwrap();
    catalog
        .create_namespace(&sales(), HashMap::new())
        .await
        .unwrap();
    let names = ["a", "b", "c", "d"];
    for name in names {
        let creation = TableCreation::builder()
            .name(name.to_string())
            .location(format!("{warehouse}/sales/{name}"))
            .schema(schema())
            .properties(HashMap::new())
            .build();
        catalog.create_table(&sales(), creation).await.unwrap();
    }
    for name in names {
        catalog
            .load_table(&TableIdent::from_strs(["sales", name]).unwrap())
            .await
            .unwrap();
    }

    caches.trim().await;

    assert_eq!(
        caches.settled_metadata_len().await,
        0,
        "four small tables over a one-entry bound: the door's trim must see the settled count and clear"
    );
    let stats = caches.metadata_stats().unwrap();
    assert_eq!(stats.evictions, 0, "{stats:?}");
}
