use super::super::*;

use bytes::Bytes;
use datafusion::prelude::SessionContext;
use iceberg::io::{FileIO, FileIOBuilder, LocalFsStorageFactory, StorageFactory};
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_storage_opendal::OpenDalStorageFactory;
use tempfile::TempDir;

fn counting_file_io(counters: &Arc<IcebergIoCounters>) -> FileIO {
    FileIOBuilder::new(Arc::new(CountingStorageFactory::new(
        Arc::new(LocalFsStorageFactory),
        Arc::clone(counters),
    )))
    .build()
}

fn count(stats: &IcebergIoStats, op: IcebergIoOp, class: IcebergFileClass) -> IcebergIoCount {
    stats.get(op, class)
}

fn one(requests: u64, bytes: u64) -> IcebergIoCount {
    IcebergIoCount { requests, bytes }
}

#[tokio::test]
async fn every_op_kind_counts_once_with_its_bytes() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_str().unwrap();
    let data = format!("{root}/t/data/00000-0-a.parquet");
    let counters = Arc::new(IcebergIoCounters::new());
    let file_io = counting_file_io(&counters);
    let payload = Bytes::from(vec![7_u8; 1000]);

    file_io
        .new_output(&data)
        .unwrap()
        .write(payload.clone())
        .await
        .unwrap();
    let stats = counters.snapshot();
    assert_eq!(
        count(&stats, IcebergIoOp::Write, IcebergFileClass::DataFile),
        one(1, 1000)
    );
    assert_eq!(stats.total(), one(1, 1000));

    counters.reset();
    assert_eq!(counters.snapshot().total(), one(0, 0));

    assert!(file_io.exists(&data).await.unwrap());
    let input = file_io.new_input(&data).unwrap();
    assert_eq!(input.metadata().await.unwrap().size, 1000);
    assert_eq!(input.read().await.unwrap().len(), 1000);
    let listed = file_io.list(format!("{root}/t/data/")).await.unwrap();
    assert_eq!(listed.len(), 1);
    file_io.delete(&data).await.unwrap();
    file_io.delete_prefix(format!("{root}/t")).await.unwrap();

    let stats = counters.snapshot();
    assert_eq!(
        count(&stats, IcebergIoOp::Exists, IcebergFileClass::DataFile),
        one(1, 0)
    );
    assert_eq!(
        count(&stats, IcebergIoOp::Metadata, IcebergFileClass::DataFile),
        one(1, 0)
    );
    assert_eq!(
        count(&stats, IcebergIoOp::Read, IcebergFileClass::DataFile),
        one(1, 1000)
    );
    assert_eq!(stats.by_op(IcebergIoOp::List), one(1, 0));
    assert_eq!(
        count(&stats, IcebergIoOp::Delete, IcebergFileClass::DataFile),
        one(1, 0)
    );
    assert_eq!(stats.by_op(IcebergIoOp::Delete), one(2, 0));
    assert_eq!(stats.by_op(IcebergIoOp::Write), one(0, 0));
    assert_eq!(stats.by_op(IcebergIoOp::RangedRead), one(0, 0));
    assert_eq!(stats.total(), one(6, 1000));
}

#[tokio::test]
async fn ranged_reads_count_the_range_length() {
    let dir = TempDir::new().unwrap();
    let path = format!("{}/t/data/00000-0-a.parquet", dir.path().to_str().unwrap());
    let counters = Arc::new(IcebergIoCounters::new());
    let file_io = counting_file_io(&counters);
    let payload: Vec<u8> = (0..=255_u8).cycle().take(4096).collect();
    file_io
        .new_output(&path)
        .unwrap()
        .write(Bytes::from(payload))
        .await
        .unwrap();
    counters.reset();

    let reader = file_io.new_input(&path).unwrap().reader().await.unwrap();
    assert_eq!(counters.snapshot().total(), one(0, 0));
    assert_eq!(reader.read(10..110).await.unwrap().len(), 100);
    assert_eq!(reader.read(4000..4096).await.unwrap().len(), 96);

    let stats = counters.snapshot();
    assert_eq!(
        count(&stats, IcebergIoOp::RangedRead, IcebergFileClass::DataFile),
        one(2, 196)
    );
    assert_eq!(stats.total(), one(2, 196));
}

#[tokio::test]
async fn input_and_output_files_from_the_wrapper_still_count() {
    let dir = TempDir::new().unwrap();
    let path = format!(
        "{}/t/metadata/00001-abc.metadata.json",
        dir.path().to_str().unwrap()
    );
    let counters = Arc::new(IcebergIoCounters::new());
    let factory =
        CountingStorageFactory::new(Arc::new(LocalFsStorageFactory), Arc::clone(&counters));
    let storage = factory.build(&iceberg::io::StorageConfig::new()).unwrap();

    let output = storage.new_output(&path).unwrap();
    let mut writer = output.writer().await.unwrap();
    writer.write(Bytes::from_static(b"{\"a\":")).await.unwrap();
    writer.write(Bytes::from_static(b"1}")).await.unwrap();
    writer.close().await.unwrap();
    assert!(output.exists().await.unwrap());
    let input = output.to_input_file();
    assert_eq!(input.read().await.unwrap().len(), 7);
    let reader = storage.new_input(&path).unwrap().reader().await.unwrap();
    assert_eq!(reader.read(0..3).await.unwrap().len(), 3);
    storage.new_output(&path).unwrap().delete().await.unwrap();

    let stats = counters.snapshot();
    let class = IcebergFileClass::TableMetadata;
    assert_eq!(count(&stats, IcebergIoOp::Write, class), one(1, 7));
    assert_eq!(count(&stats, IcebergIoOp::Exists, class), one(1, 0));
    assert_eq!(count(&stats, IcebergIoOp::Read, class), one(1, 7));
    assert_eq!(count(&stats, IcebergIoOp::RangedRead, class), one(1, 3));
    assert_eq!(count(&stats, IcebergIoOp::Delete, class), one(1, 0));
    assert_eq!(stats.by_class(class), stats.total());
}

#[test]
fn glue_and_s3tables_factories_are_the_fork_defaults() {
    let OpenDalStorageFactory::S3 {
        configured_scheme,
        customized_credential_load,
    } = glue_default_storage_factory()
    else {
        panic!("the Glue default must be the OpenDAL S3 factory");
    };
    assert_eq!(configured_scheme, "s3a");
    assert!(customized_credential_load.is_none());
    let OpenDalStorageFactory::S3 {
        configured_scheme,
        customized_credential_load,
    } = s3tables_default_storage_factory()
    else {
        panic!("the S3 Tables default must be the OpenDAL S3 factory");
    };
    assert_eq!(configured_scheme, "s3");
    assert!(customized_credential_load.is_none());
    assert_eq!(GLUE_DEFAULT_CONFIGURED_SCHEME, "s3a");
    assert_eq!(S3TABLES_DEFAULT_CONFIGURED_SCHEME, "s3");
}

#[test]
fn the_counting_factory_serializes_over_its_inner_factory() {
    let factory = CountingStorageFactory::new(
        Arc::new(glue_default_storage_factory()),
        Arc::new(IcebergIoCounters::new()),
    );
    let as_dyn: &dyn StorageFactory = &factory;
    let json = serde_json::to_value(as_dyn).unwrap();
    assert_eq!(json["type"], "CountingStorageFactory");
    assert_eq!(json["inner"]["type"], "OpenDalStorageFactory");
    assert_eq!(json["inner"]["S3"]["configured_scheme"], "s3a");
}

#[tokio::test]
async fn glue_and_s3tables_counted_builders_keep_their_prop_errors() {
    let counters = Arc::new(IcebergIoCounters::new());
    let glue = glue_catalog_counted(&HashMap::<String, String>::new(), Arc::clone(&counters))
        .await
        .unwrap_err()
        .to_string();
    assert!(glue.contains(GLUE_CATALOG_PROP_WAREHOUSE), "got: {glue}");
    let s3tables = s3tables_catalog_counted(&HashMap::<String, String>::new(), counters)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        s3tables.contains(S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN),
        "got: {s3tables}"
    );
}

#[test]
fn the_classifier_reads_literal_iceberg_paths() {
    let cases = [
        (
            "s3://b/t/metadata/00003-1b2c.metadata.json",
            IcebergFileClass::TableMetadata,
        ),
        (
            "/wh/t/metadata/v2.metadata.json",
            IcebergFileClass::TableMetadata,
        ),
        (
            "/wh/t/metadata/v2.gz.metadata.json",
            IcebergFileClass::TableMetadata,
        ),
        (
            "/wh/t/metadata/00001-x.metadata.json.gz",
            IcebergFileClass::TableMetadata,
        ),
        (
            "/wh/t/metadata/snap-123-1-abc.avro",
            IcebergFileClass::ManifestList,
        ),
        ("/wh/t/metadata/abc-m0.avro", IcebergFileClass::Manifest),
        ("/wh/t/data/00000-0-abc.parquet", IcebergFileClass::DataFile),
        ("/wh/t/data/p=1/00000-0-abc.orc", IcebergFileClass::DataFile),
        (
            "/wh/t/data/00000-5-abc-00001-deletes.parquet",
            IcebergFileClass::DeleteFile,
        ),
        (
            "/wh/t/data/00000-5-abc-00001-deletes.puffin",
            IcebergFileClass::DeleteFile,
        ),
        ("/wh/t/metadata/version-hint.text", IcebergFileClass::Other),
        ("/wh/t/metadata/stats-1.puffin", IcebergFileClass::Other),
        ("/wh/t/", IcebergFileClass::Other),
    ];
    for (path, class) in cases {
        assert_eq!(classify_iceberg_path(path), class, "path {path}");
    }
}

fn sample_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap()
}

async fn table_with_rows(
    warehouse: &str,
    caches: &CatalogCaches,
) -> (SessionContext, Arc<dyn Catalog>) {
    let catalog = memory_catalog_cached(warehouse, caches).await.unwrap();
    let sales = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&sales, HashMap::new())
        .await
        .unwrap();
    let creation = TableCreation::builder()
        .name("orders".to_string())
        .location(format!("{warehouse}/sales/orders"))
        .schema(sample_schema())
        .properties(HashMap::new())
        .build();
    catalog.create_table(&sales, creation).await.unwrap();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", Arc::clone(&catalog))
        .await
        .unwrap();
    ctx.sql("INSERT INTO ice.sales.orders VALUES (1, 'alan'), (2, 'turing'), (3, 'grace')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    (ctx, catalog)
}

fn files_under(root: &std::path::Path, found: &mut Vec<String>) {
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files_under(&path, found);
        } else {
            found.push(path.to_str().unwrap().to_string());
        }
    }
}

#[tokio::test]
async fn the_classifier_reads_the_paths_repark_writes() {
    let dir = TempDir::new().unwrap();
    let warehouse = dir.path().to_str().unwrap();
    let caches = CatalogCaches::default();
    let (_ctx, catalog) = table_with_rows(warehouse, &caches).await;
    let mut found = Vec::new();
    files_under(dir.path(), &mut found);
    let mut seen = HashMap::new();
    for path in &found {
        let class = classify_iceberg_path(path);
        assert_ne!(class, IcebergFileClass::Other, "unclassified {path}");
        *seen.entry(class).or_insert(0_usize) += 1;
    }
    for class in [
        IcebergFileClass::TableMetadata,
        IcebergFileClass::ManifestList,
        IcebergFileClass::Manifest,
        IcebergFileClass::DataFile,
    ] {
        assert!(seen.contains_key(&class), "no {class:?} among {found:?}");
    }

    let table = catalog
        .load_table(&TableIdent::from_strs(["sales", "orders"]).unwrap())
        .await
        .unwrap();
    let locations = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    for (prefix, format) in [
        ("pos-del", DataFileFormat::Parquet),
        ("dv", DataFileFormat::Puffin),
    ] {
        let names = DefaultFileNameGenerator::new(
            prefix.to_string(),
            Some("0199-uuid".to_string()),
            format,
        );
        let path = locations.generate_location(None, &names.generate_file_name());
        assert_eq!(
            classify_iceberg_path(&path),
            IcebergFileClass::DeleteFile,
            "path {path}"
        );
    }
}

#[tokio::test]
async fn a_scan_reads_data_file_ranges_through_the_counter() {
    let dir = TempDir::new().unwrap();
    let caches = CatalogCaches::default();
    let (ctx, _catalog) = table_with_rows(dir.path().to_str().unwrap(), &caches).await;
    caches.reset_io_stats();
    let batches = ctx
        .sql("SELECT id, name FROM ice.sales.orders")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );
    let stats = caches.io_stats();
    let ranged = count(&stats, IcebergIoOp::RangedRead, IcebergFileClass::DataFile);
    assert!(ranged.requests > 0, "stats {stats:?}");
    assert!(ranged.bytes > 0, "stats {stats:?}");
}

async fn second_load_metadata_reads(caches: &CatalogCaches) -> u64 {
    let dir = TempDir::new().unwrap();
    let (_ctx, catalog) = table_with_rows(dir.path().to_str().unwrap(), caches).await;
    let ident = TableIdent::from_strs(["sales", "orders"]).unwrap();
    catalog.load_table(&ident).await.unwrap();
    caches.reset_io_stats();
    catalog.load_table(&ident).await.unwrap();
    caches
        .io_stats()
        .by_class(IcebergFileClass::TableMetadata)
        .requests
}

#[tokio::test]
async fn the_metadata_cache_cuts_metadata_json_reads_on_a_second_load() {
    let uncached = second_load_metadata_reads(&CatalogCaches::disabled()).await;
    let cached = second_load_metadata_reads(&CatalogCaches::default()).await;
    assert!(uncached >= 1, "uncached second load read {uncached}");
    assert!(cached < uncached, "cached {cached} vs uncached {uncached}");
}

#[test]
fn caches_share_one_counter_set_across_clones() {
    let caches = CatalogCaches::default();
    let clone = caches.clone();
    caches
        .io_counters()
        .record(IcebergIoOp::Read, IcebergFileClass::Manifest, 1, 42);
    assert_eq!(clone.io_stats().by_op(IcebergIoOp::Read), one(1, 42));
    clone.reset_io_stats();
    assert_eq!(caches.io_stats().total(), one(0, 0));
}
