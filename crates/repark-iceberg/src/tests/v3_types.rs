//! Model: Grok 4.6
//! Fork pin `33be9a0` read/write measurement for V3-6 types.
//! pins: v3-6-v3-types/C-001, C-005

use std::collections::HashMap;
use std::sync::Arc;

use crate::append;
use datafusion::arrow::array::{
    Array, Int32Array, NullArray, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use futures::TryStreamExt;
use iceberg::arrow::{UTC_TIME_ZONE, schema_to_arrow_schema};
use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::{FileWriterBuilder, ParquetWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("build memory catalog"),
    );
    catalog
        .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
        .await
        .expect("create namespace");
    catalog
}

async fn create_v3(catalog: &Arc<dyn Catalog>, name: &str, schema: Schema) -> TableIdent {
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string());
    catalog
        .create_table(
            ident.namespace(),
            TableCreation::builder()
                .name(name.to_string())
                .schema(schema)
                .format_version(FormatVersion::V3)
                .build(),
        )
        .await
        .expect("create v3");
    ident
}

fn timestamp_ns_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "ts", Type::Primitive(PrimitiveType::TimestampNs)).into(),
        ])
        .build()
        .expect("timestamp_ns schema")
}

fn timestamptz_ns_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "ts", Type::Primitive(PrimitiveType::TimestamptzNs)).into(),
        ])
        .build()
        .expect("timestamptz_ns schema")
}

fn unknown_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "u", Type::Primitive(PrimitiveType::Unknown)).into(),
        ])
        .build()
        .expect("unknown schema")
}

fn variant_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Variant).into(),
        ])
        .build()
        .expect("variant schema")
}

fn write_default_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String))
                .with_write_default(iceberg::spec::Literal::string("anon"))
                .into(),
        ])
        .build()
        .expect("write_default schema")
}

#[tokio::test]
async fn fork_timestamp_ns_parquet_write_and_scan_round_trip() {
    let warehouse = TempDir::new().unwrap();
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_v3(&catalog, "tsns", timestamp_ns_schema()).await;
    let nanos: i64 = 1_704_164_645_123_456_789;
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(TimestampNanosecondArray::from(vec![Some(nanos)])),
        ],
    )
    .expect("batch");
    append(&catalog, &ident, vec![batch])
        .await
        .expect("timestamp_ns append");
    let table = catalog.load_table(&ident).await.expect("load");
    let batches: Vec<RecordBatch> = table
        .scan()
        .select(["id", "ts"])
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect");
    assert!(!batches.is_empty(), "timestamp_ns scan must return a batch");
    assert_eq!(
        batches[0].schema().field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    let ts = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("ns array");
    assert_eq!(ts.value(0), nanos);
}

#[tokio::test]
async fn fork_timestamptz_ns_parquet_write_and_scan_round_trip() {
    let warehouse = TempDir::new().unwrap();
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_v3(&catalog, "tstzns", timestamptz_ns_schema()).await;
    let nanos: i64 = 1_704_164_645_123_456_789;
    let tz = Some(Arc::from(UTC_TIME_ZONE));
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
                true,
            ),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(nanos)]).with_timezone(UTC_TIME_ZONE),
            ),
        ],
    )
    .expect("batch");
    append(&catalog, &ident, vec![batch])
        .await
        .expect("timestamptz_ns append");
    let table = catalog.load_table(&ident).await.expect("load");
    let batches: Vec<RecordBatch> = table
        .scan()
        .select(["id", "ts"])
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect");
    assert_eq!(
        batches[0].schema().field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, tz)
    );
    let ts = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("ns array");
    assert_eq!(ts.value(0), nanos);
}

#[tokio::test]
async fn fork_unknown_write_commits_then_scan_refuses_naming_null() {
    let mapped = schema_to_arrow_schema(&unknown_schema()).expect("unknown maps");
    assert_eq!(mapped.field(1).data_type(), &DataType::Null);
    let warehouse = TempDir::new().unwrap();
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_v3(&catalog, "unk", unknown_schema()).await;
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("u", DataType::Null, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(NullArray::new(1)),
        ],
    )
    .expect("batch");
    append(&catalog, &ident, vec![batch]).await.expect(
        "measured R91 divergence: the parquet write COMMITS an unreadable Null column \
         instead of refusing loud",
    );
    let table = catalog.load_table(&ident).await.expect("load");
    let scan =
        table.scan().select(["id", "u"]).build().expect(
            "scan build succeeds today; a build-time refusal is a behavior flip to re-record",
        );
    let mut stream = scan
        .to_arrow()
        .await
        .expect("to_arrow succeeds today; a to_arrow refusal is a behavior flip to re-record");
    match futures::StreamExt::next(&mut stream).await {
        Some(Err(err)) => {
            let text = err.to_string();
            assert!(
                text.contains("Cannot visit Arrow data type: Null"),
                "the measured scan refusal is the DataInvalid Null-visit error: {text}"
            );
        }
        Some(Ok(batch)) => panic!(
            "the fork learned to read Null columns — re-record this pin as a read: {batch:?}"
        ),
        None => panic!("the stream ended cleanly — re-record this pin: the refusal moved"),
    }
}

#[tokio::test]
async fn fork_variant_arrow_maps_and_parquet_write_refuses() {
    let mapped = schema_to_arrow_schema(&variant_schema()).expect("variant maps");
    assert!(
        mapped.field(1).data_type().to_string().contains("Struct"),
        "binary variant maps to Arrow struct of two binaries"
    );
    let warehouse = TempDir::new().unwrap();
    let path = warehouse
        .path()
        .join("variant.parquet")
        .to_string_lossy()
        .to_string();
    let output = FileIO::new_with_fs()
        .new_output(&path)
        .expect("output file");
    let Err(error) = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        Arc::new(variant_schema()),
    )
    .build(output)
    .await
    else {
        panic!("variant parquet write must refuse at builder")
    };
    assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
    assert!(
        error.message().contains("variant"),
        "variant write refusal must name the type: {}",
        error.message()
    );
    assert!(!std::path::Path::new(&path).exists());
}

/// pins: v3-6-v3-types/C-002
#[tokio::test]
async fn fork_variant_scan_refuses_naming_the_type() {
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
    use iceberg::transaction::Transaction;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::collections::HashMap;

    let warehouse = TempDir::new().unwrap();
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_v3(&catalog, "variantscan", variant_schema()).await;
    let table = catalog.load_table(&ident).await.expect("load");

    let table_location =
        std::path::Path::new(table.metadata_location().expect("metadata location"))
            .ancestors()
            .nth(2)
            .expect("table location")
            .to_path_buf();
    let data_dir = table_location.join("data");
    std::fs::create_dir_all(&data_dir).expect("fixture data dir");
    let file_path = data_dir.join("variant-scan-fixture.parquet");
    let arrow_schema = ArrowSchema::new(vec![Field::new("id", DataType::Int32, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .expect("fixture batch");
    let file = std::fs::File::create(&file_path).expect("fixture file");
    let mut writer = ArrowWriter::try_new(
        file,
        Arc::new(arrow_schema),
        Some(WriterProperties::builder().build()),
    )
    .expect("fixture writer");
    writer.write(&batch).expect("fixture write");
    writer.close().expect("fixture close");
    let file_path = file_path.to_string_lossy().to_string();

    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_format(DataFileFormat::Parquet)
        .file_path(file_path.clone())
        .file_size_in_bytes(512)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .column_sizes(HashMap::from([(1, 128)]))
        .value_counts(HashMap::from([(1, 1)]))
        .null_value_counts(HashMap::from([(1, 0)]))
        .build()
        .expect("fixture data file");
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(vec![data_file]);
    let tx = action.apply(tx).expect("apply");
    tx.commit(catalog.as_ref()).await.expect("commit");

    let table = catalog.load_table(&ident).await.expect("reload");
    let scan = table.scan().select(["id", "v"]).build();
    match scan {
        Ok(scan) => match scan.to_arrow().await {
            Ok(mut stream) => {
                let first = futures::StreamExt::next(&mut stream).await;
                match first {
                    Some(Err(error)) => {
                        let text = error.to_string().to_ascii_lowercase();
                        assert!(
                            text.contains("variant"),
                            "variant scan refusal must name the type: {text}"
                        );
                    }
                    Some(Ok(batch)) => {
                        panic!("variant scan must refuse, returned a batch: {batch:?}")
                    }
                    None => panic!("variant scan must refuse, stream ended cleanly"),
                }
            }
            Err(error) => {
                let text = error.to_string().to_ascii_lowercase();
                assert!(
                    text.contains("variant"),
                    "variant to_arrow refusal must name the type: {text}"
                );
            }
        },
        Err(error) => {
            let text = error.to_string().to_ascii_lowercase();
            assert!(
                text.contains("variant"),
                "variant scan build refusal must name the type: {text}"
            );
        }
    }
}

#[tokio::test]
async fn fork_write_default_fills_missing_column_on_append() {
    let warehouse = TempDir::new().unwrap();
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_v3(&catalog, "defaults", write_default_schema()).await;
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .expect("id-only batch");
    append(&catalog, &ident, vec![batch])
        .await
        .expect("an omitted column with a write_default must fill, not refuse");
    let table = catalog.load_table(&ident).await.expect("load");
    let field = table
        .metadata()
        .current_schema()
        .field_by_name("name")
        .expect("name field");
    assert_eq!(
        field.write_default,
        Some(iceberg::spec::Literal::string("anon"))
    );
    let batches: Vec<RecordBatch> = table
        .scan()
        .select(["id", "name"])
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect");
    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string array");
    for index in 0..names.len() {
        assert_eq!(
            names.value(index),
            "anon",
            "the write_default must fill the omitted column"
        );
    }
}

#[tokio::test]
async fn engine_append_supplied_column_kept_over_write_default() {
    let warehouse = TempDir::new().unwrap();
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_v3(&catalog, "supplied", write_default_schema()).await;
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("bob")])),
        ],
    )
    .expect("full batch");
    append(&catalog, &ident, vec![batch])
        .await
        .expect("a supplied column must survive unchanged");
    let table = catalog.load_table(&ident).await.expect("load");
    let batches: Vec<RecordBatch> = table
        .scan()
        .select(["id", "name"])
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect");
    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string array");
    assert_eq!(
        names.value(0),
        "bob",
        "a supplied value must not be replaced by the write_default"
    );
}

#[tokio::test]
async fn fork_initial_default_reads_into_files_missing_the_column() {
    let warehouse = TempDir::new().unwrap();
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_v3(
        &catalog,
        "initialdef",
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("id schema"),
    )
    .await;
    let pre = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .expect("pre-column batch");
    append(&catalog, &ident, vec![pre])
        .await
        .expect("pre-column append");
    let table = catalog.load_table(&ident).await.expect("load");
    let tx = Transaction::new(&table);
    let action = tx.update_schema().add_column_with_default(
        "tag",
        Type::Primitive(PrimitiveType::String),
        iceberg::spec::Literal::string("x"),
    );
    let tx = action.apply(tx).expect("apply");
    tx.commit(catalog.as_ref()).await.expect("commit");

    let post = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![2]))],
    )
    .expect("post-column batch");
    append(&catalog, &ident, vec![post])
        .await
        .expect("post-column append fills from write_default");
    let table = catalog.load_table(&ident).await.expect("reload");
    let batches: Vec<RecordBatch> = table
        .scan()
        .select(["id", "tag"])
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect");
    let mut tags_by_id = std::collections::HashMap::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id array");
        let tags = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("tag array");
        for index in 0..ids.len() {
            tags_by_id.insert(ids.value(index), tags.value(index).to_string());
        }
    }
    assert_eq!(
        tags_by_id.get(&1).map(String::as_str),
        Some("x"),
        "the pre-column file must read back through initial_default"
    );
    assert_eq!(
        tags_by_id.get(&2).map(String::as_str),
        Some("x"),
        "the post-column append must read back through write_default fill"
    );
}

#[tokio::test]
async fn fork_add_column_with_default_sets_initial_and_write_default() {
    let warehouse = TempDir::new().unwrap();
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_v3(
        &catalog,
        "adddef",
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("id schema"),
    )
    .await;
    let table = catalog.load_table(&ident).await.expect("load");
    let tx = Transaction::new(&table);
    let action = tx.update_schema().add_column_with_default(
        "tag",
        Type::Primitive(PrimitiveType::String),
        iceberg::spec::Literal::string("x"),
    );
    let tx = action.apply(tx).expect("apply");
    tx.commit(catalog.as_ref()).await.expect("commit");
    let table = catalog.load_table(&ident).await.expect("reload");
    let field = table
        .metadata()
        .current_schema()
        .field_by_name("tag")
        .expect("tag field");
    assert_eq!(
        field.initial_default,
        Some(iceberg::spec::Literal::string("x"))
    );
    assert_eq!(
        field.write_default,
        Some(iceberg::spec::Literal::string("x"))
    );
}
