//! Model: Grok 4.6
//! Fork pin `33be9a0` read/write measurement for V3-6 types.
//! pins: v3-6-v3-types/C-001

use std::collections::HashMap;
use std::sync::Arc;

use crate::append;
use datafusion::arrow::array::{
    Array, Int32Array, NullArray, RecordBatch, TimestampNanosecondArray,
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
async fn fork_unknown_schema_maps_to_arrow_null_and_data_io_refuses() {
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
    append(&catalog, &ident, vec![batch])
        .await
        .expect("unknown parquet write currently commits");
    let table = catalog.load_table(&ident).await.expect("load");
    let scan = table.scan().select(["id", "u"]).build();
    match scan {
        Ok(scan) => {
            let result = scan.to_arrow().await;
            match result {
                Ok(stream) => {
                    let collected: Result<Vec<RecordBatch>, _> = stream.try_collect().await;
                    match collected {
                        Ok(batches) => {
                            assert!(!batches.is_empty(), "unknown scan returned batches");
                            assert_eq!(batches[0].schema().field(1).data_type(), &DataType::Null);
                        }
                        Err(err) => {
                            let text = err.to_string().to_ascii_lowercase();
                            assert!(
                                text.contains("unknown")
                                    || text.contains("not supported")
                                    || text.contains("null"),
                                "unknown scan error must name the type or Null: {text}"
                            );
                        }
                    }
                }
                Err(err) => {
                    let text = err.to_string().to_ascii_lowercase();
                    assert!(
                        text.contains("unknown") || text.contains("not supported"),
                        "unknown to_arrow error must name the type: {text}"
                    );
                }
            }
        }
        Err(err) => {
            let text = err.to_string().to_ascii_lowercase();
            assert!(
                text.contains("unknown") || text.contains("not supported"),
                "unknown scan build error must name the type: {text}"
            );
        }
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
    let error = match ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        Arc::new(variant_schema()),
    )
    .build(output)
    .await
    {
        Ok(_) => panic!("variant parquet write must refuse at builder"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
    assert!(
        error.message().contains("variant"),
        "variant write refusal must name the type: {}",
        error.message()
    );
    assert!(!std::path::Path::new(&path).exists());
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
    let err = append(&catalog, &ident, vec![batch])
        .await
        .expect_err("engine append refuses a missing column before write_default fill");
    let text = err.to_string();
    assert!(
        text.contains("missing column `name`"),
        "engine append must name the missing column: {text}"
    );
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
