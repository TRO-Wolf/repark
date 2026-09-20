use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use datafusion::arrow::array::{ArrayData, ListArray};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::DataFusionError;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    RESERVED_FIELD_ID_ROW_ID,
};
use iceberg::spec::{
    DataContentType, DataFile, Datum, FormatVersion, ListType, ManifestContentType, MetricsConfig,
    MetricsMode, NestedField, NullOrder, PrimitiveType, Schema, SortDirection, SortField,
    SortOrder, StructType, Transform, Type, UnboundPartitionSpec,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use tempfile::TempDir;

use super::super::row_lineage::write_partitioned_lineage_files;
use super::super::write_data_files;
use crate::write::append::{append, write_partitioned_data_files_with_concurrency};
use crate::write::concurrency::WriteConcurrency;
use crate::write::position_delete::write_position_deletes;
use crate::write::write_options::{
    WriterStagingOverrides, stage_unpartitioned_stream_with_overrides,
};

fn recorded_fixture() -> Value {
    serde_json::from_str(include_str!("writer_metrics_truth.json")).expect("parse fixture")
}

fn metrics_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "s", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "d", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::optional(
                4,
                "st",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(6, "a", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(7, "b", Type::Primitive(PrimitiveType::Int)).into(),
                ])),
            )
            .into(),
            NestedField::optional(
                5,
                "xs",
                Type::List(ListType::new(
                    NestedField::optional(8, "element", Type::Primitive(PrimitiveType::Int)).into(),
                )),
            )
            .into(),
        ])
        .build()
        .expect("metrics schema builds")
}

fn cell_properties(cell: &str) -> HashMap<String, String> {
    let default_key = "write.metadata.metrics.default".to_string();
    let column_prefix = "write.metadata.metrics.column.";
    let max_inferred_key = "write.metadata.metrics.max-inferred-column-defaults".to_string();
    match cell {
        "default" => HashMap::new(),
        "none" | "sorted_none" => HashMap::from([(default_key, "none".to_string())]),
        "counts" | "sorted_counts" => HashMap::from([(default_key, "counts".to_string())]),
        "truncate4" => HashMap::from([(default_key, "truncate(4)".to_string())]),
        "full" => HashMap::from([(default_key, "full".to_string())]),
        "col_none" => HashMap::from([(format!("{column_prefix}s"), "none".to_string())]),
        "col_nested" => HashMap::from([
            (default_key, "none".to_string()),
            (format!("{column_prefix}st.a"), "full".to_string()),
        ]),
        "max_inferred_2" => HashMap::from([(max_inferred_key, "2".to_string())]),
        "max_inferred_2_default_set" => HashMap::from([
            (max_inferred_key, "2".to_string()),
            (default_key, "counts".to_string()),
        ]),
        "bad_mode" => HashMap::from([(default_key, "bogus".to_string())]),
        other => panic!("unknown metrics cell {other}"),
    }
}

fn cell_sort_field(cell: &str) -> Option<i32> {
    match cell {
        "sorted_none" => Some(2),
        "sorted_counts" => Some(3),
        _ => None,
    }
}

fn identity_order(source_id: i32) -> SortOrder {
    SortOrder::builder()
        .with_sort_field(
            SortField::builder()
                .source_id(source_id)
                .transform(Transform::Identity)
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .build(),
        )
        .build_unbound()
        .expect("identity sort order builds")
}

async fn metrics_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
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
        .create_namespace(&NamespaceIdent::new("metrics".to_string()), HashMap::new())
        .await
        .expect("create namespace");
    catalog
}

async fn create_metrics_table(catalog: &Arc<dyn Catalog>, name: &str, cell: &str) -> TableIdent {
    let staged = TableCreation::builder()
        .name(name.to_string())
        .schema(metrics_schema())
        .properties(cell_properties(cell))
        .format_version(FormatVersion::V2);
    let creation = match cell_sort_field(cell) {
        Some(source_id) => staged.sort_order(identity_order(source_id)).build(),
        None => staged.build(),
    };
    catalog
        .create_table(&NamespaceIdent::new("metrics".to_string()), creation)
        .await
        .expect("create metrics table");
    TableIdent::new(NamespaceIdent::new("metrics".to_string()), name.to_string())
}

fn metrics_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("d", DataType::Float64, true),
        Field::new(
            "st",
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Utf8, true),
                    Field::new("b", DataType::Int32, true),
                ]
                .into(),
            ),
            true,
        ),
        Field::new(
            "xs",
            DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
            true,
        ),
    ]));
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["aa", "zz"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![1, 3])) as ArrayRef,
        ),
    ]);
    let list_data = ArrayData::builder(DataType::List(Arc::new(Field::new(
        "element",
        DataType::Int32,
        true,
    ))))
    .len(2)
    .add_buffer(Buffer::from_slice_ref([0i32, 2, 3]))
    .add_child_data(Int32Array::from(vec![1, 2, 3]).into_data())
    .build()
    .expect("list data builds");
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "alpha-long-string-value-0001",
                "zulu-long-string-value-0003",
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.5, 2.5])) as ArrayRef,
            Arc::new(struct_array) as ArrayRef,
            Arc::new(ListArray::from(list_data)) as ArrayRef,
        ],
    )
    .expect("metrics batch builds")
}

fn hex_decode(raw: &str) -> Vec<u8> {
    let bytes = raw.as_bytes();
    assert!(
        bytes.len().is_multiple_of(2),
        "fixture bound hex has even length: {raw}"
    );
    (0..bytes.len())
        .step_by(2)
        .map(|at| {
            u8::from_str_radix(raw.get(at..at + 2).expect("hex pair"), 16).expect("hex decodes")
        })
        .collect()
}

fn datum_for_field(field_id: i32, raw: &[u8]) -> Datum {
    match field_id {
        1 => Datum::long(i64::from_le_bytes(
            raw.try_into().expect("long bound is 8 bytes"),
        )),
        2 | 6 => Datum::string(String::from_utf8(raw.to_vec()).expect("string bound is utf-8")),
        3 => Datum::double(f64::from_le_bytes(
            raw.try_into().expect("double bound is 8 bytes"),
        )),
        7 => Datum::int(i32::from_le_bytes(
            raw.try_into().expect("int bound is 4 bytes"),
        )),
        other => panic!("fixture holds a bound for unexpected field {other}"),
    }
}

fn expected_counts(cell: &Value, key: &str) -> HashMap<i32, u64> {
    cell[key]
        .as_object()
        .expect("counts cell holds a map")
        .iter()
        .map(|(field, count)| {
            (
                field.parse().expect("count key is a field id"),
                count.as_u64().expect("count is a u64"),
            )
        })
        .collect()
}

fn expected_bounds(cell: &Value, key: &str) -> HashMap<i32, Datum> {
    cell[key]
        .as_object()
        .expect("bounds cell holds a map")
        .iter()
        .map(|(field, bound)| {
            let field_id: i32 = field.parse().expect("bound key is a field id");
            let raw = hex_decode(bound.as_str().expect("bound is hex"));
            (field_id, datum_for_field(field_id, &raw))
        })
        .collect()
}

async fn live_single_data_file(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> DataFile {
    let table = catalog.load_table(ident).await.expect("load table");
    let metadata = table.metadata();
    let snapshot = metadata.current_snapshot().expect("snapshot exists");
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("manifest list loads");
    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("manifest loads");
        for entry in manifest.entries() {
            if entry.is_alive() && entry.data_file().content_type() == DataContentType::Data {
                files.push(entry.data_file().clone());
            }
        }
    }
    assert_eq!(files.len(), 1, "two rows stage exactly one data file");
    files.into_iter().next().expect("one data file")
}

async fn assert_metrics_cell(cell_name: &str) {
    let fixture = recorded_fixture();
    let cell = fixture["metrics"][cell_name].clone();
    assert!(cell.is_object(), "fixture holds cell {cell_name}");
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = metrics_catalog(&warehouse).await;
    let ident = create_metrics_table(&catalog, &format!("m_{cell_name}"), cell_name).await;
    append(&catalog, &ident, vec![metrics_batch()])
        .await
        .expect("append two oracle rows");
    let file = live_single_data_file(&catalog, &ident).await;
    assert_eq!(
        file.value_counts(),
        &expected_counts(&cell, "value_counts"),
        "cell {cell_name} value counts"
    );
    assert_eq!(
        file.null_value_counts(),
        &expected_counts(&cell, "null_value_counts"),
        "cell {cell_name} null counts"
    );
    assert_eq!(
        file.nan_value_counts(),
        &expected_counts(&cell, "nan_value_counts"),
        "cell {cell_name} nan counts"
    );
    assert_eq!(
        file.lower_bounds(),
        &expected_bounds(&cell, "lower_bounds"),
        "cell {cell_name} lower bounds"
    );
    assert_eq!(
        file.upper_bounds(),
        &expected_bounds(&cell, "upper_bounds"),
        "cell {cell_name} upper bounds"
    );
}

#[tokio::test]
async fn metrics_cell_default_matches_spark() {
    assert_metrics_cell("default").await;
}

#[tokio::test]
async fn metrics_cell_none_matches_spark() {
    assert_metrics_cell("none").await;
}

#[tokio::test]
async fn metrics_cell_counts_matches_spark() {
    assert_metrics_cell("counts").await;
}

#[tokio::test]
async fn metrics_cell_truncate4_matches_spark() {
    assert_metrics_cell("truncate4").await;
}

#[tokio::test]
async fn metrics_cell_full_matches_spark() {
    assert_metrics_cell("full").await;
}

#[tokio::test]
async fn metrics_cell_col_none_matches_spark() {
    assert_metrics_cell("col_none").await;
}

#[tokio::test]
async fn metrics_cell_col_nested_matches_spark() {
    assert_metrics_cell("col_nested").await;
}

#[tokio::test]
async fn metrics_cell_max_inferred_2_matches_spark() {
    assert_metrics_cell("max_inferred_2").await;
}

#[tokio::test]
async fn metrics_cell_max_inferred_2_default_set_matches_spark() {
    assert_metrics_cell("max_inferred_2_default_set").await;
}

#[tokio::test]
async fn metrics_cell_sorted_none_matches_spark() {
    assert_metrics_cell("sorted_none").await;
}

#[tokio::test]
async fn metrics_cell_sorted_counts_matches_spark() {
    assert_metrics_cell("sorted_counts").await;
}

#[tokio::test]
async fn metrics_cell_bad_mode_matches_spark() {
    assert_metrics_cell("bad_mode").await;
}

fn assert_no_metrics(file: &DataFile, path: &str) {
    assert!(
        file.value_counts().is_empty(),
        "none config writes no value counts: {path}"
    );
    assert!(
        file.null_value_counts().is_empty(),
        "none config writes no null counts: {path}"
    );
    assert!(
        file.nan_value_counts().is_empty(),
        "none config writes no nan counts: {path}"
    );
    assert!(
        file.lower_bounds().is_empty(),
        "none config writes no lower bounds: {path}"
    );
    assert!(
        file.upper_bounds().is_empty(),
        "none config writes no upper bounds: {path}"
    );
}

#[tokio::test]
async fn insert_path_applies_table_metrics_config() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = metrics_catalog(&warehouse).await;
    let ident = create_metrics_table(&catalog, "insert_none", "none").await;
    let table = catalog.load_table(&ident).await.expect("load table");
    let batches: Vec<Result<RecordBatch, DataFusionError>> = vec![Ok(metrics_batch())];
    let files = stage_unpartitioned_stream_with_overrides(
        &table,
        futures::stream::iter(batches),
        WriteConcurrency::new(1).expect("concurrency builds"),
        &WriterStagingOverrides::none(),
    )
    .await
    .expect("stage two oracle rows");
    assert_eq!(files.len(), 1, "two rows stage exactly one data file");
    assert_no_metrics(&files[0], files[0].file_path());
}

#[tokio::test]
async fn fanout_append_path_applies_table_metrics_config() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = metrics_catalog(&warehouse).await;
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "id_part", Transform::Identity)
        .expect("identity partition field builds")
        .build();
    let creation = TableCreation::builder()
        .name("fanout_none".to_string())
        .schema(metrics_schema())
        .properties(cell_properties("none"))
        .partition_spec(spec)
        .format_version(FormatVersion::V2)
        .build();
    catalog
        .create_table(&NamespaceIdent::new("metrics".to_string()), creation)
        .await
        .expect("create partitioned table");
    let ident = TableIdent::new(
        NamespaceIdent::new("metrics".to_string()),
        "fanout_none".to_string(),
    );
    let table = catalog.load_table(&ident).await.expect("load table");
    let files = write_partitioned_data_files_with_concurrency(
        &table,
        vec![metrics_batch()],
        WriteConcurrency::default(),
    )
    .await
    .expect("fanout two oracle rows");
    assert!(!files.is_empty(), "fanout stages at least one data file");
    for file in &files {
        assert_no_metrics(file, file.file_path());
    }
}

#[tokio::test]
async fn merge_rewrite_path_applies_table_metrics_config() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = metrics_catalog(&warehouse).await;
    let ident = create_metrics_table(&catalog, "cow_none", "none").await;
    let table = catalog.load_table(&ident).await.expect("load table");
    let files = write_data_files(&table, vec![metrics_batch()])
        .await
        .expect("rewrite two oracle rows");
    assert_eq!(files.len(), 1, "two rows rewrite exactly one data file");
    assert_no_metrics(&files[0], files[0].file_path());
}

#[tokio::test]
async fn merge_lineage_path_applies_table_metrics_config() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = metrics_catalog(&warehouse).await;
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "part", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("lineage schema builds");
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "part", Transform::Identity)
        .expect("identity partition field builds")
        .build();
    let creation = TableCreation::builder()
        .name("lineage_none".to_string())
        .schema(schema)
        .properties(cell_properties("none"))
        .partition_spec(spec)
        .format_version(FormatVersion::V3)
        .build();
    catalog
        .create_table(&NamespaceIdent::new("metrics".to_string()), creation)
        .await
        .expect("create lineage table");
    let ident = TableIdent::new(
        NamespaceIdent::new("metrics".to_string()),
        "lineage_none".to_string(),
    );
    let table = catalog.load_table(&ident).await.expect("load table");
    let lineage_field = |name: &str, field_id: i32| {
        Field::new(name, DataType::Int64, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )]))
    };
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("part", DataType::Int32, false),
        lineage_field(RESERVED_COL_NAME_ROW_ID, RESERVED_FIELD_ID_ROW_ID),
        lineage_field(
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None, Some(5)])) as ArrayRef,
        ],
    )
    .expect("lineage batch builds");
    let batches: Vec<Result<RecordBatch, DataFusionError>> = vec![Ok(batch)];
    let files = write_partitioned_lineage_files(&table, futures::stream::iter(batches))
        .await
        .expect("rewrite lineage rows");
    assert!(
        !files.is_empty(),
        "lineage rewrite stages at least one data file"
    );
    for file in &files {
        assert_no_metrics(file, file.file_path());
    }
}

#[tokio::test]
async fn position_delete_config_carries_table_default_and_row_overlays() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = metrics_catalog(&warehouse).await;
    let properties = HashMap::from([
        (
            "write.metadata.metrics.default".to_string(),
            "none".to_string(),
        ),
        (
            "write.metadata.metrics.column.s".to_string(),
            "full".to_string(),
        ),
    ]);
    let creation = TableCreation::builder()
        .name("delete_config".to_string())
        .schema(metrics_schema())
        .properties(properties)
        .format_version(FormatVersion::V2)
        .build();
    catalog
        .create_table(&NamespaceIdent::new("metrics".to_string()), creation)
        .await
        .expect("create table");
    let ident = TableIdent::new(
        NamespaceIdent::new("metrics".to_string()),
        "delete_config".to_string(),
    );
    let table = catalog.load_table(&ident).await.expect("load table");
    let config = MetricsConfig::for_position_delete_table(table.metadata()).expect("config builds");
    assert_eq!(config.default_mode_of(), MetricsMode::None);
    assert_eq!(config.column_mode("file_path"), MetricsMode::Full);
    assert_eq!(config.column_mode("pos"), MetricsMode::Full);
    assert_eq!(config.column_mode("row.s"), MetricsMode::Full);
    assert_eq!(config.column_mode("row.id"), MetricsMode::None);
}

#[tokio::test]
async fn position_delete_files_carry_delete_type_and_full_path_bounds() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = metrics_catalog(&warehouse).await;
    let ident = create_metrics_table(&catalog, "delete_files", "default").await;
    append(&catalog, &ident, vec![metrics_batch()])
        .await
        .expect("append two oracle rows");
    let data = live_single_data_file(&catalog, &ident).await;
    let table = catalog.load_table(&ident).await.expect("load table");
    let pairs = vec![
        (Arc::from(data.file_path()), 0),
        (Arc::from(data.file_path()), 1),
    ];
    let files = write_position_deletes(
        &table,
        &pairs,
        WriteConcurrency::new(1).expect("concurrency builds"),
    )
    .await
    .expect("write position deletes");
    assert_eq!(files.len(), 1, "one pair group writes one delete file");
    let delete = &files[0];
    assert_eq!(delete.content_type(), DataContentType::PositionDeletes);
    let expected = Datum::string(data.file_path());
    assert_eq!(
        delete
            .lower_bounds()
            .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH),
        Some(&expected),
        "file_path lower bound keeps the full path"
    );
    assert_eq!(
        delete
            .upper_bounds()
            .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH),
        Some(&expected),
        "file_path upper bound keeps the full path"
    );
    let bytes = table
        .file_io()
        .new_input(delete.file_path())
        .expect("open delete file")
        .read()
        .await
        .expect("read delete file");
    let metadata = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("open parquet")
        .metadata()
        .clone();
    let key_values = metadata
        .file_metadata()
        .key_value_metadata()
        .expect("footer carries key values");
    assert!(
        key_values
            .iter()
            .any(|entry| entry.key == "delete-type" && entry.value.as_deref() == Some("position")),
        "footer carries the delete-type position marker"
    );
}
