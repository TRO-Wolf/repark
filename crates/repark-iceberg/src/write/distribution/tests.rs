use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use datafusion::arrow::array::{Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
use futures::StreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataFile, Literal, NestedField, PrimitiveLiteral, PrimitiveType, Schema as IcebergSchema, Type,
    UnboundPartitionSpec,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::*;
use crate::write::append::write_partitioned_data_files_from_stream_with_concurrency;
use crate::write::concurrency::WriteConcurrency;
use crate::write::merge::row_lineage::table_carries_merge_lineage;
use crate::write::merge::write_new_data_files_from_stream;
use crate::write::partition_write::write_data_files_from_plan;

const PARTITION_VALUES: i64 = 8;
const NULL_EVERY: i64 = 4;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const ID_STRIDE: i64 = 100_000;

struct PartitionedSourceExec {
    schema: SchemaRef,
    rows_per_partition: Vec<usize>,
    fail_partition: Option<usize>,
    plan_properties: Arc<PlanProperties>,
}

impl PartitionedSourceExec {
    fn new(rows_per_partition: Vec<usize>) -> Self {
        Self::failing(rows_per_partition, None)
    }

    fn failing(rows_per_partition: Vec<usize>, fail_partition: Option<usize>) -> Self {
        let schema = source_schema();
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(rows_per_partition.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            rows_per_partition,
            fail_partition,
            plan_properties,
        }
    }
}

impl Debug for PartitionedSourceExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PartitionedSourceExec").finish()
    }
}

impl DisplayAs for PartitionedSourceExec {
    fn fmt_as(&self, _format: DisplayFormatType, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "PartitionedSourceExec")
    }
}

impl ExecutionPlan for PartitionedSourceExec {
    fn name(&self) -> &'static str {
        "PartitionedSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let rows = self.rows_per_partition[partition];
        let mut items = vec![source_batch(&self.schema, partition, rows)];
        if self.fail_partition == Some(partition) {
            items.push(Err(DataFusionError::Execution(
                "injected partition source failure".into(),
            )));
        }
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(items).boxed(),
        )))
    }
}

fn source_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("part", DataType::Int32, false),
        Field::new("label", DataType::Utf8, true),
    ]))
}

fn source_batch(schema: &SchemaRef, partition: usize, rows: usize) -> Result<RecordBatch> {
    let offset = i64::try_from(partition).expect("partition fits i64") * ID_STRIDE;
    let indexes = (0..i64::try_from(rows).expect("rows fit i64")).collect::<Vec<_>>();
    let ids = indexes
        .iter()
        .map(|index| offset + index)
        .collect::<Vec<_>>();
    let stamps = indexes
        .iter()
        .map(|index| (index % 2) * MICROS_PER_DAY)
        .collect::<Vec<_>>();
    let parts = indexes
        .iter()
        .map(|index| i32::try_from(index % PARTITION_VALUES).expect("part fits i32"))
        .collect::<Vec<_>>();
    let labels = indexes
        .iter()
        .map(|index| (index % NULL_EVERY != 0).then(|| format!("l{}", index % 3)))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(TimestampMicrosecondArray::from(stamps)),
            Arc::new(Int32Array::from(parts)),
            Arc::new(StringArray::from(labels)),
        ],
    )
    .map_err(DataFusionError::from)
}

fn iceberg_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
            NestedField::required(3, "part", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(4, "label", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema")
}

async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "mem",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
        )
        .await
        .expect("memory catalog");
    Arc::new(catalog)
}

async fn create_table(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    spec: Option<UnboundPartitionSpec>,
) -> Table {
    create_table_with(catalog, name, spec, HashMap::new()).await
}

async fn create_table_with(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    spec: Option<UnboundPartitionSpec>,
    properties: HashMap<String, String>,
) -> Table {
    let namespace = NamespaceIdent::new("ns".into());
    let _ = catalog.create_namespace(&namespace, HashMap::new()).await;
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(iceberg_schema())
        .partition_spec_opt(spec)
        .properties(properties)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    catalog
        .load_table(&TableIdent::new(namespace, name.into()))
        .await
        .expect("load table")
}

async fn declare_order(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    fields: Vec<crate::write::sort_order::WriteSortField>,
) -> Table {
    let ident = TableIdent::new(NamespaceIdent::new("ns".into()), name.into());
    crate::write::sort_order::apply_write_order(catalog.as_ref(), &ident, &fields, None)
        .await
        .expect("declare sort order");
    catalog.load_table(&ident).await.expect("reload table")
}

async fn file_ids(table: &Table, path: &str) -> Vec<i64> {
    use datafusion::arrow::array::{Array, Int64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let bytes = table
        .file_io()
        .new_input(path)
        .expect("open data file")
        .read()
        .await
        .expect("read data file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("parquet reader")
        .build()
        .expect("build parquet reader");
    let mut ids = Vec::new();
    for batch in reader {
        let batch = batch.expect("read data-file batch");
        let column = batch
            .column_by_name("id")
            .expect("data file has an `id` column");
        let values = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("`id` is Int64");
        ids.extend(values.values().iter().copied());
    }
    ids
}

fn identity_spec(source_id: i32, name: &str) -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder()
        .add_partition_field(source_id, name, Transform::Identity)
        .expect("identity field")
        .build()
}

fn bucket_and_day_spec() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder()
        .add_partition_field(1, "id_bucket", Transform::Bucket(4))
        .expect("bucket field")
        .add_partition_field(2, "ts_day", Transform::Day)
        .expect("day field")
        .build()
}

async fn write(table: &Table, rows_per_partition: Vec<usize>) -> Vec<DataFile> {
    let writers = rows_per_partition.len();
    write_data_files_from_plan(
        table,
        Arc::new(PartitionedSourceExec::new(rows_per_partition)),
        Arc::new(TaskContext::default()),
        WriteConcurrency::new(writers).expect("concurrency"),
    )
    .await
    .expect("write succeeds")
}

fn layout(files: &[DataFile]) -> Vec<(Vec<Option<Literal>>, u64)> {
    files
        .iter()
        .map(|file| {
            (
                file.partition()
                    .iter()
                    .map(|value: Option<&Literal>| value.cloned())
                    .collect(),
                file.record_count(),
            )
        })
        .collect()
}

fn one_file_per_value(rows: u64) -> Vec<(Vec<Option<Literal>>, u64)> {
    (0..PARTITION_VALUES)
        .map(|value| {
            let value = i32::try_from(value).expect("value fits i32");
            (
                vec![Some(Literal::Primitive(PrimitiveLiteral::Int(value)))],
                rows,
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_partition_value_lands_in_exactly_one_writer() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table(&catalog, "identity", Some(identity_spec(3, "part"))).await;
    let files = write(&table, vec![64, 64, 64, 64]).await;
    assert_eq!(layout(&files), one_file_per_value(32));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_distribution_is_deterministic_across_runs() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let mut runs = Vec::new();
    for run in 0..2 {
        let table = create_table(
            &catalog,
            &format!("run{run}"),
            Some(identity_spec(3, "part")),
        )
        .await;
        runs.push(layout(&write(&table, vec![40, 24, 64, 8]).await));
    }
    assert_eq!(runs[0], one_file_per_value(17));
    assert_eq!(runs[0], runs[1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn input_partitions_without_rows_do_not_split_a_value() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let sparse = create_table(&catalog, "sparse", Some(identity_spec(3, "part"))).await;
    let files = write(&sparse, vec![64, 0, 64, 0]).await;
    assert_eq!(layout(&files), one_file_per_value(16));
    let empty = create_table(&catalog, "empty", Some(identity_spec(3, "part"))).await;
    assert!(write(&empty, vec![0, 0, 0, 0]).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn null_partition_values_share_one_writer() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table(&catalog, "labelled", Some(identity_spec(4, "label"))).await;
    let files = layout(&write(&table, vec![64, 64, 64, 64]).await);
    let keys = files.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
    let expected = [None, Some("l0"), Some("l1"), Some("l2")]
        .into_iter()
        .map(|label| {
            vec![label.map(|label| Literal::Primitive(PrimitiveLiteral::String(label.into())))]
        })
        .collect::<Vec<_>>();
    assert_eq!(keys, expected, "{files:?}");
    assert_eq!(
        files[0].1, 64,
        "every input partition's NULLs share one file"
    );
    assert_eq!(files.iter().map(|(_, rows)| rows).sum::<u64>(), 256);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_and_day_transforms_key_on_the_transformed_value() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table(&catalog, "bucketed", Some(bucket_and_day_spec())).await;
    let files = layout(&write(&table, vec![64, 64, 64, 64]).await);
    let keys = files
        .iter()
        .map(|(key, _)| format!("{key:?}"))
        .collect::<HashSet<_>>();
    assert_eq!(
        keys.len(),
        files.len(),
        "one file per (bucket, day): {files:?}"
    );
    assert_eq!(files.len(), 8, "{files:?}");
    assert_eq!(files.iter().map(|(_, rows)| rows).sum::<u64>(), 256);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unpartitioned_table_keeps_one_file_per_input_partition() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table(&catalog, "plain", None).await;
    let files = write(&table, vec![64, 64, 64, 64]).await;
    assert_eq!(
        files.iter().map(DataFile::record_count).collect::<Vec<_>>(),
        vec![64, 64, 64, 64]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missing_partition_source_column_is_a_planning_error() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table(&catalog, "narrow", Some(identity_spec(3, "part"))).await;
    let narrow = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
    let error = partition_value_exprs(&table, &narrow).expect_err("part is absent");
    assert!(error.to_string().contains("part"), "{error}");
}

fn distribution_properties(mode: &str) -> HashMap<String, String> {
    HashMap::from([(
        crate::write::sort_order::DISTRIBUTION_MODE_PROPERTY.to_string(),
        mode.to_string(),
    )])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn none_distribution_mode_skips_the_hash_rule() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table_with(
        &catalog,
        "scattered",
        Some(identity_spec(3, "part")),
        distribution_properties("none"),
    )
    .await;
    let files = write(&table, vec![64, 64, 64, 64]).await;
    let expected = (0..PARTITION_VALUES)
        .flat_map(|value| {
            let value = i32::try_from(value).expect("value fits i32");
            vec![
                (
                    vec![Some(Literal::Primitive(PrimitiveLiteral::Int(value)))],
                    8,
                );
                4
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 32);
    assert_eq!(layout(&files), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_and_range_distribution_modes_hash_by_partition_value() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    for mode in ["hash", "range"] {
        let table = create_table_with(
            &catalog,
            &format!("grouped{mode}"),
            Some(identity_spec(3, "part")),
            distribution_properties(mode),
        )
        .await;
        let files = write(&table, vec![64, 64, 64, 64]).await;
        assert_eq!(layout(&files), one_file_per_value(32), "{mode}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_distribution_mode_is_a_planning_error() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table_with(
        &catalog,
        "sideways",
        Some(identity_spec(3, "part")),
        distribution_properties("sideways"),
    )
    .await;
    let error = write_data_files_from_plan(
        &table,
        Arc::new(PartitionedSourceExec::new(vec![32, 32])),
        Arc::new(TaskContext::default()),
        WriteConcurrency::new(2).expect("concurrency"),
    )
    .await
    .expect_err("an unknown distribution mode refuses");
    assert!(
        error.to_string().contains("write.distribution-mode"),
        "{error}"
    );
}

async fn stream_write(table: &Table, rows_per_batch: Vec<usize>) -> Vec<DataFile> {
    let schema = source_schema();
    let batches = rows_per_batch
        .into_iter()
        .enumerate()
        .map(|(index, rows)| source_batch(&schema, index, rows))
        .collect::<Vec<_>>();
    write_partitioned_data_files_from_stream_with_concurrency(
        table,
        futures::stream::iter(batches),
        WriteConcurrency::new(4).expect("concurrency"),
    )
    .await
    .expect("stream write succeeds")
}

fn warehouse_data_files(warehouse: &TempDir) -> Vec<std::path::PathBuf> {
    let mut pending = vec![warehouse.path().to_path_buf()];
    let mut found = Vec::new();
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_path_lands_one_partition_value_in_one_writer() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table(&catalog, "stream", Some(identity_spec(3, "part"))).await;
    let files = stream_write(&table, vec![64, 64, 64, 64]).await;
    assert_eq!(layout(&files), one_file_per_value(32));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_path_distribution_is_deterministic_across_runs() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let mut runs = Vec::new();
    for run in 0..2 {
        let table = create_table(
            &catalog,
            &format!("srun{run}"),
            Some(identity_spec(3, "part")),
        )
        .await;
        runs.push(layout(&stream_write(&table, vec![40, 24, 64, 8]).await));
    }
    assert_eq!(runs[0], one_file_per_value(17));
    assert_eq!(runs[0], runs[1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_path_null_partition_values_share_one_writer() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table(&catalog, "snull", Some(identity_spec(4, "label"))).await;
    let files = layout(&stream_write(&table, vec![64, 64, 64, 64]).await);
    assert_eq!(files.len(), 4, "{files:?}");
    assert_eq!(files[0], (vec![None], 64), "{files:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_path_two_field_spec_keys_on_the_transform_value() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table(&catalog, "sbucket", Some(bucket_and_day_spec())).await;
    let files = layout(&stream_write(&table, vec![64, 64, 64, 64]).await);
    let keys = files
        .iter()
        .map(|(key, _)| format!("{key:?}"))
        .collect::<HashSet<_>>();
    assert_eq!(keys.len(), files.len(), "{files:?}");
    assert_eq!(files.len(), 8, "{files:?}");
    assert_eq!(files.iter().map(|(_, rows)| rows).sum::<u64>(), 256);
}

#[test]
fn truncate_over_a_view_typed_string_keys_on_the_cast_value() {
    use datafusion::arrow::array::StringViewArray;
    use datafusion::physical_expr::expressions::Column;

    let expr = PartitionTransformExpr {
        source: Arc::new(Column::new("s", 0)),
        transform: Transform::Truncate(3),
        source_type: DataType::Utf8,
        result_type: DataType::Utf8,
    };
    let schema: SchemaRef = Arc::new(ArrowSchema::new(vec![Field::new(
        "s",
        DataType::Utf8View,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringViewArray::from(vec!["s00x1", "s01x22"]))],
    )
    .expect("view batch");
    let ColumnarValue::Array(values) = expr.evaluate(&batch).expect("truncate evaluates") else {
        panic!("truncate of a string column yields an array");
    };
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("truncated Utf8");
    assert_eq!(values.value(0), "s00");
    assert_eq!(values.value(1), "s01");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_inserts_into_a_partitioned_table_route_one_value_to_one_writer() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table(&catalog, "mergestream", Some(identity_spec(3, "part"))).await;
    assert!(
        !table_carries_merge_lineage(&table),
        "the pin covers the routed funnel, not the serial lineage writer"
    );
    let schema = source_schema();
    let batches = [64, 64, 64, 64]
        .into_iter()
        .enumerate()
        .map(|(index, rows)| source_batch(&schema, index, rows))
        .collect::<Vec<_>>();
    let write_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .expect("write schema"),
    );
    let files = write_new_data_files_from_stream(
        &table,
        &write_schema,
        futures::stream::iter(batches),
        WriteConcurrency::new(4).expect("concurrency"),
    )
    .await
    .expect("merge stream write succeeds");
    assert_eq!(layout(&files), one_file_per_value(32));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_router_sends_every_row_of_one_value_to_one_slot() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table(&catalog, "router", Some(identity_spec(3, "part"))).await;
    let router = PartitionRouter::try_new(&table, 4).expect("router");
    let batch = source_batch(&source_schema(), 0, 64).expect("batch");
    let first_pass = router.route(&batch).expect("route");
    let mut slot_of_value: HashMap<i32, HashSet<usize>> = HashMap::new();
    for (slot, part) in &first_pass {
        let part_values = part
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("part column");
        for value in part_values.values() {
            slot_of_value.entry(*value).or_default().insert(*slot);
        }
    }
    assert_eq!(slot_of_value.len(), 8);
    assert!(
        slot_of_value.values().all(|slots| slots.len() == 1),
        "{slot_of_value:?}"
    );
    let second_pass = router.route(&batch).expect("route again");
    assert_eq!(
        first_pass
            .iter()
            .map(|(slot, part)| (*slot, part.num_rows()))
            .collect::<Vec<_>>(),
        second_pass
            .iter()
            .map(|(slot, part)| (*slot, part.num_rows()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        first_pass
            .iter()
            .map(|(_, part)| part.num_rows())
            .sum::<usize>(),
        64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_failure_into_a_partitioned_table_leaves_no_data_file() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table_with(
        &catalog,
        "abort",
        Some(identity_spec(3, "part")),
        HashMap::from([(
            "write.target-file-size-bytes".to_string(),
            "65536".to_string(),
        )]),
    )
    .await;
    let error = write_data_files_from_plan(
        &table,
        Arc::new(PartitionedSourceExec::failing(
            vec![20_000, 20_000, 20_000, 20_000],
            Some(2),
        )),
        Arc::new(TaskContext::default()),
        WriteConcurrency::new(4).expect("concurrency"),
    )
    .await
    .expect_err("the injected partition failure must surface");
    assert!(
        error
            .to_string()
            .contains("injected partition source failure"),
        "{error}"
    );
    assert!(
        warehouse_data_files(&warehouse).is_empty(),
        "a failed partitioned write leaves no data file behind: {:?}",
        warehouse_data_files(&warehouse)
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn none_distribution_mode_deals_stream_batches_round_robin() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table_with(
        &catalog,
        "unrouted",
        Some(identity_spec(3, "part")),
        distribution_properties("none"),
    )
    .await;
    let files = stream_write(&table, vec![64, 64, 64, 64]).await;
    let expected = (0..PARTITION_VALUES)
        .flat_map(|value| {
            let value = i32::try_from(value).expect("value fits i32");
            vec![
                (
                    vec![Some(Literal::Primitive(PrimitiveLiteral::Int(value)))],
                    8,
                );
                4
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 32);
    assert_eq!(layout(&files), expected);
}

fn shuffled_full_batches() -> Vec<RecordBatch> {
    let schema = source_schema();
    [vec![3_i64, 1], vec![4, 0, 2]]
        .into_iter()
        .map(|ids| {
            let rows = ids.len();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(TimestampMicrosecondArray::from(vec![0; rows])),
                    Arc::new(Int32Array::from(vec![0; rows])),
                    Arc::new(StringArray::from(vec![None::<&str>; rows])),
                ],
            )
            .expect("shuffled batch")
        })
        .collect()
}

fn batch_ids(batches: &[RecordBatch]) -> Vec<i64> {
    use datafusion::arrow::array::Array;

    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("id")
            .expect("batch has an `id` column");
        let values = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("`id` is Int64");
        ids.extend(values.values().iter().copied());
    }
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_sort_order_sorts_batches_across_batch_boundaries() {
    use crate::write::sort_order::WriteSortField;
    use iceberg::spec::{NullOrder, SortDirection};

    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    create_table(&catalog, "ordered", None).await;
    let table = declare_order(
        &catalog,
        "ordered",
        vec![WriteSortField {
            name: "id".to_string(),
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
        }],
    )
    .await;
    let sorted = sort_batches_by_default_order(&table, shuffled_full_batches())
        .await
        .expect("sort succeeds");
    assert_eq!(batch_ids(&sorted), vec![0, 1, 2, 3, 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sort_batches_without_an_order_returns_its_input() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = create_table(&catalog, "plain", None).await;
    let batches = shuffled_full_batches();
    let returned = sort_batches_by_default_order(&table, batches)
        .await
        .expect("identity sort succeeds");
    assert_eq!(batch_ids(&returned), vec![3, 1, 4, 0, 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn written_files_are_sorted_by_the_declared_order() {
    use crate::write::sort_order::WriteSortField;
    use iceberg::spec::{NullOrder, SortDirection};

    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    create_table(&catalog, "ordered", Some(identity_spec(3, "part"))).await;
    let table = declare_order(
        &catalog,
        "ordered",
        vec![WriteSortField {
            name: "id".to_string(),
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
        }],
    )
    .await;
    let files = write(&table, vec![64, 64, 64, 64]).await;
    assert_eq!(files.len(), 8);
    for file in &files {
        let ids = file_ids(&table, file.file_path()).await;
        assert_eq!(ids.len(), 32);
        assert!(
            ids.windows(2).all(|pair| pair[0] <= pair[1]),
            "file {} is not sorted: {ids:?}",
            file.file_path()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unpartitioned_sorted_write_keeps_sorted_files() {
    use crate::write::sort_order::WriteSortField;
    use iceberg::spec::{NullOrder, SortDirection};

    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    create_table(&catalog, "ordered", None).await;
    let table = declare_order(
        &catalog,
        "ordered",
        vec![WriteSortField {
            name: "id".to_string(),
            direction: SortDirection::Descending,
            null_order: NullOrder::Last,
        }],
    )
    .await;
    let batches = [shuffled_full_batches(), shuffled_full_batches()].concat();
    let stream = futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>));
    let files = crate::write::merge::write_data_files_from_stream_with_concurrency(
        &table,
        stream,
        WriteConcurrency::new(2).expect("concurrency"),
    )
    .await
    .expect("sorted write succeeds");
    assert!(!files.is_empty());
    let mut ids = Vec::new();
    for file in &files {
        let file_rows = file_ids(&table, file.file_path()).await;
        assert!(
            file_rows.windows(2).all(|pair| pair[0] >= pair[1]),
            "file {} is not descending: {file_rows:?}",
            file.file_path()
        );
        ids.extend(file_rows);
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 0, 1, 1, 2, 2, 3, 3, 4, 4]);
}
