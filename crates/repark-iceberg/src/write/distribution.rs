use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute::{cast, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::hash_utils::create_hashes;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::repartition::{REPARTITION_RANDOM_STATE, RepartitionExec};
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt};
use iceberg::arrow::{PartitionValueCalculator, type_to_arrow_type};
use iceberg::spec::Transform;
use iceberg::table::Table;
use iceberg::transform::create_transform_function;

use crate::write::merge::iceberg_err;

#[derive(Debug, Eq)]
pub(crate) struct PartitionTransformExpr {
    source: Arc<dyn PhysicalExpr>,
    transform: Transform,
    source_type: DataType,
    result_type: DataType,
}

impl PartialEq for PartitionTransformExpr {
    fn eq(&self, other: &Self) -> bool {
        self.source.eq(&other.source)
            && self.transform == other.transform
            && self.source_type == other.source_type
            && self.result_type == other.result_type
    }
}

impl Hash for PartitionTransformExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source.hash(state);
        self.transform.hash(state);
        self.source_type.hash(state);
        self.result_type.hash(state);
    }
}

impl Display for PartitionTransformExpr {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}({})", self.transform, self.source)
    }
}

impl PhysicalExpr for PartitionTransformExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.result_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.source.evaluate(batch)?.into_array(batch.num_rows())?;
        let array = if array.data_type() == &self.source_type {
            array
        } else {
            cast(&array, &self.source_type)?
        };
        let transformed = create_transform_function(&self.transform)
            .map_err(iceberg_err)?
            .transform(array)
            .map_err(iceberg_err)?;
        Ok(ColumnarValue::Array(transformed))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.source]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [source] = <[Arc<dyn PhysicalExpr>; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "PartitionTransformExpr expects exactly one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self {
            source,
            transform: self.transform,
            source_type: self.source_type.clone(),
            result_type: self.result_type.clone(),
        }))
    }

    fn fmt_sql(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self}")
    }
}

pub(crate) fn partition_value_exprs(
    table: &Table,
    input_schema: &Schema,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let schema = table.metadata().current_schema();
    let spec = table.metadata().default_partition_spec();
    let partition_type = spec.partition_type(schema).map_err(iceberg_err)?;
    let mut exprs: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(spec.fields().len());
    for (field, partition_field) in spec.fields().iter().zip(partition_type.fields()) {
        let source_field = schema.field_by_id(field.source_id).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "partition field {} names source field id {}, which the table schema lacks",
                field.name, field.source_id
            ))
        })?;
        let index = input_schema.index_of(&source_field.name)?;
        let source_type = type_to_arrow_type(&source_field.field_type).map_err(iceberg_err)?;
        let result_type = type_to_arrow_type(&partition_field.field_type).map_err(iceberg_err)?;
        exprs.push(Arc::new(PartitionTransformExpr {
            source: Arc::new(Column::new(&source_field.name, index)),
            transform: field.transform,
            source_type,
            result_type,
        }));
    }
    Ok(exprs)
}

pub(crate) fn hash_distribution(
    table: &Table,
    input: Arc<dyn ExecutionPlan>,
    writers: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if writers <= 1 || table.metadata().default_partition_spec().is_unpartitioned() {
        return Ok(input);
    }
    let exprs = partition_value_exprs(table, input.schema().as_ref())?;
    Ok(Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Hash(exprs, writers),
    )?))
}

pub(crate) struct PartitionRouter {
    calculator: PartitionValueCalculator,
    slots: usize,
}

impl PartitionRouter {
    pub(crate) fn try_new(table: &Table, slots: usize) -> Result<Self> {
        let calculator = PartitionValueCalculator::try_new(
            table.metadata().default_partition_spec(),
            table.metadata().current_schema(),
        )
        .map_err(iceberg_err)?;
        Ok(Self {
            calculator,
            slots: slots.max(1),
        })
    }

    pub(crate) fn route(&self, batch: &RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
        let values = self.calculator.calculate(batch).map_err(iceberg_err)?;
        let mut hashes = vec![0u64; batch.num_rows()];
        create_hashes(
            &[values],
            REPARTITION_RANDOM_STATE.random_state(),
            &mut hashes,
        )?;
        let slots = u64::try_from(self.slots).map_err(|_| slot_count_error(self.slots))?;
        let mut rows_per_slot: Vec<Vec<u32>> = vec![Vec::new(); self.slots];
        for (row, hash) in hashes.iter().enumerate() {
            let slot = usize::try_from(hash % slots).map_err(|_| slot_count_error(self.slots))?;
            let row = u32::try_from(row).map_err(|_| {
                DataFusionError::Execution(format!(
                    "a batch of {} rows is too tall to route by partition value",
                    batch.num_rows()
                ))
            })?;
            rows_per_slot[slot].push(row);
        }
        let mut routed = Vec::with_capacity(self.slots);
        for (slot, rows) in rows_per_slot.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            routed.push((slot, take_record_batch(batch, &UInt32Array::from(rows))?));
        }
        Ok(routed)
    }
}

fn slot_count_error(slots: usize) -> DataFusionError {
    DataFusionError::Internal(format!("{slots} write workers cannot be addressed as u64"))
}

pub(crate) fn route_partitioned_stream<S>(
    table: &Table,
    slots: usize,
    stream: S,
) -> Result<impl Stream<Item = Result<Vec<(usize, RecordBatch)>>> + Unpin>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let router = PartitionRouter::try_new(table, slots)?;
    Ok(stream.map(move |item| item.and_then(|batch| router.route(&batch))))
}

pub(crate) async fn send_routed(
    senders: &mut [mpsc::Sender<RecordBatch>],
    parts: Vec<(usize, RecordBatch)>,
) -> bool {
    for (slot, part) in parts {
        if senders[slot].send(part).await.is_err() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fmt::Debug;

    use datafusion::arrow::array::{
        Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataFile, Literal, NestedField, PrimitiveLiteral, PrimitiveType, Schema as IcebergSchema,
        Type, UnboundPartitionSpec,
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
        fn fmt_as(
            &self,
            _format: DisplayFormatType,
            formatter: &mut Formatter,
        ) -> std::fmt::Result {
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
        let ColumnarValue::Array(values) = expr.evaluate(&batch).expect("truncate evaluates")
        else {
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
}
