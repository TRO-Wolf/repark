use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;
use iceberg::arrow::type_to_arrow_type;
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
    use futures::StreamExt;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataFile, Literal, NestedField, PrimitiveLiteral, PrimitiveType, Schema as IcebergSchema,
        Type, UnboundPartitionSpec,
    };
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;
    use crate::write::concurrency::WriteConcurrency;
    use crate::write::partition_write::write_data_files_from_plan;

    const PARTITION_VALUES: i64 = 8;
    const NULL_EVERY: i64 = 4;
    const MICROS_PER_DAY: i64 = 86_400_000_000;
    const ID_STRIDE: i64 = 100_000;

    struct PartitionedSourceExec {
        schema: SchemaRef,
        rows_per_partition: Vec<usize>,
        plan_properties: Arc<PlanProperties>,
    }

    impl PartitionedSourceExec {
        fn new(rows_per_partition: Vec<usize>) -> Self {
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
            let batch = source_batch(&self.schema, partition, rows);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                futures::stream::iter(vec![batch]).boxed(),
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
        let namespace = NamespaceIdent::new("ns".into());
        let _ = catalog.create_namespace(&namespace, HashMap::new()).await;
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(iceberg_schema())
            .partition_spec_opt(spec)
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
}
