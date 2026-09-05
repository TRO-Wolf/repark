use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_stream,
};
use futures::StreamExt;
use iceberg::spec::DataFile;
use iceberg::table::Table;

use crate::write::append::write_partitioned_data_files_from_stream_with_concurrency;
use crate::write::concurrency::WriteConcurrency;
use crate::write::file_order::ascending_partition_order;
use crate::write::merge::write_data_files_from_stream_with_concurrency;

pub const WRITTEN_FILES_COL_NAME: &str = "written_files";

type FileCollector = Arc<Mutex<BTreeMap<usize, Vec<DataFile>>>>;

pub struct IcebergPartitionWriteExec {
    table: Table,
    input: Arc<dyn ExecutionPlan>,
    writers: usize,
    collected: FileCollector,
    aborted: Arc<AtomicBool>,
    result_schema: SchemaRef,
    plan_properties: Arc<PlanProperties>,
}

impl Debug for IcebergPartitionWriteExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergPartitionWriteExec")
            .field("table", &self.table.identifier().to_string())
            .field("input", &self.input)
            .field("writers", &self.writers)
            .field(
                "collected",
                &self.collected.lock().map(|files| files.len()).ok(),
            )
            .field("aborted", &self.aborted)
            .field("result_schema", &self.result_schema)
            .field("plan_properties", &self.plan_properties)
            .finish()
    }
}

impl DisplayAs for IcebergPartitionWriteExec {
    fn fmt_as(&self, _format: DisplayFormatType, formatter: &mut Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "IcebergPartitionWriteExec: table={}, writers={}",
            self.table.identifier(),
            self.writers
        )
    }
}

impl IcebergPartitionWriteExec {
    fn new(
        table: Table,
        input: Arc<dyn ExecutionPlan>,
        writers: usize,
        collected: FileCollector,
    ) -> Self {
        let result_schema = result_schema();
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&result_schema)),
            Partitioning::UnknownPartitioning(writers),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            table,
            input,
            writers,
            collected,
            aborted: Arc::new(AtomicBool::new(false)),
            result_schema,
            plan_properties,
        }
    }
}

impl ExecutionPlan for IcebergPartitionWriteExec {
    fn name(&self) -> &'static str {
        "IcebergPartitionWriteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [child] = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "IcebergPartitionWriteExec expects exactly one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self::new(
            self.table.clone(),
            child,
            self.writers,
            Arc::clone(&self.collected),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = Arc::clone(&self.input);
        let table = self.table.clone();
        let collected = Arc::clone(&self.collected);
        let result_schema = Arc::clone(&self.result_schema);
        let unpartitioned = table.metadata().default_partition_spec().is_unpartitioned();
        let one_writer = WriteConcurrency::new(1)?;
        let raise_abort = Arc::clone(&self.aborted);
        let observe_abort = Arc::clone(&self.aborted);

        let written = futures::stream::once(async move {
            let batches = Box::pin(
                input
                    .execute(partition, context)?
                    .map(move |item| {
                        if item.is_err() {
                            raise_abort.store(true, Ordering::SeqCst);
                        }
                        item
                    })
                    .take_while(move |item| {
                        futures::future::ready(
                            item.is_err() || !observe_abort.load(Ordering::SeqCst),
                        )
                    }),
            );
            let files = if unpartitioned {
                write_data_files_from_stream_with_concurrency(&table, batches, one_writer).await?
            } else {
                write_partitioned_data_files_from_stream_with_concurrency(
                    &table, batches, one_writer,
                )
                .await?
            };
            let count = files.len() as u64;
            collected
                .lock()
                .map_err(|_| poisoned_collector())?
                .insert(partition, files);
            RecordBatch::try_new(
                result_schema,
                vec![Arc::new(UInt64Array::from(vec![count]))],
            )
            .map_err(DataFusionError::from)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.result_schema),
            written.boxed(),
        )))
    }
}

/// One writer per DataFusion partition; the files come back in a deterministic order.
/// # Errors
/// Returns the plan's execution error; every data file the attempt completed is deleted first.
pub async fn write_data_files_from_plan(
    table: &Table,
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>> {
    let inputs = input.output_partitioning().partition_count().max(1);
    let single = concurrency.max_concurrent_files <= 1;
    let (input, writers) = if single && inputs > 1 {
        (
            Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>,
            1,
        )
    } else if single {
        (input, 1)
    } else {
        (input, inputs)
    };
    let collected: FileCollector = Arc::new(Mutex::new(BTreeMap::new()));
    let exec = Arc::new(IcebergPartitionWriteExec::new(
        table.clone(),
        input,
        writers,
        Arc::clone(&collected),
    ));

    let mut results = execute_stream(exec, context)?;
    let mut first_error: Option<DataFusionError> = None;
    while let Some(item) = results.next().await {
        if let Err(error) = item
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    let files = drain_collected(&collected)?;
    match first_error {
        None => Ok(ascending_partition_order(files)),
        Some(error) => {
            delete_completed_files(table, &files).await;
            Err(error)
        }
    }
}

fn result_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![Field::new(
        WRITTEN_FILES_COL_NAME,
        DataType::UInt64,
        false,
    )]))
}

fn poisoned_collector() -> DataFusionError {
    DataFusionError::Execution("the Iceberg write file collector lock was poisoned".into())
}

fn drain_collected(collected: &FileCollector) -> Result<Vec<DataFile>> {
    let mut guard = collected.lock().map_err(|_| poisoned_collector())?;
    let mut files = Vec::new();
    for (_, part) in std::mem::take(&mut *guard) {
        files.extend(part);
    }
    Ok(files)
}

async fn delete_completed_files(table: &Table, files: &[DataFile]) {
    let file_io = table.file_io();
    for file in files {
        if let Err(error) = file_io.delete(file.file_path()).await {
            tracing::warn!(
                path = %file.file_path(),
                error = %error,
                "failed to delete a data file after a failed partition write"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
    use datafusion::execution::TaskContext;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;

    const BLOCKING_WRITE_MS: u64 = 120;
    const LATE_FAILURE_MS: u64 = 400;

    struct SlowPartitionedExec {
        schema: SchemaRef,
        rows_per_partition: usize,
        blocking_delay: Duration,
        fail_partition: Option<usize>,
        plan_properties: Arc<PlanProperties>,
    }

    impl SlowPartitionedExec {
        fn new(
            partitions: usize,
            rows_per_partition: usize,
            blocking_delay: Duration,
            fail_partition: Option<usize>,
        ) -> Self {
            let schema = source_schema();
            let plan_properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self {
                schema,
                rows_per_partition,
                blocking_delay,
                fail_partition,
                plan_properties,
            }
        }
    }

    impl Debug for SlowPartitionedExec {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.debug_struct("SlowPartitionedExec").finish()
        }
    }

    impl DisplayAs for SlowPartitionedExec {
        fn fmt_as(&self, _f: DisplayFormatType, formatter: &mut Formatter) -> std::fmt::Result {
            write!(formatter, "SlowPartitionedExec")
        }
    }

    impl ExecutionPlan for SlowPartitionedExec {
        fn name(&self) -> &'static str {
            "SlowPartitionedExec"
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
            let schema = Arc::clone(&self.schema);
            let emitted = Arc::clone(&self.schema);
            let rows = self.rows_per_partition * (partition + 1);
            let delay = self.blocking_delay;
            let offset = i32::try_from(partition * 1000).expect("row offset fits i32");

            if self.fail_partition == Some(partition) {
                let late = futures::stream::once(async move {
                    tokio::time::sleep(Duration::from_millis(LATE_FAILURE_MS)).await;
                    Err(DataFusionError::Execution(
                        "injected partition source failure".into(),
                    ))
                });
                return Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    late.boxed(),
                )));
            }

            let batch = futures::stream::once(async move {
                std::thread::sleep(delay);
                let ids = (0..rows)
                    .map(|row| offset + i32::try_from(row).expect("row fits i32"))
                    .collect::<Vec<_>>();
                let labels = ids
                    .iter()
                    .map(|id| Some(format!("r{id}")))
                    .collect::<Vec<_>>();
                RecordBatch::try_new(
                    emitted,
                    vec![
                        Arc::new(Int32Array::from(ids)),
                        Arc::new(StringArray::from(labels)),
                    ],
                )
                .map_err(DataFusionError::from)
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                batch.boxed(),
            )))
        }
    }

    fn source_schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            ArrowField::new("label", DataType::Utf8, true),
        ]))
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

    async fn create_table(catalog: &Arc<dyn Catalog>, name: &str) -> Table {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "label", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema");
        let namespace = NamespaceIdent::new("ns".into());
        let _ = catalog.create_namespace(&namespace, HashMap::new()).await;
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(schema)
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

    fn warehouse_data_files(warehouse: &TempDir) -> Vec<std::path::PathBuf> {
        fn walk(dir: &std::path::Path, found: &mut Vec<std::path::PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, found);
                } else if path.extension().is_some_and(|ext| ext == "parquet") {
                    found.push(path);
                }
            }
        }
        let mut found = Vec::new();
        walk(warehouse.path(), &mut found);
        found.sort();
        found
    }

    async fn parallel_write(
        catalog: &Arc<dyn Catalog>,
        name: &str,
        delay: Duration,
    ) -> Vec<DataFile> {
        let table = create_table(catalog, name).await;
        let input = Arc::new(SlowPartitionedExec::new(4, 8, delay, None));
        write_data_files_from_plan(
            &table,
            input,
            Arc::new(TaskContext::default()),
            WriteConcurrency::new(4).expect("concurrency"),
        )
        .await
        .expect("write succeeds")
    }

    async fn one_task_write(
        catalog: &Arc<dyn Catalog>,
        name: &str,
        delay: Duration,
    ) -> Vec<DataFile> {
        let table = create_table(catalog, name).await;
        let input: Arc<dyn ExecutionPlan> = Arc::new(SlowPartitionedExec::new(4, 8, delay, None));
        let context = Arc::new(TaskContext::default());
        let one_writer = WriteConcurrency::new(1).expect("concurrency");
        let mut files = Vec::new();
        for partition in 0..4 {
            let stream = input
                .execute(partition, Arc::clone(&context))
                .expect("execute");
            files.extend(
                write_data_files_from_stream_with_concurrency(&table, stream, one_writer)
                    .await
                    .expect("serial write succeeds"),
            );
        }
        files
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_input_partition_gets_its_own_writer_and_data_file() {
        let warehouse = TempDir::new().expect("warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let delay = Duration::from_millis(BLOCKING_WRITE_MS);

        let node = parallel_write(&catalog, "node", delay).await;
        let one_task = one_task_write(&catalog, "onetask", delay).await;

        assert_eq!(node.len(), 4, "one data file per DataFusion partition");
        assert_eq!(
            node.iter().map(DataFile::record_count).collect::<Vec<_>>(),
            vec![8, 16, 24, 32],
            "each writer holds exactly its own partition's rows, in writer-index order"
        );
        assert_eq!(
            one_task
                .iter()
                .map(DataFile::record_count)
                .collect::<Vec<_>>(),
            node.iter().map(DataFile::record_count).collect::<Vec<_>>(),
            "the one-task drive answers the same, only in one task"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn partition_order_of_the_returned_files_is_deterministic() {
        let warehouse = TempDir::new().expect("warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let mut runs = Vec::new();
        for run in 0..3 {
            let table = create_table(&catalog, &format!("determinism{run}")).await;
            let input = Arc::new(SlowPartitionedExec::new(4, 5, Duration::ZERO, None));
            let files = write_data_files_from_plan(
                &table,
                input,
                Arc::new(TaskContext::default()),
                WriteConcurrency::new(4).expect("concurrency"),
            )
            .await
            .expect("write succeeds");
            runs.push(files.iter().map(DataFile::record_count).collect::<Vec<_>>());
        }
        assert_eq!(runs[0], runs[1]);
        assert_eq!(runs[1], runs[2]);
        assert_eq!(
            runs[0],
            vec![5, 10, 15, 20],
            "the files come back in writer-index order, not completion order"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_partition_deletes_every_completed_data_file() {
        let warehouse = TempDir::new().expect("warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let table = create_table(&catalog, "abort").await;
        let input = Arc::new(SlowPartitionedExec::new(4, 6, Duration::ZERO, Some(2)));
        let error = write_data_files_from_plan(
            &table,
            input,
            Arc::new(TaskContext::default()),
            WriteConcurrency::new(4).expect("concurrency"),
        )
        .await
        .expect_err("the injected partition failure must surface");
        assert!(
            error
                .to_string()
                .contains("injected partition source failure"),
            "the source root cause must surface, got: {error}"
        );
        assert!(
            warehouse_data_files(&warehouse).is_empty(),
            "a failed write leaves no data file behind: {:?}",
            warehouse_data_files(&warehouse)
        );
    }
}
