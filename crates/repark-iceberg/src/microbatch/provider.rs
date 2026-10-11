use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::TryStreamExt;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::scan::FileScanTask;
use iceberg::spec::SchemaRef as IcebergSchemaRef;
use iceberg::table::Table;

use crate::catalog::scan_batches::conform_batch;
use crate::catalog::uuid_presentation::presented_arrow_schema;
use crate::iceberg_to_datafusion;
use crate::microbatch::window::WindowPlan;

#[derive(Debug, Clone)]
pub(crate) struct MicroBatchTableProvider {
    table: Table,
    files: Vec<FileScanTask>,
    schema: SchemaRef,
}

impl MicroBatchTableProvider {
    #[allow(clippy::missing_errors_doc)]
    pub(crate) fn try_new(
        table: Table,
        plan: &WindowPlan,
        read_schema: &IcebergSchemaRef,
    ) -> Result<Self> {
        let end = plan.end.snapshot.get();
        if table.metadata().snapshot_by_id(end).is_none() {
            return Err(iceberg_to_datafusion(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("Cannot find the end snapshot: {end}"),
            )));
        }
        let schema = Arc::clone(read_schema);
        let arrow = presented_arrow_schema(&schema).map_err(iceberg_to_datafusion)?;
        let field_ids: Arc<[i32]> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.id)
            .collect();
        let files = plan
            .files
            .iter()
            .map(|file| {
                let mut task = file.task.clone();
                task.schema = Arc::clone(&schema);
                task.project_field_ids = Arc::clone(&field_ids);
                task
            })
            .collect();
        Ok(Self {
            table,
            files,
            schema: Arc::new(arrow),
        })
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn provider_for_plan(
    table: Table,
    plan: &WindowPlan,
    read_schema: &IcebergSchemaRef,
) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(MicroBatchTableProvider::try_new(
        table,
        plan,
        read_schema,
    )?))
}

#[async_trait]
impl TableProvider for MicroBatchTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let output_schema = match projection {
            None => Arc::clone(&self.schema),
            Some(indices) => Arc::new(self.schema.project(indices)?),
        };
        let partition = Arc::new(MicroBatchPartition {
            table: self.table.clone(),
            files: self.files.clone(),
            schema: Arc::clone(&output_schema),
        });
        Ok(Arc::new(StreamingTableExec::try_new(
            output_schema,
            vec![partition],
            None,
            vec![],
            false,
            limit,
        )?))
    }
}

#[derive(Debug)]
struct MicroBatchPartition {
    table: Table,
    files: Vec<FileScanTask>,
    schema: SchemaRef,
}

impl PartitionStream for MicroBatchPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        let table = self.table.clone();
        let files = self.files.clone();
        let schema = Arc::clone(&self.schema);
        let stream =
            futures::stream::once(async move { read_batches(&table, files, schema) }).try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

fn read_batches(
    table: &Table,
    files: Vec<FileScanTask>,
    schema: SchemaRef,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
    let task_stream: iceberg::scan::FileScanTaskStream =
        Box::pin(futures::stream::iter(files.into_iter().map(Ok)));
    let inner = ArrowReaderBuilder::new(table.file_io().clone())
        .build()
        .read(task_stream)
        .map_err(iceberg_to_datafusion)?
        .map_err(iceberg_to_datafusion);
    let schema_for_map = Arc::clone(&schema);
    let mut projection: Option<(SchemaRef, Vec<usize>)> = None;
    let conformed = inner.and_then(move |batch| {
        futures::future::ready(conform_batch(&batch, &schema_for_map, &mut projection))
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, conformed)))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use iceberg::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType,
        Schema, Struct, Type,
    };
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{NamespaceIdent, TableCreation, TableIdent};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use tempfile::TempDir;

    use super::*;
    use crate::microbatch::offset::{FilePosition, InputOffset, SnapshotId, TableUuid};
    use crate::microbatch::window::{ReadCaps, StartPosition, WindowLimit, WindowPlanner};

    fn id_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("id schema")
    }

    fn write_int_parquet(path: &std::path::Path, ids: &[i32]) {
        let arrow_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "id",
                datafusion::arrow::datatypes::DataType::Int32,
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![Arc::new(Int32Array::from(ids.to_vec()))],
        )
        .expect("batch");
        let file = fs::File::create(path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn stored_file(path: &std::path::Path, ids: &[i32]) -> DataFile {
        write_int_parquet(path, ids);
        let size = fs::metadata(path).expect("stat").len();
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string_lossy().into_owned())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(u64::try_from(ids.len()).expect("short file"))
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .expect("data file")
    }

    #[tokio::test]
    async fn provider_reads_exactly_planned_files() {
        let warehouse = TempDir::new().expect("warehouse");
        let catalog = crate::memory_catalog(warehouse.path().to_str().expect("utf8"))
            .await
            .expect("catalog");
        catalog
            .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
            .await
            .expect("namespace");
        let ident = TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "reads".to_string(),
        );
        let table = catalog
            .create_table(
                ident.namespace(),
                TableCreation::builder()
                    .name("reads".to_string())
                    .schema(id_schema())
                    .build(),
            )
            .await
            .expect("create table");
        let data_dir = std::path::PathBuf::from(
            table
                .metadata()
                .location()
                .strip_prefix("file://")
                .unwrap_or(table.metadata().location()),
        )
        .join("data");
        fs::create_dir_all(&data_dir).expect("data dir");
        for (name, ids) in [("f1.parquet", vec![1, 2]), ("f2.parquet", vec![3])] {
            let file = stored_file(&data_dir.join(name), &ids);
            let head = catalog.load_table(&ident).await.expect("load table");
            let tx = Transaction::new(&head);
            let action = tx.fast_append().add_data_files(vec![file]);
            let tx = action.apply(tx).expect("apply append");
            tx.commit(catalog.as_ref()).await.expect("commit append");
        }
        let table = catalog.load_table(&ident).await.expect("load table");
        let planner = WindowPlanner::new(
            table.clone(),
            ReadCaps {
                max_files: None,
                max_rows: None,
            },
        );
        let from = planner
            .initial_offset(&StartPosition::Earliest)
            .await
            .expect("initial")
            .expect("some");
        let plan = planner
            .next_window(&from, WindowLimit::Capped)
            .await
            .expect("window")
            .expect("some");
        assert_eq!(plan.files.len(), 2);
        let read_schema = table.metadata().current_schema().clone();
        let provider =
            MicroBatchTableProvider::try_new(table, &plan, &read_schema).expect("provider");
        let ctx = SessionContext::new();
        ctx.register_table("reads", Arc::new(provider))
            .expect("register");
        let batches = ctx
            .sql("SELECT id FROM reads ORDER BY id")
            .await
            .expect("sql")
            .collect()
            .await
            .expect("collect");
        let mut got = Vec::new();
        for batch in &batches {
            let column = batch
                .column_by_name("id")
                .expect("id")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32");
            got.extend(column.values().iter().copied());
        }
        assert_eq!(got, vec![1, 2, 3]);
    }

    fn write_noted_parquet(path: &std::path::Path, ids: &[i32], notes: &[&str]) {
        let field = |name: &str, kind, id: &str| {
            datafusion::arrow::datatypes::Field::new(name, kind, true).with_metadata(HashMap::from(
                [(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())],
            ))
        };
        let arrow_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            field("id", datafusion::arrow::datatypes::DataType::Int32, "1").with_nullable(false),
            field("note", datafusion::arrow::datatypes::DataType::Utf8, "2"),
        ]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(notes.to_vec())),
            ],
        )
        .expect("batch");
        let file = fs::File::create(path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    async fn evolved_table() -> (TempDir, Table) {
        let warehouse = TempDir::new().expect("warehouse");
        let catalog = crate::memory_catalog(warehouse.path().to_str().expect("utf8"))
            .await
            .expect("catalog");
        catalog
            .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
            .await
            .expect("namespace");
        let ident = TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "evolve".to_string(),
        );
        let table = catalog
            .create_table(
                ident.namespace(),
                TableCreation::builder()
                    .name("evolve".to_string())
                    .schema(id_schema())
                    .build(),
            )
            .await
            .expect("create table");
        let data_dir = std::path::PathBuf::from(
            table
                .metadata()
                .location()
                .strip_prefix("file://")
                .unwrap_or(table.metadata().location()),
        )
        .join("data");
        fs::create_dir_all(&data_dir).expect("data dir");
        for (name, ids) in [("f1.parquet", vec![1, 2]), ("f2.parquet", vec![3])] {
            let file = stored_file(&data_dir.join(name), &ids);
            let head = catalog.load_table(&ident).await.expect("load table");
            let tx = Transaction::new(&head);
            let tx = tx
                .fast_append()
                .add_data_files(vec![file])
                .apply(tx)
                .expect("apply append");
            tx.commit(catalog.as_ref()).await.expect("commit append");
        }
        let head = catalog.load_table(&ident).await.expect("load table");
        let tx = Transaction::new(&head);
        let tx = tx
            .update_schema()
            .add_column("note", Type::Primitive(PrimitiveType::String))
            .apply(tx)
            .expect("apply add column");
        tx.commit(catalog.as_ref())
            .await
            .expect("commit add column");
        let path = data_dir.join("f3.parquet");
        write_noted_parquet(&path, &[4], &["x"]);
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string_lossy().into_owned())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(fs::metadata(&path).expect("stat").len())
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .expect("data file");
        let head = catalog.load_table(&ident).await.expect("load table");
        let tx = Transaction::new(&head);
        let tx = tx
            .fast_append()
            .add_data_files(vec![file])
            .apply(tx)
            .expect("apply append");
        tx.commit(catalog.as_ref()).await.expect("commit append");
        let table = catalog.load_table(&ident).await.expect("load table");
        (warehouse, table)
    }

    #[tokio::test]
    async fn provider_reads_older_files_under_the_read_schema_by_field_id() {
        let (_warehouse, table) = evolved_table().await;
        let planner = WindowPlanner::new(table.clone(), ReadCaps::default());
        let from = planner
            .initial_offset(&StartPosition::Earliest)
            .await
            .expect("initial")
            .expect("some");
        let plan = planner
            .next_window(&from, WindowLimit::Unbounded)
            .await
            .expect("window")
            .expect("some");
        assert_eq!(plan.files.len(), 3);
        let read_schema = table.metadata().current_schema().clone();
        let provider = provider_for_plan(table, &plan, &read_schema).expect("provider");
        let ctx = SessionContext::new();
        ctx.register_table("evolve", provider).expect("register");
        let batches = ctx
            .sql("SELECT id, note FROM evolve ORDER BY id")
            .await
            .expect("sql")
            .collect()
            .await
            .expect("collect");
        let mut got = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32");
            let notes = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8");
            for row in 0..batch.num_rows() {
                let note = (!notes.is_null(row)).then(|| notes.value(row).to_string());
                got.push((ids.value(row), note));
            }
        }
        assert_eq!(
            got,
            vec![
                (1, None),
                (2, None),
                (3, None),
                (4, Some(String::from("x")))
            ]
        );
    }

    #[tokio::test]
    async fn provider_refuses_unknown_end_snapshot() {
        let warehouse = TempDir::new().expect("warehouse");
        let catalog = crate::memory_catalog(warehouse.path().to_str().expect("utf8"))
            .await
            .expect("catalog");
        catalog
            .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
            .await
            .expect("namespace");
        let ident = TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "reads".to_string(),
        );
        let table = catalog
            .create_table(
                ident.namespace(),
                TableCreation::builder()
                    .name("reads".to_string())
                    .schema(id_schema())
                    .build(),
            )
            .await
            .expect("create table");
        let start = InputOffset {
            table: TableUuid::of(&table),
            table_name: table.identifier().to_string(),
            snapshot: SnapshotId::new(1),
            position: FilePosition::new(0),
        };
        let end = InputOffset {
            snapshot: SnapshotId::new(999),
            ..start.clone()
        };
        let plan = WindowPlan {
            start,
            end,
            files: Vec::new(),
            num_input_rows: 0,
        };
        let read_schema = table.metadata().current_schema().clone();
        let error =
            MicroBatchTableProvider::try_new(table, &plan, &read_schema).expect_err("must refuse");
        assert!(
            error
                .to_string()
                .contains("Cannot find the end snapshot: 999"),
            "unexpected message: {error}"
        );
    }

    async fn counted_table() -> (TempDir, Table) {
        let warehouse = TempDir::new().expect("warehouse");
        let catalog = crate::memory_catalog(warehouse.path().to_str().expect("utf8"))
            .await
            .expect("catalog");
        catalog
            .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
            .await
            .expect("namespace");
        let ident = TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "counts".to_string(),
        );
        let table = catalog
            .create_table(
                ident.namespace(),
                TableCreation::builder()
                    .name("counts".to_string())
                    .schema(id_schema())
                    .build(),
            )
            .await
            .expect("create table");
        let data_dir = std::path::PathBuf::from(
            table
                .metadata()
                .location()
                .strip_prefix("file://")
                .unwrap_or(table.metadata().location()),
        )
        .join("data");
        fs::create_dir_all(&data_dir).expect("data dir");
        for (name, ids) in [
            ("f1.parquet", vec![1, 2]),
            ("f2.parquet", Vec::new()),
            ("f3.parquet", vec![3]),
        ] {
            let file = stored_file(&data_dir.join(name), &ids);
            let head = catalog.load_table(&ident).await.expect("load table");
            let tx = Transaction::new(&head);
            let tx = tx
                .fast_append()
                .add_data_files(vec![file])
                .apply(tx)
                .expect("apply append");
            tx.commit(catalog.as_ref()).await.expect("commit append");
        }
        let table = catalog.load_table(&ident).await.expect("load table");
        (warehouse, table)
    }

    #[tokio::test]
    async fn provider_counts_rows_through_an_empty_projection() {
        let (_warehouse, table) = counted_table().await;
        let planner = WindowPlanner::new(table.clone(), ReadCaps::default());
        let from = planner
            .initial_offset(&StartPosition::Earliest)
            .await
            .expect("initial")
            .expect("some");
        let plan = planner
            .next_window(&from, WindowLimit::Unbounded)
            .await
            .expect("window")
            .expect("some");
        assert_eq!(plan.files.len(), 3);
        let read_schema = table.metadata().current_schema().clone();
        let provider = provider_for_plan(table, &plan, &read_schema).expect("provider");
        let ctx = SessionContext::new();
        ctx.register_table("counts", Arc::clone(&provider))
            .expect("register");
        let batches = ctx
            .sql("SELECT COUNT(*) AS total FROM counts")
            .await
            .expect("sql")
            .collect()
            .await
            .expect("collect");
        assert_eq!(batches.len(), 1);
        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64")
            .value(0);
        assert_eq!(total, 3);
        let listed = ctx
            .sql("SELECT id FROM counts")
            .await
            .expect("sql")
            .collect()
            .await
            .expect("collect");
        let listed_rows: usize = listed.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(listed_rows, 3);
        let state = ctx.state();
        let empty: Vec<usize> = Vec::new();
        let scan = provider
            .scan(&state, Some(&empty), &[], None)
            .await
            .expect("scan");
        let stream = scan.execute(0, ctx.task_ctx()).expect("execute");
        let projected: Vec<RecordBatch> = futures::TryStreamExt::try_collect(stream)
            .await
            .expect("stream");
        assert!(projected.iter().all(|batch| batch.num_columns() == 0));
        let rows: usize = projected.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 3);
    }
}
