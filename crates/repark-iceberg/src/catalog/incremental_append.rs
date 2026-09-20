use std::fmt::Debug;
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
use iceberg::arrow::{ArrowReaderBuilder, schema_to_arrow_schema};
use iceberg::expr::Predicate;
use iceberg::scan::IncrementalAppendScan;
use iceberg::table::Table;

use crate::catalog::iceberg_to_datafusion;
use crate::catalog::scan_batches::{conform_batch, iceberg_predicate_from_filters};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AppendWindow {
    pub from_exclusive: Option<i64>,
    pub to_inclusive: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct IncrementalAppendTableProvider {
    table: Table,
    window: AppendWindow,
    schema: SchemaRef,
}

impl IncrementalAppendTableProvider {
    #[allow(clippy::missing_errors_doc)]
    pub fn try_new(table: Table, window: AppendWindow) -> Result<Self> {
        build_scan(&table, window, None, None)?;
        let metadata = table.metadata();
        let schema = match window.to_inclusive {
            Some(to_snapshot_id) => metadata
                .snapshot_by_id(to_snapshot_id)
                .ok_or_else(|| {
                    iceberg_to_datafusion(iceberg::Error::new(
                        iceberg::ErrorKind::DataInvalid,
                        format!("Cannot find the end snapshot: {to_snapshot_id}"),
                    ))
                })?
                .schema(metadata)
                .map_err(iceberg_to_datafusion)?,
            None => match metadata.current_snapshot() {
                Some(snapshot) => snapshot.schema(metadata).map_err(iceberg_to_datafusion)?,
                None => Arc::clone(metadata.current_schema()),
            },
        };
        let arrow = schema_to_arrow_schema(&schema).map_err(iceberg_to_datafusion)?;
        Ok(Self {
            table,
            window,
            schema: Arc::new(arrow),
        })
    }
}

fn build_scan(
    table: &Table,
    window: AppendWindow,
    column_names: Option<Vec<String>>,
    filter: Option<Predicate>,
) -> Result<IncrementalAppendScan> {
    let mut builder = table.incremental_append_scan();
    if let Some(from_exclusive) = window.from_exclusive {
        builder = builder.from_snapshot_id_exclusive(from_exclusive);
    }
    if let Some(to_inclusive) = window.to_inclusive {
        builder = builder.to_snapshot_id(to_inclusive);
    }
    if let Some(names) = column_names {
        builder = builder.select(names);
    }
    if let Some(predicate) = filter {
        builder = builder.with_filter(predicate);
    }
    builder.build().map_err(iceberg_to_datafusion)
}

#[async_trait]
impl TableProvider for IncrementalAppendTableProvider {
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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let output_schema = match projection {
            None => Arc::clone(&self.schema),
            Some(indices) => Arc::new(self.schema.project(indices)?),
        };
        let column_names: Vec<String> = output_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let partition = Arc::new(AppendPartition {
            table: self.table.clone(),
            window: self.window,
            column_names,
            schema: Arc::clone(&output_schema),
            filter: iceberg_predicate_from_filters(filters),
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
struct AppendPartition {
    table: Table,
    window: AppendWindow,
    column_names: Vec<String>,
    schema: SchemaRef,
    filter: Option<Predicate>,
}

impl PartitionStream for AppendPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        let table = self.table.clone();
        let window = self.window;
        let column_names = self.column_names.clone();
        let schema = Arc::clone(&self.schema);
        let filter = self.filter.clone();
        let stream = futures::stream::once(async move {
            scan_append_batches(table, window, column_names, schema, filter).await
        })
        .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

async fn scan_append_batches(
    table: Table,
    window: AppendWindow,
    column_names: Vec<String>,
    schema: SchemaRef,
    filter: Option<Predicate>,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
    let scan = build_scan(&table, window, Some(column_names), filter)?;
    let tasks: Vec<iceberg::scan::FileScanTask> = scan
        .plan_files()
        .await
        .map_err(iceberg_to_datafusion)?
        .try_collect()
        .await
        .map_err(iceberg_to_datafusion)?;
    let task_stream: iceberg::scan::FileScanTaskStream =
        Box::pin(futures::stream::iter(tasks.into_iter().map(Ok)));
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
