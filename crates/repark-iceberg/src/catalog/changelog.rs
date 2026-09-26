use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::TryStreamExt;
use iceberg::arrow::{ArrowReaderBuilder, ChangelogReader, changelog_arrow_schema};
use iceberg::expr::Predicate;
use iceberg::scan::IncrementalChangelogScan;
use iceberg::table::Table;

use crate::catalog::iceberg_to_datafusion;
use crate::catalog::scan_batches::{conform_batch, iceberg_predicate_from_filters};
use crate::catalog::uuid_presentation::presented_arrow_schema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChangelogWindow {
    pub from_exclusive: Option<i64>,
    pub to_inclusive: Option<i64>,
    pub empty: bool,
}

#[derive(Debug, Clone)]
pub struct ChangelogTableProvider {
    table: Table,
    window: ChangelogWindow,
    schema: SchemaRef,
}

impl ChangelogTableProvider {
    #[allow(clippy::missing_errors_doc)]
    pub fn try_new(table: Table, window: ChangelogWindow) -> Result<Self> {
        build_scan(&table, window)?;
        let metadata = table.metadata();
        let user = match window.to_inclusive {
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
        let arrow = presented_arrow_schema(&user).map_err(iceberg_to_datafusion)?;
        Ok(Self {
            table,
            window,
            schema: Arc::new(changelog_arrow_schema(&arrow)),
        })
    }
}

fn build_scan(table: &Table, window: ChangelogWindow) -> Result<IncrementalChangelogScan> {
    let mut builder = table.incremental_changelog_scan();
    if let Some(from_exclusive) = window.from_exclusive {
        builder = builder.from_snapshot_id_exclusive(from_exclusive);
    }
    if let Some(to_inclusive) = window.to_inclusive {
        builder = builder.to_snapshot_id(to_inclusive);
    }
    builder.build().map_err(iceberg_to_datafusion)
}

#[allow(clippy::missing_errors_doc)]
async fn probe_changelog_plan(table: &Table, window: ChangelogWindow) -> Result<()> {
    if window.empty {
        return Ok(());
    }
    let scan = build_scan(table, window)?;
    if let Err(error) = scan.plan_files().await
        && let Some(refusal) = surfaced_changelog_refusal(error)
    {
        return Err(refusal);
    }
    Ok(())
}

fn surfaced_changelog_refusal(error: iceberg::Error) -> Option<DataFusionError> {
    if error.kind() == iceberg::ErrorKind::FeatureUnsupported {
        Some(iceberg_to_datafusion(error))
    } else {
        None
    }
}

#[async_trait]
impl TableProvider for ChangelogTableProvider {
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
        probe_changelog_plan(&self.table, self.window).await?;
        let output_schema = match projection {
            None => Arc::clone(&self.schema),
            Some(indices) => Arc::new(self.schema.project(indices)?),
        };
        let partition = Arc::new(ChangelogPartition {
            table: self.table.clone(),
            window: self.window,
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
struct ChangelogPartition {
    table: Table,
    window: ChangelogWindow,
    schema: SchemaRef,
    filter: Option<Predicate>,
}

impl PartitionStream for ChangelogPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        let table = self.table.clone();
        let window = self.window;
        let schema = Arc::clone(&self.schema);
        let filter = self.filter.clone();
        let stream = futures::stream::once(async move {
            scan_changelog_batches(table, window, schema, filter).await
        })
        .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

async fn scan_changelog_batches(
    table: Table,
    window: ChangelogWindow,
    schema: SchemaRef,
    filter: Option<Predicate>,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
    if window.empty {
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::empty(),
        )));
    }
    let mut builder = table.incremental_changelog_scan();
    if let Some(from_exclusive) = window.from_exclusive {
        builder = builder.from_snapshot_id_exclusive(from_exclusive);
    }
    if let Some(to_inclusive) = window.to_inclusive {
        builder = builder.to_snapshot_id(to_inclusive);
    }
    if let Some(predicate) = filter {
        builder = builder.with_filter(predicate);
    }
    let scan = builder.build().map_err(iceberg_to_datafusion)?;
    let tasks = scan.plan_files().await.map_err(iceberg_to_datafusion)?;
    let inner = ChangelogReader::new(ArrowReaderBuilder::new(table.file_io().clone()).build())
        .read(tasks)
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
    use super::*;

    #[test]
    fn changelog_probe_surfaces_feature_unsupported() {
        let refusal = surfaced_changelog_refusal(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "Delete files are currently not supported in changelog scans",
        ));
        let Some(DataFusionError::External(inner)) = refusal else {
            panic!("expected a surfaced External refusal, got {refusal:?}");
        };
        let kept = inner
            .downcast_ref::<iceberg::Error>()
            .expect("expected the live iceberg error");
        assert_eq!(kept.kind(), iceberg::ErrorKind::FeatureUnsupported);
        assert_eq!(
            kept.to_string(),
            "FeatureUnsupported => Delete files are currently not supported in changelog scans"
        );
    }

    #[test]
    fn changelog_probe_ignores_other_plan_failures() {
        for kind in [
            iceberg::ErrorKind::DataInvalid,
            iceberg::ErrorKind::Unexpected,
        ] {
            let refusal = surfaced_changelog_refusal(iceberg::Error::new(kind, "probe"));
            assert!(refusal.is_none(), "expected {kind:?} to stay mid-stream");
        }
    }
}
