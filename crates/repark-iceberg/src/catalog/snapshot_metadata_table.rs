use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::inspect::{
    EntriesTable, FilesTable, ManifestsTable, MetadataTableType, PartitionsTable,
    PositionDeletesTable,
};
use iceberg::table::Table;

use crate::catalog::iceberg_to_datafusion;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataAsofMode {
    ServeCurrent,
    ServeSnapshot,
    RefuseSnapshotScope,
}

#[must_use]
pub fn metadata_asof_mode(metadata_type: &MetadataTableType) -> MetadataAsofMode {
    match metadata_type {
        MetadataTableType::Snapshots
        | MetadataTableType::History
        | MetadataTableType::Refs
        | MetadataTableType::MetadataLogEntries => MetadataAsofMode::ServeCurrent,
        MetadataTableType::AllManifests
        | MetadataTableType::AllFiles
        | MetadataTableType::AllDataFiles
        | MetadataTableType::AllDeleteFiles
        | MetadataTableType::AllEntries => MetadataAsofMode::RefuseSnapshotScope,
        MetadataTableType::Manifests
        | MetadataTableType::Files
        | MetadataTableType::DataFiles
        | MetadataTableType::DeleteFiles
        | MetadataTableType::Entries
        | MetadataTableType::Partitions
        | MetadataTableType::PositionDeletes => MetadataAsofMode::ServeSnapshot,
    }
}

#[must_use]
pub fn as_of_snapshot_scope_refusal(suffix: &str) -> Option<String> {
    let metadata_type = MetadataTableType::try_from(suffix).ok()?;
    (metadata_asof_mode(&metadata_type) == MetadataAsofMode::RefuseSnapshotScope)
        .then(|| snapshot_scope_refusal_text(&metadata_type))
}

#[must_use]
pub fn snapshot_scope_refusal_text(metadata_type: &MetadataTableType) -> String {
    format!(
        "Cannot select snapshot in table: {}",
        metadata_type.as_str().to_ascii_uppercase()
    )
}

#[derive(Debug, Clone)]
pub struct SnapshotMetadataTableProvider {
    table: Table,
    metadata_type: MetadataTableType,
    snapshot_id: Option<i64>,
    serve_empty: bool,
    schema: SchemaRef,
}

impl SnapshotMetadataTableProvider {
    #[allow(clippy::missing_errors_doc)]
    pub fn try_new_current(table: Table, metadata_type: MetadataTableType) -> Result<Self> {
        if metadata_asof_mode(&metadata_type) == MetadataAsofMode::RefuseSnapshotScope {
            return Err(DataFusionError::Plan(snapshot_scope_refusal_text(
                &metadata_type,
            )));
        }
        let schema = unpinned_arrow_schema(&table, &metadata_type)?;
        Ok(Self {
            table,
            metadata_type,
            snapshot_id: None,
            serve_empty: false,
            schema,
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn try_new_scoped(
        table: Table,
        metadata_type: MetadataTableType,
        snapshot_id: i64,
    ) -> Result<Self> {
        if metadata_asof_mode(&metadata_type) == MetadataAsofMode::RefuseSnapshotScope {
            return Err(DataFusionError::Plan(snapshot_scope_refusal_text(
                &metadata_type,
            )));
        }
        let schema = unpinned_arrow_schema(&table, &metadata_type)?;
        let snapshot_id = if metadata_asof_mode(&metadata_type) == MetadataAsofMode::ServeCurrent {
            None
        } else {
            Some(snapshot_id)
        };
        Ok(Self {
            table,
            metadata_type,
            snapshot_id,
            serve_empty: false,
            schema,
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn try_new_empty(table: Table, metadata_type: MetadataTableType) -> Result<Self> {
        if metadata_asof_mode(&metadata_type) == MetadataAsofMode::RefuseSnapshotScope {
            return Err(DataFusionError::Plan(snapshot_scope_refusal_text(
                &metadata_type,
            )));
        }
        let schema = unpinned_arrow_schema(&table, &metadata_type)?;
        Ok(Self {
            table,
            metadata_type,
            snapshot_id: None,
            serve_empty: true,
            schema,
        })
    }
}

fn unpinned_arrow_schema(table: &Table, metadata_type: &MetadataTableType) -> Result<SchemaRef> {
    let inspected = table.inspect();
    let schema = match metadata_type {
        MetadataTableType::Snapshots => inspected.snapshots().schema(),
        MetadataTableType::Manifests => inspected.manifests().schema(),
        MetadataTableType::Files => FilesTable::try_all(table)
            .map_err(iceberg_to_datafusion)?
            .schema(),
        MetadataTableType::DataFiles => FilesTable::try_data(table)
            .map_err(iceberg_to_datafusion)?
            .schema(),
        MetadataTableType::DeleteFiles => FilesTable::try_deletes(table)
            .map_err(iceberg_to_datafusion)?
            .schema(),
        MetadataTableType::Entries => EntriesTable::try_new(table)
            .map_err(iceberg_to_datafusion)?
            .schema(),
        MetadataTableType::AllFiles => FilesTable::try_all_files(table)
            .map_err(iceberg_to_datafusion)?
            .schema(),
        MetadataTableType::AllDataFiles => FilesTable::try_all_data_files(table)
            .map_err(iceberg_to_datafusion)?
            .schema(),
        MetadataTableType::AllDeleteFiles => FilesTable::try_all_delete_files(table)
            .map_err(iceberg_to_datafusion)?
            .schema(),
        MetadataTableType::AllEntries => EntriesTable::try_all(table)
            .map_err(iceberg_to_datafusion)?
            .schema(),
        MetadataTableType::History => inspected.history().schema(),
        MetadataTableType::Refs => inspected.refs().schema(),
        MetadataTableType::MetadataLogEntries => inspected.metadata_log_entries().schema(),
        MetadataTableType::Partitions => PartitionsTable::try_new(table)
            .map_err(iceberg_to_datafusion)?
            .schema(),
        MetadataTableType::AllManifests => inspected.all_manifests().schema(),
        MetadataTableType::PositionDeletes => PositionDeletesTable::try_new(table)
            .map_err(iceberg_to_datafusion)?
            .schema(),
    };
    Ok(Arc::new(
        schema_to_arrow_schema(&schema).map_err(iceberg_to_datafusion)?,
    ))
}

#[async_trait]
impl TableProvider for SnapshotMetadataTableProvider {
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
        let full: Vec<usize> = (0..self.schema.fields().len()).collect();
        let partition = Arc::new(SnapshotMetadataPartition {
            table: self.table.clone(),
            metadata_type: self.metadata_type.clone(),
            snapshot_id: self.snapshot_id,
            serve_empty: self.serve_empty,
            projection: projection.cloned().unwrap_or(full),
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
struct SnapshotMetadataPartition {
    table: Table,
    metadata_type: MetadataTableType,
    snapshot_id: Option<i64>,
    serve_empty: bool,
    projection: Vec<usize>,
    schema: SchemaRef,
}

impl PartitionStream for SnapshotMetadataPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        let table = self.table.clone();
        let metadata_type = self.metadata_type.clone();
        let snapshot_id = self.snapshot_id;
        let serve_empty = self.serve_empty;
        let projection = self.projection.clone();
        let schema = Arc::clone(&self.schema);
        let stream = futures::stream::once(async move {
            scan_snapshot_metadata(
                table,
                metadata_type,
                snapshot_id,
                serve_empty,
                projection,
                schema,
            )
            .await
        })
        .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

async fn scan_snapshot_metadata(
    table: Table,
    metadata_type: MetadataTableType,
    snapshot_id: Option<i64>,
    serve_empty: bool,
    projection: Vec<usize>,
    schema: SchemaRef,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
    if serve_empty {
        let empty = futures::stream::empty::<Result<RecordBatch>>();
        return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, empty)));
    }
    let inner = scan_inspect_stream(&table, &metadata_type, snapshot_id)
        .await?
        .map_err(iceberg_to_datafusion);
    let projected = inner.and_then(move |batch| {
        futures::future::ready(batch.project(&projection).map_err(DataFusionError::from))
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, projected)))
}

async fn scan_inspect_stream(
    table: &Table,
    metadata_type: &MetadataTableType,
    snapshot_id: Option<i64>,
) -> Result<iceberg::scan::ArrowRecordBatchStream> {
    let inspected = table.inspect();
    match metadata_type {
        MetadataTableType::Snapshots => inspected
            .snapshots()
            .scan()
            .await
            .map_err(iceberg_to_datafusion),
        MetadataTableType::History => inspected
            .history()
            .scan()
            .await
            .map_err(iceberg_to_datafusion),
        MetadataTableType::Refs => inspected.refs().scan().await.map_err(iceberg_to_datafusion),
        MetadataTableType::MetadataLogEntries => inspected
            .metadata_log_entries()
            .scan()
            .await
            .map_err(iceberg_to_datafusion),
        MetadataTableType::Manifests
        | MetadataTableType::Files
        | MetadataTableType::DataFiles
        | MetadataTableType::DeleteFiles
        | MetadataTableType::Entries
        | MetadataTableType::Partitions
        | MetadataTableType::PositionDeletes => {
            scan_scoped_stream(table, metadata_type, snapshot_id).await
        }
        MetadataTableType::AllManifests
        | MetadataTableType::AllFiles
        | MetadataTableType::AllDataFiles
        | MetadataTableType::AllDeleteFiles
        | MetadataTableType::AllEntries => Err(DataFusionError::Plan(snapshot_scope_refusal_text(
            metadata_type,
        ))),
    }
}

async fn scan_scoped_stream(
    table: &Table,
    metadata_type: &MetadataTableType,
    snapshot_id: Option<i64>,
) -> Result<iceberg::scan::ArrowRecordBatchStream> {
    let inspected = table.inspect();
    match metadata_type {
        MetadataTableType::Manifests => match snapshot_id {
            Some(id) => ManifestsTable::at_snapshot(table, id)
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
            None => inspected
                .manifests()
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
        },
        MetadataTableType::Files => match snapshot_id {
            Some(id) => FilesTable::try_all_at_snapshot(table, id)
                .map_err(iceberg_to_datafusion)?
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
            None => inspected
                .files()
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
        },
        MetadataTableType::DataFiles => match snapshot_id {
            Some(id) => FilesTable::try_data_at_snapshot(table, id)
                .map_err(iceberg_to_datafusion)?
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
            None => inspected
                .data_files()
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
        },
        MetadataTableType::DeleteFiles => match snapshot_id {
            Some(id) => FilesTable::try_deletes_at_snapshot(table, id)
                .map_err(iceberg_to_datafusion)?
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
            None => inspected
                .delete_files()
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
        },
        MetadataTableType::Entries => match snapshot_id {
            Some(id) => EntriesTable::try_at_snapshot(table, id)
                .map_err(iceberg_to_datafusion)?
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
            None => inspected
                .entries()
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
        },
        MetadataTableType::Partitions => match snapshot_id {
            Some(id) => PartitionsTable::try_at_snapshot(table, id)
                .map_err(iceberg_to_datafusion)?
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
            None => inspected
                .partitions()
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
        },
        MetadataTableType::PositionDeletes => match snapshot_id {
            Some(id) => PositionDeletesTable::try_at_snapshot(table, id)
                .map_err(iceberg_to_datafusion)?
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
            None => inspected
                .position_deletes()
                .scan()
                .await
                .map_err(iceberg_to_datafusion),
        },
        _ => Err(DataFusionError::Plan(snapshot_scope_refusal_text(
            metadata_type,
        ))),
    }
}
