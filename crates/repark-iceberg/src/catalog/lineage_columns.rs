//! Serve Iceberg v3 `_row_id` and `_last_updated_sequence_number` on read.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
    RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_ROW_ID,
};
use iceberg::spec::FormatVersion;
use iceberg::table::Table;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::catalog::iceberg_to_datafusion;

/// Whether `table` is format-v3 and must serve the two lineage metadata columns.
#[must_use]
pub fn table_serves_row_lineage(table: &Table) -> bool {
    table.metadata().format_version() >= FormatVersion::V3
}

/// User-schema field names in Iceberg order (not including lineage metadata columns).
#[must_use]
pub fn user_field_names(table: &Table) -> Vec<String> {
    table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect()
}

/// Read-only provider that advertises user columns plus the two v3 lineage metadata columns.
#[derive(Debug, Clone)]
pub struct LineageColumnsTableProvider {
    table: Table,
    snapshot_id: Option<i64>,
    schema: SchemaRef,
}

impl LineageColumnsTableProvider {
    /// Build a provider over `table`'s current snapshot.
    /// # Errors
    /// Arrow conversion of the Iceberg schema fails.
    pub fn try_new(table: Table) -> Result<Self> {
        Self::try_new_with_snapshot(table, None)
    }

    /// Build a provider pinned to `snapshot_id` when `Some`.
    /// # Errors
    /// Arrow conversion of the Iceberg schema fails.
    pub fn try_new_with_snapshot(table: Table, snapshot_id: Option<i64>) -> Result<Self> {
        let user = schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(iceberg_to_datafusion)?;
        let schema = append_lineage_fields(&user);
        Ok(Self {
            table,
            snapshot_id,
            schema: Arc::new(schema),
        })
    }
}

fn append_lineage_fields(user: &Schema) -> Schema {
    let mut fields: Vec<Field> = user
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(lineage_arrow_field(
        RESERVED_COL_NAME_ROW_ID,
        RESERVED_FIELD_ID_ROW_ID,
    ));
    fields.push(lineage_arrow_field(
        RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
        RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ));
    Schema::new(fields)
}

fn lineage_arrow_field(name: &'static str, field_id: i32) -> Field {
    Field::new(name, DataType::Int64, true).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        field_id.to_string(),
    )]))
}

#[async_trait]
impl TableProvider for LineageColumnsTableProvider {
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
        let column_names: Vec<String> = output_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let partition = Arc::new(LineagePartition {
            table: self.table.clone(),
            snapshot_id: self.snapshot_id,
            column_names,
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
struct LineagePartition {
    table: Table,
    snapshot_id: Option<i64>,
    column_names: Vec<String>,
    schema: SchemaRef,
}

impl PartitionStream for LineagePartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        let table = self.table.clone();
        let snapshot_id = self.snapshot_id;
        let column_names = self.column_names.clone();
        let schema = Arc::clone(&self.schema);
        let stream = futures::stream::once(async move {
            scan_lineage_batches(table, snapshot_id, column_names, schema).await
        })
        .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

async fn scan_lineage_batches(
    table: Table,
    snapshot_id: Option<i64>,
    column_names: Vec<String>,
    schema: SchemaRef,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
    let builder = match snapshot_id {
        Some(id) => table.scan().snapshot_id(id),
        None => table.scan(),
    };
    let table_scan = builder
        .select(column_names)
        .build()
        .map_err(iceberg_to_datafusion)?;
    let inner = table_scan
        .to_arrow()
        .await
        .map_err(iceberg_to_datafusion)?
        .map_err(iceberg_to_datafusion);
    let schema_for_map = Arc::clone(&schema);
    let conformed =
        inner.and_then(move |batch| futures::future::ready(conform_batch(&batch, &schema_for_map)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, conformed)))
}

fn conform_batch(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column = batch.column_by_name(field.name()).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "lineage scan missing column '{}' in {:?}",
                field.name(),
                batch.schema()
            ))
        })?;
        columns.push(column);
    }
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| {
        DataFusionError::Internal(format!("lineage scan could not rebuild batch: {error}"))
    })
}
