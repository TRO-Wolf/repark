use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::TryStreamExt;
use iceberg::arrow::type_to_arrow_type;
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_DELETED, RESERVED_COL_NAME_FILE,
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_PARTITION,
    RESERVED_COL_NAME_POS, RESERVED_COL_NAME_ROW_ID, RESERVED_COL_NAME_SPEC_ID,
    RESERVED_FIELD_ID_DELETED, RESERVED_FIELD_ID_FILE,
    RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_PARTITION,
    RESERVED_FIELD_ID_POS, RESERVED_FIELD_ID_ROW_ID, RESERVED_FIELD_ID_SPEC_ID,
};
use iceberg::spec::Type;
use iceberg::table::Table;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::catalog::iceberg_to_datafusion;
use crate::catalog::lineage_columns::{table_serves_row_lineage, user_field_names};
use crate::catalog::uuid_presentation::{convert_uuid_column, presented_arrow_schema};

pub const METADATA_COLUMN_NAMES: [&str; 5] = [
    RESERVED_COL_NAME_FILE,
    RESERVED_COL_NAME_POS,
    RESERVED_COL_NAME_SPEC_ID,
    RESERVED_COL_NAME_PARTITION,
    RESERVED_COL_NAME_DELETED,
];

#[must_use]
pub fn is_served_metadata_column(name: &str) -> bool {
    METADATA_COLUMN_NAMES.contains(&name)
}

#[derive(Debug, Clone)]
pub struct MetadataColumnsTableProvider {
    table: Table,
    schema: SchemaRef,
    user_field_count: usize,
}

impl MetadataColumnsTableProvider {
    #[allow(clippy::missing_errors_doc)]
    pub fn try_new(table: Table) -> Result<Self> {
        let user = presented_arrow_schema(table.metadata().current_schema())
            .map_err(iceberg_to_datafusion)?;
        let schema = Arc::new(append_metadata_fields(&user, &table)?);
        Ok(Self {
            table,
            schema,
            user_field_count: user.fields().len(),
        })
    }

    fn reserved_user_fields_read(&self, projection: Option<&Vec<usize>>) -> Vec<String> {
        let mut indices: Vec<usize> = match projection {
            None => (0..self.user_field_count).collect(),
            Some(indices) => indices.clone(),
        };
        indices.sort_unstable();
        indices.dedup();
        indices
            .into_iter()
            .filter(|index| *index < self.user_field_count)
            .map(|index| self.schema.field(index).name().clone())
            .filter(|name| is_served_metadata_column(name))
            .collect()
    }
}

fn refuse_reserved_name_collision(conflicting: &[String]) -> DataFusionError {
    DataFusionError::Plan(format!(
        "Table column names conflict with names reserved for Iceberg metadata columns: [{}]. \
         Please, use ALTER TABLE statements to rename the conflicting table columns.",
        conflicting.join(", ")
    ))
}

fn metadata_field(name: &str, data_type: DataType, field_id: i32, nullable: bool) -> Field {
    Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        field_id.to_string(),
    )]))
}

fn append_metadata_fields(user: &Schema, table: &Table) -> Result<Schema> {
    let partition_type = table
        .metadata()
        .unified_partition_type()
        .map_err(iceberg_to_datafusion)?;
    let partition_arrow =
        type_to_arrow_type(&Type::Struct(partition_type)).map_err(iceberg_to_datafusion)?;
    let mut reserved = vec![
        metadata_field(
            RESERVED_COL_NAME_FILE,
            DataType::Utf8,
            RESERVED_FIELD_ID_FILE,
            false,
        ),
        metadata_field(
            RESERVED_COL_NAME_POS,
            DataType::Int64,
            RESERVED_FIELD_ID_POS,
            false,
        ),
        metadata_field(
            RESERVED_COL_NAME_SPEC_ID,
            DataType::Int32,
            RESERVED_FIELD_ID_SPEC_ID,
            false,
        ),
        metadata_field(
            RESERVED_COL_NAME_PARTITION,
            partition_arrow,
            RESERVED_FIELD_ID_PARTITION,
            true,
        ),
        metadata_field(
            RESERVED_COL_NAME_DELETED,
            DataType::Boolean,
            RESERVED_FIELD_ID_DELETED,
            false,
        ),
    ];
    if table_serves_row_lineage(table) {
        reserved.push(metadata_field(
            RESERVED_COL_NAME_ROW_ID,
            DataType::Int64,
            RESERVED_FIELD_ID_ROW_ID,
            true,
        ));
        reserved.push(metadata_field(
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            DataType::Int64,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            true,
        ));
    }
    let fields: Vec<Field> = user
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .chain(
            reserved
                .into_iter()
                .filter(|field| user.field_with_name(field.name()).is_err()),
        )
        .collect();
    Ok(Schema::new(fields))
}

#[must_use]
pub fn metadata_columns_user_field_names(table: &Table) -> Vec<String> {
    user_field_names(table)
}

#[async_trait]
impl TableProvider for MetadataColumnsTableProvider {
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
        let conflicting = self.reserved_user_fields_read(projection);
        if !conflicting.is_empty() {
            return Err(refuse_reserved_name_collision(&conflicting));
        }
        let output_schema = match projection {
            None => Arc::clone(&self.schema),
            Some(indices) => Arc::new(self.schema.project(indices)?),
        };
        let column_names: Vec<String> = output_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let partition = Arc::new(MetadataColumnsPartition {
            table: self.table.clone(),
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
struct MetadataColumnsPartition {
    table: Table,
    column_names: Vec<String>,
    schema: SchemaRef,
}

impl PartitionStream for MetadataColumnsPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        let table = self.table.clone();
        let column_names = self.column_names.clone();
        let schema = Arc::clone(&self.schema);
        let stream = futures::stream::once(async move {
            scan_metadata_batches(table, column_names, schema).await
        })
        .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

async fn scan_metadata_batches(
    table: Table,
    column_names: Vec<String>,
    schema: SchemaRef,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
    let inner = table
        .scan()
        .select(column_names)
        .project_current_schema()
        .build()
        .map_err(iceberg_to_datafusion)?
        .to_arrow()
        .await
        .map_err(iceberg_to_datafusion)?
        .map_err(iceberg_to_datafusion);
    let schema_for_map = Arc::clone(&schema);
    let mut projection: Option<(SchemaRef, Vec<usize>)> = None;
    let conformed = inner.and_then(move |batch| {
        futures::future::ready(conform_batch(&batch, &schema_for_map, &mut projection))
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, conformed)))
}

fn resolve_projection(batch: &RecordBatch, schema: &SchemaRef) -> Result<Vec<usize>> {
    let batch_schema = batch.schema();
    let by_name: HashMap<&str, usize> = batch_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name().as_str(), index))
        .collect();
    schema
        .fields()
        .iter()
        .map(|field| {
            by_name.get(field.name().as_str()).copied().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "metadata-column scan missing column '{}' in {:?}",
                    field.name(),
                    batch.schema()
                ))
            })
        })
        .collect()
}

fn conform_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    projection: &mut Option<(SchemaRef, Vec<usize>)>,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        return RecordBatch::try_new_with_options(
            Arc::clone(schema),
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|error| {
            DataFusionError::Internal(format!(
                "metadata-column scan could not rebuild batch: {error}"
            ))
        });
    }
    let batch_schema = batch.schema();
    let cached = match projection {
        Some((cached_schema, indices)) if Arc::ptr_eq(cached_schema, &batch_schema) => indices,
        _ => {
            let indices = resolve_projection(batch, schema)?;
            &projection.insert((Arc::clone(&batch_schema), indices)).1
        }
    };
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (field, index) in schema.fields().iter().zip(cached.iter()) {
        let column = batch.column(*index);
        if column.data_type() == field.data_type() {
            columns.push(Arc::clone(column));
            continue;
        }
        columns.push(convert_uuid_column(column, field.data_type())?);
    }
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| {
        DataFusionError::Internal(format!(
            "metadata-column scan could not rebuild batch: {error}"
        ))
    })
}
