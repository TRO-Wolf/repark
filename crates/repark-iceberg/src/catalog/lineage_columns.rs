//! Serve Iceberg v3 `_row_id` and `_last_updated_sequence_number` on read.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::expr::{Predicate, Reference};
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
    RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_ROW_ID,
};
use iceberg::spec::{Datum, FormatVersion};
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
///
/// Scans the table's current snapshot. Time-travel plus lineage is `V3-ROWID-2` (refused
/// at the SQL rewrite); a snapshot-pinned scan is the named follow-up, not this unit.
#[derive(Debug, Clone)]
pub struct LineageColumnsTableProvider {
    table: Table,
    schema: SchemaRef,
}

impl LineageColumnsTableProvider {
    /// Build a provider over `table`'s current snapshot.
    /// # Errors
    /// Arrow conversion of the Iceberg schema fails.
    pub fn try_new(table: Table) -> Result<Self> {
        let user = schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(iceberg_to_datafusion)?;
        let schema = append_lineage_fields(&user);
        Ok(Self {
            table,
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

fn iceberg_predicate_from_filters(filters: &[Expr]) -> Option<Predicate> {
    let converted: Vec<Predicate> = filters.iter().filter_map(equality_predicate).collect();
    converted.into_iter().reduce(Predicate::and)
}

fn equality_predicate(expr: &Expr) -> Option<Predicate> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    let (name, literal) = column_eq_literal(binary.left.as_ref(), binary.right.as_ref())
        .or_else(|| column_eq_literal(binary.right.as_ref(), binary.left.as_ref()))?;
    let datum = scalar_to_datum(literal)?;
    Some(Reference::new(name).equal_to(datum))
}

fn column_eq_literal<'a>(left: &'a Expr, right: &'a Expr) -> Option<(&'a str, &'a ScalarValue)> {
    match (left, right) {
        (Expr::Column(column), Expr::Literal(value, _)) => Some((column.name.as_str(), value)),
        _ => None,
    }
}

fn scalar_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Int32(Some(number)) => Some(Datum::int(*number)),
        ScalarValue::Int64(Some(number)) => Some(Datum::long(*number)),
        ScalarValue::Utf8(Some(text))
        | ScalarValue::Utf8View(Some(text))
        | ScalarValue::LargeUtf8(Some(text)) => Some(Datum::string(text)),
        ScalarValue::Boolean(Some(flag)) => Some(Datum::bool(*flag)),
        _ => None,
    }
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
        let partition = Arc::new(LineagePartition {
            table: self.table.clone(),
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
struct LineagePartition {
    table: Table,
    column_names: Vec<String>,
    schema: SchemaRef,
    filter: Option<Predicate>,
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
        let column_names = self.column_names.clone();
        let schema = Arc::clone(&self.schema);
        let filter = self.filter.clone();
        let stream = futures::stream::once(async move {
            scan_lineage_batches(table, column_names, schema, filter).await
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
    column_names: Vec<String>,
    schema: SchemaRef,
    filter: Option<Predicate>,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
    let mut builder = table.scan().select(column_names);
    if let Some(predicate) = filter {
        builder = builder.with_filter(predicate);
    }
    let table_scan = builder.build().map_err(iceberg_to_datafusion)?;
    let inner = table_scan
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
                    "lineage scan missing column '{}' in {:?}",
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
        columns.push(cast_with_options(
            column,
            field.data_type(),
            &CastOptions {
                safe: false,
                ..CastOptions::default()
            },
        )?);
    }
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| {
        DataFusionError::Internal(format!("lineage scan could not rebuild batch: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array};

    fn batch(values: Int32Array) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("_row_id", DataType::Int64, true),
        ]));
        let ids: ArrayRef = Arc::new(values);
        let row_ids: ArrayRef = Arc::new(Int64Array::from(vec![0_i64, 1]));
        RecordBatch::try_new(schema, vec![ids, row_ids]).expect("batch")
    }

    fn promoted_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("_row_id", DataType::Int64, true),
        ]))
    }

    /// V3-COV-2: a scan column left behind by a widening ALTER promotes instead of failing.
    #[test]
    fn conform_batch_promotes_a_narrower_scan_column_to_the_declared_type() {
        let source = batch(Int32Array::from(vec![7_i32, 8]));
        let schema = promoted_schema();
        let mut projection = None;
        let conformed = conform_batch(&source, &schema, &mut projection).expect("promotes");
        assert_eq!(conformed.schema(), schema);
        let ids = conformed
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        assert_eq!(ids.value(0), 7);
        assert_eq!(ids.value(1), 8);
    }

    /// The projection is resolved once and reused for every batch of one scan schema.
    #[test]
    fn conform_batch_reuses_the_projection_across_batches_of_one_schema() {
        let schema = promoted_schema();
        let mut projection = None;
        conform_batch(
            &batch(Int32Array::from(vec![1_i32, 2])),
            &schema,
            &mut projection,
        )
        .expect("first");
        let (_, indices) = projection.clone().expect("cached");
        assert_eq!(indices, vec![0, 1]);
        conform_batch(
            &batch(Int32Array::from(vec![3_i32, 4])),
            &schema,
            &mut projection,
        )
        .expect("second");
        assert_eq!(projection.expect("still cached").1, indices);
    }

    /// A scan that lost a lineage column names it rather than rebuilding a short batch.
    #[test]
    fn conform_batch_names_a_column_the_scan_did_not_return() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_last_updated_sequence_number",
            DataType::Int64,
            true,
        )]));
        let mut projection = None;
        let error = conform_batch(
            &batch(Int32Array::from(vec![1_i32, 2])),
            &schema,
            &mut projection,
        )
        .expect_err("missing column");
        assert!(
            error.to_string().contains("_last_updated_sequence_number"),
            "{error}"
        );
    }
}
