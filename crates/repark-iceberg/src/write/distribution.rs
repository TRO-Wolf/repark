use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use futures::{Stream, TryStreamExt};
use iceberg::arrow::type_to_arrow_type;
use iceberg::spec::{DataFile, NullOrder, SortDirection, Transform};
use iceberg::table::Table;
use iceberg::transform::create_transform_function;
use iceberg::writer::IcebergWriter;

use crate::write::merge::iceberg_err;
use crate::write::sort_order::DISTRIBUTION_MODE_PROPERTY;

mod router;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use router::PartitionRouter;
pub(crate) use router::{route_partitioned_stream, send_routed};

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
    if distribution_is_none(table)? {
        return Ok(input);
    }
    let exprs = partition_value_exprs(table, input.schema().as_ref())?;
    Ok(Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Hash(exprs, writers),
    )?))
}

pub(crate) fn distribution_is_none(table: &Table) -> Result<bool> {
    let mode = table
        .metadata()
        .properties()
        .get(DISTRIBUTION_MODE_PROPERTY)
        .map(|mode| mode.to_ascii_lowercase());
    match mode.as_deref() {
        None | Some("hash" | "range") => Ok(false),
        Some("none") => Ok(true),
        Some(other) => Err(DataFusionError::Plan(format!(
            "write.distribution-mode '{other}' is not supported — use none, hash, or range"
        ))),
    }
}

pub(crate) fn default_sort_is_declared(table: &Table) -> bool {
    !table.metadata().default_sort_order().is_unsorted()
}

pub(crate) async fn sort_batches_by_default_order(
    table: &Table,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let schema = first.schema();
    let Some(ordering) = default_sort_lex_ordering(table, schema.as_ref())? else {
        return Ok(batches);
    };
    let memory = MemorySourceConfig::try_new_exec(&[batches], schema, None)?;
    let sort = SortExec::new(ordering, memory);
    let mut stream = sort.execute(0, Arc::new(TaskContext::default()))?;
    let mut sorted = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        sorted.push(batch);
    }
    Ok(sorted)
}

fn default_sort_lex_ordering(table: &Table, schema: &Schema) -> Result<Option<LexOrdering>> {
    let iceberg_schema = table.metadata().current_schema();
    let mut exprs = Vec::new();
    for field in &table.metadata().default_sort_order().fields {
        if field.transform != Transform::Identity {
            return Err(DataFusionError::NotImplemented(format!(
                "sorting by the table's default sort order uses transform `{}` on source id {}, \
                 only identity sort fields are supported",
                field.transform, field.source_id
            )));
        }
        let source = iceberg_schema.field_by_id(field.source_id).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "the table's default sort order names source field id {}, which the table \
                     schema lacks",
                field.source_id
            ))
        })?;
        let index = schema.index_of(&source.name)?;
        exprs.push(PhysicalSortExpr {
            expr: Arc::new(Column::new(&source.name, index)),
            options: datafusion::arrow::compute::SortOptions {
                descending: field.direction == SortDirection::Descending,
                nulls_first: field.null_order == NullOrder::First,
            },
        });
    }
    Ok(LexOrdering::new(exprs))
}

pub(crate) async fn fanout_sorted_serial<S>(
    table: &Table,
    conformed: &mut S,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    if !default_sort_is_declared(table) {
        return super::append::fanout_conformed_stream_serial(table, conformed).await;
    }
    let mut collected = Vec::new();
    while let Some(batch) = conformed.try_next().await? {
        collected.push(batch);
    }
    let sorted = sort_batches_by_default_order(table, collected).await?;
    let mut ordered = futures::stream::iter(sorted.into_iter().map(Ok::<_, DataFusionError>));
    super::append::fanout_conformed_stream_serial(table, &mut ordered).await
}

pub(crate) async fn fanout_sorted_stream<S>(
    table: &Table,
    stream: S,
    aborted: Arc<AtomicBool>,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let mut stream = stream;
    if !default_sort_is_declared(table) {
        return super::append::fanout_conformed_stream_serial_with_abort(
            table,
            &mut stream,
            &aborted,
        )
        .await;
    }
    let mut collected = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        collected.push(batch);
    }
    let sorted = sort_batches_by_default_order(table, collected).await?;
    let mut ordered = futures::stream::iter(sorted.into_iter().map(Ok::<_, DataFusionError>));
    super::append::fanout_conformed_stream_serial_with_abort(table, &mut ordered, &aborted).await
}

pub(crate) async fn drive_unpartitioned<S, F, Fut, W>(
    table: &Table,
    stream: S,
    max_concurrent: usize,
    mut make_writer: F,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<W>>,
    W: IcebergWriter + Send + 'static,
{
    if !default_sort_is_declared(table) {
        if max_concurrent == 1 {
            let writer = make_writer().await?;
            let sink = super::merge::ForkBatchWriter { inner: writer };
            return super::merge::write_stream_into(sink, stream).await;
        }
        return super::merge::write_stream_into_parallel(max_concurrent, stream, make_writer).await;
    }
    let mut collected = Vec::new();
    let mut stream = stream;
    while let Some(batch) = stream.try_next().await? {
        collected.push(batch);
    }
    let sorted = sort_batches_by_default_order(table, collected).await?;
    if sorted.is_empty() {
        return Ok(Vec::new());
    }
    let writers = max_concurrent.max(1).min(sorted.len());
    let chunk = sorted.len().div_ceil(writers);
    let mut files = Vec::new();
    for part in sorted.chunks(chunk) {
        let writer = make_writer().await?;
        let sink = super::merge::ForkBatchWriter { inner: writer };
        let batches = part.iter().cloned().map(Ok::<_, DataFusionError>);
        files.extend(super::merge::write_stream_into(sink, futures::stream::iter(batches)).await?);
    }
    Ok(files)
}
