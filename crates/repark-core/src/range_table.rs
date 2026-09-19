use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{Result, ScalarValue, exec_datafusion_err, plan_err};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::SessionContext;

use crate::illegal_argument_error;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SparkRangeFunc;

impl TableFunctionImpl for SparkRangeFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        if exprs.is_empty() || exprs.len() > 4 {
            return plan_err!("range function requires 1 to 4 arguments");
        }
        let mut bounds: Vec<i64> = Vec::with_capacity(3);
        for (index, expr) in exprs.iter().take(3).enumerate() {
            bounds.push(coerce_range_bound(index, expr)?);
        }
        let (start, end, step) = match bounds.as_slice() {
            [end] => (0, *end, 1),
            [start, end] => (*start, *end, 1),
            [start, end, step] => (*start, *end, *step),
            _ => return plan_err!("range function requires 1 to 4 arguments"),
        };
        if step == 0 {
            return plan_err!("Step cannot be zero");
        }
        let count = spark_range_count(start, end, step)?;
        if let Some(expr) = exprs.get(3) {
            let partitions = coerce_range_partitions(expr)?;
            if count > 0 && partitions <= 0 {
                return Err(illegal_argument_error(
                    "Positive number of partitions required".to_string(),
                ));
            }
        }
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        Ok(Arc::new(RangeTable {
            schema,
            start,
            step,
            count,
        }))
    }
}

fn coerce_range_bound(index: usize, expr: &Expr) -> Result<i64> {
    let Expr::Literal(scalar, _) = expr else {
        return plan_err!("Arguments must be literals");
    };
    spark_long_value(scalar, index + 1, "BIGINT")
}

fn coerce_range_partitions(expr: &Expr) -> Result<i32> {
    let Expr::Literal(scalar, _) = expr else {
        return plan_err!("Arguments must be literals");
    };
    let value = spark_long_value(scalar, 4, "INT")?;
    i32::try_from(value).map_err(|_| {
        DataFusionError::Plan(
            "[UNEXPECTED_INPUT_TYPE] range function argument #4 requires INT, got out-of-range value"
                .to_string(),
        )
    })
}

fn spark_long_value(scalar: &ScalarValue, position: usize, target: &str) -> Result<i64> {
    if scalar.is_null() {
        return plan_err!(
            "[UNEXPECTED_INPUT_TYPE] range function argument #{position} requires {target}, got {}",
            scalar.data_type()
        );
    }
    match scalar {
        ScalarValue::Int8(Some(value)) => Ok(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Ok(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => Ok(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Ok(*value),
        ScalarValue::UInt8(Some(value)) => Ok(i64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Ok(i64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Ok(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value).map_err(|_| {
            DataFusionError::Plan(format!(
                "range function argument #{position} must be an INTEGER, got {value}"
            ))
        }),
        ScalarValue::Float32(Some(value)) => spark_truncate_float(f64::from(*value), position),
        ScalarValue::Float64(Some(value)) => spark_truncate_float(*value, position),
        ScalarValue::Decimal128(Some(value), _, scale) => {
            spark_truncate_decimal(*value, *scale, position)
        }
        ScalarValue::Decimal256(Some(value), _, scale) => {
            if let Some(unscaled) = value.to_i128() {
                spark_truncate_decimal(unscaled, *scale, position)
            } else {
                plan_err!(
                    "range function argument #{position} must be an INTEGER, got out-of-range decimal"
                )
            }
        }
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => match text.trim().parse::<i64>() {
            Ok(value) => Ok(value),
            Err(_) => Err(exec_datafusion_err!(
                "[CAST_INVALID_INPUT] The value '{text}' of the type \"STRING\" cannot be cast to \"{target}\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018"
            )),
        },
        _ => plan_err!(
            "range function argument #{position} must be an INTEGER, got {:?}",
            scalar.data_type()
        ),
    }
}

const LOWEST_LONG_AS_FLOAT: f64 = -9_223_372_036_854_775_808.0;
const PAST_HIGHEST_LONG_AS_FLOAT: f64 = 9_223_372_036_854_775_808.0;

#[allow(clippy::cast_possible_truncation)]
fn spark_truncate_float(value: f64, position: usize) -> Result<i64> {
    if !value.is_finite() {
        return plan_err!("range function argument #{position} must be an INTEGER, got {value}");
    }
    let truncated = value.trunc();
    if !(LOWEST_LONG_AS_FLOAT..PAST_HIGHEST_LONG_AS_FLOAT).contains(&truncated) {
        return plan_err!("range function argument #{position} must be an INTEGER, got {value}");
    }
    Ok(truncated as i64)
}

fn spark_truncate_decimal(unscaled: i128, scale: i8, position: usize) -> Result<i64> {
    if unscaled == 0 {
        return Ok(0);
    }
    let out_of_range = || {
        DataFusionError::Plan(format!(
            "range function argument #{position} must be an INTEGER, got out-of-range decimal"
        ))
    };
    let magnitude = if scale >= 0 {
        let places = u32::try_from(scale).map_err(|_| out_of_range())?;
        let divisor = 10i128.checked_pow(places).ok_or_else(out_of_range)?;
        unscaled / divisor
    } else {
        let places = u32::try_from(scale.checked_neg().ok_or_else(out_of_range)?)
            .map_err(|_| out_of_range())?;
        let factor = 10i128.checked_pow(places).ok_or_else(out_of_range)?;
        unscaled.checked_mul(factor).ok_or_else(out_of_range)?
    };
    i64::try_from(magnitude).map_err(|_| out_of_range())
}

fn spark_range_count(start: i64, end: i64, step: i64) -> Result<u64> {
    let difference = i128::from(end) - i128::from(start);
    let stride = i128::from(step);
    if (stride > 0 && difference <= 0) || (stride < 0 && difference >= 0) {
        return Ok(0);
    }
    let count = difference.unsigned_abs().div_ceil(stride.unsigned_abs());
    u64::try_from(count)
        .map_err(|_| DataFusionError::Internal("range element count exceeds u64".to_string()))
}

#[derive(Debug)]
struct RangeTable {
    schema: SchemaRef,
    start: i64,
    step: i64,
    count: u64,
}

#[async_trait]
impl TableProvider for RangeTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let output_schema = match projection {
            None => Arc::clone(&self.schema),
            Some(indices) => Arc::new(self.schema.project(indices)?),
        };
        let include_id = projection.is_none_or(|indices| indices.contains(&0));
        let mut total = self.count;
        if let Some(fetch) = limit {
            let capped = u64::try_from(fetch)
                .map_err(|_| DataFusionError::Internal("range limit exceeds u64".to_string()))?;
            total = total.min(capped);
        }
        let batch_size = state.config_options().execution.batch_size.max(1);
        let ordering: Vec<LexOrdering> = if output_schema.fields().is_empty() {
            Vec::new()
        } else {
            LexOrdering::new(vec![PhysicalSortExpr::new(
                Arc::new(Column::new("id", 0)),
                SortOptions {
                    descending: self.step < 0,
                    nulls_first: false,
                },
            )])
            .into_iter()
            .collect()
        };
        let partition: Arc<dyn PartitionStream> = Arc::new(RangePartition {
            schema: Arc::clone(&output_schema),
            value: i128::from(self.start),
            step: i128::from(self.step),
            remaining: total,
            batch_size,
            include_id,
        });
        Ok(Arc::new(StreamingTableExec::try_new(
            output_schema,
            vec![partition],
            None,
            ordering,
            false,
            limit,
        )?))
    }
}

#[derive(Debug, Clone)]
struct RangePartition {
    schema: SchemaRef,
    value: i128,
    step: i128,
    remaining: u64,
    batch_size: usize,
    include_id: bool,
}

impl PartitionStream for RangePartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let batch_size = self.batch_size;
        let include_id = self.include_id;
        let cursor = (self.value, self.step, self.remaining);
        let output = futures::stream::unfold(cursor, move |(mut value, step, mut remaining)| {
            let schema = Arc::clone(&schema);
            async move {
                if remaining == 0 {
                    return None;
                }
                let width = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(batch_size);
                let mut values = Vec::with_capacity(width);
                for _ in 0..width {
                    match i64::try_from(value) {
                        Ok(next) => values.push(next),
                        Err(_) => {
                            return Some((
                                Err(DataFusionError::Internal(
                                    "range value exceeds int64".to_string(),
                                )),
                                (value, step, 0),
                            ));
                        }
                    }
                    value += step;
                    remaining -= 1;
                }
                let batch = if include_id {
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(Int64Array::from(values)) as ArrayRef],
                    )
                } else {
                    RecordBatch::try_new_with_options(
                        Arc::clone(&schema),
                        vec![],
                        &RecordBatchOptions::new().with_row_count(Some(width)),
                    )
                };
                match batch {
                    Ok(batch) => Some((Ok(batch), (value, step, remaining))),
                    Err(error) => Some((Err(DataFusionError::from(error)), (value, step, 0))),
                }
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            output,
        ))
    }
}

pub(crate) fn register_spark_range(context: &SessionContext) {
    context.register_udtf("range", Arc::new(SparkRangeFunc));
}

#[cfg(test)]
mod tests;
