use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::spark_time_window::{micros_to_timestamp_array, timestamp_micros_batch};

pub const WINDOW_TIME_FUNCTION_NAME: &str = "window_time";
const END_FIELD_NAME: &str = "end";

#[must_use]
pub fn window_time_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkWindowTime::new()))
}

fn end_field_type(arg_types: &[DataType]) -> Result<DataType> {
    let fields = match arg_types.first() {
        Some(DataType::Struct(fields)) => fields,
        Some(other) => {
            return exec_err!("'window_time' windowColumn must be a window struct, got {other}");
        }
        None => return exec_err!("'window_time' requires a window column"),
    };
    let Some(field) = fields.iter().find(|field| field.name() == END_FIELD_NAME) else {
        return exec_err!("'window_time' windowColumn must be a window struct with start and end");
    };
    match field.data_type() {
        typed @ DataType::Timestamp(_, _) => Ok(typed.clone()),
        other => exec_err!("'window_time' windowColumn end must be a TIMESTAMP, got {other}"),
    }
}

fn fallback_time_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

fn end_field_index(field: &Field) -> Result<usize> {
    match field.data_type() {
        DataType::Struct(fields) => fields
            .iter()
            .position(|child| child.name() == END_FIELD_NAME)
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "'window_time' windowColumn must be a window struct with start and end"
                        .to_string(),
                )
            }),
        other => Err(datafusion::common::DataFusionError::Execution(format!(
            "'window_time' windowColumn must be a window struct, got {other}"
        ))),
    }
}

fn window_time_batch(
    end_type: &DataType,
    end: &ArrayRef,
    structs: &StructArray,
) -> Result<ArrayRef> {
    let instants = timestamp_micros_batch(end_type, end).map_err(|_| {
        datafusion::common::DataFusionError::Execution(
            "'window_time' windowColumn end must be a TIMESTAMP".to_string(),
        )
    })?;
    let struct_nulls = structs.nulls();
    let end_nulls = end.nulls();
    let mut values: Vec<i64> = Vec::with_capacity(structs.len());
    let mut valid: Vec<bool> = Vec::with_capacity(structs.len());
    for (row, instant) in instants.iter().enumerate() {
        if struct_nulls.is_some_and(|nulls| nulls.is_null(row))
            || end_nulls.is_some_and(|nulls| nulls.is_null(row))
        {
            values.push(0);
            valid.push(false);
            continue;
        }
        values.push(instant.checked_sub(1).ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(
                "'window_time' window end is out of range".to_string(),
            )
        })?);
        valid.push(true);
    }
    let nulls: datafusion::arrow::buffer::NullBuffer = valid.iter().copied().collect();
    let nulls = if nulls.null_count() == 0 {
        None
    } else {
        Some(nulls)
    };
    micros_to_timestamp_array(end_type, values, nulls)
}

#[derive(Debug)]
struct SparkWindowTime {
    signature: Signature,
}

impl SparkWindowTime {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkWindowTime {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkWindowTime {}

impl Hash for SparkWindowTime {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkWindowTime {
    fn name(&self) -> &str {
        WINDOW_TIME_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(end_field_type(arg_types).unwrap_or_else(|_| fallback_time_type()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let end_type = match args.arg_fields.first().map(|field| field.data_type()) {
            Some(DataType::Struct(_)) => {
                let inputs: Vec<DataType> = args
                    .arg_fields
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect();
                end_field_type(&inputs).unwrap_or_else(|_| fallback_time_type())
            }
            _ => fallback_time_type(),
        };
        Ok(Arc::new(Field::new(self.name(), end_type, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!("'window_time' requires 1 argument, got {}", arg_types.len());
        }
        Ok(vec![
            end_field_type(arg_types).map(|_| arg_types[0].clone())?,
        ])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(field) = args.arg_fields.first() else {
            return exec_err!("'window_time' requires a window column");
        };
        let end_type = end_field_type(std::slice::from_ref(field.data_type()))?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(window) = arrays.first() else {
            return exec_err!("'window_time' requires a window column");
        };
        if matches!(
            args.args.first(),
            Some(ColumnarValue::Scalar(ScalarValue::Null))
        ) {
            return Ok(ColumnarValue::Array(micros_to_timestamp_array(
                &end_type,
                vec![0; args.number_rows],
                (args.number_rows > 0).then(|| {
                    std::iter::repeat_n(false, args.number_rows)
                        .collect::<datafusion::arrow::buffer::NullBuffer>()
                }),
            )?));
        }
        let structs = window
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "'window_time' windowColumn must be a window struct".to_string(),
                )
            })?;
        let index = end_field_index(field)?;
        let end = structs.column(index).clone();
        Ok(ColumnarValue::Array(window_time_batch(
            &end_type, &end, structs,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::TimestampNanosecondType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    fn ctx_two_partitions() -> SessionContext {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        crate::register_all(&ctx);
        ctx
    }

    async fn batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert!(!batches.is_empty(), "expected rows for {sql}");
        datafusion::arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap()
    }

    async fn sql_error(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => frame.collect().await.expect_err(sql).to_string(),
        }
    }

    fn frame_sql() -> String {
        "SELECT * FROM (VALUES (TIMESTAMP '2024-01-01 10:07:30'), (TIMESTAMP '2024-01-01 10:12:00'), \
         (TIMESTAMP '2024-01-01 10:31:00')) AS t(ts)"
            .to_string()
    }

    fn window_time_micros(batch: &RecordBatch) -> Vec<Option<i64>> {
        let column = batch.column(0);
        (0..column.len())
            .map(|row| {
                if column.is_null(row) {
                    None
                } else {
                    Some(match column.data_type() {
                        DataType::Timestamp(TimeUnit::Nanosecond, _) => column
                            .as_primitive::<TimestampNanosecondType>()
                            .value(row)
                            .div_euclid(1_000),
                        DataType::Timestamp(TimeUnit::Microsecond, _) => column
                            .as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>(
                            )
                            .value(row),
                        other => panic!("expected a timestamp result, got {other}"),
                    })
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn window_time_answers_end_minus_one_microsecond() {
        for ctx in [ctx(), ctx_two_partitions()] {
            let produced = batch(
                &ctx,
                &format!(
                    "SELECT window_time(window(ts, '10 minutes')) AS wt FROM ({}) ORDER BY ts",
                    frame_sql()
                ),
            )
            .await;
            assert!(matches!(
                produced.schema().field(0).data_type(),
                DataType::Timestamp(_, _)
            ));
            let base = 1_704_103_200_000_000_i64;
            assert_eq!(
                window_time_micros(&produced),
                vec![
                    Some(base + 600_000_000 - 1),
                    Some(base + 1_200_000_000 - 1),
                    Some(base + 2_400_000_000 - 1),
                ]
            );
        }
    }

    #[tokio::test]
    async fn null_window_is_null_time() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT window_time(window(CAST(NULL AS TIMESTAMP), '10 minutes')) AS wt",
        )
        .await;
        assert_eq!(window_time_micros(&produced), vec![None]);
    }

    #[tokio::test]
    async fn non_struct_argument_refuses() {
        let ctx = ctx();
        let message = sql_error(
            &ctx,
            "SELECT window_time(TIMESTAMP '2024-01-01 10:00:00') AS wt",
        )
        .await;
        assert!(
            message.contains("must be a window struct"),
            "expected the struct refusal, got {message}"
        );
    }
}
