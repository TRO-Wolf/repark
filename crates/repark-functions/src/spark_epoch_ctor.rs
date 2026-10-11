use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, TimestampMicrosecondArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    Field, FieldRef, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimeUnit,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

const MICROS_PER_SECOND: i128 = 1_000_000;
const MICROS_PER_MILLI: i128 = 1_000;
const TEN_POW_19: i128 = 10_000_000_000_000_000_000;
const TWO_POW_63_AS_F64: f64 = 9_223_372_036_854_775_808.0;

#[must_use]
pub fn timestamp_seconds_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkEpochCtor::new(Scale::Seconds)))
}

#[must_use]
pub fn timestamp_millis_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkEpochCtor::new(Scale::Millis)))
}

#[must_use]
pub fn timestamp_micros_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkEpochCtor::new(Scale::Micros)))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        timestamp_seconds_udf(),
        timestamp_millis_udf(),
        timestamp_micros_udf(),
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scale {
    Seconds,
    Millis,
    Micros,
}

#[derive(Debug)]
struct SparkEpochCtor {
    signature: Signature,
    scale: Scale,
}

impl SparkEpochCtor {
    fn new(scale: Scale) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            scale,
        }
    }
}

impl PartialEq for SparkEpochCtor {
    fn eq(&self, other: &Self) -> bool {
        self.scale == other.scale
    }
}

impl Eq for SparkEpochCtor {}

impl Hash for SparkEpochCtor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkEpochCtor {
    fn name(&self) -> &str {
        match self.scale {
            Scale::Seconds => "timestamp_seconds",
            Scale::Millis => "timestamp_millis",
            Scale::Micros => "timestamp_micros",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(ltz_micros())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), ltz_micros(), true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "'{}' expects 1 argument, got {}",
                self.name(),
                arg_types.len()
            )));
        }
        gate_input(self.name(), self.scale, &arg_types[0])?;
        Ok(vec![arg_types[0].clone()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "'{}' expects 1 argument, got {}",
                self.name(),
                arrays.len()
            )));
        }
        let array = unpack_dictionary(&arrays[0])?;
        gate_input(self.name(), self.scale, array.data_type())?;
        let micros = epoch_micros(self.name(), self.scale, &array)?;
        let stamped = TimestampMicrosecondArray::from(micros).with_timezone("UTC");
        Ok(ColumnarValue::Array(Arc::new(stamped)))
    }
}

fn ltz_micros() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC")))
}

fn unpack_dictionary(array: &ArrayRef) -> Result<ArrayRef> {
    if let DataType::Dictionary(_, values) = array.data_type() {
        Ok(cast(array.as_ref(), values)?)
    } else {
        Ok(Arc::clone(array))
    }
}

fn gate_input(name: &str, scale: Scale, data_type: &DataType) -> Result<()> {
    if matches!(data_type, DataType::Null) {
        return Ok(());
    }
    if let DataType::Dictionary(_, values) = data_type {
        return gate_input(name, scale, values);
    }
    let accepted = match scale {
        Scale::Seconds => is_numeric(data_type),
        Scale::Millis | Scale::Micros => is_integral(data_type),
    };
    if accepted {
        return Ok(());
    }
    Err(unexpected_input(name, scale, data_type))
}

fn unexpected_input(name: &str, scale: Scale, data_type: &DataType) -> DataFusionError {
    let required = match scale {
        Scale::Seconds => "NUMERIC",
        Scale::Millis | Scale::Micros => "INTEGRAL",
    };
    let actual = crate::collection::spark_type_name(data_type);
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}({actual})\" due to \
         data type mismatch: The first parameter requires the \"{required}\" type, however the \
         argument has the type \"{actual}\". SQLSTATE: 42K09"
    ))
}

fn is_numeric(data_type: &DataType) -> bool {
    is_integral(data_type) || is_fractional(data_type)
}

fn is_integral(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn is_fractional(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}

fn epoch_micros(name: &str, scale: Scale, array: &ArrayRef) -> Result<Vec<Option<i64>>> {
    let factor = match scale {
        Scale::Seconds => MICROS_PER_SECOND,
        Scale::Millis => MICROS_PER_MILLI,
        Scale::Micros => 1,
    };
    match array.data_type() {
        DataType::Null => Ok(vec![None; array.len()]),
        DataType::Int8 => scaled_primitive::<Int8Type>(array, factor),
        DataType::Int16 => scaled_primitive::<Int16Type>(array, factor),
        DataType::Int32 => scaled_primitive::<Int32Type>(array, factor),
        DataType::Int64 => scaled_primitive::<Int64Type>(array, factor),
        DataType::UInt8 => scaled_primitive::<UInt8Type>(array, factor),
        DataType::UInt16 => scaled_primitive::<UInt16Type>(array, factor),
        DataType::UInt32 => scaled_primitive::<UInt32Type>(array, factor),
        DataType::UInt64 => scaled_primitive::<UInt64Type>(array, factor),
        DataType::Float32 if scale == Scale::Seconds => {
            let values = array.as_primitive::<Float32Type>();
            Ok(values
                .iter()
                .map(|value| value.and_then(|narrow| double_seconds_to_micros(f64::from(narrow))))
                .collect())
        }
        DataType::Float64 if scale == Scale::Seconds => {
            let values = array.as_primitive::<Float64Type>();
            Ok(values
                .iter()
                .map(|value| value.and_then(double_seconds_to_micros))
                .collect())
        }
        DataType::Decimal32(_, decimal_scale) if scale == Scale::Seconds => {
            let values = array.as_primitive::<Decimal32Type>();
            decimal_column(
                values.iter().map(|value| value.map(i128::from)),
                *decimal_scale,
            )
        }
        DataType::Decimal64(_, decimal_scale) if scale == Scale::Seconds => {
            let values = array.as_primitive::<Decimal64Type>();
            decimal_column(
                values.iter().map(|value| value.map(i128::from)),
                *decimal_scale,
            )
        }
        DataType::Decimal128(_, decimal_scale) if scale == Scale::Seconds => {
            let values = array.as_primitive::<Decimal128Type>();
            decimal_column(values.iter(), *decimal_scale)
        }
        DataType::Decimal256(_, decimal_scale) if scale == Scale::Seconds => {
            let values = array.as_primitive::<Decimal256Type>();
            values
                .iter()
                .map(|value| {
                    value
                        .map(|wide| {
                            wide.to_i128()
                                .ok_or_else(decimal_overflow)
                                .and_then(|narrow| {
                                    decimal_seconds_to_micros(narrow, *decimal_scale)
                                })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()
        }
        other => Err(unexpected_input(name, scale, other)),
    }
}

fn decimal_column(
    values: impl Iterator<Item = Option<i128>>,
    decimal_scale: i8,
) -> Result<Vec<Option<i64>>> {
    values
        .map(|value| {
            value
                .map(|unscaled| decimal_seconds_to_micros(unscaled, decimal_scale))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()
}

fn scaled_primitive<T>(array: &ArrayRef, factor: i128) -> Result<Vec<Option<i64>>>
where
    T: ArrowPrimitiveType,
    i128: From<T::Native>,
{
    let values = array.as_primitive::<T>();
    values
        .iter()
        .map(|value| {
            value
                .map(|narrow| checked_scale(i128::from(narrow), factor))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()
}

fn checked_scale(unscaled: i128, factor: i128) -> Result<i64> {
    let micros = unscaled.checked_mul(factor).ok_or_else(long_overflow)?;
    i64::try_from(micros).map_err(|_| long_overflow())
}

fn long_overflow() -> DataFusionError {
    DataFusionError::Execution("long overflow".to_string())
}

fn double_seconds_to_micros(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    let product = value * 1_000_000.0;
    if product >= TWO_POW_63_AS_F64 {
        Some(i64::MAX)
    } else if product <= -TWO_POW_63_AS_F64 {
        Some(i64::MIN)
    } else {
        #[allow(clippy::cast_possible_truncation)]
        Some(product.trunc() as i64)
    }
}

fn decimal_seconds_to_micros(unscaled: i128, decimal_scale: i8) -> Result<i64> {
    let shift = 6_i32 - i32::from(decimal_scale);
    if shift >= 0 {
        let factor = 10_i128.checked_pow(u32::try_from(shift).map_err(|_| decimal_overflow())?);
        let factor = factor.ok_or_else(decimal_overflow)?;
        let micros = unscaled.checked_mul(factor).ok_or_else(decimal_overflow)?;
        i64::try_from(micros).map_err(|_| decimal_overflow())
    } else {
        let divisor = 10_i128.checked_pow(u32::try_from(-shift).map_err(|_| decimal_overflow())?);
        let divisor = divisor.ok_or_else(decimal_overflow)?;
        let quotient = unscaled / divisor;
        let remainder = unscaled % divisor;
        if quotient >= TEN_POW_19 || quotient <= -TEN_POW_19 {
            return Err(decimal_overflow());
        }
        if remainder != 0 {
            return Err(DataFusionError::Execution("Rounding necessary".to_string()));
        }
        i64::try_from(quotient).map_err(|_| decimal_overflow())
    }
}

fn decimal_overflow() -> DataFusionError {
    DataFusionError::Execution("Overflow".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::TimestampMicrosecondArray;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx_with_epoch_ctors() -> SessionContext {
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.parse_float_as_decimal = true;
        let ctx = SessionContext::new_with_config(config);
        for udf in functions() {
            ctx.register_udf(udf.as_ref().clone());
        }
        ctx
    }

    async fn one_micros(ctx: &SessionContext, sql: &str) -> Option<i64> {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        assert_eq!(batches.len(), 1);
        let batch: &RecordBatch = &batches[0];
        assert_eq!(
            batch.schema().field(0).data_type(),
            &ltz_micros(),
            "the answer carries LTZ micros"
        );
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("TimestampMicrosecondArray");
        array.is_valid(0).then(|| array.value(0))
    }

    #[tokio::test]
    async fn integral_seconds_millis_micros_scale() {
        let ctx = ctx_with_epoch_ctors();
        assert_eq!(
            one_micros(&ctx, "SELECT timestamp_seconds(1) AS v").await,
            Some(1_000_000)
        );
        assert_eq!(
            one_micros(&ctx, "SELECT timestamp_millis(1) AS v").await,
            Some(1_000)
        );
        assert_eq!(
            one_micros(&ctx, "SELECT timestamp_micros(1) AS v").await,
            Some(1)
        );
        assert_eq!(
            one_micros(&ctx, "SELECT timestamp_seconds(-1) AS v").await,
            Some(-1_000_000)
        );
    }

    #[tokio::test]
    async fn fractional_seconds_keep_their_fraction() {
        let ctx = ctx_with_epoch_ctors();
        assert_eq!(
            one_micros(&ctx, "SELECT timestamp_seconds(1.5) AS v").await,
            Some(1_500_000)
        );
        assert_eq!(
            one_micros(
                &ctx,
                "SELECT timestamp_seconds(CAST(0.0000009 AS DOUBLE)) AS v"
            )
            .await,
            Some(0)
        );
        assert_eq!(
            one_micros(
                &ctx,
                "SELECT timestamp_seconds(CAST(0.000001 AS DECIMAL(10, 6))) AS v"
            )
            .await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn millis_and_micros_refuse_fractional_input() {
        let ctx = ctx_with_epoch_ctors();
        for sql in [
            "SELECT timestamp_millis(1.5) AS v",
            "SELECT timestamp_micros(CAST(1 AS DOUBLE)) AS v",
            "SELECT timestamp_millis(CAST(1 AS DECIMAL(10, 2))) AS v",
        ] {
            let error = ctx
                .sql(sql)
                .await
                .expect_err("fractional input must not plan");
            assert!(
                error
                    .to_string()
                    .contains("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
                "got {error}"
            );
        }
    }

    #[tokio::test]
    async fn every_spelling_refuses_strings() {
        let ctx = ctx_with_epoch_ctors();
        for sql in [
            "SELECT timestamp_seconds('1') AS v",
            "SELECT timestamp_millis('1970-01-01 00:00:01') AS v",
            "SELECT timestamp_micros(CAST(NULL AS STRING)) AS v",
        ] {
            let error = ctx.sql(sql).await.expect_err("string input must not plan");
            assert!(
                error
                    .to_string()
                    .contains("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
                "got {error}"
            );
        }
    }

    #[tokio::test]
    async fn overflow_refuses_like_spark() {
        let ctx = ctx_with_epoch_ctors();
        for (sql, message) in [
            (
                "SELECT timestamp_seconds(9223372036855) AS v",
                "long overflow",
            ),
            (
                "SELECT timestamp_millis(9223372036854776) AS v",
                "long overflow",
            ),
            (
                "SELECT timestamp_seconds(CAST(0.0000005 AS DECIMAL(10, 7))) AS v",
                "Rounding necessary",
            ),
            (
                "SELECT timestamp_seconds(CAST(99999999999999999999999999999 AS DECIMAL(38, 0))) AS v",
                "Overflow",
            ),
        ] {
            let failure = ctx.sql(sql).await.unwrap().collect().await;
            let error = failure.expect_err("overflow must not answer");
            assert!(error.to_string().contains(message), "got {error}");
        }
        assert_eq!(
            one_micros(&ctx, "SELECT timestamp_seconds(9223372036854) AS v").await,
            Some(9_223_372_036_854_000_000)
        );
        assert_eq!(
            one_micros(&ctx, "SELECT timestamp_micros(9223372036854775807) AS v").await,
            Some(9_223_372_036_854_775_807)
        );
    }

    #[tokio::test]
    async fn decimal_boundary_answers_spark_order() {
        let ctx = ctx_with_epoch_ctors();
        for (sql, message) in [
            (
                "SELECT timestamp_seconds(CAST(9223372036854.7758075 AS DECIMAL(20, 7))) AS v",
                "Rounding necessary",
            ),
            (
                "SELECT timestamp_seconds(CAST(9223372036854.7758085 AS DECIMAL(20, 7))) AS v",
                "Rounding necessary",
            ),
            (
                "SELECT timestamp_seconds(CAST(-9223372036854.7758085 AS DECIMAL(20, 7))) AS v",
                "Rounding necessary",
            ),
            (
                "SELECT timestamp_seconds(CAST(-9223372036854.7758095 AS DECIMAL(20, 7))) AS v",
                "Rounding necessary",
            ),
            (
                "SELECT timestamp_seconds(CAST(9999999999999.9999995 AS DECIMAL(20, 7))) AS v",
                "Rounding necessary",
            ),
            (
                "SELECT timestamp_seconds(CAST(9999999999999.9999999 AS DECIMAL(20, 7))) AS v",
                "Rounding necessary",
            ),
            (
                "SELECT timestamp_seconds(CAST(-9999999999999.9999995 AS DECIMAL(20, 7))) AS v",
                "Rounding necessary",
            ),
            (
                "SELECT timestamp_seconds(CAST(9223372036854.7758065 AS DECIMAL(20, 7))) AS v",
                "Rounding necessary",
            ),
            (
                "SELECT timestamp_seconds(CAST(-9223372036854.7758075 AS DECIMAL(20, 7))) AS v",
                "Rounding necessary",
            ),
            (
                "SELECT timestamp_seconds(CAST(9223372036854.775808 AS DECIMAL(19, 6))) AS v",
                "Overflow",
            ),
            (
                "SELECT timestamp_seconds(CAST(-9223372036854.775809 AS DECIMAL(19, 6))) AS v",
                "Overflow",
            ),
            (
                "SELECT timestamp_seconds(CAST(9999999999999.999999 AS DECIMAL(19, 6))) AS v",
                "Overflow",
            ),
            (
                "SELECT timestamp_seconds(CAST(10000000000000.0000005 AS DECIMAL(21, 7))) AS v",
                "Overflow",
            ),
            (
                "SELECT timestamp_seconds(CAST(10000000000000.000000 AS DECIMAL(20, 6))) AS v",
                "Overflow",
            ),
            (
                "SELECT timestamp_seconds(CAST(-9999999999999.999999 AS DECIMAL(19, 6))) AS v",
                "Overflow",
            ),
            (
                "SELECT timestamp_seconds(CAST(-10000000000000.0000005 AS DECIMAL(21, 7))) AS v",
                "Overflow",
            ),
            (
                "SELECT timestamp_seconds(CAST(-10000000000000.000000 AS DECIMAL(20, 6))) AS v",
                "Overflow",
            ),
        ] {
            let failure = ctx.sql(sql).await.unwrap().collect().await;
            let error = failure.expect_err("boundary must refuse");
            assert!(error.to_string().contains(message), "got {error} for {sql}");
        }
        for (sql, micros) in [
            (
                "SELECT timestamp_seconds(CAST(9223372036854.775807 AS DECIMAL(19, 6))) AS v",
                9_223_372_036_854_775_807,
            ),
            (
                "SELECT timestamp_seconds(CAST(9223372036854.7758070 AS DECIMAL(20, 7))) AS v",
                9_223_372_036_854_775_807,
            ),
            (
                "SELECT timestamp_seconds(CAST(9223372036854.775806 AS DECIMAL(19, 6))) AS v",
                9_223_372_036_854_775_806,
            ),
            (
                "SELECT timestamp_seconds(CAST(-9223372036854.775808 AS DECIMAL(19, 6))) AS v",
                -9_223_372_036_854_775_808,
            ),
            (
                "SELECT timestamp_seconds(CAST(-9223372036854.775807 AS DECIMAL(19, 6))) AS v",
                -9_223_372_036_854_775_807,
            ),
        ] {
            assert_eq!(one_micros(&ctx, sql).await, Some(micros));
        }
    }

    #[tokio::test]
    async fn double_edges_follow_spark() {
        let ctx = ctx_with_epoch_ctors();
        assert_eq!(
            one_micros(
                &ctx,
                "SELECT timestamp_seconds(CAST('1E300' AS DOUBLE)) AS v"
            )
            .await,
            Some(i64::MAX)
        );
        assert_eq!(
            one_micros(
                &ctx,
                "SELECT timestamp_seconds(CAST('-1E300' AS DOUBLE)) AS v"
            )
            .await,
            Some(i64::MIN)
        );
        assert_eq!(
            one_micros(&ctx, "SELECT timestamp_seconds(CAST('NaN' AS DOUBLE)) AS v").await,
            None
        );
        assert_eq!(
            one_micros(
                &ctx,
                "SELECT timestamp_seconds(CAST('Infinity' AS DOUBLE)) AS v"
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn null_in_null_out() {
        let ctx = ctx_with_epoch_ctors();
        assert_eq!(
            one_micros(&ctx, "SELECT timestamp_seconds(CAST(NULL AS BIGINT)) AS v").await,
            None
        );
        assert_eq!(
            one_micros(&ctx, "SELECT timestamp_millis(NULL) AS v").await,
            None
        );
    }
}
