//! `try_divide` / `try_mod` / `try_add` / `try_subtract` / `try_multiply`.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, PrimitiveArray};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type,
    IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
};
use datafusion::arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano};
use datafusion::common::{Result, exec_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};

use crate::decimal_precision::{decimal128_parts, spark_div_result_type, spark_result_type};

macro_rules! try_arith_udf {
    ($type_name:ident, $name_literal:literal, $kind:expr) => {
        #[derive(Debug)]
        struct $type_name {
            signature: Signature,
        }

        impl $type_name {
            fn new() -> Self {
                Self {
                    signature: Signature::user_defined(Volatility::Immutable),
                }
            }
        }

        impl PartialEq for $type_name {
            fn eq(&self, _other: &Self) -> bool {
                true
            }
        }

        impl Eq for $type_name {}

        impl Hash for $type_name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.name().hash(state);
            }
        }

        impl ScalarUDFImpl for $type_name {
            crate::shim_udf_boilerplate!($name_literal);

            fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                result_type($kind, arg_types)
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
                let arg_types: Vec<DataType> = args
                    .arg_fields
                    .iter()
                    .map(|f| f.data_type().clone())
                    .collect();
                let data_type = self.return_type(&arg_types)?;
                Ok(Arc::new(Field::new($name_literal, data_type, true)))
            }

            fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
                coerce_pair($kind, arg_types)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                invoke_try($kind, &args)
            }
        }
    };
}

#[derive(Clone, Copy)]
enum TryKind {
    Divide,
    Mod,
    Add,
    Sub,
    Mul,
}

impl TryKind {
    fn operator(self) -> Operator {
        match self {
            Self::Divide => Operator::Divide,
            Self::Mod => Operator::Modulo,
            Self::Add => Operator::Plus,
            Self::Sub => Operator::Minus,
            Self::Mul => Operator::Multiply,
        }
    }
}

try_arith_udf!(SparkTryDivide, "try_divide", TryKind::Divide);
try_arith_udf!(SparkTryMod, "try_mod", TryKind::Mod);
try_arith_udf!(SparkTryAdd, "try_add", TryKind::Add);
try_arith_udf!(SparkTrySub, "try_subtract", TryKind::Sub);
try_arith_udf!(SparkTryMul, "try_multiply", TryKind::Mul);

#[must_use]
pub fn try_divide_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTryDivide::new()))
}

#[must_use]
pub fn try_mod_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTryMod::new()))
}

#[must_use]
pub fn try_add_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTryAdd::new()))
}

#[must_use]
pub fn try_subtract_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTrySub::new()))
}

#[must_use]
pub fn try_multiply_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTryMul::new()))
}

fn two_args<'a>(arg_types: &'a [DataType], name: &str) -> Result<(&'a DataType, &'a DataType)> {
    let [left, right] = arg_types else {
        return exec_err!("'{name}' expects two arguments, got {}", arg_types.len());
    };
    Ok((left, right))
}

fn result_type(kind: TryKind, arg_types: &[DataType]) -> Result<DataType> {
    let name = kind_name(kind);
    let (left, right) = two_args(arg_types, name)?;
    if matches!(kind, TryKind::Divide) {
        if let (Some(left_meta), Some(right_meta)) =
            (decimal128_parts(left), decimal128_parts(right))
        {
            let (precision, scale) =
                spark_div_result_type(left_meta, right_meta).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "'{name}' cannot compute a Spark decimal result type"
                    ))
                })?;
            return Ok(DataType::Decimal128(precision, scale));
        }
        return Ok(DataType::Float64);
    }
    if let (Some(left_meta), Some(right_meta)) = (decimal128_parts(left), decimal128_parts(right)) {
        let (precision, scale) = spark_result_type(kind.operator(), left_meta, right_meta)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "'{name}' cannot compute a Spark decimal result type"
                ))
            })?;
        return Ok(DataType::Decimal128(precision, scale));
    }
    match (left, right) {
        (DataType::Float64 | DataType::Float32, _) | (_, DataType::Float64 | DataType::Float32) => {
            Ok(DataType::Float64)
        }
        (DataType::Int64, _) | (_, DataType::Int64) => Ok(DataType::Int64),
        (DataType::Int32, _) | (_, DataType::Int32) => Ok(DataType::Int32),
        (DataType::Int16, _) | (_, DataType::Int16) => Ok(DataType::Int16),
        (DataType::Int8, DataType::Int8) => Ok(DataType::Int8),
        (DataType::Interval(IntervalUnit::DayTime), DataType::Interval(IntervalUnit::DayTime))
            if matches!(kind, TryKind::Add | TryKind::Sub) =>
        {
            Ok(DataType::Interval(IntervalUnit::DayTime))
        }
        (
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) if matches!(kind, TryKind::Add | TryKind::Sub) => {
            Ok(DataType::Interval(IntervalUnit::MonthDayNano))
        }
        _ => exec_err!("'{name}' does not support types {left} and {right}"),
    }
}

fn coerce_pair(kind: TryKind, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let name = kind_name(kind);
    let (left, right) = two_args(arg_types, name)?;
    if decimal128_parts(left).is_some() && decimal128_parts(right).is_some() {
        return Ok(vec![left.clone(), right.clone()]);
    }
    if matches!(kind, TryKind::Divide)
        && left.is_numeric()
        && right.is_numeric()
        && decimal128_parts(left).is_none()
        && decimal128_parts(right).is_none()
    {
        return Ok(vec![DataType::Float64, DataType::Float64]);
    }
    if matches!(kind, TryKind::Add | TryKind::Sub)
        && matches!(
            (left, right),
            (
                DataType::Interval(IntervalUnit::DayTime),
                DataType::Interval(IntervalUnit::DayTime)
            ) | (
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Interval(IntervalUnit::MonthDayNano)
            )
        )
    {
        return Ok(vec![left.clone(), right.clone()]);
    }
    if left.is_floating() || right.is_floating() {
        return Ok(vec![DataType::Float64, DataType::Float64]);
    }
    let width = integer_width(left)
        .max(integer_width(right))
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'{name}' expects numeric arguments, got {left}, {right}"
            ))
        })?;
    Ok(vec![width.clone(), width])
}

fn integer_width(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Int8 => Some(DataType::Int8),
        DataType::Int16 => Some(DataType::Int16),
        DataType::Int32 | DataType::Null => Some(DataType::Int32),
        DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Some(DataType::Int64),
        _ => None,
    }
}

fn kind_name(kind: TryKind) -> &'static str {
    match kind {
        TryKind::Divide => "try_divide",
        TryKind::Mod => "try_mod",
        TryKind::Add => "try_add",
        TryKind::Sub => "try_subtract",
        TryKind::Mul => "try_multiply",
    }
}

fn invoke_try(kind: TryKind, args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    match args.return_field.data_type() {
        DataType::Decimal128(_, _) => crate::decimal_spark::try_decimal_op(kind.operator(), args),
        DataType::Float64 => invoke_float64(kind, args),
        DataType::Int64 => invoke_int::<Int64Type>(kind, args, eval_i64),
        DataType::Int32 => invoke_int::<Int32Type>(kind, args, eval_i32),
        DataType::Int16 => invoke_int::<Int16Type>(kind, args, eval_i16),
        DataType::Int8 => invoke_int::<Int8Type>(kind, args, eval_i8),
        DataType::Interval(IntervalUnit::DayTime) => invoke_interval_day_time(kind, args),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            invoke_interval_month_day_nano(kind, args)
        }
        other => exec_err!(
            "'{}' promised {other} but cannot invoke it",
            kind_name(kind)
        ),
    }
}

fn invoke_float64(kind: TryKind, args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left = float64_array(arrays[0].as_ref())?;
    let right = float64_array(arrays[1].as_ref())?;
    if left.len() != right.len() {
        return exec_err!("'{}' argument lengths differ", kind_name(kind));
    }
    let mut values: Vec<Option<f64>> = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        if !left.is_valid(row) || !right.is_valid(row) {
            values.push(None);
            continue;
        }
        values.push(eval_f64(kind, left.value(row), right.value(row)));
    }
    Ok(ColumnarValue::Array(Arc::new(Float64Array::from(values))))
}

fn float64_array(array: &dyn Array) -> Result<Float64Array> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "try_* expected Float64Array, got {}",
                array.data_type()
            ))
        })
}

fn eval_f64(kind: TryKind, left: f64, right: f64) -> Option<f64> {
    match kind {
        TryKind::Divide | TryKind::Mod if right == 0.0 => None,
        TryKind::Divide => Some(left / right),
        TryKind::Mod => Some(left % right),
        TryKind::Add => Some(left + right),
        TryKind::Sub => Some(left - right),
        TryKind::Mul => Some(left * right),
    }
}

type IntEval<T> = fn(TryKind, T, T) -> Option<T>;

fn invoke_int<T>(
    kind: TryKind,
    args: &ScalarFunctionArgs,
    eval: IntEval<T::Native>,
) -> Result<ColumnarValue>
where
    T: ArrowPrimitiveType,
    PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
{
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left = primitive::<T>(arrays[0].as_ref())?;
    let right = primitive::<T>(arrays[1].as_ref())?;
    if left.len() != right.len() {
        return exec_err!("'{}' argument lengths differ", kind_name(kind));
    }
    let mut values: Vec<Option<T::Native>> = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        if !left.is_valid(row) || !right.is_valid(row) {
            values.push(None);
            continue;
        }
        values.push(eval(kind, left.value(row), right.value(row)));
    }
    Ok(ColumnarValue::Array(Arc::new(PrimitiveArray::<T>::from(
        values,
    ))))
}

fn primitive<T: ArrowPrimitiveType>(array: &dyn Array) -> Result<PrimitiveArray<T>> {
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "try_* expected {}, got {}",
                std::any::type_name::<PrimitiveArray<T>>(),
                array.data_type()
            ))
        })
}

fn eval_i64(kind: TryKind, left: i64, right: i64) -> Option<i64> {
    match kind {
        TryKind::Mod if right == 0 => None,
        TryKind::Mod => left
            .checked_rem(right)
            .or_else(|| (right == -1).then_some(0)),
        TryKind::Add => left.checked_add(right),
        TryKind::Sub => left.checked_sub(right),
        TryKind::Mul => left.checked_mul(right),
        TryKind::Divide => None,
    }
}

fn eval_i32(kind: TryKind, left: i32, right: i32) -> Option<i32> {
    match kind {
        TryKind::Mod if right == 0 => None,
        TryKind::Mod => left
            .checked_rem(right)
            .or_else(|| (right == -1).then_some(0)),
        TryKind::Add => left.checked_add(right),
        TryKind::Sub => left.checked_sub(right),
        TryKind::Mul => left.checked_mul(right),
        TryKind::Divide => None,
    }
}

fn eval_i16(kind: TryKind, left: i16, right: i16) -> Option<i16> {
    match kind {
        TryKind::Mod if right == 0 => None,
        TryKind::Mod => left
            .checked_rem(right)
            .or_else(|| (right == -1).then_some(0)),
        TryKind::Add => left.checked_add(right),
        TryKind::Sub => left.checked_sub(right),
        TryKind::Mul => left.checked_mul(right),
        TryKind::Divide => None,
    }
}

fn eval_i8(kind: TryKind, left: i8, right: i8) -> Option<i8> {
    match kind {
        TryKind::Mod if right == 0 => None,
        TryKind::Mod => left
            .checked_rem(right)
            .or_else(|| (right == -1).then_some(0)),
        TryKind::Add => left.checked_add(right),
        TryKind::Sub => left.checked_sub(right),
        TryKind::Mul => left.checked_mul(right),
        TryKind::Divide => None,
    }
}

fn invoke_interval_day_time(kind: TryKind, args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left = primitive::<IntervalDayTimeType>(arrays[0].as_ref())?;
    let right = primitive::<IntervalDayTimeType>(arrays[1].as_ref())?;
    if left.len() != right.len() {
        return exec_err!("'{}' argument lengths differ", kind_name(kind));
    }
    let mut values: Vec<Option<IntervalDayTime>> = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        if !left.is_valid(row) || !right.is_valid(row) {
            values.push(None);
            continue;
        }
        values.push(eval_interval_day_time(
            kind,
            left.value(row),
            right.value(row),
        ));
    }
    Ok(ColumnarValue::Array(Arc::new(PrimitiveArray::<
        IntervalDayTimeType,
    >::from(values))))
}

const MICROS_PER_DAY: i64 = 86_400_000_000;
const MICROS_PER_MILLI: i64 = 1_000;

fn interval_to_micros(value: IntervalDayTime) -> Option<i64> {
    let days = i64::from(value.days);
    let millis = i64::from(value.milliseconds);
    days.checked_mul(MICROS_PER_DAY)?
        .checked_add(millis.checked_mul(MICROS_PER_MILLI)?)
}

fn micros_to_interval(micros: i64) -> Option<IntervalDayTime> {
    let days = i32::try_from(micros.div_euclid(MICROS_PER_DAY)).ok()?;
    let rem = micros.rem_euclid(MICROS_PER_DAY);
    let milliseconds = i32::try_from(rem / MICROS_PER_MILLI).ok()?;
    Some(IntervalDayTime { days, milliseconds })
}

fn invoke_interval_month_day_nano(
    kind: TryKind,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left = primitive::<IntervalMonthDayNanoType>(arrays[0].as_ref())?;
    let right = primitive::<IntervalMonthDayNanoType>(arrays[1].as_ref())?;
    if left.len() != right.len() {
        return exec_err!("'{}' argument lengths differ", kind_name(kind));
    }
    let mut values: Vec<Option<IntervalMonthDayNano>> = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        if !left.is_valid(row) || !right.is_valid(row) {
            values.push(None);
            continue;
        }
        values.push(eval_interval_month_day_nano(
            kind,
            left.value(row),
            right.value(row),
        ));
    }
    Ok(ColumnarValue::Array(Arc::new(PrimitiveArray::<
        IntervalMonthDayNanoType,
    >::from(values))))
}

fn eval_interval_month_day_nano(
    kind: TryKind,
    left: IntervalMonthDayNano,
    right: IntervalMonthDayNano,
) -> Option<IntervalMonthDayNano> {
    let (months, days, nanos) = match kind {
        TryKind::Add => (
            left.months.checked_add(right.months)?,
            left.days.checked_add(right.days)?,
            left.nanoseconds.checked_add(right.nanoseconds)?,
        ),
        TryKind::Sub => (
            left.months.checked_sub(right.months)?,
            left.days.checked_sub(right.days)?,
            left.nanoseconds.checked_sub(right.nanoseconds)?,
        ),
        _ => return None,
    };
    Some(IntervalMonthDayNano {
        months,
        days,
        nanoseconds: nanos,
    })
}

fn eval_interval_day_time(
    kind: TryKind,
    left: IntervalDayTime,
    right: IntervalDayTime,
) -> Option<IntervalDayTime> {
    let left_micros = interval_to_micros(left)?;
    let right_micros = interval_to_micros(right)?;
    let result = match kind {
        TryKind::Add => left_micros.checked_add(right_micros)?,
        TryKind::Sub => left_micros.checked_sub(right_micros)?,
        _ => return None,
    };
    micros_to_interval(result)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, Float64Array, Int16Array, Int32Array};
    use datafusion::prelude::SessionContext;

    use crate::analyzer_rules;
    use crate::try_invert::register;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        register(&ctx);
        for rule in analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn cell(sql: &str) -> (String, Vec<Option<String>>) {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let batch = &batches[0];
        let schema = batch.schema();
        let field = schema.field(0);
        let data_type = field.data_type().to_string();
        let array = batch.column(0);
        let values = (0..array.len())
            .map(|row| {
                array
                    .is_valid(row)
                    .then(|| format!("{:?}", array.slice(row, 1)))
            })
            .collect();
        (data_type, values)
    }

    #[tokio::test]
    async fn try_divide_by_zero_is_null_double() {
        let (data_type, values) = cell("SELECT try_divide(1, 0) AS v").await;
        assert_eq!(data_type, "Float64");
        assert_eq!(values, vec![None]);
    }

    #[tokio::test]
    async fn try_divide_six_over_two_is_three() {
        let (data_type, _) = cell("SELECT try_divide(CAST(6 AS INT), CAST(2 AS INT)) AS v").await;
        assert_eq!(data_type, "Float64");
        let batches = ctx()
            .sql("SELECT try_divide(CAST(6 AS INT), CAST(2 AS INT)) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(array.value(0).to_bits(), 3.0_f64.to_bits());
    }

    #[tokio::test]
    async fn try_mod_by_zero_is_null() {
        let batches = ctx()
            .sql("SELECT try_mod(CAST(7 AS INT), CAST(0 AS INT)) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(!array.is_valid(0));
    }

    #[tokio::test]
    async fn try_add_int_overflow_is_null() {
        let batches = ctx()
            .sql("SELECT try_add(CAST(2147483647 AS INT), CAST(1 AS INT)) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(!array.is_valid(0));
    }

    #[tokio::test]
    async fn try_add_smallint_overflow_is_null() {
        let batches = ctx()
            .sql("SELECT try_add(CAST(32767 AS SMALLINT), CAST(1 AS SMALLINT)) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert!(!array.is_valid(0));
    }
}
