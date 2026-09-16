use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Float64Array, Int32Array, PrimitiveBuilder,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::spark_math::unexpected_input_type;

const POW10_I128: [i128; 39] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
    10_000_000_000_000_000_000,
    100_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000_000_000,
];

#[must_use]
pub fn bround_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkBround::new()))
}

#[must_use]
pub fn call_bround(args: Vec<Expr>) -> Expr {
    crate::expr_fn::call(crate::spark_math::bround_udf(), args)
}

#[derive(Debug)]
struct SparkBround {
    signature: Signature,
}

impl SparkBround {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkBround {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkBround {}

impl Hash for SparkBround {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn is_bround_input(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn is_integral_or_null(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn scalar_to_scale(value: &ScalarValue) -> Option<i32> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(i32::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i32::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(*v),
        ScalarValue::Int64(Some(v)) => i32::try_from(*v).ok(),
        ScalarValue::UInt8(Some(v)) => Some(i32::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i32::from(*v)),
        ScalarValue::UInt32(Some(v)) => i32::try_from(*v).ok(),
        ScalarValue::UInt64(Some(v)) => i32::try_from(*v).ok(),
        _ => None,
    }
}

fn bround_output_type(input: &DataType, scale: Option<i32>) -> DataType {
    match input {
        DataType::Float32 | DataType::Float64 => DataType::Float64,
        DataType::Decimal128(precision, scale_in) => {
            if scale.is_none_or(|k| k >= i32::from(*scale_in)) {
                input.clone()
            } else {
                let target = scale.unwrap_or(0).max(0);
                let out_precision =
                    (i32::from(*precision) - i32::from(*scale_in) + target + 1).min(38);
                DataType::Decimal128(
                    u8::try_from(out_precision).unwrap_or(38),
                    i8::try_from(target).unwrap_or(0),
                )
            }
        }
        _ => input.clone(),
    }
}

fn round_half_even_quotient(value: i128, divisor: i128) -> i128 {
    let quotient = value.div_euclid(divisor);
    let remainder = value.rem_euclid(divisor);
    let twice = remainder.saturating_mul(2);
    if twice > divisor || (twice == divisor && quotient % 2 != 0) {
        quotient + 1
    } else {
        quotient
    }
}

fn round_float(value: f64, scale: i32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    if scale >= 0 {
        let factor = 10f64.powi(scale.min(308));
        let shifted = value * factor;
        if !shifted.is_finite() {
            return value;
        }
        shifted.round_ties_even() / factor
    } else {
        if scale < -323 {
            return value.signum() * 0.0;
        }
        let factor = 10f64.powi(-scale);
        (value / factor).round_ties_even() * factor
    }
}

fn plan_bround(arg_types: &[DataType]) -> Result<()> {
    if arg_types.len() != 1 && arg_types.len() != 2 {
        return exec_err!(
            "'bround' expects (col[, scale]), got {} argument(s)",
            arg_types.len()
        );
    }
    if !is_bround_input(&arg_types[0]) {
        return Err(unexpected_input_type("bround", "DOUBLE", &arg_types[0]));
    }
    if arg_types.len() == 2 && !is_integral_or_null(&arg_types[1]) {
        return Err(unexpected_input_type("bround", "INT", &arg_types[1]));
    }
    Ok(())
}

macro_rules! impl_integral_bround {
    ($input:expr, $scales:expr, $ansi:expr, $native:ty, $arrow:ty) => {{
        let values = $input.as_primitive::<$arrow>();
        let mut out = PrimitiveBuilder::<$arrow>::with_capacity(values.len());
        for row in 0..values.len() {
            if values.is_null(row) {
                out.append_null();
                continue;
            }
            let scale = $scales.get(row).copied().flatten();
            let Some(scale) = scale else {
                out.append_null();
                continue;
            };
            if scale >= 0 {
                out.append_value(values.value(row));
                continue;
            }
            let digits = scale.unsigned_abs();
            let (quotient, back) = if digits > 38 {
                (0i128, 1i128)
            } else {
                let back = POW10_I128[digits as usize];
                (
                    round_half_even_quotient(i128::from(values.value(row)), back),
                    back,
                )
            };
            let fits = quotient.checked_mul(back).and_then(|exact| <$native>::try_from(exact).ok());
            match fits {
                Some(fits) => out.append_value(fits),
                None => {
                    if $ansi {
                        return Err(crate::spark_math::overflow_error("bround"));
                    }
                    out.append_value(quotient.wrapping_mul(back) as $native);
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }};
}

impl ScalarUDFImpl for SparkBround {
    crate::shim_udf_boilerplate!("bround");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let input = arg_types.first().cloned().unwrap_or(DataType::Null);
        Ok(bround_output_type(&input, None))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let declared: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        plan_bround(&declared)?;
        let scale = args
            .scalar_arguments
            .get(1)
            .and_then(|scalar| scalar.as_ref())
            .and_then(|scalar| scalar_to_scale(scalar));
        let data_type = bround_output_type(&declared[0], scale);
        Ok(Arc::new(Field::new("bround", data_type, true)))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        if args.len() != 1 && args.len() != 2 {
            return exec_err!(
                "'bround' expects (col[, scale]), got {} argument(s)",
                args.len()
            );
        }
        let mut parts: Vec<String> = args.iter().map(crate::expr_fn::spark_expr_token).collect();
        if parts.len() == 1 {
            parts.push("0".to_owned());
        }
        Ok(format!("bround({})", parts.join(", ")))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        plan_bround(arg_types)?;
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: arg_values,
            number_rows,
            return_field,
            config_options,
            ..
        } = args;
        if arg_values.len() != 1 && arg_values.len() != 2 {
            return exec_err!(
                "'bround' expects (col[, scale]), got {} argument(s)",
                arg_values.len()
            );
        }
        let scales = read_scales(arg_values.get(1), number_rows)?;
        let input = arg_values[0].to_array(number_rows)?;
        let ansi = crate::ansi::spark_ansi_enabled_from_options(config_options.as_ref());
        apply_bround(&input, &scales, return_field.data_type(), ansi)
    }
}

fn read_scales(scale_value: Option<&ColumnarValue>, number_rows: usize) -> Result<Vec<Option<i32>>> {
    let Some(scale_value) = scale_value else {
        return Ok(vec![Some(0); number_rows]);
    };
    let array = scale_value.to_array(number_rows)?;
    let shaped = cast(array.as_ref(), &DataType::Int32)?;
    let ints = shaped.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
        datafusion::common::DataFusionError::Execution("bround needs an integral scale".to_owned())
    })?;
    Ok((0..ints.len())
        .map(|row| {
            if ints.is_null(row) {
                None
            } else {
                Some(ints.value(row))
            }
        })
        .collect())
}

fn apply_bround(
    input: &ArrayRef,
    scales: &[Option<i32>],
    output_type: &DataType,
    ansi: bool,
) -> Result<ColumnarValue> {
    use datafusion::arrow::array::new_null_array;
    match input.data_type() {
        DataType::Null => Ok(ColumnarValue::Array(new_null_array(output_type, input.len()))),
        DataType::Float32 => {
            let shaped = cast(input.as_ref(), &DataType::Float64)?;
            apply_float_bround(&shaped, scales)
        }
        DataType::Float64 => apply_float_bround(input, scales),
        DataType::Decimal128(_, scale_in) => {
            apply_decimal_bround(input, *scale_in, scales, output_type, ansi)
        }
        DataType::Int8 => impl_integral_bround!(
            input,
            scales,
            ansi,
            i8,
            datafusion::arrow::datatypes::Int8Type
        ),
        DataType::Int16 => impl_integral_bround!(
            input,
            scales,
            ansi,
            i16,
            datafusion::arrow::datatypes::Int16Type
        ),
        DataType::Int32 => impl_integral_bround!(
            input,
            scales,
            ansi,
            i32,
            datafusion::arrow::datatypes::Int32Type
        ),
        DataType::Int64 => impl_integral_bround!(
            input,
            scales,
            ansi,
            i64,
            datafusion::arrow::datatypes::Int64Type
        ),
        DataType::UInt8 => impl_integral_bround!(
            input,
            scales,
            ansi,
            u8,
            datafusion::arrow::datatypes::UInt8Type
        ),
        DataType::UInt16 => impl_integral_bround!(
            input,
            scales,
            ansi,
            u16,
            datafusion::arrow::datatypes::UInt16Type
        ),
        DataType::UInt32 => impl_integral_bround!(
            input,
            scales,
            ansi,
            u32,
            datafusion::arrow::datatypes::UInt32Type
        ),
        DataType::UInt64 => impl_integral_bround!(
            input,
            scales,
            ansi,
            u64,
            datafusion::arrow::datatypes::UInt64Type
        ),
        other => exec_err!("'bround' does not support {other}"),
    }
}

fn apply_float_bround(input: &ArrayRef, scales: &[Option<i32>]) -> Result<ColumnarValue> {
    let values = input.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
        datafusion::common::DataFusionError::Execution("bround needs float64 values".to_owned())
    })?;
    let mut out = PrimitiveBuilder::<Float64Type>::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row) {
            out.append_null();
            continue;
        }
        match scales.get(row).copied().flatten() {
            None => out.append_null(),
            Some(scale) => out.append_value(round_float(values.value(row), scale)),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(out.finish())))
}

fn apply_decimal_bround(
    input: &ArrayRef,
    scale_in: i8,
    scales: &[Option<i32>],
    output_type: &DataType,
    ansi: bool,
) -> Result<ColumnarValue> {
    let values = input
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Execution("bround needs decimal128 values".to_owned())
        })?;
    let DataType::Decimal128(out_precision, out_scale) = output_type else {
        return exec_err!("'bround' decimal output must stay decimal128");
    };
    let mut out: Vec<Option<i128>> = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row) {
            out.push(None);
            continue;
        }
        let scale = scales.get(row).copied().flatten();
        let Some(scale) = scale else {
            out.push(None);
            continue;
        };
        if scale >= i32::from(scale_in) {
            out.push(Some(values.value(row)));
            continue;
        }
        let target = scale.max(0);
        let shift = i32::from(scale_in) - scale;
        if shift > 38 {
            out.push(Some(0));
            continue;
        }
        let back_shift = target - scale;
        let divisor = POW10_I128[usize::try_from(shift).unwrap_or(38)];
        let quotient = round_half_even_quotient(values.value(row), divisor);
        let back = POW10_I128[usize::try_from(back_shift).unwrap_or(0)];
        match quotient.checked_mul(back) {
            Some(exact) => out.push(Some(exact)),
            None => {
                if ansi {
                    return Err(crate::spark_math::overflow_error("bround"));
                }
                out.push(None);
            }
        }
    }
    let shaped =
        Decimal128Array::from_iter(out).with_precision_and_scale(*out_precision, *out_scale)?;
    Ok(ColumnarValue::Array(Arc::new(shaped)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::Int32Type;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn batches_of(ctx: &SessionContext, sql: &str) -> Vec<datafusion::arrow::array::RecordBatch> {
        ctx.sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"))
    }

    #[tokio::test]
    async fn bround_double_ties_even() {
        let ctx = ctx();
        for (sql, want) in [
            ("SELECT bround(CAST(2.5 AS DOUBLE))", 2.0),
            ("SELECT bround(CAST(3.5 AS DOUBLE), 0)", 4.0),
            ("SELECT bround(CAST(-2.5 AS DOUBLE), 0)", -2.0),
            ("SELECT bround(CAST(0.125 AS DOUBLE), 2)", 0.12),
            ("SELECT bround(CAST(2.5 AS DOUBLE), -1)", 0.0),
        ] {
            let batches = batches_of(&ctx, sql).await;
            let values = batches[0].column(0).as_primitive::<Float64Type>().clone();
            assert_eq!(values.value(0), want, "{sql}");
        }
    }

    #[tokio::test]
    async fn bround_integral_shapes() {
        let ctx = ctx();
        let batches = batches_of(&ctx, "SELECT bround(CAST(25 AS INT), -1)").await;
        assert_eq!(
            batches[0].column(0).as_primitive::<Int32Type>().clone().value(0),
            20
        );
        let batches = batches_of(&ctx, "SELECT bround(CAST(2147483647 AS INT), -9)").await;
        assert_eq!(
            batches[0].column(0).as_primitive::<Int32Type>().clone().value(0),
            2_000_000_000
        );
    }

    #[tokio::test]
    async fn bround_decimal_precision() {
        let ctx = ctx();
        let batches = batches_of(&ctx, "SELECT bround(CAST(12345.6789 AS DECIMAL(10,4)), 2)").await;
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &DataType::Decimal128(9, 2)
        );
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal output");
        assert_eq!(values.value(0), 1_234_568);
        let batches = batches_of(&ctx, "SELECT bround(CAST(12345.6789 AS DECIMAL(10,4)), -2)").await;
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &DataType::Decimal128(7, 0)
        );
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal output");
        assert_eq!(values.value(0), 12_300);
    }
}
