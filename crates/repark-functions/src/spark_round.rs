use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float64Type, Int64Type,
};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::ansi::spark_ansi_enabled_from_options;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundMode {
    HalfUp,
    HalfEven,
    Ceiling,
    Floor,
}

impl RoundMode {
    fn name(self) -> &'static str {
        match self {
            Self::HalfUp => "round",
            Self::HalfEven => "bround",
            Self::Ceiling => "ceil",
            Self::Floor => "floor",
        }
    }
}

#[must_use]
pub fn round_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkScaleRound::new(
        "round",
        RoundMode::HalfUp,
    )))
}

#[must_use]
pub fn bround_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkScaleRound::new(
        "bround",
        RoundMode::HalfEven,
    )))
}

#[must_use]
pub fn ceil_udf() -> Arc<ScalarUDF> {
    Arc::new(
        ScalarUDF::from(SparkScaleRound::new("ceil", RoundMode::Ceiling))
            .with_aliases(["ceiling", "__repark_ceil__"]),
    )
}

#[must_use]
pub fn floor_udf() -> Arc<ScalarUDF> {
    Arc::new(
        ScalarUDF::from(SparkScaleRound::new("floor", RoundMode::Floor))
            .with_aliases(["__repark_floor__"]),
    )
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![round_udf(), bround_udf(), ceil_udf(), floor_udf()]
}

#[derive(Debug)]
struct SparkScaleRound {
    signature: Signature,
    name: &'static str,
    mode: RoundMode,
}

impl SparkScaleRound {
    fn new(name: &'static str, mode: RoundMode) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            name,
            mode,
        }
    }
}

impl PartialEq for SparkScaleRound {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.mode == other.mode
    }
}

impl Eq for SparkScaleRound {}

impl Hash for SparkScaleRound {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.mode.name().hash(state);
    }
}

fn is_utf8_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn decimal_parts(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => Some((*p, *s)),
        _ => None,
    }
}

fn is_integer_family(data_type: &DataType) -> bool {
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

fn for_type_decimal(data_type: &DataType) -> Option<DataType> {
    let (precision, scale) = match data_type {
        DataType::Int8 | DataType::UInt8 => (3, 0),
        DataType::Int16 | DataType::UInt16 => (5, 0),
        DataType::Int32 | DataType::UInt32 | DataType::Null => (10, 0),
        DataType::Int64 | DataType::UInt64 => (20, 0),
        DataType::Float32 => (14, 7),
        DataType::Float64 => (30, 15),
        _ => return None,
    };
    Some(DataType::Decimal128(precision, scale))
}

fn unexpected(name: &str, required: &str, got: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}(<expr>)\" due to \
         data type mismatch: The first parameter requires the \"{required}\" type, however \
         the argument has the type \"{got}\"."
    ))
}

fn scale_argument(args: &ReturnFieldArgs<'_>) -> i32 {
    match args.scalar_arguments.get(1) {
        Some(Some(ScalarValue::Int8(Some(v)))) => i32::from(*v),
        Some(Some(ScalarValue::Int16(Some(v)))) => i32::from(*v),
        Some(Some(ScalarValue::Int32(Some(v)))) => *v,
        Some(Some(ScalarValue::Int64(Some(v)))) => i32::try_from(*v).unwrap_or(0),
        _ => 0,
    }
}

fn spark_round_decimal_type(precision: u8, scale: i8, target: i32) -> (u8, i8) {
    let integral = i32::from(precision) - i32::from(scale) + 1;
    if target < 0 {
        let new_precision = integral.max(-target + 1).min(38);
        (u8::try_from(new_precision).unwrap_or(38), 0)
    } else {
        let new_scale = i32::from(scale).min(target);
        let new_precision = (integral + new_scale).min(38);
        (
            u8::try_from(new_precision).unwrap_or(38),
            i8::try_from(new_scale).unwrap_or(38),
        )
    }
}

fn pow10_i128(exponent: u32) -> Option<i128> {
    if exponent > 38 {
        return None;
    }
    Some(10i128.pow(exponent))
}

fn div_round(value: i128, divisor: i128, mode: RoundMode) -> i128 {
    if divisor == 0 {
        return value;
    }
    let quotient = value / divisor;
    let remainder = value % divisor;
    if remainder == 0 {
        return quotient;
    }
    let step = if value < 0 { -1i128 } else { 1i128 };
    match mode {
        RoundMode::Ceiling => {
            if value > 0 {
                quotient + 1
            } else {
                quotient
            }
        }
        RoundMode::Floor => {
            if value < 0 {
                quotient - 1
            } else {
                quotient
            }
        }
        RoundMode::HalfUp => {
            if remainder.abs() * 2 >= divisor.abs() {
                quotient + step
            } else {
                quotient
            }
        }
        RoundMode::HalfEven => {
            let twice = remainder.abs() * 2;
            if twice > divisor.abs() || (twice == divisor.abs() && quotient % 2 != 0) {
                quotient + step
            } else {
                quotient
            }
        }
    }
}

fn decimal_mantissa_bounds(precision: u8) -> Option<i128> {
    pow10_i128(u32::from(precision))
}

fn round_decimal_mantissa(
    value: i128,
    input_scale: i32,
    scale: i32,
    output_precision: u8,
    output_scale: i32,
    mode: RoundMode,
) -> Option<i128> {
    let drop = if scale < 0 {
        input_scale.checked_add(-scale)?
    } else {
        input_scale - output_scale
    };
    if drop <= 0 {
        return Some(value);
    }
    let quotient = match u32::try_from(drop).ok().and_then(pow10_i128) {
        Some(divisor) => div_round(value, divisor, mode),
        None => 0,
    };
    let mantissa = if scale < 0 {
        match u32::try_from(-scale).ok().and_then(pow10_i128) {
            Some(factor) => quotient.checked_mul(factor)?,
            None => 0,
        }
    } else {
        quotient
    };
    let bound = decimal_mantissa_bounds(output_precision)?;
    if mantissa.abs() >= bound {
        return None;
    }
    Some(mantissa)
}

#[allow(clippy::cast_possible_truncation)]
fn round_i64(value: i64, scale: i32, mode: RoundMode, ansi: bool) -> Result<i64> {
    if scale >= 0 {
        return Ok(value);
    }
    let shift = u32::try_from(-scale).unwrap_or(u32::MAX);
    let Some(divisor) = 10i64.checked_pow(shift) else {
        return Ok(0);
    };
    let quotient = div_round(i128::from(value), i128::from(divisor), mode);
    let Some(product) = quotient.checked_mul(i128::from(divisor)) else {
        return Ok(0);
    };
    if ansi {
        i64::try_from(product).map_err(|_| overflow_error("bigint"))
    } else {
        Ok(product as i64)
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]
fn round_f64(value: f64, scale: i32, mode: RoundMode) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let text = format!("{value:.1200}");
    let (negative, digits) = text
        .strip_prefix('-')
        .map_or((false, text.as_str()), |rest| (true, rest));
    let Some((int_part, frac_part)) = digits.split_once('.') else {
        return value;
    };
    let all: Vec<u8> = int_part
        .bytes()
        .chain(frac_part.bytes())
        .map(|b| b - b'0')
        .collect();
    let point = int_part.len() as i64;
    let keep = point + i64::from(scale);
    if keep >= all.len() as i64 {
        return value;
    }
    let mut kept: Vec<u8> = if keep <= 0 {
        Vec::new()
    } else {
        all[..keep as usize].to_vec()
    };
    let dropped = &all[keep.max(0) as usize..];
    let any_dropped = dropped.iter().any(|digit| *digit != 0);
    let increment = match mode {
        RoundMode::HalfUp => dropped.first().is_some_and(|digit| *digit >= 5),
        RoundMode::HalfEven => {
            dropped.first().is_some_and(|digit| *digit > 5)
                || (dropped.first() == Some(&5)
                    && (dropped[1..].iter().any(|digit| *digit != 0)
                        || kept.last().is_some_and(|digit| digit % 2 == 1)))
        }
        RoundMode::Ceiling => any_dropped && !negative,
        RoundMode::Floor => any_dropped && negative,
    };
    if increment {
        let mut carry = 1u8;
        for digit in kept.iter_mut().rev() {
            *digit += carry;
            if *digit >= 10 {
                *digit -= 10;
            } else {
                carry = 0;
                break;
            }
        }
        if carry == 1 {
            kept.insert(0, 1);
        }
    }
    let int_digits = (point - keep).max(0) as usize;
    let frac_digits = (keep - point).max(0) as usize;
    let mut body = String::with_capacity(kept.len() + int_digits + 1);
    for digit in &kept {
        body.push(char::from(b'0' + *digit));
    }
    body.extend(std::iter::repeat_n('0', int_digits));
    let mut out = String::new();
    if negative && body.bytes().any(|byte| byte != b'0') {
        out.push('-');
    }
    if frac_digits > 0 {
        let split = body.len() - frac_digits;
        out.push_str(&body[..split]);
        out.push('.');
        out.push_str(&body[split..]);
    } else {
        out.push_str(&body);
    }
    if out.is_empty() || out == "-" {
        out.push('0');
    }
    out.parse::<f64>().unwrap_or(0.0)
}

fn overflow_error(kind: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[ARITHMETIC_OVERFLOW] {kind} overflow. If necessary set \"spark.sql.ansi.enabled\" \
         to \"false\" to bypass this error."
    ))
}

fn non_foldable_scale(name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] Cannot resolve \"{name}(<expr>)\" due to data \
         type mismatch: The input \"scale\" should be a foldable \"INT\" expression."
    ))
}

fn scale_value(args: &ScalarFunctionArgs, name: &str) -> Result<Option<i32>> {
    if args.args.len() < 2 {
        return Ok(Some(0));
    }
    match &args.args[1] {
        ColumnarValue::Scalar(scalar) => {
            if scalar.is_null() {
                return Ok(None);
            }
            let casted = cast(&scalar.to_array()?, &DataType::Int64)?;
            let ints = casted.as_primitive::<Int64Type>();
            let value = ints.value(0);
            i32::try_from(value)
                .map(Some)
                .map_err(|_| overflow_error("int"))
        }
        ColumnarValue::Array(_) => Err(non_foldable_scale(name)),
    }
}

fn decimal_result(
    args: &ScalarFunctionArgs,
    scale: Option<i32>,
    mode: RoundMode,
) -> Result<ArrayRef> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[..1])?;
    let input = &arrays[0];
    let DataType::Decimal128(out_p, out_s) = args.return_field.data_type() else {
        return exec_err!(
            "'{}' expected a decimal return field",
            args.return_field.name()
        );
    };
    let out = (*out_p, *out_s);
    let Some((input_precision, input_scale)) = decimal_parts(input.data_type()) else {
        return exec_err!(
            "'{}' expected decimal input after coercion",
            args.return_field.name()
        );
    };
    let widened: ArrayRef = if matches!(input.data_type(), DataType::Decimal128(_, _)) {
        Arc::clone(input)
    } else {
        cast(
            input.as_ref(),
            &DataType::Decimal128(input_precision, input_scale),
        )?
    };
    decimal_typed(&widened, i32::from(input_scale), scale, out, mode)
}

fn decimal_typed(
    input: &ArrayRef,
    input_scale: i32,
    scale: Option<i32>,
    out: (u8, i8),
    mode: RoundMode,
) -> Result<ArrayRef> {
    let typed = input.as_primitive::<Decimal128Type>();
    let mut values: Vec<Option<i128>> = Vec::with_capacity(typed.len());
    for row in 0..typed.len() {
        if typed.is_null(row) {
            values.push(None);
            continue;
        }
        let Some(scale) = scale else {
            values.push(None);
            continue;
        };
        values.push(round_decimal_mantissa(
            typed.value(row),
            input_scale,
            scale,
            out.0,
            i32::from(out.1),
            mode,
        ));
    }
    let array = datafusion::arrow::array::Decimal128Array::from(values)
        .with_precision_and_scale(out.0, out.1)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    Ok(Arc::new(array))
}

fn integral_result(
    args: &ScalarFunctionArgs,
    scale: Option<i32>,
    mode: RoundMode,
    ansi: bool,
) -> Result<ArrayRef> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[..1])?;
    let input = cast(&arrays[0], &DataType::Int64)?;
    let ints = input.as_primitive::<Int64Type>();
    let mut values: Vec<Option<i64>> = Vec::with_capacity(ints.len());
    for row in 0..ints.len() {
        if ints.is_null(row) {
            values.push(None);
            continue;
        }
        match scale {
            None => values.push(None),
            Some(scale) => values.push(Some(round_i64(ints.value(row), scale, mode, ansi)?)),
        }
    }
    let result = Int64Array::from(values);
    Ok(cast(&result, args.return_field.data_type())?)
}

fn float_result(
    args: &ScalarFunctionArgs,
    scale: Option<i32>,
    mode: RoundMode,
) -> Result<ArrayRef> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[..1])?;
    let input = cast(&arrays[0], &DataType::Float64)?;
    let doubles = input.as_primitive::<Float64Type>();
    let mut values: Vec<Option<f64>> = Vec::with_capacity(doubles.len());
    for row in 0..doubles.len() {
        if doubles.is_null(row) {
            values.push(None);
            continue;
        }
        match scale {
            None => values.push(None),
            Some(scale) => values.push(Some(round_f64(doubles.value(row), scale, mode))),
        }
    }
    let result = datafusion::arrow::array::Float64Array::from(values);
    if args.return_field.data_type() == &DataType::Float32 {
        let narrowed = cast(&result, &DataType::Float32)?;
        return Ok(narrowed);
    }
    Ok(Arc::new(result))
}

#[allow(clippy::cast_possible_truncation)]
fn unary_ceil_floor(input: &ArrayRef, mode: RoundMode, out: (u8, i8)) -> Result<ArrayRef> {
    if let Some((precision, scale)) = decimal_parts(input.data_type()) {
        let widened = cast(input.as_ref(), &DataType::Decimal128(precision, scale))?;
        return decimal_typed(&widened, i32::from(scale), Some(0), out, mode);
    }
    if is_integer_family(input.data_type()) {
        return Ok(cast(input.as_ref(), &DataType::Int64)?);
    }
    let doubles = cast(input.as_ref(), &DataType::Float64)?;
    let doubles = doubles.as_primitive::<Float64Type>();
    let mut values: Vec<Option<i64>> = Vec::with_capacity(doubles.len());
    for row in 0..doubles.len() {
        if doubles.is_null(row) {
            values.push(None);
            continue;
        }
        let value = doubles.value(row);
        let rounded = match mode {
            RoundMode::Ceiling => value.ceil(),
            _ => value.floor(),
        };
        values.push(Some(rounded as i64));
    }
    Ok(Arc::new(Int64Array::from(values)))
}

impl ScalarUDFImpl for SparkScaleRound {
    fn name(&self) -> &'static str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let field = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("'{}' expects at least one argument", self.name))
        })?;
        let two_arg_scale = args.arg_fields.len() >= 2;
        let scale = scale_argument(&args);
        let data_type = match self.mode {
            RoundMode::HalfUp | RoundMode::HalfEven => {
                if let Some((p, s)) = decimal_parts(field.data_type()) {
                    let (p, s) = spark_round_decimal_type(p, s, scale);
                    DataType::Decimal128(p, s)
                } else if is_utf8_family(field.data_type()) || field.data_type() == &DataType::Null
                {
                    DataType::Float64
                } else {
                    field.data_type().clone()
                }
            }
            RoundMode::Ceiling | RoundMode::Floor => {
                if two_arg_scale {
                    let (p, s) = decimal_parts(field.data_type())
                        .ok_or_else(|| unexpected(self.name, "NUMERIC", field.data_type()))?;
                    let (p, s) = spark_round_decimal_type(p, s, scale);
                    DataType::Decimal128(p, s)
                } else if let Some((p, s)) = decimal_parts(field.data_type()) {
                    if s == 0 {
                        DataType::Decimal128(p, 0)
                    } else {
                        let precision = (i32::from(p) - i32::from(s) + 1).min(38);
                        DataType::Decimal128(u8::try_from(precision).unwrap_or(38), 0)
                    }
                } else {
                    DataType::Int64
                }
            }
        };
        Ok(Arc::new(Field::new(self.name, data_type, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let numeric = |data_type: &DataType| {
            data_type.is_numeric()
                || decimal_parts(data_type).is_some()
                || is_utf8_family(data_type)
                || data_type == &DataType::Null
        };
        match (self.mode, arg_types.len()) {
            (RoundMode::HalfUp | RoundMode::HalfEven, 1) => {
                let data_type = &arg_types[0];
                if !numeric(data_type) {
                    return Err(unexpected(self.name, "NUMERIC", data_type));
                }
                if data_type == &DataType::Null || is_utf8_family(data_type) {
                    return Ok(vec![DataType::Float64]);
                }
                Ok(vec![data_type.clone()])
            }
            (RoundMode::HalfUp | RoundMode::HalfEven, 2) => {
                let data_type = &arg_types[0];
                if !numeric(data_type) {
                    return Err(unexpected(self.name, "NUMERIC", data_type));
                }
                if !arg_types[1].is_integer() && arg_types[1] != DataType::Null {
                    return Err(unexpected(self.name, "INT", &arg_types[1]));
                }
                let value = if data_type == &DataType::Null || is_utf8_family(data_type) {
                    DataType::Float64
                } else {
                    data_type.clone()
                };
                Ok(vec![value, DataType::Int32])
            }
            (RoundMode::Ceiling | RoundMode::Floor, 1) => {
                let data_type = &arg_types[0];
                if !numeric(data_type) {
                    return Err(unexpected(self.name, "NUMERIC", data_type));
                }
                if decimal_parts(data_type).is_some() || is_integer_family(data_type) {
                    return Ok(vec![data_type.clone()]);
                }
                if matches!(
                    data_type,
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                ) || is_utf8_family(data_type)
                {
                    return Ok(vec![DataType::Float64]);
                }
                Ok(vec![DataType::Int64])
            }
            (RoundMode::Ceiling | RoundMode::Floor, 2) => {
                let data_type = &arg_types[0];
                if !numeric(data_type) {
                    return Err(unexpected(self.name, "NUMERIC", data_type));
                }
                if !arg_types[1].is_integer() && arg_types[1] != DataType::Null {
                    return Err(unexpected(self.name, "INT", &arg_types[1]));
                }
                let target = decimal_parts(data_type)
                    .map(|(p, s)| DataType::Decimal128(p, s))
                    .or_else(|| for_type_decimal(data_type))
                    .unwrap_or(DataType::Decimal128(38, 18));
                Ok(vec![target, DataType::Int32])
            }
            _ => Err(DataFusionError::Plan(format!(
                "'{}' expects one or two arguments, got {}",
                self.name,
                arg_types.len()
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scale = scale_value(&args, self.name)?;
        let input_type = args.arg_fields[0].data_type().clone();
        let one_arg_unary =
            matches!(self.mode, RoundMode::Ceiling | RoundMode::Floor) && args.args.len() == 1;
        let arrays = ColumnarValue::values_to_arrays(&args.args[..1])?;
        let input = &arrays[0];
        if one_arg_unary && decimal_parts(&input_type).is_none() {
            return Ok(ColumnarValue::Array(unary_ceil_floor(
                input,
                self.mode,
                (20, 0),
            )?));
        }
        if one_arg_unary && decimal_parts(&input_type).is_some_and(|(_, s)| s == 0) {
            return Ok(ColumnarValue::Array(cast(
                input.as_ref(),
                args.return_field.data_type(),
            )?));
        }
        if decimal_parts(&input_type).is_some() {
            return Ok(ColumnarValue::Array(decimal_result(
                &args, scale, self.mode,
            )?));
        }
        if matches!(
            input_type,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        ) {
            return Ok(ColumnarValue::Array(float_result(&args, scale, self.mode)?));
        }
        if is_integer_family(&input_type) {
            let ansi = spark_ansi_enabled_from_options(&args.config_options);
            return Ok(ColumnarValue::Array(integral_result(
                &args, scale, self.mode, ansi,
            )?));
        }
        if input_type == DataType::Null {
            let len = arrays[0].len();
            return Ok(ColumnarValue::Array(
                datafusion::arrow::array::new_null_array(args.return_field.data_type(), len),
            ));
        }
        exec_err!("'{}' unsupported input type {input_type}", self.name)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::ScalarValue;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> (ScalarValue, String) {
        let frame = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"));
        let field_type = frame.schema().field(0).data_type().to_string();
        let batches = frame
            .collect()
            .await
            .unwrap_or_else(|error| panic!("exec {sql}: {error}"));
        let value = ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0)
            .unwrap_or_else(|error| panic!("scalar {sql}: {error}"));
        (value, field_type)
    }

    #[tokio::test]
    async fn round_decimal_scale_types_match_spark() {
        let ctx = ctx();
        let (value, ty) = one(&ctx, "SELECT round(CAST(2.345 AS DECIMAL(4,3)), 2)").await;
        assert_eq!(ty, "Decimal128(4, 2)");
        assert_eq!(value, ScalarValue::Decimal128(Some(235), 4, 2));
        let (value, ty) = one(&ctx, "SELECT round(CAST(1234.5 AS DECIMAL(5,1)), -2)").await;
        assert_eq!(ty, "Decimal128(5, 0)");
        assert_eq!(value, ScalarValue::Decimal128(Some(1200), 5, 0));
        let (value, ty) = one(&ctx, "SELECT round(CAST(2.5 AS DECIMAL(3,1)))").await;
        assert_eq!(ty, "Decimal128(3, 0)");
        assert_eq!(value, ScalarValue::Decimal128(Some(3), 3, 0));
        let (_, ty) = one(&ctx, "SELECT round(CAST(12345.6789 AS DECIMAL(38,4)), 2)").await;
        assert_eq!(ty, "Decimal128(37, 2)");
        let (value, _) = one(&ctx, "SELECT round(CAST(12345.6789 AS DECIMAL(38,4)), 2)").await;
        assert_eq!(value, ScalarValue::Decimal128(Some(1_234_568), 37, 2));
        let (_, ty) = one(&ctx, "SELECT round(CAST(12345.6789 AS DECIMAL(38,4)), -2)").await;
        assert_eq!(ty, "Decimal128(35, 0)");
    }

    #[tokio::test]
    async fn bround_is_half_even_and_keeps_width() {
        let ctx = ctx();
        let (value, ty) = one(&ctx, "SELECT bround(CAST(2.5 AS DECIMAL(2,1)), 0)").await;
        assert_eq!(ty, "Decimal128(2, 0)");
        assert_eq!(value, ScalarValue::Decimal128(Some(2), 2, 0));
        let (value, _) = one(&ctx, "SELECT bround(CAST(3.5 AS DECIMAL(2,1)), 0)").await;
        assert_eq!(value, ScalarValue::Decimal128(Some(4), 2, 0));
        let (value, ty) = one(&ctx, "SELECT bround(CAST(1250 AS INT), -2)").await;
        assert_eq!(ty, "Int32");
        assert_eq!(value, ScalarValue::Int32(Some(1200)));
        let (value, ty) = one(&ctx, "SELECT bround(CAST(1350 AS INT), -2)").await;
        assert_eq!(ty, "Int32");
        assert_eq!(value, ScalarValue::Int32(Some(1400)));
        let (value, ty) = one(&ctx, "SELECT bround(CAST(2.5 AS DOUBLE), 0)").await;
        assert_eq!(ty, "Float64");
        assert_eq!(value, ScalarValue::Float64(Some(2.0)));
    }

    #[tokio::test]
    async fn round_double_and_int_follow_spark() {
        let ctx = ctx();
        let (value, ty) = one(&ctx, "SELECT round(CAST(2.5 AS DOUBLE))").await;
        assert_eq!(ty, "Float64");
        assert_eq!(value, ScalarValue::Float64(Some(3.0)));
        let (value, _) = one(&ctx, "SELECT round(CAST(-2.5 AS DOUBLE))").await;
        assert_eq!(value, ScalarValue::Float64(Some(-3.0)));
        let (value, _) = one(&ctx, "SELECT round(CAST(0.125 AS DOUBLE), 2)").await;
        assert_eq!(value, ScalarValue::Float64(Some(0.13)));
        let (value, _) = one(&ctx, "SELECT round(CAST(2.5 AS DOUBLE), -1)").await;
        assert_eq!(value, ScalarValue::Float64(Some(0.0)));
        let (value, ty) = one(&ctx, "SELECT round(CAST(125 AS INT), -1)").await;
        assert_eq!(ty, "Int32");
        assert_eq!(value, ScalarValue::Int32(Some(130)));
        let (value, ty) = one(&ctx, "SELECT round(CAST(125 AS BIGINT), -1)").await;
        assert_eq!(ty, "Int64");
        assert_eq!(value, ScalarValue::Int64(Some(130)));
        let (value, ty) = one(&ctx, "SELECT round(CAST(2.5 AS FLOAT), 0)").await;
        assert_eq!(ty, "Float32");
        assert_eq!(value, ScalarValue::Float32(Some(3.0)));
        let (value, ty) = one(
            &ctx,
            "SELECT round(CAST(1234.5 AS DECIMAL(5,1)), CAST(NULL AS INT))",
        )
        .await;
        assert_eq!(ty, "Decimal128(5, 0)");
        assert_eq!(value, ScalarValue::Decimal128(None, 5, 0));
    }

    #[tokio::test]
    async fn ceil_floor_scale_and_unary_forms() {
        let ctx = ctx();
        let (value, ty) = one(&ctx, "SELECT ceil(CAST(1.25 AS DECIMAL(3,2)))").await;
        assert_eq!(ty, "Decimal128(2, 0)");
        assert_eq!(value, ScalarValue::Decimal128(Some(2), 2, 0));
        let (value, ty) = one(&ctx, "SELECT ceil(CAST(1.25 AS DECIMAL(4,2)))").await;
        assert_eq!(ty, "Decimal128(3, 0)");
        assert_eq!(value, ScalarValue::Decimal128(Some(2), 3, 0));
        let (value, ty) = one(&ctx, "SELECT ceil(CAST(-1.5 AS DOUBLE))").await;
        assert_eq!(ty, "Int64");
        assert_eq!(value, ScalarValue::Int64(Some(-1)));
        let (value, ty) = one(
            &ctx,
            "SELECT __repark_ceil__(CAST(1.2345 AS DECIMAL(5,4)), 2)",
        )
        .await;
        assert_eq!(ty, "Decimal128(4, 2)");
        assert_eq!(value, ScalarValue::Decimal128(Some(124), 4, 2));
        let (value, ty) = one(
            &ctx,
            "SELECT __repark_floor__(CAST(-1.2345 AS DECIMAL(5,4)), 2)",
        )
        .await;
        assert_eq!(ty, "Decimal128(4, 2)");
        assert_eq!(value, ScalarValue::Decimal128(Some(-124), 4, 2));
        let (value, ty) = one(&ctx, "SELECT ceiling(CAST(2.1 AS DOUBLE))").await;
        assert_eq!(ty, "Int64");
        assert_eq!(value, ScalarValue::Int64(Some(3)));
        let (value, ty) = one(&ctx, "SELECT floor(CAST(-1.25 AS DECIMAL(4,2)))").await;
        assert_eq!(ty, "Decimal128(3, 0)");
        assert_eq!(value, ScalarValue::Decimal128(Some(-2), 3, 0));
        let (value, ty) = one(&ctx, "SELECT __repark_ceil__(CAST(125 AS BIGINT), -1)").await;
        assert_eq!(ty, "Decimal128(21, 0)");
        assert_eq!(value, ScalarValue::Decimal128(Some(130), 21, 0));
        let (value, _) = one(&ctx, "SELECT __repark_floor__(CAST(125 AS BIGINT), -2)").await;
        assert_eq!(value, ScalarValue::Decimal128(Some(100), 21, 0));
        let (value, ty) = one(
            &ctx,
            "SELECT __repark_ceil__(CAST(1234.5 AS DECIMAL(5,1)), -2)",
        )
        .await;
        assert_eq!(ty, "Decimal128(5, 0)");
        assert_eq!(value, ScalarValue::Decimal128(Some(1300), 5, 0));
        let (value, ty) = one(&ctx, "SELECT __repark_floor__(CAST(1.5 AS DOUBLE), 1)").await;
        assert_eq!(ty, "Decimal128(17, 1)");
        assert_eq!(value, ScalarValue::Decimal128(Some(15), 17, 1));
        let (value, ty) = one(&ctx, "SELECT __repark_ceil__(CAST(2.5 AS DOUBLE), 0)").await;
        assert_eq!(ty, "Decimal128(16, 0)");
        assert_eq!(value, ScalarValue::Decimal128(Some(3), 16, 0));
        let (value, ty) = one(&ctx, "SELECT __repark_ceil__(CAST(5 AS INT), -1)").await;
        assert_eq!(ty, "Decimal128(11, 0)");
        assert_eq!(value, ScalarValue::Decimal128(Some(10), 11, 0));
        let (value, ty) = one(
            &ctx,
            "SELECT __repark_ceil__(CAST(NULL AS DECIMAL(4,2)), 1)",
        )
        .await;
        assert_eq!(ty, "Decimal128(4, 1)");
        assert_eq!(value, ScalarValue::Decimal128(None, 4, 1));
    }
}
