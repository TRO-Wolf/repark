use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, PrimitiveArray, PrimitiveBuilder, StringArray,
};
use datafusion::arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::{CastOptions, binary, cast, cast_with_options, try_unary, unary};
use datafusion::arrow::datatypes::{
    ArrowNativeTypeOp, ArrowPrimitiveType, DataType, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, Field, FieldRef, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

mod bround;
pub use bround::{bround_udf, call_bround};
mod conv;
pub use conv::{call_conv, conv_udf};

#[must_use]
pub fn abs_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkAbs::new()))
}

#[must_use]
pub fn hypot_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkHypot::new()))
}

#[must_use]
pub fn bin_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkBin::new()))
}

#[must_use]
pub fn rint_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRint::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        abs_udf(),
        hypot_udf(),
        bin_udf(),
        rint_udf(),
        bround_udf(),
        conv_udf(),
    ]
}

#[derive(Debug)]
struct SparkAbs {
    signature: Signature,
}

impl SparkAbs {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkAbs {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkAbs {}

impl Hash for SparkAbs {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkAbs {
    crate::shim_udf_boilerplate!("abs");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let data_type = arg_types.first().ok_or_else(|| {
            DataFusionError::Plan("'abs' expects one numeric argument".to_string())
        })?;
        if is_utf8_family(data_type) {
            return Ok(DataType::Float64);
        }
        Ok(data_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let field = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan("'abs' expects one numeric argument".to_string())
        })?;
        let string_input = is_utf8_family(field.data_type());
        Ok(Arc::new(Field::new(
            "abs",
            if string_input {
                DataType::Float64
            } else {
                field.data_type().clone()
            },
            field.is_nullable() || string_input,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type] if data_type.is_numeric() || is_utf8_family(data_type) => {
                Ok(vec![data_type.clone()])
            }
            [DataType::Null] => Ok(vec![DataType::Int32]),
            [data_type] => Err(unexpected_input_type("abs", "NUMERIC", data_type)),
            _ => exec_err!("'abs' expects one argument, got {}", arg_types.len()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(arg) = args.args.first() else {
            return exec_err!("'abs' expects one argument");
        };
        let ansi = crate::ansi::spark_ansi_enabled_from_options(&args.config_options);
        let array = arg.to_array(args.number_rows)?;
        if matches!(
            array.data_type(),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        ) {
            return Ok(ColumnarValue::Array(array));
        }
        Ok(ColumnarValue::Array(abs_typed(array.as_ref(), ansi)?))
    }
}

#[derive(Debug)]
struct SparkHypot {
    signature: Signature,
}

impl SparkHypot {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkHypot {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkHypot {}

impl Hash for SparkHypot {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkHypot {
    crate::shim_udf_boilerplate!("hypot");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new("hypot", DataType::Float64, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [left, right] if acceptable_double(left) && acceptable_double(right) => {
                Ok(vec![DataType::Float64, DataType::Float64])
            }
            [left, right] => {
                let bad = if acceptable_double(left) { right } else { left };
                Err(unexpected_input_type("hypot", "DOUBLE", bad))
            }
            _ => exec_err!("'hypot' expects two arguments, got {}", arg_types.len()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [left, right] = arrays.as_slice() else {
            return exec_err!("'hypot' expects two arguments");
        };
        let left = float64_values(left)?;
        let right = float64_values(right)?;
        let out = binary::<Float64Type, Float64Type, _, Float64Type>(&left, &right, f64::hypot)?;
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[derive(Debug)]
struct SparkBin {
    signature: Signature,
}

impl SparkBin {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkBin {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkBin {}

impl Hash for SparkBin {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkBin {
    crate::shim_udf_boilerplate!("bin");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new("bin", DataType::Utf8, nullable)))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let parts: Vec<String> = args.iter().map(crate::expr_fn::spark_expr_token).collect();
        Ok(format!("bin({})", parts.join(", ")))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type]
                if data_type.is_numeric()
                    || matches!(
                        unwrap_dict(data_type),
                        DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ) =>
            {
                Ok(vec![DataType::Int64])
            }
            [data_type] => Err(unexpected_input_type("bin", "BIGINT", data_type)),
            _ => exec_err!("'bin' expects one argument, got {}", arg_types.len()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(arg) = args.args.first() else {
            return exec_err!("'bin' expects one argument");
        };
        let array = arg.to_array(args.number_rows)?;
        let ints: ArrayRef = cast(&array, &DataType::Int64)?;
        let ints = ints.as_primitive::<Int64Type>();
        let mut offsets = Vec::with_capacity(ints.len() + 1);
        let mut values = Vec::new();
        let mut digits = [0u8; 64];
        offsets.push(0i32);
        for index in 0..ints.len() {
            if !ints.is_null(index) {
                values.extend_from_slice(bin_render(ints.value(index), &mut digits));
            }
            offsets.push(i32::try_from(values.len()).map_err(|_| {
                DataFusionError::Execution("'bin' output exceeds i32 offsets".to_string())
            })?);
        }
        let rendered = StringArray::try_new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from(values),
            ints.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(rendered)))
    }
}

#[derive(Debug)]
struct SparkRint {
    signature: Signature,
}

impl SparkRint {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkRint {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkRint {}

impl Hash for SparkRint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkRint {
    crate::shim_udf_boilerplate!("rint");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new("rint", DataType::Float64, true)))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let parts: Vec<String> = args.iter().map(crate::expr_fn::spark_expr_token).collect();
        Ok(format!("rint({})", parts.join(", ")))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type] if acceptable_double(data_type) => Ok(vec![DataType::Float64]),
            [data_type] => Err(unexpected_input_type("rint", "DOUBLE", data_type)),
            _ => exec_err!("'rint' expects one argument, got {}", arg_types.len()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(arg) = args.args.first() else {
            return exec_err!("'rint' expects one argument");
        };
        let array = arg.to_array(args.number_rows)?;
        let doubles = float64_values(&array)?;
        let out = unary::<Float64Type, _, Float64Type>(&doubles, f64::round_ties_even);
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

fn unwrap_dict(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => unwrap_dict(value),
        other => other,
    }
}

fn acceptable_double(data_type: &DataType) -> bool {
    matches!(
        unwrap_dict(data_type),
        DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) || unwrap_dict(data_type).is_numeric()
}

pub(crate) fn unexpected_input_type(name: &str, required: &str, got: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}(<expr>)\" due to \
         data type mismatch: The first parameter requires the \"{required}\" type, however \
         the argument has the type \"{}\".",
        spark_type_name(got)
    ))
}

pub(crate) fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            "BIGINT".to_string()
        }
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BINARY".to_string(),
        other => format!("{other}").to_uppercase(),
    }
}

fn float64_values(array: &ArrayRef) -> Result<PrimitiveArray<Float64Type>> {
    if array.data_type() == &DataType::Float64 {
        return Ok(array.as_primitive::<Float64Type>().clone());
    }
    let casted = cast(array, &DataType::Float64)?;
    Ok(casted.as_primitive::<Float64Type>().clone())
}

pub(crate) fn overflow_error(kind: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[ARITHMETIC_OVERFLOW] {kind} overflow. If necessary set \"spark.sql.ansi.enabled\" \
         to \"false\" to bypass this error."
    ))
}

fn abs_typed(array: &dyn Array, ansi: bool) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => abs_utf8(array, ansi),
        DataType::Int8 => abs_primitive::<Int8Type>(array, ansi, "byte"),
        DataType::Int16 => abs_primitive::<Int16Type>(array, ansi, "short"),
        DataType::Int32 => abs_primitive::<Int32Type>(array, ansi, "integer"),
        DataType::Int64 => abs_primitive::<Int64Type>(array, ansi, "long"),
        DataType::Float32 => Ok(Arc::new(unary::<Float32Type, _, Float32Type>(
            array.as_primitive(),
            f32::abs,
        ))),
        DataType::Float64 => Ok(Arc::new(unary::<Float64Type, _, Float64Type>(
            array.as_primitive(),
            f64::abs,
        ))),
        DataType::Decimal32(_, _) => abs_primitive::<Decimal32Type>(array, ansi, "decimal"),
        DataType::Decimal64(_, _) => abs_primitive::<Decimal64Type>(array, ansi, "decimal"),
        DataType::Decimal128(_, _) => abs_primitive::<Decimal128Type>(array, ansi, "decimal"),
        DataType::Decimal256(_, _) => abs_primitive::<Decimal256Type>(array, ansi, "decimal"),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Ok(datafusion::arrow::array::make_array(array.to_data()))
        }
        DataType::Null => Ok(datafusion::arrow::array::new_null_array(
            &DataType::Int32,
            array.len(),
        )),
        other => exec_err!("'abs' on unsupported type {other}"),
    }
}

fn abs_utf8(array: &dyn Array, ansi: bool) -> Result<ArrayRef> {
    let options = CastOptions {
        safe: true,
        ..CastOptions::default()
    };
    let casted = cast_with_options(array, &DataType::Float64, &options)?;
    let doubles = casted.as_primitive::<Float64Type>();
    if ansi {
        for row in 0..array.len() {
            if !array.is_null(row) && doubles.is_null(row) {
                return Err(malformed_double_cast(&utf8_row_value(array, row)));
            }
        }
    }
    Ok(Arc::new(unary::<Float64Type, _, Float64Type>(
        doubles,
        f64::abs,
    )))
}

fn utf8_row_value(array: &dyn Array, row: usize) -> String {
    cast(array, &DataType::Utf8).map_or_else(
        |_| "invalid".to_string(),
        |utf8| {
            utf8.as_any().downcast_ref::<StringArray>().map_or_else(
                || "invalid".to_string(),
                |values| values.value(row).to_string(),
            )
        },
    )
}

fn malformed_double_cast(value: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"STRING\" cannot be cast to \
         \"DOUBLE\" because it is malformed. Correct the value as per the syntax, or change its \
         target type. Use `try_cast` to tolerate malformed input and return NULL instead."
    ))
}

fn is_utf8_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn abs_primitive<T: ArrowPrimitiveType>(
    array: &dyn Array,
    ansi: bool,
    kind: &str,
) -> Result<ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    let primitive = array.as_primitive::<T>();
    if !ansi {
        let result = unary::<T, _, T>(primitive, |value| {
            if value.is_lt(T::Native::ZERO) {
                value.neg_wrapping()
            } else {
                value
            }
        });
        return Ok(Arc::new(result.with_data_type(array.data_type().clone())));
    }
    if let Ok(result) = try_unary::<T, _, T>(primitive, |value| {
        if value.is_lt(T::Native::ZERO) {
            value
                .neg_checked()
                .map_err(|_| ArrowError::ComputeError(kind.to_string()))
        } else {
            Ok(value)
        }
    }) {
        return Ok(Arc::new(result.with_data_type(array.data_type().clone())));
    }
    let mut builder = PrimitiveBuilder::<T>::with_capacity(primitive.len())
        .with_data_type(array.data_type().clone());
    for index in 0..primitive.len() {
        if primitive.is_null(index) {
            builder.append_null();
            continue;
        }
        let value = primitive.value(index);
        if value.is_lt(T::Native::ZERO) {
            builder.append_value(value.neg_checked().map_err(|_| overflow_error(kind))?);
        } else {
            builder.append_value(value);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn bin_render(value: i64, digits: &mut [u8; 64]) -> &[u8] {
    let mut bits = value.cast_unsigned();
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + u8::try_from(bits & 1).unwrap_or_default();
        bits >>= 1;
        if bits == 0 {
            break;
        }
    }
    &digits[start..]
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

    async fn one(ctx: &SessionContext, sql: &str) -> ScalarValue {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("exec {sql}: {error}"));
        ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0)
            .unwrap_or_else(|error| panic!("scalar {sql}: {error}"))
    }

    async fn error_text(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => frame
                .collect()
                .await
                .err()
                .unwrap_or_else(|| panic!("{sql} should refuse"))
                .to_string(),
        }
    }

    #[tokio::test]
    async fn hypot_rescales_and_propagates_null() {
        let ctx = ctx();
        assert_eq!(
            one(
                &ctx,
                "SELECT hypot(CAST(1e200 AS DOUBLE), CAST(1e200 AS DOUBLE))"
            )
            .await,
            ScalarValue::Float64(Some(1.414_213_562_373_095e200))
        );
        assert_eq!(
            one(
                &ctx,
                "SELECT hypot(CAST(NULL AS DOUBLE), CAST(1 AS DOUBLE))"
            )
            .await,
            ScalarValue::Float64(None)
        );
        assert_eq!(
            one(
                &ctx,
                "SELECT hypot(CAST('Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE))"
            )
            .await,
            ScalarValue::Float64(Some(f64::INFINITY))
        );
        assert_eq!(
            one(&ctx, "SELECT hypot(CAST(-3 AS DOUBLE), CAST(4 AS DOUBLE))").await,
            ScalarValue::Float64(Some(5.0))
        );
    }

    #[tokio::test]
    async fn bin_and_rint_refuse_boolean_with_spark_class() {
        let ctx = ctx();
        for sql in ["SELECT bin(true)", "SELECT rint(true)"] {
            let error = error_text(&ctx, sql).await;
            assert!(
                error.contains("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
                "{sql} -> {error}"
            );
        }
        assert_eq!(
            one(&ctx, "SELECT bin(5)").await,
            ScalarValue::Utf8(Some("101".to_string()))
        );
        assert_eq!(
            one(&ctx, "SELECT bin(CAST(-1 AS BIGINT))").await,
            ScalarValue::Utf8(Some(
                "1111111111111111111111111111111111111111111111111111111111111111".to_string()
            ))
        );
        assert_eq!(
            one(&ctx, "SELECT rint(CAST(2.5 AS DOUBLE))").await,
            ScalarValue::Float64(Some(2.0))
        );
        assert_eq!(
            one(&ctx, "SELECT rint(CAST(3.5 AS DOUBLE))").await,
            ScalarValue::Float64(Some(4.0))
        );
    }

    #[tokio::test]
    async fn abs_min_raises_under_ansi_and_keeps_width() {
        let ctx = ctx();
        for (sql, kind) in [
            ("SELECT abs(CAST(-128 AS TINYINT))", "byte overflow"),
            ("SELECT abs(CAST(-2147483648 AS INT))", "integer overflow"),
            (
                "SELECT abs(CAST(-9223372036854775808 AS BIGINT))",
                "long overflow",
            ),
        ] {
            let error = error_text(&ctx, sql).await;
            assert!(error.contains("ARITHMETIC_OVERFLOW"), "{sql} -> {error}");
            assert!(error.contains(kind), "{sql} -> {error}");
        }
        assert_eq!(
            one(&ctx, "SELECT abs(CAST(-5 AS TINYINT))").await,
            ScalarValue::Int8(Some(5))
        );
        assert_eq!(
            one(&ctx, "SELECT abs(CAST(-5 AS BIGINT))").await,
            ScalarValue::Int64(Some(5))
        );
        assert_eq!(
            one(&ctx, "SELECT abs(CAST(NULL AS DOUBLE))").await,
            ScalarValue::Float64(None)
        );
    }
}
