use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, PrimitiveArray, PrimitiveBuilder, StringArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowNativeTypeOp, ArrowPrimitiveType, DataType, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, Field, FieldRef, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type,
};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

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
    vec![abs_udf(), hypot_udf(), bin_udf(), rint_udf()]
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
        arg_types
            .first()
            .cloned()
            .ok_or_else(|| DataFusionError::Plan("'abs' expects one numeric argument".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let field = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan("'abs' expects one numeric argument".to_string())
        })?;
        Ok(Arc::new(Field::new(
            "abs",
            field.data_type().clone(),
            field.is_nullable(),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type] if data_type.is_numeric() => Ok(vec![data_type.clone()]),
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
        let mut values = Vec::with_capacity(left.len());
        for index in 0..left.len() {
            if left.is_null(index) || right.is_null(index) {
                values.push(None);
            } else {
                values.push(Some(left.value(index).hypot(right.value(index))));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(Float64Array::from(values))))
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
        let values: Vec<Option<String>> = (0..ints.len())
            .map(|index| {
                if ints.is_null(index) {
                    None
                } else {
                    Some(format!("{:b}", ints.value(index).cast_unsigned()))
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
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
        let values: Vec<Option<f64>> = (0..doubles.len())
            .map(|index| {
                if doubles.is_null(index) {
                    None
                } else {
                    Some(doubles.value(index).round_ties_even())
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(Float64Array::from(values))))
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

fn unexpected_input_type(name: &str, required: &str, got: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}(<expr>)\" due to \
         data type mismatch: The first parameter requires the \"{required}\" type, however \
         the argument has the type \"{}\".",
        spark_type_name(got)
    ))
}

fn spark_type_name(data_type: &DataType) -> String {
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

fn overflow_error(kind: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[ARITHMETIC_OVERFLOW] {kind} overflow. If necessary set \"spark.sql.ansi.enabled\" \
         to \"false\" to bypass this error."
    ))
}

fn abs_typed(array: &dyn Array, ansi: bool) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Int8 => abs_primitive::<Int8Type>(array, ansi, "byte"),
        DataType::Int16 => abs_primitive::<Int16Type>(array, ansi, "short"),
        DataType::Int32 => abs_primitive::<Int32Type>(array, ansi, "integer"),
        DataType::Int64 => abs_primitive::<Int64Type>(array, ansi, "long"),
        DataType::Float32 => abs_primitive::<Float32Type>(array, ansi, "float"),
        DataType::Float64 => abs_primitive::<Float64Type>(array, ansi, "double"),
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

fn abs_primitive<T: ArrowPrimitiveType>(
    array: &dyn Array,
    ansi: bool,
    kind: &str,
) -> Result<ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    let primitive = array.as_primitive::<T>();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(primitive.len())
        .with_data_type(array.data_type().clone());
    for index in 0..primitive.len() {
        if primitive.is_null(index) {
            builder.append_null();
            continue;
        }
        let value = primitive.value(index);
        if value.is_lt(T::Native::ZERO) {
            match value.neg_checked() {
                Ok(result) => builder.append_value(result),
                Err(_) if ansi => return Err(overflow_error(kind)),
                Err(_) => builder.append_value(value.neg_wrapping()),
            }
        } else {
            builder.append_value(value);
        }
    }
    Ok(Arc::new(builder.finish()))
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
