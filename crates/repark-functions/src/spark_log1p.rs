use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Float64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::kernels::arity::unary;
use datafusion::arrow::compute::kernels::cmp::lt_eq;
use datafusion::arrow::compute::nullif;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

#[must_use]
pub fn log1p_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkLog1p::new()))
}

#[must_use]
pub fn expm1_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkExpm1::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![log1p_udf(), expm1_udf()]
}

pub fn register(ctx: &SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}

#[derive(Debug)]
struct SparkLog1p {
    signature: Signature,
}

impl SparkLog1p {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkLog1p {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkLog1p {}

impl Hash for SparkLog1p {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

#[derive(Debug)]
struct SparkExpm1 {
    signature: Signature,
}

impl SparkExpm1 {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkExpm1 {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkExpm1 {}

impl Hash for SparkExpm1 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn is_numeric_argument(data_type: &DataType) -> bool {
    *data_type == DataType::Null || data_type.is_numeric()
}

fn as_f64_array(array: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    Ok(cast(array.as_ref(), &DataType::Float64)?)
}

fn f64_argument(name: &str, args: &ScalarFunctionArgs) -> Result<Arc<dyn Array>> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let [value] = arrays.as_slice() else {
        return exec_err!("'{name}' requires 1 argument, got {}", arrays.len());
    };
    as_f64_array(value)
}

fn invoke_expm1(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let casted = f64_argument("expm1", args)?;
    let values = casted.as_primitive::<Float64Type>();
    let out = unary::<Float64Type, _, Float64Type>(values, f64::exp_m1);
    Ok(ColumnarValue::Array(Arc::new(out)))
}

fn invoke_log1p(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let casted = f64_argument("log1p", args)?;
    let values = casted.as_primitive::<Float64Type>();
    let computed = unary::<Float64Type, _, Float64Type>(values, f64::ln_1p);
    let below = lt_eq(values, &Float64Array::new_scalar(-1.0))?;
    Ok(ColumnarValue::Array(nullif(&computed, &below)?))
}

fn coerce_one_numeric(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    match arg_types {
        [data_type] if is_numeric_argument(data_type) => Ok(vec![DataType::Float64]),
        [data_type] => exec_err!("'{name}' argument 1 must be numeric, got {data_type}"),
        _ => exec_err!("'{name}' requires 1 argument, got {}", arg_types.len()),
    }
}

impl ScalarUDFImpl for SparkLog1p {
    crate::shim_udf_boilerplate!("log1p");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new("log1p", DataType::Float64, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_one_numeric("log1p", arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_log1p(&args)
    }
}

impl ScalarUDFImpl for SparkExpm1 {
    crate::shim_udf_boilerplate!("expm1");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new("expm1", DataType::Float64, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_one_numeric("expm1", arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_expm1(&args)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::ArrayData;
    use datafusion::arrow::buffer::Buffer;
    use datafusion::config::ConfigOptions;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn spark_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    fn ansi_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        register(&ctx);
        ctx
    }

    async fn first_f64(ctx: &SessionContext, sql: &str) -> Option<f64> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute `{sql}`: {error}"));
        let column = batches[0].column(0);
        let floats = column
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap_or_else(|| panic!("expected Float64 for {sql}, got {:?}", column.data_type()));
        floats.is_valid(0).then(|| floats.value(0))
    }

    fn assert_bits(got: f64, want: f64, sql: &str) {
        assert_eq!(got.to_bits(), want.to_bits(), "{sql}");
    }

    #[tokio::test]
    async fn log1p_tiny_argument_matches_ln_1p_on_both_doors() {
        let spark = spark_ctx();
        let ansi = ansi_ctx();
        let want = 1e-16_f64.ln_1p();
        assert_eq!(want.to_bits(), 1e-16_f64.to_bits());
        assert_ne!((1.0 + 1e-16_f64).ln().to_bits(), want.to_bits());
        for ctx in [&spark, &ansi] {
            let got = first_f64(ctx, "SELECT log1p(CAST(1e-16 AS DOUBLE))")
                .await
                .expect("log1p(1e-16)");
            assert_bits(got, want, "log1p(1e-16)");
        }
    }

    #[tokio::test]
    async fn expm1_tiny_argument_matches_exp_m1_on_both_doors() {
        let spark = spark_ctx();
        let ansi = ansi_ctx();
        let want = 1e-16_f64.exp_m1();
        assert_eq!(want.to_bits(), 1e-16_f64.to_bits());
        assert_ne!((1e-16_f64.exp() - 1.0).to_bits(), want.to_bits());
        for ctx in [&spark, &ansi] {
            let got = first_f64(ctx, "SELECT expm1(CAST(1e-16 AS DOUBLE))")
                .await
                .expect("expm1(1e-16)");
            assert_bits(got, want, "expm1(1e-16)");
        }
    }

    #[tokio::test]
    async fn log1p_domain_nulls_at_or_below_minus_one() {
        let ctx = spark_ctx();
        for sql in [
            "SELECT log1p(CAST(-1.0 AS DOUBLE))",
            "SELECT log1p(CAST(-2.0 AS DOUBLE))",
            "SELECT log1p(CAST('-Infinity' AS DOUBLE))",
            "SELECT log1p(CAST(NULL AS DOUBLE))",
        ] {
            assert_eq!(first_f64(&ctx, sql).await, None, "{sql}");
        }
    }

    #[tokio::test]
    async fn expm1_overflow_and_null() {
        let ctx = spark_ctx();
        let finite = first_f64(&ctx, "SELECT expm1(CAST(700.0 AS DOUBLE))")
            .await
            .expect("expm1(700)");
        assert_bits(finite, 700.0_f64.exp_m1(), "expm1(700)");
        let inf = first_f64(&ctx, "SELECT expm1(CAST(710.0 AS DOUBLE))")
            .await
            .expect("expm1(710)");
        assert!(inf.is_infinite() && inf.is_sign_positive(), "{inf}");
        assert_eq!(
            first_f64(&ctx, "SELECT expm1(CAST(NULL AS DOUBLE))").await,
            None
        );
    }

    #[tokio::test]
    async fn numeric_coercion_int_and_decimal_agree_across_doors() {
        let spark = spark_ctx();
        let ansi = ansi_ctx();
        let log1p_one = 1.0_f64.ln_1p();
        let expm1_one = 1.0_f64.exp_m1();
        for ctx in [&spark, &ansi] {
            assert_bits(
                first_f64(ctx, "SELECT log1p(0)").await.expect("log1p(0)"),
                0.0,
                "log1p(0)",
            );
            assert_bits(
                first_f64(ctx, "SELECT log1p(1)").await.expect("log1p(1)"),
                log1p_one,
                "log1p(1)",
            );
            assert_bits(
                first_f64(ctx, "SELECT expm1(0)").await.expect("expm1(0)"),
                0.0,
                "expm1(0)",
            );
            assert_bits(
                first_f64(ctx, "SELECT expm1(1)").await.expect("expm1(1)"),
                expm1_one,
                "expm1(1)",
            );
            assert_bits(
                first_f64(ctx, "SELECT log1p(CAST(1 AS DECIMAL(10, 0)))")
                    .await
                    .expect("log1p decimal 1"),
                log1p_one,
                "log1p decimal 1",
            );
            assert_bits(
                first_f64(ctx, "SELECT expm1(CAST(1 AS DECIMAL(10, 0)))")
                    .await
                    .expect("expm1 decimal 1"),
                expm1_one,
                "expm1 decimal 1",
            );
        }
    }

    #[tokio::test]
    async fn nan_propagates() {
        let ctx = spark_ctx();
        let log1p_nan = first_f64(&ctx, "SELECT log1p(CAST('NaN' AS DOUBLE))")
            .await
            .expect("log1p nan");
        let expm1_nan = first_f64(&ctx, "SELECT expm1(CAST('NaN' AS DOUBLE))")
            .await
            .expect("expm1 nan");
        assert!(log1p_nan.is_nan());
        assert!(expm1_nan.is_nan());
    }

    fn null_slot_with_live_buffer_value(values: Vec<f64>, null_row: usize) -> Float64Array {
        let mut validity = vec![0u8; values.len().div_ceil(8)];
        for row in 0..values.len() {
            if row != null_row {
                validity[row / 8] |= 1 << (row % 8);
            }
        }
        let data = ArrayData::builder(DataType::Float64)
            .len(values.len())
            .add_buffer(Buffer::from_vec(values))
            .null_bit_buffer(Some(Buffer::from_vec(validity)))
            .build()
            .unwrap_or_else(|error| panic!("build probe array: {error}"));
        Float64Array::from(data)
    }

    fn invoke_named(ctx: &SessionContext, name: &str, array: Arc<dyn Array>) -> Arc<dyn Array> {
        let udf = Arc::clone(
            ctx.state()
                .scalar_functions()
                .get(name)
                .unwrap_or_else(|| panic!("`{name}` is registered")),
        );
        let value = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(array)],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new(name, DataType::Float64, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap_or_else(|error| panic!("invoke `{name}`: {error}"));
        match value {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(_) => panic!("expected an array result"),
        }
    }

    #[test]
    fn null_slots_null_out_even_when_the_buffer_holds_a_live_value() {
        let ctx = spark_ctx();
        let probe = Arc::new(null_slot_with_live_buffer_value(vec![5.0], 0)) as Arc<dyn Array>;
        for name in ["log1p", "expm1"] {
            let result = invoke_named(&ctx, name, Arc::clone(&probe));
            let floats = result.as_primitive::<Float64Type>();
            assert!(
                floats.is_null(0),
                "{name}: expected NULL, got {}",
                floats.value(0)
            );
        }
    }
}
