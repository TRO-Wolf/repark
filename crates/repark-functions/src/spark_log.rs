//! Spark-door `log`: one-arg natural log, two-arg `log(base, expr)`.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Float64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

/// Spark `log` UDF. Overwrites DataFusion's base-10 `LogFunc` on the Spark door.
#[must_use]
pub fn spark_log_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkLog::new()))
}

/// The registered Spark `log` instance.
#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![spark_log_udf()]
}

#[derive(Debug)]
struct SparkLog {
    signature: Signature,
}

impl SparkLog {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkLog {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkLog {}

impl Hash for SparkLog {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn is_log_argument(data_type: &DataType) -> bool {
    *data_type == DataType::Null || data_type.is_numeric()
}

fn natural_log(value: f64) -> Option<f64> {
    if value <= 0.0 { None } else { Some(value.ln()) }
}

fn log_to_base(base: f64, value: f64) -> Option<f64> {
    if value <= 0.0 || base <= 0.0 {
        None
    } else {
        Some(value.ln() / base.ln())
    }
}

fn as_f64_array(array: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    Ok(cast(array.as_ref(), &DataType::Float64)?)
}

impl ScalarUDFImpl for SparkLog {
    crate::shim_udf_boilerplate!("log");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new("log", DataType::Float64, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return exec_err!("'log' requires 1 or 2 arguments, got {}", arg_types.len());
        }
        let mut coerced = Vec::with_capacity(arg_types.len());
        for (index, data_type) in arg_types.iter().enumerate() {
            if is_log_argument(data_type) {
                coerced.push(DataType::Float64);
            } else {
                return exec_err!(
                    "'log' argument {} must be numeric, got {data_type}",
                    index + 1
                );
            }
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        match arrays.as_slice() {
            [value] => {
                let casted = as_f64_array(value)?;
                let values = casted.as_primitive::<Float64Type>();
                let out: Float64Array = (0..values.len())
                    .map(|row| {
                        if values.is_null(row) {
                            None
                        } else {
                            natural_log(values.value(row))
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out)))
            }
            [base, value] => {
                let base_casted = as_f64_array(base)?;
                let value_casted = as_f64_array(value)?;
                let bases = base_casted.as_primitive::<Float64Type>();
                let values = value_casted.as_primitive::<Float64Type>();
                let out: Float64Array = (0..values.len())
                    .map(|row| {
                        if bases.is_null(row) || values.is_null(row) {
                            None
                        } else {
                            log_to_base(bases.value(row), values.value(row))
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out)))
            }
            _ => exec_err!("'log' requires 1 or 2 arguments, got {}", arrays.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, ArrayData};
    use datafusion::arrow::buffer::Buffer;
    use datafusion::config::ConfigOptions;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn spark_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
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

    fn invoke_log(ctx: &SessionContext, arrays: Vec<Arc<dyn Array>>) -> Arc<dyn Array> {
        let udf = Arc::clone(
            ctx.state()
                .scalar_functions()
                .get("log")
                .unwrap_or_else(|| panic!("`log` is registered")),
        );
        let value = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: arrays.into_iter().map(ColumnarValue::Array).collect(),
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("log", DataType::Float64, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap_or_else(|error| panic!("invoke `log`: {error}"));
        match value {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(_) => panic!("expected an array result"),
        }
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

    #[tokio::test]
    async fn one_arg_is_natural_log() {
        let ctx = spark_ctx();
        let got = first_f64(&ctx, "SELECT log(8)").await.expect("log(8)");
        assert!((got - 8.0_f64.ln()).abs() < 1e-15);
        assert_eq!(first_f64(&ctx, "SELECT log(1)").await, Some(0.0));
    }

    #[tokio::test]
    async fn two_arg_is_log_at_base() {
        let ctx = spark_ctx();
        assert_eq!(first_f64(&ctx, "SELECT log(2, 8)").await, Some(3.0));
        assert_eq!(first_f64(&ctx, "SELECT log(8, 8)").await, Some(1.0));
        assert_eq!(first_f64(&ctx, "SELECT log(0.5, 8)").await, Some(-3.0));
    }

    #[tokio::test]
    async fn domain_edges_are_null() {
        let ctx = spark_ctx();
        for sql in [
            "SELECT log(0)",
            "SELECT log(-1)",
            "SELECT log(CAST(NULL AS DOUBLE))",
            "SELECT log(0, 8)",
            "SELECT log(-2, 8)",
            "SELECT log(10, 0)",
            "SELECT log(10, -1)",
            "SELECT log(CAST(NULL AS DOUBLE), 8)",
            "SELECT log(10, CAST(NULL AS DOUBLE))",
            "SELECT log(CAST('-Infinity' AS DOUBLE))",
        ] {
            assert_eq!(first_f64(&ctx, sql).await, None, "{sql}");
        }
    }

    #[tokio::test]
    async fn base_one_is_ieee_not_null() {
        let ctx = spark_ctx();
        let inf = first_f64(&ctx, "SELECT log(1, 8)").await.expect("log(1,8)");
        assert!(inf.is_infinite() && inf.is_sign_positive());
        let nan = first_f64(&ctx, "SELECT log(1, 1)").await.expect("log(1,1)");
        assert!(nan.is_nan());
    }

    #[tokio::test]
    async fn register_all_overwrites_datafusion_base_ten() {
        let bare = SessionContext::new();
        let spark = spark_ctx();
        let bare_value = first_f64(&bare, "SELECT log(8)").await.expect("bare");
        let spark_value = first_f64(&spark, "SELECT log(8)").await.expect("spark");
        assert!((bare_value - 8.0_f64.log10()).abs() < 1e-12);
        assert!((spark_value - 8.0_f64.ln()).abs() < 1e-15);
        assert!((bare_value - spark_value).abs() > 0.1);
    }

    #[test]
    fn null_slots_null_out_even_when_the_buffer_holds_a_live_value() {
        let ctx = spark_ctx();
        let probe = null_slot_with_live_buffer_value(vec![5.0], 0);
        assert!(probe.is_null(0));
        assert_eq!(probe.value(0).to_bits(), 5.0_f64.to_bits());
        let probe = Arc::new(probe) as Arc<dyn Array>;
        let cases: Vec<(&str, Vec<Arc<dyn Array>>)> = vec![
            ("one-arg", vec![Arc::clone(&probe)]),
            (
                "base slot",
                vec![
                    Arc::new(null_slot_with_live_buffer_value(vec![5.0], 0)),
                    Arc::new(Float64Array::from(vec![8.0])),
                ],
            ),
            (
                "value slot",
                vec![
                    Arc::new(Float64Array::from(vec![2.0])),
                    Arc::new(null_slot_with_live_buffer_value(vec![5.0], 0)),
                ],
            ),
        ];
        for (label, arrays) in cases {
            let result = invoke_log(&ctx, arrays);
            let floats = result.as_primitive::<Float64Type>();
            assert!(
                floats.is_null(0),
                "{label}: expected NULL, got {}",
                floats.value(0)
            );
        }
    }
}
