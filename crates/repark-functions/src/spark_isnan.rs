use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, BooleanBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

#[must_use]
pub fn isnan_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkIsNan::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![isnan_udf()]
}

pub fn register(ctx: &SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}

#[derive(Debug)]
struct SparkIsNan {
    signature: Signature,
}

impl SparkIsNan {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkIsNan {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkIsNan {}

impl Hash for SparkIsNan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn is_numeric_argument(data_type: &DataType) -> bool {
    *data_type == DataType::Null || data_type.is_numeric()
}

impl ScalarUDFImpl for SparkIsNan {
    crate::shim_udf_boilerplate!("isnan");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, false)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type] if is_numeric_argument(data_type) => Ok(vec![DataType::Float64]),
            [data_type] => exec_err!("'isnan' argument 1 must be numeric, got {data_type}"),
            _ => exec_err!("'isnan' requires 1 argument, got {}", arg_types.len()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [value] = arrays.as_slice() else {
            return exec_err!("'isnan' requires 1 argument, got {}", arrays.len());
        };
        let casted = cast(value.as_ref(), &DataType::Float64)?;
        let values = casted.as_primitive::<Float64Type>();
        let mut builder = BooleanBuilder::with_capacity(values.len());
        for row in 0..values.len() {
            if values.is_null(row) {
                builder.append_value(false);
            } else {
                builder.append_value(values.value(row).is_nan());
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
