use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

use crate::ansi::spark_ansi_enabled_from_options;

#[must_use]
pub fn elt_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkElt::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![elt_udf()]
}

pub fn register(ctx: &SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}

#[derive(Debug)]
struct SparkElt {
    signature: Signature,
}

impl SparkElt {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkElt {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkElt {}

impl Hash for SparkElt {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn value_type(arg_types: &[DataType]) -> DataType {
    arg_types
        .iter()
        .skip(1)
        .find(|data_type| **data_type != DataType::Null)
        .cloned()
        .unwrap_or(DataType::Utf8)
}

fn invalid_array_index(index: i64, count: usize) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Execution(format!(
        "[INVALID_ARRAY_INDEX] The index {index} is out of bounds. The array has {count} \
         elements. Use the SQL function `get()` to tolerate accessing element at invalid index \
         and return NULL instead. SQLSTATE: 22003"
    ))
}

impl ScalarUDFImpl for SparkElt {
    crate::shim_udf_boilerplate!("elt");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 {
            return exec_err!(
                "'elt' requires at least 2 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(value_type(arg_types))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let data_type = value_type(
            &args
                .arg_fields
                .iter()
                .map(|field| field.data_type().clone())
                .collect::<Vec<_>>(),
        );
        let nullable = true;
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 {
            return exec_err!(
                "'elt' requires at least 2 arguments, got {}",
                arg_types.len()
            );
        }
        let chosen = value_type(arg_types);
        let mut coerced = Vec::with_capacity(arg_types.len());
        coerced.push(DataType::Int64);
        coerced.extend(std::iter::repeat_n(chosen, arg_types.len() - 1));
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() < 2 {
            return exec_err!("'elt' requires at least 2 arguments, got {}", arrays.len());
        }
        let indices = cast(arrays[0].as_ref(), &DataType::Int64)?;
        let indices = indices.as_primitive::<Int64Type>();
        let values = &arrays[1..];
        let count = values.len();
        let ansi_enabled = spark_ansi_enabled_from_options(args.config_options.as_ref());
        let mut out: Vec<ScalarValue> = Vec::with_capacity(indices.len());
        for row in 0..indices.len() {
            if indices.is_null(row) {
                out.push(ScalarValue::try_from(args.return_field.data_type())?);
                continue;
            }
            let index = indices.value(row);
            if index < 1 || index > i64::try_from(count).unwrap_or(i64::MAX) {
                if ansi_enabled {
                    return Err(invalid_array_index(index, count));
                }
                out.push(ScalarValue::try_from(args.return_field.data_type())?);
                continue;
            }
            let Some(slot) = usize::try_from(index - 1)
                .ok()
                .and_then(|offset| values.get(offset))
            else {
                return Err(invalid_array_index(index, count));
            };
            if slot.is_null(row) {
                out.push(ScalarValue::try_from(args.return_field.data_type())?);
            } else {
                out.push(ScalarValue::try_from_array(slot.as_ref(), row)?);
            }
        }
        Ok(ColumnarValue::Array(ScalarValue::iter_to_array(out)?))
    }
}
