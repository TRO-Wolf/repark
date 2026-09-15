use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, AsArray, BooleanArray, BooleanBuilder};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

const REPARK_ISNAN: &str = "repark_isnan";

#[must_use]
pub fn repark_isnan_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(ReparkIsNan::new()))
}

#[derive(Debug)]
struct ReparkIsNan {
    signature: Signature,
}

impl ReparkIsNan {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for ReparkIsNan {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ReparkIsNan {}

impl Hash for ReparkIsNan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn nan_mask(casted: &dyn Array) -> BooleanArray {
    let values = casted.as_primitive::<Float64Type>();
    let mut builder = BooleanBuilder::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row) {
            builder.append_value(false);
        } else {
            builder.append_value(values.value(row).is_nan());
        }
    }
    builder.finish()
}

impl ScalarUDFImpl for ReparkIsNan {
    fn name(&self) -> &str {
        REPARK_ISNAN
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(REPARK_ISNAN, DataType::Boolean, false)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!(
                "'{REPARK_ISNAN}' requires 1 argument, got {}",
                args.args.len()
            );
        };
        let array = value.to_array(args.number_rows)?;
        let options = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        match array.data_type() {
            DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View => {
                let casted = cast_with_options(array.as_ref(), &DataType::Float64, &options)?;
                Ok(ColumnarValue::Array(Arc::new(nan_mask(casted.as_ref()))))
            }
            _ => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
                false;
                array.len()
            ])))),
        }
    }
}
