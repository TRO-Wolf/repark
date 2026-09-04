use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, BooleanBuilder, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn arrays_overlap_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArraysOverlap::new()))
}

#[derive(Debug)]
struct SparkArraysOverlap {
    signature: Signature,
}

impl SparkArraysOverlap {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkArraysOverlap {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkArraysOverlap {}

impl Hash for SparkArraysOverlap {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn list_row(array: &ListArray, row: usize) -> Result<(HashSet<ScalarValue>, bool)> {
    let values = array.value(row);
    let mut seen = HashSet::new();
    let mut saw_null = false;
    for index in 0..values.len() {
        if values.is_null(index) {
            saw_null = true;
            continue;
        }
        seen.insert(ScalarValue::try_from_array(&values, index)?);
    }
    Ok((seen, saw_null))
}

impl ScalarUDFImpl for SparkArraysOverlap {
    crate::shim_udf_boilerplate!("arrays_overlap");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [left, right]
                if matches!(left, DataType::List(_) | DataType::Null)
                    && matches!(right, DataType::List(_) | DataType::Null) =>
            {
                Ok(vec![left.clone(), right.clone()])
            }
            _ => exec_err!("'arrays_overlap' expects (array, array), got {arg_types:?}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [left, right] = arrays.as_slice() else {
            return exec_err!("'arrays_overlap' expects 2 arguments, got {}", arrays.len());
        };
        let left_list = left.as_list::<i32>();
        let right_list = right.as_list::<i32>();
        if left_list.len() != right_list.len() {
            return exec_err!("'arrays_overlap' array lengths differ");
        }
        let mut builder = BooleanBuilder::with_capacity(left_list.len());
        for row in 0..left_list.len() {
            if left_list.is_null(row) || right_list.is_null(row) {
                builder.append_null();
                continue;
            }
            let (left_seen, left_null) = list_row(left_list, row)?;
            let right_values = right_list.value(row);
            let mut definite = false;
            let mut right_null = false;
            for index in 0..right_values.len() {
                if right_values.is_null(index) {
                    right_null = true;
                    continue;
                }
                let value = ScalarValue::try_from_array(&right_values, index)?;
                if left_seen.contains(&value) {
                    definite = true;
                    break;
                }
            }
            if definite {
                builder.append_value(true);
            } else if left_null || right_null {
                builder.append_null();
            } else {
                builder.append_value(false);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
