use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, Int64Builder, OffsetSizeTrait,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn array_position_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArrayPosition::new()))
}

#[derive(Debug)]
struct SparkArrayPosition {
    signature: Signature,
}

impl SparkArrayPosition {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkArrayPosition {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkArrayPosition {}

impl Hash for SparkArrayPosition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn list_element_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.data_type().clone())
        }
        _ => None,
    }
}

fn needle_equals(value: &ScalarValue, needle: &ScalarValue) -> bool {
    if value.data_type() == needle.data_type() {
        return value == needle;
    }
    match needle.cast_to(&value.data_type()) {
        Ok(cast) => value == &cast,
        Err(_) => false,
    }
}

fn position_in_values(values: &dyn Array, needle: &ScalarValue) -> Result<i64> {
    for index in 0..values.len() {
        if values.is_null(index) {
            continue;
        }
        let value = ScalarValue::try_from_array(values, index)?;
        if needle_equals(&value, needle) {
            let Ok(position) = i64::try_from(index + 1) else {
                return exec_err!("'array_position' index does not fit i64");
            };
            return Ok(position);
        }
    }
    Ok(0)
}

fn append_list_positions<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    needle: &ArrayRef,
    builder: &mut Int64Builder,
) -> Result<()> {
    for row in 0..list.len() {
        if list.is_null(row) || needle.is_null(row) {
            builder.append_null();
            continue;
        }
        let needle_value = ScalarValue::try_from_array(needle, row)?;
        builder.append_value(position_in_values(&list.value(row), &needle_value)?);
    }
    Ok(())
}

impl ScalarUDFImpl for SparkArrayPosition {
    crate::shim_udf_boilerplate!("array_position");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Null, _needle] => Ok(arg_types.to_vec()),
            [list, needle] => {
                let Some(element) = list_element_type(list) else {
                    return exec_err!("'array_position' expects (array, value), got {arg_types:?}");
                };
                let needle_out = if matches!(needle, DataType::Null) {
                    needle.clone()
                } else {
                    element
                };
                Ok(vec![list.clone(), needle_out])
            }
            _ => exec_err!("'array_position' expects (array, value), got {arg_types:?}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [array, needle] = arrays.as_slice() else {
            return exec_err!("'array_position' expects 2 arguments, got {}", arrays.len());
        };
        let mut builder = Int64Builder::with_capacity(array.len());
        match array.data_type() {
            DataType::Null => {
                for _ in 0..array.len() {
                    builder.append_null();
                }
            }
            DataType::List(_) => {
                append_list_positions(array.as_list::<i32>(), needle, &mut builder)?;
            }
            DataType::LargeList(_) => {
                append_list_positions(array.as_list::<i64>(), needle, &mut builder)?;
            }
            DataType::FixedSizeList(_, _) => {
                let list = array.as_fixed_size_list();
                for row in 0..list.len() {
                    if list.is_null(row) || needle.is_null(row) {
                        builder.append_null();
                        continue;
                    }
                    let needle_value = ScalarValue::try_from_array(needle, row)?;
                    builder.append_value(position_in_values(&list.value(row), &needle_value)?);
                }
            }
            other => {
                return exec_err!("'array_position' expects an array, got {other}");
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
