use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn flatten_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkFlatten::new()))
}

#[derive(Debug)]
struct SparkFlatten {
    signature: Signature,
}

impl SparkFlatten {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkFlatten {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkFlatten {}

impl Hash for SparkFlatten {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn inner_element_type(data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::List(field) => match field.data_type() {
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::FixedSizeList(inner, _) => Ok(inner.data_type().clone()),
            other => exec_err!("'flatten' expects ARRAY<ARRAY<T>>, got ARRAY<{other}>"),
        },
        DataType::Null => Ok(DataType::Null),
        other => exec_err!("'flatten' expects ARRAY<ARRAY<T>>, got {other}"),
    }
}

fn offset_as_usize(offset: i32) -> Result<usize> {
    if let Ok(value) = usize::try_from(offset) {
        Ok(value)
    } else {
        exec_err!("'flatten' offset does not fit usize")
    }
}

fn flatten_list(list: &ListArray, element_type: &DataType) -> Result<ArrayRef> {
    let inner_list = list.values().as_list::<i32>();
    let mut rows: Vec<ArrayRef> = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) {
            rows.push(null_list_row(element_type)?);
            continue;
        }
        let offsets = list.value_offsets();
        let start = offset_as_usize(offsets[row])?;
        let end = offset_as_usize(offsets[row + 1])?;
        let mut saw_null_sub = false;
        let mut elements: Vec<ScalarValue> = Vec::new();
        for slot in start..end {
            if inner_list.is_null(slot) {
                saw_null_sub = true;
                break;
            }
            let sub = inner_list.value(slot);
            for element in 0..sub.len() {
                elements.push(ScalarValue::try_from_array(&sub, element)?);
            }
        }
        if saw_null_sub {
            rows.push(null_list_row(element_type)?);
        } else {
            let built = ScalarValue::new_list_nullable(&elements, element_type);
            rows.push(Arc::new(built.as_ref().clone()) as ArrayRef);
        }
    }
    let refs: Vec<&dyn Array> = rows.iter().map(AsRef::as_ref).collect();
    concat(&refs).map_err(Into::into)
}

fn null_list_row(element_type: &DataType) -> Result<ArrayRef> {
    ScalarValue::new_null_list(element_type.clone(), true, 1).to_array()
}

impl ScalarUDFImpl for SparkFlatten {
    crate::shim_udf_boilerplate!("flatten");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let element = inner_element_type(&arg_types[0])?;
        Ok(DataType::List(Arc::new(Field::new("item", element, true))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let data_type = match args.arg_fields.first() {
            Some(field) => self.return_type(&[field.data_type().clone()])?,
            None => return exec_err!("'flatten' requires 1 argument"),
        };
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [data_type] if matches!(data_type, DataType::List(_) | DataType::Null) => {
                Ok(vec![data_type.clone()])
            }
            [data_type] => exec_err!("'flatten' expects ARRAY<ARRAY<T>>, got {data_type}"),
            _ => exec_err!("'flatten' requires 1 argument, got {}", arg_types.len()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [array] = arrays.as_slice() else {
            return exec_err!("'flatten' requires 1 argument, got {}", arrays.len());
        };
        if matches!(array.data_type(), DataType::Null) || array.is_empty() {
            return Ok(ColumnarValue::Array(Arc::clone(array)));
        }
        let list = array.as_list::<i32>();
        let element_type = inner_element_type(array.data_type())?;
        Ok(ColumnarValue::Array(flatten_list(list, &element_type)?))
    }
}
