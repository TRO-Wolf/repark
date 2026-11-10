use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, GenericListViewArray, LargeListArray, ListArray,
    MapArray, StructArray, make_array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::STRUCT_BY_POSITION;

#[derive(Debug)]
pub(super) struct StructByPosition {
    name: &'static str,
    target: DataType,
    signature: Signature,
}

impl StructByPosition {
    pub(super) fn new(name: &'static str, target: DataType) -> Self {
        Self {
            name,
            target,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for StructByPosition {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.target == other.target
    }
}

impl Eq for StructByPosition {}

impl Hash for StructByPosition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.target.hash(state);
    }
}

impl ScalarUDFImpl for StructByPosition {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        Ok(args.first().map(ToString::to_string).unwrap_or_default())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Execution(format!(
                "{STRUCT_BY_POSITION} expects one argument"
            )));
        };
        match value {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(renamed_array(array, &self.target)?))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = renamed_array(&scalar.to_array()?, &self.target)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &array, 0,
                )?))
            }
        }
    }
}

pub(super) fn renamed_array(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    if let Some(renamed) = renamed_encoded(array, target)? {
        return Ok(renamed);
    }
    if let Some(renamed) = renamed_list(array, target)? {
        return Ok(renamed);
    }
    renamed_struct(array, target)
}

fn renamed_encoded(array: &ArrayRef, target: &DataType) -> Result<Option<ArrayRef>> {
    match target {
        DataType::Dictionary(_, value) => {
            let Some(source) = array.as_any_dictionary_opt() else {
                return Ok(None);
            };
            let values = renamed_array(source.values(), value)?;
            Ok(Some(source.with_values(values)))
        }
        DataType::RunEndEncoded(_, values_field) => {
            let data = array.to_data();
            let [run_ends, values] = data.child_data() else {
                return Ok(None);
            };
            let values = renamed_array(&make_array(values.clone()), values_field.data_type())?;
            let rebuilt = data
                .clone()
                .into_builder()
                .data_type(target.clone())
                .child_data(vec![run_ends.clone(), values.to_data()])
                .build()?;
            Ok(Some(make_array(rebuilt)))
        }
        _ => Ok(None),
    }
}

fn renamed_list(array: &ArrayRef, target: &DataType) -> Result<Option<ArrayRef>> {
    let renamed: ArrayRef = match target {
        DataType::List(field) => {
            let Some(source) = array.as_list_opt::<i32>() else {
                return Ok(None);
            };
            Arc::new(ListArray::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                renamed_array(source.values(), field.data_type())?,
                source.nulls().cloned(),
            )?)
        }
        DataType::LargeList(field) => {
            let Some(source) = array.as_list_opt::<i64>() else {
                return Ok(None);
            };
            Arc::new(LargeListArray::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                renamed_array(source.values(), field.data_type())?,
                source.nulls().cloned(),
            )?)
        }
        DataType::FixedSizeList(field, size) => {
            let Some(source) = array.as_fixed_size_list_opt() else {
                return Ok(None);
            };
            Arc::new(FixedSizeListArray::try_new(
                Arc::clone(field),
                *size,
                renamed_array(source.values(), field.data_type())?,
                source.nulls().cloned(),
            )?)
        }
        DataType::ListView(field) => {
            let Some(source) = array.as_list_view_opt::<i32>() else {
                return Ok(None);
            };
            Arc::new(GenericListViewArray::<i32>::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                source.sizes().clone(),
                renamed_array(source.values(), field.data_type())?,
                source.nulls().cloned(),
            )?)
        }
        DataType::LargeListView(field) => {
            let Some(source) = array.as_list_view_opt::<i64>() else {
                return Ok(None);
            };
            Arc::new(GenericListViewArray::<i64>::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                source.sizes().clone(),
                renamed_array(source.values(), field.data_type())?,
                source.nulls().cloned(),
            )?)
        }
        DataType::Map(field, sorted) => {
            let Some(source) = array.as_map_opt() else {
                return Ok(None);
            };
            let entries: ArrayRef = Arc::new(source.entries().clone());
            let entries = renamed_array(&entries, field.data_type())?;
            Arc::new(MapArray::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                entries.as_struct().clone(),
                source.nulls().cloned(),
                *sorted,
            )?)
        }
        _ => return Ok(None),
    };
    Ok(Some(renamed))
}

fn renamed_struct(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    let DataType::Struct(fields) = target else {
        return Ok(Arc::clone(array));
    };
    let Some(source) = array.as_struct_opt() else {
        return Err(DataFusionError::Execution(format!(
            "{STRUCT_BY_POSITION} expects a struct, got {}",
            array.data_type()
        )));
    };
    let columns = source
        .columns()
        .iter()
        .zip(fields.iter())
        .map(|(column, field)| renamed_array(column, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        columns,
        source.nulls().cloned(),
    )?))
}
