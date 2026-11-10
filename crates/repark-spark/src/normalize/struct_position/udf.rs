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
    match target {
        DataType::Dictionary(_, value) => {
            if let Some(source) = array.as_any_dictionary_opt() {
                let values = renamed_array(source.values(), value)?;
                return Ok(source.with_values(values));
            }
        }
        DataType::RunEndEncoded(_, values_field) => {
            let data = array.to_data();
            if let [run_ends, values] = data.child_data() {
                let values = renamed_array(&make_array(values.clone()), values_field.data_type())?;
                let rebuilt = data
                    .clone()
                    .into_builder()
                    .data_type(target.clone())
                    .child_data(vec![run_ends.clone(), values.to_data()])
                    .build()?;
                return Ok(make_array(rebuilt));
            }
        }
        DataType::List(field) => {
            if let Some(source) = array.as_list_opt::<i32>() {
                let values = renamed_array(source.values(), field.data_type())?;
                return Ok(Arc::new(ListArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )?));
            }
        }
        DataType::LargeList(field) => {
            if let Some(source) = array.as_list_opt::<i64>() {
                let values = renamed_array(source.values(), field.data_type())?;
                return Ok(Arc::new(LargeListArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )?));
            }
        }
        DataType::FixedSizeList(field, size) => {
            if let Some(source) = array.as_fixed_size_list_opt() {
                let values = renamed_array(source.values(), field.data_type())?;
                return Ok(Arc::new(FixedSizeListArray::try_new(
                    Arc::clone(field),
                    *size,
                    values,
                    source.nulls().cloned(),
                )?));
            }
        }
        DataType::ListView(field) => {
            if let Some(source) = array.as_list_view_opt::<i32>() {
                let values = renamed_array(source.values(), field.data_type())?;
                return Ok(Arc::new(GenericListViewArray::<i32>::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    source.sizes().clone(),
                    values,
                    source.nulls().cloned(),
                )?));
            }
        }
        DataType::LargeListView(field) => {
            if let Some(source) = array.as_list_view_opt::<i64>() {
                let values = renamed_array(source.values(), field.data_type())?;
                return Ok(Arc::new(GenericListViewArray::<i64>::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    source.sizes().clone(),
                    values,
                    source.nulls().cloned(),
                )?));
            }
        }
        DataType::Map(field, sorted) => {
            if let Some(source) = array.as_map_opt() {
                let entries: ArrayRef = Arc::new(source.entries().clone());
                let entries = renamed_array(&entries, field.data_type())?;
                return Ok(Arc::new(MapArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    entries.as_struct().clone(),
                    source.nulls().cloned(),
                    *sorted,
                )?));
            }
        }
        _ => {}
    }
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
