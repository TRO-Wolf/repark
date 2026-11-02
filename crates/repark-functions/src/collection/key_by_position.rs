use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, ListArray, MapArray, StructArray, new_null_array,
};
use datafusion::arrow::compute::{cast, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::format::DEFAULT_CAST_OPTIONS;
use datafusion::common::{Result, ScalarValue, exec_datafusion_err, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[must_use]
pub(crate) fn key_by_position_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(KeyByPosition {
        signature: Signature::any(2, Volatility::Immutable),
    }))
}

#[derive(Debug)]
struct KeyByPosition {
    signature: Signature,
}

impl PartialEq for KeyByPosition {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for KeyByPosition {}

impl Hash for KeyByPosition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for KeyByPosition {
    crate::shim_udf_boilerplate!("__repark_key_by_position__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_, target] = arg_types else {
            return plan_err!("__repark_key_by_position__ takes a key and a typed target");
        };
        Ok(target.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(key) = args.args.first() else {
            return exec_err!("__repark_key_by_position__ takes a key and a typed target");
        };
        let target = args.return_field.data_type();
        match key {
            ColumnarValue::Scalar(value) => {
                let rebuilt = by_position(&value.to_array_of_size(1)?, target)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &rebuilt, 0,
                )?))
            }
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(by_position(array, target)?)),
        }
    }
}

fn by_position(array: &ArrayRef, to: &DataType) -> Result<ArrayRef> {
    if array.data_type() == to {
        return Ok(Arc::clone(array));
    }
    match (array.data_type(), to) {
        (DataType::Null, _) => Ok(new_null_array(to, array.len())),
        (DataType::Struct(_), DataType::Struct(fields)) => struct_by_position(array, fields),
        (
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..),
            DataType::List(field),
        ) => list_by_position(array, field),
        (DataType::Map(..), DataType::Map(entries, sorted)) => {
            map_by_position(array, entries, *sorted)
        }
        _ => Ok(cast_with_options(array, to, &DEFAULT_CAST_OPTIONS)?),
    }
}

fn struct_by_position(array: &ArrayRef, fields: &Fields) -> Result<ArrayRef> {
    let source = array
        .as_struct_opt()
        .ok_or_else(|| exec_datafusion_err!("a struct key must be a struct array"))?;
    if source.num_columns() != fields.len() {
        return exec_err!("a struct key must have as many fields as the map key");
    }
    let columns = source
        .columns()
        .iter()
        .zip(fields.iter())
        .map(|(column, field)| by_position(column, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        columns,
        source.nulls().cloned(),
    )?))
}

fn list_by_position(array: &ArrayRef, field: &FieldRef) -> Result<ArrayRef> {
    let as_list = match array.data_type() {
        DataType::LargeList(element) | DataType::FixedSizeList(element, _) => cast(
            array,
            &DataType::List(Arc::new(Field::new_list_field(
                element.data_type().clone(),
                true,
            ))),
        )?,
        _ => Arc::clone(array),
    };
    let source = as_list
        .as_list_opt::<i32>()
        .ok_or_else(|| exec_datafusion_err!("an array key must be a list array"))?;
    let values = by_position(source.values(), field.data_type())?;
    Ok(Arc::new(ListArray::try_new(
        Arc::clone(field),
        source.offsets().clone(),
        values,
        source.nulls().cloned(),
    )?))
}

fn map_by_position(array: &ArrayRef, entries: &FieldRef, sorted: bool) -> Result<ArrayRef> {
    let source = array
        .as_map_opt()
        .ok_or_else(|| exec_datafusion_err!("a map key must be a map array"))?;
    let rebuilt = by_position(
        &(Arc::new(source.entries().clone()) as ArrayRef),
        entries.data_type(),
    )?;
    let rebuilt = rebuilt
        .as_struct_opt()
        .ok_or_else(|| exec_datafusion_err!("map entries must be a struct array"))?
        .clone();
    Ok(Arc::new(MapArray::try_new(
        Arc::clone(entries),
        source.offsets().clone(),
        rebuilt,
        source.nulls().cloned(),
        sorted,
    )?))
}
