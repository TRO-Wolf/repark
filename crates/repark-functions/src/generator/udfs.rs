use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Builder, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

const GENERATOR_ORDINALITY_UDF: &str = "__repark_gen_ordinality";
const GENERATOR_FIELD_UDF: &str = "__repark_gen_field";

#[derive(Debug)]
pub(super) struct GeneratorOrdinality {
    signature: Signature,
}

impl GeneratorOrdinality {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for GeneratorOrdinality {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for GeneratorOrdinality {}

impl Hash for GeneratorOrdinality {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

impl ScalarUDFImpl for GeneratorOrdinality {
    fn name(&self) -> &'static str {
        GENERATOR_ORDINALITY_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            false,
        ))))
    }

    fn return_field_from_args(
        &self,
        args: datafusion::logical_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_some_and(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&[])?,
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(first) = args.args.into_iter().next() else {
            return exec_err!("'{GENERATOR_ORDINALITY_UDF}' requires one list argument");
        };
        match first {
            ColumnarValue::Array(array) => {
                let Some(list) = array.as_any().downcast_ref::<ListArray>() else {
                    return exec_err!("'{GENERATOR_ORDINALITY_UDF}' requires a list argument");
                };
                Ok(ColumnarValue::Array(Arc::new(ordinality(list)?)))
            }
            ColumnarValue::Scalar(ScalarValue::List(list)) => Ok(ColumnarValue::Scalar(
                ScalarValue::List(Arc::new(ordinality(list.as_ref())?)),
            )),
            other @ ColumnarValue::Scalar(_) => {
                exec_err!("'{GENERATOR_ORDINALITY_UDF}' received a non-list argument: {other:?}")
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct GeneratorField {
    signature: Signature,
}

impl GeneratorField {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl PartialEq for GeneratorField {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for GeneratorField {}

impl Hash for GeneratorField {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

fn field_name_literal<'a>(
    args: &'a datafusion::logical_expr::ReturnFieldArgs<'a>,
) -> Result<&'a str> {
    if let Some(Some(
        ScalarValue::Utf8(Some(name))
        | ScalarValue::LargeUtf8(Some(name))
        | ScalarValue::Utf8View(Some(name)),
    )) = args.scalar_arguments.get(1)
    {
        return Ok(name.as_str());
    }
    plan_err!("'{GENERATOR_FIELD_UDF}' requires a literal field name")
}

fn struct_field<'a>(data_type: &'a DataType, name: &str) -> Result<&'a FieldRef> {
    let DataType::Struct(fields) = data_type else {
        return exec_err!("'{GENERATOR_FIELD_UDF}' requires a struct argument, got {data_type}");
    };
    let Some(index) = fields.iter().position(|field| field.name() == name) else {
        return exec_err!("'{GENERATOR_FIELD_UDF}' found no field named '{name}'");
    };
    Ok(&fields[index])
}

impl ScalarUDFImpl for GeneratorField {
    fn name(&self) -> &'static str {
        GENERATOR_FIELD_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn return_field_from_args(
        &self,
        args: datafusion::logical_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        let name = field_name_literal(&args)?;
        let Some(base) = args.arg_fields.first() else {
            return exec_err!("'{GENERATOR_FIELD_UDF}' requires a struct argument");
        };
        let field = struct_field(base.data_type(), name)?;
        Ok(Arc::new(Field::new(name, field.data_type().clone(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut iter = args.args.into_iter();
        let (Some(base), Some(name_arg)) = (iter.next(), iter.next()) else {
            return exec_err!("'{GENERATOR_FIELD_UDF}' requires a struct and a field name");
        };
        let ColumnarValue::Scalar(name_scalar) = name_arg else {
            return exec_err!("'{GENERATOR_FIELD_UDF}' requires a literal field name");
        };
        let (ScalarValue::Utf8(Some(name))
        | ScalarValue::LargeUtf8(Some(name))
        | ScalarValue::Utf8View(Some(name))) = name_scalar
        else {
            return exec_err!("'{GENERATOR_FIELD_UDF}' requires a literal field name");
        };
        let mask = |array: &Arc<dyn Array>| -> Result<datafusion::arrow::array::ArrayRef> {
            let Some(structs) = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StructArray>()
            else {
                return exec_err!("'{GENERATOR_FIELD_UDF}' requires a struct argument");
            };
            let Some(child) = structs.column_by_name(name.as_str()) else {
                return exec_err!("'{GENERATOR_FIELD_UDF}' found no field named '{name}'");
            };
            let combined =
                datafusion::arrow::buffer::NullBuffer::union(structs.nulls(), child.nulls());
            let data = child.to_data().into_builder().nulls(combined).build()?;
            Ok(datafusion::arrow::array::make_array(data))
        };
        match base {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(mask(&array)?)),
            ColumnarValue::Scalar(ScalarValue::Struct(structs)) => {
                let dyn_array: Arc<dyn Array> = structs;
                let masked = mask(&dyn_array)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    masked.as_ref(),
                    0,
                )?))
            }
            other @ ColumnarValue::Scalar(_) => {
                exec_err!("'{GENERATOR_FIELD_UDF}' requires a struct argument: {other:?}")
            }
        }
    }
}

pub(super) fn ordinality(list: &ListArray) -> Result<ListArray> {
    let mut values = Int32Builder::with_capacity(list.values().len());
    let mut offsets: Vec<i32> = Vec::with_capacity(list.len() + 1);
    let mut running: i32 = 0;
    offsets.push(running);
    for window in list.offsets().windows(2) {
        let Some(length) = window[1].checked_sub(window[0]) else {
            return exec_err!("generator ordinality offset underflow");
        };
        if length < 0 {
            return exec_err!("generator ordinality negative list length");
        }
        for position in 0..length {
            values.append_value(position);
        }
        let Some(next) = running.checked_add(length) else {
            return exec_err!("generator ordinality offset overflow");
        };
        running = next;
        offsets.push(running);
    }
    let offset_buffer = OffsetBuffer::new(offsets.into());
    Ok(ListArray::try_new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        offset_buffer,
        Arc::new(values.finish()),
        list.nulls().cloned(),
    )?)
}
