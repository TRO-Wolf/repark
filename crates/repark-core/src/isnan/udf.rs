use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, AsArray, BooleanArray, BooleanBuilder};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{Result, exec_err, plan_err};
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

fn spark_container_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Struct(fields) => {
            let inner = fields
                .iter()
                .map(|field| {
                    format!(
                        "{}: {}",
                        field.name(),
                        spark_container_type(field.data_type())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("STRUCT<{inner}>")
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_container_type(field.data_type()))
        }
        DataType::Map(entry, _) => match entry.data_type() {
            DataType::Struct(pair) if pair.len() == 2 => format!(
                "MAP<{}, {}>",
                spark_container_type(pair[0].data_type()),
                spark_container_type(pair[1].data_type())
            ),
            _ => "MAP".to_string(),
        },
        other => crate::update_fields::spark_sql_type(other),
    }
}

fn refuse_container_arg(field: &FieldRef) -> Result<()> {
    if !matches!(
        field.data_type(),
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
    ) {
        return Ok(());
    }
    plan_err!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"isnan({})\" due to data \
         type mismatch: The argument requires a numeric or string type, however \"{}\" has the \
         type \"{}\". SQLSTATE: 42K09",
        field.name(),
        field.name(),
        spark_container_type(field.data_type())
    )
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if let Some(first) = args.arg_fields.first() {
            refuse_container_arg(first)?;
        }
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
