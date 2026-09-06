use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Int32Builder, ListBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::path::{evaluate_path, parse_path};
use super::reader::{JsonValue, parse_json};

#[must_use]
pub fn get_json_object_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkGetJsonObject::new()))
}

#[must_use]
pub fn json_array_length_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkJsonArrayLength::new()))
}

#[must_use]
pub fn json_object_keys_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkJsonObjectKeys::new()))
}

fn text_column(array: &ArrayRef, name: &str, position: usize) -> Result<StringArray> {
    match array.data_type() {
        DataType::Utf8 => {
            Ok(datafusion::arrow::array::AsArray::as_string::<i32>(array.as_ref()).clone())
        }
        DataType::Null => Ok(StringArray::new_null(array.len())),
        other => exec_err!("'{name}' argument {position} must be STRING, got {other}"),
    }
}

fn string_arguments(
    name: &str,
    arity: usize,
    args: &ScalarFunctionArgs,
) -> Result<Vec<StringArray>> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays.len() != arity {
        return exec_err!("'{name}' requires {arity} arguments, got {}", arrays.len());
    }
    arrays
        .iter()
        .enumerate()
        .map(|(index, array)| text_column(array, name, index + 1))
        .collect()
}

macro_rules! json_shim_traits {
    ($shim:ident, $name_literal:literal) => {
        impl $shim {
            fn new() -> Self {
                Self {
                    signature: Signature::user_defined(Volatility::Immutable),
                }
            }
        }

        impl PartialEq for $shim {
            fn eq(&self, _other: &Self) -> bool {
                true
            }
        }

        impl Eq for $shim {}

        impl Hash for $shim {
            fn hash<H: Hasher>(&self, state: &mut H) {
                $name_literal.hash(state);
            }
        }
    };
}

#[derive(Debug)]
struct SparkGetJsonObject {
    signature: Signature,
}

json_shim_traits!(SparkGetJsonObject, "get_json_object");

impl ScalarUDFImpl for SparkGetJsonObject {
    crate::shim_udf_boilerplate!("get_json_object");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() == 2 {
            Ok(vec![DataType::Utf8; 2])
        } else {
            exec_err!(
                "'get_json_object' requires 2 arguments, got {}",
                arg_types.len()
            )
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let columns = string_arguments("get_json_object", 2, &args)?;
        let (documents, paths) = (&columns[0], &columns[1]);
        let mut builder = StringBuilder::with_capacity(documents.len(), documents.len() * 8);
        for row in 0..documents.len() {
            if documents.is_null(row) || paths.is_null(row) {
                builder.append_null();
                continue;
            }
            let answer = parse_path(paths.value(row))
                .and_then(|steps| {
                    parse_json(documents.value(row))
                        .and_then(|value| evaluate_path(&value, &steps, true))
                })
                .map(|found| found.plain.unwrap_or(found.json));
            match answer {
                Some(text) => builder.append_value(text),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[derive(Debug)]
struct SparkJsonArrayLength {
    signature: Signature,
}

json_shim_traits!(SparkJsonArrayLength, "json_array_length");

impl ScalarUDFImpl for SparkJsonArrayLength {
    crate::shim_udf_boilerplate!("json_array_length");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() == 1 {
            Ok(vec![DataType::Utf8])
        } else {
            exec_err!(
                "'json_array_length' requires 1 argument, got {}",
                arg_types.len()
            )
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let columns = string_arguments("json_array_length", 1, &args)?;
        let documents = &columns[0];
        let mut builder = Int32Builder::with_capacity(documents.len());
        for row in 0..documents.len() {
            if documents.is_null(row) {
                builder.append_null();
                continue;
            }
            match parse_json(documents.value(row)) {
                Some(JsonValue::Array(items)) => match i32::try_from(items.len()) {
                    Ok(length) => builder.append_value(length),
                    Err(_) => builder.append_null(),
                },
                _ => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[derive(Debug)]
struct SparkJsonObjectKeys {
    signature: Signature,
}

json_shim_traits!(SparkJsonObjectKeys, "json_object_keys");

fn keys_element_field() -> FieldRef {
    Arc::new(Field::new("element", DataType::Utf8, false))
}

impl ScalarUDFImpl for SparkJsonObjectKeys {
    crate::shim_udf_boilerplate!("json_object_keys");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(keys_element_field()))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(keys_element_field()),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() == 1 {
            Ok(vec![DataType::Utf8])
        } else {
            exec_err!(
                "'json_object_keys' requires 1 argument, got {}",
                arg_types.len()
            )
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let columns = string_arguments("json_object_keys", 1, &args)?;
        let documents = &columns[0];
        let values = StringBuilder::new();
        let mut builder = ListBuilder::new(values).with_field(keys_element_field());
        for row in 0..documents.len() {
            if documents.is_null(row) {
                builder.append_null();
                continue;
            }
            match parse_json(documents.value(row)) {
                Some(JsonValue::Object(entries)) => {
                    for (key, _) in &entries {
                        builder.values().append_value(key.as_ref());
                    }
                    builder.append(true);
                }
                _ => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
