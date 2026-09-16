use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray, StringBuilder, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::reader::{JsonValue, json_number_text, parse_json, write_compact};
use crate::java_double::java_double_text;

pub(crate) const JSON_TUPLE_UDF: &str = "__repark_json_tuple";

#[must_use]
pub fn json_tuple_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkJsonTuple::new()))
}

#[must_use]
pub fn json_tuple_output(count: usize) -> DataType {
    DataType::Struct(
        (0..count)
            .map(|index| Arc::new(Field::new(format!("c{index}"), DataType::Utf8, true)))
            .collect::<Vec<_>>()
            .into(),
    )
}

pub(crate) fn wrong_num_args(got: usize) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Plan(format!(
        "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `json_tuple` requires > 1 parameters but the \
         actual number is {got}. Please, refer to \
         'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. SQLSTATE: 42605"
    ))
}

fn text_array(array: &ArrayRef) -> Result<StringArray> {
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().clone()),
        DataType::Null => Ok(StringArray::new_null(array.len())),
        _ => {
            let cast = datafusion::arrow::compute::cast(array.as_ref(), &DataType::Utf8)?;
            Ok(cast.as_string::<i32>().clone())
        }
    }
}

fn append_tuple_value(builder: &mut StringBuilder, value: &JsonValue<'_>) {
    match value {
        JsonValue::Null => builder.append_null(),
        JsonValue::Text(text) => builder.append_value(text.as_ref()),
        JsonValue::Number(raw) => builder.append_value(json_number_text(raw)),
        JsonValue::Bool(flag) => builder.append_value(if *flag { "true" } else { "false" }),
        JsonValue::NonFinite(found) => builder.append_value(java_double_text(*found)),
        other => {
            let mut rendered = String::new();
            write_compact(other, &mut rendered);
            builder.append_value(&rendered);
        }
    }
}

#[derive(Debug)]
struct SparkJsonTuple {
    signature: Signature,
}

impl SparkJsonTuple {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkJsonTuple {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkJsonTuple {}

impl Hash for SparkJsonTuple {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkJsonTuple {
    fn name(&self) -> &str {
        JSON_TUPLE_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        if args.arg_fields.len() < 2 {
            return Err(wrong_num_args(args.arg_fields.len()));
        }
        Ok(Arc::new(Field::new(
            self.name(),
            json_tuple_output(args.arg_fields.len() - 1),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 {
            return Err(wrong_num_args(arg_types.len()));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            return Err(wrong_num_args(args.args.len()));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let documents = text_array(&arrays[0])?;
        let mut fields: Vec<StringArray> = Vec::with_capacity(arrays.len() - 1);
        for array in &arrays[1..] {
            fields.push(text_array(array)?);
        }
        let mut builders: Vec<StringBuilder> = (0..fields.len())
            .map(|_| StringBuilder::with_capacity(documents.len(), documents.len() * 8))
            .collect();
        for row in 0..documents.len() {
            let object = if documents.is_null(row) {
                None
            } else {
                match parse_json(documents.value(row)) {
                    Some(JsonValue::Object(entries)) => Some(entries),
                    _ => None,
                }
            };
            for (index, builder) in builders.iter_mut().enumerate() {
                let name = if fields[index].is_null(row) {
                    None
                } else {
                    Some(fields[index].value(row))
                };
                match (&object, name) {
                    (Some(entries), Some(key)) => {
                        match entries.iter().rev().find(|(name, _)| name.as_ref() == key) {
                            None | Some((_, JsonValue::Null)) => builder.append_null(),
                            Some((_, found)) => append_tuple_value(builder, found),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
        }
        let DataType::Struct(output) = json_tuple_output(fields.len()) else {
            return plan_err!("'{JSON_TUPLE_UDF}' builds a struct output");
        };
        let columns: Vec<ArrayRef> = builders
            .into_iter()
            .map(|mut builder| Arc::new(builder.finish()) as ArrayRef)
            .collect();
        Ok(ColumnarValue::Array(Arc::new(StructArray::try_new(
            output, columns, None,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, ArrayRef, StringArray, StructArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};

    #[test]
    fn tuple_reads_a_sliced_string_array_without_a_panic() {
        let full = StringArray::from(vec!["x", r#"{"a":7}"#, r#"{"a":8}"#, "y"]);
        let documents: ArrayRef = Arc::new(full.slice(1, 2));
        let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "a"]));
        let udf = crate::json::tuple::json_tuple_udf();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(documents), ColumnarValue::Array(names)],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new(
                "__repark_json_tuple",
                crate::json::tuple::json_tuple_output(1),
                true,
            )),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let decoded = udf.invoke_with_args(args).expect("sliced documents decode");
        let ColumnarValue::Array(array) = decoded else {
            panic!("tuple answers an array");
        };
        let structs = array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct");
        let first = structs
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(first.value(0), "7");
        assert_eq!(first.value(1), "8");
    }
}
