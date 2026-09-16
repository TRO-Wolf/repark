use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::{
    CsvOptions, options_from_map, parse_csv_date, parse_csv_timestamp, read_options_array,
    string_column,
};

pub(crate) const SCHEMA_OF_CSV_UDF: &str = "schema_of_csv";

const TIMESTAMP_PATTERNS: [&str; 2] = ["yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss"];

#[must_use]
pub fn schema_of_csv_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSchemaOfCsv::new()))
}

fn wrong_num_args(got: usize) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Plan(format!(
        "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `schema_of_csv` requires [1, 2] parameters but \
         the actual number is {got}. Please, refer to \
         'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. SQLSTATE: 42605"
    ))
}

fn integer_shape(token: &str) -> bool {
    let body = token
        .strip_prefix('+')
        .or_else(|| token.strip_prefix('-'))
        .unwrap_or(token);
    !body.is_empty() && body.bytes().all(|byte| byte.is_ascii_digit())
}

fn infer_timestamp(token: &str, options: &CsvOptions) -> bool {
    if let Some(format) = options.timestamp_format.as_deref() {
        return parse_csv_timestamp(token, Some(format)).is_some();
    }
    TIMESTAMP_PATTERNS
        .iter()
        .any(|pattern| parse_csv_timestamp(token, Some(pattern)).is_some())
}

fn infer_token(token: &str, options: &CsvOptions) -> &'static str {
    if token.is_empty() {
        return "STRING";
    }
    if integer_shape(token) {
        if token.parse::<i32>().is_ok() {
            return "INT";
        }
        if token.parse::<i64>().is_ok() {
            return "BIGINT";
        }
        if token.parse::<f64>().is_ok() {
            return "DOUBLE";
        }
        return "STRING";
    }
    if !token.contains('_') && token.parse::<f64>().is_ok() {
        return "DOUBLE";
    }
    if infer_timestamp(token, options) {
        return "TIMESTAMP";
    }
    if matches!(token.to_ascii_lowercase().as_str(), "true" | "false") {
        return "BOOLEAN";
    }
    if parse_csv_date(token, options.date_format.as_deref()).is_some() {
        return "DATE";
    }
    "STRING"
}

pub(crate) fn infer_csv_schema(csv: &str, options: &CsvOptions) -> Result<String> {
    if csv.is_empty() {
        return plan_err!(
            "[INTERNAL_ERROR] Cannot infer a CSV schema from an empty document. SQLSTATE: XX000"
        );
    }
    let tokens = super::split_csv_record(csv, options);
    let mut fields = Vec::with_capacity(tokens.len());
    for (index, token) in tokens.iter().enumerate() {
        fields.push(format!("_c{index}: {}", infer_token(token, options)));
    }
    Ok(format!("STRUCT<{}>", fields.join(", ")))
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn unexpected_input_type(data_type: &DataType) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"schema_of_csv\" due to data \
         type mismatch: The first parameter requires the \"STRING\" type, however the input has \
         the type \"{}\". SQLSTATE: 42K09",
        super::spark_type_name(data_type)
    ))
}

#[derive(Debug)]
struct SparkSchemaOfCsv {
    signature: Signature,
}

impl SparkSchemaOfCsv {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkSchemaOfCsv {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkSchemaOfCsv {}

impl Hash for SparkSchemaOfCsv {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkSchemaOfCsv {
    fn name(&self) -> &str {
        SCHEMA_OF_CSV_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        if args.arg_fields.len() < 1 || args.arg_fields.len() > 2 {
            return Err(wrong_num_args(args.arg_fields.len()));
        }
        if !is_string_type(args.arg_fields[0].data_type())
            && !matches!(args.arg_fields[0].data_type(), DataType::Null)
        {
            return Err(unexpected_input_type(args.arg_fields[0].data_type()));
        }
        if args.arg_fields.len() == 2 {
            match args.arg_fields[1].data_type() {
                DataType::Map(entry, _) => {
                    let DataType::Struct(pair) = entry.data_type() else {
                        return plan_err!(
                            "[INVALID_OPTIONS.NON_MAP_FUNCTION] Must use the `map()` function \
                             for options."
                        );
                    };
                    let strings = pair.iter().all(|field| is_string_type(field.data_type()));
                    if !strings || pair.len() != 2 {
                        return plan_err!(
                            "[INVALID_OPTIONS.NON_STRING_TYPE] The options expression must be a \
                             map of strings."
                        );
                    }
                }
                _ => {
                    return plan_err!(
                        "[INVALID_OPTIONS.NON_MAP_FUNCTION] Must use the `map()` function for \
                         options."
                    );
                }
            }
            if let Some(scalar @ ScalarValue::Map(_)) = args.scalar_arguments[1] {
                options_from_map(scalar)?;
            }
        }
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, false)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 1 || arg_types.len() > 2 {
            return Err(wrong_num_args(arg_types.len()));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 1 || args.args.len() > 2 {
            return Err(wrong_num_args(args.args.len()));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if !is_string_type(arrays[0].data_type()) {
            return Err(unexpected_input_type(arrays[0].data_type()));
        }
        let documents = string_column(&arrays[0])?;
        let options = if arrays.len() == 2 {
            read_options_array(&arrays[1])?
        } else {
            CsvOptions::default()
        };
        let mut builder = StringBuilder::with_capacity(documents.len(), documents.len() * 32);
        for row in 0..documents.len() {
            if documents.is_null(row) {
                return exec_err!(
                    "[DATATYPE_MISMATCH.UNEXPECTED_NULL] Cannot resolve \"schema_of_csv\" due \
                     to data type mismatch: The csv must not be null. SQLSTATE: 42K09"
                );
            }
            match infer_csv_schema(documents.value(row), &options) {
                Ok(schema) => builder.append_value(schema),
                Err(error) => return Err(error),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as _))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::prelude::SessionContext;

    use super::super::CsvOptions;
    use super::infer_csv_schema;

    fn run(sql: &str) -> datafusion::common::Result<Vec<RecordBatch>> {
        let context = SessionContext::new();
        crate::register_all(&context);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async { context.sql(sql).await?.collect().await })
    }

    fn shown(sql: &str) -> String {
        let batches = run(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        ScalarValue::try_from_array(batches[0].column(0), 0)
            .expect("scalar")
            .to_string()
    }

    #[test]
    fn schema_of_csv_follows_the_spark_type_ladder() {
        let options = CsvOptions::default();
        assert_eq!(
            infer_csv_schema(
                "1,abc,2.5,true,2024-01-01,2024-01-01 10:00:00,,1e3,99999999999",
                &options
            )
            .expect("ladder"),
            "STRUCT<_c0: INT, _c1: STRING, _c2: DOUBLE, _c3: BOOLEAN, _c4: DATE, _c5: TIMESTAMP, \
             _c6: STRING, _c7: DOUBLE, _c8: BIGINT>"
        );
    }

    #[test]
    fn schema_of_csv_keeps_a_quoted_separator_inside_one_field() {
        assert_eq!(
            shown(r#"SELECT schema_of_csv('"a,b",1.0')"#),
            "STRUCT<_c0: STRING, _c1: DOUBLE>"
        );
    }

    #[test]
    fn schema_of_csv_applies_the_sep_option_before_inference() {
        assert_eq!(
            shown(r#"SELECT schema_of_csv('1|x', map('sep', '|'))"#),
            "STRUCT<_c0: INT, _c1: STRING>"
        );
    }

    #[test]
    fn schema_of_csv_refuses_an_empty_document_with_the_spark_defect() {
        let error = run("SELECT schema_of_csv('')").expect_err("empty must raise");
        assert!(error.to_string().contains("INTERNAL_ERROR"), "{error}");
    }

    #[test]
    fn schema_of_csv_refuses_a_null_document() {
        let error = run("SELECT schema_of_csv(CAST(NULL AS STRING))").expect_err("null must raise");
        assert!(error.to_string().contains("UNEXPECTED_NULL"), "{error}");
    }

    #[test]
    fn schema_of_csv_refuses_a_non_string_document() {
        let error = run("SELECT schema_of_csv(5)").expect_err("int must raise");
        assert!(
            error.to_string().contains("UNEXPECTED_INPUT_TYPE"),
            "{error}"
        );
    }

    #[test]
    fn schema_of_csv_refuses_non_map_options() {
        let error = run("SELECT schema_of_csv('a', 'b')").expect_err("options must raise");
        assert!(error.to_string().contains("NON_MAP_FUNCTION"), "{error}");
    }
}
