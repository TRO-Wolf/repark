use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::ddl::parse_schema;
use super::decode::{DecodeContext, build_root};
use super::reader::{JsonValue, parse_json};
use crate::session_time_zone::session_time_zone_from_options;
use crate::timestamp_cast::parse_session_zone;

#[must_use]
pub fn from_json_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkFromJson::new()))
}

const DEFAULT_CORRUPT_COLUMN: &str = "_corrupt_record";

struct ParseOptions {
    fail_fast: bool,
    corrupt_column: String,
}

fn read_options(value: Option<&ScalarValue>) -> Result<ParseOptions> {
    let mut options = ParseOptions {
        fail_fast: false,
        corrupt_column: DEFAULT_CORRUPT_COLUMN.to_string(),
    };
    let Some(scalar) = value else {
        return Ok(options);
    };
    for (key, setting) in scalar_map_entries(scalar)? {
        match key.to_ascii_lowercase().as_str() {
            "mode" => match setting.to_ascii_uppercase().as_str() {
                "PERMISSIVE" => options.fail_fast = false,
                "FAILFAST" => options.fail_fast = true,
                other => {
                    return exec_err!(
                        "[PARSE_MODE_UNSUPPORTED] The function `from_json` doesn't support the \
                         {other} mode. Acceptable modes are PERMISSIVE and FAILFAST."
                    );
                }
            },
            "columnnameofcorruptrecord" => options.corrupt_column = setting,
            other => {
                return exec_err!(
                    "'from_json' option {other:?} is not supported by repark; only `mode` and \
                     `columnNameOfCorruptRecord` are. Spark honours the full JSON option set."
                );
            }
        }
    }
    Ok(options)
}

fn scalar_map_entries(scalar: &ScalarValue) -> Result<Vec<(String, String)>> {
    let ScalarValue::Map(map) = scalar else {
        return exec_err!("'from_json' expects a literal MAP<STRING, STRING> of options");
    };
    if map.is_empty() {
        return Ok(Vec::new());
    }
    let entries = map.value(0);
    let keys = entries.column(0);
    let values = entries.column(1);
    let (Some(keys), Some(values)) = (
        keys.as_any().downcast_ref::<StringArray>(),
        values.as_any().downcast_ref::<StringArray>(),
    ) else {
        return exec_err!("'from_json' expects a literal MAP<STRING, STRING> of options");
    };
    let mut pairs = Vec::with_capacity(keys.len());
    for row in 0..keys.len() {
        if keys.is_null(row) || values.is_null(row) {
            continue;
        }
        pairs.push((keys.value(row).to_string(), values.value(row).to_string()));
    }
    Ok(pairs)
}

fn schema_from_argument(value: &ColumnarValue) -> Result<DataType> {
    let ColumnarValue::Scalar(ScalarValue::Utf8(Some(spec))) = value else {
        return exec_err!("'from_json' requires a foldable STRING schema argument");
    };
    parse_schema(spec)
}

fn corrupt_index(target: &DataType, name: &str) -> Result<Option<usize>> {
    let DataType::Struct(fields) = target else {
        return Ok(None);
    };
    match fields.iter().position(|field| field.name() == name) {
        Some(index) if fields[index].data_type() == &DataType::Utf8 => Ok(Some(index)),
        Some(index) => exec_err!(
            "[INVALID_CORRUPT_RECORD_TYPE] The column `{name}` for corrupt records must have \
             the nullable STRING type, but got {}",
            fields[index].data_type()
        ),
        None => Ok(None),
    }
}

fn refuse_unsupported_schema(target: &DataType) -> Result<()> {
    if matches!(
        target,
        DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _)
    ) {
        return Ok(());
    }
    exec_err!(
        "[DATATYPE_MISMATCH.INVALID_JSON_SCHEMA] Input schema {target} must be a struct, an \
         array or a map"
    )
}

#[derive(Debug)]
struct SparkFromJson {
    signature: Signature,
}

impl SparkFromJson {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkFromJson {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkFromJson {}

impl Hash for SparkFromJson {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkFromJson {
    crate::shim_udf_boilerplate!("from_json");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        exec_err!("'from_json' resolves its result type from the schema argument")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let Some(Some(ScalarValue::Utf8(Some(spec)))) = args.scalar_arguments.get(1) else {
            return exec_err!(
                "'from_json' requires a foldable STRING schema argument in position 2"
            );
        };
        Ok(Arc::new(Field::new(self.name(), parse_schema(spec)?, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [_, _] => Ok(vec![DataType::Utf8, DataType::Utf8]),
            [_, _, options] => Ok(vec![DataType::Utf8, DataType::Utf8, options.clone()]),
            _ => exec_err!(
                "'from_json' requires 2 or 3 arguments, got {}",
                arg_types.len()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(schema_argument) = args.args.get(1) else {
            return exec_err!(
                "'from_json' requires 2 or 3 arguments, got {}",
                args.args.len()
            );
        };
        let target = schema_from_argument(schema_argument)?;
        refuse_unsupported_schema(&target)?;
        let options = read_options(match args.args.get(2) {
            Some(ColumnarValue::Scalar(scalar)) => Some(scalar),
            Some(ColumnarValue::Array(_)) => {
                return exec_err!("'from_json' requires a foldable MAP options argument");
            }
            None => None,
        })?;
        let corrupt = corrupt_index(&target, &options.corrupt_column)?;
        let arrays = ColumnarValue::values_to_arrays(&args.args[..1])?;
        let documents = arrays[0].as_string::<i32>();
        let zone =
            parse_session_zone(session_time_zone_from_options(args.config_options.as_ref()))?;
        let context = DecodeContext { zone };
        let mut parsed: Vec<Option<JsonValue<'_>>> = Vec::with_capacity(documents.len());
        let mut sources: Vec<Option<&str>> = Vec::with_capacity(documents.len());
        let mut unparsed: Vec<bool> = Vec::with_capacity(documents.len());
        for row in 0..documents.len() {
            if documents.is_null(row) {
                parsed.push(None);
                sources.push(None);
                unparsed.push(false);
                continue;
            }
            let text = documents.value(row);
            if text.trim().is_empty() {
                parsed.push(None);
                sources.push(None);
                unparsed.push(false);
                continue;
            }
            sources.push(Some(text));
            let value = parse_json(text);
            unparsed.push(value.is_none());
            parsed.push(Some(value.unwrap_or_else(|| JsonValue::Object(Vec::new()))));
        }
        let rows: Vec<Option<&JsonValue<'_>>> = parsed.iter().map(Option::as_ref).collect();
        let decoded = build_root(&target, &rows, &context)?;
        let mut malformed: HashMap<usize, &str> = HashMap::new();
        for (row, source) in sources.iter().enumerate() {
            let Some(text) = source else {
                continue;
            };
            if unparsed[row] || decoded.bad[row] {
                if options.fail_fast {
                    return exec_err!(
                        "[MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] `from_json` in \
                         FAILFAST mode rejects the malformed record {text:?}"
                    );
                }
                malformed.insert(row, text);
            }
        }
        let repaired = fill_corrupt_column(decoded.array, &target, corrupt, &malformed)?;
        Ok(ColumnarValue::Array(repaired))
    }
}

fn fill_corrupt_column(
    built: ArrayRef,
    target: &DataType,
    corrupt: Option<usize>,
    malformed: &HashMap<usize, &str>,
) -> Result<ArrayRef> {
    let Some(index) = corrupt else {
        return Ok(built);
    };
    let DataType::Struct(fields) = target else {
        return Ok(built);
    };
    let entries = built.as_struct();
    let mut columns: Vec<ArrayRef> = entries.columns().to_vec();
    let mut texts: Vec<Option<&str>> = vec![None; built.len()];
    for (row, text) in malformed {
        if *row < texts.len() {
            texts[*row] = Some(*text);
        }
    }
    if texts.iter().all(Option::is_none) {
        return Ok(built);
    }
    columns[index] = Arc::new(StringArray::from(texts));
    let nulls = entries.nulls().cloned();
    Ok(Arc::new(datafusion::arrow::array::StructArray::try_new(
        fields.clone(),
        columns,
        nulls,
    )?))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::prelude::SessionContext;

    fn run(sql: &str) -> datafusion::common::Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async { ctx.sql(sql).await?.collect().await })
    }

    fn shown(sql: &str) -> String {
        let batches = run(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        ScalarValue::try_from_array(batches[0].column(0), 0)
            .expect("scalar")
            .to_string()
    }

    #[test]
    fn from_json_is_permissive_by_default() {
        assert_eq!(shown(r#"SELECT from_json('{"a":1}', 'a INT')"#), "{a:1}");
        assert_eq!(shown(r"SELECT from_json('{bad', 'a INT')"), "{a:}");
        assert_eq!(shown(r#"SELECT from_json('{"z":1}', 'a INT')"#), "{a:}");
        assert_eq!(shown(r#"SELECT from_json('{"a":"x"}', 'a INT')"#), "{a:}");
        assert_eq!(shown(r#"SELECT from_json('{"a":1.7}', 'a INT')"#), "{a:}");
        assert_eq!(
            shown(r#"SELECT from_json('{"a":99999999999}', 'a INT')"#),
            "{a:}"
        );
        assert_eq!(shown(r#"SELECT from_json('{"a":1}', 'a STRING')"#), "{a:1}");
    }

    #[test]
    fn from_json_fills_the_corrupt_record_column() {
        assert_eq!(
            shown(r"SELECT from_json('{bad', 'a INT, _corrupt_record STRING')"),
            "{a:,_corrupt_record:{bad}"
        );
    }

    #[test]
    fn from_json_failfast_raises_and_an_unknown_option_refuses() {
        let failfast = run(r"SELECT from_json('{bad', 'a INT', map(['mode'], ['FAILFAST']))")
            .expect_err("FAILFAST must raise");
        assert!(
            failfast.to_string().contains("MALFORMED_RECORD_IN_PARSING"),
            "{failfast}"
        );
        let dropped = run(r"SELECT from_json('{bad', 'a INT', map(['mode'], ['DROPMALFORMED']))")
            .expect_err("DROPMALFORMED must raise");
        assert!(
            dropped.to_string().contains("PARSE_MODE_UNSUPPORTED"),
            "{dropped}"
        );
        let unknown = run(r#"SELECT from_json('{"a":1}', 'a INT', map(['zzz'], ['1']))"#)
            .expect_err("an unsupported option must refuse");
        assert!(
            unknown.to_string().contains("not supported by repark"),
            "{unknown}"
        );
    }

    #[test]
    fn from_json_reads_nested_and_container_schemas() {
        assert_eq!(
            shown(r#"SELECT from_json('{"a":{"b":[1,2]}}', 'a STRUCT<b: ARRAY<INT>>')"#),
            "{a:{b:[1, 2]}}"
        );
        assert_eq!(
            shown(r#"SELECT from_json('[{"a":1},{"a":2}]', 'ARRAY<STRUCT<a: INT>>')"#),
            "[{a: 1}, {a: 2}]"
        );
        assert_eq!(
            shown(r#"SELECT from_json('{"a":1}', 'STRUCT<a: INT>')"#),
            "{a:1}"
        );
        assert_eq!(shown(r#"SELECT from_json('{"a":1}', 'a int')"#), "{a:1}");
    }
}
