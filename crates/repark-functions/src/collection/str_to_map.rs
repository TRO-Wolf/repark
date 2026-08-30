//! Spark `str_to_map` — both delimiters are regular expressions.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, MapBuilder, MapFieldNames, StringBuilder};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use regex::Regex;

const DEFAULT_PAIR_DELIM: &str = ",";
const DEFAULT_KV_DELIM: &str = ":";

/// UDF constructor.
#[must_use]
pub fn str_to_map_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkStrToMapRegex::new()))
}

/// Spark `str_to_map(text[, pairDelim[, keyValueDelim]])` with regex delimiters.
#[derive(Debug)]
struct SparkStrToMapRegex {
    signature: Signature,
}

impl SparkStrToMapRegex {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkStrToMapRegex {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkStrToMapRegex {}

impl Hash for SparkStrToMapRegex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn map_utf8_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            false,
        )),
        false,
    )
}

/// Bind Java's ASCII `\s`, `\d`, and `\w` classes to equivalent POSIX classes.
pub(crate) fn bind_ascii_perl_classes(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    let mut in_class = false;
    let mut class_start = false;
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek().copied() {
                Some(escape @ ('s' | 'S' | 'd' | 'D' | 'w' | 'W')) => {
                    chars.next();
                    let name = match escape.to_ascii_lowercase() {
                        's' => "space",
                        'd' => "digit",
                        _ => "word",
                    };
                    let caret = if escape.is_ascii_uppercase() { "^" } else { "" };
                    if !in_class {
                        out.push('[');
                    }
                    out.push_str("[:");
                    out.push_str(caret);
                    out.push_str(name);
                    out.push_str(":]");
                    if !in_class {
                        out.push(']');
                    }
                }
                Some(other) => {
                    chars.next();
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
            class_start = false;
            continue;
        }
        if in_class {
            if c == ']' && !class_start {
                in_class = false;
            }
            class_start = class_start && c == '^';
        } else if c == '[' {
            in_class = true;
            class_start = true;
        }
        out.push(c);
    }
    out
}

fn compile_delim(pattern: &str) -> Result<Regex> {
    let bound = bind_ascii_perl_classes(pattern);
    Regex::new(&bound).map_err(|error| {
        datafusion::common::DataFusionError::Execution(format!(
            "str_to_map: invalid delimiter regex '{pattern}': {error}"
        ))
    })
}

fn as_utf8_array(array: &ArrayRef) -> Result<ArrayRef> {
    if array.data_type() == &DataType::Utf8 {
        return Ok(array.clone());
    }
    cast(array.as_ref(), &DataType::Utf8).map_err(|error| {
        datafusion::common::DataFusionError::Execution(format!(
            "str_to_map: failed to cast delimiter/text to Utf8: {error}"
        ))
    })
}

fn utf8_row(array: &ArrayRef, row: usize) -> Result<Option<&str>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let utf8 = array
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(
                "str_to_map expected a Utf8 array after coerce_types".to_string(),
            )
        })?;
    Ok(Some(utf8.value(row)))
}

impl ScalarUDFImpl for SparkStrToMapRegex {
    crate::shim_udf_boilerplate!("str_to_map");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        datafusion::common::internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), map_utf8_type(), nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 3 {
            return exec_err!(
                "'str_to_map' expects 1 to 3 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(vec![DataType::Utf8; arg_types.len()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let raw: Vec<ArrayRef> = ColumnarValue::values_to_arrays(&args.args)?;
        let mut arrays = Vec::with_capacity(raw.len());
        for array in raw {
            arrays.push(as_utf8_array(&array)?);
        }
        let result = str_to_map_regex(&arrays)?;
        Ok(ColumnarValue::Array(result))
    }
}

fn str_to_map_regex(args: &[ArrayRef]) -> Result<ArrayRef> {
    let text_array = &args[0];
    let pair_array = args.get(1);
    let kv_array = args.get(2);
    let num_rows = text_array.len();

    let combined_nulls = NullBuffer::union_many([
        text_array.nulls(),
        pair_array.and_then(|array| array.nulls()),
        kv_array.and_then(|array| array.nulls()),
    ]);

    let field_names = MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut map_builder = MapBuilder::new(
        Some(field_names),
        StringBuilder::new(),
        StringBuilder::new(),
    );

    let mut seen_keys = HashSet::new();
    for row in 0..num_rows {
        if combined_nulls
            .as_ref()
            .is_some_and(|nulls| nulls.is_null(row))
        {
            map_builder.append(false)?;
            continue;
        }

        let text = utf8_row(text_array, row)?.unwrap_or("");
        let pair_delim = match pair_array {
            Some(array) => utf8_row(array, row)?.unwrap_or(DEFAULT_PAIR_DELIM),
            None => DEFAULT_PAIR_DELIM,
        };
        let kv_delim = match kv_array {
            Some(array) => utf8_row(array, row)?.unwrap_or(DEFAULT_KV_DELIM),
            None => DEFAULT_KV_DELIM,
        };

        if text.is_empty() {
            map_builder.keys().append_value("");
            map_builder.values().append_null();
            map_builder.append(true)?;
            continue;
        }

        let pair_regex = compile_delim(pair_delim)?;
        let kv_regex = compile_delim(kv_delim)?;
        seen_keys.clear();
        for pair in pair_regex.split(text) {
            let mut kv_iter = kv_regex.splitn(pair, 2);
            let key = kv_iter.next().unwrap_or("");
            let value = kv_iter.next();
            if !seen_keys.insert(key) {
                return exec_err!(
                    "Duplicate map key '{key}' was found, please check the input data. \
                    If you want to remove the duplicated keys, you can set \
                    spark.sql.mapKeyDedupPolicy to \"LAST_WIN\" so that the key \
                    inserted at last takes precedence."
                );
            }
            map_builder.keys().append_value(key);
            match value {
                Some(item) => map_builder.values().append_value(item),
                None => map_builder.values().append_null(),
            }
        }
        map_builder.append(true)?;
    }

    Ok(Arc::new(map_builder.finish()))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Array;
    use datafusion::prelude::SessionContext;

    use super::str_to_map_udf;

    fn ctx() -> SessionContext {
        let context = SessionContext::new();
        context.register_udf(str_to_map_udf().as_ref().clone());
        context
    }

    async fn map_row(sql: &str) -> Vec<(String, Option<String>)> {
        let batches = ctx()
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let column = batches[0].column(0);
        let maps = datafusion::arrow::array::as_map_array(column);
        let keys = datafusion::arrow::array::as_string_array(maps.keys());
        let values = datafusion::arrow::array::as_string_array(maps.values());
        let offsets = maps.offsets();
        let start = usize::try_from(*offsets.first().expect("offset 0")).expect("offset");
        let end = usize::try_from(*offsets.get(1).expect("offset 1")).expect("offset");
        (start..end)
            .map(|index| {
                let value = values
                    .is_valid(index)
                    .then(|| values.value(index).to_string());
                (keys.value(index).to_string(), value)
            })
            .collect()
    }

    #[tokio::test]
    async fn default_delimiters_split_comma_colon() {
        let entries = map_row("SELECT str_to_map('a:1,b:2')").await;
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), Some("1".to_string())),
                ("b".to_string(), Some("2".to_string())),
            ]
        );
    }

    #[tokio::test]
    async fn regex_key_value_delim_matches_spark() {
        let entries = map_row("SELECT str_to_map('ax1,bx2', ',', '[x]')").await;
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), Some("1".to_string())),
                ("b".to_string(), Some("2".to_string())),
            ]
        );
    }

    #[tokio::test]
    async fn regex_character_class_pair_delim_matches_spark() {
        let entries = map_row("SELECT str_to_map('a:1,b:2c:3', '[,c]', ':')").await;
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), Some("1".to_string())),
                ("b".to_string(), Some("2".to_string())),
                (String::new(), Some("3".to_string())),
            ]
        );
    }

    #[tokio::test]
    async fn empty_pair_fragment_is_empty_key() {
        let entries = map_row("SELECT str_to_map('a:1,,b:2')").await;
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), Some("1".to_string())),
                (String::new(), None),
                ("b".to_string(), Some("2".to_string())),
            ]
        );
    }

    /// Java `\s` is ASCII-only, so a non-breaking space does not split.
    #[tokio::test]
    async fn backslash_s_is_ascii_only_so_nbsp_does_not_split() {
        let entries = map_row("SELECT str_to_map('a:1 b:2\u{a0}c:3', '\\s', ':')").await;
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), Some("1".to_string())),
                ("b".to_string(), Some("2\u{a0}c:3".to_string())),
            ]
        );
    }

    /// The binding still splits on Java's ASCII whitespace.
    #[tokio::test]
    async fn backslash_s_still_splits_on_ascii_whitespace() {
        let entries = map_row("SELECT str_to_map('a:1\tb:2', '\\s', ':')").await;
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), Some("1".to_string())),
                ("b".to_string(), Some("2".to_string())),
            ]
        );
    }

    /// The splice also works inside a character class.
    #[tokio::test]
    async fn perl_class_inside_a_character_class_is_spliced() {
        let entries = map_row("SELECT str_to_map('a:1,b:2 c:3', '[\\s,]', ':')").await;
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), Some("1".to_string())),
                ("b".to_string(), Some("2".to_string())),
                ("c".to_string(), Some("3".to_string())),
            ]
        );
    }

    #[test]
    fn ascii_binding_rewrites_only_the_perl_classes() {
        use super::bind_ascii_perl_classes as bind;
        assert_eq!(bind(r"\s"), "[[:space:]]");
        assert_eq!(bind(r"\S"), "[[:^space:]]");
        assert_eq!(bind(r"[\d,]"), "[[:digit:],]");
        assert_eq!(bind(r"\w+"), "[[:word:]]+");
        assert_eq!(bind(r"\\s"), r"\\s");
        assert_eq!(bind(r"[,c]"), "[,c]");
        assert_eq!(bind("."), ".");
    }

    #[tokio::test]
    async fn custom_literal_delimiters() {
        let entries = map_row("SELECT str_to_map('a=1;b=2', ';', '=')").await;
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), Some("1".to_string())),
                ("b".to_string(), Some("2".to_string())),
            ]
        );
    }
}
