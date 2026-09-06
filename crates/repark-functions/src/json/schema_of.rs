use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::reader::{JsonValue, parse_json};

#[must_use]
pub fn schema_of_json_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSchemaOfJson::new()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Inferred {
    Unknown,
    Boolean,
    Long,
    Decimal(u8),
    Double,
    Text,
    Array(Box<Inferred>),
    Struct(Vec<(String, Inferred)>),
}

fn infer(value: &JsonValue<'_>) -> Inferred {
    match value {
        JsonValue::Null => Inferred::Unknown,
        JsonValue::Bool(_) => Inferred::Boolean,
        JsonValue::Number(raw) => infer_number(raw),
        JsonValue::NonFinite(_) => Inferred::Double,
        JsonValue::Text(_) => Inferred::Text,
        JsonValue::Array(items) => {
            let element = items.iter().fold(Inferred::Unknown, |carried, item| {
                merge(carried, infer(item))
            });
            Inferred::Array(Box::new(element))
        }
        JsonValue::Object(entries) => {
            let mut fields: Vec<(String, Inferred)> = Vec::with_capacity(entries.len());
            for (key, item) in entries {
                fields.push((key.to_string(), infer(item)));
            }
            fields.sort_by(|left, right| left.0.cmp(&right.0));
            fields.retain(|(_, kind)| !is_empty_struct(kind));
            Inferred::Struct(fields)
        }
    }
}

fn infer_number(raw: &str) -> Inferred {
    if raw.contains(['.', 'e', 'E']) {
        return Inferred::Double;
    }
    if raw.parse::<i64>().is_ok() {
        return Inferred::Long;
    }
    let digits = raw.chars().filter(char::is_ascii_digit).count();
    match u8::try_from(digits) {
        Ok(precision) if precision <= 38 => Inferred::Decimal(precision),
        _ => Inferred::Double,
    }
}

fn is_empty_struct(found: &Inferred) -> bool {
    matches!(found, Inferred::Struct(fields) if fields.is_empty())
}

fn quote_field_name(name: &str) -> String {
    let plain = !name.is_empty()
        && !name.starts_with(|found: char| found.is_ascii_digit())
        && name
            .chars()
            .all(|found| found.is_ascii_alphanumeric() || found == '_');
    if plain {
        name.to_string()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

fn merge(left: Inferred, right: Inferred) -> Inferred {
    match (left, right) {
        (Inferred::Unknown, other) | (other, Inferred::Unknown) => other,
        (Inferred::Array(one), Inferred::Array(two)) => {
            Inferred::Array(Box::new(merge(*one, *two)))
        }
        (Inferred::Struct(one), Inferred::Struct(two)) => Inferred::Struct(merge_fields(one, two)),
        (Inferred::Long, Inferred::Decimal(precision))
        | (Inferred::Decimal(precision), Inferred::Long) => Inferred::Decimal(precision.max(20)),
        (Inferred::Long | Inferred::Decimal(_) | Inferred::Double, Inferred::Double)
        | (Inferred::Double, Inferred::Long | Inferred::Decimal(_)) => Inferred::Double,
        (one, two) if one == two => one,
        _ => Inferred::Text,
    }
}

fn merge_fields(
    left: Vec<(String, Inferred)>,
    right: Vec<(String, Inferred)>,
) -> Vec<(String, Inferred)> {
    let mut merged = left;
    for (name, found) in right {
        if let Some(slot) = merged.iter_mut().find(|(existing, _)| *existing == name) {
            slot.1 = merge(slot.1.clone(), found);
        } else {
            merged.push((name, found));
        }
    }
    merged.sort_by(|one, two| one.0.cmp(&two.0));
    merged
}

fn render(found: &Inferred) -> String {
    match found {
        Inferred::Unknown | Inferred::Text => "STRING".to_string(),
        Inferred::Boolean => "BOOLEAN".to_string(),
        Inferred::Long => "BIGINT".to_string(),
        Inferred::Decimal(precision) => format!("DECIMAL({precision},0)"),
        Inferred::Double => "DOUBLE".to_string(),
        Inferred::Array(element) => format!("ARRAY<{}>", render(element)),
        Inferred::Struct(fields) => {
            let body = fields
                .iter()
                .map(|(name, kind)| format!("{}: {}", quote_field_name(name), render(kind)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("STRUCT<{body}>")
        }
    }
}

#[derive(Debug)]
struct SparkSchemaOfJson {
    signature: Signature,
}

impl SparkSchemaOfJson {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkSchemaOfJson {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkSchemaOfJson {}

impl Hash for SparkSchemaOfJson {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkSchemaOfJson {
    crate::shim_udf_boilerplate!("schema_of_json");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, false)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [_] => Ok(vec![DataType::Utf8]),
            [_, options] => Ok(vec![DataType::Utf8, options.clone()]),
            _ => exec_err!(
                "'schema_of_json' requires 1 or 2 arguments, got {}",
                arg_types.len()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(first) = arrays.first() else {
            return exec_err!("'schema_of_json' requires 1 or 2 arguments, got 0");
        };
        let documents = first.as_string::<i32>();
        let mut builder = StringBuilder::with_capacity(documents.len(), documents.len() * 16);
        for row in 0..documents.len() {
            if documents.is_null(row) {
                return exec_err!(
                    "'schema_of_json' requires a non-NULL foldable JSON string argument"
                );
            }
            if documents.value(row).trim().is_empty() {
                builder.append_value("STRING");
                continue;
            }
            let Some(value) = parse_json(documents.value(row)) else {
                return exec_err!(
                    "[MALFORMED_RECORD_IN_PARSING] 'schema_of_json' cannot infer a schema from \
                     the malformed JSON document {:?}",
                    documents.value(row)
                );
            };
            builder.append_value(render(&infer(&value)));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
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
    fn schema_of_json_sorts_fields_and_widens_like_spark() {
        assert_eq!(
            shown(r#"SELECT schema_of_json('{"b":1,"a":2}')"#),
            "STRUCT<a: BIGINT, b: BIGINT>"
        );
        assert_eq!(
            shown(r#"SELECT schema_of_json('{"a":[1,2.5]}')"#),
            "STRUCT<a: ARRAY<DOUBLE>>"
        );
        assert_eq!(
            shown(r#"SELECT schema_of_json('{"a":[1,"x"]}')"#),
            "STRUCT<a: ARRAY<STRING>>"
        );
        assert_eq!(
            shown(r#"SELECT schema_of_json('{"a":[1,null]}')"#),
            "STRUCT<a: ARRAY<BIGINT>>"
        );
        assert_eq!(shown("SELECT schema_of_json('[null]')"), "ARRAY<STRING>");
        assert_eq!(shown("SELECT schema_of_json('null')"), "STRING");
        assert_eq!(shown("SELECT schema_of_json('{}')"), "STRUCT<>");
        assert_eq!(
            shown(r#"SELECT schema_of_json('[{"b":1},{"a":2}]')"#),
            "ARRAY<STRUCT<a: BIGINT, b: BIGINT>>"
        );
        assert_eq!(
            shown(r#"SELECT schema_of_json('{"a":123456789012345678901234567890}')"#),
            "STRUCT<a: DECIMAL(30,0)>"
        );
    }

    #[test]
    fn schema_of_json_raises_on_a_malformed_document() {
        let error = run("SELECT schema_of_json('{bad')").expect_err("malformed must raise");
        assert!(
            error.to_string().contains("MALFORMED_RECORD_IN_PARSING"),
            "{error}"
        );
    }
}
