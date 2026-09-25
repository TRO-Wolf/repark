use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value,
};

use datafusion::common::utils::datafusion_strsim::levenshtein;
use repark_common::spark_error;

use crate::spark_type_names::spark_ddl_type_name;

pub(super) fn backtick(part: &str) -> String {
    format!("`{}`", part.replace('`', "``"))
}

pub(super) fn quote_if_needed(part: &str) -> String {
    let plain = !part.is_empty()
        && part
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        && !part.chars().all(|character| character.is_ascii_digit());
    if plain {
        part.to_string()
    } else {
        backtick(part)
    }
}

pub(super) fn quoted_column_path(path: &[String]) -> String {
    path.iter()
        .map(|part| quote_if_needed(part))
        .collect::<Vec<_>>()
        .join(".")
}

pub(super) fn backtick_path(path: &[String]) -> String {
    path.iter()
        .map(|part| backtick(part))
        .collect::<Vec<_>>()
        .join(".")
}

pub(super) fn string_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

pub(super) fn scala_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "BooleanType".to_string(),
        DataType::Int8 => "ByteType".to_string(),
        DataType::Int16 => "ShortType".to_string(),
        DataType::Int32 => "IntegerType".to_string(),
        DataType::Int64 => "LongType".to_string(),
        DataType::Float32 => "FloatType".to_string(),
        DataType::Float64 => "DoubleType".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "StringType".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BinaryType".to_string(),
        DataType::Date32 | DataType::Date64 => "DateType".to_string(),
        DataType::Timestamp(_, Some(_)) => "TimestampType".to_string(),
        DataType::Timestamp(_, None) => "TimestampNTZType".to_string(),
        DataType::Decimal128(precision, scale) => format!("DecimalType({precision},{scale})"),
        DataType::Struct(fields) => format!(
            "StructType({})",
            fields
                .iter()
                .map(|field| format!(
                    "StructField({},{},{})",
                    field.name(),
                    scala_type(field.data_type()),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
        DataType::List(element) | DataType::LargeList(element) => format!(
            "ArrayType({},{})",
            scala_type(element.data_type()),
            element.is_nullable()
        ),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(pair) if pair.len() == 2 => format!(
                "MapType({},{},{})",
                scala_type(pair[0].data_type()),
                scala_type(pair[1].data_type()),
                pair[1].is_nullable()
            ),
            other => other.to_string(),
        },
        other => other.to_string(),
    }
}

pub(super) fn pretty_expr(expr: &Expr, qualifiers: &[Vec<String>]) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => {
            let skip = qualifiers
                .iter()
                .filter(|qualifier| {
                    parts.len() > qualifier.len()
                        && qualifier
                            .iter()
                            .zip(parts)
                            .all(|(expected, part)| expected.eq_ignore_ascii_case(&part.value))
                })
                .map(Vec::len)
                .max()
                .unwrap_or(0);
            parts[skip..]
                .iter()
                .map(|part| part.value.clone())
                .collect::<Vec<_>>()
                .join(".")
        }
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => text.clone(),
            other => other.to_string(),
        },
        Expr::Nested(inner) => pretty_expr(inner, qualifiers),
        Expr::BinaryOp { left, op, right } => format!(
            "({} {op} {})",
            pretty_expr(left, qualifiers),
            pretty_expr(right, qualifiers)
        ),
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(list)
                if list.duplicate_treatment.is_none() && list.clauses.is_empty() =>
            {
                let args = list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                            pretty_expr(inner, qualifiers)
                        }
                        other => other.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({args})", function.name)
            }
            _ => expr.to_string(),
        },
        _ => expr.to_string(),
    }
}

pub(super) fn field_not_found(part: &str, fields: &Fields) -> DataFusionError {
    let names = fields
        .iter()
        .map(|field| backtick(field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    DataFusionError::Plan(format!(
        "[FIELD_NOT_FOUND] No such struct field {} in {names}. SQLSTATE: 42704",
        backtick(part)
    ))
}

pub(super) fn unresolved_key(
    parts: &[String],
    fields: &Fields,
    qualifier: Option<&String>,
) -> DataFusionError {
    let written = parts.join(".");
    let mut candidates = fields
        .iter()
        .map(|field| {
            qualifier
                .into_iter()
                .chain(std::iter::once(field.name()))
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|candidate| levenshtein(&candidate.join("."), &written));
    let suggestions = candidates
        .iter()
        .map(|candidate| backtick_path(candidate))
        .collect::<Vec<_>>()
        .join(", ");
    DataFusionError::Plan(spark_error::message(
        spark_error::UNRESOLVED_COLUMN_WITH_SUGGESTION,
        &[
            ("columnName", backtick_path(parts).as_str()),
            ("suggestions", suggestions.as_str()),
        ],
    ))
}

pub(super) fn invalid_extract(base: &str, data_type: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from \"{base}\". Need a \
         complex type [STRUCT, ARRAY, MAP] but got \"{}\". SQLSTATE: 42000",
        spark_ddl_type_name(data_type).to_ascii_uppercase()
    ))
}

pub(super) fn unexpected_index_type(base: &str, part: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{base}[{part}]\" due to data \
         type mismatch: The second parameter requires the \"INTEGRAL\" type, however \"{part}\" \
         has the type \"STRING\". SQLSTATE: 42K09"
    ))
}

pub(super) fn cannot_find_data(path: &[String]) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for the \
         table ``: Cannot find data for the output column {}. SQLSTATE: KD000",
        backtick_path(path)
    ))
}

pub(super) fn extra_struct_fields(extra: &[String], path: &[String]) -> DataFusionError {
    let fields = extra
        .iter()
        .map(|field| backtick(field))
        .collect::<Vec<_>>()
        .join(", ");
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS] Cannot write incompatible data for \
         the table ``: Cannot write extra fields {fields} to the struct {}. SQLSTATE: KD000",
        backtick_path(path)
    ))
}

pub(super) fn row_level_refusal(pretty: &[String], errors: &[String]) -> DataFusionError {
    let assignments = pretty
        .iter()
        .map(|assignment| format!("\"{assignment}\""))
        .collect::<Vec<_>>()
        .join(", ");
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.INVALID_ROW_LEVEL_OPERATION_ASSIGNMENTS] Cannot resolve \
         {assignments} due to data type mismatch: \n- {} SQLSTATE: 42K09",
        errors.join("\n- ")
    ))
}
