use std::fmt::Write;

use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::Expr;

use super::{
    NARROW_TIMESTAMP_NS_UDF_NAME, NARROWED_BESIDE_NULL_UDF_NAME, NARROWED_BESIDE_VALUE_UDF_NAME,
    TIMESTAMP_TO_DATE_UDF_NAME, WRITTEN_CAST_UDF_NAME,
};

const GENERATED_PREFIX: &str = "__repark_";
const NS_CAST_UDF_NAME: &str = "__repark_cast_timestamp_ns__";
const NS_ZONED_CAST_UDF_NAME: &str = "__repark_cast_timestamptz_ns__";
const TRANSPARENT_UDF_NAMES: [&str; 4] = [
    NARROWED_BESIDE_NULL_UDF_NAME,
    NARROWED_BESIDE_VALUE_UDF_NAME,
    "__repark_spark_nonnull__",
    "__repark_hof_array_field__",
];
const INDEXING_UDF_NAMES: [&str; 2] = ["array_element", "__repark_array_get__"];

pub(super) fn sql_text(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => sql_text(&alias.expr),
        Expr::Column(column) | Expr::OuterReferenceColumn(_, column) => column_text(column),
        Expr::LambdaVariable(variable) => variable.name.clone(),
        Expr::Literal(value, _) => literal_text(value),
        Expr::Cast(cast) if marked(&cast.expr) => sql_text(&cast.expr),
        Expr::Cast(cast) => cast_text(&cast.expr, cast.field.data_type(), "CAST"),
        Expr::TryCast(cast) => cast_text(&cast.expr, cast.field.data_type(), "TRY_CAST"),
        Expr::ScalarFunction(function) => {
            let name = function.func.name();
            let first = || function.args.first().map(sql_text).unwrap_or_default();
            if TRANSPARENT_UDF_NAMES.contains(&name) {
                return first();
            }
            if INDEXING_UDF_NAMES.contains(&name) && function.args.len() == 2 {
                return format!("{}[{}]", first(), sql_text(&function.args[1]));
            }
            match name {
                NARROW_TIMESTAMP_NS_UDF_NAME
                    if function.args.first().is_some_and(marked)
                        || !function.func.aliases().is_empty() =>
                {
                    first()
                }
                NARROW_TIMESTAMP_NS_UDF_NAME | WRITTEN_CAST_UDF_NAME => {
                    format!("CAST({} AS TIMESTAMP)", first())
                }
                NS_CAST_UDF_NAME => format!("CAST({} AS timestamp_ns)", first()),
                NS_ZONED_CAST_UDF_NAME => format!("CAST({} AS timestamptz_ns)", first()),
                TIMESTAMP_TO_DATE_UDF_NAME => format!("CAST({} AS DATE)", first()),
                _ => call_text(name, &function.args),
            }
        }
        Expr::HigherOrderFunction(function) => call_text(function.func.name(), &function.args),
        Expr::AggregateFunction(aggregate) => {
            call_text(aggregate.func.name(), &aggregate.params.args)
        }
        Expr::Lambda(lambda) => {
            format!(
                "({}) -> {}",
                lambda.params.join(", "),
                sql_text(&lambda.body)
            )
        }
        Expr::BinaryExpr(binary) => format!(
            "{} {} {}",
            sql_text(&binary.left),
            binary.op,
            sql_text(&binary.right)
        ),
        Expr::Negative(value) => format!("-{}", sql_text(value)),
        Expr::Case(case) => {
            let mut text = String::from("CASE");
            if let Some(operand) = &case.expr {
                let _ = write!(text, " {}", sql_text(operand));
            }
            for (when, then) in &case.when_then_expr {
                let _ = write!(text, " WHEN {} THEN {}", sql_text(when), sql_text(then));
            }
            if let Some(otherwise) = &case.else_expr {
                let _ = write!(text, " ELSE {}", sql_text(otherwise));
            }
            text.push_str(" END");
            text
        }
        Expr::IsNull(value) => format!("{} IS NULL", sql_text(value)),
        Expr::IsNotNull(value) => format!("{} IS NOT NULL", sql_text(value)),
        other => other.human_display().to_string(),
    }
}

fn column_text(column: &Column) -> String {
    let generated = (column.relation.as_ref())
        .is_some_and(|relation| relation.table().starts_with(GENERATED_PREFIX));
    if generated {
        column.name.clone()
    } else {
        column.flat_name()
    }
}

fn marked(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(function) if matches!(
        function.func.name(),
        NARROWED_BESIDE_NULL_UDF_NAME | NARROWED_BESIDE_VALUE_UDF_NAME
    ))
}

fn call_text(name: &str, arguments: &[Expr]) -> String {
    let arguments: Vec<String> = arguments.iter().map(sql_text).collect();
    format!("{name}({})", arguments.join(", "))
}

fn cast_text(value: &Expr, target: &DataType, keyword: &str) -> String {
    if matches!(value, Expr::Literal(literal, _) if literal.is_null()) {
        return match target {
            DataType::Timestamp(TimeUnit::Nanosecond, None) => "NULL".to_string(),
            _ => format!("{keyword}(NULL AS {})", type_text(target)),
        };
    }
    let inner = sql_text(value);
    if inner == "NULL" && matches!(target, DataType::Timestamp(_, Some(_))) {
        return inner;
    }
    format!("{keyword}({inner} AS {})", type_text(target))
}

fn type_text(data_type: &DataType) -> String {
    match data_type {
        DataType::Timestamp(TimeUnit::Nanosecond, None) => "timestamp_ns".to_string(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => "timestamptz_ns".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP_NTZ".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BINARY".to_string(),
        DataType::Decimal128(precision, scale) => format!("DECIMAL({precision}, {scale})"),
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => {
            format!("ARRAY<{}>", type_text(element.data_type()))
        }
        DataType::Struct(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|field| format!("{}: {}", field.name(), type_text(field.data_type())))
                .collect();
            format!("STRUCT<{}>", fields.join(", "))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => format!(
                "MAP<{}, {}>",
                type_text(fields[0].data_type()),
                type_text(fields[1].data_type())
            ),
            _ => "MAP".to_string(),
        },
        _ => "the type it has".to_string(),
    }
}

fn literal_text(value: &ScalarValue) -> String {
    if value.is_null() {
        return "NULL".to_string();
    }
    match value {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => format!("'{}'", text.replace('\'', "''")),
        ScalarValue::IntervalMonthDayNano(Some(interval)) => {
            interval_text(interval.months, interval.days, interval.nanoseconds)
        }
        ScalarValue::IntervalDayTime(Some(interval)) => interval_text(
            0,
            interval.days,
            i64::from(interval.milliseconds) * 1_000_000,
        ),
        ScalarValue::IntervalYearMonth(Some(months)) => interval_text(*months, 0, 0),
        ScalarValue::TimestampMicrosecond(_, zone) | ScalarValue::TimestampNanosecond(_, zone) => {
            let keyword = if zone.is_some() {
                "TIMESTAMP"
            } else {
                "TIMESTAMP_NTZ"
            };
            format!("{keyword} '{}'", timestamp_text(value))
        }
        ScalarValue::Date32(_) => format!("DATE '{value}'"),
        other => match other.data_type() {
            DataType::Interval(IntervalUnit::MonthDayNano) => "an interval".to_string(),
            _ => other.to_string(),
        },
    }
}

fn timestamp_text(value: &ScalarValue) -> String {
    let shown = (value.to_array().ok())
        .and_then(|array| array_value_to_string(array.as_ref(), 0).ok())
        .unwrap_or_default();
    match shown.strip_suffix('Z') {
        Some(instant) => format!("{}+00:00", instant.replace('T', " ")),
        None => shown.replace('T', " "),
    }
}

fn interval_text(months: i32, days: i32, nanoseconds: i64) -> String {
    let mut parts = Vec::new();
    if months != 0 {
        parts.push(format!("INTERVAL '{months}' MONTH"));
    }
    if days != 0 {
        parts.push(format!("INTERVAL '{days}' DAY"));
    }
    if nanoseconds != 0 || parts.is_empty() {
        let whole = nanoseconds / 1_000_000_000;
        let fraction = (nanoseconds % 1_000_000_000).unsigned_abs();
        let seconds = if fraction == 0 {
            whole.to_string()
        } else {
            let digits = format!("{fraction:09}");
            format!("{whole}.{}", digits.trim_end_matches('0'))
        };
        parts.push(format!("INTERVAL '{seconds}' SECOND"));
    }
    parts.join(" + ")
}
