use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    DataType as SqlDataType, ExactNumberInfo, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, Insert, ObjectName, SetExpr, TableObject, TypedString, UnaryOperator, Value,
};
use iceberg::{NamespaceIdent, TableIdent};
use repark_common::spark_error;
use repark_core::CatalogRegistry;

use super::source_leaves::{is_string_type, source_type_is_reliable};
use super::{column_name, is_bare_null};
use crate::catalog_ops::{name_parts, quoted_table_display};
use crate::write_to_branch::qualify_table_parts;

pub(crate) async fn refuse_unassignable_ltz_values(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
) -> Result<()> {
    if insert.partitioned.is_some() {
        return Ok(());
    }
    let TableObject::TableName(name) = &insert.table else {
        return Ok(());
    };
    let Some(source) = insert.source.as_ref() else {
        return Ok(());
    };
    let values = match source.body.as_ref() {
        SetExpr::Values(values) => Some(values),
        SetExpr::Query(query) => match query.body.as_ref() {
            SetExpr::Values(values) => Some(values),
            _ => None,
        },
        _ => None,
    };
    let Some(values) = values else {
        return Ok(());
    };
    let parts = qualify_table_parts(ctx, name_parts(name));
    if parts.len() < 3 {
        return Ok(());
    }
    let Some(catalog) = catalogs.get(&parts[0]) else {
        return Ok(());
    };
    let Ok(namespace) = NamespaceIdent::from_vec(parts[1..parts.len() - 1].to_vec()) else {
        return Ok(());
    };
    let ident = TableIdent::new(namespace, parts[parts.len() - 1].clone());
    let Ok(table) = catalog.load_table(&ident).await else {
        return Ok(());
    };
    let schema = table.metadata().current_schema();
    let Ok(presented) = repark_iceberg::catalog::uuid_presentation::presented_arrow_schema(schema)
    else {
        return Ok(());
    };
    if !presented
        .fields()
        .iter()
        .any(|field| is_judged_target(field.data_type()))
    {
        return Ok(());
    }
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
    let display = quoted_table_display(&parts);
    for row in &values.rows {
        check_row(
            ctx,
            &presented,
            &display,
            &row.content,
            &insert.columns,
            case_insensitive,
        )
        .await?;
    }
    Ok(())
}

async fn check_row(
    ctx: &SessionContext,
    presented: &ArrowSchema,
    display: &str,
    row: &[Expr],
    columns: &[ObjectName],
    case_insensitive: bool,
) -> Result<()> {
    if columns.is_empty() {
        if row.len() != presented.fields().len() {
            return Ok(());
        }
        for (value, field) in row.iter().zip(presented.fields()) {
            refuse_value(ctx, display, field.name(), field.data_type(), value).await?;
        }
        return Ok(());
    }
    if row.len() != columns.len() {
        return Ok(());
    }
    for (value, column) in row.iter().zip(columns.iter()) {
        let wanted = column_name(column);
        let Some(field) = presented.fields().iter().find(|field| {
            if case_insensitive {
                field.name().eq_ignore_ascii_case(&wanted)
            } else {
                field.name() == &wanted
            }
        }) else {
            continue;
        };
        refuse_value(ctx, display, field.name(), field.data_type(), value).await?;
    }
    Ok(())
}

async fn refuse_value(
    ctx: &SessionContext,
    display: &str,
    column: &str,
    target: &DataType,
    value: &Expr,
) -> Result<()> {
    if is_bare_null(value) || !is_judged_target(target) {
        return Ok(());
    }
    let source = if let Some(data_type) = literal_source_type(value) {
        data_type
    } else {
        let select = nvl_coalesce_text(value).unwrap_or_else(|| value.to_string());
        let Some(probed) = probe_source_type(ctx, &select).await else {
            return Ok(());
        };
        probed
    };
    if !is_microsecond_ltz(target)
        && (is_string_type(&source) || !source_type_is_reliable(value, target))
    {
        return Ok(());
    }
    if let Some(text) = repark_iceberg::write::update_cast::incompatible_store_message(
        display,
        &format!("`{column}`"),
        &source,
        target,
    ) {
        return Err(DataFusionError::Plan(text));
    }
    if is_microsecond_ltz(target) && source.is_numeric() {
        return Err(DataFusionError::Plan(numeric_refusal(
            display, column, &source, target,
        )));
    }
    Ok(())
}

fn cast_target_type(data_type: &SqlDataType) -> Option<DataType> {
    match data_type {
        SqlDataType::Boolean | SqlDataType::Bool => Some(DataType::Boolean),
        SqlDataType::TinyInt(_) => Some(DataType::Int8),
        SqlDataType::SmallInt(_) | SqlDataType::Int2(_) => Some(DataType::Int16),
        SqlDataType::Int(_) | SqlDataType::Int4(_) | SqlDataType::Integer(_) => {
            Some(DataType::Int32)
        }
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) => Some(DataType::Int64),
        SqlDataType::Float(_) | SqlDataType::Float4 | SqlDataType::Float32 | SqlDataType::Real => {
            Some(DataType::Float32)
        }
        SqlDataType::Double(_)
        | SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float64 => Some(DataType::Float64),
        SqlDataType::Numeric(info) | SqlDataType::Dec(info) => match info {
            ExactNumberInfo::PrecisionAndScale(_, _) => Some(decimal_info_type(info)),
            ExactNumberInfo::None | ExactNumberInfo::Precision(_) => None,
        },
        SqlDataType::String(_)
        | SqlDataType::Text
        | SqlDataType::TinyText
        | SqlDataType::MediumText
        | SqlDataType::LongText
        | SqlDataType::Varchar(_)
        | SqlDataType::Nvarchar(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::CharVarying(_) => Some(DataType::Utf8),
        SqlDataType::Date => Some(DataType::Date32),
        _ => None,
    }
}

fn numeric_refusal(display: &str, column: &str, source: &DataType, target: &DataType) -> String {
    let from = crate::spark_type_names::spark_ddl_type_name(source).to_uppercase();
    let to = crate::spark_type_names::spark_ddl_type_name(target).to_uppercase();
    let column = format!("`{column}`");
    spark_error::message(
        spark_error::INCOMPATIBLE_DATA_FOR_TABLE_CANNOT_SAFELY_CAST,
        &[
            ("tableName", display),
            ("columnName", column.as_str()),
            ("fromType", from.as_str()),
            ("toType", to.as_str()),
        ],
    )
}

fn is_judged_target(data_type: &DataType) -> bool {
    is_microsecond_ltz(data_type)
        || data_type.is_numeric()
        || matches!(data_type, DataType::Date32 | DataType::Boolean)
}

fn is_microsecond_ltz(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_))
    )
}

pub(super) fn literal_source_type(value: &Expr) -> Option<DataType> {
    match value {
        Expr::Value(literal) => match &literal.value {
            Value::Number(text, long) => Some(number_text_type(text, *long)),
            Value::SingleQuotedString(_)
            | Value::NationalStringLiteral(_)
            | Value::TripleSingleQuotedString(_) => Some(DataType::Utf8),
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Null => Some(DataType::Null),
            _ => None,
        },
        Expr::Nested(inner) => literal_source_type(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } => {
            let mut peeled = expr.as_ref();
            while let Expr::Nested(inner) = peeled {
                peeled = inner;
            }
            match peeled {
                Expr::Value(literal) => match &literal.value {
                    Value::Number(text, long) => Some(number_text_type(text, *long)),
                    _ => None,
                },
                Expr::TypedString(typed) => decimal_typed_string(typed),
                _ => None,
            }
        }
        Expr::Cast {
            data_type: SqlDataType::Decimal(info),
            ..
        } => Some(decimal_info_type(info)),
        Expr::Cast {
            data_type,
            array: false,
            ..
        } => cast_target_type(data_type),
        Expr::TypedString(typed) => typed_string_source_type(typed),
        _ => None,
    }
}

fn number_text_type(text: &str, long: bool) -> DataType {
    if long {
        DataType::Int64
    } else if text.bytes().all(|byte| byte.is_ascii_digit()) {
        integer_text_type(text)
    } else if text.bytes().any(|byte| byte == b'e' || byte == b'E') {
        DataType::Float64
    } else {
        decimal_text_type(text).unwrap_or(DataType::Float64)
    }
}

fn integer_text_type(text: &str) -> DataType {
    if text.parse::<i32>().is_ok() {
        DataType::Int32
    } else if text.parse::<i64>().is_ok() {
        DataType::Int64
    } else {
        DataType::Decimal128(u8::try_from(text.len()).unwrap_or(38), 0)
    }
}

fn nvl_coalesce_text(value: &Expr) -> Option<String> {
    let mut peeled = value;
    while let Expr::Nested(inner) = peeled {
        peeled = inner;
    }
    let Expr::Function(function) = peeled else {
        return None;
    };
    let name = function.name.to_string();
    if !name.eq_ignore_ascii_case("nvl") && !name.eq_ignore_ascii_case("ifnull") {
        return None;
    }
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let mut operands = list.args.iter().filter_map(|argument| match argument {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(operand)) => Some(operand),
        _ => None,
    });
    let (Some(first), Some(second), None) = (operands.next(), operands.next(), operands.next())
    else {
        return None;
    };
    Some(format!("coalesce({first}, {second})"))
}

fn decimal_info_type(info: &ExactNumberInfo) -> DataType {
    match info {
        ExactNumberInfo::None => DataType::Decimal128(10, 0),
        ExactNumberInfo::Precision(precision) => {
            DataType::Decimal128(u8::try_from(*precision).unwrap_or(38), 0)
        }
        ExactNumberInfo::PrecisionAndScale(precision, scale) => DataType::Decimal128(
            u8::try_from(*precision).unwrap_or(38),
            i8::try_from(*scale).unwrap_or(0),
        ),
    }
}

fn typed_string_source_type(typed: &TypedString) -> Option<DataType> {
    if matches!(typed.data_type, SqlDataType::Date) {
        return Some(DataType::Date32);
    }
    decimal_typed_string(typed)
}

fn decimal_typed_string(typed: &TypedString) -> Option<DataType> {
    let SqlDataType::Decimal(info) = &typed.data_type else {
        return None;
    };
    match info {
        ExactNumberInfo::None => match &typed.value.value {
            Value::SingleQuotedString(text)
            | Value::NationalStringLiteral(text)
            | Value::TripleSingleQuotedString(text) => decimal_text_type(text),
            _ => None,
        },
        _ => Some(decimal_info_type(info)),
    }
}

fn decimal_text_type(text: &str) -> Option<DataType> {
    let (head, tail) = match text.split_once('.') {
        Some((head, tail)) => (head, tail),
        None => (text, ""),
    };
    if head.is_empty() && tail.is_empty() {
        return None;
    }
    if !head.bytes().all(|byte| byte.is_ascii_digit())
        || !tail.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let stripped = head.trim_start_matches('0');
    let precision = (stripped.len() + tail.len()).max(tail.len()).max(1);
    Some(DataType::Decimal128(
        u8::try_from(precision).unwrap_or(38),
        i8::try_from(tail.len()).unwrap_or(0),
    ))
}

async fn probe_source_type(ctx: &SessionContext, select: &str) -> Option<DataType> {
    let frame = ctx.sql(&format!("SELECT {select} AS probe")).await.ok()?;
    frame
        .schema()
        .fields()
        .first()
        .map(|field| field.data_type().clone())
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::ast::Statement;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::*;

    fn values_row(sql: &str) -> Vec<Expr> {
        let mut statements = Parser::parse_sql(&GenericDialect, sql).unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let SetExpr::Values(values) = source.body.as_ref() else {
            panic!("want a VALUES source");
        };
        values.rows[0].content.clone()
    }

    fn typed(row: &[Expr]) -> Vec<Option<DataType>> {
        row.iter().map(literal_source_type).collect()
    }

    #[test]
    fn bare_signed_and_long_integers_read_as_int_and_bigint() {
        let row = values_row("INSERT INTO t VALUES (1, -1, +2, (3), 1L)");
        assert_eq!(
            typed(&row),
            vec![
                Some(DataType::Int32),
                Some(DataType::Int32),
                Some(DataType::Int32),
                Some(DataType::Int32),
                Some(DataType::Int64),
            ],
            "{row:?}"
        );
    }

    #[test]
    fn fractional_numbers_strings_booleans_and_nulls_read_as_their_kind() {
        let row = values_row("INSERT INTO t VALUES (1.5, 's', true, NULL)");
        assert_eq!(
            typed(&row),
            vec![
                Some(DataType::Decimal128(2, 1)),
                Some(DataType::Utf8),
                Some(DataType::Boolean),
                Some(DataType::Null),
            ],
            "{row:?}"
        );
    }

    #[test]
    fn leading_zero_fractions_count_precision_from_significant_digits() {
        let row = values_row("INSERT INTO t VALUES (0.05)");
        assert_eq!(
            typed(&row),
            vec![Some(DataType::Decimal128(2, 2))],
            "{row:?}"
        );
    }

    #[test]
    fn large_integers_and_exponent_floats_read_as_bigint_and_double() {
        let row = values_row("INSERT INTO t VALUES (12345678901, 1e3, 2147483647)");
        assert_eq!(
            typed(&row),
            vec![
                Some(DataType::Int64),
                Some(DataType::Float64),
                Some(DataType::Int32),
            ],
            "{row:?}"
        );
    }

    #[test]
    fn casts_typed_strings_and_functions_defer_to_the_probe() {
        let row = values_row(
            "INSERT INTO t VALUES (CAST(1 AS TIMESTAMP), TIMESTAMP '2024-01-04 10:00:00', abs(-1))",
        );
        assert_eq!(typed(&row), vec![None, None, None], "{row:?}");
    }

    #[test]
    fn date_typed_strings_and_plain_casts_read_without_a_probe() {
        let row = values_row(
            "INSERT INTO t VALUES (DATE '2024-01-04', CAST('1.5' AS DOUBLE), CAST('x' AS INT), \
             CAST('x' AS BOOLEAN), CAST('x' AS DATE), CAST('x' AS STRING))",
        );
        assert_eq!(
            typed(&row),
            vec![
                Some(DataType::Date32),
                Some(DataType::Float64),
                Some(DataType::Int32),
                Some(DataType::Boolean),
                Some(DataType::Date32),
                Some(DataType::Utf8),
            ],
            "{row:?}"
        );
    }

    #[test]
    fn numbers_dates_booleans_and_zoned_timestamps_are_judged() {
        for data_type in [
            DataType::Int8,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Date32,
            DataType::Boolean,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ] {
            assert!(is_judged_target(&data_type), "{data_type:?}");
        }
        for data_type in [
            DataType::Utf8,
            DataType::Binary,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ] {
            assert!(!is_judged_target(&data_type), "{data_type:?}");
        }
    }

    #[test]
    fn decimal_casts_and_typed_strings_read_as_decimal() {
        let row = values_row(
            "INSERT INTO t VALUES (CAST(1 AS DECIMAL(10,2)), DECIMAL '1.5', -DECIMAL '1.5')",
        );
        assert_eq!(
            typed(&row),
            vec![
                Some(DataType::Decimal128(10, 2)),
                Some(DataType::Decimal128(2, 1)),
                Some(DataType::Decimal128(2, 1)),
            ],
            "{row:?}"
        );
    }
}
