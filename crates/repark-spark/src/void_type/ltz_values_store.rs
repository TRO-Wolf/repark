use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    DataType as SqlDataType, ExactNumberInfo, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, Insert, ObjectName, Query, SetExpr, TableObject, TypedString, UnaryOperator,
    Value,
};
use iceberg::{NamespaceIdent, TableIdent};
use repark_common::spark_error;
use repark_core::CatalogRegistry;

use super::source_leaves::{is_string_type, source_type_is_reliable};
use super::{column_name, is_null_or_default_cell};
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
        return refuse_select_arms(ctx, catalogs, insert, source).await;
    };
    let Some((presented, display)) = load_presented(ctx, catalogs, name).await else {
        return Ok(());
    };
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
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

async fn load_presented(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    name: &ObjectName,
) -> Option<(ArrowSchema, String)> {
    let parts = qualify_table_parts(ctx, name_parts(name));
    if parts.len() < 3 {
        return None;
    }
    let presented = load_table_schema(catalogs, &parts).await?;
    if !presented
        .fields()
        .iter()
        .any(|field| is_judged_target(field.data_type()))
    {
        return None;
    }
    Some((presented, quoted_table_display(&parts)))
}

pub(super) async fn load_table_schema(
    catalogs: &CatalogRegistry,
    parts: &[String],
) -> Option<ArrowSchema> {
    let catalog = catalogs.get(&parts[0])?;
    let namespace = NamespaceIdent::from_vec(parts[1..parts.len() - 1].to_vec()).ok()?;
    let ident = TableIdent::new(namespace, parts[parts.len() - 1].clone());
    let table = catalog.load_table(&ident).await.ok()?;
    let schema = table.metadata().current_schema();
    repark_iceberg::catalog::uuid_presentation::presented_arrow_schema(schema).ok()
}

async fn refuse_select_arms(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    source: &Query,
) -> Result<()> {
    let TableObject::TableName(name) = &insert.table else {
        return Ok(());
    };
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
    let arms = super::select_values_arms::resolve_insert_arms(source, case_insensitive);
    if arms
        .iter()
        .all(|arm| arm.positions.iter().all(Vec::is_empty))
    {
        return Ok(());
    }
    let Some((presented, display)) = load_presented(ctx, catalogs, name).await else {
        return Ok(());
    };
    let mut siblings =
        super::sibling_types::SiblingJudge::new(ctx, catalogs, &arms, case_insensitive);
    for arm in &arms {
        for rows in super::select_values_arms::arm_row_groups(arm) {
            for row in rows {
                let mut projected = Vec::with_capacity(arm.positions.len());
                for (position, cells) in arm.positions.iter().enumerate() {
                    let cell = cells
                        .iter()
                        .find(|cell| cell.rows.as_ptr() == rows.as_ptr())
                        .and_then(|cell| row.content.get(cell.column));
                    let value = cell
                        .cloned()
                        .unwrap_or_else(super::select_values_arms::null_cell);
                    if siblings.skip_string(position, &value).await {
                        projected.push(super::select_values_arms::null_cell());
                    } else {
                        projected.push(value);
                    }
                }
                check_row(
                    ctx,
                    &presented,
                    &display,
                    &projected,
                    &insert.columns,
                    case_insensitive,
                )
                .await?;
            }
        }
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
    if is_null_or_default_cell(value) || !is_judged_target(target) {
        return Ok(());
    }
    let source = if let Some(data_type) = literal_source_type(value) {
        data_type
    } else {
        let select = nvl_coalesce_text(value).unwrap_or_else(|| super::probe_text(value));
        let Some(probed) = probe_source_type(ctx, &select).await? else {
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
            while let Expr::Nested(inner)
            | Expr::UnaryOp {
                op: UnaryOperator::Plus | UnaryOperator::Minus,
                expr: inner,
            } = peeled
            {
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

pub(crate) fn number_text_type(text: &str, long: bool) -> DataType {
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

pub(super) fn nvl_coalesce_text(value: &Expr) -> Option<String> {
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
    Some(format!(
        "coalesce({}, {})",
        super::probe_text(first),
        super::probe_text(second)
    ))
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

pub(super) async fn probe_source_type(
    ctx: &SessionContext,
    select: &str,
) -> Result<Option<DataType>> {
    let frame = ctx.sql(&format!("SELECT {select} AS probe")).await?;
    Ok(frame
        .schema()
        .fields()
        .first()
        .map(|field| field.data_type().clone()))
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::ast::{SelectItem, Statement};
    use datafusion::sql::sqlparser::dialect::{DatabricksDialect, Dialect, GenericDialect};
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
    fn stacked_unary_signs_read_as_their_numeric_kind() {
        let row = values_row("INSERT INTO t VALUES (- -1, - - -1, - -(1), - -1.5, +-1, -+1)");
        assert_eq!(
            typed(&row),
            vec![
                Some(DataType::Int32),
                Some(DataType::Int32),
                Some(DataType::Int32),
                Some(DataType::Decimal128(2, 1)),
                Some(DataType::Int32),
                Some(DataType::Int32),
            ],
            "{row:?}"
        );
    }

    #[test]
    fn signed_nulls_defer_to_the_probe() {
        let row = values_row("INSERT INTO t VALUES (-NULL, - -NULL)");
        assert_eq!(typed(&row), vec![None, None], "{row:?}");
    }

    #[test]
    fn stacked_minus_probes_render_without_comment_runs() {
        let row = values_row(
            "INSERT INTO t VALUES (- -1, - - -1, -1, +-1, (- -1), +- -1, +(- -1), -(- -1), - \
             -1 + 0, abs(- -1), CAST(- -1 AS INT))",
        );
        let rendered: Vec<String> = row
            .iter()
            .map(|value| crate::void_type::parenthesize_stacked_minus(value).to_string())
            .collect();
        assert_eq!(
            rendered,
            vec![
                "-(-1)",
                "-(-(-1))",
                "-1",
                "+(-1)",
                "(-(-1))",
                "+(-(-1))",
                "+(-(-1))",
                "-(-(-1))",
                "-(-1) + 0",
                "abs(-(-1))",
                "CAST(-(-1) AS INT)",
            ],
            "{row:?}"
        );
        for text in &rendered {
            assert!(!text.contains("--"), "{text}");
        }
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

    #[tokio::test]
    async fn probe_that_cannot_parse_refuses_instead_of_passing() {
        let ctx = SessionContext::new();
        let error = probe_source_type(&ctx, "-- nothing but a comment")
            .await
            .expect_err("an unparsable probe must refuse");
        assert!(error.to_string().contains("ParserError"), "{error}");
    }

    #[tokio::test]
    async fn ntz_probe_that_cannot_parse_refuses_instead_of_passing() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        let ctx = SessionContext::new();
        let fields = [
            std::sync::Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            )),
            std::sync::Arc::new(NestedField::optional(
                2,
                "n",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
        ];
        let row = [
            values_row("INSERT INTO t VALUES (1)").swap_remove(0),
            Expr::Identifier(datafusion::sql::sqlparser::ast::Ident::new(")")),
        ];
        let error = super::super::check_ntz_row(&ctx, &fields, "`t`", &row, &[], false)
            .await
            .expect_err("an unparsable NTZ probe must refuse");
        assert!(error.to_string().contains("ParserError"), "{error}");
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

    #[test]
    fn probe_strings_holding_a_quote_render_dollar_quoted() {
        let row = values_row("INSERT INTO t VALUES ('it''s', 'a\\\\''b', 'plain')");
        let rendered: Vec<String> = row.iter().map(crate::void_type::probe_text).collect();
        assert_eq!(
            rendered,
            vec!["$p$it's$p$", "$p$a\\\\'b$p$", "'plain'"],
            "{row:?}"
        );
    }

    #[test]
    fn probe_strings_without_a_quote_render_unchanged() {
        let row = values_row("INSERT INTO t VALUES ('C:\\\\temp\\\\', 'a\\nb')");
        let rendered: Vec<String> = row.iter().map(crate::void_type::probe_text).collect();
        assert_eq!(rendered, vec!["'C:\\\\temp\\\\'", "'a\\nb'"], "{row:?}");
    }

    #[test]
    fn probe_string_tag_grows_past_a_tag_collision() {
        let row = values_row("INSERT INTO t VALUES ('$p$x''y')");
        let rendered: Vec<String> = row.iter().map(crate::void_type::probe_text).collect();
        assert_eq!(rendered, vec!["$pp$$p$x'y$pp$"], "{row:?}");
    }

    #[test]
    fn probe_string_tag_grows_past_a_closer_the_value_ends_into() {
        let row = values_row("INSERT INTO t VALUES ('it''s$p', '$p$x''y$pp', '''$p')");
        let rendered: Vec<String> = row.iter().map(crate::void_type::probe_text).collect();
        assert_eq!(
            rendered,
            vec!["$pp$it's$p$pp$", "$ppp$$p$x'y$pp$ppp$", "$pp$'$p$pp$"],
            "{row:?}"
        );
    }

    #[test]
    fn probe_strings_parse_back_to_the_same_value_on_both_probe_dialects() {
        let row = values_row(
            "INSERT INTO t VALUES ('it''s', 'a\\\\''b', 'plain', 'C:\\\\temp\\\\', '$p$x''y', 'it''s$p', \
             '$p$x''y$pp', '''$p', '-- x\\\\''y')",
        );
        let generic = GenericDialect;
        let databricks = DatabricksDialect {};
        let dialects: [&dyn Dialect; 2] = [&generic, &databricks];
        for cell in &row {
            let Expr::Value(literal) = cell else {
                panic!("want a literal cell, got {cell:?}");
            };
            let Value::SingleQuotedString(want) = &literal.value else {
                panic!("want a single-quoted cell, got {cell:?}");
            };
            let rendered = crate::void_type::probe_text(cell);
            for dialect in dialects {
                let mut statements =
                    Parser::parse_sql(dialect, &format!("SELECT {rendered}")).unwrap();
                let Statement::Query(query) = statements.swap_remove(0) else {
                    panic!("want a SELECT probe, got {rendered}");
                };
                let SetExpr::Select(select) = query.body.as_ref() else {
                    panic!("want a SELECT body, got {rendered}");
                };
                let SelectItem::UnnamedExpr(Expr::Value(got)) = &select.projection[0] else {
                    panic!("want one literal column, got {rendered}");
                };
                match &got.value {
                    Value::SingleQuotedString(got) => assert_eq!(got, want, "{rendered}"),
                    Value::DollarQuotedString(got) => assert_eq!(&got.value, want, "{rendered}"),
                    other => panic!("want a string literal back, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn nvl_probe_rewrite_dollar_quotes_quoted_operands() {
        let row = values_row("INSERT INTO t VALUES (nvl(NULL, replace('a\\\\''b', '\\\\''', '')))");
        assert_eq!(
            nvl_coalesce_text(&row[0]).unwrap(),
            "coalesce(NULL, replace($p$a\\\\'b$p$, $p$\\\\'$p$, ''))",
            "{row:?}"
        );
    }

    #[tokio::test]
    async fn ltz_door_passes_a_timestamp_cast_over_a_backslash_quote_replace() {
        let ctx = SessionContext::new();
        let row = values_row(
            "INSERT INTO t VALUES (CAST(replace('2024-01-02 03:04:05\\\\''', '\\\\''', '') AS \
             TIMESTAMP))",
        );
        let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        refuse_value(&ctx, "`t`", "c", &target, &row[0])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ltz_door_names_a_backslash_quote_string_cell_as_string() {
        let ctx = SessionContext::new();
        let row = values_row("INSERT INTO t VALUES (replace('a\\\\''b', '\\\\''', ''))");
        let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let error = refuse_value(&ctx, "`t`", "c", &target, &row[0])
            .await
            .unwrap_err();
        let text = error.to_string();
        assert!(
            text.contains("CANNOT_SAFELY_CAST")
                && text.contains("\"STRING\"")
                && text.contains("\"TIMESTAMP\""),
            "{text}"
        );
    }
}
