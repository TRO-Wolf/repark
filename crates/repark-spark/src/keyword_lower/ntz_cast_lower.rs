use datafusion::sql::sqlparser::ast::{
    CastKind, DataType, Expr, TimezoneInfo, Value, ValueWithSpan,
};
use datafusion::sql::sqlparser::tokenizer::Span;
use repark_functions::decimal_cast::DECIMAL_CAST_NULLABLE_NAME;
use repark_functions::instant_ts::{
    TIMESTAMP_LTZ_CAST_NAME, TRY_TIMESTAMP_LTZ_CAST_NAME, string_carries_timezone,
};
use repark_functions::timestamp_ntz_cast::{TIMESTAMP_NTZ_CAST_NAME, TRY_TIMESTAMP_NTZ_CAST_NAME};

use super::{function_call, null_expr};

pub(super) fn is_explicit_timestamp_ltz_cast(node: &Expr) -> bool {
    matches!(
        node,
        Expr::Cast {
            data_type,
            array: false,
            format: None,
            ..
        } if data_type.to_string().eq_ignore_ascii_case("timestamp_ltz")
    )
}

pub(super) fn lower_explicit_timestamp_ltz_cast(node: &mut Expr) -> bool {
    if let Expr::Cast {
        kind,
        expr,
        data_type,
        array: false,
        format: None,
    } = node
        && data_type.to_string().eq_ignore_ascii_case("timestamp_ltz")
    {
        let name = match kind {
            CastKind::Cast | CastKind::DoubleColon => TIMESTAMP_LTZ_CAST_NAME,
            CastKind::TryCast | CastKind::SafeCast => TRY_TIMESTAMP_LTZ_CAST_NAME,
        };
        let value = std::mem::replace(expr, Box::new(null_expr()));
        *node = function_call(name, vec![*value]);
        return true;
    }
    false
}

pub(super) fn lower_timestamp_ntz_cast(node: &mut Expr) -> bool {
    if let Expr::Cast {
        kind,
        expr,
        data_type,
        array: false,
        format: None,
    } = node
        && matches!(data_type, DataType::TimestampNtz(_))
    {
        let name = match kind {
            CastKind::Cast | CastKind::DoubleColon => TIMESTAMP_NTZ_CAST_NAME,
            CastKind::TryCast | CastKind::SafeCast => TRY_TIMESTAMP_NTZ_CAST_NAME,
        };
        let value = std::mem::replace(expr, Box::new(null_expr()));
        *node = function_call(name, vec![*value]);
        return true;
    }
    if let Expr::TypedString(typed) = node
        && matches!(typed.data_type, DataType::TimestampNtz(_))
    {
        let Some(text) = typed_string_text(&typed.value.value) else {
            return false;
        };
        *node = function_call(
            TIMESTAMP_NTZ_CAST_NAME,
            vec![Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(text),
                span: Span::empty(),
            })],
        );
        return true;
    }
    false
}

pub(super) fn lower_fragment_timestamp_ltz_cast(node: &mut Expr) -> bool {
    if lower_explicit_timestamp_ltz_cast(node) {
        return true;
    }
    if let Expr::Cast {
        kind: CastKind::Cast | CastKind::DoubleColon,
        expr,
        data_type,
        array: false,
        format: None,
    } = node
        && matches!(data_type, DataType::Timestamp(None, TimezoneInfo::None))
        && matches!(
            expr.as_ref(),
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(_) | Value::DoubleQuotedString(_),
                ..
            })
        )
    {
        let value = std::mem::replace(expr, Box::new(null_expr()));
        **expr = function_call(DECIMAL_CAST_NULLABLE_NAME, vec![*value]);
        return true;
    }
    if let Expr::TypedString(typed) = node {
        let explicit_ltz = typed
            .data_type
            .to_string()
            .eq_ignore_ascii_case("timestamp_ltz");
        let bare_timestamp = matches!(
            typed.data_type,
            DataType::Timestamp(None, TimezoneInfo::None)
        );
        if (explicit_ltz || bare_timestamp)
            && let Some(text) = typed_string_text(&typed.value.value)
            && (explicit_ltz || string_carries_timezone(&text))
        {
            *node = function_call(
                TIMESTAMP_LTZ_CAST_NAME,
                vec![Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(text),
                    span: Span::empty(),
                })],
            );
            return true;
        }
    }
    false
}

fn typed_string_text(value: &Value) -> Option<String> {
    match value {
        Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => Some(text.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::ast::Statement;
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use crate::keyword_lower::lower_spark_keywords;

    fn lowered(sql: &str) -> Statement {
        let mut statement = Parser::parse_sql(&DatabricksDialect {}, sql)
            .unwrap()
            .remove(0);
        lower_spark_keywords(&mut statement);
        statement
    }

    #[test]
    fn ntz_casts_lower_to_the_embedded_cast_calls() {
        let text = lowered(
            "SELECT CAST('2024-01-01' AS TIMESTAMP_NTZ) AS a, '2024-01-01'::timestamp_ntz AS b, \
             TRY_CAST('x' AS TIMESTAMP_NTZ) AS c",
        )
        .to_string();
        assert!(
            text.contains("__repark_cast_timestamp_ntz__('2024-01-01')")
                && text.contains("__repark_try_cast_timestamp_ntz__('x')"),
            "{text}"
        );
        let escaped = lowered("SELECT TIMESTAMP_NTZ '2024-01-01 00:00:00' AS v").to_string();
        assert!(
            escaped.contains("__repark_cast_timestamp_ntz__('2024-01-01 00:00:00')"),
            "{escaped}"
        );
        let nested = "SELECT CAST(a AS ARRAY<TIMESTAMP_NTZ>) AS v";
        assert_eq!(lowered(nested).to_string(), nested);
    }

    #[test]
    fn fragment_timestamp_casts_lower_ntz_ltz_and_zoned_typed_strings() {
        let mut statement = Parser::parse_sql(
            &DatabricksDialect {},
            "SELECT CAST(TIMESTAMP '2024-01-01 02:00:00' AS TIMESTAMP_NTZ) AS a, \
             TRY_CAST(ts AS TIMESTAMP_NTZ) AS b, \
             CAST(s AS TIMESTAMP_LTZ) AS c, \
             TRY_CAST(s AS TIMESTAMP_LTZ) AS d, \
             TIMESTAMP '2024-01-01 02:00:00Z' AS e, \
             TIMESTAMP '2024-01-01 02:00:00' AS f, \
             CAST('2024-01-01 02:00:00Z' AS TIMESTAMP) AS g",
        )
        .unwrap()
        .remove(0);
        crate::keyword_lower::lower_fragment_timestamp_casts(&mut statement);
        let text = statement.to_string();
        assert!(
            text.contains("__repark_cast_timestamp_ntz__(TIMESTAMP '2024-01-01 02:00:00')"),
            "{text}"
        );
        assert!(
            text.contains("__repark_try_cast_timestamp_ntz__(ts)"),
            "{text}"
        );
        assert!(text.contains("__repark_cast_timestamp_ltz__(s)"), "{text}");
        assert!(
            text.contains("__repark_try_cast_timestamp_ltz__(s)"),
            "{text}"
        );
        assert!(
            text.contains("__repark_cast_timestamp_ltz__('2024-01-01 02:00:00Z')"),
            "{text}"
        );
        assert!(
            text.contains("TIMESTAMP '2024-01-01 02:00:00' AS f"),
            "{text}"
        );
        assert!(
            text.contains(
                "CAST(__repark_decimal_cast_nullable__('2024-01-01 02:00:00Z') AS TIMESTAMP) AS g"
            ),
            "{text}"
        );
    }
}
