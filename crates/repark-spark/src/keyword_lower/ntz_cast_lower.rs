use datafusion::sql::sqlparser::ast::{CastKind, DataType, Expr, Value, ValueWithSpan};
use datafusion::sql::sqlparser::tokenizer::Span;
use repark_functions::timestamp_ntz_cast::{TIMESTAMP_NTZ_CAST_NAME, TRY_TIMESTAMP_NTZ_CAST_NAME};

use super::{function_call, null_expr};

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
}
