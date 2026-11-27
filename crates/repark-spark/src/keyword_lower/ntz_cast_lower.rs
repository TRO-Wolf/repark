use datafusion::sql::sqlparser::ast::{CastKind, DataType, Expr, Value, ValueWithSpan};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Span, Token, Tokenizer};
use repark_functions::timestamp_ntz_cast::{TIMESTAMP_NTZ_CAST_NAME, TRY_TIMESTAMP_NTZ_CAST_NAME};
use repark_functions::timestamp_type::typed_literal_resolves_ntz;

use super::{function_call, null_expr};
use crate::spark_rewrites::{byte_offset, line_starts, skip_whitespace};

pub(super) fn lower_fragment_timestamp_ntz_cast(node: &mut Expr, nullable: &str) -> bool {
    if let Expr::Cast {
        kind: CastKind::Cast | CastKind::DoubleColon,
        expr,
        data_type: DataType::TimestampNtz(_),
        array: false,
        format: None,
    } = node
        && matches!(
            expr.as_ref(),
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(_) | Value::DoubleQuotedString(_),
                ..
            })
        )
    {
        let value = std::mem::replace(expr, Box::new(null_expr()));
        *expr = Box::new(function_call(nullable, vec![*value]));
    }
    lower_timestamp_ntz_cast(node)
}

fn is_bare_word(token: Option<&Token>, names: &[&str]) -> bool {
    matches!(token, Some(Token::Word(word))
        if word.quote_style.is_none()
            && names.iter().any(|name| word.value.eq_ignore_ascii_case(name)))
}

pub(super) fn bare_timestamp_types_as_ntz(sql: &str) -> Option<String> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
        .tokenize_with_location()
        .ok()?;
    let starts = line_starts(sql);
    let mut cast_parens = Vec::new();
    let mut previous = None;
    let mut cuts = Vec::new();
    for (index, with_span) in tokens.iter().enumerate() {
        let token = &with_span.token;
        match token {
            Token::LParen => cast_parens.push(is_bare_word(previous, &["CAST", "TRY_CAST"])),
            Token::RParen => {
                cast_parens.pop();
            }
            _ if is_bare_word(Some(token), &["TIMESTAMP"]) => {
                let next = tokens
                    .get(skip_whitespace(&tokens, index + 1))
                    .map(|next| &next.token);
                let typed_literal = matches!(next,
                    Some(Token::SingleQuotedString(text) | Token::DoubleQuotedString(text))
                        if typed_literal_resolves_ntz(text));
                let cast_target = matches!(previous, Some(Token::DoubleColon))
                    || (is_bare_word(previous, &["AS"])
                        && cast_parens.last() == Some(&true)
                        && matches!(next, Some(Token::RParen)));
                if typed_literal || cast_target {
                    cuts.push((
                        byte_offset(&starts, sql, with_span.span.start)?,
                        byte_offset(&starts, sql, with_span.span.end)?,
                    ));
                }
            }
            _ => {}
        }
        if !matches!(token, Token::Whitespace(_)) {
            previous = Some(token);
        }
    }
    if cuts.is_empty() {
        return None;
    }
    let mut rewritten = String::with_capacity(sql.len() + 4 * cuts.len());
    let mut kept = 0;
    for (start, end) in cuts {
        rewritten.push_str(sql.get(kept..start)?);
        rewritten.push_str("TIMESTAMP_NTZ");
        kept = end;
    }
    rewritten.push_str(sql.get(kept..)?);
    Some(rewritten)
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

    use super::bare_timestamp_types_as_ntz;
    use crate::keyword_lower::{lower_fragment_timestamp_ntz_casts, lower_spark_keywords};

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

    fn fragment_lowered(sql: &str) -> String {
        let mut statement = Parser::parse_sql(&DatabricksDialect {}, sql)
            .unwrap()
            .remove(0);
        lower_fragment_timestamp_ntz_casts(&mut statement);
        statement.to_string()
    }

    #[test]
    fn fragment_ntz_casts_lower_and_string_operands_stay_nullable() {
        let text = fragment_lowered(
            "SELECT CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ) AS a, s::timestamp_ntz AS b, \
             TRY_CAST('x' AS TIMESTAMP_NTZ) AS c, CAST(TIMESTAMP '2024-01-01 02:00:00' AS TIMESTAMP_NTZ) AS d",
        );
        assert!(
            text.contains(
                "__repark_cast_timestamp_ntz__(__repark_decimal_cast_nullable__('2024-01-01 02:00:00')) AS a"
            ) && text.contains("__repark_cast_timestamp_ntz__(s) AS b")
                && text.contains("__repark_try_cast_timestamp_ntz__('x') AS c")
                && text.contains("__repark_cast_timestamp_ntz__(TIMESTAMP '2024-01-01 02:00:00') AS d"),
            "{text}"
        );
        let untouched = "SELECT CAST(s AS TIMESTAMP) AS a, CAST(s AS TIMESTAMP_LTZ) AS b, \
                         CAST(s AS ARRAY<TIMESTAMP_NTZ>) AS c";
        assert_eq!(fragment_lowered(untouched), untouched);
    }

    #[test]
    fn bare_timestamp_types_become_ntz_where_they_name_a_type() {
        assert_eq!(
            bare_timestamp_types_as_ntz(
                "TIMESTAMP '2024-01-01 02:00:00' + INTERVAL 1 HOUR > cast(s AS timestamp)"
            )
            .as_deref(),
            Some("TIMESTAMP_NTZ '2024-01-01 02:00:00' + INTERVAL 1 HOUR > cast(s AS TIMESTAMP_NTZ)")
        );
        assert_eq!(
            bare_timestamp_types_as_ntz("TRY_CAST(CAST(s AS STRING) AS TIMESTAMP), s::TIMESTAMP")
                .as_deref(),
            Some("TRY_CAST(CAST(s AS STRING) AS TIMESTAMP_NTZ), s::TIMESTAMP_NTZ")
        );
        for kept in [
            "TIMESTAMP '2024-01-01 02:00:00Z'",
            "TIMESTAMP '2024-01-01 02:00:00 America/New_York'",
            "TIMESTAMP '2024-01-01T02:00:00+05:30'",
            "TIMESTAMP '02:00:00'",
            "TIMESTAMP 'not a timestamp'",
            "TIMESTAMP_LTZ '2024-01-01 02:00:00'",
            "CAST(s AS TIMESTAMP_LTZ)",
            "(SELECT s AS timestamp)",
            "s AS timestamp",
            "`timestamp` = 'TIMESTAMP ''2024-01-01'''",
            "CAST(s AS ARRAY<TIMESTAMP>)",
        ] {
            assert_eq!(bare_timestamp_types_as_ntz(kept), None, "{kept}");
        }
    }

    #[test]
    fn respelling_cuts_by_character_across_non_ascii_lines_tabs_and_crlf() {
        for (sql, respelled) in [
            (
                "concat('é', CAST(CAST(s AS TIMESTAMP) AS STRING))",
                "concat('é', CAST(CAST(s AS TIMESTAMP_NTZ) AS STRING))",
            ),
            (
                "IF(concat('日本', '🙂') = 'x', NULL, TIMESTAMP '2024-03-10 02:30:00')",
                "IF(concat('日本', '🙂') = 'x', NULL, TIMESTAMP_NTZ '2024-03-10 02:30:00')",
            ),
            (
                "IF(`café` = 'é', TIMESTAMP '2024-03-10 02:30:00', café::timestamp)",
                "IF(`café` = 'é', TIMESTAMP_NTZ '2024-03-10 02:30:00', café::TIMESTAMP_NTZ)",
            ),
            (
                "COALESCE(\n\n    CAST('x' AS TIMESTAMP),\n '🙂'::timestamp)",
                "COALESCE(\n\n    CAST('x' AS TIMESTAMP_NTZ),\n '🙂'::TIMESTAMP_NTZ)",
            ),
            (
                "IF(s\tIS NOT NULL,\tTIMESTAMP\t'2024-03-10 02:30:00',\tNULL)",
                "IF(s\tIS NOT NULL,\tTIMESTAMP_NTZ\t'2024-03-10 02:30:00',\tNULL)",
            ),
            (
                "IF('日本' = '日本',\r\n  CAST(s AS TIMESTAMP),\r\n  TIMESTAMP '2024-03-10 02:30:00')",
                "IF('日本' = '日本',\r\n  CAST(s AS TIMESTAMP_NTZ),\r\n  TIMESTAMP_NTZ '2024-03-10 02:30:00')",
            ),
        ] {
            assert_eq!(
                bare_timestamp_types_as_ntz(sql).as_deref(),
                Some(respelled),
                "{sql}"
            );
        }
    }
}
