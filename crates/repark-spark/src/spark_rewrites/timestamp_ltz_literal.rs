use std::borrow::Cow;

use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

use super::{byte_offset, line_starts, skip_whitespace};
use crate::spark_literals::{LiteralRegion, SparkLexDialect};

pub(crate) fn plan_timestamp_ltz_literal_regions(tokens: &[TokenWithSpan]) -> Vec<LiteralRegion> {
    let mut regions = Vec::new();
    for (index, with_span) in tokens.iter().enumerate() {
        let Token::Word(word) = &with_span.token else {
            continue;
        };
        if word.quote_style.is_some() || !word.value.eq_ignore_ascii_case("TIMESTAMP_LTZ") {
            continue;
        }
        let next = skip_whitespace(tokens, index + 1);
        if matches!(
            tokens.get(next).map(|candidate| &candidate.token),
            Some(Token::SingleQuotedString(_) | Token::DoubleQuotedString(_))
        ) {
            regions.push(LiteralRegion {
                start: with_span.span.start,
                end: with_span.span.end,
                replacement: "TIMESTAMP".to_string(),
            });
        }
    }
    regions
}

pub(crate) fn rewrite_fragment_timestamp_ltz_literals(sql: &str) -> Cow<'_, str> {
    if !sql.to_ascii_lowercase().contains("timestamp_ltz") {
        return Cow::Borrowed(sql);
    }
    let Ok(tokens) = Tokenizer::new(&SparkLexDialect(GenericDialect {}), sql)
        .with_unescape(false)
        .tokenize_with_location()
    else {
        return Cow::Borrowed(sql);
    };
    let starts = line_starts(sql);
    let mut replacements: Vec<(usize, usize, usize)> = Vec::new();
    for (index, with_span) in tokens.iter().enumerate() {
        let Token::Word(word) = &with_span.token else {
            continue;
        };
        if word.quote_style.is_some() || !word.value.eq_ignore_ascii_case("TIMESTAMP_LTZ") {
            continue;
        }
        let next = skip_whitespace(&tokens, index + 1);
        let Some(candidate) = tokens.get(next) else {
            continue;
        };
        if !matches!(
            candidate.token,
            Token::SingleQuotedString(_) | Token::DoubleQuotedString(_)
        ) {
            continue;
        }
        let Some(start) = byte_offset(&starts, sql, with_span.span.start) else {
            continue;
        };
        let Some(lit_start) = byte_offset(&starts, sql, candidate.span.start) else {
            continue;
        };
        let Some(lit_end) = byte_offset(&starts, sql, candidate.span.end) else {
            continue;
        };
        if start <= lit_start && lit_start <= lit_end && lit_end <= sql.len() {
            replacements.push((start, lit_start, lit_end));
        }
    }
    if replacements.is_empty() {
        return Cow::Borrowed(sql);
    }
    let mut out = String::with_capacity(sql.len() + replacements.len() * 16);
    let mut cursor = 0;
    for (start, lit_start, lit_end) in replacements {
        if start < cursor {
            continue;
        }
        out.push_str(&sql[cursor..start]);
        out.push_str("CAST(");
        out.push_str(&sql[lit_start..lit_end]);
        out.push_str(" AS TIMESTAMP_LTZ)");
        cursor = lit_end;
    }
    out.push_str(&sql[cursor..]);
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::tokenizer::Tokenizer;

    use super::*;

    fn replaced(sql: &str) -> Vec<String> {
        let tokens = Tokenizer::new(&GenericDialect {}, sql)
            .tokenize_with_location()
            .unwrap();
        plan_timestamp_ltz_literal_regions(&tokens)
            .into_iter()
            .map(|region| {
                format!(
                    "{}:{}-{}:{}={}",
                    region.start.line,
                    region.start.column,
                    region.end.line,
                    region.end.column,
                    region.replacement
                )
            })
            .collect()
    }

    #[test]
    fn a_typed_ltz_literal_becomes_a_timestamp_literal() {
        assert_eq!(
            replaced("SELECT TIMESTAMP_LTZ '2024-01-01 00:00:00'"),
            vec!["1:8-1:21=TIMESTAMP"]
        );
        assert_eq!(
            replaced("SELECT timestamp_ltz'2024-01-01'"),
            vec!["1:8-1:21=TIMESTAMP"]
        );
    }

    #[test]
    fn a_cast_target_or_a_quoted_name_is_left_alone() {
        assert!(replaced("SELECT CAST('2024-01-01' AS TIMESTAMP_LTZ)").is_empty());
        assert!(replaced("SELECT `timestamp_ltz` FROM t").is_empty());
        assert!(replaced("SELECT timestamp_ltz FROM t WHERE x = 'a'").is_empty());
    }

    #[test]
    fn fragment_ltz_literal_rewrite_preserves_explicit_ltz() {
        assert_eq!(
            rewrite_fragment_timestamp_ltz_literals("TIMESTAMP_LTZ '2024-01-01 02:00:00'"),
            "CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ)"
        );
        assert_eq!(
            rewrite_fragment_timestamp_ltz_literals("timestamp_ltz\"2024-01-01 02:00:00\""),
            "CAST(\"2024-01-01 02:00:00\" AS TIMESTAMP_LTZ)"
        );
        assert_eq!(
            rewrite_fragment_timestamp_ltz_literals("CAST('2024-01-01' AS TIMESTAMP_LTZ)"),
            "CAST('2024-01-01' AS TIMESTAMP_LTZ)"
        );
        assert_eq!(
            rewrite_fragment_timestamp_ltz_literals("`timestamp_ltz` = 'a'"),
            "`timestamp_ltz` = 'a'"
        );
    }
}
