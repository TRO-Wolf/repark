use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan};

use super::skip_whitespace;
use crate::spark_literals::LiteralRegion;

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
}
