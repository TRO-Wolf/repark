use std::borrow::Cow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use super::{LiteralRegion, SparkLexDialect, apply_regions, literal_token_value};

#[allow(clippy::missing_errors_doc)]
pub fn canonicalize_fragment_for_default_parse(
    sql: &str,
    keep_verbatim: bool,
) -> Result<Cow<'_, str>> {
    if !keep_verbatim {
        return Ok(Cow::Borrowed(sql));
    }
    if !sql.as_bytes().contains(&b'\'') && !sql.as_bytes().contains(&b'"') {
        return Ok(Cow::Borrowed(sql));
    }
    let tokens = Tokenizer::new(&SparkLexDialect(GenericDialect {}), sql)
        .with_unescape(false)
        .tokenize_with_location()
        .map_err(|error| DataFusionError::SQL(Box::new(ParserError::from(error)), None))?;
    let mut regions = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let Some(first_value) = literal_token_value(&tokens[index].token, true) else {
            index += 1;
            continue;
        };
        let start = tokens[index].span.start;
        let mut end = tokens[index].span.end;
        let mut merged = first_value;
        let mut cursor = index + 1;
        loop {
            let mut lookahead = cursor;
            while lookahead < tokens.len()
                && matches!(tokens[lookahead].token, Token::Whitespace(_))
            {
                lookahead += 1;
            }
            let Some(next_value) = tokens
                .get(lookahead)
                .and_then(|with_span| literal_token_value(&with_span.token, true))
            else {
                break;
            };
            merged.push_str(&next_value);
            end = tokens[lookahead].span.end;
            cursor = lookahead + 1;
        }
        regions.push(LiteralRegion {
            start,
            end,
            replacement: requote_default(&merged),
        });
        index = cursor;
    }
    if regions.is_empty() {
        return Ok(Cow::Borrowed(sql));
    }
    Ok(Cow::Owned(apply_regions(sql, &regions, false).sql))
}

fn requote_default(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_value(fragment_out: &str) -> String {
        let canonical =
            super::super::canonicalize_verbatim(fragment_out, false).expect("reparse parses");
        let tokens = Tokenizer::new(&GenericDialect {}, canonical.as_ref())
            .tokenize_with_location()
            .expect("canonical tokenizes");
        tokens
            .iter()
            .find_map(|with_span| match &with_span.token {
                Token::SingleQuotedString(raw) => Some(raw.clone()),
                _ => None,
            })
            .expect("one literal")
    }

    #[test]
    fn verbatim_fragments_come_back_default_stable() {
        for (fragment, holddown) in [
            ("'it''s'", "'it''''s'"),
            ("'a\\b'", "'a\\\\b'"),
            ("'k\\\\''m'", "'k\\\\\\\\''''m'"),
            ("\"q\"\"q\"", "'q\"\"q'"),
            ("'plain'", "'plain'"),
        ] {
            let rendered =
                canonicalize_fragment_for_default_parse(fragment, true).expect("fragment renders");
            assert_eq!(rendered.as_ref(), holddown, "{fragment}");
        }
    }

    #[test]
    fn verbatim_values_survive_a_forced_default_reparse() {
        for (fragment, value) in [
            ("'it''s'", "it''s"),
            ("'a\\b'", "a\\b"),
            ("'k\\\\''m'", "k\\\\''m"),
            ("\"q\"\"q\"", "q\"\"q"),
            ("'plain'", "plain"),
        ] {
            let rendered =
                canonicalize_fragment_for_default_parse(fragment, true).expect("fragment renders");
            assert_eq!(default_value(rendered.as_ref()), value, "{fragment}");
        }
    }

    #[test]
    fn default_fragments_come_back_borrowed() {
        for fragment in ["'it''s'", "'a\\\\b'", "s = 'x' AND t = 1", "id = 1"] {
            assert!(
                matches!(
                    canonicalize_fragment_for_default_parse(fragment, false).expect("renders"),
                    Cow::Borrowed(_)
                ),
                "{fragment}"
            );
        }
    }

    #[test]
    fn adjacent_literals_merge_before_requote() {
        let rendered = canonicalize_fragment_for_default_parse("'a' 'b'", true).expect("renders");
        assert_eq!(rendered.as_ref(), "'ab'");
        let rendered =
            canonicalize_fragment_for_default_parse("'a\\\\' 'b'", true).expect("renders");
        assert_eq!(rendered.as_ref(), "'a\\\\\\\\b'");
        assert_eq!(default_value(rendered.as_ref()), "a\\\\b");
    }
}
