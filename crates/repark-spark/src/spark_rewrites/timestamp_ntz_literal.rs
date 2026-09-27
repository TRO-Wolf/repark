use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan};
use repark_functions::timestamp_ntz_cast::{TIMESTAMP_NTZ_LITERAL_NAME, parse_timestamp_ntz_wall};

use super::{byte_offset, line_starts, skip_whitespace};
use crate::spark_literals::LiteralRegion;

pub(crate) fn plan_timestamp_ntz_literal_regions(
    tokens: &[TokenWithSpan],
    sql: &str,
) -> Result<Vec<LiteralRegion>> {
    let mut regions = Vec::new();
    for (index, with_span) in tokens.iter().enumerate() {
        let Token::Word(word) = &with_span.token else {
            continue;
        };
        if word.quote_style.is_some() || !word.value.eq_ignore_ascii_case("TIMESTAMP_NTZ") {
            continue;
        }
        let next = skip_whitespace(tokens, index + 1);
        let Some(candidate) = tokens.get(next) else {
            continue;
        };
        let (Token::SingleQuotedString(text) | Token::DoubleQuotedString(text)) = &candidate.token
        else {
            continue;
        };
        let end = candidate.span.end;
        match parse_timestamp_ntz_wall(text) {
            Some(wall) => regions.push(LiteralRegion {
                start: with_span.span.start,
                end,
                replacement: format!("{TIMESTAMP_NTZ_LITERAL_NAME}({wall})"),
            }),
            None => {
                return Err(invalid_literal(
                    sql,
                    with_span.span.start,
                    candidate.span.end,
                    text,
                ));
            }
        }
    }
    Ok(regions)
}

fn invalid_literal(sql: &str, word: Location, end: Location, text: &str) -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(invalid_literal_message(
            sql, word, end, text,
        ))),
        None,
    )
}

fn invalid_literal_message(sql: &str, word: Location, end: Location, text: &str) -> String {
    let starts = line_starts(sql);
    let offset = byte_offset(&starts, sql, word).unwrap_or(0);
    let end_offset = byte_offset(&starts, sql, end).unwrap_or(offset);
    let line_start = sql[..offset].rfind('\n').map_or(0, |index| index + 1);
    let line = sql[line_start..].split('\n').next().unwrap_or("");
    let column = sql[line_start..offset].chars().count() + 1;
    let window_start = column.saturating_sub(1).saturating_sub(32);
    let shown = if window_start > 0 {
        let tail: String = line.chars().skip(window_start).collect();
        format!("...{tail}")
    } else {
        line.to_string()
    };
    let caret_pad = if window_start > 0 { 35 } else { column - 1 };
    let width = sql
        .get(offset..end_offset)
        .map_or(0, |span| span.chars().count());
    let caret = format!("{}{}", " ".repeat(caret_pad), "^".repeat(width));
    format!(
        "[INVALID_TYPED_LITERAL] The value of the typed literal \"TIMESTAMP_NTZ\" is invalid: \
         '{text}'. SQLSTATE: 42604\n== SQL (line {line_number}, position {column}) \
         ==\n{shown}\n{caret}",
        line_number = word.line,
    )
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::tokenizer::Tokenizer;

    use super::*;

    fn planned(sql: &str) -> Result<Vec<LiteralRegion>> {
        let tokens = Tokenizer::new(&GenericDialect {}, sql)
            .tokenize_with_location()
            .unwrap();
        plan_timestamp_ntz_literal_regions(&tokens, sql)
    }

    fn replaced(sql: &str) -> Vec<String> {
        planned(sql)
            .unwrap()
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
    fn a_typed_ntz_literal_becomes_a_wall_literal_call() {
        assert_eq!(
            replaced("SELECT TIMESTAMP_NTZ '2024-01-01 12:00:00'"),
            vec!["1:8-1:43=__repark_timestamp_ntz__(1704110400000000)"]
        );
        assert_eq!(
            replaced("SELECT timestamp_ntz'2024-01-01'"),
            vec!["1:8-1:33=__repark_timestamp_ntz__(1704067200000000)"]
        );
    }

    #[test]
    fn a_cast_target_a_quoted_name_or_a_bare_word_is_left_alone() {
        assert!(
            planned("SELECT CAST('2024-01-01' AS TIMESTAMP_NTZ)")
                .unwrap()
                .is_empty()
        );
        assert!(planned("SELECT `timestamp_ntz` FROM t").unwrap().is_empty());
        assert!(
            planned("SELECT timestamp_ntz FROM t WHERE x = 'a'")
                .unwrap()
                .is_empty()
        );
        assert!(planned("SELECT TIMESTAMP_NTZ FROM t").unwrap().is_empty());
    }

    #[test]
    fn an_unparsable_literal_is_sparks_parse_error() {
        let failure = planned("SELECT TIMESTAMP_NTZ'x'").err().expect("x refuses");
        let DataFusionError::SQL(error, _) = failure else {
            panic!("a parse error, got {failure}");
        };
        let ParserError::ParserError(message) = *error else {
            panic!("a parser message, got {error}");
        };
        let mut lines = message.lines();
        assert_eq!(
            lines.next().unwrap_or_default(),
            "[INVALID_TYPED_LITERAL] The value of the typed literal \"TIMESTAMP_NTZ\" is invalid: \
             'x'. SQLSTATE: 42604"
        );
        assert_eq!(
            lines.next().unwrap_or_default(),
            "== SQL (line 1, position 8) =="
        );
        assert_eq!(lines.next().unwrap_or_default(), "SELECT TIMESTAMP_NTZ'x'");
        assert_eq!(lines.next().unwrap_or_default(), "       ^^^^^^^^^^^^^^^^");
    }

    #[test]
    fn the_caret_count_is_the_char_length_of_the_literal() {
        let literal = "TIMESTAMP_NTZ'2024-13-45'";
        let failure = planned(&format!("SELECT {literal}"))
            .err()
            .expect("2024-13-45 refuses");
        let DataFusionError::SQL(error, _) = failure else {
            panic!("a parse error, got {failure}");
        };
        let ParserError::ParserError(message) = *error else {
            panic!("a parser message, got {error}");
        };
        let shown: Vec<&str> = message.lines().collect();
        assert_eq!(shown.len(), 4);
        assert_eq!(shown[3], "       ^^^^^^^^^^^^^^^^^^^^^^^^^");
        assert_eq!(
            shown[3].trim_start_matches(' ').chars().count(),
            literal.chars().count(),
            "V-001: the caret count is the CHAR length of the literal"
        );
    }

    #[test]
    fn the_window_and_carets_count_chars_before_non_ascii_sql() {
        let sql = format!("SELECT '{}', TIMESTAMP_NTZ'x'", "é".repeat(30));
        let failure = planned(&sql).err().expect("x refuses");
        let DataFusionError::SQL(error, _) = failure else {
            panic!("a parse error, got {failure}");
        };
        let ParserError::ParserError(message) = *error else {
            panic!("a parser message, got {error}");
        };
        let shown: Vec<&str> = message.lines().collect();
        assert_eq!(shown.len(), 4);
        assert_eq!(
            shown[0],
            "[INVALID_TYPED_LITERAL] The value of the typed literal \"TIMESTAMP_NTZ\" is \
             invalid: 'x'. SQLSTATE: 42604"
        );
        assert_eq!(shown[1], "== SQL (line 1, position 42) ==");
        assert_eq!(
            shown[2],
            format!("...{}', TIMESTAMP_NTZ'x'", "é".repeat(29)),
            "V-007: the window starts at a char boundary"
        );
        assert_eq!(
            shown[3],
            format!("{}^^^^^^^^^^^^^^^^", " ".repeat(35)),
            "V-007: the pad and the caret count are chars"
        );
    }

    #[test]
    fn a_non_ascii_literal_gets_sixteen_carets() {
        let failure = planned("SELECT TIMESTAMP_NTZ'é'").err().expect("é refuses");
        let DataFusionError::SQL(error, _) = failure else {
            panic!("a parse error, got {failure}");
        };
        let ParserError::ParserError(message) = *error else {
            panic!("a parser message, got {error}");
        };
        let shown: Vec<&str> = message.lines().collect();
        assert_eq!(shown.len(), 4);
        assert_eq!(shown[1], "== SQL (line 1, position 8) ==");
        assert_eq!(shown[2], "SELECT TIMESTAMP_NTZ'é'");
        assert_eq!(
            shown[3].trim_start_matches(' ').chars().count(),
            16,
            "V-009: the caret count is the CHAR length of the literal"
        );
    }
}
