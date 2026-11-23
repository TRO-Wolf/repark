use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan};

use super::skip_whitespace;
use crate::spark_literals::LiteralRegion;

const DATE_PART_SPELLINGS: &[&str] = &[
    "Y",
    "YR",
    "YRS",
    "YEAROFWEEK",
    "QTR",
    "MON",
    "MONS",
    "W",
    "D",
    "DAYOFWEEK",
    "DOW",
    "DAYOFWEEK_ISO",
    "DOW_ISO",
    "H",
    "HR",
    "HRS",
    "M",
    "MIN",
    "MINS",
    "S",
    "SEC",
    "SECS",
];

pub(crate) fn sql_may_have_extract(sql: &str) -> bool {
    sql.as_bytes()
        .windows(b"extract".len())
        .any(|window| window.eq_ignore_ascii_case(b"extract"))
}

fn bare_word<'a>(tokens: &'a [TokenWithSpan], index: usize) -> Option<&'a str> {
    match tokens.get(index).map(|with_span| &with_span.token) {
        Some(Token::Word(word)) if word.quote_style.is_none() => Some(word.value.as_str()),
        _ => None,
    }
}

pub(crate) fn plan_extract_field_regions(tokens: &[TokenWithSpan]) -> Vec<LiteralRegion> {
    let mut regions = Vec::new();
    for (index, with_span) in tokens.iter().enumerate() {
        if !bare_word(tokens, index).is_some_and(|word| word.eq_ignore_ascii_case("EXTRACT")) {
            continue;
        }
        let open = skip_whitespace(tokens, index + 1);
        if !matches!(
            tokens.get(open).map(|with_span| &with_span.token),
            Some(Token::LParen)
        ) {
            continue;
        }
        let field_at = skip_whitespace(tokens, open + 1);
        let Some(field) = bare_word(tokens, field_at).filter(|field| {
            DATE_PART_SPELLINGS
                .iter()
                .any(|spelling| field.eq_ignore_ascii_case(spelling))
        }) else {
            continue;
        };
        let from_at = skip_whitespace(tokens, field_at + 1);
        if !bare_word(tokens, from_at).is_some_and(|word| word.eq_ignore_ascii_case("FROM")) {
            continue;
        }
        regions.push(LiteralRegion {
            start: with_span.span.start,
            end: tokens[from_at].span.end,
            replacement: format!("date_part('{field}',"),
        });
    }
    regions
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::tokenizer::Tokenizer;

    use super::*;

    fn rewritten(sql: &str) -> String {
        let canonical = crate::spark_literals::canonicalize_verbatim(sql, false).unwrap();
        canonical.into_owned()
    }

    #[test]
    fn a_spark_only_field_becomes_a_date_part_call() {
        assert_eq!(
            rewritten("SELECT extract(YEAROFWEEK FROM d) AS y FROM t"),
            "SELECT date_part('YEAROFWEEK', d) AS y FROM t"
        );
        assert_eq!(
            rewritten("SELECT EXTRACT ( hrs\n FROM ts ), extract(dow_iso FROM d)"),
            "SELECT date_part('hrs', ts ), date_part('dow_iso', d)"
        );
    }

    #[test]
    fn the_fields_the_stock_planner_answers_stay_as_written() {
        for sql in [
            "SELECT extract(YEAR FROM d)",
            "SELECT extract(HOUR FROM ts)",
            "SELECT extract(SECOND FROM ts)",
            "SELECT extract(FOO FROM d)",
            "SELECT regexp_extract(s, 'a', 0)",
            "SELECT extract FROM t",
            "SELECT `extract`(H FROM d)",
            "SELECT extract(`H` FROM d)",
        ] {
            assert_eq!(rewritten(sql), sql);
        }
    }

    #[test]
    fn the_rewrite_keeps_the_other_canonical_regions() {
        assert_eq!(
            rewritten("SELECT extract(DAYOFWEEK FROM TIMESTAMP_LTZ '2020-12-31 00:00:00')"),
            "SELECT date_part('DAYOFWEEK', TIMESTAMP '2020-12-31 00:00:00')"
        );
    }

    #[test]
    fn the_planner_spans_the_extract_word_through_from() {
        let sql = "SELECT extract(W FROM d)";
        let tokens = Tokenizer::new(&GenericDialect {}, sql)
            .tokenize_with_location()
            .unwrap();
        let regions = plan_extract_field_regions(&tokens);
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].replacement, "date_part('W',");
        assert_eq!((regions[0].start.column, regions[0].end.column), (8, 22));
        assert!(sql_may_have_extract("select EXTRACT(h from d)"));
        assert!(!sql_may_have_extract("select hour(d)"));
    }
}
