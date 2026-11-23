use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan, Word};
use repark_functions::generator::GENERATOR_ALIAS_UDF;

use super::matching_paren;
use crate::spark_literals::LiteralRegion;

const LIST_ENDS: &[&str] = &[
    "FROM",
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "OFFSET",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "MINUS",
    "WINDOW",
    "QUALIFY",
    "LATERAL",
    "CLUSTER",
    "DISTRIBUTE",
    "SORT",
    "INTO",
];

pub(crate) fn sql_may_have_explode(sql: &str) -> bool {
    sql.as_bytes()
        .windows(b"explode".len())
        .any(|window| window.eq_ignore_ascii_case(b"explode"))
}

fn word(tokens: &[TokenWithSpan], index: usize) -> Option<&Word> {
    match tokens.get(index).map(|with_span| &with_span.token) {
        Some(Token::Word(word)) => Some(word),
        _ => None,
    }
}

fn is_keyword(tokens: &[TokenWithSpan], index: usize, keyword: &str) -> bool {
    word(tokens, index)
        .is_some_and(|word| word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword))
}

fn select_items(tokens: &[TokenWithSpan], start: usize) -> Vec<Vec<usize>> {
    let mut items: Vec<Vec<usize>> = vec![Vec::new()];
    let mut depth = 0usize;
    for index in start..tokens.len() {
        let token = &tokens[index].token;
        match token {
            Token::Whitespace(_) => continue,
            Token::LParen | Token::LBracket => depth += 1,
            Token::RParen | Token::RBracket | Token::SemiColon if depth == 0 => break,
            Token::RParen | Token::RBracket => depth -= 1,
            Token::Comma if depth == 0 => {
                items.push(Vec::new());
                continue;
            }
            Token::Word(_)
                if depth == 0
                    && LIST_ENDS
                        .iter()
                        .any(|keyword| is_keyword(tokens, index, keyword)) =>
            {
                break;
            }
            _ => {}
        }
        if let Some(item) = items.last_mut() {
            item.push(index);
        }
    }
    if let Some(first) = items.first_mut()
        && first.first().is_some_and(|&index| {
            is_keyword(tokens, index, "DISTINCT") || is_keyword(tokens, index, "ALL")
        })
    {
        first.remove(0);
    }
    items
}

fn bare_explode_call(tokens: &[TokenWithSpan], item: &[usize]) -> Option<(usize, usize)> {
    let (&word_at, rest) = item.split_first()?;
    let name = word(tokens, word_at).filter(|word| word.quote_style.is_none())?;
    if !(name.value.eq_ignore_ascii_case("explode")
        || name.value.eq_ignore_ascii_case("explode_outer"))
    {
        return None;
    }
    let &open = rest.first()?;
    let close = matching_paren(tokens, open)?;
    (item.last() == Some(&close)).then_some((word_at, close))
}

fn names_col(tokens: &[TokenWithSpan], item: &[usize]) -> bool {
    let Some((&last, before)) = item.split_last() else {
        return false;
    };
    if !word(tokens, last).is_some_and(|word| word.value.eq_ignore_ascii_case("col")) {
        return false;
    }
    matches!(
        before.last().map(|&index| &tokens[index].token),
        None | Some(
            Token::Period
                | Token::Word(_)
                | Token::RParen
                | Token::Number(..)
                | Token::SingleQuotedString(_)
        )
    )
}

pub(crate) fn plan_bare_explode_regions(tokens: &[TokenWithSpan]) -> Vec<LiteralRegion> {
    let mut regions = Vec::new();
    for index in 0..tokens.len() {
        if !is_keyword(tokens, index, "SELECT") {
            continue;
        }
        let items = select_items(tokens, index + 1);
        let calls: Vec<(usize, usize)> = items
            .iter()
            .filter_map(|item| bare_explode_call(tokens, item))
            .collect();
        let [(word_at, close)] = calls.as_slice() else {
            continue;
        };
        if !items.iter().any(|item| names_col(tokens, item)) {
            continue;
        }
        regions.push(LiteralRegion {
            start: tokens[*word_at].span.start,
            end: tokens[*word_at].span.end,
            replacement: format!("{GENERATOR_ALIAS_UDF}({}", tokens[*word_at].token),
        });
        regions.push(LiteralRegion {
            start: tokens[*close].span.start,
            end: tokens[*close].span.end,
            replacement: "))".to_string(),
        });
    }
    regions
}

#[cfg(test)]
mod tests {
    use crate::spark_literals as literals;

    fn rewritten(sql: &str) -> String {
        literals::canonicalize_verbatim(sql, false)
            .unwrap()
            .into_owned()
    }

    #[test]
    fn a_bare_explode_beside_a_col_sibling_gets_a_plan_name_that_cannot_collide() {
        assert_eq!(
            rewritten("SELECT col, explode(m) FROM t"),
            "SELECT col, __repark_gen_alias(explode(m)) FROM t"
        );
        assert_eq!(
            rewritten("SELECT EXPLODE_OUTER(m), t.COL FROM t"),
            "SELECT __repark_gen_alias(EXPLODE_OUTER(m)), t.COL FROM t"
        );
        assert_eq!(
            rewritten("SELECT DISTINCT id AS col, explode(map(1, 2)) FROM t"),
            "SELECT DISTINCT id AS col, __repark_gen_alias(explode(map(1, 2))) FROM t"
        );
        assert_eq!(
            rewritten("SELECT * FROM (SELECT tag col, explode(m) FROM t) AS g"),
            "SELECT * FROM (SELECT tag col, __repark_gen_alias(explode(m)) FROM t) AS g"
        );
    }

    #[test]
    fn other_select_lists_stay_as_written() {
        for sql in [
            "SELECT id, explode(m) FROM t",
            "SELECT col, explode(m) AS x FROM t",
            "SELECT col, posexplode(m) FROM t",
            "SELECT col + 1, explode(m) FROM t",
            "SELECT col, explode(m) + 1 FROM t",
            "SELECT col FROM (SELECT explode(arr) FROM t) AS g",
            "SELECT col, `explode`(m) FROM t",
            "SELECT col, explode(a), explode(b) FROM t",
        ] {
            assert_eq!(rewritten(sql), sql);
        }
        assert!(super::sql_may_have_explode("select EXPLODE(m)"));
        assert!(!super::sql_may_have_explode("select inline(m)"));
    }
}
