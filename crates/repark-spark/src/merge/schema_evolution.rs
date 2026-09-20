use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use crate::insert_by_name::{is_word_at, token_span_offsets};

const CLAUSE: [&str; 3] = ["WITH", "SCHEMA", "EVOLUTION"];

pub(crate) fn strip_schema_evolution(sql: &str) -> Option<String> {
    if !might_carry_the_clause(sql) {
        return None;
    }
    let dialect = DatabricksDialect {};
    let spanned = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .ok()?;
    let meaningful: Vec<usize> = spanned
        .iter()
        .enumerate()
        .filter(|(_, item)| !matches!(item.token, Token::Whitespace(_)))
        .map(|(index, _)| index)
        .collect();
    if meaningful.len() < CLAUSE.len() + 1 || !is_word_at(&spanned, meaningful[0], "MERGE") {
        return None;
    }
    for (offset, keyword) in CLAUSE.iter().enumerate() {
        if !is_word_at(&spanned, meaningful[offset + 1], keyword) {
            return None;
        }
    }
    let (start, end) = token_span_offsets(
        sql,
        &spanned[meaningful[1]],
        &spanned[meaningful[CLAUSE.len()]],
    )?;
    Some(format!(
        "{} {}",
        sql[..start].trim_end(),
        sql[end..].trim_start()
    ))
}

fn might_carry_the_clause(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut seen = [false; CLAUSE.len()];
    while index < bytes.len() {
        if bytes[index].is_ascii_alphabetic() || bytes[index] == b'_' {
            let start = index;
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
            {
                index += 1;
            }
            let word = sql[start..index].to_ascii_uppercase();
            for (position, keyword) in CLAUSE.iter().enumerate() {
                if word == *keyword {
                    seen[position] = true;
                }
            }
            if seen.iter().all(|found| *found) {
                return true;
            }
        } else {
            index += 1;
        }
    }
    false
}

#[cfg(test)]
mod tests;
