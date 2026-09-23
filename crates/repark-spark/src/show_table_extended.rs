use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use crate::catalog_ops::name_parts;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ShowTableExtended {
    pub(crate) scope: Option<Vec<String>>,
    pub(crate) pattern: String,
    pub(crate) has_partition: bool,
}

pub(crate) fn try_parse_show_table_extended(sql: &str) -> Option<Result<ShowTableExtended>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keywords(&[Keyword::SHOW, Keyword::TABLE, Keyword::EXTENDED]) {
        return None;
    }
    let scope = if parser.parse_keyword(Keyword::IN) || parser.parse_keyword(Keyword::FROM) {
        let name = match parser.parse_object_name(false) {
            Ok(name) => name,
            Err(_) => return Some(Err(syntax_error_at(&parser))),
        };
        Some(name_parts(&name))
    } else {
        None
    };
    if !parser.parse_keyword(Keyword::LIKE) {
        return Some(Err(syntax_error_at(&parser)));
    }
    let pattern = match parser.next_token().token {
        Token::SingleQuotedString(pattern) | Token::DoubleQuotedString(pattern) => pattern,
        _ => return Some(Err(syntax_error_at(&parser))),
    };
    let has_partition = if parser.parse_keyword(Keyword::PARTITION) {
        if !consume_parenthesized(&mut parser) {
            return Some(Err(syntax_error_at(&parser)));
        }
        true
    } else {
        false
    };
    if !at_statement_end(&parser) {
        return Some(Err(syntax_error_at(&parser)));
    }
    Some(Ok(ShowTableExtended {
        scope,
        pattern,
        has_partition,
    }))
}

fn consume_parenthesized(parser: &mut Parser) -> bool {
    if !parser.consume_token(&Token::LParen) {
        return false;
    }
    let mut depth = 1_usize;
    while depth > 0 {
        match parser.next_token().token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::EOF => return false,
            _ => {}
        }
    }
    true
}

fn at_statement_end(parser: &Parser) -> bool {
    matches!(parser.peek_token().token, Token::EOF | Token::SemiColon)
}

fn syntax_error_at(parser: &Parser) -> DataFusionError {
    let near = match parser.peek_token().token {
        Token::EOF => "end of input".to_string(),
        token => format!("'{token}'"),
    };
    DataFusionError::Plan(format!(
        "[PARSE_SYNTAX_ERROR] Syntax error at or near {near}. SQLSTATE: 42601"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_scope_like_partition_and_semicolon() {
        let parsed = try_parse_show_table_extended(
            "show table extended from `ice`.`sales` like 'p*' partition (cat = 'a');",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            parsed,
            ShowTableExtended {
                scope: Some(vec!["ice".to_string(), "sales".to_string()]),
                pattern: "p*".to_string(),
                has_partition: true,
            }
        );
    }

    #[test]
    fn parse_accepts_ambient_scope() {
        let parsed = try_parse_show_table_extended("SHOW TABLE EXTENDED LIKE 'pl'")
            .unwrap()
            .unwrap();
        assert_eq!(parsed.scope, None);
        assert_eq!(parsed.pattern, "pl");
        assert!(!parsed.has_partition);
    }

    #[test]
    fn parse_refuses_missing_like_at_the_end() {
        let error = try_parse_show_table_extended("SHOW TABLE EXTENDED IN ice.sales")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. \
             SQLSTATE: 42601"
        );
    }

    #[test]
    fn parse_leaves_near_misses_alone() {
        for sql in [
            "SHOW TABLES IN sales",
            "SHOW TABLES EXTENDED IN sales LIKE '*'",
            "SHOW TBLPROPERTIES sales.pl",
            "SHOW CREATE TABLE sales.pl",
            "SHOW TABLE EXTENSION LIKE 'pl'",
        ] {
            assert!(try_parse_show_table_extended(sql).is_none(), "{sql}");
        }
    }
}
