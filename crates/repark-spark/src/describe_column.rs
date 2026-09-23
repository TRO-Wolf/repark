use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;

use crate::catalog_ops::name_parts;
use crate::namespace_ddl::consume_word;

pub(crate) fn parse_describe_column_tail(
    parser: &mut Parser,
) -> Option<Result<Option<Vec<String>>>> {
    match parser.peek_token().token {
        Token::EOF | Token::SemiColon => Some(Ok(None)),
        _ => {
            let column = name_parts(&parser.parse_object_name(false).ok()?);
            match column.as_slice() {
                [word]
                    if (word.eq_ignore_ascii_case("VERSION")
                        || word.eq_ignore_ascii_case("TIMESTAMP"))
                        && parser.parse_keyword(Keyword::AS)
                        && consume_word(parser, "OF") =>
                {
                    Some(Err(describe_column_parse_error("OF")))
                }
                [word] if word.eq_ignore_ascii_case("FOR") && consume_word(parser, "VERSION") => {
                    Some(Err(describe_column_parse_error("VERSION")))
                }
                _ if matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) => {
                    Some(Ok(Some(column)))
                }
                _ => None,
            }
        }
    }
}

fn describe_column_parse_error(near: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601"
    ))
}
