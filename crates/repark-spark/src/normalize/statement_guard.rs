use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

pub(crate) fn refuse_multi_statement_sql(sql: &str) -> Result<()> {
    let refusal =
        || crate::show_create::multi_statement_refusal_error(sql, multi_statement_parse_error());
    let dialect = DatabricksDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return Ok(());
    };
    match Parser::new(&dialect)
        .with_tokens(tokens.clone())
        .parse_statements()
    {
        Ok(statements) if statements.len() > 1 => Err(refusal()),
        Ok(_) => Ok(()),
        Err(_) => {
            if tokens_have_nontrailing_content_after_semicolon(&tokens) {
                Err(refusal())
            } else {
                Ok(())
            }
        }
    }
}

pub(crate) fn tokens_have_nontrailing_content_after_semicolon(tokens: &[Token]) -> bool {
    let mut saw_semicolon = false;
    for token in tokens {
        match token {
            Token::EOF => break,
            Token::Whitespace(_) => {}
            Token::SemiColon => {
                saw_semicolon = true;
            }
            _ if saw_semicolon => return true,
            _ => {}
        }
    }
    false
}

pub(crate) fn multi_statement_parse_error() -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(
            "[PARSE_SYNTAX_ERROR] Syntax error: multiple SQL statements in one call are not \
             supported (Spark parity). Only a single statement is accepted; a trailing \
             semicolon, whitespace, or comment after that statement is allowed. SQLSTATE: 42601"
                .to_string(),
        )),
        None,
    )
}
