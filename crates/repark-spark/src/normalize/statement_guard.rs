use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

const UNCLOSED_BRACKETED_COMMENT_MESSAGE: &str = "[UNCLOSED_BRACKETED_COMMENT] Found an unclosed bracketed comment. \
     Please, append */ at the end of the comment. SQLSTATE: 42601";

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

pub(crate) fn refuse_unclosed_bracketed_comment(sql: &str) -> Result<()> {
    if !sql.contains("/*") {
        return Ok(());
    }
    let dialect = DatabricksDialect {};
    let Err(error) = Tokenizer::new(&dialect, sql).tokenize() else {
        return Ok(());
    };
    if error.message != "Unexpected EOF while in a multi-line comment" {
        return Ok(());
    }
    let Some(opener) = outermost_unclosed_bracketed_comment(sql) else {
        return Ok(());
    };
    if sql.as_bytes().get(opener + 2) == Some(&b'+') {
        return Ok(());
    }
    Err(unclosed_bracketed_comment_error())
}

pub(crate) fn unclosed_bracketed_comment_error() -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(
            UNCLOSED_BRACKETED_COMMENT_MESSAGE.to_string(),
        )),
        None,
    )
}

fn outermost_unclosed_bracketed_comment(sql: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut openers = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if !openers.is_empty() {
            if bytes.get(index..index + 2) == Some(b"/*") {
                openers.push(index);
                index += 2;
            } else if bytes.get(index..index + 2) == Some(b"*/") {
                openers.pop();
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        match bytes[index] {
            b'\'' | b'"' | b'`' => {
                index = skip_quoted_text(bytes, index);
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index = skip_line_comment(bytes, index);
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                openers.push(index);
                index += 2;
            }
            _ => {
                index += 1;
            }
        }
    }
    openers.first().copied()
}

fn skip_quoted_text(bytes: &[u8], mut index: usize) -> usize {
    let quote = bytes[index];
    index += 1;
    while index < bytes.len() {
        if bytes[index] == b'\\' && index + 1 < bytes.len() {
            index += 2;
        } else if bytes[index] == quote && bytes.get(index + 1) == Some(&quote) {
            index += 2;
        } else if bytes[index] == quote {
            return index + 1;
        } else {
            index += 1;
        }
    }
    index
}

fn skip_line_comment(bytes: &[u8], mut index: usize) -> usize {
    index += 2;
    while index < bytes.len() {
        if matches!(bytes[index], b'\n' | b'\r') {
            return index + 1;
        }
        index += 1;
    }
    index
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
