use datafusion::sql::sqlparser::ast::{Expr, SqlOption, StructField, Value};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::Token;

pub const REQUIRED_CHILD_OPTION: &str = "repark_not_null";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Opener {
    Struct,
    Map,
    Other,
}

fn syntax_error_near(token: &Token) -> String {
    format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{token}'. SQLSTATE: 42601")
}

fn previous_keyword(out: &[Token]) -> Option<Keyword> {
    out.iter()
        .rev()
        .find(|token| !matches!(token, Token::Whitespace(_)))
        .and_then(|token| match token {
            Token::Word(word) => Some(word.keyword),
            _ => None,
        })
}

fn next_significant(tokens: &[Token], from: usize) -> Option<(usize, &Token)> {
    tokens
        .iter()
        .enumerate()
        .skip(from)
        .find(|(_, token)| !matches!(token, Token::Whitespace(_)))
}

fn is_keyword(token: &Token, keyword: Keyword) -> bool {
    matches!(token, Token::Word(word) if word.keyword == keyword)
}

fn close(stack: &mut Vec<Opener>, out: &mut Vec<Token>, map_parens: bool) {
    match stack.pop() {
        Some(Opener::Map) if map_parens => out.push(Token::RParen),
        _ => out.push(Token::Gt),
    }
}

fn required_option_tokens() -> [Token; 6] {
    [
        Token::make_keyword("OPTIONS"),
        Token::LParen,
        Token::make_word(REQUIRED_CHILD_OPTION, None),
        Token::Eq,
        Token::make_keyword("TRUE"),
        Token::RParen,
    ]
}

#[expect(
    clippy::missing_errors_doc,
    reason = "round comment ban: the Err is Spark's PARSE_SYNTAX_ERROR text for a hand-written struct-field OPTIONS clause, recorded in the unit ledger"
)]
pub fn rewrite_nested_type_tokens(
    tokens: &[Token],
    map_parens: bool,
) -> std::result::Result<Vec<Token>, String> {
    let mut out = Vec::with_capacity(tokens.len());
    let mut stack: Vec<Opener> = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let token = &tokens[index];
        index += 1;
        match token {
            Token::Lt => {
                let opener = match previous_keyword(&out) {
                    Some(Keyword::STRUCT) => Opener::Struct,
                    Some(Keyword::MAP) => Opener::Map,
                    Some(Keyword::ARRAY) => Opener::Other,
                    _ => {
                        out.push(Token::Lt);
                        continue;
                    }
                };
                stack.push(opener);
                out.push(if opener == Opener::Map && map_parens {
                    Token::LParen
                } else {
                    Token::Lt
                });
            }
            Token::Gt if !stack.is_empty() => close(&mut stack, &mut out, map_parens),
            Token::ShiftRight if !stack.is_empty() => {
                close(&mut stack, &mut out, map_parens);
                close(&mut stack, &mut out, map_parens);
            }
            Token::Word(word)
                if word.keyword == Keyword::OPTIONS && stack.last() == Some(&Opener::Struct) =>
            {
                return Err(syntax_error_near(token));
            }
            Token::Word(word)
                if word.keyword == Keyword::NOT && stack.last() == Some(&Opener::Struct) =>
            {
                match next_significant(tokens, index) {
                    Some((at, next)) if is_keyword(next, Keyword::NULL) => {
                        out.extend(required_option_tokens());
                        index = at + 1;
                    }
                    _ => out.push(token.clone()),
                }
            }
            _ => out.push(token.clone()),
        }
    }
    Ok(out)
}

#[must_use]
pub fn has_nested_type_opener(tokens: &[Token]) -> bool {
    let mut previous: Option<&Token> = None;
    for token in tokens {
        if matches!(token, Token::Whitespace(_)) {
            continue;
        }
        if *token == Token::Lt
            && previous.is_some_and(|word| {
                is_keyword(word, Keyword::STRUCT) || is_keyword(word, Keyword::MAP)
            })
        {
            return true;
        }
        previous = Some(token);
    }
    false
}

#[expect(
    clippy::missing_errors_doc,
    reason = "round comment ban: any struct-field OPTIONS entry the rewrite did not write refuses with Spark's PARSE_SYNTAX_ERROR text"
)]
pub fn struct_field_required(field: &StructField) -> std::result::Result<bool, String> {
    let Some(options) = &field.options else {
        return Ok(false);
    };
    let mut required = false;
    for option in options {
        match option {
            SqlOption::KeyValue {
                key,
                value: Expr::Value(value),
            } if key.value == REQUIRED_CHILD_OPTION
                && matches!(value.value, Value::Boolean(true)) =>
            {
                required = true;
            }
            _ => return Err(syntax_error_near(&Token::make_keyword("OPTIONS"))),
        }
    }
    Ok(required)
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::ast::{DataType, Statement};
    use datafusion::sql::sqlparser::dialect::{GenericDialect, SparkSqlDialect};
    use datafusion::sql::sqlparser::parser::Parser;
    use datafusion::sql::sqlparser::tokenizer::Tokenizer;

    use super::*;

    fn rewritten(sql: &str, map_parens: bool) -> std::result::Result<String, String> {
        let tokens = Tokenizer::new(&GenericDialect {}, sql)
            .tokenize()
            .map_err(|error| error.to_string())?;
        let out = rewrite_nested_type_tokens(&tokens, map_parens)?;
        Ok(out.iter().map(ToString::to_string).collect())
    }

    fn struct_fields(sql: &str) -> Vec<StructField> {
        let statement = Parser::parse_sql(&SparkSqlDialect {}, sql)
            .expect("parses")
            .pop()
            .expect("one statement");
        let Statement::CreateTable(create) = statement else {
            panic!("not a CREATE TABLE");
        };
        match &create.columns[1].data_type {
            DataType::Struct(fields, _) => fields.clone(),
            other => panic!("not a struct: {other}"),
        }
    }

    #[test]
    fn a_struct_child_not_null_becomes_the_required_option() {
        let sql = rewritten(
            "CREATE TABLE t (id INT, s STRUCT<a: INT NOT NULL, b: STRING>)",
            false,
        )
        .expect("rewrites");
        assert_eq!(
            sql,
            "CREATE TABLE t (id INT, s STRUCT<a: INT OPTIONS(repark_not_null=TRUE), b: STRING>)"
        );
        let fields = struct_fields(&sql);
        assert_eq!(struct_field_required(&fields[0]), Ok(true));
        assert_eq!(struct_field_required(&fields[1]), Ok(false));
    }

    #[test]
    fn map_angle_brackets_become_parentheses_only_when_asked() {
        let sql = "CREATE TABLE t (m MAP<STRING, STRUCT<q: INT NOT NULL>>, id INT NOT NULL)";
        assert_eq!(
            rewritten(sql, true).expect("rewrites"),
            "CREATE TABLE t (m MAP(STRING, STRUCT<q: INT OPTIONS(repark_not_null=TRUE)>), id INT NOT NULL)"
        );
        assert_eq!(
            rewritten(sql, false).expect("rewrites"),
            "CREATE TABLE t (m MAP<STRING, STRUCT<q: INT OPTIONS(repark_not_null=TRUE)>>, id INT NOT NULL)"
        );
    }

    #[test]
    fn a_hand_written_struct_field_options_clause_refuses() {
        let refused = rewritten(
            "CREATE TABLE t (s STRUCT<a: INT OPTIONS(repark_not_null = TRUE)>)",
            false,
        )
        .expect_err("OPTIONS is not Spark struct-field syntax");
        assert_eq!(
            refused,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'OPTIONS'. SQLSTATE: 42601"
        );
        let fields = struct_fields("CREATE TABLE t (id INT, s STRUCT<a: INT OPTIONS(x = 1)>)");
        assert!(struct_field_required(&fields[0]).is_err());
    }

    #[test]
    fn comparisons_and_array_children_are_left_alone() {
        let sql = "SELECT a < b, c > d FROM t WHERE x NOT NULL";
        assert_eq!(rewritten(sql, true).expect("rewrites"), sql);
        let array = "CREATE TABLE t (a ARRAY<INT NOT NULL>)";
        assert_eq!(rewritten(array, true).expect("rewrites"), array);
        assert!(!has_nested_type_opener(
            &Tokenizer::new(&GenericDialect {}, sql)
                .tokenize()
                .expect("lexes")
        ));
    }
}
