use datafusion::sql::sqlparser::ast::{
    AccessExpr, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart,
    Subscript, Value,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

use crate::spark_typed::SPARK_AS_NAME;

pub(crate) fn name_struct_fields_in_sql(sql: &str) -> Option<String> {
    if !sql.to_ascii_lowercase().contains("struct") {
        return None;
    }
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .ok()?;
    let offsets = byte_offsets(sql, &tokens)?;
    let mut inserts = Vec::new();
    for index in 0..tokens.len() {
        if is_struct_call(sql, &tokens, &offsets, index) {
            inserts.extend(struct_call_names(sql, &tokens, &offsets, index).unwrap_or_default());
        }
    }
    if inserts.is_empty() {
        return None;
    }
    inserts.sort_by(|left, right| right.0.cmp(&left.0));
    let mut named = sql.to_string();
    for (at, text) in inserts {
        named.insert_str(at, &text);
    }
    Some(named)
}

fn byte_offsets(sql: &str, tokens: &[TokenWithSpan]) -> Option<Vec<usize>> {
    let mut line_starts = vec![0];
    line_starts.extend(sql.match_indices('\n').map(|(at, _)| at + 1));
    let mut offsets = tokens
        .iter()
        .map(|token| {
            let line = usize::try_from(token.span.start.line)
                .ok()?
                .checked_sub(1)?;
            let column = usize::try_from(token.span.start.column)
                .ok()?
                .checked_sub(1)?;
            let start = *line_starts.get(line)?;
            let rest = sql.get(start..)?;
            rest.char_indices()
                .map(|(at, _)| start + at)
                .chain(std::iter::once(start + rest.len()))
                .nth(column)
        })
        .collect::<Option<Vec<_>>>()?;
    offsets.push(sql.len());
    Some(offsets)
}

fn significant(tokens: &[TokenWithSpan], index: usize) -> bool {
    !matches!(tokens[index].token, Token::Whitespace(_))
}

fn is_struct_call(sql: &str, tokens: &[TokenWithSpan], offsets: &[usize], index: usize) -> bool {
    let Token::Word(word) = &tokens[index].token else {
        return false;
    };
    if word.quote_style.is_some() || !word.value.eq_ignore_ascii_case("struct") {
        return false;
    }
    let spelled = sql
        .get(offsets[index]..offsets[index + 1])
        .is_some_and(|text| text.eq_ignore_ascii_case("struct"));
    let after_dot = (0..index)
        .rev()
        .find(|&at| significant(tokens, at))
        .is_some_and(|at| tokens[at].token == Token::Period);
    let opens = (index + 1..tokens.len())
        .find(|&at| significant(tokens, at))
        .is_some_and(|at| tokens[at].token == Token::LParen);
    spelled && !after_dot && opens
}

struct Argument {
    first: usize,
    last: usize,
    named: bool,
}

fn struct_call_arguments(tokens: &[TokenWithSpan], index: usize) -> Option<Vec<Argument>> {
    let open = (index + 1..tokens.len()).find(|&at| significant(tokens, at))?;
    let mut arguments = Vec::new();
    let mut depth = 0usize;
    let mut current: Option<Argument> = None;
    for at in open + 1..tokens.len() {
        if !significant(tokens, at) {
            continue;
        }
        let token = &tokens[at].token;
        if depth == 0 && matches!(token, Token::Comma | Token::RParen) {
            arguments.push(current.take()?);
            if *token == Token::RParen {
                return Some(arguments);
            }
            continue;
        }
        match token {
            Token::LParen | Token::LBracket | Token::LBrace => depth += 1,
            Token::RParen | Token::RBracket | Token::RBrace => depth = depth.checked_sub(1)?,
            _ => {}
        }
        let as_keyword = matches!(token, Token::Word(word) if word.keyword == Keyword::AS);
        let argument = current.get_or_insert(Argument {
            first: at,
            last: at,
            named: false,
        });
        argument.last = at;
        argument.named |= depth == 0 && as_keyword;
    }
    None
}

fn struct_call_names(
    sql: &str,
    tokens: &[TokenWithSpan],
    offsets: &[usize],
    index: usize,
) -> Option<Vec<(usize, String)>> {
    let arguments = struct_call_arguments(tokens, index)?;
    let wildcard = arguments.iter().any(|argument| {
        tokens[argument.last].token == Token::Mul
            && (argument.first == argument.last || tokens[argument.last - 1].token == Token::Period)
    });
    if wildcard {
        return None;
    }
    let mut names = Vec::new();
    for (position, argument) in arguments.iter().enumerate() {
        if argument.named {
            continue;
        }
        let end = offsets[argument.last + 1];
        let text = sql.get(offsets[argument.first]..end)?;
        let name = parsed_value(text)
            .and_then(|value| spark_field_name(&value))
            .unwrap_or_else(|| format!("col{}", position + 1));
        names.push((end, format!(" AS `{}`", name.replace('`', "``"))));
    }
    Some(names)
}

fn parsed_value(text: &str) -> Option<Expr> {
    let dialect = DatabricksDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(text).ok()?;
    let value = parser.parse_expr().ok()?;
    (parser.peek_token().token == Token::EOF).then_some(value)
}

fn spark_field_name(value: &Expr) -> Option<String> {
    match value {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.clone()),
        Expr::Nested(inner) => spark_field_name(inner),
        Expr::CompoundFieldAccess { access_chain, .. } => match access_chain.last()? {
            AccessExpr::Dot(Expr::Identifier(ident)) => Some(ident.value.clone()),
            AccessExpr::Subscript(Subscript::Index { index }) => string_key(index),
            _ => None,
        },
        Expr::Function(function) => extracted_field(function),
        _ => None,
    }
}

fn extracted_field(function: &Function) -> Option<String> {
    let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
        return None;
    };
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(display)))) = list.args.first()
    else {
        return None;
    };
    let Value::SingleQuotedString(display) = &display.value else {
        return None;
    };
    let (base, path) = display.rsplit_once(").")?;
    let field = path.rsplit('.').next()?.trim_matches('`');
    (name.value == SPARK_AS_NAME
        && base.to_ascii_lowercase().starts_with("named_struct(")
        && !field.is_empty())
    .then(|| field.to_string())
}

fn string_key(index: &Expr) -> Option<String> {
    match index {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => Some(text.clone()),
            _ => None,
        },
        Expr::Identifier(ident) if ident.quote_style == Some('"') => Some(ident.value.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named_sql(sql: &str) -> String {
        name_struct_fields_in_sql(sql).unwrap_or_else(|| sql.to_string())
    }

    #[test]
    fn unnamed_fields_take_sparks_names() {
        assert_eq!(
            named_sql(
                "SELECT struct(1, 'x'), struct(id, t.v, (w), id + 1), struct(1 AS col2, 2) FROM t"
            ),
            "SELECT struct(1 AS `col1`, 'x' AS `col2`), struct(id AS `id`, t.v AS `v`, (w) AS `w`, \
             id + 1 AS `col4`), struct(1 AS col2, 2 AS `col2`) FROM t"
        );
    }

    #[test]
    fn a_subscript_or_field_access_takes_its_string_key_or_field_name() {
        let rows = [
            ("m['a']", "a"),
            ("m[\"a\"]", "a"),
            ("s['x']", "x"),
            ("m['B']", "B"),
            ("(m['a'])", "a"),
            ("map('a', 1)['a']", "a"),
            ("named_struct('x', 1).x", "x"),
            ("n.a['x']", "x"),
            ("ls[0].a", "a"),
            ("n.b.x", "x"),
            ("m.a", "a"),
            ("l[0]", "col1"),
            ("l[1 - 1]", "col1"),
            ("m[NULL]", "col1"),
            ("m[k]", "col1"),
            ("m['a'] + 1", "col1"),
            ("CAST(id AS BIGINT)", "col1"),
            ("__repark_spark_as__('named_struct(x, 1).x', (1))", "x"),
            (
                "__repark_spark_as__('named_struct(a, named_struct(b, 1)).a.b', (1))",
                "b",
            ),
            ("__repark_spark_as__('1L', (1))", "col1"),
        ];
        for (value, name) in rows {
            let sql = named_sql(&format!("SELECT struct({value}) FROM t"));
            assert!(
                sql.ends_with(&format!(" AS `{name}`) FROM t")),
                "struct({value}) should name its field {name}: {sql}"
            );
        }
        assert_eq!(
            named_sql("SELECT struct(m['a'], s['x'], l[0]) FROM t"),
            "SELECT struct(m['a'] AS `a`, s['x'] AS `x`, l[0] AS `col3`) FROM t"
        );
    }

    #[test]
    fn every_statement_kind_is_named_once_at_the_text() {
        let rows = [
            (
                "MERGE INTO t USING s ON struct(t.id) = struct(s.k) \
                 WHEN MATCHED AND STRUCT(t.v) IN (struct(1)) THEN UPDATE SET v = struct(s.w).w",
                "MERGE INTO t USING s ON struct(t.id AS `id`) = struct(s.k AS `k`) \
                 WHEN MATCHED AND STRUCT(t.v AS `v`) IN (struct(1 AS `col1`)) \
                 THEN UPDATE SET v = struct(s.w AS `w`).w",
            ),
            (
                "DELETE FROM t WHERE array(struct(id)) = array(struct(1))",
                "DELETE FROM t WHERE array(struct(id AS `id`)) = array(struct(1 AS `col1`))",
            ),
            (
                "WITH c AS (SELECT struct(struct(id)) AS r FROM t) SELECT * FROM c",
                "WITH c AS (SELECT struct(struct(id AS `id`) AS `col1`) AS r FROM t) SELECT * FROM c",
            ),
            (
                "CREATE TEMPORARY VIEW v AS SELECT struct(id,\n  v -- a note, )\n) FROM t",
                "CREATE TEMPORARY VIEW v AS SELECT struct(id AS `id`,\n  v AS `v` -- a note, )\n) FROM t",
            ),
            (
                "EXPLAIN SELECT (SELECT struct(id) FROM t) = struct(`we``ird`)",
                "EXPLAIN SELECT (SELECT struct(id AS `id`) FROM t) = struct(`we``ird` AS `we``ird`)",
            ),
        ];
        for (sql, named) in rows {
            assert_eq!(named_sql(sql), named, "{sql}");
            assert_eq!(named_sql(named), named, "naming is idempotent: {named}");
        }
    }

    #[test]
    fn other_struct_spellings_are_left_as_written() {
        for sql in [
            "SELECT named_struct('a', 1), struct(*), struct(t.*) FROM t",
            "CREATE TABLE t (s STRUCT<a: INT>, `struct` INT)",
            "SELECT t.struct(1), 'struct(1)', `struct`(1) FROM t",
            "SELECT struct() FROM t",
        ] {
            assert_eq!(name_struct_fields_in_sql(sql), None, "{sql}");
        }
    }
}
