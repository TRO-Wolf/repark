use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{Ident, ObjectName, ObjectType, Statement};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use repark_common::spark_error;
use repark_core::{CatalogRegistry, TempViewSession, spark_numeric_literal};

use crate::namespace_ddl::consume_word;
use crate::normalize::refuse_multi_statement_sql;
use crate::normalize::statement_guard::tokens_have_nontrailing_content_after_semicolon;
use crate::spark_rewrites::{byte_offset, line_starts};
use crate::spark_type_names::spark_ddl_type_name;
use crate::view_ddl::describe::describe_rows_batch;
use crate::view_ddl::execute::{
    execute_show_views_with, route_create_temp_view, temp_view_err, temp_view_name_arg,
};
use crate::view_ddl::parse::try_parse_show_views;
use crate::view_ddl::temp_alter::{refuse_quoted_view_rename, route_temp_view_alter};
use crate::view_ddl::temp_parse::{spark_parse_error, try_parse_create_temp_view};
use crate::view_ddl::temp_view::temp_view_column_comments;
use crate::view_ddl::temp_write::{double_quoted_target_refusal, refuse_temp_view_write_target};
use crate::write_options::StatementWriteOptions;

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn route_temp_view_statement(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &StatementWriteOptions,
    temp_views: Option<&dyn TempViewSession>,
) -> Option<Result<DataFrame>> {
    if let Some(refusal) = refuse_leading_semicolon(sql) {
        return Some(Err(refusal));
    }
    if let Some(parsed) = try_parse_create_temp_view(sql) {
        return Some(
            route_create_temp_view(ctx, catalogs, parsed, write_options, temp_views).await,
        );
    }
    let temp_views = temp_views?;
    let head = first_word(sql)?;
    if write_options.is_empty()
        && let Some(refusal) = refuse_temp_view_write_target(&head, sql, temp_views)
    {
        return Some(Err(refusal));
    }
    if write_options.is_empty()
        && head.eq_ignore_ascii_case("ALTER")
        && let Some(outcome) = route_temp_view_alter(ctx, sql, temp_views).await
    {
        return Some(outcome);
    }
    if head.eq_ignore_ascii_case("DROP") {
        return try_drop_temp_view(ctx, sql, write_options, temp_views);
    }
    if head.eq_ignore_ascii_case("DESCRIBE") || head.eq_ignore_ascii_case("DESC") {
        return try_describe_temp_view(ctx, sql, write_options, temp_views).await;
    }
    if !head.eq_ignore_ascii_case("SHOW") {
        return None;
    }
    let parsed = try_parse_show_views(sql)?;
    if let Err(error) = refuse_multi_statement_sql(sql) {
        return Some(Err(error));
    }
    Some(match (parsed, temp_views.list_temp_view_names()) {
        (Ok(statement), Ok(names)) => {
            execute_show_views_with(ctx, catalogs, statement, names).await
        }
        (Err(error), _) => Err(error),
        (_, Err(error)) => Err(temp_view_err(error)),
    })
}

fn refuse_leading_semicolon(sql: &str) -> Option<DataFusionError> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    let first = tokens
        .into_iter()
        .find(|token| !matches!(token, Token::Whitespace(_)))?;
    matches!(first, Token::SemiColon).then(|| {
        spark_parse_error(spark_error::message(
            spark_error::PARSE_SYNTAX_ERROR,
            &[("near", "';'")],
        ))
    })
}

pub(crate) fn refuse_unparsable_target(sql: &str) -> Option<DataFusionError> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
        .tokenize_with_location()
        .ok()?;
    let mut parser = Parser::new(&DatabricksDialect {}).with_tokens_with_locations(tokens);
    let drop = if parser.parse_keyword(Keyword::DROP) {
        if !parser.parse_keyword(Keyword::TABLE) && !parser.parse_keyword(Keyword::VIEW) {
            return None;
        }
        let _ = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        true
    } else if parser.parse_keywords(&[Keyword::TRUNCATE, Keyword::TABLE]) {
        false
    } else {
        return refuse_quoted_view_rename(&mut parser);
    };
    let start = byte_offset(&line_starts(sql), sql, parser.peek_token().span.start)?;
    if let Some(near) = sql
        .get(start..)
        .and_then(|rest| unparsable_near(rest, drop))
    {
        return Some(parse_refusal(&near));
    }
    (drop && parser.parse_object_name(false).is_ok() && parser.consume_token(&Token::Comma))
        .then(|| parse_refusal("','"))
}

fn unparsable_near(rest: &str, drop: bool) -> Option<String> {
    let first = rest.chars().next()?;
    if matches!(first, '\'' | '"') {
        return Some(format!("'{}'", quoted_token(rest, first)));
    }
    let end = rest
        .find(|char: char| !(char.is_alphanumeric() || char == '_'))
        .unwrap_or(rest.len());
    let (word, tail) = rest.split_at(end);
    if word.is_empty() || tail.starts_with('.') {
        return None;
    }
    if spark_numeric_literal(word) {
        return Some(format!("'{word}'"));
    }
    if !drop || tail.starts_with("--") || tail.starts_with("/*") {
        return None;
    }
    let next = tail
        .chars()
        .next()
        .filter(|char| !char.is_whitespace() && !matches!(char, ';' | ','))?;
    Some(format!("'{next}': extra input '{next}'"))
}

fn quoted_token(rest: &str, quote: char) -> &str {
    let mut chars = rest.char_indices().skip(1).peekable();
    while let Some((index, char)) = chars.next() {
        if char != quote {
            continue;
        }
        if chars.next_if(|(_, next)| *next == quote).is_none() {
            return &rest[..index + char.len_utf8()];
        }
    }
    rest
}

fn parse_refusal(near: &str) -> DataFusionError {
    spark_parse_error(spark_error::message(
        spark_error::PARSE_SYNTAX_ERROR,
        &[("near", near)],
    ))
}

fn first_word(sql: &str) -> Option<String> {
    Tokenizer::new(&DatabricksDialect {}, sql)
        .tokenize()
        .ok()?
        .into_iter()
        .find(|token| !matches!(token, Token::Whitespace(_)))
        .and_then(|token| match token {
            Token::Word(word) if word.quote_style.is_none() => Some(word.value),
            _ => None,
        })
}

fn try_drop_temp_view(
    ctx: &SessionContext,
    sql: &str,
    write_options: &StatementWriteOptions,
    temp_views: &dyn TempViewSession,
) -> Option<Result<DataFrame>> {
    let statements = Parser::parse_sql(&DatabricksDialect {}, sql).ok()?;
    let [
        Statement::Drop {
            object_type,
            names,
            temporary: false,
            ..
        },
    ] = statements.as_slice()
    else {
        return None;
    };
    let context = match object_type {
        ObjectType::View => "DROP VIEW",
        ObjectType::Table => "DROP TABLE",
        _ => return None,
    };
    let [name] = names.as_slice() else {
        return None;
    };
    let target = match temp_view_target(name, temp_views) {
        Ok(target) => target?,
        Err(error) => return Some(Err(error)),
    };
    if let Some(refusal) = double_quoted_target_refusal(name) {
        return Some(Err(refusal));
    }
    Some(
        write_options
            .refuse_if_non_empty(context)
            .and_then(|()| temp_views.drop_temp_view(&target).map_err(temp_view_err))
            .and_then(|_| ctx.read_empty()),
    )
}

async fn try_describe_temp_view(
    ctx: &SessionContext,
    sql: &str,
    write_options: &StatementWriteOptions,
    temp_views: &dyn TempViewSession,
) -> Option<Result<DataFrame>> {
    let name = parse_describe_target(sql)?;
    let home = match temp_view_home_segments(&name, temp_views) {
        Ok(home) => home?,
        Err(error) => return Some(Err(error)),
    };
    Some(describe_temp_view(ctx, &home, write_options).await)
}

async fn describe_temp_view(
    ctx: &SessionContext,
    home: &[String],
    write_options: &StatementWriteOptions,
) -> Result<DataFrame> {
    write_options.refuse_if_non_empty("DESCRIBE TABLE")?;
    let [catalog, schema, table] = home else {
        return ctx.read_empty();
    };
    let provider = ctx
        .table_provider(TableReference::full(
            catalog.as_str(),
            schema.as_str(),
            table.as_str(),
        ))
        .await?;
    let comments = temp_view_column_comments(provider.as_ref())?;
    let rows = provider
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let comment = comments
                .as_ref()
                .and_then(|comments| comments.get(index).cloned().flatten());
            (
                field.name().clone(),
                spark_ddl_type_name(field.data_type()),
                comment,
            )
        })
        .collect::<Vec<_>>();
    ctx.read_batch(describe_rows_batch(rows)?)
}

fn parse_describe_target(sql: &str) -> Option<ObjectName> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    if tokens_have_nontrailing_content_after_semicolon(&tokens) {
        return None;
    }
    let mut parser = Parser::new(&DatabricksDialect {}).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::DESCRIBE) && !parser.parse_keyword(Keyword::DESC) {
        return None;
    }
    let _ = parser.parse_keyword(Keyword::TABLE);
    let _ = parser.parse_keyword(Keyword::EXTENDED) || consume_word(&mut parser, "FORMATTED");
    let name = parser.parse_object_name(false).ok()?;
    matches!(parser.peek_token().token, Token::EOF | Token::SemiColon).then_some(name)
}

fn temp_view_target(name: &ObjectName, temp_views: &dyn TempViewSession) -> Result<Option<String>> {
    Ok(temp_view_home_segments(name, temp_views)?.map(|home| {
        home.last()
            .map(|table| format!("\"{}\"", table.replace('"', "\"\"")))
            .unwrap_or_default()
    }))
}

pub(crate) fn temp_view_home_segments(
    name: &ObjectName,
    temp_views: &dyn TempViewSession,
) -> Result<Option<Vec<String>>> {
    let idents = name
        .0
        .iter()
        .filter_map(|part| part.as_ident())
        .collect::<Vec<_>>();
    if idents.len() != name.0.len() {
        return Ok(None);
    }
    let table = match idents.as_slice() {
        [table] => *table,
        [catalog, schema, table] => {
            let home = temp_views.temp_view_home().map_err(temp_view_err)?;
            let [home_catalog, home_schema] = home.as_slice() else {
                return Ok(None);
            };
            if !ident_names(catalog, home_catalog) || !ident_names(schema, home_schema) {
                return Ok(None);
            }
            *table
        }
        _ => return Ok(None),
    };
    temp_views
        .resolve_temp_view_home_ref(&temp_view_name_arg(table))
        .map_err(temp_view_err)
}

fn ident_names(ident: &Ident, expected: &str) -> bool {
    if ident.quote_style.is_some() {
        ident.value == expected
    } else {
        ident.value.eq_ignore_ascii_case(expected)
    }
}
