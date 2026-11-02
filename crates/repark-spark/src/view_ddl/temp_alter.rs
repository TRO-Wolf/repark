use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{Ident, ObjectName};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use repark_common::spark_error;
use repark_core::TempViewSession;

use crate::namespace_ddl::consume_word;
use crate::normalize::refuse_multi_statement_sql;
use crate::view_ddl::execute::{temp_view_err, temp_view_name_arg};
use crate::view_ddl::temp_ddl::temp_view_home_segments;
use crate::view_ddl::temp_parse::spark_parse_error;
use crate::view_ddl::temp_view::renamed_temp_view;
use crate::view_ddl::temp_write::{quoted, written_leaf};

const SESSION_DATABASE: &str = "default";

#[derive(Clone, Copy)]
enum Alternative {
    NoAlternative,
    UseAlterView,
}

enum AlterRoute {
    Refuse(&'static str, Alternative),
    Rename(ObjectName, Token),
    Extension(&'static str),
    Unknown(String),
}

pub(crate) async fn route_temp_view_alter(
    ctx: &SessionContext,
    sql: &str,
    temp_views: &dyn TempViewSession,
) -> Option<Result<DataFrame>> {
    let (name, prefixed, route) = alter_target(sql)?;
    let home = match temp_view_home_segments(&name, temp_views) {
        Ok(home) => home?,
        Err(error) => return Some(Err(error)),
    };
    let view = home.last()?;
    let source = written_leaf(&name, view);
    let refused = |operation: &str, alternative| {
        Err(DataFusionError::Plan(refusal_text(
            operation,
            alternative,
            &source,
        )))
    };
    Some(match route {
        AlterRoute::Refuse(operation, alternative) => refused(operation, alternative),
        AlterRoute::Rename(destination, next) if !prefixed => {
            rename_temp_view(ctx, sql, temp_views, &home, &source, &destination, &next).await
        }
        AlterRoute::Rename(..) => refused("RENAME", Alternative::NoAlternative),
        AlterRoute::Extension(_) if !prefixed => return None,
        AlterRoute::Extension(head) => refused(head, Alternative::NoAlternative),
        AlterRoute::Unknown(head) => refused(&head, Alternative::NoAlternative),
    })
}

fn alter_target(sql: &str) -> Option<(ObjectName, bool, AlterRoute)> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    let mut parser = Parser::new(&DatabricksDialect {}).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::ALTER) {
        return None;
    }
    let iceberg = parser.parse_keyword(Keyword::ICEBERG);
    if !parser.parse_keyword(Keyword::TABLE) {
        return None;
    }
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let only = parser.parse_keyword(Keyword::ONLY);
    let name = parser.parse_object_name(false).ok()?;
    let quoted_target = name
        .0
        .iter()
        .filter_map(|part| part.as_ident())
        .any(|ident| ident.quote_style.is_some_and(|quote| quote != '`'));
    let on_cluster = parser.parse_keywords(&[Keyword::ON, Keyword::CLUSTER]);
    if on_cluster {
        parser.parse_identifier().ok()?;
    }
    let route = alter_route(&mut parser)?;
    Some((
        name,
        iceberg || if_exists || only || on_cluster || quoted_target,
        route,
    ))
}

fn refuse(operation: &'static str) -> AlterRoute {
    AlterRoute::Refuse(operation, Alternative::NoAlternative)
}

fn refuse_use_alter_view(operation: &'static str) -> AlterRoute {
    AlterRoute::Refuse(operation, Alternative::UseAlterView)
}

fn unknown(head: &str) -> AlterRoute {
    AlterRoute::Unknown(head.to_string())
}

fn alter_route(parser: &mut Parser<'_>) -> Option<AlterRoute> {
    let head = match parser.next_token().token {
        Token::EOF | Token::SemiColon => return None,
        Token::Word(word) => word.value.to_ascii_uppercase(),
        other => return Some(unknown(&other.to_string())),
    };
    Some(match head.as_str() {
        "ADD" => add_route(parser),
        "DROP" => drop_route(parser),
        "RENAME" => rename_route(parser),
        "ALTER" => refuse("ALTER COLUMN"),
        "CHANGE" => refuse("CHANGE COLUMN"),
        "REPLACE" => replace_route(parser),
        "SET" => set_route(parser),
        "UNSET" => refuse_use_alter_view("UNSET TBLPROPERTIES"),
        "PARTITION" => partition_route(parser),
        "CLUSTER" => refuse("CLUSTER BY"),
        "RECOVER" => refuse("RECOVER PARTITIONS"),
        "DEFAULT" if consume_word(parser, "COLLATION") => refuse("DEFAULT COLLATION"),
        "WRITE" => AlterRoute::Extension("WRITE"),
        "CREATE" => AlterRoute::Extension("CREATE"),
        _ => AlterRoute::Unknown(head),
    })
}

fn add_route(parser: &mut Parser<'_>) -> AlterRoute {
    if consume_word(parser, "COLUMNS") {
        return refuse("ADD COLUMNS");
    }
    if consume_word(parser, "CONSTRAINT") {
        return refuse("ADD CONSTRAINT");
    }
    let _ = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    if consume_word(parser, "PARTITION") {
        if consume_word(parser, "FIELD") {
            return AlterRoute::Extension("ADD");
        }
        return refuse("ADD PARTITION ...");
    }
    refuse("ADD COLUMN")
}

fn bare_words(parser: &mut Parser<'_>, words: &[&str]) -> bool {
    let matched = words.iter().enumerate().all(|(index, word)| {
        matches!(
            &parser.peek_nth_token(index).token,
            Token::Word(peeked) if peeked.quote_style.is_none()
                && peeked.value.eq_ignore_ascii_case(word)
        )
    });
    if matched {
        for _ in words {
            parser.next_token();
        }
    }
    matched
}

fn ref_head(parser: &mut Parser<'_>) -> bool {
    let named = matches!(parser.peek_nth_token(1).token, Token::Word(_));
    named && (bare_words(parser, &["BRANCH"]) || bare_words(parser, &["TAG"]))
}

fn drop_route(parser: &mut Parser<'_>) -> AlterRoute {
    if bare_words(parser, &["IDENTIFIER", "FIELDS"]) || ref_head(parser) {
        return AlterRoute::Extension("DROP");
    }
    if consume_word(parser, "CONSTRAINT") {
        return refuse("DROP CONSTRAINT");
    }
    let _ = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    if consume_word(parser, "PARTITION") {
        if consume_word(parser, "FIELD") {
            return AlterRoute::Extension("DROP");
        }
        return refuse("DROP PARTITION ...");
    }
    refuse("DROP COLUMNS")
}

fn rename_route(parser: &mut Parser<'_>) -> AlterRoute {
    if !parser.parse_keyword(Keyword::TO) {
        return refuse("RENAME COLUMN");
    }
    match parser.parse_object_name(false) {
        Ok(destination) => AlterRoute::Rename(destination, parser.peek_token().token),
        Err(_) => unknown("RENAME"),
    }
}

fn replace_route(parser: &mut Parser<'_>) -> AlterRoute {
    if ["PARTITION", "BRANCH", "TAG"]
        .iter()
        .any(|word| consume_word(parser, word))
    {
        return AlterRoute::Extension("REPLACE");
    }
    refuse("REPLACE COLUMNS")
}

fn set_route(parser: &mut Parser<'_>) -> AlterRoute {
    if consume_word(parser, "TBLPROPERTIES") {
        return refuse_use_alter_view("SET TBLPROPERTIES");
    }
    if consume_word(parser, "SERDE") || consume_word(parser, "SERDEPROPERTIES") {
        return refuse_use_alter_view("SET [SERDE|SERDEPROPERTIES]");
    }
    if consume_word(parser, "LOCATION") {
        return refuse("SET LOCATION ...");
    }
    if bare_words(parser, &["IDENTIFIER", "FIELDS"]) {
        return AlterRoute::Extension("SET");
    }
    unknown("SET")
}

fn partition_route(parser: &mut Parser<'_>) -> AlterRoute {
    if !parser.consume_token(&Token::LParen) {
        return unknown("PARTITION");
    }
    let mut depth = 1_usize;
    while depth > 0 {
        match parser.next_token().token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::EOF => return unknown("PARTITION"),
            _ => {}
        }
    }
    if consume_word(parser, "RENAME") {
        return refuse("RENAME TO PARTITION");
    }
    if consume_word(parser, "SET") {
        return set_route(parser);
    }
    unknown("PARTITION")
}

fn refusal_text(operation: &str, alternative: Alternative, shown: &str) -> String {
    let sentence = format!(
        "'ALTER TABLE ... {operation}' expects a table but {} is a view.",
        quoted(shown)
    );
    match alternative {
        Alternative::NoAlternative => {
            format!("[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] {sentence}  SQLSTATE: 42809")
        }
        Alternative::UseAlterView => format!(
            "[EXPECT_TABLE_NOT_VIEW.USE_ALTER_VIEW] {sentence} Please use ALTER VIEW instead. \
             SQLSTATE: 42809"
        ),
    }
}

async fn rename_temp_view(
    ctx: &SessionContext,
    sql: &str,
    temp_views: &dyn TempViewSession,
    home: &[String],
    source: &str,
    destination: &ObjectName,
    next: &Token,
) -> Result<DataFrame> {
    if let Some(refusal) = quoted_destination_refusal(destination) {
        return Err(refusal);
    }
    if !matches!(next, Token::EOF | Token::SemiColon) {
        let near = format!("'{next}'");
        return Err(spark_parse_error(spark_error::message(
            spark_error::PARSE_SYNTAX_ERROR,
            &[("near", near.as_str())],
        )));
    }
    refuse_multi_statement_sql(sql)?;
    let target = rename_destination(ctx, source, destination)?;
    let argument = temp_view_name_arg(target);
    if temp_views
        .resolve_temp_view_home_ref(&argument)
        .map_err(temp_view_err)?
        .is_some()
    {
        let relation = quoted(&target.value);
        return Err(DataFusionError::Plan(spark_error::message(
            spark_error::TABLE_OR_VIEW_ALREADY_EXISTS,
            &[("relationName", relation.as_str())],
        )));
    }
    let new_home = temp_views
        .temp_view_target_ref(&argument)
        .map_err(temp_view_err)?;
    let (Some(old_reference), Some(new_reference)) =
        (home_reference(home), home_reference(&new_home))
    else {
        return Err(DataFusionError::Plan(format!(
            "temporary view home must name a catalog, a namespace and a view, got {home:?} \
             and {new_home:?}"
        )));
    };
    let provider = ctx.table_provider(old_reference.clone()).await?;
    let renamed = match renamed_temp_view(&provider, new_home, quoted(&target.value))? {
        Some(view) => ctx.read_table(view)?.into_view(),
        None => provider,
    };
    ctx.register_table(new_reference, renamed)?;
    ctx.deregister_table(old_reference)?;
    ctx.read_empty()
}

pub(crate) fn refuse_quoted_view_rename(parser: &mut Parser<'_>) -> Option<DataFusionError> {
    if !parser.parse_keywords(&[Keyword::ALTER, Keyword::VIEW]) {
        return None;
    }
    parser.parse_object_name(false).ok()?;
    if !parser.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
        return None;
    }
    quoted_destination_refusal(&parser.parse_object_name(false).ok()?)
}

fn quoted_destination_refusal(destination: &ObjectName) -> Option<DataFusionError> {
    let (ident, quote) = destination
        .0
        .iter()
        .filter_map(|part| part.as_ident())
        .find_map(|ident| match ident.quote_style {
            Some(quote @ ('"' | '\'')) => Some((ident, quote)),
            _ => None,
        })?;
    let doubled = format!("{quote}{quote}");
    let near = format!("'{quote}{}{quote}'", ident.value.replace(quote, &doubled));
    Some(spark_parse_error(spark_error::message(
        spark_error::PARSE_SYNTAX_ERROR,
        &[("near", near.as_str())],
    )))
}

fn rename_destination<'a>(
    ctx: &SessionContext,
    source: &str,
    destination: &'a ObjectName,
) -> Result<&'a Ident> {
    let idents = destination
        .0
        .iter()
        .filter_map(|part| part.as_ident())
        .collect::<Vec<_>>();
    let rendered = idents
        .iter()
        .map(|ident| quoted(&ident.value))
        .collect::<Vec<_>>()
        .join(".");
    match idents.as_slice() {
        [name] if idents.len() == destination.0.len() => Ok(name),
        [database, _] => {
            let folded = if crate::spark_door_case_insensitive(ctx.state().config().options()) {
                database.value.to_lowercase()
            } else {
                database.value.clone()
            };
            Err(DataFusionError::Plan(if folded == SESSION_DATABASE {
                format!(
                    "RENAME TEMPORARY VIEW from '{}' to '{rendered}': cannot specify database \
                     name '{}' in the destination table.",
                    quoted(source),
                    database.value
                )
            } else {
                format!(
                    "RENAME TABLE source and destination databases do not match: \
                     '{SESSION_DATABASE}' != '{folded}'."
                )
            }))
        }
        _ => Err(DataFusionError::Plan(spark_error::message(
            spark_error::IDENTIFIER_TOO_MANY_NAME_PARTS,
            &[("identifier", rendered.as_str()), ("limit", "2")],
        ))),
    }
}

fn home_reference(home: &[String]) -> Option<TableReference> {
    let [catalog, schema, table] = home else {
        return None;
    };
    Some(TableReference::full(
        catalog.as_str(),
        schema.as_str(),
        table.as_str(),
    ))
}
