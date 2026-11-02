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
}

pub(crate) async fn route_temp_view_alter(
    ctx: &SessionContext,
    sql: &str,
    temp_views: &dyn TempViewSession,
) -> Option<Result<DataFrame>> {
    let (name, route) = alter_target(sql)?;
    let home = temp_view_home_segments(&name, temp_views).ok()??;
    let view = home.last()?;
    let source = written_leaf(&name, view);
    Some(match route {
        AlterRoute::Refuse(operation, alternative) => Err(DataFusionError::Plan(refusal_text(
            operation,
            alternative,
            &source,
        ))),
        AlterRoute::Rename(destination, next) => {
            rename_temp_view(ctx, sql, temp_views, &home, &source, &destination, &next).await
        }
    })
}

fn alter_target(sql: &str) -> Option<(ObjectName, AlterRoute)> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    let mut parser = Parser::new(&DatabricksDialect {}).with_tokens(tokens);
    if !parser.parse_keywords(&[Keyword::ALTER, Keyword::TABLE]) {
        return None;
    }
    let _ = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let name = parser.parse_object_name(false).ok()?;
    let route = alter_route(&mut parser)?;
    Some((name, route))
}

fn refuse(operation: &'static str) -> AlterRoute {
    AlterRoute::Refuse(operation, Alternative::NoAlternative)
}

fn refuse_use_alter_view(operation: &'static str) -> AlterRoute {
    AlterRoute::Refuse(operation, Alternative::UseAlterView)
}

fn alter_route(parser: &mut Parser<'_>) -> Option<AlterRoute> {
    let Token::Word(head) = parser.next_token().token else {
        return None;
    };
    match head.value.to_ascii_uppercase().as_str() {
        "ADD" => add_route(parser),
        "DROP" => drop_route(parser),
        "RENAME" => rename_route(parser),
        "ALTER" => Some(refuse("ALTER COLUMN")),
        "CHANGE" => Some(refuse("CHANGE COLUMN")),
        "REPLACE" => replace_route(parser),
        "SET" => set_route(parser),
        "UNSET" => Some(refuse_use_alter_view("UNSET TBLPROPERTIES")),
        "PARTITION" => partition_route(parser),
        "CLUSTER" => Some(refuse("CLUSTER BY")),
        "RECOVER" => Some(refuse("RECOVER PARTITIONS")),
        _ => None,
    }
}

fn add_route(parser: &mut Parser<'_>) -> Option<AlterRoute> {
    if consume_word(parser, "COLUMNS") {
        return Some(refuse("ADD COLUMNS"));
    }
    if consume_word(parser, "CONSTRAINT") {
        return Some(refuse("ADD CONSTRAINT"));
    }
    let _ = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    if consume_word(parser, "PARTITION") {
        return (!consume_word(parser, "FIELD")).then(|| refuse("ADD PARTITION ..."));
    }
    Some(refuse("ADD COLUMN"))
}

fn drop_route(parser: &mut Parser<'_>) -> Option<AlterRoute> {
    if ["IDENTIFIER", "BRANCH", "TAG"]
        .iter()
        .any(|word| consume_word(parser, word))
    {
        return None;
    }
    if consume_word(parser, "CONSTRAINT") {
        return Some(refuse("DROP CONSTRAINT"));
    }
    let _ = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    if consume_word(parser, "PARTITION") {
        return (!consume_word(parser, "FIELD")).then(|| refuse("DROP PARTITION ..."));
    }
    Some(refuse("DROP COLUMNS"))
}

fn rename_route(parser: &mut Parser<'_>) -> Option<AlterRoute> {
    if !parser.parse_keyword(Keyword::TO) {
        return Some(refuse("RENAME COLUMN"));
    }
    let destination = parser.parse_object_name(false).ok()?;
    Some(AlterRoute::Rename(destination, parser.peek_token().token))
}

fn replace_route(parser: &mut Parser<'_>) -> Option<AlterRoute> {
    if ["PARTITION", "BRANCH", "TAG"]
        .iter()
        .any(|word| consume_word(parser, word))
    {
        return None;
    }
    Some(refuse("REPLACE COLUMNS"))
}

fn set_route(parser: &mut Parser<'_>) -> Option<AlterRoute> {
    if consume_word(parser, "TBLPROPERTIES") {
        return Some(refuse_use_alter_view("SET TBLPROPERTIES"));
    }
    if consume_word(parser, "SERDE") || consume_word(parser, "SERDEPROPERTIES") {
        return Some(refuse_use_alter_view("SET [SERDE|SERDEPROPERTIES]"));
    }
    consume_word(parser, "LOCATION").then(|| refuse("SET LOCATION ..."))
}

fn partition_route(parser: &mut Parser<'_>) -> Option<AlterRoute> {
    if !parser.consume_token(&Token::LParen) {
        return None;
    }
    let mut depth = 1_usize;
    while depth > 0 {
        match parser.next_token().token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::EOF => return None,
            _ => {}
        }
    }
    if consume_word(parser, "RENAME") {
        return Some(refuse("RENAME TO PARTITION"));
    }
    if consume_word(parser, "SET") {
        return set_route(parser);
    }
    None
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
