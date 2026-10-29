use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::ObjectName;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;
use repark_core::TempViewSession;

use crate::view_ddl::temp_ddl::temp_view_home_segments;

const WRITE_HEADS: [&str; 5] = ["DELETE", "UPDATE", "MERGE", "INSERT", "TRUNCATE"];

#[derive(Clone, Copy)]
enum WriteRoute {
    Delete,
    Update,
    Merge,
    Insert,
    Truncate,
}

pub(crate) fn refuse_temp_view_write_target(
    head: &str,
    sql: &str,
    temp_views: &dyn TempViewSession,
) -> Option<DataFusionError> {
    if !WRITE_HEADS
        .iter()
        .any(|write| head.eq_ignore_ascii_case(write))
    {
        return None;
    }
    let (route, name) = write_target(sql)?;
    let home = temp_view_home_segments(&name, temp_views).ok()??;
    let view = home.last()?;
    Some(DataFusionError::Plan(refusal_text(route, &name, view)))
}

fn write_target(sql: &str) -> Option<(WriteRoute, ObjectName)> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    let mut parser = Parser::new(&DatabricksDialect {}).with_tokens(tokens);
    let route = if parser.parse_keywords(&[Keyword::DELETE, Keyword::FROM]) {
        WriteRoute::Delete
    } else if parser.parse_keyword(Keyword::UPDATE) {
        WriteRoute::Update
    } else if parser.parse_keywords(&[Keyword::MERGE, Keyword::INTO]) {
        WriteRoute::Merge
    } else if parser.parse_keyword(Keyword::INSERT) {
        insert_route(&mut parser)?
    } else if parser.parse_keywords(&[Keyword::TRUNCATE, Keyword::TABLE]) {
        WriteRoute::Truncate
    } else {
        return None;
    };
    let name = parser.parse_object_name(false).ok()?;
    Some((route, name))
}

fn insert_route(parser: &mut Parser<'_>) -> Option<WriteRoute> {
    if !parser.parse_keyword(Keyword::INTO) {
        let overwrite = parser.parse_keyword(Keyword::OVERWRITE);
        if !overwrite
            || parser.peek_keyword(Keyword::LOCAL)
            || parser.peek_keyword(Keyword::DIRECTORY)
        {
            return None;
        }
    }
    let _ = parser.parse_keyword(Keyword::TABLE);
    Some(WriteRoute::Insert)
}

fn refusal_text(route: WriteRoute, name: &ObjectName, view: &str) -> String {
    match route {
        WriteRoute::Delete => expects_table("DELETE", view),
        WriteRoute::Insert => expects_table("INSERT", view),
        WriteRoute::Truncate => expects_table("TRUNCATE TABLE", &written_leaf(name, view)),
        WriteRoute::Update => unsupported_operation("UPDATE TABLE", view),
        WriteRoute::Merge => unsupported_operation("MERGE INTO TABLE", view),
    }
}

fn written_leaf(name: &ObjectName, view: &str) -> String {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map_or_else(|| view.to_string(), |ident| ident.value.clone())
}

fn quoted(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn expects_table(operation: &str, view: &str) -> String {
    format!(
        "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] '{operation}' expects a table but {} is a view. \
         SQLSTATE: 42809",
        quoted(view)
    )
}

fn unsupported_operation(operation: &str, view: &str) -> String {
    format!(
        "[UNSUPPORTED_FEATURE.TABLE_OPERATION] The feature is not supported: Table {} does not \
         support {operation}. Please check the current catalog and namespace to make sure the \
         qualified table name is expected, and also check the catalog implementation which is \
         configured by \"spark.sql.catalog\". SQLSTATE: 0A000",
        quoted(view)
    )
}
