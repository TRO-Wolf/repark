use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::catalog_ops::name_parts;
use crate::namespace_ddl::consume_word;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ShowCreateStatement {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) as_serde: bool,
}

impl ShowCreateStatement {
    fn complete_from_session(&mut self, catalogs: &CatalogRegistry) {
        if self.catalog.is_empty() || self.namespace.is_empty() {
            let (catalog, namespace) = crate::use_ddl::session_defaults(catalogs);
            if self.catalog.is_empty() {
                self.catalog = catalog;
            }
            if self.namespace.is_empty() {
                self.namespace = namespace;
            }
        }
    }

    fn ident(&self) -> TableIdent {
        TableIdent::new(
            NamespaceIdent::new(self.namespace.clone()),
            self.table.clone(),
        )
    }
}

pub(crate) fn try_parse_show_create(sql: &str) -> Option<Result<ShowCreateStatement>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keywords(&[Keyword::SHOW, Keyword::CREATE, Keyword::TABLE]) {
        return None;
    }
    if at_statement_end(&parser) {
        return Some(Err(parse_class_error(
            "[INVALID_STATEMENT_OR_CLAUSE] The statement or clause: SHOW CREATE TABLE is not \
             valid. SQLSTATE: 42601"
                .to_string(),
        )));
    }
    let Ok(name) = parser.parse_object_name(false) else {
        return Some(Err(syntax_error_at(&parser)));
    };
    let as_serde = if parser.parse_keyword(Keyword::AS) {
        if !consume_word(&mut parser, "SERDE") {
            return Some(Err(syntax_error_at(&parser)));
        }
        true
    } else {
        false
    };
    if !at_statement_end(&parser) {
        return Some(Err(syntax_error_at(&parser)));
    }
    let parts = name_parts(&name);
    let (catalog, namespace, table) = match parts.as_slice() {
        [catalog, namespace, table] => (catalog.clone(), namespace.clone(), table.clone()),
        [namespace, table] => (String::new(), namespace.clone(), table.clone()),
        [table] => (String::new(), String::new(), table.clone()),
        _ => return None,
    };
    Some(Ok(ShowCreateStatement {
        catalog,
        namespace,
        table,
        as_serde,
    }))
}

fn at_statement_end(parser: &Parser) -> bool {
    matches!(parser.peek_token().token, Token::EOF | Token::SemiColon)
}

fn syntax_error_at(parser: &Parser) -> DataFusionError {
    let near = match parser.peek_token().token {
        Token::EOF => "end of input".to_string(),
        token => format!("'{token}'"),
    };
    parse_class_error(format!(
        "[PARSE_SYNTAX_ERROR] Syntax error at or near {near}. SQLSTATE: 42601"
    ))
}

fn parse_class_error(message: String) -> DataFusionError {
    DataFusionError::SQL(Box::new(ParserError::ParserError(message)), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_one_two_and_three_part_names_and_as_serde() {
        let parsed = try_parse_show_create("show create table ice.sales.t")
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed,
            ShowCreateStatement {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "t".to_string(),
                as_serde: false,
            }
        );
        let parsed = try_parse_show_create("SHOW CREATE TABLE sales.t AS SERDE;")
            .unwrap()
            .unwrap();
        assert!(parsed.as_serde);
        assert!(parsed.catalog.is_empty());
        let parsed = try_parse_show_create("SHOW CREATE TABLE `we-ird`")
            .unwrap()
            .unwrap();
        assert_eq!(parsed.table, "we-ird");
    }

    #[test]
    fn parse_leaves_near_misses_alone() {
        for sql in [
            "SHOW CREATE VIEW v",
            "SHOW CREATE",
            "SHOW TABLES",
            "SHOW COLUMNS IN t",
            "SHOW TBLPROPERTIES t",
            "SHOW CREATE TABLE a.b.c.d",
            "SELECT 1",
        ] {
            assert!(try_parse_show_create(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn parse_refuses_malformed_forms_loudly() {
        let missing = try_parse_show_create("SHOW CREATE TABLE")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(
            missing.contains("[INVALID_STATEMENT_OR_CLAUSE]"),
            "{missing}"
        );
        let trailing = try_parse_show_create("SHOW CREATE TABLE t EXTRA")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(trailing.contains("[PARSE_SYNTAX_ERROR]"), "{trailing}");
        let as_json = try_parse_show_create("SHOW CREATE TABLE t AS JSON")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(as_json.contains("[PARSE_SYNTAX_ERROR]"), "{as_json}");
    }
}
