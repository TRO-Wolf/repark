use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::alter::{set_table_properties, unset_table_properties};

use crate::{catalog_handle, iceberg_err, name_parts};

#[derive(Debug)]
pub(crate) struct CommentOnTableDdl {
    table_parts: Vec<String>,
    doc: Option<String>,
}

pub(crate) fn try_parse_comment_on_table_ddl(sql: &str) -> Option<Result<CommentOnTableDdl>> {
    let head = sql.trim_start();
    if head.len() < 7 || !head.as_bytes()[..7].eq_ignore_ascii_case(b"COMMENT") {
        return None;
    }
    let dialect = DatabricksDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(sql).ok()?;
    if !parser.parse_keywords(&[Keyword::COMMENT, Keyword::ON]) {
        return None;
    }
    if !parser.parse_keyword(Keyword::TABLE) {
        return None;
    }
    let table_parts = match parser.parse_object_name(false) {
        Ok(name) => name_parts(&name),
        Err(error) => {
            return Some(Err(DataFusionError::Plan(format!(
                "COMMENT ON TABLE expects a three-part `catalog.namespace.table` name: {error}"
            ))));
        }
    };
    if table_parts.len() != 3 {
        return Some(Err(DataFusionError::Plan(format!(
            "COMMENT ON TABLE expects a three-part `catalog.namespace.table` name, got `{}`",
            table_parts.join(".")
        ))));
    }
    if !parser.parse_keyword(Keyword::IS) {
        return Some(Err(DataFusionError::Plan(
            "COMMENT ON TABLE expects IS 'comment' after the table name".to_string(),
        )));
    }
    let doc = if parser.parse_keyword(Keyword::NULL) {
        None
    } else {
        match parser.parse_literal_string() {
            Ok(literal) => Some(literal),
            Err(error) => {
                return Some(Err(DataFusionError::Plan(format!(
                    "COMMENT ON TABLE expects a string literal after IS: {error}"
                ))));
            }
        }
    };
    if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return Some(Err(DataFusionError::Plan(format!(
            "trailing tokens after COMMENT ON TABLE (starting at `{}`)",
            parser.peek_token().token
        ))));
    }
    Some(Ok(CommentOnTableDdl { table_parts, doc }))
}

pub(crate) async fn execute_comment_on_table_ddl(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ddl: CommentOnTableDdl,
) -> Result<DataFrame> {
    let [catalog_name, namespace, table] = ddl.table_parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "COMMENT ON TABLE expects a three-part `catalog.namespace.table` name, got `{}`",
            ddl.table_parts.join(".")
        )));
    };
    let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
    let handle = catalog_handle(catalogs, catalog_name)?;
    match ddl.doc {
        Some(doc) => {
            set_table_properties(
                handle.as_ref(),
                &ident,
                &HashMap::from([("comment".to_string(), doc)]),
            )
            .await
            .map_err(iceberg_err)?;
        }
        None => {
            unset_table_properties(handle.as_ref(), &ident, &["comment".to_string()])
                .await
                .map_err(iceberg_err)?;
        }
    }
    ctx.read_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_comment_on_table_set_and_unset() {
        let set = try_parse_comment_on_table_ddl("COMMENT ON TABLE ice.sales.t IS 'doc here'")
            .expect("recognize")
            .expect("parse");
        assert_eq!(set.table_parts, vec!["ice", "sales", "t"]);
        assert_eq!(set.doc.as_deref(), Some("doc here"));
        let unset = try_parse_comment_on_table_ddl("COMMENT ON TABLE ice.sales.t IS NULL")
            .expect("recognize")
            .expect("parse");
        assert_eq!(unset.doc, None);
    }

    #[test]
    fn parse_comment_on_table_leaves_other_shapes_alone() {
        assert!(try_parse_comment_on_table_ddl("SELECT 1").is_none());
        assert!(try_parse_comment_on_table_ddl("COMMENT ON COLUMN ice.sales.t.c IS 'x'").is_none());
        assert!(
            try_parse_comment_on_table_ddl("COMMENT ON TABLE ice.sales.t IS 'a' EXTRA")
                .expect("recognize")
                .is_err()
        );
    }

    #[test]
    fn parse_comment_on_table_requires_three_part_name_and_is() {
        assert!(
            try_parse_comment_on_table_ddl("COMMENT ON TABLE sales.t IS 'x'")
                .expect("recognize")
                .is_err()
        );
        assert!(
            try_parse_comment_on_table_ddl("COMMENT ON TABLE ice.sales.t 'x'")
                .expect("recognize")
                .is_err()
        );
    }
}
