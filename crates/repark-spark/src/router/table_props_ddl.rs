use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::alter::starts_with_alter;
use repark_iceberg::write::set_location::set_table_location;

use crate::{catalog_handle, iceberg_err, name_parts, reregister};

#[derive(Debug)]
pub(crate) struct SetLocationDdl {
    table_parts: Vec<String>,
    location: String,
}

pub(crate) fn try_parse_set_location_ddl(sql: &str) -> Option<Result<SetLocationDdl>> {
    if !starts_with_alter(sql) {
        return None;
    }
    let dialect = DatabricksDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(sql).ok()?;
    if !parser.parse_keywords(&[Keyword::ALTER, Keyword::TABLE]) {
        return None;
    }
    let table_name = parser.parse_object_name(false).ok()?;
    if !parser.parse_keyword(Keyword::SET) {
        return None;
    }
    if !parser.parse_keyword(Keyword::LOCATION) {
        return None;
    }
    let table_parts = name_parts(&table_name);
    if table_parts.len() != 3 {
        return Some(Err(DataFusionError::Plan(format!(
            "ALTER TABLE … SET LOCATION expects a three-part `catalog.namespace.table` name, got \
             `{}`",
            table_parts.join(".")
        ))));
    }
    let location = match parser.parse_literal_string() {
        Ok(literal) => literal,
        Err(error) => {
            return Some(Err(DataFusionError::Plan(format!(
                "ALTER TABLE … SET LOCATION expects a quoted string literal path: {error}"
            ))));
        }
    };
    if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return Some(Err(DataFusionError::Plan(format!(
            "trailing tokens after ALTER TABLE … SET LOCATION (starting at `{}`)",
            parser.peek_token().token
        ))));
    }
    Some(Ok(SetLocationDdl {
        table_parts,
        location,
    }))
}

pub(crate) async fn execute_set_location_ddl(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ddl: SetLocationDdl,
) -> Result<DataFrame> {
    let [catalog_name, namespace, table] = ddl.table_parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "ALTER TABLE … SET LOCATION expects a three-part `catalog.namespace.table` name, got \
             `{}`",
            ddl.table_parts.join(".")
        )));
    };
    let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
    let handle = catalog_handle(catalogs, catalog_name)?;
    set_table_location(handle.as_ref(), &ident, &ddl.location)
        .await
        .map_err(iceberg_err)?;
    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, handle.clone(), catalog_name, &namespace).await?;
    ctx.read_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_set_location_keeps_the_path_verbatim() {
        let sql = "ALTER TABLE ice.sales.t SET LOCATION 's3://bucket/prefix/a b/c?d=e'";
        let ddl = try_parse_set_location_ddl(sql)
            .expect("recognize")
            .expect("parse");
        assert_eq!(ddl.table_parts, vec!["ice", "sales", "t"]);
        assert_eq!(ddl.location, "s3://bucket/prefix/a b/c?d=e");
    }

    #[test]
    fn parse_set_location_is_case_insensitive_on_the_keywords() {
        let ddl = try_parse_set_location_ddl("alter table ice.sales.t set location '/tmp/x'")
            .expect("recognize")
            .expect("parse");
        assert_eq!(ddl.location, "/tmp/x");
    }

    #[test]
    fn parse_leaves_every_other_shape_alone() {
        assert!(try_parse_set_location_ddl("SELECT 1").is_none());
        assert!(try_parse_set_location_ddl("ALTER TABLE ice.sales.t ADD COLUMN c INT").is_none());
        assert!(
            try_parse_set_location_ddl("ALTER TABLE ice.sales.t SET TBLPROPERTIES ('a'='b')")
                .is_none()
        );
        assert!(
            try_parse_set_location_ddl("ALTER TABLE ice.sales.t UNSET TBLPROPERTIES ('a')")
                .is_none()
        );
        assert!(
            try_parse_set_location_ddl("ALTER TABLE ice.sales.t CREATE BRANCH audit").is_none()
        );
        assert!(try_parse_set_location_ddl("ALTER TABLE ice.sales.t SET BRANCH audit").is_none());
        assert!(
            try_parse_set_location_ddl("ALTER TABLE ice.sales.t SET LOCATION 'p' EXTRA")
                .expect("recognize")
                .is_err()
        );
    }

    #[test]
    fn parse_claims_a_table_name_with_ddl_verb_segments() {
        let ddl = try_parse_set_location_ddl("ALTER TABLE ice.drop.tag SET LOCATION 's3://x'")
            .expect("recognize")
            .expect("parse");
        assert_eq!(ddl.table_parts, vec!["ice", "drop", "tag"]);
        assert_eq!(ddl.location, "s3://x");
    }

    #[test]
    fn parse_requires_a_three_part_name_once_the_clause_is_claimed() {
        assert!(
            try_parse_set_location_ddl("ALTER TABLE sales.t SET LOCATION 'x'")
                .expect("recognize")
                .is_err()
        );
        assert!(
            try_parse_set_location_ddl("ALTER TABLE t SET LOCATION 'x'")
                .expect("recognize")
                .is_err()
        );
    }

    #[test]
    fn parse_without_a_string_literal_refuses_naming_the_clause() {
        let missing = try_parse_set_location_ddl("ALTER TABLE ice.sales.t SET LOCATION")
            .expect("recognize")
            .expect_err("no path must refuse");
        assert!(
            missing.to_string().contains("SET LOCATION"),
            "got: {missing}"
        );
        let unquoted = try_parse_set_location_ddl("ALTER TABLE ice.sales.t SET LOCATION /tmp/x")
            .expect("recognize")
            .expect_err("a non-string token must refuse");
        assert!(
            unquoted.to_string().contains("SET LOCATION"),
            "got: {unquoted}"
        );
    }

    #[test]
    fn parse_malformed_object_name_falls_through() {
        assert!(try_parse_set_location_ddl("ALTER TABLE 123 SET LOCATION 'x'").is_none());
    }
}
