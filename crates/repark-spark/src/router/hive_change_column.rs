use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::DataType as SqlDataType;
use datafusion::sql::sqlparser::dialect::SparkSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use iceberg::spec::Type;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_functions::timestamp_type::spark_timestamp_type_from_options;
use repark_iceberg::write::alter::{SchemaChange, apply_schema_changes, starts_with_alter};

use crate::create_table::sql_type_to_iceberg_with_timestamp_type;
use crate::{catalog_handle, iceberg_err, name_parts, reregister};

#[derive(Debug)]
pub(crate) struct HiveChangeColumnDdl {
    table_parts: Vec<String>,
    old_name: String,
    new_name: String,
    data_type: SqlDataType,
    doc: Option<String>,
}

pub(crate) fn try_parse_hive_change_column_ddl(sql: &str) -> Option<Result<HiveChangeColumnDdl>> {
    if !starts_with_alter(sql) {
        return None;
    }
    let dialect = SparkSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(sql).ok()?;
    if !parser.parse_keywords(&[Keyword::ALTER, Keyword::TABLE]) {
        return None;
    }
    let table_name = parser.parse_object_name(false).ok()?;
    if !parser.parse_keyword(Keyword::CHANGE) {
        return None;
    }
    let table_parts = name_parts(&table_name);
    if table_parts.len() != 3 {
        return Some(Err(DataFusionError::Plan(format!(
            "ALTER TABLE expects a three-part `catalog.namespace.table` name, got `{}`",
            table_parts.join(".")
        ))));
    }
    let _ = parser.parse_keyword(Keyword::COLUMN);
    let old_name = match parser.parse_identifier() {
        Ok(ident) => ident.value,
        Err(error) => {
            return Some(Err(DataFusionError::Plan(format!(
                "ALTER TABLE CHANGE COLUMN expects the old and new column names: {error}"
            ))));
        }
    };
    let new_name = match parser.parse_identifier() {
        Ok(ident) => ident.value,
        Err(error) => {
            return Some(Err(DataFusionError::Plan(format!(
                "ALTER TABLE CHANGE COLUMN expects the old and new column names: {error}"
            ))));
        }
    };
    let data_type = match parser.parse_data_type() {
        Ok(data_type) => data_type,
        Err(error) => {
            return Some(Err(DataFusionError::Plan(format!(
                "ALTER TABLE CHANGE COLUMN `{old_name}` expects a column type: {error}"
            ))));
        }
    };
    let doc = if parser.parse_keyword(Keyword::COMMENT) {
        match parser.parse_literal_string() {
            Ok(literal) => Some(literal),
            Err(error) => {
                return Some(Err(DataFusionError::Plan(format!(
                    "ALTER TABLE CHANGE COLUMN `{old_name}` COMMENT expects a string literal: \
                     {error}"
                ))));
            }
        }
    } else {
        None
    };
    if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return Some(Err(DataFusionError::Plan(format!(
            "trailing tokens after ALTER TABLE CHANGE COLUMN `{old_name}` (starting at `{}`)",
            parser.peek_token().token
        ))));
    }
    Some(Ok(HiveChangeColumnDdl {
        table_parts,
        old_name,
        new_name,
        data_type,
        doc,
    }))
}

pub(crate) async fn execute_hive_change_column_ddl(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ddl: HiveChangeColumnDdl,
) -> Result<DataFrame> {
    let [catalog_name, namespace, table] = ddl.table_parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "ALTER TABLE expects a three-part `catalog.namespace.table` name, got `{}`",
            ddl.table_parts.join(".")
        )));
    };
    let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
    let handle = catalog_handle(catalogs, catalog_name)?;
    let timestamp_type = spark_timestamp_type_from_options(ctx.copied_config().options());
    let iceberg_type = sql_type_to_iceberg_with_timestamp_type(&ddl.data_type, timestamp_type)?;
    let Type::Primitive(new_type) = iceberg_type else {
        return Err(DataFusionError::NotImplemented(format!(
            "ALTER TABLE CHANGE COLUMN `{}` TYPE to a non-primitive is not supported",
            ddl.old_name
        )));
    };
    let mut changes = Vec::new();
    changes.push(SchemaChange::UpdateColumnType {
        name: ddl.old_name.clone(),
        new_type,
    });
    if let Some(doc) = ddl.doc {
        changes.push(SchemaChange::UpdateColumnDoc {
            name: ddl.old_name.clone(),
            doc: Some(doc),
        });
    }
    if ddl.old_name != ddl.new_name {
        changes.push(SchemaChange::RenameColumn {
            from: ddl.old_name.clone(),
            to: ddl.new_name.clone(),
        });
    }
    apply_schema_changes(handle.as_ref(), &ident, &changes)
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
    fn parse_hive_change_column_type_and_comment() {
        let ddl = try_parse_hive_change_column_ddl(
            "ALTER TABLE ice.sales.t CHANGE COLUMN id id BIGINT COMMENT 'hive-style'",
        )
        .expect("recognize")
        .expect("parse");
        assert_eq!(ddl.table_parts, vec!["ice", "sales", "t"]);
        assert_eq!(ddl.old_name, "id");
        assert_eq!(ddl.new_name, "id");
        assert_eq!(ddl.data_type, SqlDataType::BigInt(None));
        assert_eq!(ddl.doc.as_deref(), Some("hive-style"));
    }

    #[test]
    fn parse_hive_change_column_rename_without_comment() {
        let ddl = try_parse_hive_change_column_ddl(
            "ALTER TABLE ice.sales.t CHANGE COLUMN data payload STRING",
        )
        .expect("recognize")
        .expect("parse");
        assert_eq!(ddl.old_name, "data");
        assert_eq!(ddl.new_name, "payload");
        assert_eq!(ddl.doc, None);
    }

    #[test]
    fn parse_hive_change_column_leaves_other_alter_shapes_alone() {
        assert!(
            try_parse_hive_change_column_ddl("ALTER TABLE ice.sales.t ALTER COLUMN id TYPE BIGINT")
                .is_none()
        );
        assert!(
            try_parse_hive_change_column_ddl("ALTER TABLE ice.sales.t ADD COLUMN c STRING")
                .is_none()
        );
        assert!(try_parse_hive_change_column_ddl("SELECT 1").is_none());
        assert!(
            try_parse_hive_change_column_ddl(
                "ALTER TABLE ice.sales.t CHANGE COLUMN id id BIGINT EXTRA"
            )
            .expect("recognize")
            .is_err()
        );
    }
}
