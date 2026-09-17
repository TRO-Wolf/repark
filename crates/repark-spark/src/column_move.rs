use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::parser::ParserError;
use repark_core::CatalogRegistry;
use repark_iceberg::write::alter::{
    ColumnPosition, SchemaChange, apply_schema_changes_on_table, resolve_move_names,
    starts_with_alter,
};

use crate::alter::{
    Sig, collect_name_parts, is_period_at, render_sig_at, table_parts_to_ident,
    tokenize_significant, word_at, word_eq,
};
use crate::{catalog_handle, iceberg_err, reregister};

#[derive(Debug)]
pub(crate) struct ColumnMoveDdl {
    table_parts: Vec<String>,
    name: String,
    position: ColumnPosition,
}

pub(crate) fn try_parse_column_move_ddl(sql: &str) -> Option<Result<ColumnMoveDdl>> {
    if !starts_with_alter(sql) {
        return None;
    }
    let significant = tokenize_significant(sql)?;
    if significant.len() < 7 {
        return None;
    }
    if !(word_eq(&significant, 0, "ALTER") && word_eq(&significant, 1, "TABLE")) {
        return None;
    }
    let mut index = 2usize;
    word_at(&significant, index)?;
    let table_start = index;
    index += 1;
    while is_period_at(&significant, index) && word_at(&significant, index + 1).is_some() {
        index += 2;
    }
    let table_parts = collect_name_parts(&significant, table_start, index)?;
    if table_parts.len() != 3 {
        return Some(Err(DataFusionError::Plan(format!(
            "ALTER TABLE expects a three-part `catalog.namespace.table` name, got `{}`",
            table_parts.join(".")
        ))));
    }
    if !(word_eq(&significant, index, "ALTER") && word_eq(&significant, index + 1, "COLUMN")) {
        return None;
    }
    index += 2;
    let (name, next) = parse_column_path(&significant, index)?;
    if word_eq(&significant, next, "FIRST") {
        if next + 1 < significant.len() {
            return Some(Err(DataFusionError::Plan(format!(
                "trailing tokens after ALTER COLUMN `{name}` FIRST (starting at `{}`)",
                render_sig_at(&significant, next + 1)
            ))));
        }
        return Some(Ok(ColumnMoveDdl {
            table_parts,
            name,
            position: ColumnPosition::First,
        }));
    }
    if word_eq(&significant, next, "AFTER") {
        if is_period_at(&significant, next + 2) {
            return Some(Err(DataFusionError::SQL(
                Box::new(ParserError::ParserError(
                    "[PARSE_SYNTAX_ERROR] Syntax error at or near '.'. SQLSTATE: 42601".to_string(),
                )),
                None,
            )));
        }
        let (reference, after) = parse_column_path(&significant, next + 1)?;
        if after < significant.len() {
            return Some(Err(DataFusionError::Plan(format!(
                "trailing tokens after ALTER COLUMN `{name}` AFTER `{reference}` (starting at `{}`)",
                render_sig_at(&significant, after)
            ))));
        }
        return Some(Ok(ColumnMoveDdl {
            table_parts,
            name,
            position: ColumnPosition::After(reference),
        }));
    }
    None
}

fn parse_column_path(significant: &[Sig], start: usize) -> Option<(String, usize)> {
    let first = word_at(significant, start)?;
    let mut parts = vec![first.to_string()];
    let mut index = start + 1;
    while is_period_at(significant, index) {
        let part = word_at(significant, index + 1)?;
        parts.push(part.to_string());
        index += 2;
    }
    Some((parts.join("."), index))
}

pub(crate) async fn execute_column_move_ddl(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ddl: ColumnMoveDdl,
) -> Result<DataFrame> {
    let (catalog_name, ident) = table_parts_to_ident(&ddl.table_parts)?;
    let handle = catalog_handle(catalogs, &catalog_name)?;
    let table = handle.load_table(&ident).await.map_err(iceberg_err)?;
    let (mover, at) = resolve_move_names(
        table.metadata().current_schema(),
        &[],
        &ddl.name,
        &ddl.position,
    )
    .map_err(DataFusionError::Plan)?;
    apply_schema_changes_on_table(
        handle.as_ref(),
        &table,
        &[SchemaChange::MoveColumn {
            name: mover,
            position: at,
        }],
    )
    .await
    .map_err(iceberg_err)?;
    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, handle.clone(), &catalog_name, &namespace).await?;
    ctx.read_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alter::refuse_unsupported_alter_sql;

    #[test]
    fn parse_column_move_first_after_and_nested() {
        let first = try_parse_column_move_ddl("ALTER TABLE ice.sales.t ALTER COLUMN b FIRST")
            .expect("recognize")
            .expect("parse");
        assert_eq!(first.table_parts, vec!["ice", "sales", "t"]);
        assert_eq!(first.name, "b");
        assert_eq!(first.position, ColumnPosition::First);
        let after = try_parse_column_move_ddl("ALTER TABLE ice.sales.t ALTER COLUMN b AFTER id")
            .expect("recognize")
            .expect("parse");
        assert_eq!(after.name, "b");
        assert_eq!(after.position, ColumnPosition::After("id".to_string()));
        let nested = try_parse_column_move_ddl("ALTER TABLE ice.sales.t ALTER COLUMN s.b FIRST")
            .expect("recognize")
            .expect("parse");
        assert_eq!(nested.name, "s.b");
        assert_eq!(nested.position, ColumnPosition::First);
    }

    #[test]
    fn parse_column_move_leaves_other_alter_forms_alone() {
        assert!(
            try_parse_column_move_ddl("ALTER TABLE ice.sales.t ALTER COLUMN n TYPE BIGINT")
                .is_none()
        );
        assert!(
            try_parse_column_move_ddl("ALTER TABLE ice.sales.t ADD COLUMN c STRING AFTER id")
                .is_none()
        );
        assert!(
            try_parse_column_move_ddl("ALTER TABLE ice.sales.t ALTER COLUMN b FIRST EXTRA")
                .expect("recognize")
                .is_err()
        );
        assert!(
            refuse_unsupported_alter_sql("ALTER TABLE ice.sales.t ALTER COLUMN b FIRST").is_none(),
            "a parsed move must not hit the residual refuse path"
        );
    }

    #[test]
    fn parse_column_move_refuses_dotted_after_reference() {
        let refused =
            try_parse_column_move_ddl("ALTER TABLE ice.sales.t ALTER COLUMN s.b AFTER s.a")
                .expect("recognize")
                .expect_err("a dotted AFTER reference must refuse");
        let message = refused.to_string();
        assert!(
            message.contains("[PARSE_SYNTAX_ERROR]")
                && message.contains("at or near '.'")
                && message.contains("42601"),
            "a dotted AFTER reference must refuse Spark-shaped, got: {message}"
        );
        assert!(matches!(refused, DataFusionError::SQL(_, _)));
    }

    #[test]
    fn parse_column_move_skips_non_alter_without_tokenizing() {
        assert!(try_parse_column_move_ddl("SELECT * FROM ice.sales.t").is_none());
        assert!(try_parse_column_move_ddl("-- lead\nSELECT 1").is_none());
        assert!(
            try_parse_column_move_ddl("ALTER TABLE ice.sales.t ALTER COLUMN s.b AFTER a")
                .expect("recognize")
                .expect("parse")
                .position
                == ColumnPosition::After("a".to_string())
        );
    }
}
