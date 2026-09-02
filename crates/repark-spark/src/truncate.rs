//! Spark-door `TRUNCATE TABLE`.

use std::sync::Arc;

use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{ObjectName, Truncate};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::catalog_ops::{
    iceberg_err, name_parts, namespace_schema_name, refuse_read_only_dml_table_sql,
    reject_path_escape_ident, reregister,
};
use crate::is_metadata_table_name;

/// Execute whole-table `TRUNCATE TABLE`.
///
/// # Errors
/// Unsupported shape, missing table, view target, metadata target, or commit failure.
pub(crate) async fn execute_truncate(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    truncate: &Truncate,
) -> Result<DataFrame> {
    refuse_unsupported_truncate_shape(truncate)?;
    let table_name = single_truncate_target(truncate)?;
    let table_sql = table_name.to_string();
    if let Some(message) = refuse_read_only_dml_table_sql(catalogs, &table_sql) {
        return Err(DataFusionError::Plan(message));
    }
    let parts = name_parts(table_name);
    for part in &parts {
        reject_path_escape_ident(part, "TRUNCATE TABLE")?;
    }
    if parts
        .last()
        .is_some_and(|suffix| is_metadata_table_name(suffix))
        && parts.len() >= 4
    {
        return Err(DataFusionError::Plan(format!(
            "Iceberg metadata table `{table_sql}` is read-only — INSERT/UPDATE/DELETE/MERGE/\
             CTAS/TRUNCATE/CREATE VIEW/DROP/ALTER targeting a metadata table is not supported"
        )));
    }
    match resolve_iceberg_truncate_target(ctx, catalogs, table_name).await? {
        Some((catalog_name, catalog, table, branch)) => {
            repark_iceberg::write::commit_truncate_to(&catalog, &table, branch.as_deref()).await?;
            let namespace = namespace_schema_name(table.identifier().namespace());
            reregister(ctx, catalog, &catalog_name, &namespace).await?;
            ctx.read_empty()
        }
        None => Err(missing_or_view_error(ctx, table_name).await),
    }
}

fn refuse_unsupported_truncate_shape(truncate: &Truncate) -> Result<()> {
    if truncate.partitions.is_some() {
        return Err(DataFusionError::Plan(
            "[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED] The partition \
             command is invalid. Table does not support partition management. SQLSTATE: 42601"
                .to_string(),
        ));
    }
    if truncate.if_exists {
        return Err(DataFusionError::Plan(
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'IF'. SQLSTATE: 42601".to_string(),
        ));
    }
    if !truncate.table {
        return Err(DataFusionError::Plan(
            "[PARSE_SYNTAX_ERROR] Syntax error at or near identifier: missing 'TABLE'. \
             SQLSTATE: 42601"
                .to_string(),
        ));
    }
    if truncate.identity.is_some() || truncate.cascade.is_some() || truncate.on_cluster.is_some() {
        return Err(DataFusionError::NotImplemented(
            "TRUNCATE TABLE identity/cascade/ON CLUSTER options are not supported".to_string(),
        ));
    }
    Ok(())
}

fn single_truncate_target(truncate: &Truncate) -> Result<&ObjectName> {
    let [target] = truncate.table_names.as_slice() else {
        return Err(DataFusionError::Plan(
            "TRUNCATE TABLE expects exactly one table name".to_string(),
        ));
    };
    if target.only || target.has_asterisk {
        return Err(DataFusionError::NotImplemented(
            "TRUNCATE TABLE ONLY / * descendant options are not supported".to_string(),
        ));
    }
    Ok(&target.name)
}

async fn resolve_iceberg_truncate_target(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_name: &ObjectName,
) -> Result<Option<(String, Arc<dyn Catalog>, Table, Option<String>)>> {
    let mut parts = name_parts(table_name);
    let branch = match crate::write_to_branch::split_write_ref_parts(&parts) {
        Some((table_parts, crate::write_to_branch::RefSelectorKind::Branch(name))) => {
            parts = table_parts;
            Some(name)
        }
        Some((_, crate::write_to_branch::RefSelectorKind::Tag)) => {
            return Err(crate::write_to_branch::tag_write_error("TRUNCATE"));
        }
        None => None,
    };
    parts = crate::write_to_branch::qualify_table_parts(ctx, parts);
    if parts.len() < 3 {
        return Ok(None);
    }
    let catalog_name = parts[0].clone();
    let table_leaf = parts[parts.len() - 1].clone();
    let namespace_parts = parts[1..parts.len() - 1].to_vec();
    let Ok(namespace) = NamespaceIdent::from_vec(namespace_parts) else {
        return Ok(None);
    };
    let Some(catalog) = catalogs.get(&catalog_name) else {
        return Ok(None);
    };
    let ident = TableIdent::new(namespace, table_leaf);
    match catalog.table_exists(&ident).await {
        Ok(true) => {
            let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
            Ok(Some((catalog_name, Arc::clone(catalog), table, branch)))
        }
        Ok(false) => Ok(None),
        Err(error) => Err(iceberg_err(error)),
    }
}

async fn missing_or_view_error(ctx: &SessionContext, table_name: &ObjectName) -> DataFusionError {
    let display = table_name.to_string();
    if let Ok(provider) = ctx.table_provider(display.as_str()).await {
        match provider.table_type() {
            TableType::View | TableType::Temporary => {
                return DataFusionError::Plan(format!(
                    "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'TRUNCATE TABLE' expects a table but \
                     `{display}` is a view. SQLSTATE: 42809"
                ));
            }
            TableType::Base => {}
        }
    }
    DataFusionError::Plan(format!(
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `{display}` cannot be found. Verify the \
         spelling and correctness of the schema and catalog."
    ))
}
