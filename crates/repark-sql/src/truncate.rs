//! ANSI-door `TRUNCATE TABLE`.

use std::sync::Arc;

use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::{ObjectName, Truncate};
use iceberg::inspect::MetadataTableType;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::EngineContext;

use crate::schema_ddl::{catalog_handle, iceberg_err, name_parts, reject_path_escape_ident};

/// Execute whole-table `TRUNCATE TABLE`.
///
/// # Errors
/// Unsupported shape, missing table, view target, metadata target, or commit failure.
pub(crate) async fn execute_truncate(
    cx: &EngineContext<'_>,
    truncate: &Truncate,
) -> Result<DataFrame> {
    refuse_unsupported_truncate_shape(truncate)?;
    let table_name = single_truncate_target(truncate)?;
    let table_sql = table_name.to_string();
    let parts = name_parts(table_name);
    for part in &parts {
        reject_path_escape_ident(part, "TRUNCATE TABLE")?;
    }
    if let Some(catalog_name) = parts.first()
        && cx.catalogs.is_read_only_catalog(catalog_name)
    {
        return Err(DataFusionError::Plan(
            crate::guards::read_only_catalog_message(catalog_name, "TRUNCATE TABLE"),
        ));
    }
    if parts.last().is_some_and(|suffix| {
        MetadataTableType::try_from(suffix.to_ascii_lowercase().as_str()).is_ok()
    }) && parts.len() >= 4
    {
        return Err(DataFusionError::Plan(format!(
            "Iceberg metadata table `{table_sql}` is read-only — INSERT/UPDATE/DELETE/MERGE/\
             CTAS/TRUNCATE/CREATE VIEW/DROP/ALTER targeting a metadata table is not supported"
        )));
    }
    match resolve_iceberg_truncate_target(cx, table_name).await? {
        Some((catalog_name, catalog, table)) => {
            repark_iceberg::write::commit_truncate(&catalog, &table).await?;
            let leaf = table
                .identifier()
                .namespace()
                .as_ref()
                .last()
                .cloned()
                .unwrap_or_default();
            repark_iceberg::catalog::invalidate_catalog_namespaces(
                cx.ctx,
                catalog,
                &catalog_name,
                &[&leaf],
            )
            .await?;
            cx.ctx.read_empty()
        }
        None => Err(missing_or_view_error(cx, table_name).await),
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
    cx: &EngineContext<'_>,
    table_name: &ObjectName,
) -> Result<Option<(String, Arc<dyn Catalog>, Table)>> {
    let parts = name_parts(table_name);
    if parts.len() < 3 {
        return Ok(None);
    }
    let catalog_name = parts[0].clone();
    let table_leaf = parts[parts.len() - 1].clone();
    let namespace_parts = parts[1..parts.len() - 1].to_vec();
    let Ok(namespace) = NamespaceIdent::from_vec(namespace_parts) else {
        return Ok(None);
    };
    let Ok(catalog) = catalog_handle(cx.catalogs, &catalog_name) else {
        return Ok(None);
    };
    let ident = TableIdent::new(namespace, table_leaf);
    match catalog.table_exists(&ident).await {
        Ok(true) => {
            let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
            Ok(Some((catalog_name, Arc::clone(catalog), table)))
        }
        Ok(false) => Ok(None),
        Err(error) => Err(iceberg_err(error)),
    }
}

async fn missing_or_view_error(cx: &EngineContext<'_>, table_name: &ObjectName) -> DataFusionError {
    let display = table_name.to_string();
    if let Ok(provider) = cx.ctx.table_provider(display.as_str()).await {
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
