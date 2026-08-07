//! Catalog lookup, P11 read-only DML refuse, and error mapping.
//!
//! Extracted MOVE-ONLY from the v1 SQL crate's `lib.rs` (r25 T0 DataFusion-style reorg). PR-2
//! PARTIAL rider: this port carries the subset the spine consumes. The v1 helpers serving the
//! PR-3a/PR-3b handler modules — `reject_path_escape_ident`, `sqlparser_err`, the r24 P7
//! `reregister*` provider-invalidation family, and `namespace_schema_name` — return verbatim
//! with those modules (ledger-declared).

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{FromTable, ObjectName};
use iceberg::Catalog;
use repark_core::CatalogRegistry;

use crate::normalize::object_name_from_table_with_joins;
use crate::spark_ast;

/// Look up a registered Iceberg catalog handle by name, or emit the P11 direction-note when
/// `name` is a known postgres (read-only) catalog on this registry snapshot.
pub(crate) fn catalog_handle<'a>(
    catalogs: &'a CatalogRegistry,
    name: &str,
) -> Result<&'a Arc<dyn Catalog>> {
    if let Some(handle) = catalogs.get(name) {
        return Ok(handle);
    }
    if catalogs.is_read_only_catalog(name) {
        return Err(DataFusionError::Plan(postgres_read_only_dml_message(name)));
    }
    Err(DataFusionError::Plan(format!("unknown catalog `{name}`")))
}

/// If `table_name` is a three-part `catalog.…` targeting a read-only catalog, return the P11
/// direction-note. Used for INSERT/UPDATE/DELETE passthrough paths that never hit
/// [`catalog_handle`].
pub(crate) fn refuse_read_only_dml_table_sql(
    catalogs: &CatalogRegistry,
    table_sql: &str,
) -> Option<String> {
    let catalog = table_sql.split('.').next()?.trim().trim_matches('"');
    if catalogs.is_read_only_catalog(catalog) {
        Some(postgres_read_only_dml_message(catalog))
    } else {
        None
    }
}

/// Run the plain-SQL passthrough unless a P11 read-only-catalog refusal message is present.
pub(crate) async fn passthrough_after_p11(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    refusal: Option<String>,
) -> Result<DataFrame> {
    if let Some(message) = refusal {
        return Err(DataFusionError::Plan(message));
    }
    spark_ast::execute_passthrough(ctx, catalogs, sql).await
}

pub(crate) fn refuse_read_only_dml_from_delete(
    catalogs: &CatalogRegistry,
    delete: &datafusion::sql::sqlparser::ast::Delete,
) -> Option<String> {
    for name in &delete.tables {
        if let Some(message) = refuse_read_only_dml_table_sql(catalogs, &name.to_string()) {
            return Some(message);
        }
    }
    let from_tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    for table in from_tables {
        // Prefer bare ObjectName so aliases cannot bypass the P11 catalog check.
        let table_sql = object_name_from_table_with_joins(table)
            .map_or_else(|| table.to_string(), ToString::to_string);
        if let Some(message) = refuse_read_only_dml_table_sql(catalogs, &table_sql) {
            return Some(message);
        }
    }
    None
}

/// P11 direction-note for DML targeting a postgres catalog (pinned by PG3 tests).
#[must_use]
pub fn postgres_read_only_dml_message(catalog_name: &str) -> String {
    format!(
        "catalog '{catalog_name}' is not an Iceberg catalog; postgres catalogs are read-only in v1 — \
         supported direction is MERGE INTO <iceberg> USING {catalog_name}.…"
    )
}

/// Resolve a two-part `catalog.namespace` object name.
pub(crate) fn resolve_namespace(name: &ObjectName) -> Result<(String, String)> {
    let parts = name_parts(name);
    let [catalog, namespace] = parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "expected a two-part `catalog.namespace` name, got `{name}`"
        )));
    };
    Ok((catalog.clone(), namespace.clone()))
}

/// The dotted identifier parts of an object name (`a.b.c` → `["a", "b", "c"]`).
pub(crate) fn name_parts(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect()
}

/// Fold an iceberg error into a DataFusion error (the session layer carries one engine error type).
pub(crate) fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
