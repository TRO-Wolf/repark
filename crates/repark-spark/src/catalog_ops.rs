//! Catalog lookup, P11 read-only DML refuse, re-register helpers, and error mapping.
//!
//! Extracted MOVE-ONLY from `lib.rs` (r25 T0 DataFusion-style reorg). Zero behavior change.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{FromTable, ObjectName};
use datafusion::sql::sqlparser::parser::ParserError;
use iceberg::{Catalog, NamespaceIdent};

use repark_core::CatalogRegistry;

use crate::normalize::object_name_from_table_with_joins;
use crate::spark_ast;

/// Reject identifier segments that could escape a warehouse root via `..` or path separators
/// when composed into `LocalFs` / object-store paths
/// (C2-SEC-003 / CALL table identity C1-SEC-001 / O3-C4-SEC-001 path-escape mirror).
///
/// Needles live in [`repark_iceberg::write::idents::path_escape_kind`] (r23 QI1 single-source); empty
/// segments are refused here at compose-time only.
pub(crate) fn reject_path_escape_ident(segment: &str, kind: &str) -> Result<()> {
    if segment.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{kind} identifier must not be empty"
        )));
    }
    match repark_iceberg::write::idents::path_escape_kind(segment) {
        Some(repark_iceberg::write::idents::PathEscapeKind::Traversal) => {
            Err(DataFusionError::Plan(format!(
                "{kind} identifier {segment:?} must not contain path traversal ('..')"
            )))
        }
        Some(repark_iceberg::write::idents::PathEscapeKind::Separator) => {
            Err(DataFusionError::Plan(format!(
                "{kind} identifier {segment:?} must not contain path separators"
            )))
        }
        None => Ok(()),
    }
}

/// Fold a sqlparser error into a plan-class [`DataFusionError`] (create-namespace parse errors join
/// the existing create-namespace / N5 errors as `Plan`, classified `AnalysisException` by WG-3).
// By-value to stay a clean `.map_err(sqlparser_err)` adapter (the `engine_err` pattern,
// lessons 2026-06-05: prefer `allow` over reshaping for a clippy-only lint).
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn sqlparser_err(err: ParserError) -> DataFusionError {
    DataFusionError::Plan(format!("could not parse CREATE NAMESPACE: {err}"))
}

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

// === r24 P7: catalog-provider rebuild path ====================================================
// PERF-07: product DDL invalidates the touched namespace only (O(1)); full rebuild is the
// explicit refresh / OOB escape hatch. Do not expand into SB1 DDL-gate or free-SQL cardinality.
// ==============================================================================================

/// Invalidate the DF catalog-provider name directory for `namespace` after a product DDL mutation.
///
/// O(1) listing cost via [`repark_iceberg::catalog::invalidate_catalog_namespaces`] (PERF-07). Prefer this
/// over [`reregister_catalog_provider`] for CREATE/DROP TABLE, CTAS, ALTER RENAME, etc.
pub(crate) async fn reregister(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    namespace: &str,
) -> Result<()> {
    reregister_namespaces(ctx, catalog, catalog_name, &[namespace]).await
}

/// Invalidate one or more namespaces (e.g. ALTER TABLE RENAME across namespaces).
pub(crate) async fn reregister_namespaces(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    namespaces: &[&str],
) -> Result<()> {
    repark_iceberg::catalog::invalidate_catalog_namespaces(ctx, catalog, catalog_name, namespaces)
        .await
}

/// Drop a namespace entry from the DF provider after product `DROP NAMESPACE` (no listing).
pub(crate) async fn reregister_drop_namespace(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    namespace: &str,
) -> Result<()> {
    repark_iceberg::catalog::drop_catalog_namespace_from_provider(
        ctx,
        catalog,
        catalog_name,
        namespace,
    )
    .await
}

/// Full provider rebuild — explicit session refresh / free-SQL OOB recovery (ADR-0004).
///
/// # Errors
/// Provider build / registration failures as [`DataFusionError`].
pub async fn reregister_catalog_provider(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    name: &str,
) -> Result<()> {
    // Full O(databases) path — session `refresh_catalog_provider` and test helpers only.
    repark_iceberg::catalog::rebuild_catalog_provider(ctx, catalog, name).await
}

/// Leaf schema name for a [`NamespaceIdent`] (top-level two-part model).
pub(crate) fn namespace_schema_name(namespace: &NamespaceIdent) -> String {
    namespace
        .as_ref()
        .last()
        .cloned()
        .unwrap_or_else(|| namespace.to_url_string())
}

/// Fold an iceberg error into a DataFusion error (the session layer carries one engine error type).
///
/// Hadoop-catalog metadata pointers (`vN.metadata.json`) register and read, but the fork's
/// [`iceberg::catalog::MetadataLocation`] parser cannot compute the next pointer from that
/// name — writes then fail with `Invalid metadata file name format`. That message names the
/// symptom. This wrapper names the convention and the shape that works, which is what V3-1
/// owns on the adopt path (registry `V3-ADOPT-1`).
pub(crate) fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    repark_iceberg::catalog::iceberg_to_datafusion(err)
}
