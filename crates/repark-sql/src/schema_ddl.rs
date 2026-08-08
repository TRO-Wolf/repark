//! Catalog DDL: `CREATE SCHEMA … WITH (location = …)`, `DROP SCHEMA`, `DROP TABLE`.
//!
//! "Schema" is ANSI's word for what Spark calls a namespace or database and what Iceberg calls a
//! namespace — one concept, three names. This door speaks ANSI, so `CREATE SCHEMA` it is; the
//! wrong-door sniff catches the other two spellings and steers.
//!
//! These handlers reach the SAME catalog operations the Spark door uses — `create_namespace` /
//! `drop_namespace` / `drop_table` on the iceberg handle, plus the provider-invalidation helpers
//! in `repark-iceberg`. Sharing the operations (not the parsers) is exactly the delegate-first
//! shape: the grammars differ per door, the effects must not.
//!
//! `location` is the one schema property that matters, because a create with no table location
//! resolves through it. It is mirrored onto `location_uri` by
//! `repark_iceberg::catalog::mirror_namespace_location_keys` — unidirectional, never overwriting
//! an explicit key — so a real Glue database's canonical `locationUri` field is set whichever key
//! the catalog implementation maps.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::{Expr, ObjectName, SqlOption, Value};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext};

/// The curated `CREATE SCHEMA … WITH (…)` vocabulary.
const SCHEMA_KEYS: &[&str] = &["location"];

/// ===========================================================================================
/// `CREATE SCHEMA [IF NOT EXISTS] catalog.schema [WITH (location = '…')]`.
/// ===========================================================================================
///
/// # Errors
/// An unqualified name, an unknown catalog, an unknown `WITH` key, or an iceberg create failure.
pub(crate) async fn execute_create_schema(
    cx: &EngineContext<'_>,
    name: &ObjectName,
    if_not_exists: bool,
    with: Option<&Vec<SqlOption>>,
) -> Result<DataFrame> {
    let (catalog_name, namespace) = resolve_namespace(cx.catalogs, name, "CREATE SCHEMA")?;
    let handle = catalog_handle(cx.catalogs, &catalog_name)?;
    let mut properties = schema_properties(with.map_or(&[][..], Vec::as_slice))?;

    let ident = NamespaceIdent::from_strs(&namespace).map_err(iceberg_err)?;
    if if_not_exists && handle.namespace_exists(&ident).await.map_err(iceberg_err)? {
        return cx.ctx.read_empty();
    }
    repark_iceberg::catalog::mirror_namespace_location_keys(&mut properties);
    handle
        .create_namespace(&ident, properties)
        .await
        .map_err(iceberg_err)?;
    let leaf = namespace.last().cloned().unwrap_or_default();
    repark_iceberg::catalog::invalidate_catalog_namespaces(
        cx.ctx,
        Arc::clone(handle),
        &catalog_name,
        &[&leaf],
    )
    .await?;
    cx.ctx.read_empty()
}

/// ===========================================================================================
/// `DROP SCHEMA [IF EXISTS] catalog.schema` — `IF EXISTS` is idempotent.
///
/// `CASCADE` refuses: dropping a schema's tables as a side effect of a DDL statement is a
/// destructive operation this engine will not perform implicitly.
/// ===========================================================================================
///
/// # Errors
/// `CASCADE`, an unqualified name, an unknown catalog, or an iceberg drop failure.
pub(crate) async fn execute_drop_schema(
    cx: &EngineContext<'_>,
    names: &[ObjectName],
    if_exists: bool,
    cascade: bool,
) -> Result<DataFrame> {
    if cascade {
        return Err(DataFusionError::NotImplemented(
            "DROP SCHEMA … CASCADE is not supported — dropping tables as a side effect of a \
             schema drop is destructive and implicit. Drop the tables explicitly, then drop the \
             schema."
                .to_string(),
        ));
    }
    for name in names {
        let (catalog_name, namespace) = resolve_namespace(cx.catalogs, name, "DROP SCHEMA")?;
        let handle = catalog_handle(cx.catalogs, &catalog_name)?;
        let ident = NamespaceIdent::from_strs(&namespace).map_err(iceberg_err)?;
        if if_exists && !handle.namespace_exists(&ident).await.map_err(iceberg_err)? {
            continue;
        }
        handle.drop_namespace(&ident).await.map_err(iceberg_err)?;
        let leaf = namespace.last().cloned().unwrap_or_default();
        repark_iceberg::catalog::drop_catalog_namespace_from_provider(
            cx.ctx,
            Arc::clone(handle),
            &catalog_name,
            &leaf,
        )
        .await?;
    }
    cx.ctx.read_empty()
}

/// ===========================================================================================
/// `DROP TABLE [IF EXISTS] catalog.schema.table[, …]` — `IF EXISTS` is idempotent.
/// ===========================================================================================
///
/// # Errors
/// An unqualified name, an unknown catalog, or an iceberg drop failure.
pub(crate) async fn execute_drop_table(
    cx: &EngineContext<'_>,
    names: &[ObjectName],
    if_exists: bool,
) -> Result<DataFrame> {
    for name in names {
        let parts = name_parts(name);
        let [catalog_name, namespace @ .., table] = parts.as_slice() else {
            return Err(unqualified_error("DROP TABLE", &name.to_string()));
        };
        if namespace.is_empty() {
            return Err(unqualified_error("DROP TABLE", &name.to_string()));
        }
        let handle = catalog_handle(cx.catalogs, catalog_name)?;
        let ident = TableIdent::new(
            NamespaceIdent::from_strs(namespace).map_err(iceberg_err)?,
            table.clone(),
        );
        if if_exists && !handle.table_exists(&ident).await.map_err(iceberg_err)? {
            continue;
        }
        handle.drop_table(&ident).await.map_err(iceberg_err)?;
        let leaf = namespace.last().cloned().unwrap_or_default();
        repark_iceberg::catalog::invalidate_catalog_namespaces(
            cx.ctx,
            Arc::clone(handle),
            catalog_name,
            &[&leaf],
        )
        .await?;
    }
    cx.ctx.read_empty()
}

// === Shared helpers =========================================================================

/// Split a two-or-more-part `catalog.schema[.sub…]` name.
fn resolve_namespace(
    catalogs: &CatalogRegistry,
    name: &ObjectName,
    form: &str,
) -> Result<(String, Vec<String>)> {
    let parts = name_parts(name);
    let [catalog_name, namespace @ ..] = parts.as_slice() else {
        return Err(unqualified_error(form, &name.to_string()));
    };
    if namespace.is_empty() {
        return Err(unqualified_error(form, &name.to_string()));
    }
    // A read-only catalog is a real catalog with a specific answer, not an unknown name.
    if catalogs.is_read_only_catalog(catalog_name) && catalogs.get(catalog_name).is_none() {
        return Err(DataFusionError::Plan(
            crate::guards::read_only_catalog_message(catalog_name, form),
        ));
    }
    Ok((catalog_name.clone(), namespace.to_vec()))
}

/// The refusal for a name that does not lead with a catalog.
fn unqualified_error(form: &str, full_name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "{form}: `{full_name}` must be qualified with a registered Iceberg catalog — write \
         `<catalog>.<schema>` for a schema, `<catalog>.<schema>.<table>` for a table"
    ))
}

/// Look up a registered Iceberg catalog handle by name.
pub(crate) fn catalog_handle<'a>(
    catalogs: &'a CatalogRegistry,
    name: &str,
) -> Result<&'a Arc<dyn Catalog>> {
    if let Some(handle) = catalogs.get(name) {
        return Ok(handle);
    }
    if catalogs.is_read_only_catalog(name) {
        return Err(DataFusionError::Plan(
            crate::guards::read_only_catalog_message(name, "this operation"),
        ));
    }
    Err(DataFusionError::Plan(format!(
        "unknown catalog `{name}` — it is not registered on this session"
    )))
}

/// Validate the `CREATE SCHEMA … WITH (…)` options into namespace properties.
fn schema_properties(options: &[SqlOption]) -> Result<HashMap<String, String>> {
    let mut properties = HashMap::new();
    for option in options {
        let SqlOption::KeyValue { key, value } = option else {
            return Err(DataFusionError::NotImplemented(format!(
                "CREATE SCHEMA WITH: only `key = value` properties are supported (got `{option}`)"
            )));
        };
        let name = key.value.to_ascii_lowercase();
        if !SCHEMA_KEYS.contains(&name.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "CREATE SCHEMA WITH: unknown schema property `{}` (supported: {})",
                key.value,
                SCHEMA_KEYS
                    .iter()
                    .map(|key| format!("`{key}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        let Expr::Value(literal) = value else {
            return Err(DataFusionError::Plan(format!(
                "CREATE SCHEMA WITH: property `{name}` must be a string literal (got `{value}`)"
            )));
        };
        let text = match &literal.value {
            Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => text.clone(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "CREATE SCHEMA WITH: property `{name}` must be a string literal (got \
                     `{other}`)"
                )));
            }
        };
        if properties.insert(name.clone(), text).is_some() {
            return Err(DataFusionError::Plan(format!(
                "CREATE SCHEMA WITH: property `{name}` is specified more than once"
            )));
        }
    }
    Ok(properties)
}

/// The dotted identifier parts of an object name (`a.b.c` → `["a", "b", "c"]`).
pub(crate) fn name_parts(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect()
}

/// Reject identifier segments that could escape a warehouse root once composed into a path.
///
/// Needles come from `repark_iceberg::write::idents::path_escape_kind`, the workspace's single
/// source for what counts as traversal or a separator; empty segments are rejected here because
/// they are only meaningless at compose time.
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

/// Fold an iceberg error into the engine's one error type.
pub(crate) fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

#[cfg(test)]
mod tests;
