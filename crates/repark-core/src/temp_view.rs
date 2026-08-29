//! Temp-view name resolution is centralized here because DataFusion has no session-local namespace.
//! The build-time home name and provider identity are snapshotted; qualified names and a replaced
//! provider refuse, preventing writes into catalogs. Every temp-view entry point uses this choke
//! point, while product reads emit the pinned home spelling rather than a live-default bare name.

use std::sync::Arc;

use datafusion::catalog::SchemaProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use repark_common::{Error, Result};

/// Where a session's temp views live: snapshotted ONCE at `ReparkSessionBuilder::build` from the
/// final config, never re-read at registration time.
#[derive(Debug, Clone)]
pub(crate) struct TempViewHome {
    pub(crate) catalog: String,
    pub(crate) schema: String,
    /// The schema provider that sat under `catalog.schema` at build time — the session-local
    /// in-memory schema DataFusion creates with the context. `None` when the build-time config
    /// left no such schema (`create_default_catalog_and_schema = false`), which is itself a
    /// refusal: there is nowhere session-local to put a temp view.
    pub(crate) provider: Option<Arc<dyn SchemaProvider>>,
}

/// Refuse when a catalog has replaced the provider captured as the session's temp-view home.
///
/// # Errors
/// [`Error::Analysis`] when the home schema is absent or is not the build-time provider.
pub(crate) fn assert_home_intact(context: &SessionContext, home: &TempViewHome) -> Result<()> {
    let live = context
        .catalog(&home.catalog)
        .and_then(|catalog| catalog.schema(&home.schema));
    match (&home.provider, &live) {
        (Some(built), Some(live)) if Arc::ptr_eq(built, live) => Ok(()),
        _ => Err(Error::Analysis(format!(
            "this session has no session-local temp-view home: '{}.{}' (the build-time \
             `datafusion.catalog.default_catalog` / `default_schema`) is not the session-local \
             schema the session was built with — a catalog was registered over it. A temporary \
             view is SESSION-LOCAL and is never created in a catalog or database, so the \
             temp-view API refuses rather than write that catalog. Build the session with a \
             `default_catalog` that no registered catalog shares a name with.",
            home.catalog, home.schema
        ))),
    }
}

/// Resolve an already-parsed segment without re-parsing quoted dots. Unquoted segments are folded
/// to match DataFusion's `TableReference::parse_str`; quoted segments retain their spelling.
pub(crate) fn temp_view_ref_from_segment(
    home: &TempViewHome,
    segment: &str,
    quoted: bool,
) -> TableReference {
    let table = if quoted {
        segment.to_string()
    } else {
        segment.to_ascii_lowercase()
    };
    TableReference::full(home.catalog.clone(), home.schema.clone(), table)
}

/// Resolve a caller's temp-view `name` against `home`.
///
/// Single-part names and the session's own home-qualified spelling resolve to the build-time home;
/// other qualified names refuse.
///
/// # Errors
/// [`Error::Analysis`] when `name` is neither a single-part identifier nor the session's own
/// home-qualified spelling.
pub(crate) fn temp_view_ref(home: &TempViewHome, name: &str) -> Result<TableReference> {
    let quoted = name.starts_with('"') || name.starts_with('`');
    match TableReference::parse_str(name) {
        TableReference::Bare { table } if quoted || !table.contains('.') => Ok(
            TableReference::full(home.catalog.clone(), home.schema.clone(), table),
        ),
        TableReference::Full {
            catalog,
            schema,
            table,
        } if *catalog == *home.catalog && *schema == *home.schema => Ok(TableReference::full(
            home.catalog.clone(),
            home.schema.clone(),
            table,
        )),
        _ => Err(Error::Analysis(format!(
            "temp view name '{name}' is qualified: a temporary view is SESSION-LOCAL and is \
             never created in a catalog or database — use a single-part name. (Writing a catalog \
             table is CREATE TABLE / CTAS, which goes through the pre-execute guards; the \
             temp-view API deliberately cannot.)"
        ))),
    }
}

#[cfg(test)]
mod tests;
