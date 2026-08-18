//! The temp-view name choke point (SQM round 6, R6-1).
//!
//! DataFusion has no temp-view namespace. `SessionContext::register_table` takes a
//! `TableReference`, and a **bare** one resolves against the LIVE
//! `datafusion.catalog.default_catalog` / `default_schema`. Both halves of that are a hazard for
//! a Spark-shaped `createOrReplaceTempView`, which is SESSION-LOCAL by definition:
//!
//! 1. a qualified name (`createOrReplaceTempView("ice.sales.v")`) registered straight into the
//!    Iceberg catalog provider and **persisted a real table** — including a `tightenNulls`
//!    `required: true` payload, the exact thing the [`crate::pre_execute`] belt refuses on the
//!    SQL doors (MEASURED on BASE `68e98f4`; see `task/se1-declared-sorted-ledger.md` round 6);
//! 2. `SET datafusion.catalog.default_catalog = ice` moved a ONE-part registration into that
//!    same catalog (MEASURED — the registration left the session).
//!
//! So the session snapshots its temp-view home at build time ([`TempViewHome`]) and every
//! temp-view entry point turns a caller's name into a reference through [`temp_view_ref`].
//! Fixing it here — one function, one home — is what keeps the sibling paths (batch/plan
//! `create_or_replace_temp_view*`, `register_record_batches_as_temp_view`, materialize, cache,
//! `declare_temp_view_sorted`, `drop_temp_view`, one-part `table_exists`, `list_temp_view_names`)
//! from each needing their own guard.
//!
//! # The home NAME is not enough (round 6 critic S1)
//!
//! Snapshotting the configured default `catalog.schema` defends against a runtime `SET` — but
//! `datafusion.catalog.default_catalog` is also a first-class BUILD-time key
//! (`DATAFUSION_CONFIG_PREFIX`, `session.rs`), so a session built with `default_catalog = ice`
//! snapshots the home name `ice.sales`, and a later `register_memory_catalog("ice")` REPLACES
//! the provider that name points at with the Iceberg one. MEASURED on the name-only fix:
//! `register_record_batches_as_temp_view("vempty", <required schema>, vec![])` returned `Ok`,
//! `table_exists("ice.sales.vempty")` was `true`, and the persisted provider schema carried the
//! `required: true` tighten payload — the S1 leak, still open.
//!
//! So the home also snapshots the schema **provider handle**, and every entry point re-checks
//! that the live provider under the home name is still that same object
//! ([`assert_home_intact`]). It never is after a catalog took the name over — MEASURED:
//! `Arc::ptr_eq(before_register, after_register) == false` — so the temp-view API refuses loud
//! instead of writing a catalog. Identity, not a type check: it needs no downcast and it also
//! catches a plain re-registration of the default catalog.

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

/// Refuse when the session's temp-view home is no longer the session-local schema it was built
/// as — i.e. something (an Iceberg/memory catalog registered under the same name) replaced the
/// provider that `catalog.schema` resolves to. Without this, a session built with
/// `datafusion.catalog.default_catalog = <name of a catalog registered later>` would register
/// temp views straight INTO that catalog and persist real tables (round-6 critic S1, MEASURED).
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

/// The reference for an ALREADY-PARSED single segment — the quote-aware
/// `parse_table_identifier_segments` path (`table_exists`), whose segments arrive with quotes
/// already stripped and case NOT folded.
///
/// Re-feeding such a segment to [`temp_view_ref`] is wrong twice: a quoted dotted name
/// (`"a.b"` — an allowed one-identifier spelling) comes back as the bare string `a.b` and is
/// refused as "qualified", and an unquoted `MyView` would keep its case where BASE's
/// `table_exist("MyView")` folded it. So normalization is applied here to match
/// `TableReference::parse_str`: unquoted folds ASCII-lowercase, quoted is verbatim.
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
/// A **qualified** name refuses loud: a temp view is never created in a catalog. PySpark refuses
/// the same spellings with `AnalysisException` (a two-part name as a database prefix on a
/// temporary view, a three-part name as an invalid view name); repark mirrors the CLASS
/// ([`Error::Analysis`] → facade `AnalysisException`) with its own message text — the exact
/// PySpark strings are not measured here (no JVM in this tier; recorded NOT-RUN in the ledger).
///
/// A **single-part** name is pinned `Full` against `home`, so a later `SET` cannot move it.
///
/// The session's OWN home spelling (`<home.catalog>.<home.schema>.<view>`, quoted or not) names
/// the very same session-local view and is accepted as such (R7-1) — it is not a catalog write,
/// it is the home written out. Product READ paths need that spelling: a bare reference inside a
/// SQL body is re-resolved against the LIVE `datafusion.catalog.default_catalog`, so under a
/// `SET` to another catalog the facade's internal scratch views (`__repark_selx_*`,
/// `__repark_cache_*`, …) were minted in the home and then read in the other catalog — MEASURED
/// missing. Any OTHER qualified spelling still refuses.
///
/// The name is parsed by DataFusion's own `TableReference::parse_str` — the same parse
/// `register_table(&str)` did before this change — so identifier normalization (unquoted
/// lowercasing, quote stripping) is byte-identical to BASE.
///
/// # Errors
/// [`Error::Analysis`] when `name` is not a single-part identifier.
pub(crate) fn temp_view_ref(home: &TempViewHome, name: &str) -> Result<TableReference> {
    // MEASURED: `TableReference::parse_str` yields `Bare` for a FOUR-part name too — it falls
    // back to "the whole string is one identifier" past three parts (`a.b.c.d` →
    // `Bare { table: "a.b.c.d" }`). So `Bare` alone is not "single-part": an embedded dot in an
    // UNQUOTED name is still a qualified spelling and must refuse. A quoted name (`"a.b"`,
    // `` `a.b` ``) is genuinely one identifier that happens to contain a dot — that is allowed,
    // matching how `parse_table_identifier_segments` (C2-L-006) reads quotes elsewhere.
    let quoted = name.starts_with('"') || name.starts_with('`');
    match TableReference::parse_str(name) {
        TableReference::Bare { table } if quoted || !table.contains('.') => Ok(
            TableReference::full(home.catalog.clone(), home.schema.clone(), table),
        ),
        // R7-1: the home written out is still the home — same view, same registration.
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
