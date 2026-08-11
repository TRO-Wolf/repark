//! Iceberg time-travel PINS: spec parsing, snapshot resolution, and the reader-options path.
//!
//! Hoisted MOVE-ONLY (bodies byte-faithful at the port-source pin) from the v1 SQL crate's
//! `time_travel` module — the subset the SESSION owns: [`TimeTravelSpec`] + its parsers
//! ([`parse_version_value`], [`parse_timestamp_to_ms`]), snapshot resolution
//! ([`resolve_snapshot_id`], [`snapshot_id_as_of_time`]), and [`read_table_at`] (the
//! `read_iceberg_table` reader-options path — never a post-hoc filter; I1 / R-TIME-TRAVEL).
//!
//! The SQL-TEXT half of v1's module (`sql_has_time_travel` / `prepare_time_travel_sql` + the
//! token-level `VERSION AS OF` / `TIMESTAMP AS OF` span scan and FROM/JOIN splice) is DEFERRED
//! with the phase-2 statement router (design §5 deferred list) — v1 stays authoritative for it.
//!
//! Fork citations (pin `4723104b`):
//! - Static provider: `crates/integrations/datafusion/src/table/mod.rs:420`
//! - `snapshot_by_id` / `snapshot_for_ref` / `history`: `crates/iceberg/src/spec/table_metadata.rs:290-326`
//! - `snapshot_id_as_of_time` (`<=` semantics): `crates/iceberg/src/inspect/metadata_log_entries.rs:129-138`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::NaiveDateTime;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::TableMetadata;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergStaticTableProvider;

use crate::catalog_state::CatalogRegistry;

/// Process-wide counter so ephemeral temp-view names never collide across concurrent sessions.
/// The ONE counter behind [`next_temp_view_name`] — see that function for why there must not be
/// a second.
static TEMP_VIEW_SEQ: AtomicU64 = AtomicU64::new(1);

/// Fold an iceberg error into a DataFusion error (this module's contract is
/// `datafusion::error::Result`, like v1's SQL layer; the session folds further via `engine_err`).
/// Body identical to the v1 SQL crate's `iceberg_err` — hoisted alongside because the crate-level
/// `crate::iceberg_err` here is the SESSION fold (`iceberg::Error` → `repark_common::Error`),
/// a different type.
#[allow(clippy::needless_pass_by_value)]
fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

/// ===========================================================================================
/// A time-travel pin: snapshot id, named ref (branch/tag), or as-of timestamp (epoch ms).
/// ===========================================================================================
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeTravelSpec {
    /// Pin to a concrete snapshot id.
    SnapshotId(i64),
    /// Pin to a branch or tag name (Spark Iceberg `VERSION AS OF '<ref>'`).
    VersionRef(String),
    /// Pin to the latest snapshot with `timestamp_ms <=` this epoch-ms value.
    TimestampMs(i64),
}

/// ===========================================================================================
/// Resolve `spec` against `metadata` to a concrete snapshot id.
///
/// Snapshot id uses `snapshot_by_id` (unknown → loud, naming the id). Ref uses
/// `snapshot_for_ref` for branch/tag (unknown → loud, naming the ref). Timestamp walks
/// history with `timestamp_ms <= ts` (fork `snapshot_id_as_of_time`); earlier than the first
/// snapshot → loud error.
///
/// # Errors
/// Returns [`DataFusionError::Plan`] when the pin cannot be resolved.
/// ===========================================================================================
pub fn resolve_snapshot_id(metadata: &TableMetadata, spec: &TimeTravelSpec) -> Result<i64> {
    match spec {
        TimeTravelSpec::SnapshotId(snapshot_id) => metadata
            .snapshot_by_id(*snapshot_id)
            .map(|snapshot| snapshot.snapshot_id())
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "unknown Iceberg snapshot id {snapshot_id}: not found in table metadata"
                ))
            }),
        TimeTravelSpec::VersionRef(ref_name) => metadata
            .snapshot_for_ref(ref_name)
            .map(|snapshot| snapshot.snapshot_id())
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "unknown Iceberg snapshot ref {ref_name:?}: no branch or tag with that name"
                ))
            }),
        TimeTravelSpec::TimestampMs(timestamp_ms) => {
            snapshot_id_as_of_time(metadata, *timestamp_ms).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "no Iceberg snapshot at or before timestamp_ms={timestamp_ms} \
                     (as-of is earlier than the table's first snapshot)"
                ))
            })
        }
    }
}

/// Latest snapshot in `metadata.history()` with `timestamp_ms <= as_of_ms`.
///
/// Mirrors fork `snapshot_id_as_of_time`
/// (`crates/iceberg/src/inspect/metadata_log_entries.rs:129-138`, pin `4723104b`).
#[must_use]
pub fn snapshot_id_as_of_time(metadata: &TableMetadata, as_of_ms: i64) -> Option<i64> {
    let mut snapshot_id = None;
    for entry in metadata.history() {
        if entry.timestamp_ms <= as_of_ms {
            snapshot_id = Some(entry.snapshot_id);
        }
    }
    snapshot_id
}

/// ===========================================================================================
/// Parse a Spark Iceberg `VERSION AS OF` value into a [`TimeTravelSpec`].
///
/// Integer (or integer-shaped string) → snapshot id; otherwise → branch/tag ref name.
/// Decision (Spark Iceberg docs — "Time travel" / `VERSION AS OF`): a string that is not an
/// integer is treated as a branch or tag name.
///
/// # Errors
/// Empty string → plan error.
/// ===========================================================================================
pub fn parse_version_value(raw: &str) -> Result<TimeTravelSpec> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(DataFusionError::Plan(
            "VERSION AS OF requires a non-empty snapshot id or branch/tag name".to_string(),
        ));
    }
    if let Ok(snapshot_id) = trimmed.parse::<i64>() {
        return Ok(TimeTravelSpec::SnapshotId(snapshot_id));
    }
    Ok(TimeTravelSpec::VersionRef(trimmed.to_string()))
}

/// ===========================================================================================
/// Parse a Spark Iceberg `TIMESTAMP AS OF` / `as-of-timestamp` value into epoch milliseconds.
///
/// Accepts a bare integer (already epoch ms) or a UTC wall-clock string
/// (`YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, `YYYY-MM-DDTHH:MM:SS`, optional fractional seconds).
/// Session timezone is UTC in repark.
///
/// # Errors
/// Unparsable string → plan error naming the input.
/// ===========================================================================================
pub fn parse_timestamp_to_ms(raw: &str) -> Result<i64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(DataFusionError::Plan(
            "TIMESTAMP AS OF requires a non-empty timestamp".to_string(),
        ));
    }
    if let Ok(ms) = trimmed.parse::<i64>() {
        return Ok(ms);
    }
    // RFC 3339 / ISO-8601 with offset or `Z` (Spark jobs and JSON often emit these).
    // Without this arm, `…T00:00:00Z` failed loud while epoch-ms and naive UTC worked —
    // a shipping-path compatibility hole (octo C3-Q-001).
    if let Ok(offset_dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return Ok(offset_dt.timestamp_millis());
    }
    // `YYYY-MM-DD[ T]HH:MM:SS[.f]Z` (Zulu suffix on an otherwise-naive wall clock → UTC).
    let without_z = trimmed
        .strip_suffix('Z')
        .or_else(|| trimmed.strip_suffix('z'))
        .unwrap_or(trimmed);
    // Strip optional surrounding SQL TIMESTAMP keyword residue is handled at the token layer;
    // here we only see the string payload.
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ];
    for format in formats {
        if let Ok(naive) = NaiveDateTime::parse_from_str(without_z, format) {
            return Ok(naive.and_utc().timestamp_millis());
        }
        // Date-only: midnight UTC.
        if format == "%Y-%m-%d"
            && let Ok(date) = chrono::NaiveDate::parse_from_str(without_z, format)
            && let Some(naive) = date.and_hms_opt(0, 0, 0)
        {
            return Ok(naive.and_utc().timestamp_millis());
        }
    }
    Err(DataFusionError::Plan(format!(
        "cannot parse TIMESTAMP AS OF value {trimmed:?} \
         (expected epoch ms, RFC3339, or YYYY-MM-dd[ HH:MM:SS][Z])"
    )))
}

/// ===========================================================================================
/// Build a snapshot-pinned [`DataFrame`] for a three-part Iceberg table + [`TimeTravelSpec`].
///
/// **Two callers, with different correct dispositions for the registration it leaves behind:**
/// 1. the reader-options path (`session.rs`'s `read_iceberg_table` —
///    `spark.read.option("snapshot-id" | "as-of-timestamp" | "branch" | "tag", …)`), which KEEPS
///    it: the ephemeral view backs the frame handed to the user and there is no statement
///    boundary to release at (the documented residual, H-1b);
/// 2. the ANSI door (`repark_sql::time_travel::register_pinned_view`), which composes its own
///    `__repark_ansi_tt_<n>` view over the returned frame and RELEASES both names once the
///    statement is planned.
///
/// Caller 2 recovers the name minted here by reading it off the returned frame's logical plan
/// (`repark_sql::time_travel::core_pinned_name`: a bare `LogicalPlan::TableScan` whose table name
/// carries the `__repark_tt_` prefix). **That plan shape and that prefix are load-bearing at this
/// producing site**: wrapping the returned frame in another node here, or minting under a
/// different prefix, silently turns that recovery into a `None` and restores the leak — which is
/// why the pin that fences it (`repark-sql`'s
/// `tests/introspection.rs::time_travel_pinned_views_do_not_leak_into_the_introspection_surface`)
/// asserts on the broad `LIKE '__repark_tt%'` pattern rather than on a name.
/// ===========================================================================================
///
/// # Errors
/// Catalog / snapshot / provider errors as [`DataFusionError`].
pub async fn read_table_at(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_parts: &[String],
    spec: &TimeTravelSpec,
) -> Result<DataFrame> {
    let snapshot_id = resolve_table_snapshot(catalogs, table_parts, spec).await?;
    let table = load_iceberg_table(catalogs, table_parts).await?;
    let provider = IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
        .await
        .map_err(iceberg_err)?;
    let temp_name = next_temp_view_name();
    let _ = ctx.deregister_table(temp_name.as_str());
    ctx.register_table(temp_name.as_str(), Arc::new(provider))
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to register time-travel temp view {temp_name}: {error}"
            ))
        })?;
    ctx.table(temp_name.as_str()).await.map_err(|error| {
        DataFusionError::Plan(format!(
            "time-travel temp view {temp_name} unresolved: {error}"
        ))
    })
}

/// ===========================================================================================
/// Mint the next ephemeral `__repark_tt_<n>` temp-view name. **The only minter of that prefix.**
///
/// PUBLIC on purpose, and the reason is a correctness one rather than a convenience one: the
/// `__repark_tt_` namespace is SHARED across crates, on a single `SessionContext`, so two
/// independent counters do not merely produce untidy numbering — they produce the SAME name and
/// one registration silently destroys the other. Three call sites live in that namespace:
/// [`read_table_at`] below (the reader-options path, whose registration must SURVIVE — it backs
/// the `DataFrame` handed to the user), the Spark door's SQL rewrite (`repark_spark::time_travel`),
/// and the ANSI door by composition over [`read_table_at`] (`repark_sql::time_travel`).
///
/// Until H-1b (2026-08-11) the Spark door minted from a counter of its own, also starting at 1.
/// On a session that had used the reader-options path first, the door's mint step therefore
/// deregistered the reader's live view before registering its pinned provider under the same
/// name, and the post-planning `PinnedViews::release` then deleted that name outright — a
/// `spark.read.option("snapshot-id", …)` frame unregistered by an unrelated `VERSION AS OF`
/// statement. Minting HERE, from one process-global counter, makes the collision impossible by
/// construction. **Do not add a second counter**; the pin is
/// `repark-spark`'s `tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view`.
/// ===========================================================================================
#[must_use]
pub fn next_temp_view_name() -> String {
    let sequence = TEMP_VIEW_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("__repark_tt_{sequence}")
}

async fn resolve_table_snapshot(
    catalogs: &CatalogRegistry,
    table_parts: &[String],
    spec: &TimeTravelSpec,
) -> Result<i64> {
    let table = load_iceberg_table(catalogs, table_parts).await?;
    resolve_snapshot_id(table.metadata(), spec)
}

async fn load_iceberg_table(
    catalogs: &CatalogRegistry,
    table_parts: &[String],
) -> Result<iceberg::table::Table> {
    let (catalog_name, ident) = three_part_ident(table_parts)?;
    let catalog = catalogs.get(&catalog_name).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "catalog '{catalog_name}' is not registered — cannot time-travel table {}",
            table_parts.join(".")
        ))
    })?;
    catalog.load_table(&ident).await.map_err(iceberg_err)
}

fn three_part_ident(parts: &[String]) -> Result<(String, TableIdent)> {
    match parts {
        [catalog, namespace, table] => {
            let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
            Ok((catalog.clone(), ident))
        }
        _ => Err(DataFusionError::Plan(format!(
            "time travel requires a three-part catalog.namespace.table identifier, got `{}`",
            parts.join(".")
        ))),
    }
}

#[cfg(test)]
mod tests;
