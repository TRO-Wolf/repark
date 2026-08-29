//! Iceberg time-travel parsing, snapshot resolution, and reader-option support.
//!
//! SQL-text rewriting remains with the phase-2 statement router. The reader path registers a
//! snapshot-pinned static provider and preserves its plan shape and name prefix for cleanup.

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

/// Fold an Iceberg error into the DataFusion error type used by this module.
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
/// Build a snapshot-pinned [`DataFrame`] for a three-part Iceberg table and specification.
///
/// The reader-options caller keeps the registered provider; SQL callers recover its name from
/// the bare `TableScan` plan and release it after planning. The `__repark_tt_` prefix and shape
/// are therefore part of the cross-crate cleanup contract.
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
/// Mint the next ephemeral `__repark_tt_<n>` temp-view name.
///
/// All crates share this process-wide counter. A second counter could reuse a live name and
/// unregister another caller's provider.
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
