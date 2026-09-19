use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::TableMetadata;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergStaticTableProvider;
use repark_common::Error;

use crate::SessionTimeZone;
use crate::catalog_state::CatalogRegistry;
use crate::illegal_argument_error;

mod sql_ast;
mod sql_eval;
mod sql_text;

pub use sql_eval::evaluate_sql_timestamp_asof;
pub use sql_text::{
    extract_timestamp_expr, format_snapshot_bound_ms, parse_timestamp_asof_to_ms,
    parse_timestamp_string_to_ms, parse_timestamp_to_ms, parse_version_value,
};

static TEMP_VIEW_SEQ: AtomicU64 = AtomicU64::new(1);

#[allow(clippy::needless_pass_by_value)]
fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeTravelSpec {
    SnapshotId(i64),
    VersionRef(String),
    TimestampMs(i64),
}

#[derive(Debug, Clone, Default)]
pub struct TimeTravelOpts {
    pub snapshot_id: Option<i64>,
    pub as_of_timestamp_ms: Option<i64>,
    pub branch: Option<String>,
    pub tag: Option<String>,
}

impl TimeTravelOpts {
    #[allow(clippy::missing_errors_doc)]
    pub fn into_spec(self) -> repark_common::Result<Option<TimeTravelSpec>> {
        let mut set: Vec<(&str, TimeTravelSpec)> = Vec::new();
        if let Some(snapshot_id) = self.snapshot_id {
            set.push(("snapshot-id", TimeTravelSpec::SnapshotId(snapshot_id)));
        }
        if let Some(ms) = self.as_of_timestamp_ms {
            set.push(("as-of-timestamp", TimeTravelSpec::TimestampMs(ms)));
        }
        if let Some(branch) = self.branch {
            let trimmed = branch.trim();
            if trimmed.is_empty() {
                return Err(Error::Analysis(
                    "Iceberg reader option branch requires a non-empty branch name".to_string(),
                ));
            }
            set.push(("branch", TimeTravelSpec::VersionRef(trimmed.to_string())));
        }
        if let Some(tag) = self.tag {
            let trimmed = tag.trim();
            if trimmed.is_empty() {
                return Err(Error::Analysis(
                    "Iceberg reader option tag requires a non-empty tag name".to_string(),
                ));
            }
            set.push(("tag", TimeTravelSpec::VersionRef(trimmed.to_string())));
        }
        match set.len() {
            0 => Ok(None),
            1 => Ok(Some(set.remove(0).1)),
            _ => {
                let names: Vec<&str> = set.iter().map(|(name, _)| *name).collect();
                Err(Error::Analysis(format!(
                    "Iceberg time-travel reader options are mutually exclusive; got {}",
                    names.join(" and ")
                )))
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReaderTimeTravel {
    pub snapshot_id: Option<i64>,
    pub as_of_timestamp_ms: Option<i64>,
    pub branch: Option<String>,
    pub tag: Option<String>,
    pub version_as_of: Option<String>,
    pub timestamp_as_of: Option<String>,
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve_reader_spec(
    opts: &ReaderTimeTravel,
    zone: &SessionTimeZone,
    in_branch: bool,
) -> Result<Option<TimeTravelSpec>> {
    let version = opts.version_as_of.as_deref().map(str::trim);
    let timestamp = opts.timestamp_as_of.as_deref().map(str::trim);
    if version.is_some() && timestamp.is_some() {
        return Err(DataFusionError::Plan(
            "[INVALID_TIME_TRAVEL_SPEC] Cannot specify both version and timestamp when time travelling the table. SQLSTATE: 42K0E".to_string(),
        ));
    }
    if opts.snapshot_id.is_some() && (version.is_some() || timestamp.is_some()) {
        return Err(illegal_argument_error(
            "Time travel option `snapshot-id` is no longer supported, use Spark built-in `versionAsOf` instead"
                .to_string(),
        ));
    }
    if opts.as_of_timestamp_ms.is_some() && (version.is_some() || timestamp.is_some()) {
        return Err(illegal_argument_error(
            "Time travel option `as-of-timestamp` (in millis) is no longer supported, use Spark built-in `timestampAsOf` instead (properly formatted timestamp)"
                .to_string(),
        ));
    }
    if (opts.branch.is_some() || opts.tag.is_some() || in_branch)
        && (version.is_some() || timestamp.is_some())
    {
        return Err(illegal_argument_error(
            "Can't time travel in branch".to_string(),
        ));
    }
    if let Some(raw) = version {
        if raw.is_empty() {
            return Err(illegal_argument_error(
                "Cannot find matching snapshot ID or reference name for version ".to_string(),
            ));
        }
        if let Ok(snapshot_id) = raw.parse::<i64>() {
            return Ok(Some(TimeTravelSpec::SnapshotId(snapshot_id)));
        }
        return Ok(Some(TimeTravelSpec::VersionRef(raw.to_string())));
    }
    if let Some(raw) = timestamp {
        let Some(millis) = parse_timestamp_asof_to_ms(raw, zone) else {
            return Err(invalid_timestamp_input(raw));
        };
        return Ok(Some(TimeTravelSpec::TimestampMs(millis)));
    }
    let legacy = TimeTravelOpts {
        snapshot_id: opts.snapshot_id,
        as_of_timestamp_ms: opts.as_of_timestamp_ms,
        branch: opts.branch.clone(),
        tag: opts.tag.clone(),
    };
    match legacy.into_spec() {
        Ok(spec) => Ok(spec),
        Err(Error::Analysis(message)) => Err(DataFusionError::Plan(message)),
        Err(Error::IllegalArgument(message)) => Err(illegal_argument_error(message)),
        Err(other) => Err(DataFusionError::External(Box::new(other))),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve_snapshot_id(
    metadata: &TableMetadata,
    spec: &TimeTravelSpec,
    zone: &SessionTimeZone,
) -> Result<i64> {
    match spec {
        TimeTravelSpec::SnapshotId(snapshot_id) => metadata
            .snapshot_by_id(*snapshot_id)
            .map(|snapshot| snapshot.snapshot_id())
            .ok_or_else(|| {
                illegal_argument_error(format!("Cannot find snapshot with ID {snapshot_id}"))
            }),
        TimeTravelSpec::VersionRef(ref_name) => metadata
            .snapshot_for_ref(ref_name)
            .map(|snapshot| snapshot.snapshot_id())
            .ok_or_else(|| {
                illegal_argument_error(format!(
                    "Cannot find matching snapshot ID or reference name for version {ref_name}"
                ))
            }),
        TimeTravelSpec::TimestampMs(timestamp_ms) => {
            snapshot_id_as_of_time(metadata, *timestamp_ms).ok_or_else(|| {
                illegal_argument_error(format!(
                    "Cannot find a snapshot older than {}",
                    format_snapshot_bound_ms(*timestamp_ms, zone)
                ))
            })
        }
    }
}

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

#[must_use]
pub fn invalid_timestamp_input(display: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT] The time travel timestamp expression \"{display}\" is invalid. Cannot be casted to the \"TIMESTAMP\" type. SQLSTATE: 42K0E"
    ))
}

#[must_use]
pub fn nondeterministic_timestamp_expr(display: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.NON_DETERMINISTIC] The time travel timestamp expression \"{display}\" is invalid. Must be deterministic. SQLSTATE: 42K0E"
    ))
}

#[must_use]
pub fn invalid_version_pin() -> DataFusionError {
    DataFusionError::SQL(
        Box::new(
            datafusion::sql::sqlparser::parser::ParserError::ParserError(
                "Invalid time travel spec: version must be an integer snapshot id or a 'branch-or-tag' string literal."
                    .to_string(),
            ),
        ),
        None,
    )
}

#[must_use]
pub fn branch_time_travel_refusal() -> DataFusionError {
    illegal_argument_error("Can't time travel in branch".to_string())
}

#[must_use]
pub fn timestamp_column_refusal() -> DataFusionError {
    DataFusionError::SQL(
        Box::new(
            datafusion::sql::sqlparser::parser::ParserError::ParserError(
                "Invalid time travel spec: timestamp expression cannot refer to any columns."
                    .to_string(),
            ),
        ),
        None,
    )
}

#[allow(clippy::missing_errors_doc)]
pub async fn read_table_at(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_parts: &[String],
    spec: &TimeTravelSpec,
    zone: &SessionTimeZone,
) -> Result<DataFrame> {
    let snapshot_id = resolve_table_snapshot(catalogs, table_parts, spec, zone).await?;
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

#[must_use]
pub fn next_temp_view_name() -> String {
    let sequence = TEMP_VIEW_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("__repark_tt_{sequence}")
}

async fn resolve_table_snapshot(
    catalogs: &CatalogRegistry,
    table_parts: &[String],
    spec: &TimeTravelSpec,
    zone: &SessionTimeZone,
) -> Result<i64> {
    let table = load_iceberg_table(catalogs, table_parts).await?;
    resolve_snapshot_id(table.metadata(), spec, zone)
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
