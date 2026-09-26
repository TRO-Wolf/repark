use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::TableMetadata;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergStaticTableProvider;
use repark_common::{Error, spark_error};
use repark_iceberg::catalog::{
    AppendWindow, ChangelogTableProvider, IncrementalAppendTableProvider,
};

use crate::SessionTimeZone;
use crate::catalog_state::CatalogRegistry;
use crate::illegal_argument_error;

pub mod incremental;
pub mod metadata_at;
mod sql_ast;
mod sql_eval;
mod sql_text;

pub use sql_eval::evaluate_sql_timestamp_asof;
pub use sql_text::{
    extract_timestamp_expr, format_snapshot_bound_ms, parse_timestamp_to_ms, parse_version_value,
};

static TEMP_VIEW_SEQ: AtomicU64 = AtomicU64::new(1);

#[allow(clippy::needless_pass_by_value)]
fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

fn incremental_window_refusal(error: DataFusionError) -> DataFusionError {
    let DataFusionError::External(inner) = &error else {
        return error;
    };
    let Some(iceberg_error) = inner.downcast_ref::<iceberg::Error>() else {
        return error;
    };
    if iceberg_error.kind() == iceberg::ErrorKind::DataInvalid {
        illegal_argument_error(iceberg_error.to_string())
    } else {
        error
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeTravelSpec {
    SnapshotId(i64),
    VersionRef(String),
    TimestampMs(i64),
    Incremental { from: Option<i64>, to: Option<i64> },
    Changelog(incremental::IncrementalWindow),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RefSelector {
    #[default]
    None,
    Branch,
    Tag,
}

impl RefSelector {
    #[must_use]
    pub fn from_table_parts(parts: &[String]) -> Self {
        let Some(last) = parts.last() else {
            return Self::None;
        };
        if parts.len() < 4 {
            return Self::None;
        }
        let lowered = last.to_ascii_lowercase();
        if lowered.starts_with("branch_") {
            Self::Branch
        } else if lowered.starts_with("tag_") {
            Self::Tag
        } else {
            Self::None
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn resolve_reader_spec(
    ctx: &SessionContext,
    opts: &ReaderTimeTravel,
    selector: RefSelector,
) -> Result<Option<TimeTravelSpec>> {
    let version = opts.version_as_of.as_deref().map(str::trim);
    let timestamp = opts.timestamp_as_of.as_deref().map(str::trim);
    if version.is_some() && timestamp.is_some() {
        return Err(DataFusionError::Plan(spark_error::message(
            spark_error::INVALID_TIME_TRAVEL_SPEC,
            &[],
        )));
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
    if (opts.branch.is_some() || opts.tag.is_some() || selector == RefSelector::Branch)
        && (version.is_some() || timestamp.is_some())
    {
        return Err(branch_time_travel_refusal());
    }
    if selector == RefSelector::Tag && (version.is_some() || timestamp.is_some()) {
        return Err(selector_time_travel_refusal());
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
        if let Ok(seconds) = raw.parse::<i64>() {
            let millis = seconds
                .checked_mul(1000)
                .ok_or_else(|| invalid_timestamp_input(raw))?;
            return Ok(Some(TimeTravelSpec::TimestampMs(millis)));
        }
        let millis = sql_eval::cast_string_to_timestamp_ms(ctx, raw).await?;
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
        TimeTravelSpec::Incremental { .. } | TimeTravelSpec::Changelog(_) => {
            Err(DataFusionError::Internal(
                "an incremental window has no single snapshot to resolve".to_string(),
            ))
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
    DataFusionError::Plan(spark_error::message(
        spark_error::INVALID_TIME_TRAVEL_TIMESTAMP_EXPR_INPUT,
        &[("display", display)],
    ))
}

#[must_use]
pub fn nondeterministic_timestamp_expr(display: &str) -> DataFusionError {
    DataFusionError::Plan(spark_error::message(
        spark_error::INVALID_TIME_TRAVEL_TIMESTAMP_EXPR_NON_DETERMINISTIC,
        &[("display", display)],
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
pub fn selector_time_travel_refusal() -> DataFusionError {
    illegal_argument_error(
        "Can't time travel using selector and Spark time travel spec at the same time".to_string(),
    )
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
    if let Some(frame) =
        metadata_at::read_metadata_path_at(ctx, catalogs, table_parts, spec, zone).await?
    {
        return Ok(frame);
    }
    let table = load_iceberg_table(catalogs, table_parts).await?;
    let provider: Arc<dyn TableProvider> = match spec {
        TimeTravelSpec::Incremental { from, to } => {
            let window = AppendWindow {
                from_exclusive: *from,
                to_inclusive: *to,
            };
            let provider = IncrementalAppendTableProvider::try_new(table, window)
                .map_err(incremental_window_refusal)?;
            Arc::new(provider)
        }
        TimeTravelSpec::Changelog(window) => {
            let bounds = window.changelog_bounds(table.metadata())?;
            Arc::new(ChangelogTableProvider::try_new(table, bounds)?)
        }
        pinned => {
            let snapshot_id = resolve_snapshot_id(table.metadata(), pinned, zone)?;
            if let TimeTravelSpec::VersionRef(name) = pinned {
                Arc::new(
                    IcebergStaticTableProvider::try_new_from_table_ref(table, name)
                        .await
                        .map_err(iceberg_err)?
                        .with_uuid_as_string(true),
                )
            } else {
                Arc::new(
                    IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
                        .await
                        .map_err(iceberg_err)?
                        .with_uuid_as_string(true),
                )
            }
        }
    };
    let temp_name = next_temp_view_name();
    let qualified = format!("datafusion.public.{temp_name}");
    let catalog = ctx.catalog("datafusion").ok_or_else(|| {
        DataFusionError::Plan(format!(
            "no session catalog `datafusion` for time-travel temp view (have {:?})",
            ctx.catalog_names()
        ))
    })?;
    let schema = catalog.schema("public").ok_or_else(|| {
        DataFusionError::Plan("no schema `datafusion.public` for time-travel temp view".to_string())
    })?;
    let _ = schema.deregister_table(&temp_name);
    schema
        .register_table(temp_name.clone(), provider)
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to register time-travel temp view {qualified}: {error}"
            ))
        })?;
    ctx.table(qualified.as_str()).await.map_err(|error| {
        DataFusionError::Plan(format!(
            "time-travel temp view {qualified} unresolved: {error}"
        ))
    })
}

#[must_use]
pub fn next_temp_view_name() -> String {
    let sequence = TEMP_VIEW_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("__repark_tt_{sequence}")
}

pub(crate) async fn load_iceberg_table(
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
