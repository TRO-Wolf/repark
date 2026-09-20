use std::collections::BTreeMap;

use datafusion::error::{DataFusionError, Result as EngineResult};
use datafusion::prelude::DataFrame;
use iceberg::spec::{Snapshot, TableMetadata};
use repark_common::{Error, Result};
use repark_iceberg::catalog::{AppendWindow, ChangelogWindow};

use super::{TimeTravelSpec, read_table_at};
use crate::Session;
use crate::error_map::engine_err;
use crate::idents::parse_table_identifier_segments;
use crate::illegal_argument_error;

pub const START_SNAPSHOT_ID: &str = "start-snapshot-id";
pub const END_SNAPSHOT_ID: &str = "end-snapshot-id";
pub const START_TIMESTAMP: &str = "start-timestamp";
pub const END_TIMESTAMP: &str = "end-timestamp";

pub const INCREMENTAL_OPTIONS: [&str; 4] = [
    START_SNAPSHOT_ID,
    END_SNAPSHOT_ID,
    START_TIMESTAMP,
    END_TIMESTAMP,
];

const TIME_TRAVEL_IN_INCREMENTAL: &str = "Cannot use time travel in incremental scan";
const LEGACY_SNAPSHOT_ID: &str = "Time travel option `snapshot-id` is no longer supported, use Spark built-in `versionAsOf` instead";
const TIMESTAMPS_ARE_CHANGELOG_ONLY: &str = "Only changelog scans support `start-timestamp` and `end-timestamp`. Use `start-snapshot-id` and `end-snapshot-id` for incremental scans.";
const END_WITHOUT_START: &str =
    "Cannot set only `end-snapshot-id` for incremental scans. Please, set `start-snapshot-id` too.";
const TIME_TRAVEL_IN_CHANGELOG: &str = "Can't time travel in changelog";
const BRANCH_IN_CHANGELOG: &str = "Cannot specify branch for changelogs";

pub const CHANGES_RELATION: &str = "changes";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IncrementalWindow {
    pub start_snapshot_id: Option<i64>,
    pub end_snapshot_id: Option<i64>,
    pub start_timestamp_ms: Option<i64>,
    pub end_timestamp_ms: Option<i64>,
}

impl IncrementalWindow {
    #[allow(clippy::missing_errors_doc)]
    pub fn from_options(options: &BTreeMap<String, String>) -> EngineResult<Option<Self>> {
        let mut window = Self::default();
        let mut seen = false;
        for (key, raw) in options {
            let lowered = key.to_ascii_lowercase();
            let slot = match lowered.as_str() {
                START_SNAPSHOT_ID => &mut window.start_snapshot_id,
                END_SNAPSHOT_ID => &mut window.end_snapshot_id,
                START_TIMESTAMP => &mut window.start_timestamp_ms,
                END_TIMESTAMP => &mut window.end_timestamp_ms,
                _ => continue,
            };
            *slot = Some(parse_long_option(&lowered, raw)?);
            seen = true;
        }
        Ok(seen.then_some(window))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn append_boundaries(&self) -> EngineResult<AppendWindow> {
        if self.start_timestamp_ms.is_some() || self.end_timestamp_ms.is_some() {
            return Err(illegal_argument_error(
                TIMESTAMPS_ARE_CHANGELOG_ONLY.to_string(),
            ));
        }
        if self.start_snapshot_id.is_none() {
            return Err(illegal_argument_error(END_WITHOUT_START.to_string()));
        }
        Ok(AppendWindow {
            from_exclusive: self.start_snapshot_id,
            to_inclusive: self.end_snapshot_id,
        })
    }
}

impl IncrementalWindow {
    #[allow(clippy::missing_errors_doc)]
    pub fn changelog_bounds(&self, metadata: &TableMetadata) -> EngineResult<ChangelogWindow> {
        if self.start_snapshot_id.is_some() && self.start_timestamp_ms.is_some() {
            return Err(illegal_argument_error(both_bounds(
                START_SNAPSHOT_ID,
                START_TIMESTAMP,
            )));
        }
        if self.end_snapshot_id.is_some() && self.end_timestamp_ms.is_some() {
            return Err(illegal_argument_error(both_bounds(
                END_SNAPSHOT_ID,
                END_TIMESTAMP,
            )));
        }
        if let (Some(start), Some(end)) = (self.start_timestamp_ms, self.end_timestamp_ms)
            && start >= end
        {
            return Err(illegal_argument_error(format!(
                "Cannot set {START_TIMESTAMP} to be greater than {END_TIMESTAMP} for changelogs"
            )));
        }
        let mut from_exclusive = self.start_snapshot_id;
        let mut to_inclusive = self.end_snapshot_id;
        if let Some(timestamp_ms) = self.start_timestamp_ms {
            if no_snapshots_after(metadata, timestamp_ms) {
                return Ok(EMPTY_CHANGELOG);
            }
            from_exclusive = start_snapshot_for_timestamp(metadata, timestamp_ms);
        }
        if let Some(timestamp_ms) = self.end_timestamp_ms {
            to_inclusive = end_snapshot_for_timestamp(metadata, timestamp_ms);
            if no_snapshots_between(from_exclusive, to_inclusive) {
                return Ok(EMPTY_CHANGELOG);
            }
        }
        Ok(ChangelogWindow {
            from_exclusive,
            to_inclusive,
            empty: false,
        })
    }
}

const EMPTY_CHANGELOG: ChangelogWindow = ChangelogWindow {
    from_exclusive: None,
    to_inclusive: None,
    empty: true,
};

fn both_bounds(first: &str, second: &str) -> String {
    format!("Cannot set both {first} and {second} for changelogs")
}

fn current_ancestors(metadata: &TableMetadata) -> Vec<&Snapshot> {
    let mut ancestors = Vec::new();
    let mut current = metadata.current_snapshot();
    while let Some(snapshot) = current {
        ancestors.push(snapshot.as_ref());
        current = snapshot
            .parent_snapshot_id()
            .and_then(|parent| metadata.snapshot_by_id(parent));
    }
    ancestors
}

fn no_snapshots_after(metadata: &TableMetadata, timestamp_ms: i64) -> bool {
    match metadata.current_snapshot() {
        None => true,
        Some(snapshot) => timestamp_ms > snapshot.timestamp_ms(),
    }
}

fn no_snapshots_between(from_exclusive: Option<i64>, to_inclusive: Option<i64>) -> bool {
    match (from_exclusive, to_inclusive) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some(from), to) => Some(from) == to,
    }
}

fn oldest_ancestor_after(metadata: &TableMetadata, timestamp_ms: i64) -> Option<&Snapshot> {
    let mut oldest_at_or_after = None;
    for snapshot in current_ancestors(metadata) {
        if snapshot.timestamp_ms() < timestamp_ms {
            return oldest_at_or_after;
        }
        if snapshot.timestamp_ms() == timestamp_ms {
            return Some(snapshot);
        }
        oldest_at_or_after = Some(snapshot);
    }
    oldest_at_or_after.filter(|snapshot| snapshot.parent_snapshot_id().is_none())
}

fn start_snapshot_for_timestamp(metadata: &TableMetadata, timestamp_ms: i64) -> Option<i64> {
    let snapshot = oldest_ancestor_after(metadata, timestamp_ms)?;
    if snapshot.timestamp_ms() == timestamp_ms {
        return Some(snapshot.snapshot_id());
    }
    snapshot.parent_snapshot_id()
}

fn end_snapshot_for_timestamp(metadata: &TableMetadata, timestamp_ms: i64) -> Option<i64> {
    current_ancestors(metadata)
        .into_iter()
        .find(|snapshot| snapshot.timestamp_ms() <= timestamp_ms)
        .map(Snapshot::snapshot_id)
}

fn parse_long_option(key: &str, raw: &str) -> EngineResult<i64> {
    raw.trim().parse::<i64>().map_err(|_| {
        illegal_argument_error(format!(
            "Cannot parse reader option `{key}` as a snapshot bound: {raw}"
        ))
    })
}

#[must_use]
pub fn is_legacy_snapshot_pin(key: &str) -> bool {
    let lowered = key.to_ascii_lowercase();
    lowered == "snapshot_id" || lowered == "snapshot-id"
}

#[must_use]
pub fn names_changelog_relation(parts: &[String]) -> bool {
    parts.len() >= 4
        && parts
            .last()
            .is_some_and(|last| last.eq_ignore_ascii_case(CHANGES_RELATION))
}

#[allow(clippy::missing_errors_doc)]
pub async fn read_incremental(
    session: &Session,
    table_name: &str,
    window: &BTreeMap<String, String>,
    travel: &BTreeMap<String, String>,
) -> Result<DataFrame> {
    let mut parts = parse_table_identifier_segments(table_name).map_err(|message| {
        Error::Analysis(format!(
            "read_iceberg_incremental: invalid table identifier: {message}"
        ))
    })?;
    let changelog = names_changelog_relation(&parts);
    let spec = if changelog {
        parts.pop();
        changelog_spec(window, travel).map_err(engine_err)?
    } else {
        incremental_spec(window, travel).map_err(engine_err)?
    };
    let catalogs = session.catalogs_snapshot();
    let zone = session.session_time_zone();
    read_table_at(session.context(), &catalogs, &parts, &spec, &zone)
        .await
        .map_err(engine_err)
}

fn changelog_spec(
    window: &BTreeMap<String, String>,
    travel: &BTreeMap<String, String>,
) -> EngineResult<TimeTravelSpec> {
    if travel.keys().any(|key| {
        let lowered = key.to_ascii_lowercase();
        lowered == "branch" || lowered == "tag"
    }) {
        return Err(illegal_argument_error(BRANCH_IN_CHANGELOG.to_string()));
    }
    if !travel.is_empty() {
        return Err(illegal_argument_error(TIME_TRAVEL_IN_CHANGELOG.to_string()));
    }
    Ok(TimeTravelSpec::Changelog(
        IncrementalWindow::from_options(window)?.unwrap_or_default(),
    ))
}

fn incremental_spec(
    window: &BTreeMap<String, String>,
    travel: &BTreeMap<String, String>,
) -> EngineResult<TimeTravelSpec> {
    if travel.keys().any(|key| !is_legacy_snapshot_pin(key)) {
        return Err(illegal_argument_error(
            TIME_TRAVEL_IN_INCREMENTAL.to_string(),
        ));
    }
    let parsed = IncrementalWindow::from_options(window)?.ok_or_else(|| {
        DataFusionError::Internal(
            "read_iceberg_incremental called without an incremental window".to_string(),
        )
    })?;
    let bounds = parsed.append_boundaries()?;
    if !travel.is_empty() {
        return Err(illegal_argument_error(LEGACY_SNAPSHOT_ID.to_string()));
    }
    Ok(TimeTravelSpec::Incremental {
        from: bounds.from_exclusive,
        to: bounds.to_inclusive,
    })
}

#[cfg(test)]
mod tests;
