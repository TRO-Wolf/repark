use std::collections::BTreeMap;

use datafusion::error::{DataFusionError, Result as EngineResult};
use datafusion::prelude::DataFrame;
use repark_common::{Error, Result};
use repark_iceberg::catalog::AppendWindow;

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

#[allow(clippy::missing_errors_doc)]
pub async fn read_incremental(
    session: &Session,
    table_name: &str,
    window: &BTreeMap<String, String>,
    travel: &BTreeMap<String, String>,
) -> Result<DataFrame> {
    let parts = parse_table_identifier_segments(table_name).map_err(|message| {
        Error::Analysis(format!(
            "read_iceberg_incremental: invalid table identifier: {message}"
        ))
    })?;
    let spec = incremental_spec(window, travel).map_err(engine_err)?;
    let catalogs = session.catalogs_snapshot();
    let zone = session.session_time_zone();
    read_table_at(session.context(), &catalogs, &parts, &spec, &zone)
        .await
        .map_err(engine_err)
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
