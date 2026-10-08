use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use datafusion::datasource::provider_as_source;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::{SchemaRef, TableMetadata};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_iceberg::catalog::uuid_presentation::presented_arrow_schema;
use repark_iceberg::microbatch::error::MicroBatchError;
use repark_iceberg::microbatch::offset::{FilePosition, InputOffset, SnapshotId, TableUuid};
use repark_iceberg::microbatch::provider::provider_for_plan;
use repark_iceberg::microbatch::window::{
    ReadCaps, StartPosition, WindowLimit, WindowPlan, WindowPlanner,
};

use crate::Session;
use crate::idents::parse_table_identifier_segments;

const MAX_FILES_KEY: &str = "streaming-max-files-per-micro-batch";
const MAX_ROWS_KEY: &str = "streaming-max-rows-per-micro-batch";
const FROM_TIMESTAMP_KEY: &str = "stream-from-timestamp";
const START_AFTER_SNAPSHOT_KEY: &str = "repark.cdc.start-after-snapshot-id";
const SKIP_OVERWRITE_KEY: &str = "streaming-skip-overwrite-snapshots";
const SKIP_DELETE_KEY: &str = "streaming-skip-delete-snapshots";
const UNSUPPORTED_SPARK_KEYS: [(&str, &str); 4] = [
    (
        "streaming-snapshot-polling-interval-ms",
        "the trigger interval governs polling",
    ),
    (
        "async-micro-batch-planning-enabled",
        "RePark plans each micro-batch synchronously in its trigger",
    ),
    (
        "async-queue-preload-file-limit",
        "it sizes the asynchronous planning queue, which RePark does not run",
    ),
    (
        "async-queue-preload-row-limit",
        "it sizes the asynchronous planning queue, which RePark does not run",
    ),
];
const STREAMING_PREFIX: &str = "streaming-";
const STREAM_PREFIX: &str = "stream-";
const REPARK_CDC_PREFIX: &str = "repark.cdc.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceOptions {
    pub caps: ReadCaps,
    pub start: StartPosition,
}

impl SourceOptions {
    #[allow(clippy::missing_errors_doc)]
    pub fn from_options(options: &BTreeMap<String, String>) -> Result<Self, MicroBatchError> {
        let mut caps = ReadCaps::default();
        let mut start = StartPosition::Earliest;
        let mut timestamp_seen = false;
        let mut snapshot_seen = false;
        for (folded, (key, value)) in fold_key_case(options)? {
            match folded.as_str() {
                SKIP_OVERWRITE_KEY | SKIP_DELETE_KEY => {
                    let option = if folded == SKIP_OVERWRITE_KEY {
                        SKIP_OVERWRITE_KEY
                    } else {
                        SKIP_DELETE_KEY
                    };
                    if parse_skip_flag(option, value)? {
                        return Err(MicroBatchError::SkipOptionRefused { option });
                    }
                }
                MAX_FILES_KEY => {
                    caps.max_files = Some(parse_file_cap(value)?);
                }
                MAX_ROWS_KEY => {
                    caps.max_rows = Some(parse_row_cap(value)?);
                }
                FROM_TIMESTAMP_KEY => {
                    start = StartPosition::FromTimestamp {
                        millis: parse_timestamp_millis(value)?,
                    };
                    timestamp_seen = true;
                }
                START_AFTER_SNAPSHOT_KEY => {
                    start = StartPosition::AfterSnapshot(parse_snapshot_id(value)?);
                    snapshot_seen = true;
                }
                _ if let Some((option, reason)) = UNSUPPORTED_SPARK_KEYS
                    .iter()
                    .find(|(option, _)| *option == folded) =>
                {
                    return Err(MicroBatchError::Catalog(format!(
                        "{option} is a Spark/Iceberg streaming option RePark does not support; {reason}"
                    )));
                }
                _ if has_interpreted_prefix(&folded) => {
                    return Err(MicroBatchError::UnknownOption {
                        key: key.to_string(),
                    });
                }
                _ => {}
            }
        }
        if timestamp_seen && snapshot_seen {
            return Err(MicroBatchError::Catalog(format!(
                "{FROM_TIMESTAMP_KEY} and {START_AFTER_SNAPSHOT_KEY} are both set; pass one start"
            )));
        }
        Ok(Self { caps, start })
    }
}

fn fold_key_case(
    options: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, (&str, &str)>, MicroBatchError> {
    let mut folded: BTreeMap<String, (&str, &str)> = BTreeMap::new();
    for (key, value) in options {
        if !key.is_ascii() && has_interpreted_prefix(&key.to_lowercase()) {
            return Err(MicroBatchError::Catalog(format!(
                "streaming option {key:?} names a streaming key only under Unicode case folding; option keys match ASCII case-insensitively, so spell it in ASCII"
            )));
        }
        let folded_key = key.to_ascii_lowercase();
        let boolean = matches!(folded_key.as_str(), SKIP_OVERWRITE_KEY | SKIP_DELETE_KEY);
        match folded.entry(folded_key) {
            Entry::Vacant(slot) => {
                slot.insert((key.as_str(), value.as_str()));
            }
            Entry::Occupied(slot) => {
                let (first, seen) = *slot.get();
                let same = seen == value.as_str()
                    || (boolean
                        && boolean_flag(seen)
                            .is_some_and(|flag| boolean_flag(value) == Some(flag)));
                if !same {
                    return Err(MicroBatchError::Catalog(format!(
                        "streaming options {first} and {key} are the same key in different case with different values; pass it once"
                    )));
                }
            }
        }
    }
    Ok(folded)
}

fn has_interpreted_prefix(key: &str) -> bool {
    key.starts_with(STREAMING_PREFIX)
        || key.starts_with(STREAM_PREFIX)
        || key.starts_with(REPARK_CDC_PREFIX)
}

fn boolean_flag(raw: &str) -> Option<bool> {
    if raw.eq_ignore_ascii_case("true") {
        Some(true)
    } else if raw.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

fn parse_skip_flag(option: &str, raw: &str) -> Result<bool, MicroBatchError> {
    boolean_flag(raw).ok_or_else(|| {
        MicroBatchError::Catalog(format!("{option} needs true or false, got {raw:?}"))
    })
}

fn parse_file_cap(raw: &str) -> Result<NonZeroUsize, MicroBatchError> {
    parse_int_cap(raw)
        .and_then(|cap| NonZeroUsize::try_from(cap).ok())
        .ok_or_else(|| cap_refusal(MAX_FILES_KEY, "file", raw))
}

fn parse_row_cap(raw: &str) -> Result<NonZeroU64, MicroBatchError> {
    parse_int_cap(raw)
        .map(NonZeroU64::from)
        .ok_or_else(|| cap_refusal(MAX_ROWS_KEY, "row", raw))
}

fn parse_int_cap(raw: &str) -> Option<NonZeroU32> {
    raw.parse::<i32>()
        .ok()
        .and_then(|cap| u32::try_from(cap).ok())
        .and_then(NonZeroU32::new)
}

fn cap_refusal(key: &str, noun: &str, raw: &str) -> MicroBatchError {
    MicroBatchError::Catalog(format!(
        "{key} needs a positive integer {noun} count no larger than {}, got {raw:?}",
        i32::MAX
    ))
}

fn parse_timestamp_millis(raw: &str) -> Result<i64, MicroBatchError> {
    raw.parse::<i64>().map_err(|_| {
        MicroBatchError::Catalog(format!(
            "{FROM_TIMESTAMP_KEY} needs integer milliseconds since the epoch, got {raw:?}"
        ))
    })
}

fn parse_snapshot_id(raw: &str) -> Result<SnapshotId, MicroBatchError> {
    raw.parse::<i64>().map(SnapshotId::new).map_err(|_| {
        MicroBatchError::Catalog(format!(
            "{START_AFTER_SNAPSHOT_KEY} needs an integer snapshot id, got {raw:?}"
        ))
    })
}

pub struct SourceBatch {
    pub start: InputOffset,
    pub end: InputOffset,
    pub frame: DataFrame,
    pub num_input_rows: u64,
}

#[derive(Clone)]
pub(crate) struct WeakSessionState(Arc<dyn Fn() -> Option<SessionState> + Send + Sync>);

impl WeakSessionState {
    pub(crate) fn of(context: &SessionContext) -> Self {
        let state = context.state_weak_ref();
        WeakSessionState(Arc::new(move || {
            state.upgrade().map(|state| state.read().clone())
        }))
    }

    pub(crate) fn snapshot(&self) -> Option<SessionState> {
        (self.0)()
    }
}

pub struct MicroBatchSource {
    context: WeakSessionState,
    catalog: Arc<dyn Catalog>,
    ident: TableIdent,
    name: String,
    uuid: TableUuid,
    caps: ReadCaps,
    start: StartPosition,
    read_schema: SchemaRef,
}

async fn load_table(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    name: &str,
) -> Result<Table, MicroBatchError> {
    catalog.load_table(ident).await.map_err(|error| {
        MicroBatchError::Catalog(format!(
            "microbatch source cannot open table {name:?}: {error}"
        ))
    })
}

impl MicroBatchSource {
    #[allow(clippy::missing_errors_doc)]
    pub async fn open(
        session: &Session,
        table: &str,
        options: SourceOptions,
    ) -> Result<Self, MicroBatchError> {
        let parts = parse_table_identifier_segments(table).map_err(|message| {
            MicroBatchError::Catalog(format!(
                "microbatch source table {table:?} is not a valid identifier: {message}"
            ))
        })?;
        let [catalog_name, namespace, table_name] = parts.as_slice() else {
            return Err(MicroBatchError::Catalog(format!(
                "microbatch source table {table:?} must be catalog.namespace.table"
            )));
        };
        let catalog = session
            .catalogs_snapshot()
            .get(catalog_name)
            .cloned()
            .ok_or_else(|| {
                MicroBatchError::Catalog(format!(
                    "microbatch source cannot open table {table:?}: catalog {catalog_name:?} is not registered"
                ))
            })?;
        let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table_name.clone());
        let opened = load_table(&catalog, &ident, table).await?;
        Ok(Self {
            context: WeakSessionState::of(session.context()),
            catalog,
            ident,
            name: table.to_string(),
            uuid: TableUuid::of(&opened),
            caps: options.caps,
            start: options.start,
            read_schema: opened.metadata().current_schema().clone(),
        })
    }

    async fn load(&self) -> Result<Table, MicroBatchError> {
        load_table(&self.catalog, &self.ident, &self.name).await
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn initial_offset(&self) -> Result<Option<InputOffset>, MicroBatchError> {
        let table = self.load().await?;
        WindowPlanner::new(table, self.caps)
            .named(self.name.clone())
            .initial_offset(&self.start)
            .await
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn table_uuid(&self) -> TableUuid {
        self.uuid
    }

    #[must_use]
    pub fn table_identifier(&self) -> String {
        self.ident.to_string()
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn arrow_schema(&self) -> Result<datafusion::arrow::datatypes::SchemaRef, MicroBatchError> {
        presented_arrow_schema(&self.read_schema)
            .map(Arc::new)
            .map_err(|error| {
                MicroBatchError::Catalog(format!(
                    "microbatch source {name} has a schema Arrow cannot present: {error}",
                    name = self.name
                ))
            })
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn next_batch(
        &self,
        from: &InputOffset,
        limit: WindowLimit,
    ) -> Result<Option<SourceBatch>, MicroBatchError> {
        let table = self.load().await?;
        let planner = WindowPlanner::new(table.clone(), self.caps).named(self.name.clone());
        let Some(plan) = planner.next_window(from, limit).await? else {
            return Ok(None);
        };
        self.batch_of(table, plan).map(Some)
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn available_now_target(
        &self,
        from: &InputOffset,
    ) -> Result<Option<InputOffset>, MicroBatchError> {
        let table = self.load().await?;
        let planner = WindowPlanner::new(table, self.caps).named(self.name.clone());
        Ok(planner
            .next_window(from, WindowLimit::Unbounded)
            .await?
            .map(|plan| plan.end))
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn next_batch_until(
        &self,
        from: &InputOffset,
        until: &InputOffset,
    ) -> Result<Option<SourceBatch>, MicroBatchError> {
        if same_position(from, until) {
            return Ok(None);
        }
        let table = self.load().await?;
        let planner = WindowPlanner::new(table.clone(), self.caps).named(self.name.clone());
        let Some(plan) = planner.next_window(from, WindowLimit::Capped).await? else {
            return Ok(None);
        };
        let Some(plan) = bounded(plan, table.metadata(), until) else {
            return Ok(None);
        };
        self.batch_of(table, plan).map(Some)
    }

    fn batch_of(&self, table: Table, plan: WindowPlan) -> Result<SourceBatch, MicroBatchError> {
        let end_snapshot = plan.end.snapshot.get();
        let provider = provider_for_plan(table, &plan, &self.read_schema).map_err(|error| {
            MicroBatchError::Catalog(format!(
                "microbatch source cannot read the batch ending at snapshot {end_snapshot}: {error}"
            ))
        })?;
        let state = self.context.snapshot().ok_or_else(|| {
            MicroBatchError::Catalog(format!(
                "microbatch source cannot read the batch ending at snapshot {end_snapshot}: the session ended"
            ))
        })?;
        let scan = LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(provider), None)
            .and_then(LogicalPlanBuilder::build)
            .map_err(|error| {
                MicroBatchError::Catalog(format!(
                    "microbatch source cannot read the batch ending at snapshot {end_snapshot}: {error}"
                ))
            })?;
        Ok(SourceBatch {
            start: plan.start,
            end: plan.end,
            frame: DataFrame::new(state, scan),
            num_input_rows: plan.num_input_rows,
        })
    }
}

#[must_use]
pub fn same_position(left: &InputOffset, right: &InputOffset) -> bool {
    left.table == right.table && left.snapshot == right.snapshot && left.position == right.position
}

fn bounded(plan: WindowPlan, metadata: &TableMetadata, until: &InputOffset) -> Option<WindowPlan> {
    let mut before: HashSet<i64> = HashSet::new();
    let mut cursor = metadata
        .snapshot_by_id(until.snapshot.get())
        .and_then(|snapshot| snapshot.parent_snapshot_id());
    while let Some(id) = cursor
        && before.len() < metadata.snapshots().len()
        && before.insert(id)
    {
        cursor = metadata
            .snapshot_by_id(id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
    }
    let files: Vec<_> = plan
        .files
        .into_iter()
        .take_while(|file| {
            if file.snapshot == until.snapshot {
                file.position.get() < until.position.get()
            } else {
                before.contains(&file.snapshot.get())
            }
        })
        .collect();
    let last = files.last()?;
    let end = InputOffset {
        table: plan.start.table,
        table_name: plan.start.table_name.clone(),
        snapshot: last.snapshot,
        position: FilePosition::new(last.position.get().saturating_add(1)),
    };
    let num_input_rows = files
        .iter()
        .fold(0u64, |rows, file| rows.saturating_add(file.record_count));
    Some(WindowPlan {
        start: plan.start,
        end,
        files,
        num_input_rows,
    })
}

#[cfg(test)]
#[path = "microbatch_source_tests.rs"]
mod tests;
