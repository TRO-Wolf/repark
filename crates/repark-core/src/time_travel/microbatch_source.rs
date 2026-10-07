use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_iceberg::microbatch::error::MicroBatchError;
use repark_iceberg::microbatch::offset::{InputOffset, SnapshotId};
use repark_iceberg::microbatch::provider::provider_for_plan;
use repark_iceberg::microbatch::window::{ReadCaps, StartPosition, WindowLimit, WindowPlanner};

use crate::Session;
use crate::idents::parse_table_identifier_segments;

const MAX_FILES_KEY: &str = "streaming-max-files-per-micro-batch";
const MAX_ROWS_KEY: &str = "streaming-max-rows-per-micro-batch";
const FROM_TIMESTAMP_KEY: &str = "stream-from-timestamp";
const START_AFTER_SNAPSHOT_KEY: &str = "repark.cdc.start-after-snapshot-id";
const SKIP_OVERWRITE_KEY: &str = "streaming-skip-overwrite-snapshots";
const SKIP_DELETE_KEY: &str = "streaming-skip-delete-snapshots";
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
        match folded.entry(key.to_ascii_lowercase()) {
            Entry::Vacant(slot) => {
                slot.insert((key.as_str(), value.as_str()));
            }
            Entry::Occupied(slot) => {
                let (first, seen) = *slot.get();
                if seen != value.as_str() {
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

fn parse_skip_flag(option: &str, raw: &str) -> Result<bool, MicroBatchError> {
    if raw.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if raw.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(MicroBatchError::Catalog(format!(
            "{option} needs true or false, got {raw:?}"
        )))
    }
}

fn parse_file_cap(raw: &str) -> Result<NonZeroUsize, MicroBatchError> {
    raw.parse::<usize>()
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            MicroBatchError::Catalog(format!(
                "{MAX_FILES_KEY} needs a positive integer file count, got {raw:?}"
            ))
        })
}

fn parse_row_cap(raw: &str) -> Result<NonZeroU64, MicroBatchError> {
    raw.parse::<u64>()
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or_else(|| {
            MicroBatchError::Catalog(format!(
                "{MAX_ROWS_KEY} needs a positive integer row count, got {raw:?}"
            ))
        })
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

pub struct MicroBatchSource {
    context: SessionContext,
    catalog: Arc<dyn Catalog>,
    ident: TableIdent,
    name: String,
    caps: ReadCaps,
    start: StartPosition,
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
        let source = Self {
            context: session.context().clone(),
            catalog,
            ident: TableIdent::new(NamespaceIdent::new(namespace.clone()), table_name.clone()),
            name: table.to_string(),
            caps: options.caps,
            start: options.start,
        };
        source.load().await?;
        Ok(source)
    }

    async fn load(&self) -> Result<Table, MicroBatchError> {
        self.catalog.load_table(&self.ident).await.map_err(|error| {
            MicroBatchError::Catalog(format!(
                "microbatch source cannot open table {:?}: {error}",
                self.name
            ))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn initial_offset(&self) -> Result<Option<InputOffset>, MicroBatchError> {
        let table = self.load().await?;
        WindowPlanner::new(table, self.caps)
            .initial_offset(&self.start)
            .await
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn next_batch(
        &self,
        from: &InputOffset,
        limit: WindowLimit,
    ) -> Result<Option<SourceBatch>, MicroBatchError> {
        let table = self.load().await?;
        let planner = WindowPlanner::new(table.clone(), self.caps);
        let Some(plan) = planner.next_window(from, limit).await? else {
            return Ok(None);
        };
        let end_snapshot = plan.end.snapshot.get();
        let provider = provider_for_plan(table, &plan).map_err(|error| {
            MicroBatchError::Catalog(format!(
                "microbatch source cannot read the batch ending at snapshot {end_snapshot}: {error}"
            ))
        })?;
        let frame = self.context.read_table(provider).map_err(|error| {
            MicroBatchError::Catalog(format!(
                "microbatch source cannot read the batch ending at snapshot {end_snapshot}: {error}"
            ))
        })?;
        Ok(Some(SourceBatch {
            start: plan.start,
            end: plan.end,
            frame,
            num_input_rows: plan.num_input_rows,
        }))
    }
}

#[cfg(test)]
#[path = "microbatch_source_tests.rs"]
mod tests;
