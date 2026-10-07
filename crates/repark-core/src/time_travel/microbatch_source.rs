use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};

use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::table::Table;
use repark_iceberg::microbatch::error::MicroBatchError;
use repark_iceberg::microbatch::offset::{InputOffset, SnapshotId};
use repark_iceberg::microbatch::provider::provider_for_plan;
use repark_iceberg::microbatch::window::{ReadCaps, StartPosition, WindowLimit, WindowPlanner};

use super::load_iceberg_table;
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
        for (key, value) in options {
            match key.as_str() {
                SKIP_OVERWRITE_KEY | SKIP_DELETE_KEY => {
                    let option = if key.as_str() == SKIP_OVERWRITE_KEY {
                        SKIP_OVERWRITE_KEY
                    } else {
                        SKIP_DELETE_KEY
                    };
                    return Err(MicroBatchError::SkipOptionRefused { option });
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
                _ if has_interpreted_prefix(key) => {
                    return Err(MicroBatchError::UnknownOption { key: key.clone() });
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

fn has_interpreted_prefix(key: &str) -> bool {
    key.starts_with(STREAMING_PREFIX)
        || key.starts_with(STREAM_PREFIX)
        || key.starts_with(REPARK_CDC_PREFIX)
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
    table: Table,
    planner: WindowPlanner,
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
        if parts.len() != 3 {
            return Err(MicroBatchError::Catalog(format!(
                "microbatch source table {table:?} must be catalog.namespace.table"
            )));
        }
        let catalogs = session.catalogs_snapshot();
        let iceberg_table = load_iceberg_table(&catalogs, &parts)
            .await
            .map_err(|error| {
                MicroBatchError::Catalog(format!(
                    "microbatch source cannot open table {table:?}: {error}"
                ))
            })?;
        let planner = WindowPlanner::new(iceberg_table.clone(), options.caps);
        Ok(Self {
            context: session.context().clone(),
            table: iceberg_table,
            planner,
            start: options.start,
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn initial_offset(&self) -> Result<Option<InputOffset>, MicroBatchError> {
        self.planner.initial_offset(&self.start).await
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn next_batch(
        &self,
        from: &InputOffset,
        limit: WindowLimit,
    ) -> Result<Option<SourceBatch>, MicroBatchError> {
        let Some(plan) = self.planner.next_window(from, limit).await? else {
            return Ok(None);
        };
        let end_snapshot = plan.end.snapshot.get();
        let provider = provider_for_plan(self.table.clone(), &plan).map_err(|error| {
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
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::DataType;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{NamespaceIdent, TableCreation};
    use tempfile::TempDir;

    use super::*;

    fn options_of(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (String::from(*key), String::from(*value)))
            .collect()
    }

    #[test]
    fn from_options_defaults_to_unbounded_earliest() {
        let options =
            SourceOptions::from_options(&BTreeMap::new()).expect("empty options must parse");
        assert_eq!(options.caps, ReadCaps::default());
        assert_eq!(options.caps.max_files, None);
        assert_eq!(options.caps.max_rows, None);
        assert_eq!(options.start, StartPosition::Earliest);
    }

    #[test]
    fn from_options_parses_caps() {
        let options = SourceOptions::from_options(&options_of(&[
            ("streaming-max-files-per-micro-batch", "3"),
            ("streaming-max-rows-per-micro-batch", "1000"),
        ]))
        .expect("valid caps must parse");
        assert_eq!(options.caps.max_files, NonZeroUsize::new(3));
        assert_eq!(options.caps.max_rows, NonZeroU64::new(1000));
        assert_eq!(options.start, StartPosition::Earliest);
    }

    #[test]
    fn from_options_parses_from_timestamp() {
        let options =
            SourceOptions::from_options(&options_of(&[("stream-from-timestamp", "1724720185000")]))
                .expect("a millis timestamp must parse");
        assert_eq!(
            options.start,
            StartPosition::FromTimestamp {
                millis: 1_724_720_185_000
            }
        );
    }

    #[test]
    fn from_options_parses_start_after_snapshot_id() {
        let options = SourceOptions::from_options(&options_of(&[(
            "repark.cdc.start-after-snapshot-id",
            "7",
        )]))
        .expect("a snapshot id must parse");
        assert_eq!(
            options.start,
            StartPosition::AfterSnapshot(SnapshotId::new(7))
        );
    }

    #[test]
    fn from_options_refuses_both_skip_keys() {
        for key in [
            "streaming-skip-overwrite-snapshots",
            "streaming-skip-delete-snapshots",
        ] {
            let error = SourceOptions::from_options(&options_of(&[(key, "true")]))
                .expect_err("a skip key must refuse");
            assert!(matches!(error, MicroBatchError::SkipOptionRefused { .. }));
            assert_eq!(
                error.to_string(),
                format!("{key} is not accepted: Bronze is append-only (O-5)")
            );
        }
    }

    #[test]
    fn from_options_refuses_unknown_keys_under_interpreted_prefixes() {
        for key in ["streaming-bogus", "stream-bogus", "repark.cdc.bogus"] {
            let error = SourceOptions::from_options(&options_of(&[(key, "1")]))
                .expect_err("an unknown prefixed key must refuse");
            assert!(matches!(error, MicroBatchError::UnknownOption { .. }));
            assert_eq!(
                error.to_string(),
                format!("unknown streaming option {key}; remove it or fix the spelling")
            );
        }
    }

    #[test]
    fn from_options_passes_unprefixed_keys() {
        let options = SourceOptions::from_options(&options_of(&[
            ("checkpointLocation", "/tmp/checkpoints/q1"),
            ("path", "s3://bucket/table"),
            ("streaming-max-files-per-micro-batch", "2"),
        ]))
        .expect("unprefixed keys must pass");
        assert_eq!(options.caps.max_files, NonZeroUsize::new(2));
    }

    #[test]
    fn from_options_refuses_malformed_values() {
        for (key, value) in [
            ("streaming-max-files-per-micro-batch", "0"),
            ("streaming-max-files-per-micro-batch", "abc"),
            ("streaming-max-files-per-micro-batch", ""),
            ("streaming-max-rows-per-micro-batch", "-1"),
            ("stream-from-timestamp", "yesterday"),
            ("repark.cdc.start-after-snapshot-id", "1.5"),
        ] {
            let error = SourceOptions::from_options(&options_of(&[(key, value)]))
                .expect_err("a malformed value must refuse");
            assert!(matches!(error, MicroBatchError::Catalog(_)));
            assert!(
                error.to_string().contains(key),
                "the refusal must name {key}: {error}"
            );
        }
    }

    #[test]
    fn from_options_refuses_two_start_keys() {
        let error = SourceOptions::from_options(&options_of(&[
            ("stream-from-timestamp", "1724720185000"),
            ("repark.cdc.start-after-snapshot-id", "7"),
        ]))
        .expect_err("two start keys must refuse");
        assert!(matches!(error, MicroBatchError::Catalog(_)));
        let text = error.to_string();
        assert!(
            text.contains("stream-from-timestamp")
                && text.contains("repark.cdc.start-after-snapshot-id"),
            "the refusal must name both start keys: {text}"
        );
    }

    async fn session_with_two_appends() -> (TempDir, Session) {
        let warehouse = TempDir::new().expect("a scratch warehouse must build");
        let root = warehouse
            .path()
            .to_str()
            .expect("the warehouse path must be text");
        let session = Session::builder().build().expect("a session must build");
        session
            .register_memory_catalog("ice", root)
            .await
            .expect("the memory catalog must register");
        let handle = session
            .catalogs_snapshot()
            .get("ice")
            .cloned()
            .expect("the catalog must be visible");
        let sales = NamespaceIdent::new("sales".to_string());
        handle
            .create_namespace(&sales, HashMap::new())
            .await
            .expect("the namespace must create");
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("the schema must build");
        let creation = TableCreation::builder()
            .name("orders".to_string())
            .location(format!("{root}/sales/orders"))
            .schema(schema)
            .properties(HashMap::new())
            .build();
        handle
            .create_table(&sales, creation)
            .await
            .expect("the table must create");
        session
            .refresh_catalog_provider("ice")
            .await
            .expect("the provider must refresh");
        for values in ["(1), (2), (3)", "(4), (5)"] {
            session
                .sql(&format!("INSERT INTO ice.sales.orders VALUES {values}"))
                .await
                .expect("the insert must plan")
                .collect()
                .await
                .expect("the insert must commit");
        }
        (warehouse, session)
    }

    fn read_ids(frames: &[RecordBatch]) -> Vec<i64> {
        let mut ids = Vec::new();
        for frame in frames {
            assert_eq!(frame.column(0).data_type(), &DataType::Int64);
            let column = frame
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id must read as Int64");
            ids.extend((0..column.len()).map(|index| column.value(index)));
        }
        ids
    }

    #[tokio::test]
    async fn open_streams_two_appends_then_reports_none() {
        let (_warehouse, session) = session_with_two_appends().await;
        let options =
            SourceOptions::from_options(&BTreeMap::new()).expect("empty options must parse");
        let source = MicroBatchSource::open(&session, "ice.sales.orders", options)
            .await
            .expect("the source must open");
        let from = source
            .initial_offset()
            .await
            .expect("the initial offset must resolve")
            .expect("a nonempty table must have a start");
        assert_eq!(from.position.get(), 0);
        let batch = source
            .next_batch(&from, WindowLimit::Unbounded)
            .await
            .expect("the window must plan")
            .expect("new rows must batch");
        assert_eq!(batch.start, from);
        assert_eq!(batch.num_input_rows, 5);
        let frames = batch.frame.collect().await.expect("the batch must read");
        let ids = read_ids(&frames);
        assert_eq!(
            u64::try_from(ids.len()).expect("test row counts fit"),
            batch.num_input_rows
        );
        let mut sorted = ids;
        sorted.sort_unstable();
        assert_eq!(sorted, vec![1, 2, 3, 4, 5]);
        let drained = source
            .next_batch(&batch.end, WindowLimit::Unbounded)
            .await
            .expect("the drain check must plan");
        assert!(drained.is_none());
    }

    #[tokio::test]
    async fn capped_batches_agree_with_their_plans_row_for_row() {
        let (_warehouse, session) = session_with_two_appends().await;
        let options = SourceOptions::from_options(&options_of(&[(
            "streaming-max-files-per-micro-batch",
            "1",
        )]))
        .expect("a file cap must parse");
        let source = MicroBatchSource::open(&session, "ice.sales.orders", options)
            .await
            .expect("the source must open");
        let mut from = source
            .initial_offset()
            .await
            .expect("the initial offset must resolve")
            .expect("a nonempty table must have a start");
        let mut batches = 0u32;
        let mut ids = Vec::new();
        while let Some(batch) = source
            .next_batch(&from, WindowLimit::Capped)
            .await
            .expect("each window must plan")
        {
            batches += 1;
            let frames = batch.frame.collect().await.expect("each batch must read");
            let mut batch_ids = read_ids(&frames);
            assert_eq!(
                u64::try_from(batch_ids.len()).expect("test row counts fit"),
                batch.num_input_rows,
                "batch {batches} must read exactly its planned rows"
            );
            ids.append(&mut batch_ids);
            from = batch.end;
        }
        assert!(
            batches >= 2,
            "one file per batch must split two appends, got {batches}"
        );
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn open_refuses_unknown_tables_and_malformed_identifiers() {
        let warehouse = TempDir::new().expect("a scratch warehouse must build");
        let root = warehouse
            .path()
            .to_str()
            .expect("the warehouse path must be text");
        let session = Session::builder().build().expect("a session must build");
        session
            .register_memory_catalog("ice", root)
            .await
            .expect("the memory catalog must register");
        let options =
            SourceOptions::from_options(&BTreeMap::new()).expect("empty options must parse");
        for table in ["nope.sales.orders", "ice.sales.missing", "just-a-table", ""] {
            let error = MicroBatchSource::open(&session, table, options.clone())
                .await
                .err()
                .expect("a bad table must refuse");
            assert!(
                matches!(error, MicroBatchError::Catalog(_)),
                "{table} must refuse Catalog, got {error}"
            );
        }
        let error = MicroBatchSource::open(&session, "ice.sales", options)
            .await
            .err()
            .expect("a two-part name must refuse");
        assert!(
            error.to_string().contains("catalog.namespace.table"),
            "the refusal must name the shape: {error}"
        );
    }

    #[tokio::test]
    async fn initial_offset_on_an_empty_table_reads_none() {
        let warehouse = TempDir::new().expect("a scratch warehouse must build");
        let root = warehouse
            .path()
            .to_str()
            .expect("the warehouse path must be text");
        let session = Session::builder().build().expect("a session must build");
        session
            .register_memory_catalog("ice", root)
            .await
            .expect("the memory catalog must register");
        session
            .create_namespace("ice", "sales", HashMap::new())
            .await
            .expect("the namespace must create");
        session
            .testing_oob_create_table("ice", "sales", "orders", root)
            .await
            .expect("the table must create");
        session
            .refresh_catalog_provider("ice")
            .await
            .expect("the provider must refresh");
        let options =
            SourceOptions::from_options(&BTreeMap::new()).expect("empty options must parse");
        let source = MicroBatchSource::open(&session, "ice.sales.orders", options)
            .await
            .expect("the source must open");
        let start = source
            .initial_offset()
            .await
            .expect("the empty start must resolve");
        assert!(start.is_none());
    }
}
