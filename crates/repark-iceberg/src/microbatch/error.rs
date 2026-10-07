use std::time::Duration;

use iceberg::spec::Operation;
use repark_common::Generation;
use thiserror::Error;

use crate::microbatch::offset::{
    Epoch, FilePosition, QueryId, RunId, SinkRecord, SnapshotId, TableUuid,
};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum MicroBatchError {
    #[error("Cannot process {word} snapshot: {snapshot}; Bronze is append-only (O-5) and the stream does not skip it. Start a new query (new queryName) with repark.cdc.start-after-snapshot-id={snapshot}", word = operation.as_str())]
    NonAppendSnapshot {
        table: String,
        snapshot: SnapshotId,
        operation: Operation,
        from: Option<SnapshotId>,
        to: SnapshotId,
    },
    #[error("{option} is not accepted: Bronze is append-only (O-5)")]
    SkipOptionRefused { option: &'static str },
    #[error("unknown streaming option {key}; remove it or fix the spelling")]
    UnknownOption { key: String },
    #[error(
        "checkpointLocation must be specified either through option(\"checkpointLocation\", ...) or SparkSession.conf.set(\"spark.sql.streaming.checkpointLocation\", ...)."
    )]
    CheckpointLocationMissing,
    #[error(
        "outputMode({mode}) is not supported on a streaming sink; use outputMode(append) or foreachBatch"
    )]
    OutputModeRefused { mode: String },
    #[error("{operator} is not supported on a streaming DataFrame; use foreachBatch")]
    StatefulOperatorRefused { operator: String },
    #[error("{feature} is not implemented")]
    FeatureRefused { feature: String },
    #[error(
        "streaming needs a shared catalog; {catalog} is a local filesystem catalog. Use Glue, S3 Tables, the Postgres catalog or REST"
    )]
    LocalCatalogRefused { catalog: String },
    #[error("source table {table} has no primary key; a streaming source must be keyed (O-6)")]
    KeylessSource { table: String },
    #[error(
        "no sink declared for this streaming query; pass .option(\"repark.cdc.sink\", \"<table>\")"
    )]
    SinkUndeclared,
    #[error("sink {sink} already has an active batch; one batch per sink at a time")]
    SinkBusy { sink: String },
    #[error(
        "epoch {epoch} already stamped the sink; refusing a second sink write in the same batch: a restart resumes after epoch {epoch}, so this write's rows would never land. Write the sink once per batch body, or combine the writes into a single write"
    )]
    SinkCommittedTwice { epoch: Epoch },
    #[error("query {query} epoch {epoch} is already committed")]
    AlreadyCommitted { query: QueryId, epoch: Epoch },
    #[error("query {query} epoch {epoch} lost the sink to run {winner}; stop this driver")]
    Fenced {
        query: QueryId,
        epoch: Epoch,
        winner: RunId,
    },
    #[error("query {query} resumed with generation {resumed_value} but the sink holds generation {stamped_value}; start a new query (new queryName) to reset", resumed_value = resumed.get(), stamped_value = stamped.get())]
    GenerationMismatch {
        query: QueryId,
        resumed: Generation,
        stamped: Generation,
    },
    #[error(
        "query {query} inputs changed from {recorded:?} to {current:?}; start a new query (new queryName)"
    )]
    InputsChanged {
        query: QueryId,
        recorded: Vec<String>,
        current: Vec<String>,
    },
    #[error(
        "source table {table} was replaced (recorded uuid {recorded}, current uuid {current}); start a new query (new queryName)"
    )]
    SourceReplaced {
        table: String,
        recorded: TableUuid,
        current: TableUuid,
    },
    #[error(
        "Cannot resume: start snapshot {snapshot} expired; oldest available is {oldest}; see expire_snapshots retention"
    )]
    SourceSnapshotExpired {
        table: String,
        snapshot: SnapshotId,
        oldest: SnapshotId,
    },
    #[error(
        "Cannot find snapshot after {snapshot}: not an ancestor of table's current snapshot {head} in table {table}; it left the current lineage (for example through rollback_to). Start a new query (new queryName) with repark.cdc.start-after-snapshot-id set to an ancestor of {head}"
    )]
    SnapshotNotInLineage {
        table: String,
        snapshot: SnapshotId,
        head: SnapshotId,
    },
    #[error(
        "Cannot resume: table {table} history is truncated at snapshot {oldest} (parent {missing_parent} expired); start a new query with repark.cdc.start-after-snapshot-id"
    )]
    TruncatedHistory {
        table: String,
        oldest: SnapshotId,
        missing_parent: SnapshotId,
    },
    #[error(
        "Cannot resume: offset format version {found} is not supported by this build (supports {supported}); upgrade to a build that reads it"
    )]
    UnsupportedOffsetFormat { found: String, supported: u32 },
    #[error(
        "Cannot resume: offset position {position_value} is past the {files} added files of snapshot {snapshot} in table {table}; the stored offset is corrupt. Start a new query (new queryName)", position_value = position.get()
    )]
    OffsetPositionOutOfRange {
        table: String,
        snapshot: SnapshotId,
        position: FilePosition,
        files: u64,
    },
    #[error("stamped write into {sink} needs {property}=serializable")]
    MergeIsolationRefused { sink: String, property: String },
    #[error("batch {epoch} failed: {cause}")]
    BatchFailed { epoch: Epoch, cause: String },
    #[error("recovery required for query {query} epoch {epoch}: {reason}")]
    RecoveryRequired {
        query: QueryId,
        epoch: Epoch,
        durable: Option<Box<SinkRecord>>,
        reason: RecoveryReason,
    },
    #[error("{0}")]
    Catalog(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RecoveryReason {
    #[error("commit outcome unknown (operation id: {operation_id_text})", operation_id_text = operation_id.as_deref().unwrap_or("not recorded"))]
    CommitOutcomeUnknown { operation_id: Option<String> },
    #[error("stop timed out after {waited:?}")]
    StopTimeout { waited: Duration },
    #[error("summary epoch {summary_epoch_text} disagrees with property epoch {property_epoch_text}", summary_epoch_text = summary_epoch.map_or_else(|| "none".to_string(), |epoch| epoch.get().to_string()), property_epoch_text = property_epoch.map_or_else(|| "none".to_string(), |epoch| epoch.get().to_string()))]
    OffsetMismatch {
        summary_epoch: Option<Epoch>,
        property_epoch: Option<Epoch>,
    },
    #[error("stamped snapshot expired; raise history.expire.min-snapshots-to-keep retention")]
    StampedSnapshotExpired,
    #[error(
        "stamped snapshot {snapshot} is retained but not in the sink's current lineage: the sink was rolled back past it, so its rows are not live. Start a new query (new queryName), or restore the sink to snapshot {snapshot}"
    )]
    StampNotInLineage { snapshot: SnapshotId },
    #[error(
        "sink advanced to snapshot {snapshot} without a stamp; a commit bypassed the batch scope"
    )]
    UnstampedSinkCommit { snapshot: SnapshotId },
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn query_id() -> QueryId {
        QueryId::new(Uuid::nil())
    }

    fn table_uuid() -> TableUuid {
        TableUuid::new(Uuid::nil())
    }

    fn generation(value: u64) -> Generation {
        Generation::new(value).expect("positive generation")
    }

    #[test]
    fn every_error_variant_renders_a_message() {
        let query = query_id();
        let table = table_uuid();
        let errors = [
            MicroBatchError::NonAppendSnapshot {
                table: String::from("bronze.events"),
                snapshot: SnapshotId::new(7),
                operation: Operation::Overwrite,
                from: None,
                to: SnapshotId::new(9),
            },
            MicroBatchError::SkipOptionRefused {
                option: "streaming-skip-overwrite-snapshots",
            },
            MicroBatchError::UnknownOption {
                key: String::from("streaming-typo"),
            },
            MicroBatchError::CheckpointLocationMissing,
            MicroBatchError::OutputModeRefused {
                mode: String::from("complete"),
            },
            MicroBatchError::StatefulOperatorRefused {
                operator: String::from("aggregate"),
            },
            MicroBatchError::FeatureRefused {
                feature: String::from("trigger(continuous)"),
            },
            MicroBatchError::LocalCatalogRefused {
                catalog: String::from("hadoop"),
            },
            MicroBatchError::KeylessSource {
                table: String::from("bronze.events"),
            },
            MicroBatchError::SinkUndeclared,
            MicroBatchError::SinkBusy {
                sink: String::from("silver.events"),
            },
            MicroBatchError::SinkCommittedTwice {
                epoch: Epoch::FIRST,
            },
            MicroBatchError::AlreadyCommitted {
                query,
                epoch: Epoch::FIRST,
            },
            MicroBatchError::Fenced {
                query,
                epoch: Epoch::FIRST,
                winner: RunId::new(Uuid::nil()),
            },
            MicroBatchError::GenerationMismatch {
                query,
                resumed: generation(1),
                stamped: generation(2),
            },
            MicroBatchError::InputsChanged {
                query,
                recorded: vec![String::from("bronze.a")],
                current: vec![String::from("bronze.b")],
            },
            MicroBatchError::SourceReplaced {
                table: String::from("bronze.events"),
                recorded: table,
                current: table,
            },
            MicroBatchError::SourceSnapshotExpired {
                table: String::from("bronze.events"),
                snapshot: SnapshotId::new(7),
                oldest: SnapshotId::new(9),
            },
            MicroBatchError::SnapshotNotInLineage {
                table: String::from("bronze.events"),
                snapshot: SnapshotId::new(7),
                head: SnapshotId::new(9),
            },
            MicroBatchError::TruncatedHistory {
                table: String::from("bronze.events"),
                oldest: SnapshotId::new(9),
                missing_parent: SnapshotId::new(8),
            },
            MicroBatchError::UnsupportedOffsetFormat {
                found: String::from("2"),
                supported: 1,
            },
            MicroBatchError::MergeIsolationRefused {
                sink: String::from("silver.events"),
                property: String::from("write.merge.isolation-level"),
            },
            MicroBatchError::BatchFailed {
                epoch: Epoch::FIRST,
                cause: String::from("boom"),
            },
            MicroBatchError::RecoveryRequired {
                query,
                epoch: Epoch::FIRST,
                durable: None,
                reason: RecoveryReason::StampedSnapshotExpired,
            },
            MicroBatchError::Catalog(String::from("catalog exploded")),
        ];
        assert!(errors.iter().all(|error| !error.to_string().is_empty()));
    }

    #[test]
    fn every_recovery_reason_renders_a_message() {
        let reasons = [
            RecoveryReason::CommitOutcomeUnknown { operation_id: None },
            RecoveryReason::StopTimeout {
                waited: Duration::from_secs(30),
            },
            RecoveryReason::OffsetMismatch {
                summary_epoch: Some(Epoch::FIRST),
                property_epoch: None,
            },
            RecoveryReason::StampedSnapshotExpired,
            RecoveryReason::StampNotInLineage {
                snapshot: SnapshotId::new(12),
            },
            RecoveryReason::UnstampedSinkCommit {
                snapshot: SnapshotId::new(11),
            },
        ];
        for reason in reasons {
            assert!(!reason.to_string().is_empty());
        }
    }

    #[test]
    fn overwrite_snapshot_refusal_matches_spark_prefix() {
        let error = MicroBatchError::NonAppendSnapshot {
            table: String::from("bronze.events"),
            snapshot: SnapshotId::new(123),
            operation: Operation::Overwrite,
            from: Some(SnapshotId::new(100)),
            to: SnapshotId::new(200),
        };
        assert_eq!(
            error.to_string(),
            "Cannot process overwrite snapshot: 123; Bronze is append-only (O-5) and the stream does not skip it. Start a new query (new queryName) with repark.cdc.start-after-snapshot-id=123"
        );
    }

    #[test]
    fn delete_snapshot_refusal_matches_spark_prefix() {
        let error = MicroBatchError::NonAppendSnapshot {
            table: String::from("bronze.events"),
            snapshot: SnapshotId::new(456),
            operation: Operation::Delete,
            from: None,
            to: SnapshotId::new(456),
        };
        assert_eq!(
            error.to_string(),
            "Cannot process delete snapshot: 456; Bronze is append-only (O-5) and the stream does not skip it. Start a new query (new queryName) with repark.cdc.start-after-snapshot-id=456"
        );
    }

    #[test]
    fn offset_position_refusal_names_position_count_and_snapshot() {
        let error = MicroBatchError::OffsetPositionOutOfRange {
            table: String::from("bronze.events"),
            snapshot: SnapshotId::new(7),
            position: FilePosition::new(99),
            files: 2,
        };
        assert_eq!(
            error.to_string(),
            "Cannot resume: offset position 99 is past the 2 added files of snapshot 7 in table bronze.events; the stored offset is corrupt. Start a new query (new queryName)"
        );
    }

    #[test]
    fn skip_option_refusal_names_the_option() {
        let error = MicroBatchError::SkipOptionRefused {
            option: "streaming-skip-overwrite-snapshots",
        };
        assert_eq!(
            error.to_string(),
            "streaming-skip-overwrite-snapshots is not accepted: Bronze is append-only (O-5)"
        );
    }

    #[test]
    fn checkpoint_location_refusal_matches_spark_verbatim() {
        assert_eq!(
            MicroBatchError::CheckpointLocationMissing.to_string(),
            "checkpointLocation must be specified either through option(\"checkpointLocation\", ...) or SparkSession.conf.set(\"spark.sql.streaming.checkpointLocation\", ...)."
        );
    }

    #[test]
    fn expired_snapshot_refusal_matches_resume_standard() {
        let error = MicroBatchError::SourceSnapshotExpired {
            table: String::from("bronze.events"),
            snapshot: SnapshotId::new(7),
            oldest: SnapshotId::new(9),
        };
        assert_eq!(
            error.to_string(),
            "Cannot resume: start snapshot 7 expired; oldest available is 9; see expire_snapshots retention"
        );
    }

    #[test]
    fn lineage_refusal_starts_with_spark_text_and_never_says_expired() {
        let error = MicroBatchError::SnapshotNotInLineage {
            table: String::from("ice.bronze.events"),
            snapshot: SnapshotId::new(7),
            head: SnapshotId::new(9),
        };
        let text = error.to_string();
        assert_eq!(
            text,
            "Cannot find snapshot after 7: not an ancestor of table's current snapshot 9 in table ice.bronze.events; it left the current lineage (for example through rollback_to). Start a new query (new queryName) with repark.cdc.start-after-snapshot-id set to an ancestor of 9"
        );
        assert!(!text.contains("expired"));
    }

    #[test]
    fn local_catalog_refusal_names_shared_catalogs() {
        let error = MicroBatchError::LocalCatalogRefused {
            catalog: String::from("hadoop"),
        };
        assert_eq!(
            error.to_string(),
            "streaming needs a shared catalog; hadoop is a local filesystem catalog. Use Glue, S3 Tables, the Postgres catalog or REST"
        );
    }

    #[test]
    fn sink_undeclared_refusal_names_sink_option() {
        assert_eq!(
            MicroBatchError::SinkUndeclared.to_string(),
            "no sink declared for this streaming query; pass .option(\"repark.cdc.sink\", \"<table>\")"
        );
    }

    #[test]
    fn merge_isolation_refusal_names_serializable() {
        let error = MicroBatchError::MergeIsolationRefused {
            sink: String::from("silver.events"),
            property: String::from("write.update.isolation-level"),
        };
        assert_eq!(
            error.to_string(),
            "stamped write into silver.events needs write.update.isolation-level=serializable"
        );
    }

    #[test]
    fn recovery_reasons_render() {
        let unknown = RecoveryReason::CommitOutcomeUnknown {
            operation_id: Some(String::from("op-1")),
        };
        assert_eq!(
            unknown.to_string(),
            "commit outcome unknown (operation id: op-1)"
        );
        let unknown_missing = RecoveryReason::CommitOutcomeUnknown { operation_id: None };
        assert_eq!(
            unknown_missing.to_string(),
            "commit outcome unknown (operation id: not recorded)"
        );
        let timeout = RecoveryReason::StopTimeout {
            waited: Duration::from_secs(30),
        };
        assert_eq!(timeout.to_string(), "stop timed out after 30s");
        let mismatch = RecoveryReason::OffsetMismatch {
            summary_epoch: Some(Epoch::new(3)),
            property_epoch: None,
        };
        assert_eq!(
            mismatch.to_string(),
            "summary epoch 3 disagrees with property epoch none"
        );
        let required = MicroBatchError::RecoveryRequired {
            query: query_id(),
            epoch: Epoch::new(3),
            durable: None,
            reason: RecoveryReason::StampedSnapshotExpired,
        };
        assert_eq!(
            required.to_string(),
            "recovery required for query 00000000-0000-0000-0000-000000000000 epoch 3: stamped snapshot expired; raise history.expire.min-snapshots-to-keep retention"
        );
    }

    #[test]
    fn sink_committed_twice_names_the_loss_and_the_fix() {
        let twice = MicroBatchError::SinkCommittedTwice {
            epoch: Epoch::new(4),
        };
        assert_eq!(
            twice.to_string(),
            "epoch 4 already stamped the sink; refusing a second sink write in the same batch: a restart resumes after epoch 4, so this write's rows would never land. Write the sink once per batch body, or combine the writes into a single write"
        );
    }
}
