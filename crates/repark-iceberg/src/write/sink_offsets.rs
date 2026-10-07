use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use datafusion::error::DataFusionError;
use iceberg::spec::MAIN_BRANCH;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind};
use repark_common::redaction::mask_value_credentials;
use uuid::Uuid;

use crate::microbatch::error::{MicroBatchError, RecoveryReason};
use crate::microbatch::offset::{
    OFFSETS_PROPERTY_PREFIX, QUERY_ID_KEY, QueryId, SinkDoor, SinkRecord, SnapshotId, TableUuid,
};
use crate::write::summary_collision::EngineSummary;
use crate::write::write_options::summary_with_extras;

pub const SCOPE_TOKEN_KEY: &str = "repark.cdc.scope-token";

const STAMP_KEY_PREFIXES: [&str; 2] = ["repark.cdc.", "spark.sql.streaming."];

#[derive(Clone, PartialEq, Eq)]
pub struct ScopeToken(Uuid);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitStamp {
    pub record: SinkRecord,
    pub door: SinkDoor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedStamp {
    pub stamp: CommitStamp,
    pub base: Option<SnapshotId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeOutcome {
    NotCommitted,
    Committed { snapshot: SnapshotId },
}

#[derive(Debug, Clone, Copy)]
pub struct BatchScope;

#[derive(Debug)]
pub struct BatchScopeGuard {
    sink: TableUuid,
    token: ScopeToken,
}

#[derive(Debug)]
struct ScopeEntry {
    token: ScopeToken,
    stamp: CommitStamp,
    claimed: bool,
    committed: Option<SnapshotId>,
}

fn scopes() -> MutexGuard<'static, HashMap<TableUuid, ScopeEntry>> {
    static SCOPES: OnceLock<Mutex<HashMap<TableUuid, ScopeEntry>>> = OnceLock::new();
    SCOPES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

impl ScopeToken {
    fn mint() -> ScopeToken {
        ScopeToken(Uuid::new_v4())
    }

    #[must_use]
    pub fn parse(text: &str) -> Option<ScopeToken> {
        Uuid::parse_str(text).ok().map(ScopeToken)
    }

    #[must_use]
    pub fn summary_entry(&self) -> (String, String) {
        (SCOPE_TOKEN_KEY.to_string(), self.to_string())
    }

    fn carried_by(extra: &[(String, String)]) -> Option<ScopeToken> {
        extra
            .iter()
            .find(|(key, _)| key == SCOPE_TOKEN_KEY)
            .and_then(|(_, value)| ScopeToken::parse(value))
    }
}

impl fmt::Display for ScopeToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.hyphenated().fmt(formatter)
    }
}

impl fmt::Debug for ScopeToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ScopeToken(..)")
    }
}

impl BatchScope {
    #[allow(clippy::missing_errors_doc)]
    pub fn enter(sink: TableUuid, stamp: CommitStamp) -> Result<BatchScopeGuard, MicroBatchError> {
        let mut entries = scopes();
        if entries.contains_key(&sink) {
            return Err(MicroBatchError::SinkBusy {
                sink: sink.to_string(),
            });
        }
        let token = ScopeToken::mint();
        entries.insert(
            sink,
            ScopeEntry {
                token: token.clone(),
                stamp,
                claimed: false,
                committed: None,
            },
        );
        Ok(BatchScopeGuard { sink, token })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn claim(
        table: &Table,
        token: &ScopeToken,
    ) -> Result<Option<ClaimedStamp>, MicroBatchError> {
        BatchScope::claim_checked(table, token, |_| Ok(()))
    }

    fn claim_checked(
        table: &Table,
        token: &ScopeToken,
        check: impl FnOnce(&CommitStamp) -> Result<(), MicroBatchError>,
    ) -> Result<Option<ClaimedStamp>, MicroBatchError> {
        let mut entries = scopes();
        let Some(entry) = entries
            .get_mut(&TableUuid::of(table))
            .filter(|entry| entry.token == *token)
        else {
            return Ok(None);
        };
        if entry.claimed {
            return Err(MicroBatchError::SinkCommittedTwice {
                epoch: entry.stamp.record.epoch,
            });
        }
        check(&entry.stamp)?;
        entry.claimed = true;
        Ok(Some(ClaimedStamp {
            stamp: entry.stamp.clone(),
            base: table.metadata().current_snapshot_id().map(SnapshotId::new),
        }))
    }
}

impl BatchScopeGuard {
    #[must_use]
    pub fn token(&self) -> &ScopeToken {
        &self.token
    }

    #[must_use]
    pub fn outcome(&self) -> ScopeOutcome {
        match scopes().get(&self.sink).and_then(|entry| entry.committed) {
            Some(snapshot) => ScopeOutcome::Committed { snapshot },
            None => ScopeOutcome::NotCommitted,
        }
    }
}

impl Drop for BatchScopeGuard {
    fn drop(&mut self) {
        scopes().remove(&self.sink);
    }
}

impl ClaimedStamp {
    #[allow(clippy::missing_errors_doc)]
    pub fn summary_entries(&self) -> Result<Vec<(String, String)>, MicroBatchError> {
        self.stamp.record.summary_entries(self.stamp.door)
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn stamp_transaction(&self, tx: Transaction) -> Result<Transaction, MicroBatchError> {
        let (key, value) = self.stamp.record.property()?;
        tx.update_table_properties()
            .set(key, value)
            .apply(tx)
            .map_err(|error| masked(&error))
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn record_commit(self, committed: &Table) -> Result<(), MicroBatchError> {
        let record = &self.stamp.record;
        let Some(head) = committed.metadata().current_snapshot() else {
            return Err(MicroBatchError::Catalog(format!(
                "stamped commit for query {query} epoch {epoch} left the sink without a snapshot",
                query = record.query,
                epoch = record.epoch
            )));
        };
        let snapshot = SnapshotId::new(head.snapshot_id());
        let stamped = SinkRecord::from_summary(&head.summary().additional_properties)?;
        if stamped.as_ref() != Some(record) {
            return Err(MicroBatchError::RecoveryRequired {
                query: record.query,
                epoch: record.epoch,
                durable: None,
                reason: RecoveryReason::UnstampedSinkCommit { snapshot },
            });
        }
        if let Some(entry) = scopes().get_mut(&TableUuid::of(committed))
            && entry.claimed
            && entry.stamp == self.stamp
        {
            entry.committed = Some(snapshot);
        }
        Ok(())
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn read_resume_point(
    table: &Table,
    query: QueryId,
) -> Result<Option<SinkRecord>, MicroBatchError> {
    let summary = newest_stamp(table, query)?;
    let property = property_record(table, query)?;
    match (summary, property) {
        (None, None) => Ok(None),
        (Some(summary), Some(property)) if summary == property => Ok(Some(summary)),
        (None, Some(property)) => Err(MicroBatchError::RecoveryRequired {
            query,
            epoch: property.epoch,
            durable: Some(Box::new(property)),
            reason: RecoveryReason::StampedSnapshotExpired,
        }),
        (Some(summary), property) => Err(MicroBatchError::RecoveryRequired {
            query,
            epoch: summary.epoch,
            reason: RecoveryReason::OffsetMismatch {
                summary_epoch: Some(summary.epoch),
                property_epoch: property.map(|record| record.epoch),
            },
            durable: Some(Box::new(summary)),
        }),
    }
}

fn newest_stamp(table: &Table, query: QueryId) -> Result<Option<SinkRecord>, MicroBatchError> {
    let metadata = table.metadata();
    let mut cursor = metadata.current_snapshot();
    for _ in 0..metadata.snapshots().len() {
        let Some(snapshot) = cursor else {
            return Ok(None);
        };
        let summary = &snapshot.summary().additional_properties;
        if stamped_by(summary, query) {
            return match SinkRecord::from_summary(summary)? {
                Some(record) => Ok(Some(record)),
                None => Err(MicroBatchError::Catalog(format!(
                    "repark.cdc stamp of query {query} on snapshot {id} misses its format version",
                    id = snapshot.snapshot_id()
                ))),
            };
        }
        cursor = snapshot
            .parent_snapshot_id()
            .and_then(|parent| metadata.snapshot_by_id(parent));
    }
    Ok(None)
}

fn stamped_by(summary: &HashMap<String, String>, query: QueryId) -> bool {
    summary
        .get(QUERY_ID_KEY)
        .and_then(|text| Uuid::parse_str(text).ok())
        .is_some_and(|stamped| stamped == query.get())
}

fn property_record(table: &Table, query: QueryId) -> Result<Option<SinkRecord>, MicroBatchError> {
    let key = format!("{OFFSETS_PROPERTY_PREFIX}{query}");
    table
        .metadata()
        .properties()
        .get(&key)
        .map(|value| SinkRecord::from_property(query, value))
        .transpose()
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_stamp_only(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    stamp: &CommitStamp,
    token: Option<&ScopeToken>,
) -> Result<SnapshotId, MicroBatchError> {
    let active = match token {
        Some(token) => BatchScope::claim_checked(table, token, |active| {
            if active == stamp {
                return Ok(());
            }
            Err(MicroBatchError::Catalog(format!(
                "stamp-only commit for epoch {asked} does not match the active batch epoch {active_epoch}",
                asked = stamp.record.epoch,
                active_epoch = active.record.epoch
            )))
        })?,
        None => None,
    };
    let claimed = active.unwrap_or_else(|| ClaimedStamp {
        stamp: stamp.clone(),
        base: table.metadata().current_snapshot_id().map(SnapshotId::new),
    });
    let engine = EngineSummary::for_append(table, &[], None);
    let (operation_id, summary) = summary_with_extras(&claimed.summary_entries()?, &engine)
        .map_err(|error| masked(&error))?;
    let tx = Transaction::new(table);
    let tx = tx
        .merge_append()
        .set_snapshot_properties(summary)
        .apply(tx)
        .map_err(|error| masked(&error))?;
    let tx = claimed.stamp_transaction(tx)?;
    let committed = match tx.commit(catalog.as_ref()).await {
        Ok(committed) => committed,
        Err(error) if error.kind() == ErrorKind::CommitStateUnknown => {
            return Err(MicroBatchError::RecoveryRequired {
                query: stamp.record.query,
                epoch: stamp.record.epoch,
                durable: None,
                reason: RecoveryReason::CommitOutcomeUnknown {
                    operation_id: Some(operation_id),
                },
            });
        }
        Err(error) => return Err(masked(&error)),
    };
    let snapshot = committed
        .metadata()
        .current_snapshot_id()
        .map(SnapshotId::new);
    claimed.record_commit(&committed)?;
    snapshot
        .ok_or_else(|| MicroBatchError::Catalog(String::from("stamp-only commit left no snapshot")))
}

#[derive(Debug, Default)]
pub(crate) struct SiteStamp {
    claimed: Option<ClaimedStamp>,
}

impl SiteStamp {
    pub(crate) fn claim(
        table: &Table,
        branch: Option<&str>,
        extra: &[(String, String)],
    ) -> datafusion::error::Result<SiteStamp> {
        if branch.is_some_and(|name| name != MAIN_BRANCH) {
            return Ok(SiteStamp::default());
        }
        let Some(token) = ScopeToken::carried_by(extra) else {
            return Ok(SiteStamp::default());
        };
        let claimed =
            BatchScope::claim_checked(table, &token, |stamp| refuse_stamp_keys(extra, stamp))
                .map_err(microbatch_error)?;
        Ok(SiteStamp { claimed })
    }

    pub(crate) fn extras<'extra>(
        &self,
        extra: &'extra [(String, String)],
    ) -> datafusion::error::Result<Cow<'extra, [(String, String)]>> {
        let carries_token = extra.iter().any(|(key, _)| key == SCOPE_TOKEN_KEY);
        if self.claimed.is_none() && !carries_token {
            return Ok(Cow::Borrowed(extra));
        }
        let mut stamped: Vec<(String, String)> = extra
            .iter()
            .filter(|(key, _)| key != SCOPE_TOKEN_KEY)
            .cloned()
            .collect();
        if let Some(claimed) = &self.claimed {
            stamped.extend(claimed.summary_entries().map_err(microbatch_error)?);
        }
        Ok(Cow::Owned(stamped))
    }

    pub(crate) fn transaction(&self, tx: Transaction) -> datafusion::error::Result<Transaction> {
        match &self.claimed {
            Some(claimed) => claimed.stamp_transaction(tx).map_err(microbatch_error),
            None => Ok(tx),
        }
    }

    pub(crate) fn record(self, committed: &Table) -> datafusion::error::Result<()> {
        match self.claimed {
            Some(claimed) => claimed.record_commit(committed).map_err(microbatch_error),
            None => Ok(()),
        }
    }
}

fn refuse_stamp_keys(
    extra: &[(String, String)],
    stamp: &CommitStamp,
) -> Result<(), MicroBatchError> {
    let collision = extra.iter().map(|(key, _)| key).find(|key| {
        let folded = key.to_ascii_lowercase();
        key.as_str() != SCOPE_TOKEN_KEY
            && STAMP_KEY_PREFIXES
                .iter()
                .any(|prefix| folded.starts_with(prefix))
    });
    match collision {
        Some(key) => Err(MicroBatchError::Catalog(format!(
            "snapshot property {key} collides with the stamp of query {query} epoch {epoch}; remove it from the stamped commit",
            query = stamp.record.query,
            epoch = stamp.record.epoch
        ))),
        None => Ok(()),
    }
}

fn microbatch_error(error: MicroBatchError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn masked(error: &impl std::fmt::Display) -> MicroBatchError {
    MicroBatchError::Catalog(mask_value_credentials(&error.to_string()))
}

#[cfg(test)]
#[path = "sink_offsets_tests.rs"]
mod tests;
