use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use datafusion::error::DataFusionError;
use iceberg::spec::{FormatVersion, MAIN_BRANCH, Snapshot, SnapshotRef, TableMetadata};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind};
use repark_common::redaction::mask_value_credentials;
use uuid::Uuid;

use crate::microbatch::error::{MicroBatchError, RecoveryReason};
use crate::microbatch::offset::{
    OFFSETS_PROPERTY_PREFIX, QUERY_ID_KEY, QueryId, SinkDoor, SinkRecord, SnapshotId, TableUuid,
};
use crate::microbatch::starting_mark::StartingMark;
use crate::write::merge::{CommitScope, IsolationLevel, OPERATION_ID_PROP};
use crate::write::summary_collision::EngineSummary;
use crate::write::write_options::summary_with_extras;

mod append_fence;
mod body_scope;
mod lineage;

use append_fence::AppendFence;
pub use body_scope::{guard_body_catalog, in_body_scope, refuse_planned_sink_write};
pub use lineage::{
    Floor, SinkMark, Stamped, Stray, carried_by_foreach, commit_starting_mark, read_starting_mark,
    stray_on_main,
};

pub const SCOPE_TOKEN_KEY: &str = "repark.cdc.scope-token";

pub(crate) const RESERVED_STAMP_PREFIX: &str = "repark.cdc.";

const STAMP_KEY_PREFIXES: [&str; 2] = [RESERVED_STAMP_PREFIX, "spark.sql.streaming."];

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
    pub started: Option<StartingMark>,
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
    refused: Option<MicroBatchError>,
    violation: Option<MicroBatchError>,
    outcome_unknown: bool,
    began: Began,
}

#[derive(Debug, Clone, Copy)]
enum Began {
    Unread,
    At(Option<i64>),
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
        Self::entered(sink, stamp, Began::Unread)
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn enter_on(table: &Table, stamp: CommitStamp) -> Result<BatchScopeGuard, MicroBatchError> {
        let head = table.metadata().current_snapshot_id();
        Self::entered(TableUuid::of(table), stamp, Began::At(head))
    }

    fn entered(
        sink: TableUuid,
        stamp: CommitStamp,
        began: Began,
    ) -> Result<BatchScopeGuard, MicroBatchError> {
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
                refused: None,
                violation: None,
                outcome_unknown: false,
                began,
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
        if entry.claimed && entry.outcome_unknown && entry.committed.is_none() {
            return Err(MicroBatchError::Catalog(format!(
                "epoch {epoch}: an earlier stamped commit of this batch to the sink is still in flight or its outcome is unknown, so a second one is refused",
                epoch = entry.stamp.record.epoch
            )));
        }
        if entry.claimed {
            let twice = MicroBatchError::SinkCommittedTwice {
                epoch: entry.stamp.record.epoch,
            };
            entry.violation = Some(twice.clone());
            return Err(twice);
        }
        if let Some(refused) = &entry.refused {
            return Err(refused.clone());
        }
        check(&entry.stamp)?;
        if let Err(error) = epoch_check(table, &entry.stamp) {
            if durable_refusal(&error) {
                entry.refused = Some(error.clone());
            }
            return Err(error);
        }
        if let Began::At(head) = entry.began
            && let Some(stray) = stray_since(table.metadata(), head, &entry.stamp)
        {
            return Err(over_a_stray(stray, &entry.stamp));
        }
        entry.claimed = true;
        Ok(Some(ClaimedStamp::on(table, entry.stamp.clone())))
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

    #[must_use]
    pub fn body_refusal(&self) -> Option<MicroBatchError> {
        let entries = scopes();
        let entry = entries.get(&self.sink)?;
        entry.refused.clone().or_else(|| entry.violation.clone())
    }

    #[must_use]
    pub fn outcome_unknown(&self) -> bool {
        scopes()
            .get(&self.sink)
            .is_some_and(|entry| entry.outcome_unknown)
    }
}

impl Drop for BatchScopeGuard {
    fn drop(&mut self) {
        scopes().remove(&self.sink);
    }
}

impl ClaimedStamp {
    fn on(table: &Table, stamp: CommitStamp) -> ClaimedStamp {
        let started = read_starting_mark(table, stamp.record.query).ok().flatten();
        ClaimedStamp {
            stamp,
            base: table.metadata().current_snapshot_id().map(SnapshotId::new),
            started,
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn summary_entries(&self) -> Result<Vec<(String, String)>, MicroBatchError> {
        let mut entries = self.stamp.record.summary_entries(self.stamp.door)?;
        entries.extend(self.started.map(|mark| mark.summary_entry()));
        Ok(entries)
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
                reason: RecoveryReason::UnstampedSinkCommit {
                    snapshot,
                    operation: None,
                },
            });
        }
        mark_committed(committed, &self.stamp, snapshot);
        Ok(())
    }
}

fn stray_since<'metadata>(
    metadata: &'metadata TableMetadata,
    floor: Option<i64>,
    stamp: &CommitStamp,
) -> Option<&'metadata SnapshotRef> {
    if stamp.door != SinkDoor::ForeachBatch {
        return None;
    }
    main_lineage(metadata)
        .take_while(|snapshot| Some(snapshot.snapshot_id()) != floor)
        .find(|snapshot| lineage::unstamped(snapshot))
}

fn over_a_stray(stray: &SnapshotRef, stamp: &CommitStamp) -> MicroBatchError {
    MicroBatchError::Catalog(format!(
        "epoch {epoch}: snapshot {id} ({operation}) landed on the sink without a stamp after this batch began, so the batch's stamped commit is refused before it lands over it",
        epoch = stamp.record.epoch,
        id = stray.snapshot_id(),
        operation = stray.summary().operation.as_str()
    ))
}

fn mark_committed(sink: &Table, stamp: &CommitStamp, snapshot: SnapshotId) {
    if let Some(entry) = scopes().get_mut(&TableUuid::of(sink))
        && entry.claimed
        && entry.stamp == *stamp
    {
        entry.committed = Some(snapshot);
    }
}

fn release_claim(sink: &Table, stamp: &CommitStamp) {
    if let Some(entry) = scopes().get_mut(&TableUuid::of(sink))
        && entry.claimed
        && entry.committed.is_none()
        && entry.stamp == *stamp
    {
        entry.claimed = false;
    }
}

fn latch_refusal(sink: &Table, stamp: &CommitStamp, refusal: &MicroBatchError) {
    if let Some(entry) = scopes().get_mut(&TableUuid::of(sink))
        && entry.claimed
        && entry.stamp == *stamp
        && durable_refusal(refusal)
    {
        entry.claimed = false;
        entry.refused = Some(refusal.clone());
    }
}

fn durable_refusal(error: &MicroBatchError) -> bool {
    matches!(
        error,
        MicroBatchError::AlreadyCommitted { .. }
            | MicroBatchError::Fenced { .. }
            | MicroBatchError::GenerationMismatch { .. }
    )
}

fn epoch_check(table: &Table, stamp: &CommitStamp) -> Result<(), MicroBatchError> {
    let record = &stamp.record;
    let Some(durable) = read_resume_point(table, record.query)? else {
        return Ok(());
    };
    if durable.generation != record.generation {
        return Err(MicroBatchError::GenerationMismatch {
            query: record.query,
            resumed: record.generation,
            stamped: durable.generation,
        });
    }
    if durable.epoch.get() < record.epoch.get() {
        return Ok(());
    }
    if durable.run == record.run {
        return Err(MicroBatchError::AlreadyCommitted {
            query: record.query,
            epoch: record.epoch,
        });
    }
    let winner = stamp_at_epoch(table, record)
        .map(|committer| committer.run)
        .filter(|run| *run != record.run)
        .unwrap_or(durable.run);
    Err(MicroBatchError::Fenced {
        query: record.query,
        epoch: record.epoch,
        winner,
    })
}

fn stamp_at_epoch(table: &Table, record: &SinkRecord) -> Option<SinkRecord> {
    main_lineage(table.metadata())
        .filter_map(|snapshot| {
            let summary = &snapshot.summary().additional_properties;
            stamped_by(summary, record.query)
                .then(|| SinkRecord::from_summary(summary).ok().flatten())
                .flatten()
        })
        .take_while(|stamped| stamped.epoch.get() >= record.epoch.get())
        .find(|stamped| stamped.epoch == record.epoch)
}

fn main_lineage(metadata: &TableMetadata) -> impl Iterator<Item = &SnapshotRef> {
    std::iter::successors(metadata.current_snapshot(), |snapshot| {
        snapshot
            .parent_snapshot_id()
            .and_then(|parent| metadata.snapshot_by_id(parent))
    })
    .take(metadata.snapshots().len())
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
        (None, Some(property)) => match off_lineage_stamp(table, query, &property) {
            Some(snapshot) => Err(MicroBatchError::RecoveryRequired {
                query,
                epoch: property.epoch,
                durable: None,
                reason: RecoveryReason::StampNotInLineage { snapshot },
            }),
            None => Err(MicroBatchError::RecoveryRequired {
                query,
                epoch: property.epoch,
                durable: Some(Box::new(property)),
                reason: RecoveryReason::StampedSnapshotExpired,
            }),
        },
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
    let Some(snapshot) = main_lineage(table.metadata())
        .find(|snapshot| stamped_by(&snapshot.summary().additional_properties, query))
    else {
        return Ok(None);
    };
    match SinkRecord::from_summary(&snapshot.summary().additional_properties)? {
        Some(record) => Ok(Some(record)),
        None => Err(MicroBatchError::Catalog(format!(
            "repark.cdc stamp of query {query} on snapshot {id} misses its format version",
            id = snapshot.snapshot_id()
        ))),
    }
}

fn off_lineage_stamp(table: &Table, query: QueryId, property: &SinkRecord) -> Option<SnapshotId> {
    let metadata = table.metadata();
    let stamped = metadata.snapshots().find(|snapshot| {
        let summary = &snapshot.summary().additional_properties;
        stamped_by(summary, query)
            && matches!(SinkRecord::from_summary(summary), Ok(Some(record)) if record == *property)
    })?;
    let (oldest, reaches_root) = oldest_reachable_ancestor(metadata)?;
    if reaches_root || commit_order(metadata, stamped) > commit_order(metadata, oldest) {
        return Some(SnapshotId::new(stamped.snapshot_id()));
    }
    None
}

fn oldest_reachable_ancestor(metadata: &TableMetadata) -> Option<(&SnapshotRef, bool)> {
    let mut cursor = metadata.current_snapshot()?;
    for _ in 0..metadata.snapshots().len() {
        let Some(parent) = cursor.parent_snapshot_id() else {
            return Some((cursor, true));
        };
        match metadata.snapshot_by_id(parent) {
            Some(next) => cursor = next,
            None => return Some((cursor, false)),
        }
    }
    None
}

fn commit_order(metadata: &TableMetadata, snapshot: &Snapshot) -> i64 {
    match metadata.format_version() {
        FormatVersion::V1 => snapshot.timestamp_ms(),
        _ => snapshot.sequence_number(),
    }
}

fn stamped_by(summary: &HashMap<String, String>, query: QueryId) -> bool {
    summary
        .get(QUERY_ID_KEY)
        .and_then(|text| Uuid::parse_str(text).ok())
        .is_some_and(|stamped| stamped == query.get())
}

fn property_record(table: &Table, query: QueryId) -> Result<Option<SinkRecord>, MicroBatchError> {
    let key = format!("{OFFSETS_PROPERTY_PREFIX}{query}");
    let Some(value) = table.metadata().properties().get(&key) else {
        return Ok(None);
    };
    if StartingMark::from_property(value)?.is_some() {
        return Ok(None);
    }
    SinkRecord::from_property(query, value).map(Some)
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_stamp_only(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    stamp: &CommitStamp,
    token: Option<&ScopeToken>,
) -> Result<SnapshotId, MicroBatchError> {
    if crate::write::carries_encryption_key(table.metadata().properties()) {
        return Err(crate::write::EncryptedTableRefusal::of(table.identifier()).into());
    }
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
    if active.is_none() {
        epoch_check(table, stamp)?;
    }
    let claimed = active.unwrap_or_else(|| ClaimedStamp::on(table, stamp.clone()));
    let engine = EngineSummary::for_append(table, &[], None);
    let (operation_id, mut summary) =
        summary_with_extras(&[], &engine).map_err(|error| masked(&error))?;
    summary.extend(claimed.summary_entries()?);
    let tx = Transaction::new(table);
    let tx = tx
        .merge_append()
        .set_snapshot_properties(summary)
        .apply(tx)
        .map_err(|error| masked(&error))?;
    let tx = claimed.stamp_transaction(tx)?;
    let fenced = AppendFence::install(catalog, &claimed);
    let committed = match tx.commit(fenced.as_ref()).await {
        Ok(committed) => committed,
        Err(error) if error.kind() == ErrorKind::CommitStateUnknown => {
            return resolve_unknown_outcome(catalog, table, stamp, Some(&operation_id)).await;
        }
        Err(error) => {
            return Err(append_fence::refusal_of(&error).unwrap_or_else(|| masked(&error)));
        }
    };
    let snapshot = committed
        .metadata()
        .current_snapshot_id()
        .map(SnapshotId::new);
    claimed.record_commit(&committed)?;
    snapshot
        .ok_or_else(|| MicroBatchError::Catalog(String::from("stamp-only commit left no snapshot")))
}

#[allow(clippy::missing_errors_doc)]
pub async fn resolve_unknown_outcome(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    stamp: &CommitStamp,
    operation_id: Option<&str>,
) -> Result<SnapshotId, MicroBatchError> {
    let record = &stamp.record;
    let unknown = |durable: Option<Box<SinkRecord>>, resume_refusal: Option<RecoveryReason>| {
        MicroBatchError::RecoveryRequired {
            query: record.query,
            epoch: record.epoch,
            durable,
            reason: RecoveryReason::CommitOutcomeUnknown {
                operation_id: operation_id.map(str::to_string),
                resume_refusal: resume_refusal.map(Box::new),
            },
        }
    };
    let Ok(reloaded) = catalog.load_table(table.identifier()).await else {
        return Err(unknown(None, None));
    };
    let base = table.metadata().current_snapshot_id();
    if let Some(snapshot) = landed_attempt(&reloaded, base, record, operation_id) {
        mark_committed(&reloaded, stamp, snapshot);
        return Ok(snapshot);
    }
    match read_resume_point(&reloaded, record.query) {
        Ok(durable) => Err(unknown(durable.map(Box::new), None)),
        Err(MicroBatchError::RecoveryRequired {
            durable, reason, ..
        }) => Err(unknown(durable, Some(reason))),
        Err(error) => Err(error),
    }
}

fn landed_attempt(
    reloaded: &Table,
    base: Option<i64>,
    record: &SinkRecord,
    operation_id: Option<&str>,
) -> Option<SnapshotId> {
    let metadata = reloaded.metadata();
    let mut above_base = Vec::new();
    let mut cursor = metadata.current_snapshot();
    while let Some(snapshot) = cursor
        && Some(snapshot.snapshot_id()) != base
        && above_base.len() < metadata.snapshots().len()
    {
        above_base.push(snapshot);
        cursor = snapshot
            .parent_snapshot_id()
            .and_then(|parent| metadata.snapshot_by_id(parent));
    }
    let by_operation = operation_id.and_then(|id| {
        above_base.iter().find(|snapshot| {
            snapshot
                .summary()
                .additional_properties
                .get(OPERATION_ID_PROP)
                .is_some_and(|found| found == id)
        })
    });
    let landed = by_operation.or_else(|| {
        above_base.iter().find(|snapshot| {
            matches!(
                SinkRecord::from_summary(&snapshot.summary().additional_properties),
                Ok(Some(found)) if found == *record
            )
        })
    })?;
    Some(SnapshotId::new(landed.snapshot_id()))
}

#[derive(Debug, Default)]
pub(crate) struct SiteStamp {
    claimed: Option<ClaimedStamp>,
    sink: Option<Table>,
    attempted: AtomicBool,
}

impl Drop for SiteStamp {
    fn drop(&mut self) {
        if !self.attempted.load(Ordering::SeqCst) {
            self.release();
        }
    }
}

impl SiteStamp {
    pub(crate) fn claim(
        table: &Table,
        branch: Option<&str>,
        extra: &[(String, String)],
    ) -> datafusion::error::Result<SiteStamp> {
        Self::claim_with(table, branch, extra, || Ok(()))
    }

    pub(crate) fn claim_isolated(
        table: &Table,
        branch: Option<&str>,
        extra: &[(String, String)],
        scope: &CommitScope,
    ) -> datafusion::error::Result<SiteStamp> {
        Self::claim_with(table, branch, extra, || match scope.isolation {
            IsolationLevel::Serializable => Ok(()),
            IsolationLevel::Snapshot => Err(MicroBatchError::MergeIsolationRefused {
                sink: table.identifier().to_string(),
                property: scope.isolation_property.to_string(),
            }),
        })
    }

    fn claim_with(
        table: &Table,
        branch: Option<&str>,
        extra: &[(String, String)],
        isolation: impl FnOnce() -> Result<(), MicroBatchError>,
    ) -> datafusion::error::Result<SiteStamp> {
        if branch.is_some_and(|name| name != MAIN_BRANCH) {
            return Ok(SiteStamp::default());
        }
        let Some(token) =
            ScopeToken::carried_by(extra).or_else(|| body_scope::ambient_token(table))
        else {
            return Ok(SiteStamp::default());
        };
        let claimed = BatchScope::claim_checked(table, &token, |stamp| {
            refuse_stamp_keys(extra, stamp)?;
            isolation()
        })
        .map_err(microbatch_error)?;
        Ok(SiteStamp {
            sink: claimed.is_some().then(|| table.clone()),
            claimed,
            attempted: AtomicBool::new(false),
        })
    }

    fn release(&self) {
        if let (Some(claimed), Some(sink)) = (&self.claimed, &self.sink) {
            release_claim(sink, &claimed.stamp);
        }
    }

    pub(crate) fn attempt(&self) {
        self.attempted.store(true, Ordering::SeqCst);
    }

    pub(crate) fn failed(&self, error: &iceberg::Error) {
        if error.kind() != ErrorKind::CommitStateUnknown {
            self.release();
        }
    }

    pub(crate) fn summary(
        &self,
        extra: &[(String, String)],
        engine: &EngineSummary,
    ) -> datafusion::error::Result<(String, HashMap<String, String>)> {
        let (operation_id, mut summary) = summary_with_extras(extra, engine)?;
        if let Some(claimed) = &self.claimed {
            summary.extend(claimed.summary_entries().map_err(microbatch_error)?);
        }
        Ok((operation_id, summary))
    }

    pub(crate) fn transaction(&self, tx: Transaction) -> datafusion::error::Result<Transaction> {
        match &self.claimed {
            Some(claimed) => claimed.stamp_transaction(tx).map_err(microbatch_error),
            None => Ok(tx),
        }
    }

    pub(crate) fn fenced(&self, catalog: &Arc<dyn Catalog>) -> Arc<dyn Catalog> {
        match &self.claimed {
            Some(claimed) => AppendFence::install(catalog, claimed),
            None => Arc::clone(catalog),
        }
    }

    pub(crate) async fn commit_append(
        &self,
        tx: Transaction,
        catalog: &Arc<dyn Catalog>,
    ) -> datafusion::error::Result<iceberg::Result<Table>> {
        self.attempt();
        let result = tx.commit(self.fenced(catalog).as_ref()).await;
        match (&self.claimed, result) {
            (Some(_), Err(error)) => {
                self.failed(&error);
                match append_fence::refusal_of(&error) {
                    Some(refusal) => Err(microbatch_error(refusal)),
                    None => Ok(Err(error)),
                }
            }
            (_, result) => Ok(result),
        }
    }

    pub(crate) fn record(mut self, committed: &Table) -> datafusion::error::Result<()> {
        self.attempt();
        match self.claimed.take() {
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
