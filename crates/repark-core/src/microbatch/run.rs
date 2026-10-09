use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use datafusion::error::DataFusionError;
use datafusion::prelude::{DataFrame, SessionContext};
use futures::FutureExt;
use iceberg::table::Table;
use repark_common::Generation;
use repark_common::redaction::mask_value_credentials;
use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
use repark_iceberg::microbatch::offset::{
    Epoch, InputOffset, OffsetFormatVersion, OffsetVector, SinkDoor, SinkRecord, SnapshotId,
    TableUuid,
};
use repark_iceberg::microbatch::window::WindowLimit;
use repark_iceberg::write::sink_offsets::{
    BatchScope, BatchScopeGuard, CommitStamp, SCOPE_TOKEN_KEY, ScopeOutcome, SinkMark,
    commit_stamp_only, read_resume_point, resolve_unknown_outcome, unstamped_since_stamp,
};
use repark_iceberg::write::{
    CommitStateUnknownError, SESSION_SNAPSHOT_PREFIX, apply_session_write_key,
    commit_append_with_summary, concurrency_from_ctx, is_commit_state_unknown,
    resolve_empty_session_write, stage_overwrite_files_with,
};
use tokio::time::Instant;

use crate::microbatch::driver::{
    BatchBody, Ending, LOAD_SINK, Pending, QueryShared, Trigger, bounded,
};
use crate::microbatch::progress::TriggerReport;
use crate::microbatch::relation::PlanTemplate;
use crate::time_travel::microbatch_source::{MicroBatchSource, SourceBatch, WeakSessionState};

#[derive(Clone)]
pub(crate) enum Door {
    Table,
    ForeachBatch(Arc<dyn BatchBody>),
}

impl Door {
    fn kind(&self) -> SinkDoor {
        match self {
            Door::Table => SinkDoor::Table,
            Door::ForeachBatch(_) => SinkDoor::ForeachBatch,
        }
    }
}

struct BatchDone {
    add_batch: Duration,
    num_output_rows: Option<u64>,
}

struct Cursor {
    generation: Generation,
    epoch: Epoch,
    from: Option<InputOffset>,
    baseline: Option<i64>,
}

enum Wake {
    Tick,
    Stop,
}

pub(crate) struct Run {
    shared: Arc<QueryShared>,
    source: MicroBatchSource,
    plan: Option<PlanTemplate>,
    context: WeakSessionState,
    door: Door,
}

impl Run {
    pub(crate) fn new(shared: Arc<QueryShared>, pending: Pending) -> Run {
        Run {
            shared,
            source: pending.source,
            plan: pending.plan,
            context: pending.context,
            door: pending.door,
        }
    }

    pub(crate) async fn drive(self) {
        let ending = match AssertUnwindSafe(self.trigger_loop()).catch_unwind().await {
            Ok(ending) => ending,
            Err(panic) => Err(self.shared.panicked(panic.as_ref())),
        };
        self.shared.finish(ending);
    }

    async fn load_sink(&self) -> Result<Table, MicroBatchError> {
        bounded(
            self.shared.catalog_timeout,
            LOAD_SINK,
            self.shared.sink.load(),
        )
        .await
    }

    async fn available_now_target(
        &self,
        cursor: &mut Cursor,
    ) -> Result<Option<InputOffset>, MicroBatchError> {
        let Some(from) = self.start_offset(cursor).await? else {
            return Ok(None);
        };
        self.source.available_now_target(&from).await
    }

    async fn trigger_loop(&self) -> Result<Ending, MicroBatchError> {
        let sink = self.load_sink().await?;
        let resumed = read_resume_point(&sink, self.shared.id)?;
        self.shared.resumed(resumed.clone());
        let mut cursor = self.resume(resumed.as_ref())?;
        if resumed.is_none() {
            cursor.baseline = sink.metadata().current_snapshot_id();
        }
        self.refuse_moved_sink(&sink, &cursor)?;
        let target = match self.shared.trigger {
            Trigger::AvailableNow => self.available_now_target(&mut cursor).await?,
            Trigger::Once | Trigger::ProcessingTime(_) => None,
        };
        let mut ran_batch = false;
        loop {
            if self.shared.stop_requested() {
                return Ok(Ending::Stopped);
            }
            let started = Instant::now();
            let started_at = SystemTime::now();
            self.shared.progress().trigger_started();
            let batch = match self.next_batch(&mut cursor, target.as_ref()).await {
                Err(_) if self.shared.session_ended() => return Ok(Ending::Stopped),
                planned => planned?,
            };
            let planned = Instant::now();
            let found = batch.is_some();
            if found && self.shared.stop_requested() {
                return Ok(Ending::Stopped);
            }
            let report = if let Some(batch) = batch {
                self.shared.progress().data_found();
                let epoch = cursor.epoch;
                let start_offset = self.shared.durable().map(|_| batch.start.clone());
                let end_offset = Some(batch.end.clone());
                let num_input_rows = batch.num_input_rows;
                let Some(done) = self.run_batch(&mut cursor, batch).await? else {
                    return Ok(Ending::Stopped);
                };
                TriggerReport {
                    executed: true,
                    epoch,
                    started_at,
                    started,
                    planned,
                    finished: Instant::now(),
                    add_batch: done.add_batch,
                    num_input_rows,
                    start_offset,
                    end_offset,
                    num_output_rows: done.num_output_rows,
                }
            } else {
                let committed = self.shared.durable().and(cursor.from.clone());
                TriggerReport {
                    executed: false,
                    epoch: cursor.epoch,
                    started_at,
                    started,
                    planned,
                    finished: Instant::now(),
                    add_batch: Duration::ZERO,
                    num_input_rows: 0,
                    start_offset: committed.clone(),
                    end_offset: committed,
                    num_output_rows: None,
                }
            };
            let draining = matches!(self.shared.trigger, Trigger::AvailableNow | Trigger::Once);
            if found || !(draining && ran_batch) {
                self.shared.report(&report);
            }
            ran_batch |= found;
            self.shared.progress().trigger_finished(found);
            let next = match self.shared.trigger {
                Trigger::AvailableNow if found => continue,
                Trigger::Once | Trigger::AvailableNow => return Ok(Ending::Drained),
                Trigger::ProcessingTime(interval) if interval.is_zero() => {
                    if found {
                        continue;
                    }
                    Instant::now() + self.shared.polling_delay
                }
                Trigger::ProcessingTime(interval) => {
                    started + until_next_trigger(started_at, interval)
                }
            };
            if let Wake::Stop = self.wait_until(next).await {
                return Ok(Ending::Stopped);
            }
        }
    }

    async fn wait_until(&self, deadline: Instant) -> Wake {
        let mut stop = self.shared.stop.subscribe();
        if *stop.borrow_and_update() {
            return Wake::Stop;
        }
        tokio::select! {
            () = tokio::time::sleep_until(deadline) => Wake::Tick,
            _ = stop.wait_for(|stopped| *stopped) => Wake::Stop,
            () = self.shared.session_gone() => Wake::Stop,
        }
    }

    async fn enter_scope(
        &self,
        stamp: &CommitStamp,
    ) -> Result<Option<(Table, BatchScopeGuard)>, MicroBatchError> {
        let deadline = Instant::now() + self.shared.catalog_timeout;
        loop {
            let sink = self.load_sink().await?;
            match BatchScope::enter(TableUuid::of(&sink), stamp.clone()) {
                Ok(guard) => return Ok(Some((sink, guard))),
                Err(MicroBatchError::SinkBusy { .. }) if Instant::now() < deadline => {
                    let retry = (Instant::now() + self.shared.polling_delay).min(deadline);
                    if let Wake::Stop = self.wait_until(retry).await {
                        return Ok(None);
                    }
                }
                Err(MicroBatchError::SinkBusy { .. }) => {
                    return Err(MicroBatchError::SinkBusy {
                        sink: self.shared.sink.name.clone(),
                    });
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn resume(&self, record: Option<&SinkRecord>) -> Result<Cursor, MicroBatchError> {
        let Some(record) = record else {
            let generation = Generation::new(1).ok_or_else(|| {
                MicroBatchError::Catalog(String::from("generation 1 is not a valid generation"))
            })?;
            return Ok(Cursor {
                generation,
                epoch: Epoch::FIRST,
                from: None,
                baseline: None,
            });
        };
        let current = self.source.table_identifier();
        let same_input = match record.offsets.inputs() {
            [input] => input.table == self.source.table_uuid() || input.table_name == current,
            _ => false,
        };
        if !same_input {
            return Err(MicroBatchError::InputsChanged {
                query: self.shared.id,
                recorded: record
                    .offsets
                    .inputs()
                    .iter()
                    .map(|input| input.table_name.clone())
                    .collect(),
                current: vec![current],
            });
        }
        Ok(Cursor {
            generation: record.generation,
            epoch: record.epoch.next(),
            from: record.offsets.inputs().first().cloned(),
            baseline: None,
        })
    }

    fn recovery(&self, epoch: Epoch, reason: RecoveryReason) -> MicroBatchError {
        MicroBatchError::RecoveryRequired {
            query: self.shared.id,
            epoch,
            durable: self.shared.durable().map(Box::new),
            reason,
        }
    }

    fn refuse_moved_sink(&self, sink: &Table, cursor: &Cursor) -> Result<(), MicroBatchError> {
        if !matches!(self.door, Door::ForeachBatch(_)) {
            return Ok(());
        }
        let found = TableUuid::of(sink);
        if found != self.shared.sink_uuid {
            return Err(self.recovery(
                cursor.epoch,
                RecoveryReason::UnstampedSinkChange {
                    what: format!(
                        "the table under the sink's name was replaced (uuid {was}, now {found})",
                        was = self.shared.sink_uuid
                    ),
                },
            ));
        }
        match unstamped_since_stamp(sink, self.shared.id, cursor.baseline) {
            Some(reason) => Err(self.recovery(cursor.epoch, reason)),
            None => Ok(()),
        }
    }

    async fn start_offset(
        &self,
        cursor: &mut Cursor,
    ) -> Result<Option<InputOffset>, MicroBatchError> {
        if cursor.from.is_none() {
            cursor.from = self.source.initial_offset().await?;
        }
        Ok(cursor.from.clone())
    }

    async fn next_batch(
        &self,
        cursor: &mut Cursor,
        target: Option<&InputOffset>,
    ) -> Result<Option<SourceBatch>, MicroBatchError> {
        let Some(from) = self.start_offset(cursor).await? else {
            return Ok(None);
        };
        match (self.shared.trigger, target) {
            (Trigger::AvailableNow, Some(target)) => {
                self.source.next_batch_until(&from, target).await
            }
            (Trigger::AvailableNow, None) => Ok(None),
            (Trigger::Once, _) => self.source.next_batch(&from, WindowLimit::Unbounded).await,
            (Trigger::ProcessingTime(_), _) => {
                self.source.next_batch(&from, WindowLimit::Capped).await
            }
        }
    }

    async fn run_batch(
        &self,
        cursor: &mut Cursor,
        batch: SourceBatch,
    ) -> Result<Option<BatchDone>, MicroBatchError> {
        let epoch = cursor.epoch;
        self.shared.begin_batch(epoch);
        let record = SinkRecord {
            format: OffsetFormatVersion::CURRENT,
            query: self.shared.id,
            run: self.shared.run_id,
            epoch,
            generation: cursor.generation,
            offsets: OffsetVector::single(batch.end.clone()),
        };
        let stamp = CommitStamp {
            record: record.clone(),
            door: self.door.kind(),
        };
        let Some((sink, guard)) = self.enter_scope(&stamp).await? else {
            return Ok(None);
        };
        if let Some(durable) = read_resume_point(&sink, self.shared.id)?
            && durable.epoch.get() >= epoch.get()
        {
            self.already_durable(cursor, &durable)?;
            return Ok(Some(BatchDone {
                add_batch: Duration::ZERO,
                num_output_rows: None,
            }));
        }
        self.refuse_moved_sink(&sink, cursor)?;
        let frame = match &self.plan {
            Some(template) => template.bind(&batch)?,
            None => batch.frame,
        };
        let door_started = Instant::now();
        let committed = match &self.door {
            Door::Table => {
                let Some(batch) = self.batch_session(&guard) else {
                    return Ok(None);
                };
                let appended = self.append(&batch, &stamp, &sink, frame).await;
                appended.map(|(rows, snapshot)| (Some(rows), snapshot))
            }
            Door::ForeachBatch(body) => self
                .foreach_batch(&guard, &stamp, &sink, body.as_ref(), frame)
                .await
                .map(|snapshot| (None, snapshot)),
        };
        let add_batch = door_started.elapsed();
        let outcome = guard.outcome();
        drop(guard);
        let (num_output_rows, snapshot) = committed?;
        if let ScopeOutcome::NotCommitted = outcome {
            return Err(MicroBatchError::RecoveryRequired {
                query: self.shared.id,
                epoch,
                durable: self.shared.durable().map(Box::new),
                reason: RecoveryReason::UnstampedSinkCommit {
                    snapshot,
                    operation: None,
                },
            });
        }
        self.shared.end_batch(Some(record));
        cursor.epoch = epoch.next();
        cursor.from = Some(batch.end);
        Ok(Some(BatchDone {
            add_batch,
            num_output_rows,
        }))
    }

    fn already_durable(
        &self,
        cursor: &mut Cursor,
        durable: &SinkRecord,
    ) -> Result<(), MicroBatchError> {
        self.shared.resumed(Some(durable.clone()));
        if durable.generation != cursor.generation {
            return Err(MicroBatchError::GenerationMismatch {
                query: self.shared.id,
                resumed: cursor.generation,
                stamped: durable.generation,
            });
        }
        if durable.run != self.shared.run_id {
            return Err(MicroBatchError::Fenced {
                query: self.shared.id,
                epoch: cursor.epoch,
                winner: durable.run,
            });
        }
        cursor.epoch = durable.epoch.next();
        cursor.from = durable.offsets.inputs().first().cloned();
        self.shared.end_batch(None);
        Ok(())
    }

    fn batch_session(&self, guard: &BatchScopeGuard) -> Option<SessionContext> {
        let batch = SessionContext::new_with_state(self.context.snapshot()?);
        {
            let state = batch.state_ref();
            let mut state = state.write();
            let _ = apply_session_write_key(
                state.config_mut().options_mut(),
                &format!("{SESSION_SNAPSHOT_PREFIX}{SCOPE_TOKEN_KEY}"),
                &guard.token().to_string(),
            );
        }
        Some(batch)
    }

    async fn resolve_unknown(
        &self,
        sink: &Table,
        stamp: &CommitStamp,
        operation_id: Option<&str>,
    ) -> Result<SnapshotId, MicroBatchError> {
        let catalog = &self.shared.sink.catalog;
        let walk = resolve_unknown_outcome(catalog, sink, stamp, operation_id);
        match tokio::time::timeout(self.shared.catalog_timeout, walk).await {
            Ok(resolved) => resolved,
            Err(_) => Err(MicroBatchError::RecoveryRequired {
                query: self.shared.id,
                epoch: stamp.record.epoch,
                durable: self.shared.durable().map(Box::new),
                reason: RecoveryReason::CommitOutcomeUnknown {
                    operation_id: operation_id.map(str::to_string),
                    resume_refusal: None,
                },
            }),
        }
    }

    async fn foreach_batch(
        &self,
        guard: &BatchScopeGuard,
        stamp: &CommitStamp,
        base: &Table,
        body: &dyn BatchBody,
        frame: DataFrame,
    ) -> Result<SnapshotId, MicroBatchError> {
        let epoch = stamp.record.epoch;
        let mark = SinkMark::of(base);
        let ran = guard.scope_body(body.run(frame, epoch)).await;
        let landed = match guard.outcome() {
            ScopeOutcome::Committed { snapshot } => Some(snapshot),
            ScopeOutcome::NotCommitted if guard.outcome_unknown() => {
                Some(self.resolve_unknown(base, stamp, None).await?)
            }
            ScopeOutcome::NotCommitted => None,
        };
        let sink = self.load_sink().await?;
        if let Some(reason) = mark.violation(&sink) {
            return Err(self.recovery(epoch, reason));
        }
        if let Err(error) = ran {
            if landed.is_some() {
                self.shared.end_batch(Some(stamp.record.clone()));
            }
            return Err(guard
                .body_refusal()
                .unwrap_or_else(|| MicroBatchError::BatchFailed {
                    epoch,
                    cause: mask_value_credentials(&error.to_string()),
                }));
        }
        if let Some(snapshot) = landed {
            return Ok(snapshot);
        }
        let commit =
            commit_stamp_only(&self.shared.sink.catalog, &sink, stamp, Some(guard.token()));
        match tokio::time::timeout(self.shared.catalog_timeout, commit).await {
            Ok(committed) => committed,
            Err(_) => self.resolve_unknown(&sink, stamp, None).await,
        }
    }

    async fn append(
        &self,
        batch: &SessionContext,
        stamp: &CommitStamp,
        sink: &Table,
        frame: DataFrame,
    ) -> Result<(u64, SnapshotId), MicroBatchError> {
        let (extra, mut staging) =
            resolve_empty_session_write(batch).map_err(|error| engine_error(&error))?;
        staging.fork_insert_dictionary_rule = true;
        let stream = frame
            .execute_stream()
            .await
            .map_err(|error| engine_error(&error))?;
        let files = stage_overwrite_files_with(
            sink,
            stream,
            Vec::new(),
            concurrency_from_ctx(batch),
            &staging,
        )
        .await
        .map_err(|error| engine_error(&error))?;
        let rows = files
            .iter()
            .fold(0u64, |rows, file| rows.saturating_add(file.record_count()));
        let catalog = &self.shared.sink.catalog;
        let commit = commit_append_with_summary(catalog, sink, files, &extra, None);
        let resolved = match tokio::time::timeout(self.shared.catalog_timeout, commit).await {
            Ok(Ok(committed)) => {
                let head = committed.metadata().current_snapshot_id();
                return head
                    .map(|snapshot| (rows, SnapshotId::new(snapshot)))
                    .ok_or_else(|| {
                        MicroBatchError::Catalog(format!(
                            "batch {epoch} of query {query} left the sink without a snapshot",
                            epoch = stamp.record.epoch,
                            query = self.shared.id
                        ))
                    });
            }
            Ok(Err(error)) if is_commit_state_unknown(&error) => {
                let operation_id = unknown_operation_id(&error);
                self.resolve_unknown(sink, stamp, operation_id.as_deref())
                    .await
            }
            Ok(Err(error)) => return Err(engine_error(&error)),
            Err(_) => self.resolve_unknown(sink, stamp, None).await,
        };
        resolved.map(|snapshot| (rows, snapshot))
    }
}

pub(crate) fn until_next_trigger(started_at: SystemTime, interval: Duration) -> Duration {
    let interval_ms = interval.as_millis().max(1);
    let started_ms = started_at
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis());
    let next_ms = started_ms / interval_ms * interval_ms + interval_ms;
    Duration::from_millis(u64::try_from(next_ms - started_ms).unwrap_or(u64::MAX))
}

fn unknown_operation_id(error: &DataFusionError) -> Option<String> {
    match error.find_root() {
        DataFusionError::External(inner) => inner
            .downcast_ref::<CommitStateUnknownError>()
            .map(|unknown| unknown.operation_id().to_string()),
        _ => None,
    }
}

pub(crate) fn engine_error(error: &DataFusionError) -> MicroBatchError {
    if let DataFusionError::External(inner) = error.find_root()
        && let Some(found) = inner.downcast_ref::<MicroBatchError>()
    {
        return found.clone();
    }
    MicroBatchError::Catalog(mask_value_credentials(&error.to_string()))
}

#[cfg(test)]
#[path = "run_tests.rs"]
mod tests;
