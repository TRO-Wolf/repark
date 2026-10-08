use std::sync::Arc;
use std::time::{Duration, SystemTime};

use datafusion::error::DataFusionError;
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::table::Table;
use repark_common::Generation;
use repark_common::redaction::mask_value_credentials;
use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
use repark_iceberg::microbatch::offset::{
    Epoch, InputOffset, OffsetFormatVersion, OffsetVector, SinkDoor, SinkRecord, TableUuid,
};
use repark_iceberg::microbatch::window::WindowLimit;
use repark_iceberg::write::sink_offsets::{
    BatchScope, BatchScopeGuard, CommitStamp, SCOPE_TOKEN_KEY, ScopeOutcome, commit_stamp_only,
    read_resume_point, resolve_unknown_outcome,
};
use repark_iceberg::write::{
    CommitStateUnknownError, SESSION_SNAPSHOT_PREFIX, apply_session_write_key,
    commit_append_with_summary, concurrency_from_ctx, is_commit_state_unknown,
    resolve_empty_session_write, stage_overwrite_files_with,
};
use tokio::time::Instant;

use crate::microbatch::driver::{BatchBody, Ending, Pending, QueryShared, Trigger};
use crate::microbatch::progress::TriggerReport;
use crate::time_travel::microbatch_source::{MicroBatchSource, SourceBatch};

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
}

enum Wake {
    Tick,
    Stop,
}

pub(crate) struct Run {
    shared: Arc<QueryShared>,
    source: MicroBatchSource,
    context: SessionContext,
    door: Door,
}

impl Run {
    pub(crate) fn new(shared: Arc<QueryShared>, pending: Pending) -> Run {
        Run {
            shared,
            source: pending.source,
            context: pending.context,
            door: pending.door,
        }
    }

    pub(crate) async fn drive(self) {
        let ending = self.trigger_loop().await;
        self.shared.finish(ending);
    }

    async fn trigger_loop(&self) -> Result<Ending, MicroBatchError> {
        let sink = self.shared.sink.load().await?;
        let resumed = read_resume_point(&sink, self.shared.id)?;
        self.shared.resumed(resumed.clone());
        let mut cursor = self.resume(resumed.as_ref())?;
        let target = match self.shared.trigger {
            Trigger::AvailableNow => match self.start_offset(&mut cursor).await? {
                Some(from) => self.source.available_now_target(&from).await?,
                None => None,
            },
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
            let batch = self.next_batch(&mut cursor, target.as_ref()).await?;
            let planned = Instant::now();
            let found = batch.is_some();
            let report = if let Some(batch) = batch {
                self.shared.progress().data_found();
                let epoch = cursor.epoch;
                let start_offset = self.shared.durable().map(|_| batch.start.clone());
                let end_offset = Some(batch.end.clone());
                let num_input_rows = batch.num_input_rows;
                let done = self.run_batch(&mut cursor, batch).await?;
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
                Trigger::ProcessingTime(interval) => started + interval,
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
        })
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
    ) -> Result<BatchDone, MicroBatchError> {
        let epoch = cursor.epoch;
        self.shared.begin_batch(epoch);
        let sink = self.shared.sink.load().await?;
        if let Some(durable) = read_resume_point(&sink, self.shared.id)?
            && durable.epoch.get() >= epoch.get()
        {
            self.already_durable(cursor, &durable)?;
            return Ok(BatchDone {
                add_batch: Duration::ZERO,
                num_output_rows: None,
            });
        }
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
        let guard = BatchScope::enter(TableUuid::of(&sink), stamp.clone())?;
        let door_started = Instant::now();
        let committed = match &self.door {
            Door::Table => self
                .append(&guard, &stamp, &sink, batch.frame)
                .await
                .map(Some),
            Door::ForeachBatch(body) => self
                .foreach_batch(&guard, &stamp, body.as_ref(), batch.frame)
                .await
                .map(|()| None),
        };
        let add_batch = door_started.elapsed();
        let outcome = guard.outcome();
        drop(guard);
        let num_output_rows = committed?;
        if let ScopeOutcome::NotCommitted = outcome {
            return Err(self.unstamped(epoch).await);
        }
        self.shared.end_batch(Some(record));
        cursor.epoch = epoch.next();
        cursor.from = Some(batch.end);
        Ok(BatchDone {
            add_batch,
            num_output_rows,
        })
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

    async fn unstamped(&self, epoch: Epoch) -> MicroBatchError {
        let snapshot = match self.shared.sink.load().await {
            Ok(table) => table.metadata().current_snapshot_id(),
            Err(error) => return error,
        };
        match snapshot {
            Some(snapshot) => MicroBatchError::RecoveryRequired {
                query: self.shared.id,
                epoch,
                durable: self.shared.durable().map(Box::new),
                reason: RecoveryReason::UnstampedSinkCommit {
                    snapshot: repark_iceberg::microbatch::offset::SnapshotId::new(snapshot),
                },
            },
            None => MicroBatchError::Catalog(format!(
                "batch {epoch} of query {query} left the sink without a snapshot",
                query = self.shared.id
            )),
        }
    }

    fn batch_session(&self, guard: &BatchScopeGuard) -> SessionContext {
        let batch = SessionContext::new_with_state(self.context.state());
        {
            let state = batch.state_ref();
            let mut state = state.write();
            let _ = apply_session_write_key(
                state.config_mut().options_mut(),
                &format!("{SESSION_SNAPSHOT_PREFIX}{SCOPE_TOKEN_KEY}"),
                &guard.token().to_string(),
            );
        }
        batch
    }

    async fn foreach_batch(
        &self,
        guard: &BatchScopeGuard,
        stamp: &CommitStamp,
        body: &dyn BatchBody,
        frame: DataFrame,
    ) -> Result<(), MicroBatchError> {
        let epoch = stamp.record.epoch;
        body.run(frame, epoch)
            .await
            .map_err(|error| MicroBatchError::BatchFailed {
                epoch,
                cause: mask_value_credentials(&error.to_string()),
            })?;
        let sink = self.shared.sink.load().await?;
        commit_stamp_only(&self.shared.sink.catalog, &sink, stamp, Some(guard.token()))
            .await
            .map(|_| ())
    }

    async fn append(
        &self,
        guard: &BatchScopeGuard,
        stamp: &CommitStamp,
        sink: &Table,
        frame: DataFrame,
    ) -> Result<u64, MicroBatchError> {
        let batch = self.batch_session(guard);
        let (extra, mut staging) =
            resolve_empty_session_write(&batch).map_err(|error| engine_error(&error))?;
        staging.fork_insert_dictionary_rule = true;
        let stream = frame
            .execute_stream()
            .await
            .map_err(|error| engine_error(&error))?;
        let files = stage_overwrite_files_with(
            sink,
            stream,
            Vec::new(),
            concurrency_from_ctx(&batch),
            &staging,
        )
        .await
        .map_err(|error| engine_error(&error))?;
        let rows = files
            .iter()
            .fold(0u64, |rows, file| rows.saturating_add(file.record_count()));
        let catalog = &self.shared.sink.catalog;
        match commit_append_with_summary(catalog, sink, files, &extra, None).await {
            Ok(_) => Ok(rows),
            Err(error) if is_commit_state_unknown(&error) => {
                let operation_id = unknown_operation_id(&error);
                resolve_unknown_outcome(catalog, sink, stamp, operation_id.as_deref())
                    .await
                    .map(|_| rows)
            }
            Err(error) => Err(engine_error(&error)),
        }
    }
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
