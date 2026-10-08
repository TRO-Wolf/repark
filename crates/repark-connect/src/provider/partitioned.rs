use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use tokio::sync::mpsc::{Receiver, Sender, channel};

use super::scan::{ScanPlan, place_until_refusal};
use super::schema::external;
use crate::read::postgres::{ScanMeter, ScanRequest};
use crate::read::postgres_lanes::{LaneStream, scan_lanes};

pub(crate) struct Partitioned {
    pub(crate) column: Arc<str>,
    pub(crate) strides: Vec<ScanRequest>,
    pub(crate) max_connections: usize,
}

type Sent = Result<(RecordBatch, MemoryReservation)>;

async fn drive(
    mut lane: LaneStream,
    plan: Arc<ScanPlan>,
    sender: Sender<Sent>,
    reservation: MemoryReservation,
) {
    while let Some(batch) = lane.next().await {
        for placed in place_until_refusal(&plan, batch) {
            let sent = placed.map_err(external).and_then(|batch| {
                let size = batch.get_array_memory_size();
                reservation.try_grow(size)?;
                Ok((batch, reservation.split(size)))
            });
            let failed = sent.is_err();
            if sender.send(sent).await.is_err() || failed {
                return;
            }
        }
    }
}

struct Start {
    plan: Arc<ScanPlan>,
    meter: ScanMeter,
    memory: Arc<dyn MemoryPool>,
    baseline: BaselineMetrics,
}

struct Running {
    receiver: Receiver<Sent>,
    tasks: Vec<SpawnedTask<()>>,
    held: Option<MemoryReservation>,
    remaining: Option<u64>,
    baseline: BaselineMetrics,
}

enum Merge {
    Start(Start),
    Running(Running),
}

impl Start {
    async fn open(self) -> Result<Running> {
        let Start {
            plan,
            meter,
            memory,
            baseline,
        } = self;
        let Some(partition) = &plan.partition else {
            return Err(DataFusionError::Internal(
                "a partitioned stream needs a partition plan".to_string(),
            ));
        };
        let lanes = scan_lanes(
            Arc::clone(&plan.pool),
            partition.strides.clone(),
            partition.max_connections,
            plan.options,
            meter,
        )
        .await
        .map_err(external)?;
        let (sender, receiver) = channel(lanes.len().max(1));
        let tasks = lanes
            .into_iter()
            .map(|lane| {
                let reservation = MemoryConsumer::new("PostgresScan").register(&memory);
                SpawnedTask::spawn(drive(lane, Arc::clone(&plan), sender.clone(), reservation))
            })
            .collect();
        Ok(Running {
            receiver,
            tasks,
            held: None,
            remaining: plan.limit,
            baseline,
        })
    }
}

impl Running {
    fn finished(&self) -> bool {
        self.remaining == Some(0)
    }

    async fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.finished() {
            return None;
        }
        match self.receiver.recv().await {
            Some(Ok((batch, piece))) => {
                self.held = Some(piece);
                let rows = batch.num_rows();
                let kept = self.remaining.map_or(rows, |left| {
                    usize::try_from(left).map_or(rows, |left| left.min(rows))
                });
                self.remaining = self
                    .remaining
                    .map(|left| left.saturating_sub(u64::try_from(kept).unwrap_or(u64::MAX)));
                let batch = if kept < rows {
                    batch.slice(0, kept)
                } else {
                    batch
                };
                Some(Ok(batch.record_output(&self.baseline)))
            }
            Some(Err(error)) => Some(Err(error)),
            None => {
                for task in self.tasks.drain(..) {
                    if let Err(error) = task.join().await {
                        return Some(Err(DataFusionError::ExecutionJoin(Box::new(error))));
                    }
                }
                None
            }
        }
    }
}

async fn step(state: Option<Merge>) -> Option<(Result<RecordBatch>, Option<Merge>)> {
    let mut running = match state? {
        Merge::Start(start) => match start.open().await {
            Ok(running) => running,
            Err(error) => return Some((Err(error), None)),
        },
        Merge::Running(running) => running,
    };
    let next = running.next().await?;
    let rest = (next.is_ok() && !running.finished()).then_some(Merge::Running(running));
    Some((next, rest))
}

pub(crate) fn stream(
    plan: Arc<ScanPlan>,
    meter: ScanMeter,
    memory: Arc<dyn MemoryPool>,
    baseline: BaselineMetrics,
) -> SendableRecordBatchStream {
    let schema = Arc::clone(&plan.schema);
    let start = Merge::Start(Start {
        plan,
        meter,
        memory,
        baseline,
    });
    let merged = futures::stream::unfold(Some(start), step);
    Box::pin(RecordBatchStreamAdapter::new(schema, merged))
}
