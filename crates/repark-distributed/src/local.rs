use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{
    ExecutionPlan, RecordBatchStream, SendableRecordBatchStream, execute_stream,
};
use datafusion::prelude::SessionContext;
use futures::Stream;
use repark_core::{Error, Result, engine_err};

use crate::executor::{DistributedExecutor, JobHandle, JobId, JobStatus};

pub struct LocalDataFusionExecutor {
    context: SessionContext,
    jobs: Arc<Mutex<HashMap<JobId, JobSlot>>>,
    next_id: AtomicU64,
}

struct JobSlot {
    status: JobStatus,
    cancelled: Arc<AtomicBool>,
}

struct JobStream {
    inner: SendableRecordBatchStream,
    jobs: Arc<Mutex<HashMap<JobId, JobSlot>>>,
    id: JobId,
    cancelled: Arc<AtomicBool>,
    started: bool,
}

fn jobs_lock(jobs: &Mutex<HashMap<JobId, JobSlot>>) -> MutexGuard<'_, HashMap<JobId, JobSlot>> {
    jobs.lock().unwrap_or_else(PoisonError::into_inner)
}

fn unknown_job(job: JobId) -> Error {
    Error::DataFusion(format!("unknown distributed job {job}"))
}

fn mark_running(jobs: &Mutex<HashMap<JobId, JobSlot>>, id: JobId) {
    let mut jobs = jobs_lock(jobs);
    if let Some(slot) = jobs.get_mut(&id)
        && matches!(slot.status, JobStatus::Queued)
    {
        slot.status = JobStatus::Running {
            completed_stages: 0,
            total_stages: 1,
        };
    }
}

fn mark_terminal(jobs: &Mutex<HashMap<JobId, JobSlot>>, id: JobId, terminal: JobStatus) {
    let mut jobs = jobs_lock(jobs);
    if let Some(slot) = jobs.get_mut(&id)
        && matches!(slot.status, JobStatus::Queued | JobStatus::Running { .. })
    {
        slot.status = terminal;
    }
}

impl LocalDataFusionExecutor {
    #[must_use]
    pub fn new(context: SessionContext) -> Self {
        Self {
            context,
            jobs: Arc::new(Mutex::new(HashMap::new())),
            next_id: AtomicU64::new(1),
        }
    }
}

impl fmt::Debug for LocalDataFusionExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let job_count = jobs_lock(&self.jobs).len();
        formatter
            .debug_struct("LocalDataFusionExecutor")
            .field("job_count", &job_count)
            .field("next_id", &self.next_id)
            .finish_non_exhaustive()
    }
}

#[allow(clippy::missing_errors_doc, clippy::new_without_default)]
#[async_trait]
impl DistributedExecutor for LocalDataFusionExecutor {
    async fn execute(&self, plan: Arc<dyn ExecutionPlan>) -> Result<JobHandle> {
        let inner = execute_stream(plan, self.context.task_ctx()).map_err(engine_err)?;
        let id = JobId::from_raw(self.next_id.fetch_add(1, Ordering::Relaxed));
        let cancelled = Arc::new(AtomicBool::new(false));
        {
            let mut jobs = jobs_lock(&self.jobs);
            jobs.insert(
                id,
                JobSlot {
                    status: JobStatus::Queued,
                    cancelled: Arc::clone(&cancelled),
                },
            );
        }
        let stream: SendableRecordBatchStream = Box::pin(JobStream {
            inner,
            jobs: Arc::clone(&self.jobs),
            id,
            cancelled,
            started: false,
        });
        Ok(JobHandle::new(id, stream))
    }

    async fn status(&self, job: JobId) -> Result<JobStatus> {
        let jobs = jobs_lock(&self.jobs);
        match jobs.get(&job) {
            Some(slot) => Ok(slot.status.clone()),
            None => Err(unknown_job(job)),
        }
    }

    async fn cancel(&self, job: JobId) -> Result<()> {
        let mut jobs = jobs_lock(&self.jobs);
        match jobs.get_mut(&job) {
            Some(slot) => {
                slot.cancelled.store(true, Ordering::SeqCst);
                if matches!(slot.status, JobStatus::Queued | JobStatus::Running { .. }) {
                    slot.status = JobStatus::Cancelled;
                }
                Ok(())
            }
            None => Err(unknown_job(job)),
        }
    }
}

impl Stream for JobStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.cancelled.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }
        if !this.started {
            this.started = true;
            mark_running(&this.jobs, this.id);
        }
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                if !this.cancelled.load(Ordering::SeqCst) {
<<<<<<< HEAD
                    mark_terminal(
                        &this.jobs,
                        this.id,
                        JobStatus::Completed {
                            stages: Vec::new(),
                            retried_stages: 0,
                        },
                    );
=======
                    mark_terminal(&this.jobs, this.id, JobStatus::Completed);
>>>>>>> origin/main
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                if !this.cancelled.load(Ordering::SeqCst) {
                    mark_terminal(&this.jobs, this.id, JobStatus::Failed(error.to_string()));
                }
                Poll::Ready(Some(Err(error)))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for JobStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
