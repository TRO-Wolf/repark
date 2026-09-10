use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use repark_core::Result;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JobId(u64);

impl JobId {
    pub(crate) const fn from_raw(id: u64) -> Self {
        Self(id)
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
<<<<<<< HEAD
pub struct StageMetrics {
    pub stage_id: usize,
    pub rows: u64,
    pub bytes_shuffled: u64,
    pub wall_time_ms: u128,
    pub attempt_num: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
=======
>>>>>>> origin/main
pub enum JobStatus {
    Queued,
    Running {
        completed_stages: usize,
        total_stages: usize,
    },
<<<<<<< HEAD
    Completed {
        stages: Vec<StageMetrics>,
        retried_stages: usize,
    },
=======
    Completed,
>>>>>>> origin/main
    Failed(String),
    Cancelled,
}

pub struct JobHandle {
    pub id: JobId,
    stream: SendableRecordBatchStream,
}

impl fmt::Debug for JobHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobHandle")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl JobHandle {
    pub(crate) fn new(id: JobId, stream: SendableRecordBatchStream) -> Self {
        Self { id, stream }
    }

    #[must_use]
    pub fn stream(self) -> SendableRecordBatchStream {
        self.stream
    }
}

#[allow(clippy::missing_errors_doc)]
#[async_trait]
pub trait DistributedExecutor: Send + Sync {
    async fn execute(&self, plan: Arc<dyn ExecutionPlan>) -> Result<JobHandle>;

    async fn status(&self, job: JobId) -> Result<JobStatus>;

    async fn cancel(&self, job: JobId) -> Result<()>;
}
