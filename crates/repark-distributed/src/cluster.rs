use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ballista_core::JobId as BallistaJobId;
use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_core::serde::protobuf::{SuccessfulJob, job_status, task_status};
use ballista_core::serde::scheduler::PartitionLocation;
use ballista_core::utils::{GrpcServerConfig, create_grpc_server, create_grpc_server_incoming};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{SchedulerConfig, TaskDistributionPolicy};
use ballista_scheduler::metrics::default_metrics_collector;
use ballista_scheduler::scheduler_server::SchedulerServer;
use ballista_scheduler::state::execution_stage::ExecutionStage;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::Stream;
use futures::StreamExt;
use futures::future::BoxFuture;
use repark_core::{Error, Result};
use tokio::task::JoinHandle;

use crate::codec::repark_ballista_codec;
use crate::executor::{DistributedExecutor, JobHandle, JobId, JobStatus};
use crate::session_provider::ReparkSessionProvider;

const EXECUTOR_WAIT: Duration = Duration::from_secs(30);
const SCHEDULER_CONNECT_WAIT: Duration = Duration::from_secs(10);
const JOB_WAIT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(50);
const TASKS_PER_EXECUTOR: usize = 1;

type SubmitFn =
    Arc<dyn Fn(Arc<dyn ExecutionPlan>) -> BoxFuture<'static, Result<BallistaJobId>> + Send + Sync>;
type CancelFn = Arc<dyn Fn(BallistaJobId) -> BoxFuture<'static, Result<()>> + Send + Sync>;

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct ClusterJobSlot {
    ballista_id: BallistaJobId,
    cancelled: bool,
}

pub struct ReparkClusterExecutor {
    cluster: BallistaCluster,
    session_config: SessionConfig,
    jobs: Arc<Mutex<HashMap<JobId, ClusterJobSlot>>>,
    next_id: AtomicU64,
    submit: SubmitFn,
    cancel_job: CancelFn,
    _scheduler_server: AbortOnDrop,
}

fn jobs_lock(
    jobs: &Mutex<HashMap<JobId, ClusterJobSlot>>,
) -> MutexGuard<'_, HashMap<JobId, ClusterJobSlot>> {
    jobs.lock().unwrap_or_else(PoisonError::into_inner)
}

fn unknown_job(job: JobId) -> Error {
    Error::DataFusion(format!("unknown distributed job {job}"))
}

fn ballista_err(error: impl fmt::Display) -> Error {
    Error::DataFusion(error.to_string())
}

fn slot_for(jobs: &HashMap<JobId, ClusterJobSlot>, job: JobId) -> Result<&ClusterJobSlot> {
    jobs.get(&job).ok_or_else(|| unknown_job(job))
}

macro_rules! connect_scheduler {
    ($url:expr) => {{
        let scheduler_url = $url;
        let deadline = Instant::now() + SCHEDULER_CONNECT_WAIT;
        loop {
            match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
                Ok(client) => break client,
                Err(error) => {
                    if Instant::now() >= deadline {
                        return Err(ballista_err(format!(
                            "scheduler connect {scheduler_url}: {error}"
                        )));
                    }
                    tokio::time::sleep(POLL).await;
                }
            }
        }
    }};
}

async fn wait_for_executors(cluster: &BallistaCluster, executor_count: usize) -> Result<()> {
    let deadline = Instant::now() + EXECUTOR_WAIT;
    loop {
        let ready = cluster
            .cluster_state()
            .registered_executor_metadata()
            .await
            .len();
        if ready >= executor_count {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ballista_err(format!(
                "timed out waiting for {executor_count} executors, have {ready}"
            )));
        }
        tokio::time::sleep(POLL).await;
    }
}

fn stage_progress(stages: &HashMap<usize, ExecutionStage>) -> (usize, usize) {
    let total = stages.len();
    let completed = stages
        .values()
        .filter(|stage| matches!(stage, ExecutionStage::Successful(_)))
        .count();
    (completed, total.max(1))
}

fn executor_id_of(status: &task_status::Status) -> Option<&str> {
    match status {
        task_status::Status::Successful(successful) => Some(successful.executor_id.as_str()),
        task_status::Status::Running(running) => Some(running.executor_id.as_str()),
        task_status::Status::Failed(_) => None,
    }
}

fn count_stage_tasks(stage: &ExecutionStage, counts: &mut HashMap<String, usize>) {
    match stage {
        ExecutionStage::Successful(successful) => {
            for info in &successful.task_infos {
                if let Some(executor_id) = executor_id_of(&info.task_status) {
                    *counts.entry(executor_id.to_owned()).or_insert(0) += 1;
                }
            }
        }
        ExecutionStage::Running(running) => {
            for info in running.task_infos.iter().flatten() {
                if let Some(executor_id) = executor_id_of(&info.task_status) {
                    *counts.entry(executor_id.to_owned()).or_insert(0) += 1;
                }
            }
        }
        ExecutionStage::Failed(failed) => {
            for info in failed.task_infos.iter().flatten() {
                if let Some(executor_id) = executor_id_of(&info.task_status) {
                    *counts.entry(executor_id.to_owned()).or_insert(0) += 1;
                }
            }
        }
        ExecutionStage::UnResolved(_) | ExecutionStage::Resolved(_) => {}
    }
}

async fn fetch_partitions(
    successful: SuccessfulJob,
    max_message_size: usize,
) -> std::result::Result<Vec<RecordBatch>, DataFusionError> {
    let mut batches = Vec::new();
    for location in successful.partition_location {
        let converted: PartitionLocation = location
            .try_into()
            .map_err(|error| DataFusionError::Execution(format!("partition location: {error}")))?;
        let mut client = ballista_core::client::BallistaClient::try_new(
            &converted.executor_meta.host,
            converted.executor_meta.port,
            max_message_size,
            false,
            None,
            3,
            250,
            0,
            0,
        )
        .await
        .map_err(|error| DataFusionError::Execution(format!("flight client: {error}")))?;
        let mut stream = client
            .fetch_partition(
                &converted.executor_meta.id,
                &converted.partition_id,
                converted.file_id,
                converted.is_sort_shuffle,
                true,
            )
            .await
            .map_err(|error| DataFusionError::Execution(format!("fetch partition: {error}")))?;
        while let Some(item) = stream.next().await {
            batches.push(item?);
        }
    }
    Ok(batches)
}

async fn collect_job(
    cluster: BallistaCluster,
    ballista_id: BallistaJobId,
    max_message_size: usize,
) -> std::result::Result<Vec<RecordBatch>, DataFusionError> {
    let deadline = Instant::now() + JOB_WAIT;
    loop {
        if Instant::now() >= deadline {
            return Err(DataFusionError::Execution(format!(
                "job {ballista_id} timed out after {JOB_WAIT:?}"
            )));
        }
        let status = cluster
            .job_state()
            .get_job_status(&ballista_id)
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        match status.and_then(|job| job.status) {
            None | Some(job_status::Status::Queued(_) | job_status::Status::Running(_)) => {
                tokio::time::sleep(POLL).await;
            }
            Some(job_status::Status::Failed(failed)) => {
                return Err(DataFusionError::Execution(format!(
                    "job {ballista_id} failed: {}",
                    failed.error
                )));
            }
            Some(job_status::Status::Successful(successful)) => {
                return fetch_partitions(successful, max_message_size).await;
            }
        }
    }
}

struct ClusterJobStream {
    schema: SchemaRef,
    pending: Option<BoxFuture<'static, std::result::Result<Vec<RecordBatch>, DataFusionError>>>,
    batches: Option<std::vec::IntoIter<RecordBatch>>,
}

impl Stream for ClusterJobStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(iter) = this.batches.as_mut() {
            return Poll::Ready(iter.next().map(Ok));
        }
        let Some(pending) = this.pending.as_mut() else {
            return Poll::Ready(None);
        };
        match pending.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(batches)) => {
                this.pending = None;
                this.batches = Some(batches.into_iter());
                match this.batches.as_mut() {
                    Some(iter) => Poll::Ready(iter.next().map(Ok)),
                    None => Poll::Ready(None),
                }
            }
            Poll::Ready(Err(error)) => {
                this.pending = None;
                Poll::Ready(Some(Err(error)))
            }
        }
    }
}

impl RecordBatchStream for ClusterJobStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl ReparkClusterExecutor {
    #[allow(clippy::missing_errors_doc)]
    pub async fn new(
        executor_count: usize,
        scheduler_bind: SocketAddr,
        provider: ReparkSessionProvider,
    ) -> Result<Self> {
        if executor_count == 0 {
            return Err(Error::Config(
                "ReparkClusterExecutor executor_count must be at least 1".to_owned(),
            ));
        }
        let session_builder = provider.session_builder();
        let config_producer = provider.config_producer();
        let session_config = config_producer();
        let codec = repark_ballista_codec();
        let cluster = BallistaCluster::new_memory(
            scheduler_bind.to_string(),
            session_builder,
            config_producer.clone(),
        );
        let metrics = default_metrics_collector().map_err(ballista_err)?;
        let scheduler_config = Arc::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PullStaged)
                .with_task_distribution(TaskDistributionPolicy::RoundRobin),
        );
        let mut scheduler = SchedulerServer::new(
            scheduler_bind.to_string(),
            cluster.clone(),
            codec.clone(),
            scheduler_config,
            metrics,
        );
        scheduler.init().await.map_err(ballista_err)?;
        let incoming = create_grpc_server_incoming(scheduler_bind, &GrpcServerConfig::default())
            .map_err(ballista_err)?;
        let bound = incoming.local_addr().map_err(ballista_err)?;
        let max_message_size = session_config.ballista_grpc_client_max_message_size();
        let grpc_service = SchedulerGrpcServer::new(scheduler.clone())
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        #[expect(
            clippy::disallowed_methods,
            reason = "in-process scheduler gRPC is aborted when ReparkClusterExecutor drops"
        )]
        let scheduler_task = tokio::spawn(async move {
            let _ = create_grpc_server(&GrpcServerConfig::default())
                .add_service(grpc_service)
                .serve_with_incoming(incoming)
                .await;
        });
        let scheduler_url = format!("http://{bound}");
        drop(connect_scheduler!(scheduler_url.clone()));
        for _ in 0..executor_count {
            let client = connect_scheduler!(scheduler_url.clone());
            ballista_executor::new_standalone_executor_from_builder(
                client,
                TASKS_PER_EXECUTOR,
                config_producer.clone(),
                provider.runtime_producer(),
                codec.clone(),
                provider.function_registry(),
            )
            .await
            .map_err(ballista_err)?;
        }
        wait_for_executors(&cluster, executor_count).await?;
        let submit_scheduler = scheduler.clone();
        let submit_state = provider.session_state();
        let submit = Arc::new(move |plan: Arc<dyn ExecutionPlan>| {
            let scheduler = submit_scheduler.clone();
            let state = submit_state.clone();
            Box::pin(async move {
                let context = Arc::new(SessionContext::new_with_state(state));
                scheduler
                    .submit_physical_plan("repark-cluster", context, plan, None)
                    .await
                    .map_err(ballista_err)
            }) as BoxFuture<'static, Result<BallistaJobId>>
        });
        let cancel_scheduler = scheduler.clone();
        let cancel_job = Arc::new(move |job_id: BallistaJobId| {
            let scheduler = cancel_scheduler.clone();
            Box::pin(async move { scheduler.cancel_job(job_id).await.map_err(ballista_err) })
                as BoxFuture<'static, Result<()>>
        });
        Ok(Self {
            cluster,
            session_config,
            jobs: Arc::new(Mutex::new(HashMap::new())),
            next_id: AtomicU64::new(1),
            submit,
            cancel_job,
            _scheduler_server: AbortOnDrop(scheduler_task),
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn executor_task_counts(&self, job: JobId) -> Result<HashMap<String, usize>> {
        let ballista_id = {
            let jobs = jobs_lock(&self.jobs);
            slot_for(&jobs, job)?.ballista_id.clone()
        };
        let graph = self
            .cluster
            .job_state()
            .get_execution_graph(&ballista_id)
            .await
            .map_err(ballista_err)?
            .ok_or_else(|| Error::DataFusion(format!("execution graph missing for job {job}")))?;
        let mut counts = HashMap::new();
        for stage in graph.stages().values() {
            count_stage_tasks(stage, &mut counts);
        }
        Ok(counts)
    }
}

impl fmt::Debug for ReparkClusterExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let job_count = jobs_lock(&self.jobs).len();
        formatter
            .debug_struct("ReparkClusterExecutor")
            .field("job_count", &job_count)
            .finish_non_exhaustive()
    }
}

#[allow(clippy::missing_errors_doc)]
#[async_trait]
impl DistributedExecutor for ReparkClusterExecutor {
    async fn execute(&self, plan: Arc<dyn ExecutionPlan>) -> Result<JobHandle> {
        let schema = plan.schema();
        let ballista_id = (self.submit)(plan).await?;
        let id = JobId::from_raw(self.next_id.fetch_add(1, Ordering::Relaxed));
        {
            let mut jobs = jobs_lock(&self.jobs);
            jobs.insert(
                id,
                ClusterJobSlot {
                    ballista_id: ballista_id.clone(),
                    cancelled: false,
                },
            );
        }
        let max_message_size = self.session_config.ballista_grpc_client_max_message_size();
        let pending = collect_job(self.cluster.clone(), ballista_id, max_message_size);
        let stream: SendableRecordBatchStream = Box::pin(ClusterJobStream {
            schema,
            pending: Some(Box::pin(pending)),
            batches: None,
        });
        Ok(JobHandle::new(id, stream))
    }

    async fn status(&self, job: JobId) -> Result<JobStatus> {
        let (ballista_id, cancelled) = {
            let jobs = jobs_lock(&self.jobs);
            let slot = slot_for(&jobs, job)?;
            (slot.ballista_id.clone(), slot.cancelled)
        };
        if cancelled {
            return Ok(JobStatus::Cancelled);
        }
        let proto = self
            .cluster
            .job_state()
            .get_job_status(&ballista_id)
            .await
            .map_err(ballista_err)?;
        match proto.and_then(|job_status| job_status.status) {
            None | Some(job_status::Status::Queued(_)) => Ok(JobStatus::Queued),
            Some(job_status::Status::Running(_)) => {
                let (completed_stages, total_stages) = match self
                    .cluster
                    .job_state()
                    .get_execution_graph(&ballista_id)
                    .await
                    .map_err(ballista_err)?
                {
                    Some(graph) => stage_progress(graph.stages()),
                    None => (0, 1),
                };
                Ok(JobStatus::Running {
                    completed_stages,
                    total_stages,
                })
            }
            Some(job_status::Status::Failed(failed)) => Ok(JobStatus::Failed(failed.error)),
            Some(job_status::Status::Successful(_)) => Ok(JobStatus::Completed),
        }
    }

    async fn cancel(&self, job: JobId) -> Result<()> {
        let ballista_id = {
            let mut jobs = jobs_lock(&self.jobs);
            match jobs.get_mut(&job) {
                Some(slot) => {
                    slot.cancelled = true;
                    slot.ballista_id.clone()
                }
                None => return Err(unknown_job(job)),
            }
        };
        (self.cancel_job)(ballista_id).await
    }
}
