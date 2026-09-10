mod executor;
mod local;

pub use executor::{DistributedExecutor, JobHandle, JobId, JobStatus};
pub use local::LocalDataFusionExecutor;
