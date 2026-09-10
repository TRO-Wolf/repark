mod executor;
mod local;

#[cfg(feature = "cluster")]
mod cluster;
#[cfg(feature = "cluster")]
mod codec;
#[cfg(feature = "cluster")]
mod session_provider;

pub use executor::{DistributedExecutor, JobHandle, JobId, JobStatus};
pub use local::LocalDataFusionExecutor;

#[cfg(feature = "cluster")]
pub use cluster::ReparkClusterExecutor;
#[cfg(feature = "cluster")]
pub use codec::{ReparkLogicalExtensionCodec, ReparkPhysicalExtensionCodec, repark_ballista_codec};
#[cfg(feature = "cluster")]
pub use session_provider::ReparkSessionProvider;
