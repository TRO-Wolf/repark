mod executor;
mod local;

#[cfg(feature = "cluster")]
mod cluster;
#[cfg(feature = "cluster")]
mod codec;
#[cfg(feature = "cluster")]
mod iceberg_provider;
#[cfg(feature = "cluster")]
mod predicate_expr;
#[cfg(feature = "cluster")]
mod session_provider;

pub use executor::{DistributedExecutor, JobHandle, JobId, JobStatus, StageMetrics};
pub use local::LocalDataFusionExecutor;

#[cfg(feature = "cluster")]
pub use cluster::ReparkClusterExecutor;
#[cfg(feature = "cluster")]
pub use codec::{ReparkLogicalExtensionCodec, ReparkPhysicalExtensionCodec, repark_ballista_codec};
#[cfg(feature = "cluster")]
pub use iceberg_provider::{IcebergScanSpec, iceberg_scan_predicates_match};
#[cfg(feature = "cluster")]
pub use predicate_expr::predicate_to_expr;
#[cfg(feature = "cluster")]
pub use session_provider::ReparkSessionProvider;
