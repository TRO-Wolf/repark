#![forbid(unsafe_code)]

pub(crate) mod body_statements;
pub mod driver;
#[cfg(test)]
mod exactly_once_tests;
#[cfg(test)]
mod fence_tests;
#[cfg(test)]
mod foreach_tests;
#[cfg(test)]
mod lifecycle_tests;
pub mod progress;
#[cfg(test)]
mod race_tests;
pub mod relation;
#[cfg(test)]
mod reload_tests;
mod run;
#[cfg(test)]
mod self_stop_tests;
#[cfg(test)]
mod table_door_tests;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod timeout_tests;

pub use iceberg::spec::Operation;
pub use repark_common::Generation;
pub use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
pub use repark_iceberg::microbatch::offset::{
    Epoch, FilePosition, QueryId, RunId, SinkRecord, SnapshotId, TableUuid,
};
