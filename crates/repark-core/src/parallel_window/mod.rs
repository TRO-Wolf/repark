use std::any::Any;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionConfig;

mod exec;
mod rule;

pub use rule::ParallelWindowRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParallelSinglePartitionConfig {
    pub enabled: bool,
}

impl Default for ParallelSinglePartitionConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl ConfigExtension for ParallelSinglePartitionConfig {
    const PREFIX: &'static str = "repark.parallel";
}

impl ExtensionOptions for ParallelSinglePartitionConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(*self)
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: single-partition parallelism is set with \
             `ReparkSessionBuilder::parallel_single_partition` when the session is built",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[must_use]
pub(crate) fn with_parallel_single_partition(
    config: SessionConfig,
    enabled: Option<bool>,
) -> SessionConfig {
    config.with_option_extension(ParallelSinglePartitionConfig {
        enabled: enabled.unwrap_or(true),
    })
}

#[must_use]
pub fn parallel_single_partition_enabled(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<ParallelSinglePartitionConfig>()
        .is_none_or(|config| config.enabled)
}
