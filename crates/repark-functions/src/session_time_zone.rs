//! Session-timezone carrier for calendar extractors, never a second knob.

use std::any::Any;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionConfig;

/// The zone the extractors use when no [`SessionTimeZoneConfig`] is installed.
pub const DEFAULT_EXTRACTION_TIME_ZONE: &str = "UTC";

/// The authoritative key name used in the carrier refusal message.
const AUTHORITATIVE_KEY: &str = "spark.sql.session.timeZone";

/// The resolved session zone on [`ConfigOptions`] so extractors can read it at invoke time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTimeZoneConfig {
    zone: String,
}

impl Default for SessionTimeZoneConfig {
    fn default() -> Self {
        Self {
            zone: DEFAULT_EXTRACTION_TIME_ZONE.to_string(),
        }
    }
}

impl SessionTimeZoneConfig {
    /// The zone id the extractors resolve instants in (`UTC`, `America/New_York`, `+05:30`).
    #[must_use]
    pub fn zone(&self) -> &str {
        &self.zone
    }
}

impl ConfigExtension for SessionTimeZoneConfig {
    /// Two segments keep the carrier unreachable through `SET`.
    const PREFIX: &'static str = "repark.session";
}

impl ExtensionOptions for SessionTimeZoneConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    /// Refuse because the session build owns the setting.
    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the session timezone is set with \
             `{AUTHORITATIVE_KEY}` on the session builder and is fixed at session build",
            Self::PREFIX
        )))
    }

    /// Keep the carrier out of `SET` listings.
    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

/// Attach the resolved session zone to a [`SessionConfig`] from the Spark door's `configure` hook.
#[must_use]
pub fn with_session_time_zone(config: SessionConfig, zone: &str) -> SessionConfig {
    config.with_option_extension(SessionTimeZoneConfig {
        zone: zone.to_string(),
    })
}

/// Read the session zone back out of live config options — the extractors' one accessor.
#[must_use]
pub fn session_time_zone_from_options(options: &ConfigOptions) -> &str {
    options
        .extensions
        .get::<SessionTimeZoneConfig>()
        .map_or(DEFAULT_EXTRACTION_TIME_ZONE, SessionTimeZoneConfig::zone)
}

#[cfg(test)]
mod tests;
