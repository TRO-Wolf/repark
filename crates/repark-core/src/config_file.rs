mod discovery;
mod interpolate;
mod profile;
mod redact;
mod sources;
#[cfg(test)]
mod tests;

use repark_common::{Error, Result};
use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigFile {}

#[allow(dead_code, clippy::unnecessary_wraps)]
pub fn load() -> Result<ConfigFile> {
    Ok(ConfigFile::default())
}

#[allow(dead_code)]
pub(crate) fn parse(text: &str) -> Result<ConfigFile> {
    toml::from_str(text).map_err(|error| Error::Config(error.to_string()))
}
