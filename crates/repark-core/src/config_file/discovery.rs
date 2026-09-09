use std::path::{Path, PathBuf};

use repark_common::{Error, Result};

use super::EnvironmentLookup;

const CONFIG_VARIABLE: &str = "REPARK_CONFIG";
const FILE_NAME: &str = "repark.toml";

pub(crate) fn discover(
    environment: EnvironmentLookup<'_>,
    current_directory: &Path,
    home: Option<&Path>,
) -> Result<Option<PathBuf>> {
    if let Some(value) = environment(CONFIG_VARIABLE) {
        if value.is_empty() {
            return Ok(None);
        }
        let explicit = PathBuf::from(value);
        if !explicit.exists() {
            return Err(Error::Config(format!(
                "{CONFIG_VARIABLE} names `{}`, which does not exist",
                explicit.display()
            )));
        }
        return Ok(Some(explicit));
    }
    let local = current_directory.join(FILE_NAME);
    if local.exists() {
        return Ok(Some(local));
    }
    if let Some(home) = home {
        let home_config = home.join(".config").join("repark").join(FILE_NAME);
        if home_config.exists() {
            return Ok(Some(home_config));
        }
    }
    Ok(None)
}
