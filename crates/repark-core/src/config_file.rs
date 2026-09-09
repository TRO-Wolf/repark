mod discovery;
mod interpolate;
mod profile;
mod redact;
mod sources;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::path::PathBuf;

use repark_common::{Error, Result};

use self::discovery::discover;
use self::profile::profile_from_table;

pub(crate) type EnvironmentLookup<'a> = &'a dyn Fn(&str) -> Option<String>;

#[allow(dead_code)]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConfigFile {
    pub profiles: BTreeMap<String, Profile>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Profile {
    pub display: Option<toml::Table>,
    pub session: Option<toml::Table>,
    pub conf: Option<toml::Table>,
    pub catalog: Option<toml::Table>,
    pub database: Option<toml::Table>,
}

#[allow(dead_code)]
pub fn load() -> Result<ConfigFile> {
    let environment = |name: &str| std::env::var(name).ok();
    let current_directory = std::env::current_dir()
        .map_err(|error| Error::Config(format!("cannot read the current directory: {error}")))?;
    let home = environment("HOME").map(PathBuf::from);
    let Some(path) = discover(&environment, &current_directory, home.as_deref())? else {
        return Ok(ConfigFile::default());
    };
    let text = std::fs::read_to_string(&path)
        .map_err(|error| Error::Config(format!("cannot read `{}`: {error}", path.display())))?;
    parse(&text)
}

#[allow(dead_code)]
pub(crate) fn parse(text: &str) -> Result<ConfigFile> {
    let document =
        toml::from_str::<toml::Table>(text).map_err(|error| Error::Config(error.to_string()))?;
    ConfigFile::from_document(document)
}

impl ConfigFile {
    #[allow(dead_code)]
    fn from_document(document: toml::Table) -> Result<ConfigFile> {
        let mut profiles = BTreeMap::new();
        for (name, value) in document {
            let toml::Value::Table(table) = value else {
                return Err(Error::Config(format!(
                    "unknown top-level key `{name}` at the document root; a top-level key must name a profile table"
                )));
            };
            let profile = profile_from_table(&name, &table)?;
            profiles.insert(name, profile);
        }
        Ok(ConfigFile { profiles })
    }
}
