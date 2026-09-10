use std::collections::{BTreeMap, HashMap};

use repark_common::{Error, Result};

use super::Profile;
use super::redact::redact_value;
use crate::catalog_config::{CatalogSpec, parse_catalog_specs};

pub(crate) const CATALOG_KEY_PREFIX: &str = "repark.sql.catalog.";
const DATABASE_KIND_SPELLINGS: &[&str] = &["postgres", "sqlserver", "trino"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceKind {
    Postgres,
    SqlServer,
    Trino,
}

impl SourceKind {
    fn from_spelling(spelling: &str) -> Option<SourceKind> {
        match spelling {
            "postgres" => Some(SourceKind::Postgres),
            "sqlserver" => Some(SourceKind::SqlServer),
            "trino" => Some(SourceKind::Trino),
            _ => None,
        }
    }

    pub(crate) fn spelling(self) -> &'static str {
        match self {
            SourceKind::Postgres => "postgres",
            SourceKind::SqlServer => "sqlserver",
            SourceKind::Trino => "trino",
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SourceSpec {
    pub name: String,
    pub kind: SourceKind,
    pub props: BTreeMap<String, String>,
}

impl std::fmt::Debug for SourceSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let props: Vec<(String, String)> = self
            .props
            .iter()
            .map(|(key, value)| (key.clone(), redact_value(key, value)))
            .collect();
        f.debug_struct("SourceSpec")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("props", &props)
            .finish()
    }
}

#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ProfileSources {
    pub catalogs: Vec<CatalogSpec>,
    pub sources: Vec<SourceSpec>,
}

pub(crate) fn profile_sources(profile_name: &str, profile: &Profile) -> Result<ProfileSources> {
    let catalogs = match &profile.catalog {
        Some(catalog) => catalog_specs(profile_name, catalog)?,
        None => Vec::new(),
    };
    let sources = match &profile.database {
        Some(database) => source_specs(profile_name, database)?,
        None => Vec::new(),
    };
    refuse_duplicate_names(profile_name, &catalogs, &sources)?;
    Ok(ProfileSources { catalogs, sources })
}

fn catalog_specs(profile_name: &str, catalog: &toml::Table) -> Result<Vec<CatalogSpec>> {
    let mut flat = HashMap::new();
    for (name, block) in catalog {
        if name.contains('.') {
            return Err(Error::Config(format!(
                "catalog name `{name}` must not contain `.` — key `{profile_name}.catalog.{name}` \
                 would silently re-split into `repark.sql.catalog.<name>.<prop>` keys"
            )));
        }
        let toml::Value::Table(props) = block else {
            return Err(Error::Config(format!(
                "key `{profile_name}.catalog.{name}` must be a table of catalog properties"
            )));
        };
        if props.is_empty() {
            return Err(Error::Config(format!(
                "catalog `{name}` at `{profile_name}.catalog.{name}` carries no properties — \
                 set `type` (glue / s3tables / memory) or `catalog-impl`"
            )));
        }
        for (prop, value) in props {
            let toml::Value::String(text) = value else {
                return Err(Error::Config(format!(
                    "key `{profile_name}.catalog.{name}.{prop}` must be a string"
                )));
            };
            flat.insert(format!("{CATALOG_KEY_PREFIX}{name}.{prop}"), text.clone());
        }
    }
    parse_catalog_specs(&flat)
}

fn source_specs(profile_name: &str, database: &toml::Table) -> Result<Vec<SourceSpec>> {
    let mut sources = Vec::new();
    for (kind_name, names) in database {
        let Some(kind) = SourceKind::from_spelling(kind_name) else {
            return Err(Error::Config(format!(
                "unknown database kind `{profile_name}.database.{kind_name}`; expected one of: {}",
                DATABASE_KIND_SPELLINGS.join(", ")
            )));
        };
        let toml::Value::Table(name_table) = names else {
            return Err(Error::Config(format!(
                "key `{profile_name}.database.{kind_name}` must be a table of source names"
            )));
        };
        for (name, block) in name_table {
            let toml::Value::Table(props) = block else {
                return Err(Error::Config(format!(
                    "key `{profile_name}.database.{kind_name}.{name}` must be a table of \
                     connection properties"
                )));
            };
            let mut converted = BTreeMap::new();
            for (prop, value) in props {
                let toml::Value::String(text) = value else {
                    return Err(Error::Config(format!(
                        "key `{profile_name}.database.{kind_name}.{name}.{prop}` must be a string"
                    )));
                };
                converted.insert(prop.clone(), text.clone());
            }
            sources.push(SourceSpec {
                name: name.clone(),
                kind,
                props: converted,
            });
        }
    }
    Ok(sources)
}

fn refuse_duplicate_names(
    profile_name: &str,
    catalogs: &[CatalogSpec],
    sources: &[SourceSpec],
) -> Result<()> {
    let mut named: Vec<(String, String)> = catalogs
        .iter()
        .map(|spec| {
            (
                spec.name.clone(),
                format!("{profile_name}.catalog.{}", spec.name),
            )
        })
        .collect();
    named.extend(sources.iter().map(|spec| {
        (
            spec.name.clone(),
            format!(
                "{profile_name}.database.{}.{}",
                spec.kind.spelling(),
                spec.name
            ),
        )
    }));
    for (index, (name, path)) in named.iter().enumerate() {
        let prior = named[..index]
            .iter()
            .find(|(prior_name, _)| prior_name == name)
            .map(|(_, prior_path)| prior_path);
        if let Some(prior) = prior {
            return Err(Error::Config(format!(
                "duplicate name `{name}` in profile `{profile_name}`: `{prior}` and `{path}` — \
                 names must be unique across the catalog and database families"
            )));
        }
    }
    Ok(())
}
