use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use repark_common::{Error, Result};

use super::discovery::discover;
use super::interpolate::interpolate_table;
use super::maintenance::MaintenancePolicy;
use super::profile::{DEFAULT_PROFILE_NAME, effective_table, profile_as_table, profile_from_table};
use super::redact::redact_value;
use super::sources::{CATALOG_KEY_PREFIX, ProfileSources, profile_sources};
use super::{ConfigFile, EnvironmentLookup, Profile, read_and_parse};

pub(crate) const ENV_PROFILE_VARIABLE: &str = "REPARK_ENV";

const DISPLAY_PREFIX: &str = "repark.display.";
const MEMORY_LIMIT_GB_KEY: &str = "repark.memory.limit.gb";
const BATCH_SIZE_KEY: &str = "repark.batch.size";
const TARGET_PARTITIONS_KEY: &str = "repark.target.partitions";
const SESSION_KNOB_KEYS: &[&str] = &[MEMORY_LIMIT_GB_KEY, BATCH_SIZE_KEY, TARGET_PARTITIONS_KEY];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyOrigin {
    Profile,
    Default,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FileConfig {
    pub provenance: Option<(PathBuf, String)>,
    pub pairs: Vec<(String, String)>,
    pub origins: HashMap<String, KeyOrigin>,
    pub memory_limit_gb: Option<usize>,
    pub batch_size: Option<usize>,
    pub target_partitions: Option<usize>,
    pub maintenance: Option<(String, Option<MaintenancePolicy>)>,
    pub warnings: Vec<String>,
}

impl FileConfig {
    pub(crate) fn pairs_for_map(&self) -> Vec<(String, String)> {
        self.pairs
            .iter()
            .filter(|(key, _)| !SESSION_KNOB_KEYS.contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

pub(crate) fn load_for_build(forced: Option<PathBuf>) -> Result<FileConfig> {
    let environment = |name: &str| std::env::var(name).ok();
    let current_directory = std::env::current_dir()
        .map_err(|error| Error::Config(format!("cannot read the current directory: {error}")))?;
    let home = environment("HOME").map(PathBuf::from);
    load_file_config(forced, &environment, &current_directory, home.as_deref())
}

pub(crate) fn load_file_config(
    forced: Option<PathBuf>,
    environment: EnvironmentLookup<'_>,
    current_directory: &Path,
    home: Option<&Path>,
) -> Result<FileConfig> {
    let forced_path = forced.is_some();
    let discovered = match forced {
        Some(path) => {
            if !path.exists() {
                return Err(Error::Config(format!(
                    "config file `{}` does not exist",
                    path.display()
                )));
            }
            Some(path)
        }
        None => discover(environment, current_directory, home)?,
    };
    let Some(path) = discovered else {
        return Ok(FileConfig::default());
    };
    let trusted = forced_path
        || environment(super::discovery::CONFIG_VARIABLE).is_some_and(|value| !value.is_empty());
    let document = read_and_parse(&path)?;
    let profile_name = environment(ENV_PROFILE_VARIABLE).filter(|name| !name.is_empty());
    translate_document(
        &path,
        &document,
        profile_name.as_deref(),
        environment,
        trusted,
    )
}

fn cloud_catalog_warning(path: &Path, catalog: &toml::Table) -> Option<String> {
    let mut cloud: Vec<String> = Vec::new();
    for (name, block) in catalog {
        let toml::Value::Table(props) = block else {
            continue;
        };
        let cloud_type = props
            .get("type")
            .and_then(toml::Value::as_str)
            .is_some_and(|kind| {
                matches!(
                    kind.trim().to_ascii_lowercase().as_str(),
                    "glue" | "s3tables" | "rest"
                )
            });
        let cloud_impl = props
            .get("catalog-impl")
            .and_then(toml::Value::as_str)
            .is_some_and(|class| {
                let class = class.trim();
                class.ends_with("GlueCatalog") || class.ends_with("S3TablesCatalog")
            });
        if cloud_type || cloud_impl {
            cloud.push(name.clone());
        }
    }
    if cloud.is_empty() {
        return None;
    }
    cloud.sort();
    Some(format!(
        "config file `{}` names cloud catalogs ({}) but was found by directory search, \
         not by REPARK_CONFIG or configFile; building with ambient credentials",
        path.display(),
        cloud.join(", ")
    ))
}

fn refuse_pending_sources(label: &str, sources: &ProfileSources) -> Result<()> {
    if sources.sources.is_empty() {
        return Ok(());
    }
    let names: Vec<String> = sources
        .sources
        .iter()
        .map(|source| {
            format!(
                "{label}.database.{}.{}",
                source.kind.spelling(),
                source.name
            )
        })
        .collect();
    Err(Error::Config(format!(
        "database sources ({}) are parsed but named-source registration arrives with \
         CFG-2 — drop the `[<profile>.database]` tables until that card lands",
        names.join(", ")
    )))
}

fn discovery_warnings(path: &Path, profile: &Profile, trusted: bool) -> Vec<String> {
    if trusted {
        return Vec::new();
    }
    profile
        .catalog
        .as_ref()
        .and_then(|catalog| cloud_catalog_warning(path, catalog))
        .into_iter()
        .collect()
}

fn translate_document(
    path: &Path,
    document: &ConfigFile,
    profile_name: Option<&str>,
    environment: EnvironmentLookup<'_>,
    trusted: bool,
) -> Result<FileConfig> {
    let effective = effective_table(document, profile_name)?;
    let interpolated = interpolate_table(&effective, environment)?;
    let label = profile_name.unwrap_or(DEFAULT_PROFILE_NAME);
    let profile = profile_from_table(label, &interpolated)?;
    let sources = profile_sources(label, &profile)?;
    refuse_pending_sources(label, &sources)?;
    let mut pairs = Vec::new();
    let mut origins = HashMap::new();
    let selected_keys = match profile_name {
        None => HashSet::new(),
        Some(name) => {
            let table = document
                .profiles
                .get(name)
                .map(profile_as_table)
                .unwrap_or_default();
            section_keys(&table)
        }
    };
    let mut note = |key: String, value: String| {
        origins.insert(
            key.clone(),
            if selected_keys.contains(&key) {
                KeyOrigin::Profile
            } else {
                KeyOrigin::Default
            },
        );
        pairs.push((key, value));
    };
    if let Some(display) = profile.display.as_ref() {
        for (key, value) in display {
            note(
                format!("{DISPLAY_PREFIX}{key}"),
                plain_string(label, "display", key, value)?,
            );
        }
    }
    let mut memory_limit_gb = None;
    let mut batch_size = None;
    let mut target_partitions = None;
    if let Some(session) = profile.session.as_ref() {
        memory_limit_gb = parse_knob(label, "memory_limit_gb", session.get("memory_limit_gb"))?;
        batch_size = parse_knob(label, "batch_size", session.get("batch_size"))?;
        target_partitions =
            parse_knob(label, "target_partitions", session.get("target_partitions"))?;
        if let Some(gb) = memory_limit_gb {
            note(MEMORY_LIMIT_GB_KEY.to_string(), gb.to_string());
        }
        if let Some(rows) = batch_size {
            note(BATCH_SIZE_KEY.to_string(), rows.to_string());
        }
        if let Some(partitions) = target_partitions {
            note(TARGET_PARTITIONS_KEY.to_string(), partitions.to_string());
        }
    }
    if let Some(conf) = profile.conf.as_ref() {
        for (key, value) in flatten_conf(label, conf)? {
            note(key, value);
        }
    }
    if let Some(catalog) = profile.catalog.as_ref() {
        for (name, block) in catalog {
            let Some(props) = block.as_table() else {
                continue;
            };
            for (prop, value) in props {
                note(
                    format!("{CATALOG_KEY_PREFIX}{name}.{prop}"),
                    plain_string(label, &format!("catalog.{name}"), prop, value)?,
                );
            }
        }
    }
    let warnings = discovery_warnings(path, &profile, trusted);
    Ok(FileConfig {
        provenance: Some((path.to_path_buf(), label.to_string())),
        pairs,
        origins,
        memory_limit_gb,
        batch_size,
        target_partitions,
        maintenance: Some((label.to_string(), resolve_maintenance(label, &profile)?)),
        warnings,
    })
}

fn resolve_maintenance(label: &str, profile: &Profile) -> Result<Option<MaintenancePolicy>> {
    match profile.maintenance.as_ref() {
        None => Ok(None),
        Some(table) => MaintenancePolicy::from_table(label, table).map(Some),
    }
}

fn plain_string(label: &str, section: &str, key: &str, value: &toml::Value) -> Result<String> {
    match value {
        toml::Value::String(text) => Ok(text.clone()),
        toml::Value::Integer(number) => Ok(number.to_string()),
        _ => Err(Error::Config(format!(
            "key `{label}.{section}.{key}` must be a string"
        ))),
    }
}

fn flatten_conf(label: &str, table: &toml::Table) -> Result<Vec<(String, String)>> {
    let mut pairs = Vec::new();
    flatten_conf_into(label, table, "", &mut pairs)?;
    let mut seen = HashSet::new();
    for (key, _) in &pairs {
        if !seen.insert(key.clone()) {
            return Err(Error::Config(format!(
                "conf key `{label}.conf.{key}` is set twice (quoted and nested spellings collide)"
            )));
        }
    }
    Ok(pairs)
}

fn flatten_conf_into(
    label: &str,
    table: &toml::Table,
    prefix: &str,
    pairs: &mut Vec<(String, String)>,
) -> Result<()> {
    for (key, value) in table {
        let dotted = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            toml::Value::String(text) => pairs.push((dotted, text.clone())),
            toml::Value::Integer(number) => pairs.push((dotted, number.to_string())),
            toml::Value::Table(inner) => flatten_conf_into(label, inner, &dotted, pairs)?,
            _ => {
                return Err(Error::Config(format!(
                    "key `{label}.conf.{dotted}` must be a string"
                )));
            }
        }
    }
    Ok(())
}

fn flatten_conf_keys(table: &toml::Table, prefix: &str, keys: &mut HashSet<String>) {
    for (key, value) in table {
        let dotted = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            toml::Value::Table(inner) => flatten_conf_keys(inner, &dotted, keys),
            _ => {
                keys.insert(dotted);
            }
        }
    }
}

fn parse_knob(label: &str, key: &str, value: Option<&toml::Value>) -> Result<Option<usize>> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        toml::Value::Integer(number) => usize::try_from(*number).map(Some).map_err(|_| {
            Error::Config(format!(
                "key `{label}.session.{key}` must be a non-negative integer"
            ))
        }),
        toml::Value::String(text) => text.trim().parse::<usize>().map(Some).map_err(|_| {
            Error::Config(format!(
                "key `{label}.session.{key}` must be a non-negative integer, got `{text}`"
            ))
        }),
        _ => Err(Error::Config(format!(
            "key `{label}.session.{key}` must be a non-negative integer"
        ))),
    }
}

fn section_keys(table: &toml::Table) -> HashSet<String> {
    let mut keys = HashSet::new();
    if let Some(display) = table.get("display").and_then(toml::Value::as_table) {
        keys.extend(display.keys().map(|key| format!("{DISPLAY_PREFIX}{key}")));
    }
    if let Some(session) = table.get("session").and_then(toml::Value::as_table) {
        for key in session.keys() {
            match key.as_str() {
                "memory_limit_gb" => {
                    keys.insert(MEMORY_LIMIT_GB_KEY.to_string());
                }
                "batch_size" => {
                    keys.insert(BATCH_SIZE_KEY.to_string());
                }
                "target_partitions" => {
                    keys.insert(TARGET_PARTITIONS_KEY.to_string());
                }
                _ => {}
            }
        }
    }
    if let Some(conf) = table.get("conf").and_then(toml::Value::as_table) {
        flatten_conf_keys(conf, "", &mut keys);
    }
    if let Some(catalog) = table.get("catalog").and_then(toml::Value::as_table) {
        for (name, block) in catalog {
            if let Some(props) = block.as_table() {
                keys.extend(
                    props
                        .keys()
                        .map(|prop| format!("{CATALOG_KEY_PREFIX}{name}.{prop}")),
                );
            }
        }
    }
    keys
}

pub(crate) fn conf_dump_rows(
    file: &FileConfig,
    builder_config: &HashMap<String, String>,
) -> Vec<(String, String, String)> {
    let mut merged: BTreeMap<String, (String, String)> = BTreeMap::new();
    for (key, value) in file.pairs_for_map() {
        let source = match &file.provenance {
            Some((path, profile)) if file.origins.get(&key) == Some(&KeyOrigin::Profile) => {
                format!("file:{}#{profile}", path.display())
            }
            _ => "default".to_string(),
        };
        merged.insert(key.clone(), (value, source));
    }
    for (key, value) in builder_config {
        merged.insert(key.clone(), (value.clone(), "builder".to_string()));
    }
    merged
        .into_iter()
        .map(|(key, (value, source))| (key.clone(), redact_value(&key, &value), source))
        .collect()
}
