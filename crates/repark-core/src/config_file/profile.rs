use repark_common::{Error, Result};

use super::{ConfigFile, Profile};

pub(crate) const DEFAULT_PROFILE_NAME: &str = "default";

const PROFILE_TABLE_KEYS: &[&str] = &["catalog", "conf", "database", "display", "session"];
const DISPLAY_KEYS: &[&str] = &["max_cols", "max_rows", "str_len", "style"];
const SESSION_KEYS: &[&str] = &["batch_size", "memory_limit_gb", "target_partitions"];

pub(crate) fn profile_from_table(name: &str, table: &toml::Table) -> Result<Profile> {
    let mut profile = Profile::default();
    for (key, value) in table {
        match key.as_str() {
            "display" => profile.display = Some(typed_table(name, key, value, DISPLAY_KEYS)?),
            "session" => profile.session = Some(typed_table(name, key, value, SESSION_KEYS)?),
            "conf" => profile.conf = Some(table_slot(name, key, value)?),
            "catalog" => profile.catalog = Some(table_slot(name, key, value)?),
            "database" => profile.database = Some(table_slot(name, key, value)?),
            other => {
                return Err(Error::Config(format!(
                    "unknown key `{name}.{other}`; expected one of: {}",
                    PROFILE_TABLE_KEYS.join(", ")
                )));
            }
        }
    }
    Ok(profile)
}

pub(crate) fn effective_table(
    config: &ConfigFile,
    profile_name: Option<&str>,
) -> Result<toml::Table> {
    let default_table = config
        .profiles
        .get(DEFAULT_PROFILE_NAME)
        .map(profile_as_table)
        .unwrap_or_default();
    let Some(name) = profile_name else {
        return Ok(default_table);
    };
    let profile = config.profiles.get(name).ok_or_else(|| {
        let known = config.profiles.keys().cloned().collect::<Vec<_>>().join(", ");
        let known = if known.is_empty() {
            "none".to_string()
        } else {
            known
        };
        Error::Config(format!(
            "unknown profile `{name}` named by REPARK_ENV; the config file carries profiles: {known}"
        ))
    })?;
    Ok(merge_tables(&default_table, &profile_as_table(profile)))
}

fn merge_tables(base: &toml::Table, overlay: &toml::Table) -> toml::Table {
    let mut merged = base.clone();
    for (key, overlay_value) in overlay {
        let merged_value = match (merged.get(key), overlay_value) {
            (Some(toml::Value::Table(base_table)), toml::Value::Table(overlay_table)) => {
                toml::Value::Table(merge_tables(base_table, overlay_table))
            }
            _ => overlay_value.clone(),
        };
        merged.insert(key.clone(), merged_value);
    }
    merged
}

pub(crate) fn profile_as_table(profile: &Profile) -> toml::Table {
    let slots = [
        ("catalog", &profile.catalog),
        ("conf", &profile.conf),
        ("database", &profile.database),
        ("display", &profile.display),
        ("session", &profile.session),
    ];
    let mut table = toml::Table::new();
    for (name, slot) in slots {
        if let Some(value) = slot {
            table.insert(name.to_string(), toml::Value::Table(value.clone()));
        }
    }
    table
}

fn table_slot(name: &str, key: &str, value: &toml::Value) -> Result<toml::Table> {
    if let toml::Value::Table(table) = value {
        return Ok(table.clone());
    }
    Err(Error::Config(format!("key `{name}.{key}` must be a table")))
}

fn typed_table(
    name: &str,
    key: &str,
    value: &toml::Value,
    known_keys: &[&str],
) -> Result<toml::Table> {
    let table = table_slot(name, key, value)?;
    for inner_key in table.keys() {
        if !known_keys.contains(&inner_key.as_str()) {
            return Err(Error::Config(format!(
                "unknown key `{name}.{key}.{inner_key}`; expected one of: {}",
                known_keys.join(", ")
            )));
        }
    }
    Ok(table)
}
