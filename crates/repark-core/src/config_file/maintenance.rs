use std::collections::BTreeMap;
use std::time::Duration;

use repark_common::{Error, Result};

const POLICY_KEYS: &[&str] = &[
    "orphan_older_than",
    "position_delete_ratio",
    "rewrite_manifests",
    "snapshot_older_than",
    "snapshot_retain_last",
    "tables",
    "target_file_size_bytes",
];
const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 3_600;
const SECONDS_PER_DAY: u64 = 86_400;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TablePolicy {
    pub target_file_size_bytes: Option<u64>,
    pub snapshot_retain_last: Option<u64>,
    pub snapshot_older_than: Option<Duration>,
    pub orphan_older_than: Option<Duration>,
    pub rewrite_manifests: Option<bool>,
    pub position_delete_ratio: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MaintenancePolicy {
    pub target_file_size_bytes: Option<u64>,
    pub snapshot_retain_last: Option<u64>,
    pub snapshot_older_than: Option<Duration>,
    pub orphan_older_than: Option<Duration>,
    pub rewrite_manifests: Option<bool>,
    pub position_delete_ratio: Option<f64>,
    pub tables: BTreeMap<String, TablePolicy>,
}

impl MaintenancePolicy {
    pub(crate) fn from_table(profile_name: &str, table: &toml::Table) -> Result<Self> {
        let prefix = format!("{profile_name}.maintenance");
        let mut profile = TablePolicy::default();
        let mut tables = BTreeMap::new();
        for (key, value) in table {
            if key == "tables" {
                let Some(nested) = value.as_table() else {
                    return Err(Error::Config(format!(
                        "key `{prefix}.tables` must be a table"
                    )));
                };
                for (name, entry) in nested {
                    let Some(entry_table) = entry.as_table() else {
                        return Err(Error::Config(format!(
                            "key `{prefix}.tables.{name}` must be a table"
                        )));
                    };
                    tables.insert(
                        name.clone(),
                        table_policy(&format!("{prefix}.tables.{name}"), entry_table)?,
                    );
                }
            } else {
                read_field(&prefix, &mut profile, key, value)?;
            }
        }
        Ok(Self {
            target_file_size_bytes: profile.target_file_size_bytes,
            snapshot_retain_last: profile.snapshot_retain_last,
            snapshot_older_than: profile.snapshot_older_than,
            orphan_older_than: profile.orphan_older_than,
            rewrite_manifests: profile.rewrite_manifests,
            position_delete_ratio: profile.position_delete_ratio,
            tables,
        })
    }

    #[must_use]
    pub fn resolve(&self, table_name: &str) -> TablePolicy {
        let entry = self.tables.get(table_name);
        TablePolicy {
            target_file_size_bytes: entry
                .and_then(|policy| policy.target_file_size_bytes)
                .or(self.target_file_size_bytes),
            snapshot_retain_last: entry
                .and_then(|policy| policy.snapshot_retain_last)
                .or(self.snapshot_retain_last),
            snapshot_older_than: entry
                .and_then(|policy| policy.snapshot_older_than)
                .or(self.snapshot_older_than),
            orphan_older_than: entry
                .and_then(|policy| policy.orphan_older_than)
                .or(self.orphan_older_than),
            rewrite_manifests: entry
                .and_then(|policy| policy.rewrite_manifests)
                .or(self.rewrite_manifests),
            position_delete_ratio: entry
                .and_then(|policy| policy.position_delete_ratio)
                .or(self.position_delete_ratio),
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_maintenance_policy(
    profile_name: &str,
    document: &str,
) -> Result<Option<MaintenancePolicy>> {
    let file = super::parse(document)?;
    let Some(profile) = file.profiles.get(profile_name) else {
        let known = file.profiles.keys().cloned().collect::<Vec<_>>().join(", ");
        let known = if known.is_empty() {
            "none"
        } else {
            known.as_str()
        };
        return Err(Error::Config(format!(
            "unknown profile `{profile_name}`; the config file carries profiles: {known}"
        )));
    };
    let Some(table) = profile.maintenance.as_ref() else {
        return Ok(None);
    };
    MaintenancePolicy::from_table(profile_name, table).map(Some)
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_duration(key_path: &str, text: &str) -> Result<Duration> {
    let invalid = || {
        Error::Config(format!(
            "key `{key_path}` has an invalid duration `{text}`; expected `<n>d`, `<n>h` or `<n>m`"
        ))
    };
    let Some(suffix) = text.chars().last() else {
        return Err(invalid());
    };
    let unit = match suffix {
        'd' => SECONDS_PER_DAY,
        'h' => SECONDS_PER_HOUR,
        'm' => SECONDS_PER_MINUTE,
        _ => return Err(invalid()),
    };
    let digits = &text[..text.len() - suffix.len_utf8()];
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let amount: u64 = digits.parse().map_err(|_| invalid())?;
    let seconds = amount.checked_mul(unit).ok_or_else(invalid)?;
    Ok(Duration::from_secs(seconds))
}

fn table_policy(prefix: &str, table: &toml::Table) -> Result<TablePolicy> {
    let mut policy = TablePolicy::default();
    for (key, value) in table {
        read_field(prefix, &mut policy, key, value)?;
    }
    Ok(policy)
}

fn read_field(
    prefix: &str,
    policy: &mut TablePolicy,
    key: &str,
    value: &toml::Value,
) -> Result<()> {
    let path = format!("{prefix}.{key}");
    match key {
        "adaptive_partitioning" => Err(Error::Config(format!("key `{path}` is not yet supported"))),
        "target_file_size_bytes" => {
            policy.target_file_size_bytes = Some(non_negative_integer(&path, value)?);
            Ok(())
        }
        "snapshot_retain_last" => {
            policy.snapshot_retain_last = Some(non_negative_integer(&path, value)?);
            Ok(())
        }
        "snapshot_older_than" => {
            policy.snapshot_older_than = Some(duration_value(&path, value)?);
            Ok(())
        }
        "orphan_older_than" => {
            policy.orphan_older_than = Some(duration_value(&path, value)?);
            Ok(())
        }
        "rewrite_manifests" => {
            let toml::Value::Boolean(flag) = value else {
                return Err(Error::Config(format!("key `{path}` must be a boolean")));
            };
            policy.rewrite_manifests = Some(*flag);
            Ok(())
        }
        "position_delete_ratio" => {
            let toml::Value::Float(ratio) = value else {
                return Err(Error::Config(format!(
                    "key `{path}` must be a number like `0.3`"
                )));
            };
            policy.position_delete_ratio = Some(*ratio);
            Ok(())
        }
        _ => Err(Error::Config(format!(
            "unknown key `{path}`; expected one of: {}",
            POLICY_KEYS.join(", ")
        ))),
    }
}

fn non_negative_integer(path: &str, value: &toml::Value) -> Result<u64> {
    let toml::Value::Integer(number) = value else {
        return Err(Error::Config(format!(
            "key `{path}` must be a non-negative integer"
        )));
    };
    u64::try_from(*number)
        .map_err(|_| Error::Config(format!("key `{path}` must be a non-negative integer")))
}

fn duration_value(path: &str, value: &toml::Value) -> Result<Duration> {
    let toml::Value::String(text) = value else {
        return Err(Error::Config(format!(
            "key `{path}` must be a duration string like `7d`"
        )));
    };
    parse_duration(path, text)
}
