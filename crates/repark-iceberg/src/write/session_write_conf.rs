use std::any::Any;
use std::collections::HashMap;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::write::merge::OPERATION_ID_PROP;
use crate::write::write_options::WriterStagingOverrides;
use crate::write::writer_props::parse_compression;

pub const SESSION_CODEC_KEY: &str = "spark.sql.iceberg.compression-codec";
pub const SESSION_LEVEL_KEY: &str = "spark.sql.iceberg.compression-level";
pub const SESSION_SNAPSHOT_PREFIX: &str = "spark.sql.iceberg.snapshot-property.";

#[derive(Debug, Clone, Default)]
pub struct IcebergSessionWriteConf {
    codec: Option<String>,
    level: Option<String>,
    snapshot: Vec<(String, String)>,
}

impl IcebergSessionWriteConf {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.codec.is_none() && self.level.is_none() && self.snapshot.is_empty()
    }

    #[must_use]
    pub fn view(&self) -> SessionWriteView {
        SessionWriteView {
            codec: self.codec.clone(),
            level: self.level.clone(),
            snapshot: self.snapshot.clone(),
        }
    }

    fn set_key(&mut self, key: &str, value: &str) -> bool {
        if key == SESSION_CODEC_KEY {
            self.codec = Some(value.to_string());
            return true;
        }
        if key == SESSION_LEVEL_KEY {
            self.level = Some(value.to_string());
            return true;
        }
        if let Some(suffix) = key.strip_prefix(SESSION_SNAPSHOT_PREFIX) {
            if let Some(position) = self.snapshot.iter().position(|(prior, _)| prior == suffix) {
                self.snapshot[position] = (suffix.to_string(), value.to_string());
            } else {
                self.snapshot.push((suffix.to_string(), value.to_string()));
            }
            return true;
        }
        false
    }

    fn unset_key(&mut self, key: &str) -> bool {
        if key == SESSION_CODEC_KEY {
            self.codec = None;
            return true;
        }
        if key == SESSION_LEVEL_KEY {
            self.level = None;
            return true;
        }
        if let Some(suffix) = key.strip_prefix(SESSION_SNAPSHOT_PREFIX) {
            self.snapshot.retain(|(prior, _)| prior != suffix);
            return true;
        }
        false
    }
}

impl ConfigExtension for IcebergSessionWriteConf {
    const PREFIX: &'static str = "repark.iceberg-session-write";
}

impl ExtensionOptions for IcebergSessionWriteConf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the Iceberg session write confs ride \
             `{SESSION_CODEC_KEY}`, `{SESSION_LEVEL_KEY}` and `{SESSION_SNAPSHOT_PREFIX}<k>` \
             on the session builder, changed at runtime through the session conf map",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SessionWriteView {
    codec: Option<String>,
    level: Option<String>,
    snapshot: Vec<(String, String)>,
}

impl SessionWriteView {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.codec.is_none() && self.level.is_none() && self.snapshot.is_empty()
    }

    #[must_use]
    pub fn merged_snapshot_extra(&self, statement: &[(String, String)]) -> Vec<(String, String)> {
        let mut merged: Vec<(String, String)> =
            Vec::with_capacity(statement.len().saturating_add(self.snapshot.len()));
        for (key, value) in statement {
            merged.push((key.clone(), value.clone()));
        }
        for (key, value) in &self.snapshot {
            if !merged.iter().any(|(prior, _)| prior == key) {
                merged.push((key.clone(), value.clone()));
            }
        }
        merged
    }

    #[must_use]
    pub fn merged_staging(&self, statement: &WriterStagingOverrides) -> WriterStagingOverrides {
        WriterStagingOverrides {
            codec: statement.codec.clone().or_else(|| self.codec.clone()),
            level: statement.level.clone().or_else(|| self.level.clone()),
            target_file_size_bytes: statement.target_file_size_bytes,
        }
    }
}

#[must_use]
pub fn session_write_conf_from_options(options: &ConfigOptions) -> SessionWriteView {
    options
        .extensions
        .get::<IcebergSessionWriteConf>()
        .map_or_else(SessionWriteView::default, IcebergSessionWriteConf::view)
}

#[must_use]
pub fn session_write_conf_from_ctx(ctx: &SessionContext) -> SessionWriteView {
    session_write_conf_from_options(ctx.copied_config().options())
}

#[must_use]
pub fn session_write_conf_is_set(ctx: &SessionContext) -> bool {
    !session_write_conf_from_ctx(ctx).is_empty()
}

#[must_use]
pub fn with_session_write_conf(
    config: SessionConfig,
    conf: IcebergSessionWriteConf,
) -> SessionConfig {
    config.with_option_extension(conf)
}

pub fn session_write_conf_from_config_map<S>(
    config: &HashMap<String, String, S>,
) -> IcebergSessionWriteConf
where
    S: std::hash::BuildHasher,
{
    let mut conf = IcebergSessionWriteConf::default();
    let mut keys: Vec<&String> = config
        .keys()
        .filter(|key| {
            *key == SESSION_CODEC_KEY
                || *key == SESSION_LEVEL_KEY
                || key.starts_with(SESSION_SNAPSHOT_PREFIX)
        })
        .collect();
    keys.sort();
    for key in keys {
        conf.set_key(key, &config[key]);
    }
    conf
}

pub fn apply_session_write_key(options: &mut ConfigOptions, key: &str, value: &str) -> bool {
    if let Some(carrier) = options.extensions.get_mut::<IcebergSessionWriteConf>() {
        carrier.set_key(key, value)
    } else {
        let mut carrier = IcebergSessionWriteConf::default();
        let handled = carrier.set_key(key, value);
        if handled {
            options.extensions.insert(carrier);
        }
        handled
    }
}

pub fn unset_session_write_key(options: &mut ConfigOptions, key: &str) -> bool {
    match options.extensions.get_mut::<IcebergSessionWriteConf>() {
        Some(carrier) => carrier.unset_key(key),
        None => false,
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve_empty_session_write(
    ctx: &SessionContext,
) -> Result<(Vec<(String, String)>, WriterStagingOverrides)> {
    let session = session_write_conf_from_ctx(ctx);
    resolve_write_for_session(&[], &WriterStagingOverrides::none(), &session)
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve_write_for_session(
    statement_snapshot: &[(String, String)],
    statement_staging: &WriterStagingOverrides,
    session: &SessionWriteView,
) -> Result<(Vec<(String, String)>, WriterStagingOverrides)> {
    let staging = session.merged_staging(statement_staging);
    parse_compression(staging.codec.as_deref(), staging.level.as_deref())?;
    Ok((session.merged_snapshot_extra(statement_snapshot), staging))
}

pub fn apply_session_extras<S>(summary: &mut HashMap<String, String, S>, extra: &[(String, String)])
where
    S: std::hash::BuildHasher,
{
    for (key, value) in extra {
        let folded = key.to_ascii_lowercase();
        if folded == "operation" || folded == OPERATION_ID_PROP {
            continue;
        }
        summary.insert(key.clone(), value.clone());
    }
}
