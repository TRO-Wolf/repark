use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};

pub use repark_iceberg::write::OverwriteIntent;

pub const PARTITION_OVERWRITE_MODE_KEY: &str = "spark.sql.sources.partitionOverwriteMode";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionOverwriteMode {
    #[default]
    Static,
    Dynamic,
}

impl PartitionOverwriteMode {
    #[must_use]
    pub fn is_dynamic(self) -> bool {
        self == Self::Dynamic
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_partition_overwrite_mode(raw: &str) -> Result<PartitionOverwriteMode> {
    match raw.to_ascii_lowercase().as_str() {
        "static" => Ok(PartitionOverwriteMode::Static),
        "dynamic" => Ok(PartitionOverwriteMode::Dynamic),
        _ => Err(DataFusionError::Configuration(format!(
            "[INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS] The value '{}' in the config \
             \"{PARTITION_OVERWRITE_MODE_KEY}\" is invalid. It should be one of 'STATIC, \
             DYNAMIC'. SQLSTATE: 22022",
            repark_common::redaction::mask_value_credentials(raw)
        ))),
    }
}

pub const PARTITION_OVERWRITE_MODE_OPTION: &str = "partitionOverwriteMode";

pub const PLAIN_DYNAMIC_OVERWRITE_CARD: &str = "PLAIN-DYNAMIC-OVERWRITE-GUARD-1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlainPathWrite<'a> {
    pub path: &'a str,
    pub overwrite: bool,
    pub partitioned: bool,
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_dynamic_plain_overwrite<'a, I>(
    write: PlainPathWrite<'_>,
    session: PartitionOverwriteMode,
    options: I,
) -> std::result::Result<(), crate::Error>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    if !(write.overwrite && write.partitioned) {
        return Ok(());
    }
    let path = repark_common::redaction::mask_url_userinfo(write.path);
    let chosen = options
        .into_iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case(PARTITION_OVERWRITE_MODE_OPTION))
        .last();
    let (mode, source) = match chosen {
        Some((_, raw)) => (
            parse_partition_overwrite_mode(raw).map_err(|_| invalid_option_refusal(&path, raw))?,
            "the writer option partitionOverwriteMode",
        ),
        None => (session, PARTITION_OVERWRITE_MODE_KEY),
    };
    if !mode.is_dynamic() {
        return Ok(());
    }
    Err(crate::Error::NotImplemented(format!(
        "DataFrameWriter cannot overwrite the partitioned path {path} while {source} is \
         DYNAMIC: Spark would replace only the partitions the written rows hold and keep every \
         other partition, and RePark does not implement dynamic partition overwrite for path \
         writes, so it refuses rather than overwrite every partition; nothing was written. To \
         replace the whole path, write with {PARTITION_OVERWRITE_MODE_KEY} STATIC and no \
         partitionOverwriteMode writer option; to replace only the written partitions, write to \
         an Iceberg table with writeTo(...).overwritePartitions() ({PLAIN_DYNAMIC_OVERWRITE_CARD})"
    )))
}

fn invalid_option_refusal(path: &str, raw: &str) -> crate::Error {
    crate::Error::IllegalArgument(format!(
        "DataFrameWriter cannot overwrite the partitioned path {path}: the writer option \
         partitionOverwriteMode is '{}', which is neither STATIC nor DYNAMIC; Spark fails this \
         write, and RePark refuses it before writing anything ({PLAIN_DYNAMIC_OVERWRITE_CARD})",
        repark_common::redaction::mask_value_credentials(raw)
    ))
}

#[allow(clippy::missing_errors_doc)]
pub fn partition_overwrite_mode_from_config_map<S>(
    config: &HashMap<String, String, S>,
) -> Result<PartitionOverwriteMode>
where
    S: BuildHasher,
{
    match config.get(PARTITION_OVERWRITE_MODE_KEY) {
        Some(raw) => parse_partition_overwrite_mode(raw),
        None => Ok(PartitionOverwriteMode::default()),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionOverwriteModeConfig {
    pub mode: PartitionOverwriteMode,
}

impl ConfigExtension for PartitionOverwriteModeConfig {
    const PREFIX: &'static str = "repark.overwrite";
}

impl ExtensionOptions for PartitionOverwriteModeConfig {
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
            "`{}.{key}` is not a settable option: the overwrite mode is set with \
             `{PARTITION_OVERWRITE_MODE_KEY}` on the session builder; change it at runtime with \
             `SET {PARTITION_OVERWRITE_MODE_KEY}`",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[must_use]
pub fn with_partition_overwrite_mode(
    config: SessionConfig,
    mode: PartitionOverwriteMode,
) -> SessionConfig {
    config.with_option_extension(PartitionOverwriteModeConfig { mode })
}

#[must_use]
pub fn partition_overwrite_mode_from_options(options: &ConfigOptions) -> PartitionOverwriteMode {
    options
        .extensions
        .get::<PartitionOverwriteModeConfig>()
        .map_or(PartitionOverwriteMode::default(), |extension| {
            extension.mode
        })
}

#[must_use]
pub fn partition_overwrite_mode_from_ctx(ctx: &SessionContext) -> PartitionOverwriteMode {
    partition_overwrite_mode_from_options(ctx.copied_config().options())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_static_dynamic_case_insensitive() {
        for (raw, expected) in [
            ("static", PartitionOverwriteMode::Static),
            ("STATIC", PartitionOverwriteMode::Static),
            ("dynamic", PartitionOverwriteMode::Dynamic),
            ("DYNAMIC", PartitionOverwriteMode::Dynamic),
            ("DyNaMiC", PartitionOverwriteMode::Dynamic),
        ] {
            assert_eq!(parse_partition_overwrite_mode(raw).unwrap(), expected);
        }
    }

    #[test]
    fn parse_refuses_unknown_naming_the_key() {
        for raw in ["bogus", "", "dynamic ", " true"] {
            let error = parse_partition_overwrite_mode(raw)
                .expect_err("an unknown mode must not silently become static");
            assert!(
                error.to_string().contains(PARTITION_OVERWRITE_MODE_KEY),
                "refusal must name the key: {error}"
            );
        }
    }

    #[test]
    fn missing_map_key_defaults_static() {
        let config = HashMap::<String, String>::new();
        assert_eq!(
            partition_overwrite_mode_from_config_map(&config).unwrap(),
            PartitionOverwriteMode::Static
        );
    }

    #[test]
    fn map_dynamic_installs_dynamic() {
        let mut config = HashMap::new();
        config.insert(
            PARTITION_OVERWRITE_MODE_KEY.to_string(),
            "dynamic".to_string(),
        );
        assert_eq!(
            partition_overwrite_mode_from_config_map(&config).unwrap(),
            PartitionOverwriteMode::Dynamic
        );
    }

    #[test]
    fn missing_extension_defaults_static() {
        let options = ConfigOptions::new();
        assert_eq!(
            partition_overwrite_mode_from_options(&options),
            PartitionOverwriteMode::Static
        );
    }

    fn guard(
        overwrite: bool,
        partitioned: bool,
        session: PartitionOverwriteMode,
        options: &[(&str, &str)],
    ) -> std::result::Result<(), crate::Error> {
        refuse_dynamic_plain_overwrite(
            PlainPathWrite {
                path: "s3://user:secret@bucket/out",
                overwrite,
                partitioned,
            },
            session,
            options.iter().copied(),
        )
    }

    #[test]
    fn dynamic_partitioned_overwrite_refuses_naming_spark_and_the_card() {
        let error = guard(true, true, PartitionOverwriteMode::Dynamic, &[]).unwrap_err();
        assert!(
            matches!(error, crate::Error::NotImplemented(_)),
            "{error:?}"
        );
        let message = error.to_string();
        for fragment in [
            PLAIN_DYNAMIC_OVERWRITE_CARD,
            PARTITION_OVERWRITE_MODE_KEY,
            "Spark would replace only the partitions the written rows hold",
            "refuses rather than overwrite every partition",
            "nothing was written",
        ] {
            assert!(
                message.contains(fragment),
                "{fragment:?} missing: {message}"
            );
        }
        assert!(
            !message.contains("secret"),
            "userinfo must be masked: {message}"
        );
    }

    #[test]
    fn only_a_partitioned_overwrite_is_guarded() {
        for (overwrite, partitioned) in [(false, true), (true, false), (false, false)] {
            for session in [
                PartitionOverwriteMode::Static,
                PartitionOverwriteMode::Dynamic,
            ] {
                assert!(guard(overwrite, partitioned, session, &[]).is_ok());
                assert!(
                    guard(
                        overwrite,
                        partitioned,
                        session,
                        &[("partitionOverwriteMode", "bogus")]
                    )
                    .is_ok()
                );
            }
        }
    }

    #[test]
    fn the_writer_option_replaces_the_session_both_ways() {
        let static_option = [("partitionOverwriteMode", "STATIC")];
        assert!(guard(true, true, PartitionOverwriteMode::Dynamic, &static_option).is_ok());
        let dynamic_option = [("partitionOverwriteMode", "DYNAMIC")];
        let error = guard(true, true, PartitionOverwriteMode::Static, &dynamic_option).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("the writer option partitionOverwriteMode is DYNAMIC"),
            "{error}"
        );
    }

    #[test]
    fn the_last_option_spelling_wins_case_insensitively() {
        let dynamic_last = [
            ("partitionOverwriteMode", "static"),
            ("PARTITIONOVERWRITEMODE", "dynamic"),
        ];
        assert!(guard(true, true, PartitionOverwriteMode::Static, &dynamic_last).is_err());
        let static_last = [
            ("PartitionOverWriteMODE", "DyNaMiC"),
            ("partitionoverwritemode", "Static"),
        ];
        assert!(guard(true, true, PartitionOverwriteMode::Dynamic, &static_last).is_ok());
        let mixed = [("PartitionOverWriteMODE", "DyNaMiC")];
        assert!(guard(true, true, PartitionOverwriteMode::Static, &mixed).is_err());
    }

    #[test]
    fn an_unparsable_option_refuses_a_partitioned_overwrite() {
        for raw in ["bogus", "", " dynamic"] {
            let error = guard(
                true,
                true,
                PartitionOverwriteMode::Static,
                &[("partitionOverwriteMode", raw)],
            )
            .unwrap_err();
            assert!(
                matches!(error, crate::Error::IllegalArgument(_)),
                "{error:?}"
            );
            let message = error.to_string();
            assert!(message.contains(PLAIN_DYNAMIC_OVERWRITE_CARD), "{message}");
            assert!(message.contains("neither STATIC nor DYNAMIC"), "{message}");
        }
    }

    #[test]
    fn installed_dynamic_is_readable() {
        let config =
            with_partition_overwrite_mode(SessionConfig::new(), PartitionOverwriteMode::Dynamic);
        assert_eq!(
            partition_overwrite_mode_from_options(config.options()),
            PartitionOverwriteMode::Dynamic
        );
    }
}
