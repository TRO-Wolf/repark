use datafusion::error::{DataFusionError, Result};

const SNAPSHOT_PROPERTY_PREFIX: &str = "snapshot-property.";

#[derive(Debug, Default, Clone)]
pub struct StatementWriteOptions {
    pub raw: Vec<(String, String)>,
    pub snapshot_extra: Vec<(String, String)>,
    pub write_format: Option<String>,
    pub target_file_size_bytes: Option<u64>,
    pub codec: Option<String>,
    pub level: Option<String>,
    pub distribution_mode: Option<String>,
    pub isolation: Option<String>,
    pub overwrite_intent: repark_iceberg::write::OverwriteIntent,
    pub overwrite_mode_dynamic: bool,
}

impl StatementWriteOptions {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn refuse_if_non_empty(&self, context: &str) -> Result<()> {
        if self.raw.is_empty() {
            return Ok(());
        }
        let keys: Vec<&str> = self.raw.iter().map(|(key, _)| key.as_str()).collect();
        Err(DataFusionError::Plan(format!(
            "{context} does not support write options ({}); they are only \
             honoured on Iceberg table writes (ICE-WRITE-OPTIONS-1)",
            keys.join(", ")
        )))
    }

    #[must_use]
    pub fn overwrite_mode(
        &self,
        ctx: &datafusion::prelude::SessionContext,
    ) -> repark_iceberg::write::OverwriteMode {
        repark_iceberg::write::OverwriteMode {
            session_dynamic: repark_core::partition_overwrite_mode_from_ctx(ctx).is_dynamic(),
            intent: self.overwrite_intent,
            option_dynamic: self.overwrite_mode_dynamic,
        }
    }

    #[must_use]
    pub fn staging_overrides(&self) -> repark_iceberg::write::WriterStagingOverrides {
        repark_iceberg::write::WriterStagingOverrides {
            codec: self.codec.clone(),
            level: self.level.clone(),
            target_file_size_bytes: self.target_file_size_bytes,
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub(crate) fn validate(pairs: Vec<(String, String)>) -> Result<Self> {
        let mut merged: Vec<(String, String)> = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            let lowered = key.to_ascii_lowercase();
            if let Some(position) = merged.iter().position(|(prior, _)| *prior == lowered) {
                merged[position] = (lowered, value);
            } else {
                merged.push((lowered, value));
            }
        }
        let mut options = StatementWriteOptions {
            raw: merged,
            ..StatementWriteOptions::default()
        };
        for (key, value) in options.raw.clone() {
            if let Some(suffix) = key.strip_prefix(SNAPSHOT_PROPERTY_PREFIX) {
                options.snapshot_extra.push((suffix.to_string(), value));
                continue;
            }
            match key.as_str() {
                "write-format" => options.write_format = Some(validate_write_format(&value)?),
                "target-file-size-bytes" => {
                    options.target_file_size_bytes =
                        Some(repark_iceberg::write::parse_target_file_size(&value)?);
                }
                "compression-codec" => options.codec = Some(value),
                "compression-level" => options.level = Some(value),
                "distribution-mode" => {
                    options.distribution_mode = Some(validate_distribution_mode(&value)?);
                }
                "isolation-level" => options.isolation = Some(validate_isolation_level(&value)?),
                repark_iceberg::write::OVERWRITE_MODE_OPTION => {
                    options.overwrite_mode_dynamic =
                        repark_iceberg::write::overwrite_mode_option_is_dynamic(&value);
                }
                _ => {}
            }
        }
        repark_iceberg::write::parse_compression(
            options.codec.as_deref(),
            options.level.as_deref(),
        )?;
        Ok(options)
    }
}

fn validate_write_format(raw: &str) -> Result<String> {
    match raw.to_ascii_lowercase().as_str() {
        "parquet" => Ok("parquet".to_string()),
        "orc" | "avro" => Err(DataFusionError::NotImplemented(format!(
            "write-format {raw:?} has no RePark Iceberg writer — only parquet is written \
             (ICE-WRITE-OPTIONS-1 ORC/AVRO declared 2026-09-17)"
        ))),
        _ => Err(DataFusionError::Plan(format!("Invalid file format: {raw}"))),
    }
}

fn validate_distribution_mode(raw: &str) -> Result<String> {
    match raw.to_ascii_lowercase().as_str() {
        "none" | "hash" | "range" => Ok(raw.to_ascii_lowercase()),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid distribution mode: {raw}"
        ))),
    }
}

fn validate_isolation_level(raw: &str) -> Result<String> {
    match raw.to_ascii_lowercase().as_str() {
        "none" | "snapshot" | "serializable" => Ok(raw.to_string()),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid isolation level: {raw}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(key: &str, value: &str) -> (String, String) {
        (key.to_string(), value.to_string())
    }

    #[test]
    fn empty_pairs_validate_empty() {
        let options = StatementWriteOptions::validate(Vec::new()).expect("validate");
        assert!(options.is_empty());
    }

    #[test]
    fn snapshot_extra_survives_validation() {
        let options = StatementWriteOptions::validate(vec![
            pair("snapshot-property.run_id", "abc"),
            pair("write-format", "parquet"),
        ])
        .expect("validate");
        assert_eq!(
            options.snapshot_extra,
            vec![("run_id".to_string(), "abc".to_string())]
        );
        assert_eq!(options.write_format.as_deref(), Some("parquet"));
        assert!(!options.is_empty());
    }

    #[test]
    fn suffix_lower_cases_like_spark() {
        let options =
            StatementWriteOptions::validate(vec![pair("SNAPSHOT-PROPERTY.UPPER_KEY", "v")])
                .expect("validate");
        assert_eq!(
            options.snapshot_extra,
            vec![("upper_key".to_string(), "v".to_string())]
        );
    }

    #[test]
    fn empty_suffix_keeps_empty_key_like_spark() {
        let options = StatementWriteOptions::validate(vec![pair("snapshot-property.", "v")])
            .expect("validate");
        assert_eq!(
            options.snapshot_extra,
            vec![(String::new(), "v".to_string())]
        );
    }

    #[test]
    fn duplicate_keys_last_wins_like_spark() {
        let options = StatementWriteOptions::validate(vec![
            pair("write-format", "orc"),
            pair("WRITE-FORMAT", "parquet"),
        ])
        .expect("validate");
        assert_eq!(options.write_format.as_deref(), Some("parquet"));
    }

    #[test]
    fn orc_refuses_naming_the_registry_row() {
        let error =
            StatementWriteOptions::validate(vec![pair("write-format", "orc")]).expect_err("orc");
        assert!(error.to_string().contains("ICE-WRITE-OPTIONS-1"));
    }

    #[test]
    fn bogus_format_mirrors_spark_text() {
        let error = StatementWriteOptions::validate(vec![pair("write-format", "bogus")])
            .expect_err("bogus");
        assert!(error.to_string().contains("Invalid file format: bogus"));
    }

    #[test]
    fn unknown_keys_ignored_like_spark() {
        let options =
            StatementWriteOptions::validate(vec![pair("repark-nope", "zzz")]).expect("validate");
        assert!(!options.is_empty());
        assert!(options.snapshot_extra.is_empty());
    }

    #[test]
    fn utf8_pairs_pass_through_unscathed() {
        let options = StatementWriteOptions::validate(vec![pair(
            "snapshot-property.café-🎉",
            "naïve töne 🎉",
        )])
        .expect("validate");
        assert_eq!(
            options.snapshot_extra,
            vec![("café-🎉".to_string(), "naïve töne 🎉".to_string())]
        );
    }
}
