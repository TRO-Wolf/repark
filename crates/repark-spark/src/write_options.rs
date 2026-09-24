use datafusion::error::{DataFusionError, Result};

const SNAPSHOT_PROPERTY_PREFIX: &str = "snapshot-property.";

const MERGE_SCHEMA_OPTION: &str = "mergeschema";

const MERGE_SCHEMA_OPTION_ICEBERG: &str = "merge-schema";

#[derive(Debug, Default, Clone)]
pub struct StatementWriteOptions {
    pub raw: Vec<(String, String)>,
    pub snapshot_extra: Vec<(String, String)>,
    pub write_format: Option<String>,
    pub delete_format: Option<String>,
    pub target_file_size_bytes: Option<u64>,
    pub codec: Option<String>,
    pub level: Option<String>,
    pub distribution_mode: Option<String>,
    pub isolation: Option<String>,
    pub overwrite_intent: repark_iceberg::write::OverwriteIntent,
    pub overwrite_mode_dynamic: bool,
    pub merge_schema: Option<bool>,
    pub output_spec_id: Option<i32>,
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

    #[must_use]
    pub fn merge_schema(&self, ctx: &datafusion::prelude::SessionContext) -> bool {
        self.merge_schema.unwrap_or_else(|| {
            repark_functions::merge_schema::merge_schema_from_options(ctx.copied_config().options())
        })
    }

    #[must_use]
    pub fn carries_only_merge_schema(&self) -> bool {
        self.raw.iter().all(|(key, _)| is_merge_schema_key(key))
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn refuse_if_non_empty(&self, context: &str) -> Result<()> {
        let keys: Vec<&str> = self
            .raw
            .iter()
            .filter(|(key, _)| !is_merge_schema_key(key))
            .map(|(key, _)| key.as_str())
            .collect();
        if keys.is_empty() {
            return Ok(());
        }
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
            output_spec_id: self.output_spec_id,
            ..repark_iceberg::write::WriterStagingOverrides::none()
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub(crate) fn resolve_with_session(
        &self,
        ctx: &datafusion::prelude::SessionContext,
    ) -> Result<(
        Vec<(String, String)>,
        repark_iceberg::write::WriterStagingOverrides,
    )> {
        let session = repark_iceberg::write::session_write_conf_from_ctx(ctx);
        repark_iceberg::write::resolve_write_for_session(
            &self.snapshot_extra,
            &self.staging_overrides(),
            &session,
        )
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
                "delete-format" => options.delete_format = Some(value),
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
                "output-spec-id" => {
                    options.output_spec_id =
                        Some(repark_iceberg::write::parse_output_spec_id(&value)?);
                }
                repark_iceberg::write::OVERWRITE_MODE_OPTION => {
                    options.overwrite_mode_dynamic =
                        repark_iceberg::write::overwrite_mode_option_is_dynamic(&value);
                }
                MERGE_SCHEMA_OPTION | MERGE_SCHEMA_OPTION_ICEBERG => {
                    options.merge_schema = Some(value.trim().eq_ignore_ascii_case("true"));
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

#[must_use]
pub(crate) fn is_merge_schema_key(key: &str) -> bool {
    key.eq_ignore_ascii_case(MERGE_SCHEMA_OPTION)
        || key.eq_ignore_ascii_case(MERGE_SCHEMA_OPTION_ICEBERG)
}

fn validate_write_format(raw: &str) -> Result<String> {
    match raw.to_ascii_lowercase().as_str() {
        "parquet" => Ok("parquet".to_string()),
        "orc" | "avro" => Ok(raw.to_ascii_lowercase()),
        _ => Err(repark_iceberg::write::illegal_argument_error(format!(
            "Invalid file format: {raw}"
        ))),
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
    fn orc_writes_naming_the_normalised_value() {
        let options =
            StatementWriteOptions::validate(vec![pair("write-format", "orc")]).expect("orc");
        assert_eq!(options.write_format.as_deref(), Some("orc"));
    }

    #[test]
    fn bogus_format_mirrors_spark_text() {
        let error = StatementWriteOptions::validate(vec![pair("write-format", "bogus")])
            .expect_err("bogus");
        let DataFusionError::External(inner) = &error else {
            panic!("expected an External marker, got {error:?}");
        };
        let marker = inner
            .downcast_ref::<repark_iceberg::write::IllegalArgumentMarker>()
            .expect("expected an IllegalArgumentMarker");
        assert_eq!(marker.0, "Invalid file format: bogus");
    }

    #[test]
    fn output_spec_id_parses_and_reaches_staging() {
        let options =
            StatementWriteOptions::validate(vec![pair("OUTPUT-SPEC-ID", "0")]).expect("validate");
        assert_eq!(options.output_spec_id, Some(0));
        assert_eq!(options.staging_overrides().output_spec_id, Some(0));
        let error = StatementWriteOptions::validate(vec![pair("output-spec-id", "x")])
            .expect_err("non-int refuses");
        let DataFusionError::External(inner) = &error else {
            panic!("expected an External marker, got {error:?}");
        };
        let marker = inner
            .downcast_ref::<repark_iceberg::write::IllegalArgumentMarker>()
            .expect("expected an IllegalArgumentMarker");
        assert_eq!(marker.0, "For input string: \"x\"");
    }

    #[test]
    fn unknown_keys_ignored_like_spark() {
        let options =
            StatementWriteOptions::validate(vec![pair("repark-nope", "zzz")]).expect("validate");
        assert!(!options.is_empty());
        assert!(options.snapshot_extra.is_empty());
    }

    #[test]
    fn both_merge_schema_spellings_parse_last_wins() {
        let named =
            StatementWriteOptions::validate(vec![pair("mergeSchema", "true")]).expect("validate");
        assert_eq!(named.merge_schema, Some(true));
        let iceberg =
            StatementWriteOptions::validate(vec![pair("MERGE-SCHEMA", "TRUE")]).expect("validate");
        assert_eq!(iceberg.merge_schema, Some(true));
        let last = StatementWriteOptions::validate(vec![
            pair("mergeSchema", "true"),
            pair("mergeSchema", "false"),
        ])
        .expect("validate");
        assert_eq!(last.merge_schema, Some(false));
    }

    #[test]
    fn a_non_true_merge_schema_value_is_false_like_javas_parse_boolean() {
        let options =
            StatementWriteOptions::validate(vec![pair("mergeSchema", "yes")]).expect("validate");
        assert_eq!(options.merge_schema, Some(false));
    }

    #[test]
    fn merge_schema_is_unset_when_no_option_names_it() {
        let options = StatementWriteOptions::validate(vec![pair("write-format", "parquet")])
            .expect("validate");
        assert_eq!(options.merge_schema, None);
    }

    #[test]
    fn merge_schema_alone_does_not_trip_the_unsupported_options_refusal() {
        let options =
            StatementWriteOptions::validate(vec![pair("mergeSchema", "true")]).expect("validate");
        options
            .refuse_if_non_empty("INSERT ... BY NAME")
            .expect("merge-schema is honoured on the by-name path");
        let mixed = StatementWriteOptions::validate(vec![
            pair("mergeSchema", "true"),
            pair("compression-codec", "zstd"),
        ])
        .expect("validate");
        let error = mixed
            .refuse_if_non_empty("INSERT ... BY NAME")
            .expect_err("a real unsupported option still refuses");
        assert!(error.to_string().contains("compression-codec"), "{error}");
        assert!(!error.to_string().contains("mergeschema"), "{error}");
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
