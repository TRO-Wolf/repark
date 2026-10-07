use std::collections::HashMap;

use datafusion::error::DataFusionError;
use pyo3::prelude::*;
use repark_core::{Error, ReparkSession, ReparkSessionBuilder, Result, SESSION_TIME_ZONE_KEY};
use repark_functions::ansi::{
    SPARK_SQL_ANSI_ENABLED_KEY, SparkAnsiConfig, parse_runtime_spark_sql_ansi_enabled,
    parse_spark_sql_ansi_enabled,
};
use repark_functions::case_sensitive::{
    SPARK_SQL_CASE_SENSITIVE_KEY, SparkCaseSensitiveConfig, parse_runtime_spark_sql_case_sensitive,
};
use repark_functions::merge_schema::{
    BooleanConfRefusal, MergeSchemaConfig, SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY,
    boolean_type_mismatch_message, is_merge_schema_session_key, parse_merge_schema_value,
};
use repark_functions::session_time_zone::SessionTimeZoneConfig;

use crate::fence::fenced_span;

const DEFAULT_CATALOG_KEY: &str = repark_core::CatalogRegistry::DEFAULT_CATALOG_KEY;
use crate::session::PyReparkSession;
use crate::to_py_err;

#[pyfunction]
pub fn set_runtime_config(
    session: PyRef<'_, PyReparkSession>,
    key: &str,
    value: &str,
) -> PyResult<()> {
    fenced_span!("py.session", "set_runtime_config", {
        apply_runtime_config(&session.session, key, value, true).map_err(to_py_err)
    })
}

#[pyfunction]
pub fn restore_runtime_config(
    session: PyRef<'_, PyReparkSession>,
    key: &str,
    value: &str,
) -> PyResult<()> {
    fenced_span!("py.session", "restore_runtime_config", {
        apply_runtime_config(&session.session, key, value, false).map_err(to_py_err)
    })
}

#[pyfunction]
pub fn unset_runtime_config(session: PyRef<'_, PyReparkSession>, key: &str) -> PyResult<()> {
    fenced_span!("py.session", "unset_runtime_config", {
        if key == DEFAULT_CATALOG_KEY {
            apply_default_catalog(&session.session, None);
            Ok(())
        } else if session.session.unset_iceberg_session_write_conf(key) {
            Ok(())
        } else {
            Err(to_py_err(Error::IllegalArgument(format!(
                "unset_runtime_config refuses unknown key {key:?}"
            ))))
        }
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(set_runtime_config, module)?)?;
    module.add_function(wrap_pyfunction!(restore_runtime_config, module)?)?;
    module.add_function(wrap_pyfunction!(unset_runtime_config, module)?)?;
    module.add_function(wrap_pyfunction!(session_zone_canonical, module)?)?;
    module.add_function(wrap_pyfunction!(session_defaults, module)?)?;
    module.add_function(wrap_pyfunction!(current_catalog_checked, module)?)?;
    module.add_function(wrap_pyfunction!(auto_session_catalog_wanted, module)?)?;
    module.add_function(wrap_pyfunction!(session_catalog_names, module)?)?;
    module.add_function(wrap_pyfunction!(session_refused_catalog_names, module)?)?;
    module.add_function(wrap_pyfunction!(register_late_catalog_block, module)?)?;
    Ok(())
}

#[pyfunction]
pub fn session_defaults(session: &PyReparkSession) -> PyResult<(String, String)> {
    fenced_span!("py.session", "session_defaults", {
        Ok(session.session.catalogs_snapshot().current_defaults())
    })
}

#[pyfunction]
pub fn current_catalog_checked(session: &PyReparkSession) -> PyResult<(String, String)> {
    fenced_span!("py.session", "current_catalog_checked", {
        session.session.current_catalog_checked().map_err(to_py_err)
    })
}

#[pyfunction]
#[allow(clippy::needless_pass_by_value)]
pub fn auto_session_catalog_wanted(config: HashMap<String, String>) -> bool {
    ReparkSession::auto_session_catalog_wanted(&config)
}

#[pyfunction]
pub fn session_catalog_names(session: &PyReparkSession) -> Vec<String> {
    session
        .session
        .catalogs_snapshot()
        .registered_catalog_names()
}

#[pyfunction]
pub fn session_refused_catalog_names(session: &PyReparkSession) -> Vec<String> {
    session.session.catalogs_snapshot().refused_catalog_names()
}

fn apply_default_catalog(session: &ReparkSession, value: Option<&str>) {
    let name = value.filter(|name| !name.is_empty());
    repark_spark::session_catalog::apply_default_catalog(session, name);
}

#[pyfunction]
pub fn register_late_catalog_block(
    py: Python<'_>,
    session: &PyReparkSession,
    config: HashMap<String, String>,
) -> PyResult<bool> {
    fenced_span!("py.catalog", "register_late_catalog_block", {
        py.detach(|| {
            session
                .runtime
                .block_on(session.session.register_late_catalog_block(&config))
        })
        .map_err(to_py_err)
    })
}

#[pyfunction]
pub fn session_zone_canonical(session: PyRef<'_, PyReparkSession>) -> PyResult<String> {
    fenced_span!("py.session", "session_zone_canonical", {
        let zone = session.session.session_time_zone();
        Ok(repark_core::canonical_session_zone_id(zone.id()))
    })
}

fn apply_runtime_config(
    session: &ReparkSession,
    key: &str,
    value: &str,
    strict_boolean: bool,
) -> Result<()> {
    repark_core::redaction::register_config_value(value);
    if key == SPARK_SQL_ANSI_ENABLED_KEY {
        let enabled = if strict_boolean {
            parse_runtime_spark_sql_ansi_enabled(value).map_err(refused_boolean_knob)?
        } else {
            parse_spark_sql_ansi_enabled(value).map_err(|error| {
                Error::IllegalArgument(repark_core::redaction::mask_value_credentials(
                    &configuration_message(error),
                ))
            })?
        };
        write_ansi_flag(session, enabled)?;
        return Ok(());
    }
    if key == SESSION_TIME_ZONE_KEY {
        let zone = repark_core::parse_runtime_session_zone_value(value)?;
        write_session_zone(session, zone)?;
        return Ok(());
    }
    if key == SPARK_SQL_CASE_SENSITIVE_KEY {
        let enabled =
            parse_runtime_spark_sql_case_sensitive(value).map_err(refused_boolean_knob)?;
        write_case_sensitive_flag(session, enabled)?;
        return Ok(());
    }
    if is_merge_schema_session_key(key) {
        let enabled = parse_merge_schema_value(value).map_err(refused_boolean_knob)?;
        write_merge_schema_flag(session, enabled)?;
        return Ok(());
    }
    if repark_spark::wap::is_wap_session_key(key) {
        let parsed = repark_spark::wap::parse_runtime_wap_value(value);
        write_wap_value(session, key, parsed)?;
        return Ok(());
    }
    if key == repark_core::PARTITION_OVERWRITE_MODE_KEY {
        let mode = repark_core::parse_partition_overwrite_mode(value)
            .map_err(|error| Error::IllegalArgument(configuration_message(error)))?;
        write_overwrite_mode(session, mode)?;
        return Ok(());
    }
    if key == DEFAULT_CATALOG_KEY {
        apply_default_catalog(session, Some(value));
        return Ok(());
    }
    if key == repark_core::TIME_PARSER_POLICY_KEY {
        session.set_time_parser_policy(value)?;
        return Ok(());
    }
    if session.set_iceberg_session_write_conf(key, value) {
        return Ok(());
    }
    Err(Error::IllegalArgument(format!(
        "set_runtime_config refuses unknown key {key:?} (served: \
         {SPARK_SQL_ANSI_ENABLED_KEY:?}, {SESSION_TIME_ZONE_KEY:?}, \
         {SPARK_SQL_CASE_SENSITIVE_KEY:?}, {SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY:?}, \
         {:?}, {:?}, {:?})",
        repark_core::PARTITION_OVERWRITE_MODE_KEY,
        repark_spark::wap::WAP_BRANCH_KEY,
        repark_spark::wap::WAP_ID_KEY
    )))
}

fn configuration_message(error: DataFusionError) -> String {
    match error {
        DataFusionError::Configuration(message) => message,
        other => other.to_string(),
    }
}

fn refused_boolean_knob(refusal: BooleanConfRefusal) -> Error {
    let BooleanConfRefusal { key, raw } = refusal;
    Error::IllegalArgument(boolean_type_mismatch_message(
        key,
        &repark_core::redaction::mask_value_credentials(&raw),
    ))
}

fn write_ansi_flag(session: &ReparkSession, enabled: bool) -> Result<()> {
    let state_lock = session.context().state_ref();
    let mut state = state_lock.write();
    let carrier = state
        .config_mut()
        .options_mut()
        .extensions
        .get_mut::<SparkAnsiConfig>();
    match carrier {
        Some(carrier) => {
            carrier.enabled = enabled;
            Ok(())
        }
        None => Err(Error::IllegalArgument(format!(
            "set_runtime_config refuses {SPARK_SQL_ANSI_ENABLED_KEY:?}: \
             the live session has no Spark ANSI carrier"
        ))),
    }
}

fn write_case_sensitive_flag(session: &ReparkSession, enabled: bool) -> Result<()> {
    let state_lock = session.context().state_ref();
    let mut state = state_lock.write();
    let carrier = state
        .config_mut()
        .options_mut()
        .extensions
        .get_mut::<SparkCaseSensitiveConfig>();
    match carrier {
        Some(carrier) => {
            carrier.enabled = enabled;
            Ok(())
        }
        None => Err(Error::IllegalArgument(format!(
            "set_runtime_config refuses {SPARK_SQL_CASE_SENSITIVE_KEY:?}: \
             the live session has no case-sensitivity carrier"
        ))),
    }
}

fn write_merge_schema_flag(session: &ReparkSession, enabled: bool) -> Result<()> {
    let state_lock = session.context().state_ref();
    let mut state = state_lock.write();
    let carrier = state
        .config_mut()
        .options_mut()
        .extensions
        .get_mut::<MergeSchemaConfig>();
    match carrier {
        Some(carrier) => {
            carrier.enabled = enabled;
            Ok(())
        }
        None => Err(Error::IllegalArgument(format!(
            "set_runtime_config refuses {SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY:?}: \
             the live session has no schema-merging carrier"
        ))),
    }
}

fn write_overwrite_mode(
    session: &ReparkSession,
    mode: repark_core::PartitionOverwriteMode,
) -> Result<()> {
    let state_lock = session.context().state_ref();
    let mut state = state_lock.write();
    let carrier = state
        .config_mut()
        .options_mut()
        .extensions
        .get_mut::<repark_core::PartitionOverwriteModeConfig>();
    match carrier {
        Some(carrier) => {
            carrier.mode = mode;
            Ok(())
        }
        None => Err(Error::IllegalArgument(format!(
            "set_runtime_config refuses {:?}: \
             the live session has no partition-overwrite-mode carrier",
            repark_core::PARTITION_OVERWRITE_MODE_KEY
        ))),
    }
}

fn write_wap_value(session: &ReparkSession, key: &str, value: Option<String>) -> Result<()> {
    let state_lock = session.context().state_ref();
    let mut state = state_lock.write();
    let carrier = state
        .config_mut()
        .options_mut()
        .extensions
        .get_mut::<repark_spark::wap::WapSessionConfig>();
    match carrier {
        Some(carrier) => carrier
            .set_value(key, value)
            .map_err(|error| Error::IllegalArgument(configuration_message(error))),
        None => Err(Error::IllegalArgument(format!(
            "set_runtime_config refuses {key:?}: the live session has no WAP carrier"
        ))),
    }
}

fn write_session_zone(session: &ReparkSession, zone: repark_core::SessionTimeZone) -> Result<()> {
    let state_lock = session.context().state_ref();
    let mut state = state_lock.write();
    let carrier = state
        .config_mut()
        .options_mut()
        .extensions
        .get_mut::<SessionTimeZoneConfig>();
    match carrier {
        Some(carrier) => {
            carrier.set_zone(zone.id());
            session.set_runtime_zone(zone);
            Ok(())
        }
        None => Err(Error::IllegalArgument(format!(
            "set_runtime_config refuses {SESSION_TIME_ZONE_KEY:?}: \
             the live session has no session-zone carrier"
        ))),
    }
}

pub(crate) fn prepare_session_sql(query: &str) -> PyResult<std::borrow::Cow<'_, str>> {
    repark_spark::refuse_declared_function_in_sql(query).map_err(crate::datafusion_to_py_err)?;
    Ok(repark_functions::cast_map::rewrite_map_casts(query)
        .map_or(std::borrow::Cow::Borrowed(query), std::borrow::Cow::Owned))
}

pub(crate) fn register_native_door_functions(ctx: &datafusion::prelude::SessionContext) {
    repark_functions::spark_log1p::register(ctx);
    repark_functions::cast_map::register(ctx);
}

pub(crate) fn apply_session_knobs(
    memory_limit_gb: Option<usize>,
    batch_size: Option<usize>,
    target_partitions: Option<usize>,
    config: Option<HashMap<String, String>>,
) -> PyResult<ReparkSessionBuilder> {
    let mut builder = ReparkSession::builder();
    match memory_limit_gb {
        None => {}
        Some(0) => builder = builder.memory_limit_bytes(0),
        Some(gb) => builder = builder.memory_limit_gb(gb),
    }
    if let Some(0) = batch_size {
        return Err(to_py_err(repark_core::Error::Config(
            "batch_size must be >= 1 (got 0)".to_string(),
        )));
    }
    if let Some(0) = target_partitions {
        return Err(to_py_err(repark_core::Error::Config(
            "target_partitions must be >= 1 (got 0)".to_string(),
        )));
    }
    if let Some(rows) = batch_size {
        builder = builder.batch_size(rows);
    }
    if let Some(parts) = target_partitions {
        builder = builder.target_partitions(parts);
    }
    if let Some(config) = config {
        builder = builder.configs(config);
    }
    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::{
        BooleanConfRefusal, SPARK_SQL_ANSI_ENABLED_KEY, SPARK_SQL_CASE_SENSITIVE_KEY,
        SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY, refused_boolean_knob,
    };
    use repark_core::Error;

    const URL: &str = "postgresql://u:pw@h/db";
    const MASKED: &str = "postgresql://u:***@h/db";

    fn message_for(key: &'static str, raw: &str) -> String {
        match refused_boolean_knob(BooleanConfRefusal {
            key,
            raw: raw.to_string(),
        }) {
            Error::IllegalArgument(message) => message,
            other => panic!("boolean knob refusal must be IllegalArgument, got {other}"),
        }
    }

    fn assert_masked_refusal(key: &'static str) {
        let message = message_for(key, URL);
        assert_eq!(
            repark_common::redaction::mask_value_credentials(URL),
            MASKED,
            "the pin must use the masker's own string"
        );
        assert_eq!(
            message,
            format!(
                "[INVALID_CONF_VALUE.TYPE_MISMATCH] The value '{MASKED}' in the config \"{key}\" \
                 is invalid. It should be a/an 'boolean' value. SQLSTATE: 22022"
            )
        );
    }

    #[test]
    fn case_sensitive_refusal_masks_a_url_password() {
        assert_masked_refusal(SPARK_SQL_CASE_SENSITIVE_KEY);
    }

    #[test]
    fn ansi_refusal_masks_a_url_password() {
        assert_masked_refusal(SPARK_SQL_ANSI_ENABLED_KEY);
    }

    #[test]
    fn merge_schema_refusal_masks_a_url_password() {
        assert_masked_refusal(SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY);
    }

    #[test]
    fn conf_set_masks_a_url_password_for_case_sensitive_and_merge_schema() {
        let session = repark_core::ReparkSession::builder()
            .build()
            .expect("session");
        for key in [
            SPARK_SQL_CASE_SENSITIVE_KEY,
            SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY,
        ] {
            let error = super::apply_runtime_config(&session, key, URL, true)
                .expect_err("a url must refuse");
            let Error::IllegalArgument(message) = error else {
                panic!("boolean knob refusal must be IllegalArgument, got {error}");
            };
            assert!(message.contains(MASKED), "{message}");
            assert!(!message.contains("u:pw@"), "{message}");
            assert!(message.contains("SQLSTATE: 22022"), "{message}");
        }
    }

    #[test]
    fn restore_ansi_masks_a_url_password() {
        let session = repark_core::ReparkSession::builder()
            .build()
            .expect("session");
        let error = super::apply_runtime_config(&session, SPARK_SQL_ANSI_ENABLED_KEY, URL, false)
            .expect_err("a url must refuse");
        let Error::IllegalArgument(message) = error else {
            panic!("ansi restore refusal must be IllegalArgument, got {error}");
        };
        assert!(message.contains(MASKED), "{message}");
        assert!(!message.contains("u:pw@"), "{message}");
        assert!(message.contains("should be boolean, but was"), "{message}");
    }

    #[test]
    fn a_credential_free_bad_value_is_unchanged() {
        let raw = "maybe";
        assert_eq!(repark_common::redaction::mask_value_credentials(raw), raw);
        let message = message_for(SPARK_SQL_CASE_SENSITIVE_KEY, raw);
        assert_eq!(
            message,
            format!(
                "[INVALID_CONF_VALUE.TYPE_MISMATCH] The value '{raw}' in the config \
                 \"{SPARK_SQL_CASE_SENSITIVE_KEY}\" is invalid. It should be a/an 'boolean' \
                 value. SQLSTATE: 22022"
            )
        );
    }
}
