//! Spark-door `spark.sql.ansi.enabled` carrier + the ANSI `/0` / `% 0` raise kernel.
//!
//! **Why a sibling `ConfigExtension`, not [`crate::cardinality::ReparkSqlSettings`].**
//! The cardinality extension is the `repark.sql.*` safety namespace (plan-time expansion
//! ceilings, local-filesystem DDL). This key is Spark `SQLConf` (`spark.sql.ansi.enabled`).
//! Mixing it into `repark.sql` would be a second spelling. DataFusion looks an extension
//! namespace up on the text before the FIRST `.`, so `PREFIX = "spark"` would swallow every
//! `spark.*` `SET` (including `spark.sql.session.timeZone`). `PREFIX = "repark.ansi"` is the
//! same two-segment carrier shape as [`crate::session_time_zone`] — installed only by
//! `SparkExtension::configure`, never a `SET`-able twin.
//!
//! **Type-validation seam (A1).** `repark_common::Error::Config` documents Spark's
//! `IllegalArgumentException` for `spark.sql.ansi.enabled=notabool`. That variant is produced
//! only inside `repark-core` (session builder / catalog parse) — `configure()` returns
//! `datafusion::error::Result` and `session.rs` folds it with `engine_err`, which never emits
//! `Error::Config`. The builder is CLOSED. This module fail-louds with
//! [`DataFusionError::Configuration`] and the Spark message needle (`should be boolean, but
//! was …`). The exception *class* at the Python boundary is therefore the base
//! `PySparkException`, not `IllegalArgumentException` — named residue, not a silent omit.
//!
//! **Default TRUE** (owner Q10=A / Spark 4). Missing carrier (a bare `SessionContext` that
//! still installed [`crate::analyzer::SparkExprSemantics`]) is also TRUE — the analyzer *is*
//! the Spark-door semantics layer. `ansi=false` restores the `nullif` `/0` wrap.

use std::any::Any;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionConfig;

/// Canonical Spark `SQLConf` key. Parsed from the session builder map in `configure()`.
pub const SPARK_SQL_ANSI_ENABLED_KEY: &str = "spark.sql.ansi.enabled";

/// Spark 4 / owner Q10=A default.
pub const DEFAULT_SPARK_SQL_ANSI_ENABLED: bool = true;

/// Embedded UDF name. The analyzer matches on this so a second analyze is a fixpoint.
pub(crate) const ANSI_NONZERO_DIVISOR_NAME: &str = "__repark_ansi_nonzero_divisor__";

/// ===========================================================================================
/// Session-scoped ANSI flag the Spark analyzer reads out of [`ConfigOptions`].
/// ===========================================================================================
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkAnsiConfig {
    /// `true` → `/0` and `% 0` raise; `false` → NULL (legacy `nullif` wrap).
    pub enabled: bool,
}

impl Default for SparkAnsiConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_SPARK_SQL_ANSI_ENABLED,
        }
    }
}

impl ConfigExtension for SparkAnsiConfig {
    /// Two segments so `SET spark.sql.ansi.enabled` cannot address this carrier (DataFusion
    /// looks up an extension on the text before the first `.`).
    const PREFIX: &'static str = "repark.ansi";
}

impl ExtensionOptions for SparkAnsiConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    /// Always refuses — the knob is `spark.sql.ansi.enabled` on the session builder.
    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: ANSI mode is set with \
             `{SPARK_SQL_ANSI_ENABLED_KEY}` on the session builder and is fixed at session build",
            Self::PREFIX
        )))
    }

    /// Empty so the carrier is not advertised as a `SET`-able option.
    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

/// ===========================================================================================
/// Parse `spark.sql.ansi.enabled`. Spark's needle: `should be boolean, but was {raw}`.
/// ===========================================================================================
///
/// # Errors
/// A present value that is not a boolean token.
pub fn parse_spark_sql_ansi_enabled(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(DataFusionError::Configuration(format!(
            "The value '{raw}' in the config \"{SPARK_SQL_ANSI_ENABLED_KEY}\" is invalid. \
             {SPARK_SQL_ANSI_ENABLED_KEY} should be boolean, but was {raw}"
        ))),
    }
}

/// ===========================================================================================
/// Read the builder conf map. Missing key → [`DEFAULT_SPARK_SQL_ANSI_ENABLED`].
/// ===========================================================================================
///
/// # Errors
/// Present but unparsable value (the `notabool` fail-loud).
pub fn spark_ansi_from_config_map<S>(config: &HashMap<String, String, S>) -> Result<bool>
where
    S: BuildHasher,
{
    match config.get(SPARK_SQL_ANSI_ENABLED_KEY) {
        Some(raw) => parse_spark_sql_ansi_enabled(raw),
        None => Ok(DEFAULT_SPARK_SQL_ANSI_ENABLED),
    }
}

/// ===========================================================================================
/// Attach the ANSI flag to a [`SessionConfig`] (Spark door `configure` hook).
/// ===========================================================================================
#[must_use]
pub fn with_spark_ansi_config(config: SessionConfig, enabled: bool) -> SessionConfig {
    config.with_option_extension(SparkAnsiConfig { enabled })
}

/// ===========================================================================================
/// Analyzer accessor. Missing carrier → default TRUE (Spark-door semantics layer).
/// ===========================================================================================
#[must_use]
pub fn spark_ansi_enabled_from_options(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<SparkAnsiConfig>()
        .map_or(DEFAULT_SPARK_SQL_ANSI_ENABLED, |extension| {
            extension.enabled
        })
}

/// ===========================================================================================
/// Wrap `divisor` in the embedded raise-on-zero UDF (ANSI ON path of `guard_zero_divisor`).
/// ===========================================================================================
#[must_use]
pub fn guard_nonzero_divisor(divisor: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        ansi_nonzero_divisor_udf(),
        vec![divisor],
    ))
}

/// The embedded UDF instance. Never registered — the analyzer embeds it by value.
#[must_use]
pub fn ansi_nonzero_divisor_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(AnsiNonzeroDivisor::new()))
}

/// ===========================================================================================
/// Pass-through numeric kernel: zero → Spark-shaped `DIVIDE_BY_ZERO`; NULL / nonzero pass.
/// ===========================================================================================
#[derive(Debug)]
struct AnsiNonzeroDivisor {
    signature: Signature,
}

impl AnsiNonzeroDivisor {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for AnsiNonzeroDivisor {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for AnsiNonzeroDivisor {}

impl Hash for AnsiNonzeroDivisor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for AnsiNonzeroDivisor {
    crate::shim_udf_boilerplate!("__repark_ansi_nonzero_divisor__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let data_type = arg_types.first().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'{ANSI_NONZERO_DIVISOR_NAME}' expects one numeric argument"
            ))
        })?;
        if !data_type.is_numeric() {
            return Err(DataFusionError::Plan(format!(
                "'{ANSI_NONZERO_DIVISOR_NAME}' expects a numeric argument, got {data_type}"
            )));
        }
        Ok(data_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(first) = args.args.first() else {
            return Err(DataFusionError::Execution(format!(
                "'{ANSI_NONZERO_DIVISOR_NAME}' expects one argument"
            )));
        };
        match first {
            ColumnarValue::Scalar(scalar) => {
                refuse_if_numeric_zero(scalar)?;
                Ok(ColumnarValue::Scalar(scalar.clone()))
            }
            ColumnarValue::Array(array) => {
                for row in 0..array.len() {
                    if array.is_null(row) {
                        continue;
                    }
                    let scalar = ScalarValue::try_from_array(array.as_ref(), row)?;
                    refuse_if_numeric_zero(&scalar)?;
                }
                Ok(ColumnarValue::Array(Arc::clone(array)))
            }
        }
    }
}

fn refuse_if_numeric_zero(value: &ScalarValue) -> Result<()> {
    if is_numeric_zero(value) {
        return Err(divide_by_zero_error());
    }
    Ok(())
}

fn is_numeric_zero(value: &ScalarValue) -> bool {
    if value.is_null() {
        return false;
    }
    match ScalarValue::new_zero(&value.data_type()) {
        Ok(zero) => *value == zero,
        Err(_) => false,
    }
}

fn divide_by_zero_error() -> DataFusionError {
    DataFusionError::Execution(
        "[DIVIDE_BY_ZERO] Division by zero. Use try_divide to tolerate divisor being 0 \
         and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" \
         to bypass this error. (ArithmeticException)"
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_boolean_tokens() {
        assert!(parse_spark_sql_ansi_enabled("true").unwrap());
        assert!(parse_spark_sql_ansi_enabled("TRUE").unwrap());
        assert!(parse_spark_sql_ansi_enabled("1").unwrap());
        assert!(!parse_spark_sql_ansi_enabled("false").unwrap());
        assert!(!parse_spark_sql_ansi_enabled("FALSE").unwrap());
        assert!(!parse_spark_sql_ansi_enabled("0").unwrap());
    }

    #[test]
    fn parse_notabool_names_the_spark_needle() {
        let error = parse_spark_sql_ansi_enabled("notabool")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("should be boolean, but was notabool"),
            "Spark message needle missing: {error}"
        );
        assert!(
            error.contains(SPARK_SQL_ANSI_ENABLED_KEY),
            "error must name the key: {error}"
        );
    }

    #[test]
    fn missing_map_key_defaults_true() {
        let config = HashMap::<String, String>::new();
        assert!(spark_ansi_from_config_map(&config).unwrap());
    }

    #[test]
    fn map_false_is_legacy() {
        let mut config = HashMap::new();
        config.insert(SPARK_SQL_ANSI_ENABLED_KEY.to_string(), "false".to_string());
        assert!(!spark_ansi_from_config_map(&config).unwrap());
    }

    #[test]
    fn missing_extension_defaults_true() {
        let options = ConfigOptions::new();
        assert!(spark_ansi_enabled_from_options(&options));
    }

    #[test]
    fn installed_false_is_readable() {
        let config = with_spark_ansi_config(SessionConfig::new(), false);
        assert!(!spark_ansi_enabled_from_options(config.options()));
    }
}
