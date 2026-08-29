//! Plan-time cardinality ceilings for planner-visible expansion functions (SEC-01).
//!
//! Arrow expression buffers bypass the `FairSpillPool`; planner-visible literal expansions therefore
//! fail early with a catchable error naming [`MAX_ARRAY_ELEMENTS_KEY`]. Non-literal counts remain residuals.

use std::sync::Arc;

use datafusion::common::config::{ConfigExtension, ConfigOptions};
use datafusion::common::extensions_options;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::{BinaryExpr, ScalarFunction};
use datafusion::logical_expr::{Expr, LogicalPlan, Operator};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionConfig;

/// Max recursion depth when folding planner-visible const integer trees (`Cast` / `BinaryExpr`).
const CONST_FOLD_MAX_DEPTH: u32 = 32;

/// Canonical conf key (Spark-style camelCase). Default: [`DEFAULT_MAX_ARRAY_ELEMENTS`].
pub const MAX_ARRAY_ELEMENTS_KEY: &str = "repark.sql.maxArrayElements";

/// Underscore alias for [`MAX_ARRAY_ELEMENTS_KEY`].
pub const MAX_ARRAY_ELEMENTS_KEY_ALT: &str = "repark.sql.max_array_elements";

/// Default plan-time expansion ceiling (Q23 / greylight).
pub const DEFAULT_MAX_ARRAY_ELEMENTS: u64 = 10_000_000;

/// Canonical conf key for local-filesystem DDL (SEC-02). Default **false**.
pub const ALLOW_LOCAL_FILESYSTEM_DDL_KEY: &str = "repark.sql.allowLocalFilesystemDDL";

/// Underscore alias for [`ALLOW_LOCAL_FILESYSTEM_DDL_KEY`].
pub const ALLOW_LOCAL_FILESYSTEM_DDL_KEY_ALT: &str = "repark.sql.allow_local_filesystem_ddl";

/// Canonical conf key for CREATE/CTAS `format-version = 3` (V3-2). Default **false**.
pub const ALLOW_CREATE_FORMAT_VERSION_3_KEY: &str = "repark.sql.allowCreateFormatVersion3";

/// Underscore alias for [`ALLOW_CREATE_FORMAT_VERSION_3_KEY`].
pub const ALLOW_CREATE_FORMAT_VERSION_3_KEY_ALT: &str = "repark.sql.allow_create_format_version_3";

/// Validated session knobs for plan-time SQL safety ceilings / gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReparkSqlSettings {
    /// Max planner-visible array / string expansion size.
    pub max_array_elements: u64,
    /// When true, allow `CREATE EXTERNAL` / `COPY TO` local paths outside warehouse roots.
    pub allow_local_filesystem_ddl: bool,
    /// When true, CREATE/CTAS may request Iceberg format v3. Default false: v3 tables cannot
    /// yet do merge-on-read / deletion-vector writes (V3-3), so accidental create is a trap.
    pub allow_create_format_version_3: bool,
}

impl Default for ReparkSqlSettings {
    fn default() -> Self {
        Self {
            max_array_elements: DEFAULT_MAX_ARRAY_ELEMENTS,
            allow_local_filesystem_ddl: false,
            allow_create_format_version_3: false,
        }
    }
}

impl ReparkSqlSettings {
    /// Parse `repark.sql.maxArrayElements` (positive integer; `0` refused).
    ///
    /// # Errors
    /// Non-integer or zero values fail loud naming the conf key.
    pub fn parse_max_array_elements(raw: &str) -> Result<u64> {
        let value: u64 = raw.parse().map_err(|_| {
            DataFusionError::Plan(format!(
                "config `{MAX_ARRAY_ELEMENTS_KEY}` must be a positive integer (got {raw:?})"
            ))
        })?;
        if value == 0 {
            return Err(DataFusionError::Plan(format!(
                "config `{MAX_ARRAY_ELEMENTS_KEY}` must be >= 1 (got 0)"
            )));
        }
        Ok(value)
    }

    /// Parse `repark.sql.allowLocalFilesystemDDL` (`true`/`false`, case-insensitive).
    ///
    /// # Errors
    /// Unknown values fail loud naming the conf key.
    pub fn parse_allow_local_filesystem_ddl(raw: &str) -> Result<bool> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            _ => Err(DataFusionError::Plan(format!(
                "config `{ALLOW_LOCAL_FILESYSTEM_DDL_KEY}` must be true or false (got {raw:?})"
            ))),
        }
    }

    /// Parse `repark.sql.allowCreateFormatVersion3` (`true`/`false`, case-insensitive).
    ///
    /// # Errors
    /// Unknown values fail loud naming the conf key.
    pub fn parse_allow_create_format_version_3(raw: &str) -> Result<bool> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            _ => Err(DataFusionError::Plan(format!(
                "config `{ALLOW_CREATE_FORMAT_VERSION_3_KEY}` must be true or false (got {raw:?})"
            ))),
        }
    }
}

/// ===========================================================================================
/// Pull SQL safety knobs from a builder conf map. Missing keys → defaults.
/// ===========================================================================================
///
/// # Errors
/// Present but unparsable values.
pub fn repark_sql_settings_from_config_map<S>(
    config: &std::collections::HashMap<String, String, S>,
) -> Result<ReparkSqlSettings>
where
    S: std::hash::BuildHasher,
{
    let max_array_elements = match config
        .get(MAX_ARRAY_ELEMENTS_KEY)
        .or_else(|| config.get(MAX_ARRAY_ELEMENTS_KEY_ALT))
    {
        Some(raw) => ReparkSqlSettings::parse_max_array_elements(raw)?,
        None => DEFAULT_MAX_ARRAY_ELEMENTS,
    };
    let allow_local_filesystem_ddl = match config
        .get(ALLOW_LOCAL_FILESYSTEM_DDL_KEY)
        .or_else(|| config.get(ALLOW_LOCAL_FILESYSTEM_DDL_KEY_ALT))
    {
        Some(raw) => ReparkSqlSettings::parse_allow_local_filesystem_ddl(raw)?,
        None => false,
    };
    let allow_create_format_version_3 = match config
        .get(ALLOW_CREATE_FORMAT_VERSION_3_KEY)
        .or_else(|| config.get(ALLOW_CREATE_FORMAT_VERSION_3_KEY_ALT))
    {
        Some(raw) => ReparkSqlSettings::parse_allow_create_format_version_3(raw)?,
        None => false,
    };
    Ok(ReparkSqlSettings {
        max_array_elements,
        allow_local_filesystem_ddl,
        allow_create_format_version_3,
    })
}

extensions_options! {
    /// RePark SQL safety knobs (session-scoped).
    pub struct ReparkSqlConfig {
        /// Plan-time expansion ceiling (`repark.sql.maxArrayElements`).
        pub max_array_elements: u64, default = 10_000_000_u64
        /// When true, local CREATE EXTERNAL / COPY TO outside warehouse roots is allowed.
        pub allow_local_filesystem_ddl: bool, default = false
        /// When true, CREATE/CTAS may request Iceberg format v3.
        pub allow_create_format_version_3: bool, default = false
    }
}

impl ConfigExtension for ReparkSqlConfig {
    const PREFIX: &'static str = "repark.sql";
}

/// ===========================================================================================
/// Attach [`ReparkSqlSettings`] to a [`SessionConfig`] (called from session build).
/// ===========================================================================================
#[must_use]
pub fn with_repark_sql_config(config: SessionConfig, settings: ReparkSqlSettings) -> SessionConfig {
    config.with_option_extension(ReparkSqlConfig {
        max_array_elements: settings.max_array_elements,
        allow_local_filesystem_ddl: settings.allow_local_filesystem_ddl,
        allow_create_format_version_3: settings.allow_create_format_version_3,
    })
}

/// ===========================================================================================
/// Resolve settings from a live config options map (analyzer / SQL gate).
/// ===========================================================================================
#[must_use]
pub fn repark_sql_settings_from_options(options: &ConfigOptions) -> ReparkSqlSettings {
    options
        .extensions
        .get::<ReparkSqlConfig>()
        .map(|extension| ReparkSqlSettings {
            max_array_elements: if extension.max_array_elements == 0 {
                DEFAULT_MAX_ARRAY_ELEMENTS
            } else {
                extension.max_array_elements
            },
            allow_local_filesystem_ddl: extension.allow_local_filesystem_ddl,
            allow_create_format_version_3: extension.allow_create_format_version_3,
        })
        .unwrap_or_default()
}

/// ===========================================================================================
/// Resolve CREATE/CTAS Iceberg format version; v3 requires [`ALLOW_CREATE_FORMAT_VERSION_3_KEY`].
/// pins: v3-2-create-v3-opt-in/C-001, C-003, C-004
/// ===========================================================================================
///
/// Model: Grok 4.6 xHigh
///
/// # Errors
/// Unsupported version, or v3 requested while the session opt-in is off.
pub fn resolve_create_format_version(
    requested: Option<&str>,
    allow_v3: bool,
    property_name: &str,
    form: &str,
) -> Result<u8> {
    let Some(raw) = requested.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(2);
    };
    match raw {
        "2" => Ok(2),
        "3" if allow_v3 => Ok(3),
        "3" => Err(DataFusionError::NotImplemented(format!(
            "{form} '{property_name}' = '3' is not enabled — set `{ALLOW_CREATE_FORMAT_VERSION_3_KEY}` \
             = true (v3 tables cannot yet do merge-on-read row-level writes; default create stays \
             format v2)"
        ))),
        other => Err(DataFusionError::NotImplemented(format!(
            "{form} '{property_name}' = '{other}' is not supported (tables are created as Iceberg \
             format v2)"
        ))),
    }
}

/// ===========================================================================================
/// Refuse when a planner-visible expansion exceeds `max` — names the conf to raise.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] when `cardinality > max`.
pub fn refuse_if_over_ceiling(function_name: &str, cardinality: u64, max: u64) -> Result<()> {
    if cardinality > max {
        return Err(DataFusionError::Plan(format!(
            "{function_name} requested {cardinality} elements, which exceeds conf \
             `{MAX_ARRAY_ELEMENTS_KEY}` = {max}. Raise the conf (session builder \
             `.config(\"{MAX_ARRAY_ELEMENTS_KEY}\", \"…\")`) if a larger expansion is intentional."
        )));
    }
    Ok(())
}

/// Extract a planner-visible non-negative integer from literals and bounded constant trees.
#[must_use]
pub fn literal_nonneg_u64(expr: &Expr) -> Option<u64> {
    let value = const_i128(expr, CONST_FOLD_MAX_DEPTH)?;
    if value < 0 {
        return None;
    }
    u64::try_from(value).ok()
}

fn scalar_to_i128(scalar: &ScalarValue) -> Option<i128> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Float32(Some(value)) => f64_trunc_to_i128(f64::from(*value)),
        ScalarValue::Float64(Some(value)) => f64_trunc_to_i128(*value),
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => parse_decimal_integer_text(text),
        _ => None,
    }
}

fn parse_decimal_integer_text(text: &str) -> Option<i128> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<i128>().ok()
}

/// Truncate a finite `f64` toward zero into `i128` for ceiling checks.
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn f64_trunc_to_i128(value: f64) -> Option<i128> {
    if !value.is_finite() {
        return None;
    }
    let truncated = value.trunc();
    format!("{truncated:.0}").parse::<i128>().ok()
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn const_f64(expr: &Expr, depth: u32) -> Option<f64> {
    if depth == 0 {
        return None;
    }
    match expr {
        Expr::Literal(scalar, _) => scalar_to_f64(scalar),
        Expr::Cast(cast) => const_f64(cast.expr.as_ref(), depth - 1)
            .or_else(|| const_i128(cast.expr.as_ref(), depth - 1).map(|v| v as f64)),
        Expr::TryCast(try_cast) => const_f64(try_cast.expr.as_ref(), depth - 1)
            .or_else(|| const_i128(try_cast.expr.as_ref(), depth - 1).map(|v| v as f64)),
        Expr::Negative(inner) => Some(-const_f64(inner.as_ref(), depth - 1)?),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = const_f64(left.as_ref(), depth - 1)?;
            let right = const_f64(right.as_ref(), depth - 1)?;
            match op {
                Operator::Plus => Some(left + right),
                Operator::Minus => Some(left - right),
                Operator::Multiply => Some(left * right),
                Operator::Divide if right != 0.0 => Some(left / right),
                Operator::Modulo if right != 0.0 => Some(left % right),
                _ => None,
            }
        }
        Expr::Alias(alias) => const_f64(alias.expr.as_ref(), depth - 1),
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            const_f64_scalar_function(func.name(), args, depth)
        }
        _ => None,
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn scalar_to_f64(scalar: &ScalarValue) -> Option<f64> {
    match scalar {
        ScalarValue::Float32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Float64(Some(value)) => Some(*value),
        other => scalar_to_i128(other).map(|value| value as f64),
    }
}

fn const_f64_scalar_function(name: &str, args: &[Expr], depth: u32) -> Option<f64> {
    match name.to_ascii_lowercase().as_str() {
        "abs" if args.len() == 1 => Some(const_f64(&args[0], depth - 1)?.abs()),
        "floor" if args.len() == 1 => Some(const_f64(&args[0], depth - 1)?.floor()),
        "ceil" | "ceiling" if args.len() == 1 => Some(const_f64(&args[0], depth - 1)?.ceil()),
        "trunc" | "truncate" if args.len() == 1 => Some(const_f64(&args[0], depth - 1)?.trunc()),
        "round" if args.len() == 1 => Some(const_f64(&args[0], depth - 1)?.round()),
        "round" if args.len() == 2 => {
            let value = const_f64(&args[0], depth - 1)?;
            let digits = const_i128(&args[1], depth - 1)?;
            if digits == 0 {
                Some(value.round())
            } else {
                None
            }
        }
        "power" | "pow" if args.len() == 2 => {
            let base = const_f64(&args[0], depth - 1)?;
            let exponent = const_f64(&args[1], depth - 1)?;
            let value = base.powf(exponent);
            if value.is_finite() { Some(value) } else { None }
        }
        "log10" if args.len() == 1 => {
            let value = const_f64(&args[0], depth - 1)?;
            (value > 0.0).then_some(value.log10())
        }
        "log2" if args.len() == 1 => {
            let value = const_f64(&args[0], depth - 1)?;
            (value > 0.0).then_some(value.log2())
        }
        "ln" | "log" if args.len() == 1 => {
            let value = const_f64(&args[0], depth - 1)?;
            (value > 0.0).then_some(value.ln())
        }
        "exp" if args.len() == 1 => {
            let value = const_f64(&args[0], depth - 1)?.exp();
            value.is_finite().then_some(value)
        }
        "sqrt" if args.len() == 1 => {
            let value = const_f64(&args[0], depth - 1)?;
            (value >= 0.0).then_some(value.sqrt())
        }
        "arrow_cast" | "cast" if !args.is_empty() => const_f64(&args[0], depth - 1),
        _ => None,
    }
}

/// Fold a planner-visible integer const tree to `i128` (depth-bounded).
fn const_i128(expr: &Expr, depth: u32) -> Option<i128> {
    if depth == 0 {
        return None;
    }
    match expr {
        Expr::Literal(scalar, _) => scalar_to_i128(scalar),
        Expr::Cast(cast) => const_i128(cast.expr.as_ref(), depth - 1)
            .or_else(|| f64_trunc_to_i128(const_f64(cast.expr.as_ref(), depth - 1)?)),
        Expr::TryCast(try_cast) => const_i128(try_cast.expr.as_ref(), depth - 1)
            .or_else(|| f64_trunc_to_i128(const_f64(try_cast.expr.as_ref(), depth - 1)?)),
        Expr::Negative(inner) => const_i128(inner.as_ref(), depth - 1)?.checked_neg(),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = const_i128(left.as_ref(), depth - 1)?;
            let right = const_i128(right.as_ref(), depth - 1)?;
            match op {
                Operator::Plus => left.checked_add(right),
                Operator::Minus => left.checked_sub(right),
                Operator::Multiply => left.checked_mul(right),
                Operator::Divide if right != 0 => Some(left / right),
                Operator::Modulo if right != 0 => Some(left % right),
                Operator::BitwiseAnd => Some(left & right),
                Operator::BitwiseOr => Some(left | right),
                Operator::BitwiseXor => Some(left ^ right),
                Operator::BitwiseShiftLeft if (0..128).contains(&right) => {
                    left.checked_shl(u32::try_from(right).ok()?)
                }
                Operator::BitwiseShiftRight if (0..128).contains(&right) => {
                    Some(left.checked_shr(u32::try_from(right).ok()?)?)
                }
                _ => None,
            }
        }
        Expr::Alias(alias) => const_i128(alias.expr.as_ref(), depth - 1),
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            match func.name().to_ascii_lowercase().as_str() {
                "abs" if args.len() == 1 => const_i128(&args[0], depth - 1)
                    .and_then(i128::checked_abs)
                    .or_else(|| f64_trunc_to_i128(const_f64(&args[0], depth - 1)?.abs())),
                "arrow_cast" | "cast" if !args.is_empty() => const_i128(&args[0], depth - 1)
                    .or_else(|| f64_trunc_to_i128(const_f64(&args[0], depth - 1)?)),
                "floor" | "ceil" | "ceiling" | "trunc" | "truncate" | "round" | "power" | "pow"
                | "log" | "log10" | "log2" | "ln" | "exp" | "sqrt" => {
                    f64_trunc_to_i128(const_f64_scalar_function(func.name(), args, depth)?)
                }
                "coalesce" => {
                    for arg in args {
                        if let Some(value) = const_i128(arg, depth - 1) {
                            return Some(value);
                        }
                    }
                    None
                }
                "greatest" | "least" if !args.is_empty() => {
                    let mut values = Vec::with_capacity(args.len());
                    for arg in args {
                        values.push(const_i128(arg, depth - 1)?);
                    }
                    if func.name().eq_ignore_ascii_case("greatest") {
                        values.into_iter().max()
                    } else {
                        values.into_iter().min()
                    }
                }
                "nullif" if args.len() == 2 => {
                    let left = const_i128(&args[0], depth - 1)?;
                    let right = const_i128(&args[1], depth - 1)?;
                    if left == right { None } else { Some(left) }
                }
                _ => None,
            }
        }
        Expr::Case(case) => const_i128_case(case, depth),
        Expr::ScalarSubquery(subquery) if subquery.outer_ref_columns.is_empty() => {
            const_i128_from_scalar_subquery(subquery.subquery.as_ref(), depth - 1)
        }
        _ => None,
    }
}

/// Trivial scalar subquery `SELECT <const>` (no outer refs) — C5-SEC-001.
fn const_i128_from_scalar_subquery(plan: &LogicalPlan, depth: u32) -> Option<i128> {
    if depth == 0 {
        return None;
    }
    match plan {
        LogicalPlan::Projection(projection) if projection.expr.len() == 1 => {
            const_i128(&projection.expr[0], depth - 1)
        }
        LogicalPlan::Filter(filter) => {
            const_i128_from_scalar_subquery(filter.input.as_ref(), depth - 1)
        }
        LogicalPlan::Limit(limit) => {
            const_i128_from_scalar_subquery(limit.input.as_ref(), depth - 1)
        }
        LogicalPlan::Subquery(subquery) => {
            const_i128_from_scalar_subquery(subquery.subquery.as_ref(), depth - 1)
        }
        _ => None,
    }
}

fn const_bool(expr: &Expr, depth: u32) -> Option<bool> {
    if depth == 0 {
        return None;
    }
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(value)), _) => Some(*value),
        Expr::Alias(alias) => const_bool(alias.expr.as_ref(), depth - 1),
        Expr::Not(inner) => Some(!const_bool(inner.as_ref(), depth - 1)?),
        _ => None,
    }
}

/// Evaluate constant `CASE` trees:
/// - searched: `CASE WHEN const-bool THEN const … [ELSE const]`
/// - simple: `CASE const WHEN const THEN const …` via integer equality
fn const_i128_case(case: &datafusion::logical_expr::expr::Case, depth: u32) -> Option<i128> {
    if depth == 0 {
        return None;
    }
    if let Some(base) = &case.expr {
        let base_value = const_i128(base.as_ref(), depth - 1)?;
        for (when_expr, then_expr) in &case.when_then_expr {
            let when_value = const_i128(when_expr.as_ref(), depth - 1)?;
            if when_value == base_value {
                return const_i128(then_expr.as_ref(), depth - 1);
            }
        }
        return case
            .else_expr
            .as_ref()
            .and_then(|else_expr| const_i128(else_expr.as_ref(), depth - 1));
    }
    for (when_expr, then_expr) in &case.when_then_expr {
        if const_bool(when_expr.as_ref(), depth - 1)? {
            return const_i128(then_expr.as_ref(), depth - 1);
        }
    }
    case.else_expr
        .as_ref()
        .and_then(|else_expr| const_i128(else_expr.as_ref(), depth - 1))
}

/// Signed integer const (for sequence start/stop/step), including CAST / arithmetic.
#[must_use]
pub fn literal_i64(expr: &Expr) -> Option<i64> {
    let value = const_i128(expr, CONST_FOLD_MAX_DEPTH)?;
    i64::try_from(value).ok()
}

/// Cardinality of an inclusive numeric sequence `start..=stop` by `stride` (Spark `sequence`).
///
/// Returns `None` when `stride == 0` or the direction cannot produce values (empty series → `Some(0)`).
#[must_use]
pub fn sequence_cardinality(start: i64, stop: i64, stride: i64) -> Option<u64> {
    if stride == 0 {
        return None;
    }
    if stride > 0 {
        if start > stop {
            return Some(0);
        }
        let span = stop.checked_sub(start)?;
        let steps = span / stride;
        u64::try_from(steps).ok()?.checked_add(1)
    } else {
        if start < stop {
            return Some(0);
        }
        let span = start.checked_sub(stop)?;
        let steps = span / (-stride);
        u64::try_from(steps).ok()?.checked_add(1)
    }
}

/// Check facade-built `array_repeat` / `repeat` / `sequence` args against the default ceiling;
/// the analyzer variant reads the session-specific ceiling.
///
/// # Errors
/// Plan error when a literal expansion exceeds [`DEFAULT_MAX_ARRAY_ELEMENTS`].
pub fn refuse_facade_literal_expansion(function_name: &str, args: &[Expr]) -> Result<()> {
    refuse_literal_expansion(function_name, args, DEFAULT_MAX_ARRAY_ELEMENTS)
}

/// Shared literal expansion check (facade + analyzer).
///
/// # Errors
/// Plan error when a literal expansion exceeds `max`.
pub fn refuse_literal_expansion(function_name: &str, args: &[Expr], max: u64) -> Result<()> {
    match function_name {
        "array_repeat" | "repeat" => {
            if args.len() >= 2
                && let Some(count) = literal_nonneg_u64(&args[1])
            {
                refuse_if_over_ceiling(function_name, count, max)?;
            }
        }
        "sequence" | "generate_series" | "gen_series" | "range" if args.len() >= 2 => {
            let start = literal_i64(&args[0]);
            let stop = literal_i64(&args[1]);
            let stride = if args.len() >= 3 {
                literal_i64(&args[2]).unwrap_or(1)
            } else {
                1
            };
            if let (Some(start), Some(stop)) = (start, stop)
                && let Some(card) = sequence_cardinality(start, stop, stride)
            {
                refuse_if_over_ceiling(function_name, card, max)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// ===========================================================================================
/// Analyzer rule: refuse planner-visible `array_repeat` / `repeat` / `sequence` expansions.
/// ===========================================================================================
#[derive(Debug, Default)]
pub struct ArrayCardinalityCeiling;

impl AnalyzerRule for ArrayCardinalityCeiling {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let max = repark_sql_settings_from_options(config).max_array_elements;
        plan.transform_up_with_subqueries(|node| check_plan_node(node, max))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "array_cardinality_ceiling"
    }
}

fn check_plan_node(plan: LogicalPlan, max: u64) -> Result<Transformed<LogicalPlan>> {
    plan.map_expressions(|expr| {
        expr.transform_up(|node| {
            if let Expr::ScalarFunction(ScalarFunction { func, args }) = &node {
                refuse_literal_expansion(func.name(), args, max)?;
            }
            Ok(Transformed::no(node))
        })
    })
}

/// Analyzer rules this module contributes (installed by the session with Spark semantics).
#[must_use]
pub fn analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    vec![Arc::new(ArrayCardinalityCeiling)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::lit;
    use datafusion::prelude::SessionContext;

    #[test]
    fn defaults_match_q23_and_q22() {
        let settings = ReparkSqlSettings::default();
        assert_eq!(settings.max_array_elements, 10_000_000);
        assert!(!settings.allow_local_filesystem_ddl);
    }

    #[test]
    fn parse_max_rejects_zero_and_garbage() {
        assert!(ReparkSqlSettings::parse_max_array_elements("0").is_err());
        assert!(ReparkSqlSettings::parse_max_array_elements("nope").is_err());
        assert_eq!(
            ReparkSqlSettings::parse_max_array_elements("100").unwrap(),
            100
        );
    }

    #[test]
    fn sequence_cardinality_inclusive() {
        assert_eq!(sequence_cardinality(1, 3, 1), Some(3));
        assert_eq!(sequence_cardinality(1, 10, 3), Some(4)); // 1,4,7,10
        assert_eq!(sequence_cardinality(5, 1, -2), Some(3)); // 5,3,1
        assert_eq!(sequence_cardinality(1, 0, 1), Some(0));
        assert_eq!(sequence_cardinality(1, 10, 0), None);
    }

    #[test]
    fn refuse_array_repeat_literal_over_ceiling() {
        let args = vec![lit(1i64), lit(10_000_001i64)];
        let err = refuse_literal_expansion("array_repeat", &args, DEFAULT_MAX_ARRAY_ELEMENTS)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(MAX_ARRAY_ELEMENTS_KEY) && err.contains("10000001"),
            "message must name conf and cardinality: {err}"
        );
    }

    #[test]
    fn under_ceiling_ok() {
        let args = vec![lit(1i64), lit(3i64)];
        refuse_literal_expansion("array_repeat", &args, DEFAULT_MAX_ARRAY_ELEMENTS).unwrap();
    }

    #[test]
    fn config_map_camel_and_snake() {
        let mut map = std::collections::HashMap::new();
        map.insert(MAX_ARRAY_ELEMENTS_KEY.to_string(), "42".to_string());
        map.insert(
            ALLOW_LOCAL_FILESYSTEM_DDL_KEY.to_string(),
            "true".to_string(),
        );
        let settings = repark_sql_settings_from_config_map(&map).unwrap();
        assert_eq!(settings.max_array_elements, 42);
        assert!(settings.allow_local_filesystem_ddl);
        assert!(!settings.allow_create_format_version_3);

        map.clear();
        map.insert(MAX_ARRAY_ELEMENTS_KEY_ALT.to_string(), "7".to_string());
        map.insert(
            ALLOW_LOCAL_FILESYSTEM_DDL_KEY_ALT.to_string(),
            "false".to_string(),
        );
        map.insert(
            ALLOW_CREATE_FORMAT_VERSION_3_KEY_ALT.to_string(),
            "true".to_string(),
        );
        let settings = repark_sql_settings_from_config_map(&map).unwrap();
        assert_eq!(settings.max_array_elements, 7);
        assert!(!settings.allow_local_filesystem_ddl);
        assert!(settings.allow_create_format_version_3);
    }

    // pins: v3-2-create-v3-opt-in/C-001, C-002, C-003, C-004, C-007
    #[test]
    fn resolve_create_format_version_v3_needs_opt_in() {
        assert_eq!(
            resolve_create_format_version(None, false, "format-version", "TBLPROPERTIES").unwrap(),
            2
        );
        assert_eq!(
            resolve_create_format_version(Some("2"), false, "format-version", "TBLPROPERTIES")
                .unwrap(),
            2
        );
        assert_eq!(
            resolve_create_format_version(Some("3"), true, "format-version", "TBLPROPERTIES")
                .unwrap(),
            3
        );
        let err =
            resolve_create_format_version(Some("3"), false, "format-version", "TBLPROPERTIES")
                .unwrap_err()
                .to_string();
        assert!(
            err.contains(ALLOW_CREATE_FORMAT_VERSION_3_KEY) && err.contains("format-version"),
            "opt-in refuse must name conf and property: {err}"
        );
        let err = resolve_create_format_version(Some("1"), false, "format_version", "WITH")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("format_version") && err.contains('1'),
            "v1 must refuse naming the key: {err}"
        );
        assert_eq!(
            resolve_create_format_version(None, true, "format-version", "TBLPROPERTIES").unwrap(),
            2,
            "opt-in must not change the unspecified default"
        );
        assert_eq!(
            resolve_create_format_version(Some("2"), true, "format-version", "TBLPROPERTIES")
                .unwrap(),
            2
        );
        let err = ReparkSqlSettings::parse_allow_create_format_version_3("notabool")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(ALLOW_CREATE_FORMAT_VERSION_3_KEY) && err.contains("notabool"),
            "unparsable opt-in must fail loud naming the conf: {err}"
        );
    }

    #[tokio::test]
    async fn analyzer_refuses_sql_array_repeat() {
        let settings = ReparkSqlSettings {
            max_array_elements: 100,
            ..ReparkSqlSettings::default()
        };
        let config = with_repark_sql_config(SessionConfig::new(), settings);
        let ctx = SessionContext::new_with_config(config);
        for rule in analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        let df = ctx.sql("SELECT array_repeat(1, 101) AS a").await.unwrap();
        let err = crate::analyze_eagerly(&ctx.state(), df.logical_plan().clone())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(MAX_ARRAY_ELEMENTS_KEY),
            "free-SQL path must name conf: {err}"
        );
    }

    /// Constant arithmetic and casts must not bypass the ceiling.
    #[tokio::test]
    async fn analyzer_refuses_const_arithmetic_and_cast_counts() {
        let settings = ReparkSqlSettings {
            max_array_elements: 100,
            ..ReparkSqlSettings::default()
        };
        let config = with_repark_sql_config(SessionConfig::new(), settings);
        let ctx = SessionContext::new_with_config(config);
        for rule in analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        for sql in [
            "SELECT array_repeat(1, 100 + 1) AS a",
            "SELECT array_repeat(1, 10 * 11) AS a",
            "SELECT array_repeat(1, CAST(101 AS BIGINT)) AS a",
            "SELECT array_repeat(1, CAST(CAST(101 AS INT) AS BIGINT)) AS a",
            "SELECT array_repeat(1, CAST('101' AS BIGINT)) AS a",
            "SELECT array_repeat(1, CAST(power(10, 2) AS BIGINT) + 1) AS a",
            "SELECT array_repeat(1, CAST(floor(101.1) AS BIGINT)) AS a",
            "SELECT array_repeat(1, CAST(ceil(100.1) AS BIGINT)) AS a",
            "SELECT array_repeat(1, CAST(round(101.4) AS BIGINT)) AS a",
            "SELECT array_repeat(1, CAST(trunc(101.9) AS BIGINT)) AS a",
            "SELECT array_repeat(1, CAST(log10(1000) AS BIGINT) * 50) AS a",
            "SELECT array_repeat(1, CAST(sqrt(10201) AS BIGINT)) AS a",
            "SELECT array_repeat(1, 101 & 127) AS a",
            "SELECT array_repeat(1, 50 << 1 | 1) AS a",
            "SELECT array_repeat(1, arrow_cast(101, 'Int64')) AS a",
            "SELECT array_repeat(1, (SELECT 101)) AS a",
            "SELECT array_repeat(1, -(-101)) AS a",
            "SELECT array_repeat(1, abs(101)) AS a",
            "SELECT array_repeat(1, coalesce(101, 1)) AS a",
            "SELECT array_repeat(1, greatest(101, 1)) AS a",
            "SELECT array_repeat(1, least(200, 101)) AS a",
            "SELECT array_repeat(1, nullif(101, 0)) AS a",
            "SELECT array_repeat(1, CASE WHEN true THEN 101 ELSE 1 END) AS a",
            "SELECT array_repeat(1, CASE WHEN false THEN 1 WHEN true THEN 101 END) AS a",
            "SELECT array_repeat(1, CASE 1 WHEN 1 THEN 101 ELSE 1 END) AS a",
            "SELECT range(1, 101) AS a",
            "SELECT generate_series(1, 50 + 51) AS a",
            "SELECT repeat('x', 101) AS a",
        ] {
            let df = ctx
                .sql(sql)
                .await
                .unwrap_or_else(|error| panic!("plan {sql}: {error}"));
            let err = crate::analyze_eagerly(&ctx.state(), df.logical_plan().clone())
                .expect_err("const over-ceiling must refuse")
                .to_string();
            assert!(
                err.contains(MAX_ARRAY_ELEMENTS_KEY),
                "must name conf for {sql}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn analyzer_allows_under_ceiling() {
        let settings = ReparkSqlSettings {
            max_array_elements: 100,
            ..ReparkSqlSettings::default()
        };
        let config = with_repark_sql_config(SessionConfig::new(), settings);
        let ctx = SessionContext::new_with_config(config);
        for rule in analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        let df = ctx.sql("SELECT array_repeat(1, 3) AS a").await.unwrap();
        let plan = crate::analyze_eagerly(&ctx.state(), df.logical_plan().clone()).unwrap();
        let df = ctx.execute_logical_plan(plan).await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
    }
}
