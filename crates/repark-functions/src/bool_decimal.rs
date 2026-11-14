use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, AsArray, Decimal128Array, Decimal256Array, PrimitiveArray, as_boolean_array,
};
use arrow::datatypes::{
    ArrowPrimitiveType, Decimal128Type, DecimalType, Float32Type, Float64Type, i256,
};
use arrow::error::ArrowError;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

use crate::ansi::spark_ansi_enabled_from_options;

pub(crate) const BOOL_TO_DECIMAL_NAME: &str = "__repark_bool_to_decimal__";
pub(crate) const FLOAT_TO_DECIMAL_NAME: &str = "__repark_float_to_decimal__";
const ARROW_CAST_NAME: &str = "arrow_cast";

pub fn install_bool_decimal_cast(ctx: &SessionContext) {
    ctx.add_analyzer_rule(Arc::new(BoolDecimalCast));
}

pub fn install_shared_analyzer_rules(ctx: &SessionContext) {
    crate::integer_spark::install_integer_overflow(ctx);
    install_bool_decimal_cast(ctx);
}

#[derive(Debug, Default)]
pub struct BoolDecimalCast;

impl AnalyzerRule for BoolDecimalCast {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let null_on_overflow = !spark_ansi_enabled_from_options(config);
        plan.transform_up_with_subqueries(|node| rewrite_plan(node, null_on_overflow))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "bool_decimal_cast"
    }
}

fn rewrite_plan(plan: LogicalPlan, null_on_overflow: bool) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten =
            expr.transform_down(|node| Ok(rewrite_cast(node, &schema, null_on_overflow)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_cast(expr: Expr, schema: &DFSchema, null_on_overflow: bool) -> Transformed<Expr> {
    match float_decimal_cast(&expr, schema) {
        Some(rewritten) => Transformed::yes(rewritten),
        None => rewrite_bool_cast(expr, schema, null_on_overflow),
    }
}

fn float_decimal_cast(expr: &Expr, schema: &DFSchema) -> Option<Expr> {
    let (operand, target, safe) = match expr {
        Expr::Cast(cast) => (cast.expr.as_ref(), cast.field.data_type().clone(), false),
        Expr::TryCast(cast) => (cast.expr.as_ref(), cast.field.data_type().clone(), true),
        Expr::ScalarFunction(function)
            if function.func.name() == ARROW_CAST_NAME && function.args.len() == 2 =>
        {
            let (Some(operand), Some(Expr::Literal(ScalarValue::Utf8(Some(text)), _))) =
                (function.args.first(), function.args.get(1))
            else {
                return None;
            };
            (operand, text.parse::<DataType>().ok()?, false)
        }
        _ => return None,
    };
    if !matches!(target, DataType::Decimal128(_, _)) {
        return None;
    }
    if !matches!(
        operand.get_type(schema),
        Ok(DataType::Float32 | DataType::Float64)
    ) {
        return None;
    }
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::from(FloatToDecimal::new(target, safe))),
        vec![operand.clone()],
    )))
}

fn rewrite_bool_cast(expr: Expr, schema: &DFSchema, null_on_overflow: bool) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    let target = cast.field.data_type().clone();
    if !matches!(
        target,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    ) {
        return Transformed::no(expr);
    }
    if !matches!(cast.expr.get_type(schema), Ok(DataType::Boolean)) {
        return Transformed::no(expr);
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::from(BoolToDecimal::new(
            target,
            null_on_overflow,
        ))),
        vec![cast.expr.as_ref().clone()],
    )))
}

#[derive(Debug)]
pub struct BoolToDecimal {
    signature: Signature,
    target: DataType,
    null_on_overflow: bool,
}

impl BoolToDecimal {
    fn new(target: DataType, null_on_overflow: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target,
            null_on_overflow,
        }
    }

    fn precision_scale(&self) -> (u8, i8) {
        match self.target {
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                (precision, scale)
            }
            _ => (10, 0),
        }
    }

    fn unscaled_of(&self, bit: bool, precision: u8, scale: i8) -> Result<Option<i128>> {
        if !bit {
            return Ok(Some(0));
        }
        if i32::from(precision) - i32::from(scale) < 1 {
            if self.null_on_overflow {
                return Ok(None);
            }
            return Err(crate::decimal_spark::numeric_out_of_range_error(
                precision, scale,
            ));
        }
        Ok(Some(pow10(scale)))
    }

    fn decimal_of(&self, bit: bool, precision: u8, scale: i8) -> Result<ScalarValue> {
        let unscaled = self.unscaled_of(bit, precision, scale)?;
        match self.target {
            DataType::Decimal256(_, _) => Ok(ScalarValue::Decimal256(
                unscaled.map(i256::from_i128),
                precision,
                scale,
            )),
            _ => Ok(ScalarValue::Decimal128(unscaled, precision, scale)),
        }
    }
}

impl PartialEq for BoolToDecimal {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
            && self.target == other.target
            && self.null_on_overflow == other.null_on_overflow
    }
}

impl Eq for BoolToDecimal {}

impl Hash for BoolToDecimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.target.hash(state);
        self.null_on_overflow.hash(state);
    }
}

impl ScalarUDFImpl for BoolToDecimal {
    crate::shim_udf_boilerplate!("__repark_bool_to_decimal__");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let first = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("'{BOOL_TO_DECIMAL_NAME}' expects one argument"))
        })?;
        let (precision, scale) = self.precision_scale();
        let exposed = i32::from(precision) - i32::from(scale) < 1;
        Ok(Field::new(
            self.name(),
            self.target.clone(),
            first.is_nullable() || exposed,
        )
        .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (precision, scale) = self.precision_scale();
        match args.args.first() {
            Some(ColumnarValue::Scalar(ScalarValue::Boolean(value))) => {
                Ok(ColumnarValue::Scalar(match value {
                    None => match self.target {
                        DataType::Decimal256(_, _) => {
                            ScalarValue::Decimal256(None, precision, scale)
                        }
                        _ => ScalarValue::Decimal128(None, precision, scale),
                    },
                    Some(bit) => self.decimal_of(*bit, precision, scale)?,
                }))
            }
            Some(ColumnarValue::Array(array)) => {
                let bools = as_boolean_array(array.as_ref());
                let mut values = Vec::with_capacity(bools.len());
                for index in 0..bools.len() {
                    if bools.is_null(index) {
                        values.push(None);
                    } else {
                        values.push(self.unscaled_of(bools.value(index), precision, scale)?);
                    }
                }
                if matches!(self.target, DataType::Decimal256(_, _)) {
                    let wide: Vec<Option<i256>> =
                        values.into_iter().map(|v| v.map(i256::from_i128)).collect();
                    Ok(ColumnarValue::Array(Arc::new(
                        Decimal256Array::from(wide).with_precision_and_scale(precision, scale)?,
                    )))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(
                        Decimal128Array::from(values).with_precision_and_scale(precision, scale)?,
                    )))
                }
            }
            _ => Err(DataFusionError::Execution(format!(
                "'{BOOL_TO_DECIMAL_NAME}' expects a boolean argument"
            ))),
        }
    }
}

#[derive(Debug)]
pub struct FloatToDecimal {
    signature: Signature,
    target: DataType,
    safe: bool,
}

impl FloatToDecimal {
    fn new(target: DataType, safe: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target,
            safe,
        }
    }

    fn precision_scale(&self) -> (u8, i8) {
        match self.target {
            DataType::Decimal128(precision, scale) => (precision, scale),
            _ => (38, 0),
        }
    }

    fn unscaled_of(&self, value: f64, precision: u8, scale: i8) -> Result<Option<i128>> {
        let overflow = || {
            ArrowError::CastError(format!(
                "Cannot cast to Decimal128({precision}, {scale}). Overflowing on {value:?}"
            ))
        };
        let Some(unscaled) = repark_common::java_double::java_double_unscaled(value, scale) else {
            return if self.safe {
                Ok(None)
            } else {
                Err(overflow().into())
            };
        };
        if Decimal128Type::is_valid_decimal_precision(unscaled, precision) {
            return Ok(Some(unscaled));
        }
        if self.safe {
            return Ok(None);
        }
        Decimal128Type::validate_decimal_precision(unscaled, precision, scale)?;
        Err(overflow().into())
    }

    fn convert<T>(&self, values: &PrimitiveArray<T>) -> Result<Decimal128Array>
    where
        T: ArrowPrimitiveType,
        T::Native: Into<f64>,
    {
        let (precision, scale) = self.precision_scale();
        let converted: Decimal128Array = if self.safe {
            values.unary_opt(|value| {
                self.unscaled_of(value.into(), precision, scale)
                    .ok()
                    .flatten()
            })
        } else {
            values.try_unary(|value| {
                self.unscaled_of(value.into(), precision, scale)?
                    .ok_or_else(|| DataFusionError::Internal(String::new()))
            })?
        };
        Ok(converted.with_precision_and_scale(precision, scale)?)
    }
}

impl PartialEq for FloatToDecimal {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target && self.safe == other.safe
    }
}

impl Eq for FloatToDecimal {}

impl Hash for FloatToDecimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.target.hash(state);
        self.safe.hash(state);
    }
}

impl ScalarUDFImpl for FloatToDecimal {
    crate::shim_udf_boilerplate!("__repark_float_to_decimal__");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let first = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("'{FLOAT_TO_DECIMAL_NAME}' expects one argument"))
        })?;
        Ok(Field::new(
            self.name(),
            self.target.clone(),
            first.is_nullable() || self.safe,
        )
        .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (precision, scale) = self.precision_scale();
        match args.args.first() {
            Some(ColumnarValue::Scalar(scalar)) => {
                let value = match scalar {
                    ScalarValue::Float64(value) => *value,
                    ScalarValue::Float32(value) => value.map(f64::from),
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "'{FLOAT_TO_DECIMAL_NAME}' expects a FLOAT or DOUBLE argument"
                        )));
                    }
                };
                let unscaled = match value {
                    Some(value) => self.unscaled_of(value, precision, scale)?,
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    unscaled, precision, scale,
                )))
            }
            Some(ColumnarValue::Array(array)) => {
                let converted = match array.data_type() {
                    DataType::Float64 => self.convert(array.as_primitive::<Float64Type>())?,
                    DataType::Float32 => self.convert(array.as_primitive::<Float32Type>())?,
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "'{FLOAT_TO_DECIMAL_NAME}' expects a FLOAT or DOUBLE argument"
                        )));
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(converted)))
            }
            None => Err(DataFusionError::Execution(format!(
                "'{FLOAT_TO_DECIMAL_NAME}' expects one argument"
            ))),
        }
    }
}

fn pow10(scale: i8) -> i128 {
    let mut value = 1i128;
    for _ in 0..scale.max(0) {
        value = value.saturating_mul(10);
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionConfig;

    fn ctx_with(ansi: Option<bool>) -> SessionContext {
        let config = match ansi {
            Some(enabled) => crate::ansi::with_spark_ansi_config(SessionConfig::new(), enabled),
            None => SessionConfig::new(),
        };
        let ctx = SessionContext::new_with_config(config);
        ctx.add_analyzer_rule(Arc::new(BoolDecimalCast));
        ctx
    }

    async fn flags(ctx: &SessionContext, sql: &str) -> Vec<bool> {
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        crate::analyze_eagerly(&ctx.state(), plan)
            .unwrap()
            .schema()
            .fields()
            .iter()
            .map(|field| field.is_nullable())
            .collect()
    }

    async fn values(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in batches {
            let column = batch.column(0);
            for row in 0..batch.num_rows() {
                out.push(array_value_to_string(column, row).unwrap());
            }
        }
        out
    }

    #[tokio::test]
    async fn wide_targets_are_nonnull_with_one_and_zero() {
        let ctx = ctx_with(Some(true));
        assert_eq!(
            flags(&ctx, "SELECT CAST(true AS DECIMAL(10,2)) AS v").await,
            vec![false]
        );
        assert_eq!(
            values(&ctx, "SELECT CAST(true AS DECIMAL(10,2)) AS v").await,
            vec!["1.00".to_string()]
        );
        assert_eq!(
            values(&ctx, "SELECT CAST(false AS DECIMAL(10,2)) AS v").await,
            vec!["0.00".to_string()]
        );
        assert_eq!(
            values(&ctx, "SELECT CAST(true AS DECIMAL(1,0)) AS v").await,
            vec!["1".to_string()]
        );
    }

    #[tokio::test]
    async fn narrow_target_nulls_or_raises_by_ansi() {
        let legacy = ctx_with(Some(false));
        assert_eq!(
            flags(&legacy, "SELECT CAST(true AS DECIMAL(2,2)) AS v").await,
            vec![true]
        );
        assert_eq!(
            values(&legacy, "SELECT CAST(true AS DECIMAL(2,2)) AS v").await,
            vec![String::new()]
        );
        let ansi = ctx_with(Some(true));
        assert_eq!(
            flags(&ansi, "SELECT CAST(true AS DECIMAL(2,2)) AS v").await,
            vec![true]
        );
        let error = ansi
            .sql("SELECT CAST(true AS DECIMAL(2,2)) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("NUMERIC_VALUE_OUT_OF_RANGE"), "{error}");
    }

    #[tokio::test]
    async fn native_door_defaults_to_ansi() {
        let native = ctx_with(None);
        let error = native
            .sql("SELECT CAST(true AS DECIMAL(2,2)) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("NUMERIC_VALUE_OUT_OF_RANGE"), "{error}");
        assert_eq!(
            values(&native, "SELECT CAST(true AS DECIMAL(10,2)) AS v").await,
            vec!["1.00".to_string()]
        );
    }

    #[test]
    fn the_kernel_rounds_the_jdk_17_text_half_up() {
        use repark_common::java_double::java_double_unscaled;
        let widened = |value: f32| f64::from(value);
        for (value, scale, want) in [
            (1.255, 2, Some(126)),
            (1.005, 2, Some(101)),
            (2.675, 2, Some(268)),
            (9.995, 2, Some(1000)),
            (-1.255, 2, Some(-126)),
            (0.125, 2, Some(13)),
            (-0.0, 2, Some(0)),
            (1e-20, 18, Some(0)),
            (1.254_999_999_999_999_7, 2, Some(125)),
            (0.300_000_000_000_000_04, 18, Some(300_000_000_000_000_040)),
            (1.005, 18, Some(1_005_000_000_000_000_000)),
            (1e23, 0, Some(99_999_999_999_999_990_000_000)),
            (
                2.828_793_848_061_59e17,
                18,
                Some(282_879_384_806_159_008 * 10_i128.pow(18)),
            ),
            (widened(1.255), 2, Some(125)),
            (widened(1.255), 18, Some(1_254_999_995_231_628_400)),
            (1.255, 30, Some(1255 * 10_i128.pow(27))),
            (1e300, 2, None),
            (f64::NAN, 2, None),
            (f64::INFINITY, 0, None),
        ] {
            assert_eq!(
                java_double_unscaled(value, scale),
                want,
                "{value:?} at scale {scale}"
            );
        }
    }

    fn half_up_from_text(text: &str, scale: i8) -> i128 {
        let (negative, body) = text
            .strip_prefix('-')
            .map_or((false, text), |rest| (true, rest));
        let (mantissa, exponent) = body
            .split_once('E')
            .map_or((body, 0), |(m, e)| (m, e.parse::<i32>().unwrap()));
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let digits: String = format!("{whole}{fraction}");
        let point = i32::try_from(whole.len()).unwrap() + exponent + i32::from(scale);
        let padded = if point > i32::try_from(digits.len()).unwrap() {
            format!(
                "{digits}{}",
                "0".repeat(usize::try_from(point).unwrap() - digits.len())
            )
        } else {
            digits
        };
        let (kept, next) = if point <= 0 {
            (
                String::from("0"),
                if point == 0 {
                    padded.chars().next()
                } else {
                    None
                },
            )
        } else {
            let cut = usize::try_from(point).unwrap();
            (padded[..cut].to_string(), padded.chars().nth(cut))
        };
        let mut value: i128 = kept.parse().unwrap();
        if next.is_some_and(|digit| digit >= '5') {
            value += 1;
        }
        if negative { -value } else { value }
    }

    #[test]
    fn the_fast_path_agrees_with_the_text_on_a_dense_sweep() {
        use repark_common::java_double::java_double_unscaled;
        let mut value = 0.000_1_f64;
        let mut checked = 0;
        while value < 2_000.0 {
            for scale in [0_i8, 1, 2, 3, 4, 6, 9] {
                for signed in [value, -value] {
                    let text = crate::java_double::java_double_text(signed);
                    assert_eq!(
                        java_double_unscaled(signed, scale),
                        Some(half_up_from_text(&text, scale)),
                        "{text} at scale {scale}"
                    );
                    checked += 1;
                }
            }
            value = value * 1.000_37 + 0.000_013;
        }
        for thousandths in 0..300_000_u32 {
            let value = f64::from(thousandths) / 1000.0;
            for scale in [0_i8, 1, 2, 15] {
                let text = crate::java_double::java_double_text(value);
                assert_eq!(
                    java_double_unscaled(value, scale),
                    Some(half_up_from_text(&text, scale)),
                    "{text} at scale {scale}"
                );
                checked += 1;
            }
        }
        assert!(checked > 1_000_000, "{checked}");
    }

    async fn first(ctx: &SessionContext, sql: &str) -> String {
        values(ctx, sql)
            .await
            .into_iter()
            .next()
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn a_float_cast_try_cast_and_arrow_cast_to_decimal_round_the_text() {
        let ctx = ctx_with(Some(true));
        for (sql, want) in [
            (
                "SELECT CAST(CAST('1.255' AS DOUBLE) AS DECIMAL(10,2))",
                "1.26",
            ),
            (
                "SELECT CAST(CAST('1.005' AS DOUBLE) AS DECIMAL(5,2))",
                "1.01",
            ),
            (
                "SELECT CAST(CAST('-1.255' AS DOUBLE) AS DECIMAL(10,2))",
                "-1.26",
            ),
            (
                "SELECT CAST(CAST('1.255' AS FLOAT) AS DECIMAL(10,2))",
                "1.25",
            ),
            (
                "SELECT TRY_CAST(CAST('1.255' AS DOUBLE) AS DECIMAL(10,2))",
                "1.26",
            ),
            (
                "SELECT arrow_cast(CAST('1.255' AS DOUBLE), 'Decimal128(10, 2)')",
                "1.26",
            ),
            (
                "SELECT TRY_CAST(CAST('NaN' AS DOUBLE) AS DECIMAL(10,2))",
                "",
            ),
            (
                "SELECT TRY_CAST(CAST('1e10' AS DOUBLE) AS DECIMAL(10,2))",
                "",
            ),
        ] {
            assert_eq!(first(&ctx, sql).await, want, "{sql}");
        }
        assert_eq!(
            first(
                &ctx,
                "SELECT CAST(v AS DECIMAL(10,2)) FROM (VALUES (CAST('1.255' AS DOUBLE))) t(v)"
            )
            .await,
            "1.26"
        );
    }

    #[tokio::test]
    async fn a_float_cast_to_decimal_keeps_todays_refusals() {
        for ansi in [Some(true), Some(false), None] {
            let ctx = ctx_with(ansi);
            for (sql, text) in [
                (
                    "SELECT CAST(CAST('NaN' AS DOUBLE) AS DECIMAL(10,2))",
                    "Cannot cast to Decimal128(10, 2). Overflowing on NaN",
                ),
                (
                    "SELECT CAST(CAST('-Infinity' AS DOUBLE) AS DECIMAL(10,2))",
                    "Overflowing on -inf",
                ),
                (
                    "SELECT CAST(CAST('123456789.125' AS DOUBLE) AS DECIMAL(10,2))",
                    "123456789.13 is too large to store in a Decimal128 of precision 10",
                ),
                (
                    "SELECT CAST(CAST('1e300' AS DOUBLE) AS DECIMAL(38,2))",
                    "Overflowing on 1e300",
                ),
            ] {
                let error = ctx
                    .sql(sql)
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(error.contains(text), "{sql}: {error}");
            }
        }
    }
}
