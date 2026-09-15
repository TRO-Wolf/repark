use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array,
    Float64Array, IntervalMonthDayNanoArray, IntervalYearMonthArray, StringArray, StringBuilder,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, IntervalMonthDayNano, IntervalUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

use super::{
    MICROS_PER_SECOND, broadcast_arrays, exec_error, int_field, plan_error, second_out_of_bounds,
};

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::new_from_impl(MakeYmInterval::new())),
        Arc::new(ScalarUDF::new_from_impl(TryMakeInterval::new())),
        Arc::new(ScalarUDF::new_from_impl(IntervalToString::new())),
    ]
}

#[must_use]
pub fn make_ym_interval_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(MakeYmInterval::new()))
}

#[must_use]
pub fn try_make_interval_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(TryMakeInterval::new()))
}

#[must_use]
pub fn interval_string_cast_rule() -> Arc<dyn AnalyzerRule + Send + Sync> {
    Arc::new(IntervalStringCast)
}

fn decimal_micros(value: i128, scale: i8) -> Option<i64> {
    if scale <= 6 {
        let exp = u32::try_from(6 - i32::from(scale)).ok()?;
        let factor = 10_i128.checked_pow(exp)?;
        i64::try_from(value.checked_mul(factor)?).ok()
    } else {
        let exp = u32::try_from(i32::from(scale) - 6).ok()?;
        let divisor = 10_i128.checked_pow(exp)?;
        let half = divisor / 2;
        let rounded = if value >= 0 {
            value.checked_add(half)?.checked_div(divisor)?
        } else {
            value.checked_sub(half)?.checked_div(divisor)?
        };
        i64::try_from(rounded).ok()
    }
}

fn numeric_secs_error(other: &DataType) -> DataFusionError {
    plan_error(format!(
        "temporal constructor expects a numeric seconds argument, got {other}"
    ))
}

pub(crate) fn precast_secs(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            super::precast_int(array)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            super::precast_column(
                array,
                &DataType::Int64,
                numeric_secs_error(array.data_type()),
            )
        }
        DataType::Float32 | DataType::Float64 => super::precast_column(
            array,
            &DataType::Float64,
            exec_error("float cast did not yield Float64".to_string()),
        ),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => super::precast_column(
            array,
            &DataType::Utf8,
            exec_error("string cast did not yield Utf8".to_string()),
        ),
        _ => Ok(Arc::clone(array)),
    }
}

pub(crate) fn secs_to_micros(array: &dyn Array, row: usize) -> Result<Option<i64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let value = int_field(array, row)?.unwrap_or(0);
            value
                .checked_mul(MICROS_PER_SECOND)
                .map(Some)
                .ok_or_else(|| second_out_of_bounds(value))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let casted =
                cast(array, &DataType::Int64).map_err(|_| numeric_secs_error(array.data_type()))?;
            secs_to_micros(casted.as_ref(), row)
        }
        DataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| exec_error("float cast did not yield Float64".to_string()))?;
            float_secs_to_micros(values.value(row))
        }
        DataType::Float32 => {
            let casted = cast(array, &DataType::Float64)
                .map_err(|_| exec_error("float cast did not yield Float64".to_string()))?;
            let values = casted
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| exec_error("float cast did not yield Float64".to_string()))?;
            float_secs_to_micros(values.value(row))
        }
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => decimal_array_secs(array, row),
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| exec_error("string cast did not yield Utf8".to_string()))?;
            text_secs_to_micros(values.value(row))
        }
        DataType::LargeUtf8 | DataType::Utf8View => {
            let casted = cast(array, &DataType::Utf8)
                .map_err(|_| exec_error("string cast did not yield Utf8".to_string()))?;
            let values = casted
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| exec_error("string cast did not yield Utf8".to_string()))?;
            text_secs_to_micros(values.value(row))
        }
        DataType::Null => Ok(None),
        other => Err(plan_error(format!(
            "temporal constructor expects a numeric seconds argument, got {other}"
        ))),
    }
}

pub(crate) fn decimal_array_secs(array: &dyn Array, row: usize) -> Result<Option<i64>> {
    let scale = match array.data_type() {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => *scale,
        other => {
            return Err(plan_error(format!(
                "temporal constructor expects a decimal seconds argument, got {other}"
            )));
        }
    };
    let value = match array.data_type() {
        DataType::Decimal32(_, _) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal32Array>()
                .ok_or_else(|| exec_error("decimal cast failed".to_string()))?;
            i128::from(values.value(row))
        }
        DataType::Decimal64(_, _) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal64Array>()
                .ok_or_else(|| exec_error("decimal cast failed".to_string()))?;
            i128::from(values.value(row))
        }
        DataType::Decimal128(_, _) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| exec_error("decimal cast failed".to_string()))?;
            values.value(row)
        }
        _ => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| exec_error("decimal cast failed".to_string()))?;
            values.value(row).to_i128().unwrap_or(i128::MAX)
        }
    };
    decimal_micros(value, scale).map(Some).ok_or_else(|| {
        plan_error(format!(
            "temporal constructor seconds value overflows microseconds, got {}",
            array.data_type()
        ))
    })
}

fn float_secs_to_micros(seconds: f64) -> Result<Option<i64>> {
    if !seconds.is_finite() {
        return Err(second_out_of_bounds(0));
    }
    #[allow(clippy::cast_possible_truncation)]
    Ok(Some((seconds * 1_000_000.0).round() as i64))
}

fn text_secs_to_micros(text: &str) -> Result<Option<i64>> {
    let seconds: f64 = text.trim().parse().map_err(|_| {
        plan_error(format!(
            "temporal constructor expects a numeric seconds argument, got {text}"
        ))
    })?;
    float_secs_to_micros(seconds)
}

fn interval_overflow() -> DataFusionError {
    exec_error(
        "[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION] Integer overflow while operating with \
         intervals. Try devising appropriate values for the interval parameters. SQLSTATE: 22015"
            .to_string(),
    )
}

#[derive(Debug)]
struct MakeYmInterval {
    signature: Signature,
}

impl MakeYmInterval {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for MakeYmInterval {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for MakeYmInterval {}

impl Hash for MakeYmInterval {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for MakeYmInterval {
    crate::shim_udf_boilerplate!("make_ym_interval");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::YearMonth))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() <= 2 {
            Ok(arg_types.to_vec())
        } else {
            Err(plan_error(format!(
                "'make_ym_interval' expects at most 2 arguments, got {}",
                arg_types.len()
            )))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let arrays = broadcast_arrays(&arrays);
        let worked: Vec<ArrayRef> = arrays
            .iter()
            .map(super::precast_int)
            .collect::<Result<_>>()?;
        let width = worked.first().map_or(args.number_rows, Array::len);
        let mut values: Vec<Option<i32>> = Vec::with_capacity(width);
        for row in 0..width {
            let years = if worked.is_empty() {
                Some(0)
            } else {
                int_field(worked[0].as_ref(), row)?
            };
            let months = if worked.len() < 2 {
                Some(0)
            } else {
                int_field(worked[1].as_ref(), row)?
            };
            match (years, months) {
                (Some(years), Some(months)) => {
                    let total = years
                        .checked_mul(12)
                        .and_then(|scaled| scaled.checked_add(months))
                        .and_then(|total| i32::try_from(total).ok());
                    match total {
                        Some(total) => values.push(Some(total)),
                        None => return Err(interval_overflow()),
                    }
                }
                _ => values.push(None),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(
            IntervalYearMonthArray::from(values),
        )))
    }
}

#[derive(Debug)]
struct TryMakeInterval {
    signature: Signature,
}

impl TryMakeInterval {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for TryMakeInterval {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for TryMakeInterval {}

impl Hash for TryMakeInterval {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for TryMakeInterval {
    crate::shim_udf_boilerplate!("try_make_interval");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            Err(plan_error(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `try_make_interval` requires [1, 2, 3, \
                 4, 5, 6, 7] parameters but the actual number is 0."
                    .to_string(),
            ))
        } else if arg_types.len() <= 7 {
            Ok(arg_types.to_vec())
        } else {
            Err(plan_error(format!(
                "'try_make_interval' expects at most 7 arguments, got {}",
                arg_types.len()
            )))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let arrays = broadcast_arrays(&arrays);
        let worked: Vec<ArrayRef> = arrays
            .iter()
            .enumerate()
            .map(|(index, array)| {
                if index < 6 {
                    super::precast_int(array)
                } else {
                    precast_secs(array)
                }
            })
            .collect::<Result<_>>()?;
        let width = worked.first().map_or(args.number_rows, Array::len);
        let mut values: Vec<Option<IntervalMonthDayNano>> = Vec::with_capacity(width);
        for row in 0..width {
            values.push(calendar_interval_row(&worked, row)?);
        }
        Ok(ColumnarValue::Array(Arc::new(
            IntervalMonthDayNanoArray::from(values),
        )))
    }
}

fn interval_int(arrays: &[ArrayRef], index: usize, row: usize) -> Result<Option<i64>> {
    if index < arrays.len() {
        int_field(arrays[index].as_ref(), row)
    } else {
        Ok(Some(0))
    }
}

fn calendar_interval_row(arrays: &[ArrayRef], row: usize) -> Result<Option<IntervalMonthDayNano>> {
    let years = interval_int(arrays, 0, row)?;
    let months = interval_int(arrays, 1, row)?;
    let weeks = interval_int(arrays, 2, row)?;
    let days = interval_int(arrays, 3, row)?;
    let hours = interval_int(arrays, 4, row)?;
    let mins = interval_int(arrays, 5, row)?;
    let secs = if arrays.len() > 6 {
        secs_to_micros(arrays[6].as_ref(), row)?
    } else {
        Some(0)
    };
    let present = [years, months, weeks, days, hours, mins, secs];
    if present.iter().any(Option::is_none) {
        return Ok(None);
    }
    let (years, months, weeks, days, hours, mins, secs) = (
        years.unwrap_or(0),
        months.unwrap_or(0),
        weeks.unwrap_or(0),
        days.unwrap_or(0),
        hours.unwrap_or(0),
        mins.unwrap_or(0),
        secs.unwrap_or(0),
    );
    let total_months = years
        .checked_mul(12)
        .and_then(|scaled| scaled.checked_add(months))
        .and_then(|total| i32::try_from(total).ok());
    let total_days = weeks
        .checked_mul(7)
        .and_then(|scaled| scaled.checked_add(days))
        .and_then(|total| i32::try_from(total).ok());
    let total_nanos = hours
        .checked_mul(3_600_000_000_000)
        .and_then(|scaled| scaled.checked_add(mins.checked_mul(60_000_000_000)?))
        .and_then(|scaled| scaled.checked_add(secs.checked_mul(super::NANOS_PER_MICRO)?));
    match (total_months, total_days, total_nanos) {
        (Some(months), Some(days), Some(nanoseconds)) => Ok(Some(IntervalMonthDayNano {
            months,
            days,
            nanoseconds,
        })),
        _ => Ok(None),
    }
}

pub(crate) fn format_year_month(months: i32) -> String {
    let total = i64::from(months);
    let sign = if total < 0 { "-" } else { "" };
    let abs = total.abs();
    format!(
        "INTERVAL '{sign}{}-{}' YEAR TO MONTH",
        abs.div_euclid(12),
        abs.rem_euclid(12)
    )
}

pub(crate) fn format_calendar(months: i32, days: i32, nanos: i64) -> String {
    let total_months = i64::from(months);
    let years = total_months / 12;
    let months = total_months % 12;
    let hours = nanos / 3_600_000_000_000;
    let minutes = (nanos % 3_600_000_000_000) / 60_000_000_000;
    let rest = nanos % 60_000_000_000;
    let secs = rest / 1_000_000_000;
    let frac = rest % 1_000_000_000;
    let mut parts: Vec<String> = Vec::new();
    if years != 0 {
        parts.push(format!("{years} years"));
    }
    if months != 0 {
        parts.push(format!("{months} months"));
    }
    if days != 0 {
        parts.push(format!("{days} days"));
    }
    if hours != 0 {
        parts.push(format!("{hours} hours"));
    }
    if minutes != 0 {
        parts.push(format!("{minutes} minutes"));
    }
    push_seconds(&mut parts, secs, frac);
    if parts.is_empty() {
        return "0 seconds".to_string();
    }
    parts.join(" ")
}

fn push_seconds(parts: &mut Vec<String>, secs: i64, frac: i64) {
    if secs == 0 && frac == 0 {
        return;
    }
    if frac == 0 {
        parts.push(format!("{secs} seconds"));
        return;
    }
    let digits = format!("{:09}", frac.abs())
        .trim_end_matches('0')
        .to_string();
    if secs == 0 {
        let sign = if frac < 0 { "-" } else { "" };
        parts.push(format!("{sign}0.{digits} seconds"));
    } else {
        parts.push(format!("{secs}.{digits} seconds"));
    }
}

#[derive(Debug)]
struct IntervalToString {
    signature: Signature,
}

impl IntervalToString {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for IntervalToString {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for IntervalToString {}

impl Hash for IntervalToString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for IntervalToString {
    crate::shim_udf_boilerplate!("__repark_interval_to_string__");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs<'_>,
    ) -> Result<arrow::datatypes::FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(arrow::datatypes::Field::new(
            self.name(),
            DataType::Utf8,
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::MonthDayNano)] => {
                Ok(arg_types.to_vec())
            }
            _ => Err(plan_error(
                "'__repark_interval_to_string__' expects an INTERVAL argument".to_string(),
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = &arrays[0];
        match input.data_type() {
            DataType::Interval(IntervalUnit::YearMonth) => {
                let values = input
                    .as_any()
                    .downcast_ref::<arrow::array::IntervalYearMonthArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("interval cast failed".to_string())
                    })?;
                let mut out = StringBuilder::new();
                for row in 0..values.len() {
                    if values.is_null(row) {
                        out.append_null();
                    } else {
                        out.append_value(format_year_month(values.value(row)));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let values = input
                    .as_any()
                    .downcast_ref::<arrow::array::IntervalMonthDayNanoArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("interval cast failed".to_string())
                    })?;
                let mut out = StringBuilder::new();
                for row in 0..values.len() {
                    if values.is_null(row) {
                        out.append_null();
                    } else {
                        let value = values.value(row);
                        out.append_value(format_calendar(
                            value.months,
                            value.days,
                            value.nanoseconds,
                        ));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }
            other => Err(plan_error(format!(
                "'__repark_interval_to_string__' expects an INTERVAL argument, got {other}"
            ))),
        }
    }
}

pub(crate) fn interval_to_string_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(IntervalToString::new()))
}

#[derive(Debug, Default)]
pub struct IntervalStringCast;

impl AnalyzerRule for IntervalStringCast {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let _ = config;
        plan.transform_up_with_subqueries(rewrite_interval_plan)
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "interval_string_cast"
    }
}

fn rewrite_interval_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved = preserver.save(&expr);
        let rewritten = expr.transform_down(|node| Ok(rewrite_interval_cast(node, &schema)))?;
        Ok(rewritten.update_data(|node| saved.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_interval_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    if !matches!(
        cast.field.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Transformed::no(expr);
    }
    if !matches!(
        cast.expr.get_type(schema),
        Ok(DataType::Interval(
            IntervalUnit::YearMonth | IntervalUnit::MonthDayNano
        ))
    ) {
        return Transformed::no(expr);
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        interval_to_string_udf(),
        vec![cast.expr.as_ref().clone()],
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx_with(ansi: bool, zone: &str) -> SessionContext {
        let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), ansi);
        let config = crate::session_time_zone::with_session_time_zone(config, zone);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        ctx.add_analyzer_rule(Arc::new(IntervalStringCast));
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in batches {
            let column = batch.column(0);
            for row in 0..batch.num_rows() {
                out.push(
                    datafusion::arrow::util::display::array_value_to_string(column, row).unwrap(),
                );
            }
        }
        out
    }

    #[tokio::test]
    async fn ym_overflow_raises_condition() {
        let ctx = ctx_with(true, "UTC");
        let error = ctx
            .sql("SELECT make_ym_interval(178956970, 8) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION]"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn try_interval_zero_args_names_wrong_num_args() {
        let ctx = ctx_with(true, "UTC");
        let error = ctx
            .sql("SELECT try_make_interval() AS v")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn try_interval_overflow_is_null() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(&ctx, "SELECT try_make_interval(2147483647, 12) AS v").await,
            vec![String::new()]
        );
    }

    #[tokio::test]
    async fn try_interval_decimal_secs_scale_to_nanos() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT CAST(try_make_interval(2014, 12, 5, 28, 6, 30, 45.887) AS STRING) AS v"
            )
            .await,
            vec!["2015 years 63 days 6 hours 30 minutes 45.887 seconds".to_string()]
        );
    }

    #[tokio::test]
    async fn interval_cast_signs_each_unit() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT CAST(try_make_interval(1, -2, 0, 3, -4, 5, 6.5) AS STRING) AS v"
            )
            .await,
            vec!["10 months 3 days -3 hours -54 minutes -53.5 seconds".to_string()]
        );
        assert_eq!(
            one(
                &ctx,
                "SELECT CAST(try_make_interval(-1, 2, 0, -3, 4, -5, -6.5) AS STRING) AS v"
            )
            .await,
            vec!["-10 months -3 days 3 hours 54 minutes 53.5 seconds".to_string()]
        );
    }

    #[tokio::test]
    async fn interval_cast_keeps_sub_second_sign() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT CAST(try_make_interval(0, 0, 0, 0, 0, 0, -0.000001) AS STRING) AS s"
            )
            .await,
            vec!["-0.000001 seconds".to_string()]
        );
    }

    #[tokio::test]
    async fn ym_interval_zero_args_answers_single_zero_row() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(&ctx, "SELECT CAST(make_ym_interval() AS STRING) AS v").await,
            vec!["INTERVAL '0-0' YEAR TO MONTH".to_string()]
        );
    }

    #[tokio::test]
    async fn interval_cast_uses_spark_text() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT CAST(make_ym_interval(2014, 12) AS STRING) AS v"
            )
            .await,
            vec!["INTERVAL '2015-0' YEAR TO MONTH".to_string()]
        );
    }
}
