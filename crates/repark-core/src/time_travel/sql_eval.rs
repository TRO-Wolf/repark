use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr as PlanExpr, LogicalPlan, Volatility};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr, Query, SelectItem, SetExpr, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;

use crate::SessionTimeZone;

use super::sql_ast::check_timestamp_expr;
use super::sql_text::zoned_wall_to_ms;
use super::{invalid_timestamp_input, nondeterministic_timestamp_expr, timestamp_column_refusal};

fn map_eval_error(error: &DataFusionError, display: &str) -> DataFusionError {
    if error.to_string().contains("No field named") {
        return timestamp_column_refusal();
    }
    invalid_timestamp_input(display)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn cast_string_to_timestamp_ms(ctx: &SessionContext, text: &str) -> Result<i64> {
    let escaped = text.replace('\'', "''");
    let frame = ctx
        .sql(&format!("SELECT CAST('{escaped}' AS TIMESTAMP)"))
        .await
        .map_err(|_| invalid_timestamp_input(text))?;
    let batches = frame
        .collect()
        .await
        .map_err(|_| invalid_timestamp_input(text))?;
    let Some(batch) = batches.first() else {
        return Err(invalid_timestamp_input(text));
    };
    if batch.num_rows() != 1 || batch.num_columns() != 1 {
        return Err(invalid_timestamp_input(text));
    }
    let scalar = ScalarValue::try_from_array(batch.column(0), 0)
        .map_err(|_| invalid_timestamp_input(text))?;
    timestamp_scalar_to_ms(&scalar, text)
}

async fn scalar_to_ms(
    ctx: &SessionContext,
    scalar: &ScalarValue,
    zone: &SessionTimeZone,
    display: &str,
) -> Result<i64> {
    match scalar {
        ScalarValue::TimestampSecond(_, _)
        | ScalarValue::TimestampMillisecond(_, _)
        | ScalarValue::TimestampMicrosecond(_, _)
        | ScalarValue::TimestampNanosecond(_, _) => timestamp_scalar_to_ms(scalar, display),
        ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_) => {
            let Some(seconds) = scalar_to_i64(scalar) else {
                return Err(invalid_timestamp_input(display));
            };
            seconds
                .checked_mul(1000)
                .ok_or_else(|| invalid_timestamp_input(display))
        }
        ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_) => {
            let seconds = scalar_to_u64(scalar).ok_or_else(|| invalid_timestamp_input(display))?;
            i64::try_from(seconds)
                .ok()
                .and_then(|seconds| seconds.checked_mul(1000))
                .ok_or_else(|| invalid_timestamp_input(display))
        }
        ScalarValue::Float16(_) | ScalarValue::Float32(_) | ScalarValue::Float64(_) => {
            let seconds = scalar_to_f64(scalar).ok_or_else(|| invalid_timestamp_input(display))?;
            float_seconds_to_ms(seconds, display)
        }
        ScalarValue::Decimal128(value, _, scale) => {
            let digits = value.ok_or_else(|| invalid_timestamp_input(display))?;
            decimal_to_ms(digits, *scale, display)
        }
        ScalarValue::Decimal256(value, _, scale) => {
            let digits = value
                .and_then(decimal_wide_to_i128)
                .ok_or_else(|| invalid_timestamp_input(display))?;
            decimal_to_ms(digits, *scale, display)
        }
        ScalarValue::Date32(days) => {
            let days = days.ok_or_else(|| invalid_timestamp_input(display))?;
            date_to_ms(i64::from(days), zone, display)
        }
        ScalarValue::Date64(millis) => {
            let millis = millis.ok_or_else(|| invalid_timestamp_input(display))?;
            date_to_ms(millis.div_euclid(86_400_000), zone, display)
        }
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            let text = value
                .as_ref()
                .ok_or_else(|| invalid_timestamp_input(display))?;
            cast_string_to_timestamp_ms(ctx, text).await
        }
        _ => Err(invalid_timestamp_input(display)),
    }
}

fn timestamp_scalar_to_ms(scalar: &ScalarValue, display: &str) -> Result<i64> {
    let millis = match scalar {
        ScalarValue::TimestampSecond(value, _) => {
            value.and_then(|seconds| seconds.checked_mul(1000))
        }
        ScalarValue::TimestampMillisecond(value, _) => *value,
        ScalarValue::TimestampMicrosecond(value, _) => value.map(|micros| micros.div_euclid(1000)),
        ScalarValue::TimestampNanosecond(value, _) => {
            value.map(|nanos| nanos.div_euclid(1_000_000))
        }
        _ => None,
    };
    millis.ok_or_else(|| invalid_timestamp_input(display))
}

fn scalar_to_i64(scalar: &ScalarValue) -> Option<i64> {
    match scalar {
        ScalarValue::Int8(value) => value.map(i64::from),
        ScalarValue::Int16(value) => value.map(i64::from),
        ScalarValue::Int32(value) => value.map(i64::from),
        ScalarValue::Int64(value) => *value,
        _ => None,
    }
}

fn scalar_to_u64(scalar: &ScalarValue) -> Option<u64> {
    match scalar {
        ScalarValue::UInt8(value) => value.map(u64::from),
        ScalarValue::UInt16(value) => value.map(u64::from),
        ScalarValue::UInt32(value) => value.map(u64::from),
        ScalarValue::UInt64(value) => *value,
        _ => None,
    }
}

fn decimal_wide_to_i128(wide: datafusion::arrow::datatypes::i256) -> Option<i128> {
    let bytes = wide.to_be_bytes();
    let (high, low) = bytes.split_at(16);
    if high.iter().all(|byte| *byte == 0) || high.iter().all(|byte| *byte == 0xFF) {
        let mut fixed = [0_u8; 16];
        fixed.copy_from_slice(low);
        Some(i128::from_be_bytes(fixed))
    } else {
        None
    }
}

fn scalar_to_f64(scalar: &ScalarValue) -> Option<f64> {
    match scalar {
        ScalarValue::Float16(Some(value)) => Some(value.to_f64()),
        ScalarValue::Float32(value) => value.map(f64::from),
        ScalarValue::Float64(value) => *value,
        _ => None,
    }
}

#[allow(clippy::cast_precision_loss)]
#[allow(clippy::cast_possible_truncation)]
fn float_seconds_to_ms(seconds: f64, display: &str) -> Result<i64> {
    if !seconds.is_finite() {
        return Err(invalid_timestamp_input(display));
    }
    let millis = seconds * 1000.0;
    if millis < i64::MIN as f64 || millis > i64::MAX as f64 {
        return Err(invalid_timestamp_input(display));
    }
    Ok(millis.floor() as i64)
}

fn decimal_to_ms(digits: i128, scale: i8, display: &str) -> Result<i64> {
    let scaled = if scale >= 0 {
        let factor = 10_i128.checked_pow(u32::from(scale.cast_unsigned()));
        digits
            .checked_mul(1000)
            .and_then(|wide| factor.and_then(|factor| wide.checked_div(factor)))
    } else {
        let factor = 10_i128.checked_pow(u32::from(scale.unsigned_abs()));
        digits
            .checked_mul(1000)
            .and_then(|wide| factor.and_then(|factor| wide.checked_mul(factor)))
    };
    scaled
        .and_then(|millis| i64::try_from(millis).ok())
        .ok_or_else(|| invalid_timestamp_input(display))
}

fn date_to_ms(days: i64, zone: &SessionTimeZone, display: &str) -> Result<i64> {
    let absolute = days
        .checked_add(719_163)
        .and_then(|days| i32::try_from(days).ok())
        .ok_or_else(|| invalid_timestamp_input(display))?;
    let date = chrono::NaiveDate::from_num_days_from_ce_opt(absolute)
        .ok_or_else(|| invalid_timestamp_input(display))?;
    date.and_hms_opt(0, 0, 0)
        .and_then(|midnight| zoned_wall_to_ms(midnight, zone))
        .ok_or_else(|| invalid_timestamp_input(display))
}

#[allow(clippy::missing_errors_doc)]
pub async fn evaluate_sql_timestamp_asof(
    ctx: &SessionContext,
    value_tokens: &[Token],
    zone: &SessionTimeZone,
) -> Result<i64> {
    let text: String = value_tokens.iter().map(ToString::to_string).collect();
    let display = text.trim();
    if display.is_empty() {
        return Err(invalid_timestamp_input(display));
    }
    let mut statements = Parser::parse_sql(&GenericDialect, &format!("SELECT {display}"))
        .map_err(|error| timestamp_parse_error(error.to_string()))?;
    if statements.len() != 1 {
        return Err(timestamp_parse_error(format!("SELECT {display}")));
    }
    let statement = statements.remove(0);
    let Statement::Query(query) = statement else {
        return Err(timestamp_parse_error(format!("SELECT {display}")));
    };
    let mut query = *query;
    let Some(expr) = query_expr_mut(&mut query) else {
        return Err(timestamp_parse_error(format!("SELECT {display}")));
    };
    check_timestamp_expr(expr, false)?;
    let frame = ctx
        .sql(&format!("SELECT {expr}"))
        .await
        .map_err(|error| map_eval_error(&error, display))?;
    if plan_uses_volatile_scalar(frame.logical_plan()) {
        return Err(nondeterministic_timestamp_expr(display));
    }
    let batches = frame
        .collect()
        .await
        .map_err(|error| map_eval_error(&error, display))?;
    let Some(batch) = batches.first() else {
        return Err(invalid_timestamp_input(display));
    };
    if batch.num_rows() != 1 || batch.num_columns() != 1 {
        return Err(invalid_timestamp_input(display));
    }
    let scalar = ScalarValue::try_from_array(batch.column(0), 0)
        .map_err(|error| map_eval_error(&error, display))?;
    scalar_to_ms(ctx, &scalar, zone, display).await
}

fn plan_uses_volatile_scalar(plan: &LogicalPlan) -> bool {
    let mut plans = vec![plan];
    while let Some(current) = plans.pop() {
        for expr in current.expressions() {
            if expr_uses_volatile_scalar(&expr) {
                return true;
            }
        }
        plans.extend(current.inputs());
    }
    false
}

fn expr_uses_volatile_scalar(expr: &PlanExpr) -> bool {
    let mut found = false;
    let _applied = expr.apply(|node| {
        let volatile = match node {
            PlanExpr::ScalarFunction(function) => {
                function.func.signature().volatility == Volatility::Volatile
            }
            PlanExpr::ScalarSubquery(subquery) => plan_uses_volatile_scalar(&subquery.subquery),
            PlanExpr::Exists(exists) => plan_uses_volatile_scalar(&exists.subquery.subquery),
            PlanExpr::InSubquery(in_subquery) => {
                plan_uses_volatile_scalar(&in_subquery.subquery.subquery)
            }
            _ => false,
        };
        if volatile {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn timestamp_parse_error(message: String) -> DataFusionError {
    DataFusionError::SQL(
        Box::new(datafusion::sql::sqlparser::parser::ParserError::ParserError(message)),
        None,
    )
}

fn query_expr_mut(query: &mut Query) -> Option<&mut Expr> {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };
    if select.projection.len() != 1 {
        return None;
    }
    match &mut select.projection[0] {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => Some(expr),
        _ => None,
    }
}
