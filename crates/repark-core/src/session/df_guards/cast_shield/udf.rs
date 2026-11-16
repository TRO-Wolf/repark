use std::cmp::Ordering;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::preimage::PreimageResult;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TryCast, Volatility,
};

pub(crate) const CAST_SHIELD_NAME: &str = "__repark_cast_shield__";

static CAST_SHIELD: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(CastShield {
        signature: Signature::any(1, Volatility::Immutable),
    }))
});

#[must_use]
pub fn cast_shield_udf() -> Arc<ScalarUDF> {
    Arc::clone(&CAST_SHIELD)
}

#[must_use]
pub(crate) fn cast_shield_call(cast: Expr) -> Expr {
    cast_shield_udf().call(vec![cast])
}

#[must_use]
pub(crate) fn shielded_cast(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::ScalarFunction(call) if call.name() == CAST_SHIELD_NAME => match call.args.as_slice()
        {
            [cast] => Some(cast),
            _ => None,
        },
        _ => None,
    }
}

#[must_use]
pub(crate) fn cast_parts(expr: &Expr) -> Option<(&Expr, &DataType)> {
    match expr {
        Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field }) => {
            Some((expr.as_ref(), field.data_type()))
        }
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CastShield {
    signature: Signature,
}

impl ScalarUDFImpl for CastShield {
    fn name(&self) -> &str {
        CAST_SHIELD_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [data_type] = arg_types else {
            return plan_err!("{CAST_SHIELD_NAME} takes one argument");
        };
        Ok(data_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return plan_err!("{CAST_SHIELD_NAME} takes one argument");
        };
        Ok(Arc::new(field.as_ref().clone().with_name(self.name())))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut values = args.args;
        let (Some(value), true) = (values.pop(), values.is_empty()) else {
            return exec_err!("{CAST_SHIELD_NAME} takes one argument");
        };
        Ok(value)
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        let [cast] = args else {
            return Ok(PreimageResult::None);
        };
        let Some((operand, target)) = cast_parts(cast) else {
            return Ok(PreimageResult::None);
        };
        let Expr::Literal(constant, _) = lit_expr else {
            return Ok(PreimageResult::None);
        };
        if operand.is_volatile() {
            return Ok(PreimageResult::None);
        }
        let Ok(source) = info.get_data_type(operand) else {
            return Ok(PreimageResult::None);
        };
        let Some((lower, upper)) = cast_preimage(&source, target, constant) else {
            return Ok(PreimageResult::None);
        };
        Ok(PreimageResult::Range {
            expr: operand.clone(),
            interval: Box::new(Interval::try_new(lower, upper)?),
        })
    }
}

fn integer_max(data_type: &DataType) -> Option<i128> {
    match data_type {
        DataType::Int8 => Some(i128::from(i8::MAX)),
        DataType::Int16 => Some(i128::from(i16::MAX)),
        DataType::Int32 => Some(i128::from(i32::MAX)),
        DataType::Int64 => Some(i128::from(i64::MAX)),
        _ => None,
    }
}

fn integer_digits(data_type: &DataType) -> Option<i32> {
    match data_type {
        DataType::Int8 => Some(3),
        DataType::Int16 => Some(5),
        DataType::Int32 => Some(10),
        DataType::Int64 => Some(19),
        _ => None,
    }
}

fn integer_literal(constant: &ScalarValue) -> Option<i128> {
    match constant {
        ScalarValue::Int8(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(i128::from(*value)),
        _ => None,
    }
}

fn pow10(exponent: i32) -> Option<i128> {
    10i128.checked_pow(u32::try_from(exponent).ok()?)
}

fn digits_top(digits: i32) -> Option<i128> {
    if digits <= 0 {
        return Some(0);
    }
    pow10(digits)?.checked_sub(1)
}

fn integer_part_overflows(precision: u8, scale: i8, max: i128) -> bool {
    digits_top(i32::from(precision) - i32::from(scale)).is_none_or(|top| top > max)
}

#[must_use]
pub(crate) fn is_narrowing(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::Decimal128(precision, scale), DataType::Decimal128(to_precision, to_scale)) => {
            to_scale < scale
                || i32::from(*to_precision) - i32::from(*to_scale)
                    < i32::from(*precision) - i32::from(*scale)
        }
        (DataType::Decimal128(precision, scale), _) => integer_max(target)
            .is_some_and(|max| *scale > 0 || integer_part_overflows(*precision, *scale, max)),
        _ => match (integer_max(source), integer_max(target)) {
            (Some(from), Some(to)) => to < from,
            _ => false,
        },
    }
}

#[must_use]
pub(crate) fn is_lossless(source: &DataType, target: &DataType) -> bool {
    if source == target {
        return true;
    }
    match (source, target) {
        (DataType::Decimal128(precision, scale), DataType::Decimal128(to_precision, to_scale)) => {
            to_scale >= scale
                && i32::from(*to_precision) - i32::from(*to_scale)
                    >= i32::from(*precision) - i32::from(*scale)
        }
        (DataType::Decimal128(precision, scale), _) => integer_max(target)
            .is_some_and(|max| *scale <= 0 && !integer_part_overflows(*precision, *scale, max)),
        (_, DataType::Decimal128(to_precision, to_scale)) => {
            integer_digits(source).is_some_and(|digits| {
                *to_scale >= 0 && i32::from(*to_precision) - i32::from(*to_scale) >= digits
            })
        }
        _ => match (integer_max(source), integer_max(target)) {
            (Some(from), Some(to)) => to >= from,
            _ => false,
        },
    }
}

fn can_overflow(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::Decimal128(precision, scale), DataType::Decimal128(to_precision, to_scale)) => {
            let integer = i32::from(*precision) - i32::from(*scale);
            let to_integer = i32::from(*to_precision) - i32::from(*to_scale);
            if to_scale < scale {
                to_integer <= integer
            } else {
                to_integer < integer
            }
        }
        (DataType::Decimal128(precision, scale), _) => {
            integer_max(target).is_none_or(|max| integer_part_overflows(*precision, *scale, max))
        }
        _ => true,
    }
}

fn truncation_range(constant: i128, scale: i8) -> Option<(i128, i128)> {
    let unit = pow10(i32::from(scale))?;
    match constant.cmp(&0) {
        Ordering::Greater => Some((
            constant.checked_mul(unit)?,
            constant.checked_add(1)?.checked_mul(unit)?,
        )),
        Ordering::Less => Some((
            constant.checked_sub(1)?.checked_mul(unit)?.checked_add(1)?,
            constant.checked_mul(unit)?.checked_add(1)?,
        )),
        Ordering::Equal => Some((1 - unit, unit)),
    }
}

fn rounding_range(constant: i128, dropped: i32) -> Option<(i128, i128)> {
    if dropped <= 0 {
        return None;
    }
    let step = pow10(dropped)?;
    let half = pow10(dropped - 1)?.checked_mul(5)?;
    let centre = constant.checked_mul(step)?;
    match constant.cmp(&0) {
        Ordering::Greater => Some((centre.checked_sub(half)?, centre.checked_add(half)?)),
        Ordering::Less => Some((
            centre.checked_sub(half)?.checked_add(1)?,
            centre.checked_add(half)?.checked_add(1)?,
        )),
        Ordering::Equal => Some((1 - half, half)),
    }
}

#[must_use]
pub(crate) fn cast_preimage(
    source: &DataType,
    target: &DataType,
    constant: &ScalarValue,
) -> Option<(ScalarValue, ScalarValue)> {
    let DataType::Decimal128(precision, scale) = *source else {
        return None;
    };
    if !is_narrowing(source, target) || can_overflow(source, target) {
        return None;
    }
    if constant.data_type() != *target {
        return None;
    }
    let (lower, upper) = match (target, constant) {
        (DataType::Decimal128(_, to_scale), ScalarValue::Decimal128(Some(unscaled), _, _)) => {
            rounding_range(*unscaled, i32::from(scale) - i32::from(*to_scale))?
        }
        _ => truncation_range(integer_literal(constant)?, scale)?,
    };
    let top = digits_top(i32::from(precision))?;
    let lower = lower.max(-top);
    if upper > top || lower >= upper {
        return None;
    }
    Some((
        ScalarValue::Decimal128(Some(lower), precision, scale),
        ScalarValue::Decimal128(Some(upper), precision, scale),
    ))
}
