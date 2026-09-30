use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::{BinaryExpr, ScalarFunction};

use crate::cardinality::{CONST_FOLD_MAX_DEPTH, const_f64, const_i128, f64_trunc_to_i128};

pub(crate) fn nullif_const_int(first: &Expr, second: &Expr, depth: u32) -> Option<i128> {
    let bound = nullif_bound_value(first, depth).or_else(|| decimal_core_trunc(first))?;
    (!nullif_proven_equal(first, second, depth)).then_some(bound)
}

fn nullif_bound_value(expr: &Expr, depth: u32) -> Option<i128> {
    const_i128(&prune_string_literals(expr), depth)
}

fn nullif_int_value(expr: &Expr, depth: u32) -> Option<i128> {
    const_i128(
        &unwrap_decimal_nullable(&prune_string_literals(expr)),
        depth,
    )
}

fn prune_string_literals(expr: &Expr) -> Expr {
    expr.clone()
        .transform(|node| match node {
            Expr::Literal(
                ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) | ScalarValue::Utf8View(_),
                _,
            ) => Ok(Transformed::yes(Expr::Literal(ScalarValue::Null, None))),
            other => Ok(Transformed::no(other)),
        })
        .map_or_else(|_| expr.clone(), |done| done.data)
}

fn unwrap_decimal_nullable(expr: &Expr) -> Expr {
    expr.clone()
        .transform(|node| match node {
            Expr::ScalarFunction(function)
                if function.func.name() == crate::decimal_cast::DECIMAL_CAST_NULLABLE_NAME
                    && function.args.len() == 1 =>
            {
                Ok(Transformed::yes(function.args[0].clone()))
            }
            other => Ok(Transformed::no(other)),
        })
        .map_or_else(|_| expr.clone(), |done| done.data)
}

fn is_float_or_decimal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn strip_numeric_core(expr: &Expr) -> Option<(ScalarValue, bool)> {
    let mut current = expr;
    let mut negate = false;
    loop {
        match current {
            Expr::Cast(cast) if is_float_or_decimal(cast.field.data_type()) => {
                current = cast.expr.as_ref();
            }
            Expr::TryCast(cast) if is_float_or_decimal(cast.field.data_type()) => {
                current = cast.expr.as_ref();
            }
            Expr::Alias(alias) => current = alias.expr.as_ref(),
            Expr::Negative(inner) => {
                negate = !negate;
                current = inner.as_ref();
            }
            Expr::Literal(scalar, _) => return Some((scalar.clone(), negate)),
            _ => return None,
        }
    }
}

fn decimal_scaled_to_i128(unscaled: i128, scale: i8, exact: bool) -> Option<i128> {
    if scale >= 0 {
        let divisor = 10i128.checked_pow(u32::try_from(scale).ok()?)?;
        if exact && unscaled.checked_rem(divisor)? != 0 {
            return None;
        }
        unscaled.checked_div(divisor)
    } else {
        let places = u32::try_from(scale.checked_neg()?).ok()?;
        unscaled.checked_mul(10i128.checked_pow(places)?)
    }
}

fn decimal_core_trunc(expr: &Expr) -> Option<i128> {
    let (scalar, negate) = strip_numeric_core(expr)?;
    let ScalarValue::Decimal128(Some(unscaled), _, scale) = scalar else {
        return None;
    };
    let value = decimal_scaled_to_i128(unscaled, scale, false)?;
    if negate {
        value.checked_neg()
    } else {
        Some(value)
    }
}

#[allow(
    clippy::float_cmp,
    reason = "Spark nullif compares DOUBLE images with == plus NaN equality"
)]
fn nullif_proven_equal(first: &Expr, second: &Expr, depth: u32) -> bool {
    if let (Some(left), Some(right)) = (
        nullif_int_value(first, depth),
        nullif_int_value(second, depth),
    ) {
        if left == right {
            return true;
        }
    }
    if nullif_decimal_core_equal(first, second, depth) {
        return true;
    }
    if !nullif_float_involved(first) && !nullif_float_involved(second) {
        return false;
    }
    let (Some(left), Some(right)) = (
        const_f64(first, CONST_FOLD_MAX_DEPTH),
        const_f64(second, CONST_FOLD_MAX_DEPTH),
    ) else {
        return false;
    };
    left == right || (left.is_nan() && right.is_nan())
}

fn nullif_decimal_core_equal(first: &Expr, second: &Expr, depth: u32) -> bool {
    let (Some(want), Some((scalar, negate))) =
        (nullif_int_value(first, depth), strip_numeric_core(second))
    else {
        return false;
    };
    let ScalarValue::Decimal128(Some(unscaled), _, scale) = scalar else {
        return false;
    };
    let Some(got) = decimal_scaled_to_i128(unscaled, scale, true) else {
        return false;
    };
    let got = if negate { got.checked_neg() } else { Some(got) };
    got == Some(want)
}

fn nullif_float_involved(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Float32(_) | ScalarValue::Float64(_), _) => true,
        Expr::Cast(cast) => {
            matches!(
                cast.field.data_type(),
                DataType::Float32 | DataType::Float64
            ) || nullif_float_involved(cast.expr.as_ref())
        }
        Expr::TryCast(try_cast) => {
            matches!(
                try_cast.field.data_type(),
                DataType::Float32 | DataType::Float64
            ) || nullif_float_involved(try_cast.expr.as_ref())
        }
        Expr::Negative(inner) => nullif_float_involved(inner.as_ref()),
        Expr::Alias(alias) => nullif_float_involved(alias.expr.as_ref()),
        Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            nullif_float_involved(left.as_ref()) || nullif_float_involved(right.as_ref())
        }
        _ => false,
    }
}
