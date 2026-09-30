use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::Expr;

use crate::cardinality::{const_f64, const_i128};

#[derive(Clone, Copy)]
pub(crate) enum NullifPosition {
    Count,
    Nested,
}

pub(crate) fn nullif_value(
    first: &Expr,
    second: &Expr,
    depth: u32,
    position: NullifPosition,
) -> Option<i128> {
    let left = exact_i128(first, depth)?;
    match exact_i128(second, depth) {
        Some(right) => {
            if left == right {
                None
            } else {
                Some(left)
            }
        }
        None => match position {
            NullifPosition::Count => Some(left),
            NullifPosition::Nested => None,
        },
    }
}

#[allow(clippy::cast_precision_loss, clippy::float_cmp)]
fn exact_i128(expr: &Expr, depth: u32) -> Option<i128> {
    let rewritten = rewrite_decimal_literals(expr);
    let value = const_i128(&rewritten, depth)?;
    if let Some(float) = const_f64(&rewritten, depth)
        && float != value as f64
    {
        return None;
    }
    Some(value)
}

fn rewrite_decimal_literals(expr: &Expr) -> Expr {
    expr.clone()
        .transform(|node| match node {
            Expr::Literal(ScalarValue::Decimal128(unscaled, precision, scale), meta) => {
                Ok(Transformed::yes(Expr::Literal(
                    decimal_replacement(unscaled, precision, scale),
                    meta,
                )))
            }
            other => Ok(Transformed::no(other)),
        })
        .map_or_else(|_| expr.clone(), |done| done.data)
}

fn decimal_replacement(unscaled: Option<i128>, precision: u8, scale: i8) -> ScalarValue {
    let Some(raw) = unscaled else {
        return ScalarValue::Decimal128(None, precision, scale);
    };
    integral_quotient(raw, scale)
        .and_then(|quotient| i64::try_from(quotient).ok())
        .map_or(ScalarValue::Int64(None), |fits| {
            ScalarValue::Int64(Some(fits))
        })
}

fn integral_quotient(unscaled: i128, scale: i8) -> Option<i128> {
    if scale >= 0 {
        let divisor = 10i128.checked_pow(u32::try_from(scale).ok()?)?;
        if unscaled.checked_rem(divisor)? != 0 {
            return None;
        }
        unscaled.checked_div(divisor)
    } else {
        let places = u32::try_from(scale.checked_neg()?).ok()?;
        unscaled.checked_mul(10i128.checked_pow(places)?)
    }
}
