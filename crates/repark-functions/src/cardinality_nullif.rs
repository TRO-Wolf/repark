use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::Expr;

use crate::cardinality::{const_f64, const_i128};

#[derive(Clone, Copy)]
pub(crate) enum NullifPosition {
    Count,
    Nested,
}

pub(crate) fn nvl_fold(args: &[Expr], depth: u32) -> Option<i128> {
    for arg in args {
        if let Some(value) = const_i128(arg, depth) {
            return Some(value);
        }
    }
    None
}

pub(crate) fn nvl2_fold(args: &[Expr], depth: u32) -> Option<i128> {
    let [test, second, third] = args else {
        return None;
    };
    if const_i128(test, depth).is_some() {
        const_i128(second, depth)
    } else if is_exact_null(test, depth) {
        const_i128(third, depth)
    } else {
        None
    }
}

pub(crate) fn nullifzero_nested(arg: &Expr, depth: u32) -> Option<i128> {
    nullif_value(arg, &zero_literal(), depth, NullifPosition::Nested)
}

pub(crate) fn zeroifnull_fold(arg: &Expr, depth: u32) -> Option<i128> {
    nvl_fold(&[arg.clone(), zero_literal()], depth)
}

pub(crate) fn is_exact_null(expr: &Expr, depth: u32) -> bool {
    if depth == 0 {
        return false;
    }
    match expr {
        Expr::Literal(scalar, _) => scalar.is_null(),
        Expr::Alias(alias) => is_exact_null(alias.expr.as_ref(), depth - 1),
        Expr::Cast(cast) => is_exact_null(cast.expr.as_ref(), depth - 1),
        Expr::TryCast(try_cast) => is_exact_null(try_cast.expr.as_ref(), depth - 1),
        Expr::ScalarFunction(function) => exact_null_call(
            function.func.name().to_ascii_lowercase().as_str(),
            &function.args,
            depth - 1,
        ),
        _ => false,
    }
}

fn exact_null_call(name: &str, args: &[Expr], depth: u32) -> bool {
    match name {
        "nullif" | "__repark_nullif_compare" if args.len() == 2 => {
            matches!(
                (exact_i128(&args[0], depth), exact_i128(&args[1], depth)),
                (Some(left), Some(right)) if left == right
            )
        }
        "nullifzero" if args.len() == 1 => {
            matches!(exact_i128(&args[0], depth), Some(0))
        }
        _ => false,
    }
}

fn zero_literal() -> Expr {
    Expr::Literal(ScalarValue::Int32(Some(0)), None)
}

pub(crate) fn nullif_value(
    first: &Expr,
    second: &Expr,
    depth: u32,
    position: NullifPosition,
) -> Option<i128> {
    let left = const_i128(first, depth)?;
    let proven = match (exact_i128(first, depth), exact_i128(second, depth)) {
        (Some(owned), Some(other)) => Some(owned == other),
        _ => None,
    };
    match proven {
        Some(true) => None,
        Some(false) => Some(left),
        None => match position {
            NullifPosition::Count => Some(left),
            NullifPosition::Nested => None,
        },
    }
}

#[allow(clippy::cast_precision_loss, clippy::float_cmp)]
fn exact_i128(expr: &Expr, depth: u32) -> Option<i128> {
    if is_integral_cast(expr) {
        return const_i128(&rewrite_decimal_trunc(expr), depth);
    }
    let rewritten = rewrite_decimal_literals(expr);
    let value = const_i128(&rewritten, depth)?;
    if let Some(float) = const_f64(&rewritten, depth)
        && float != value as f64
    {
        return None;
    }
    Some(value)
}

fn is_integral_cast(expr: &Expr) -> bool {
    let mut current = expr;
    loop {
        match current {
            Expr::Alias(alias) => current = alias.expr.as_ref(),
            Expr::Cast(cast) => return is_integral_target(cast.field.data_type()),
            Expr::TryCast(try_cast) => return is_integral_target(try_cast.field.data_type()),
            _ => return false,
        }
    }
}

fn is_integral_target(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn rewrite_decimal_literals(expr: &Expr) -> Expr {
    rewrite_decimal_with(expr, decimal_replacement)
}

fn rewrite_decimal_trunc(expr: &Expr) -> Expr {
    rewrite_decimal_with(expr, decimal_trunc_replacement)
}

fn rewrite_decimal_with(expr: &Expr, replace: fn(Option<i128>, u8, i8) -> ScalarValue) -> Expr {
    expr.clone()
        .transform(|node| match node {
            Expr::Literal(ScalarValue::Decimal128(unscaled, precision, scale), meta) => Ok(
                Transformed::yes(Expr::Literal(replace(unscaled, precision, scale), meta)),
            ),
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

fn decimal_trunc_replacement(unscaled: Option<i128>, precision: u8, scale: i8) -> ScalarValue {
    let Some(raw) = unscaled else {
        return ScalarValue::Decimal128(None, precision, scale);
    };
    trunc_quotient(raw, scale)
        .and_then(|quotient| i64::try_from(quotient).ok())
        .map_or(ScalarValue::Int64(None), |fits| {
            ScalarValue::Int64(Some(fits))
        })
}

fn trunc_quotient(unscaled: i128, scale: i8) -> Option<i128> {
    if scale >= 0 {
        let divisor = 10i128.checked_pow(u32::try_from(scale).ok()?)?;
        unscaled.checked_div(divisor)
    } else {
        let places = u32::try_from(scale.checked_neg()?).ok()?;
        unscaled.checked_mul(10i128.checked_pow(places)?)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::lit;

    use super::*;
    use crate::spark_nvl_udf::{
        ifnull_expr, nullif_expr, nullifzero_expr, nvl_expr, zeroifnull_expr,
    };

    const DEPTH: u32 = 8;

    fn null_literal() -> Expr {
        Expr::Literal(ScalarValue::Null, None)
    }

    #[test]
    fn nvl_fold_skips_exact_null_first() {
        assert_eq!(
            nvl_fold(&[nullif_expr(lit(1), lit(1)), lit(1000)], DEPTH),
            Some(1000)
        );
        assert_eq!(
            nvl_fold(&[nullif_expr(lit(1), lit(0)), lit(1000)], DEPTH),
            Some(1)
        );
        assert_eq!(nvl_fold(&[null_literal(), lit(1000)], DEPTH), Some(1000));
    }

    #[test]
    fn nvl2_fold_picks_branch_by_test() {
        assert_eq!(nvl2_fold(&[lit(1), lit(1000), lit(0)], DEPTH), Some(1000));
        assert_eq!(
            nvl2_fold(&[null_literal(), lit(1000), lit(5)], DEPTH),
            Some(5)
        );
        assert_eq!(
            nvl2_fold(&[nullif_expr(lit(1), lit(1)), lit(1000), lit(5)], DEPTH),
            Some(5)
        );
        assert_eq!(nvl2_fold(&[lit(1), lit(1000)], DEPTH), None);
    }

    #[test]
    fn nullifzero_nested_folds_exact_value() {
        assert_eq!(nullifzero_nested(&lit(101), DEPTH), Some(101));
        assert_eq!(nullifzero_nested(&lit(0), DEPTH), None);
        assert_eq!(nullifzero_nested(&null_literal(), DEPTH), None);
    }

    #[test]
    fn exact_null_names_null_shapes_only() {
        assert!(is_exact_null(&null_literal(), DEPTH));
        assert!(is_exact_null(&nullif_expr(lit(1), lit(1)), DEPTH));
        assert!(!is_exact_null(&zeroifnull_expr(lit(0)), DEPTH));
        assert!(!is_exact_null(&zeroifnull_expr(null_literal()), DEPTH));
        assert!(is_exact_null(&nullifzero_expr(lit(0)), DEPTH));
        assert!(!is_exact_null(&lit(1), DEPTH));
        assert!(!is_exact_null(&nullif_expr(lit(1), lit(0)), DEPTH));
        assert!(!is_exact_null(&zeroifnull_expr(lit(101)), DEPTH));
        assert!(!is_exact_null(&nvl_expr(lit(1), lit(1000)), DEPTH));
    }

    #[test]
    fn zeroifnull_fold_matches_nvl_with_zero() {
        use datafusion::logical_expr::{col, lit};
        assert_eq!(zeroifnull_fold(&lit(0), DEPTH), Some(0));
        assert_eq!(zeroifnull_fold(&null_literal(), DEPTH), Some(0));
        assert_eq!(zeroifnull_fold(&lit(101), DEPTH), Some(101));
        assert_eq!(zeroifnull_fold(&col("c"), DEPTH), Some(0));
    }

    #[test]
    fn nullif_first_argument_folds_without_exactness() {
        use datafusion::logical_expr::{Cast, TryCast, lit};
        let cast = Expr::Cast(Cast::new(Box::new(lit(150.5f64)), DataType::Int32));
        let attempt = Expr::TryCast(TryCast::new(Box::new(lit(150.5f64)), DataType::Int32));
        for first in [cast, attempt] {
            assert_eq!(
                nullif_value(&first, &lit(0), DEPTH, NullifPosition::Count),
                Some(150)
            );
            assert_eq!(
                nullif_value(&first, &lit(0), DEPTH, NullifPosition::Nested),
                Some(150)
            );
        }
        assert_eq!(
            nullif_value(&lit(101.4f64), &lit(0), DEPTH, NullifPosition::Count),
            Some(101)
        );
        assert_eq!(
            nullif_value(&lit(101.4f64), &lit(0), DEPTH, NullifPosition::Nested),
            None
        );
    }

    #[test]
    fn nullif_integral_cast_seconds_prove_equality() {
        use datafusion::logical_expr::{Cast, TryCast, lit};
        let cast = Expr::Cast(Cast::new(Box::new(lit(101.5f64)), DataType::Int32));
        let attempt = Expr::TryCast(TryCast::new(Box::new(lit(101.5f64)), DataType::Int32));
        for second in [cast, attempt] {
            assert_eq!(
                nullif_value(&lit(101), &second, DEPTH, NullifPosition::Count),
                None
            );
        }
        assert_eq!(
            nullif_value(&lit(101), &lit(101.4f64), DEPTH, NullifPosition::Count),
            Some(101)
        );
    }

    #[test]
    fn nullif_integral_cast_folds_through_decimal_literals() {
        use datafusion::logical_expr::Cast;
        let decimal = Expr::Literal(ScalarValue::Decimal128(Some(1015), 4, 1), None);
        let double = Expr::Cast(Cast::new(Box::new(decimal), DataType::Float64));
        let second = Expr::Cast(Cast::new(Box::new(double), DataType::Int32));
        assert_eq!(
            nullif_value(&lit(101), &second, DEPTH, NullifPosition::Count),
            None
        );
    }

    #[test]
    fn ifnull_and_nvl_share_nvl_fold() {
        let ifnull = ifnull_expr(nullif_expr(lit(1), lit(1)), lit(1000));
        let Expr::ScalarFunction(function) = ifnull else {
            panic!("ifnull_expr must build a scalar call");
        };
        assert_eq!(nvl_fold(&function.args, DEPTH), Some(1000));
    }
}
