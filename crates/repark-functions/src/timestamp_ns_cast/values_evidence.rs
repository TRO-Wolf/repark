use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::{Expr, ExprSchemable, Operator};

use super::strip_values_wrappers;

const NAMED_WALLS: [&str; 3] = ["to_timestamp_ntz", "make_timestamp_ntz", "localtimestamp"];
const NAMED_INSTANTS: [&str; 2] = ["from_utc_timestamp", "to_utc_timestamp"];

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Evidence {
    Wall,
    Timestamp,
    NamedInstant,
    None,
}

pub(super) fn values_cell_evidence(pre: &Expr) -> Evidence {
    let core = strip_values_wrappers(pre);
    let evidence = core_evidence(core);
    if evidence != Evidence::None {
        return evidence;
    }
    match core {
        Expr::Cast(cast) if matches!(cast.field.data_type(), DataType::Timestamp(_, None)) => {
            core_evidence(strip_values_wrappers(&cast.expr))
        }
        _ => Evidence::None,
    }
}

fn core_evidence(expr: &Expr) -> Evidence {
    let (base, shifted) = peel_interval_arithmetic(expr);
    match base {
        Expr::ScalarFunction(function) => {
            let name = function.func.name();
            if matches!(
                name,
                crate::timestamp_ntz_cast::TIMESTAMP_NTZ_LITERAL_NAME
                    | crate::timestamp_ntz_cast::TIMESTAMP_NTZ_CAST_NAME
                    | crate::timestamp_ntz_cast::TRY_TIMESTAMP_NTZ_CAST_NAME
            ) {
                Evidence::Wall
            } else if shifted {
                Evidence::None
            } else if NAMED_WALLS.contains(&name)
                && matches!(
                    base.get_type(&DFSchema::empty()),
                    Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
                )
            {
                Evidence::Wall
            } else if NAMED_INSTANTS.contains(&name) {
                Evidence::NamedInstant
            } else {
                Evidence::None
            }
        }
        Expr::Literal(ScalarValue::TimestampMicrosecond(_, None), _) => Evidence::Wall,
        Expr::TryCast(cast) if is_naive_ns(cast.field.data_type()) && !shifted => {
            Evidence::Timestamp
        }
        Expr::Cast(cast) if is_naive_ns(cast.field.data_type()) => {
            match strip_values_wrappers(&cast.expr) {
                Expr::Literal(
                    ScalarValue::Utf8(Some(_))
                    | ScalarValue::LargeUtf8(Some(_))
                    | ScalarValue::Utf8View(Some(_)),
                    _,
                ) => Evidence::Timestamp,
                Expr::Cast(inner) if is_naive_ns(inner.field.data_type()) => Evidence::Timestamp,
                _ if shifted => Evidence::Timestamp,
                _ => Evidence::None,
            }
        }
        _ => Evidence::None,
    }
}

fn peel_interval_arithmetic(mut expr: &Expr) -> (&Expr, bool) {
    let mut shifted = false;
    loop {
        expr = strip_values_wrappers(expr);
        let Expr::BinaryExpr(binary) = expr else {
            return (expr, shifted);
        };
        if matches!(binary.op, Operator::Plus | Operator::Minus)
            && is_interval_literal(&binary.right)
        {
            expr = binary.left.as_ref();
        } else if binary.op == Operator::Plus && is_interval_literal(&binary.left) {
            expr = binary.right.as_ref();
        } else {
            return (expr, shifted);
        }
        shifted = true;
    }
}

fn is_naive_ns(data_type: &DataType) -> bool {
    data_type == &DataType::Timestamp(TimeUnit::Nanosecond, None)
}

fn is_interval_literal(expr: &Expr) -> bool {
    matches!(
        strip_values_wrappers(expr),
        Expr::Literal(
            ScalarValue::IntervalMonthDayNano(_)
                | ScalarValue::IntervalDayTime(_)
                | ScalarValue::IntervalYearMonth(_),
            _,
        )
    )
}
