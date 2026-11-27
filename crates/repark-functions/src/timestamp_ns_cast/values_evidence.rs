use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::{Expr, ExprSchemable, Operator};

use super::strip_values_wrappers;

const NAMED_WALLS: [&str; 3] = ["to_timestamp_ntz", "make_timestamp_ntz", "localtimestamp"];
const NAMED_INSTANTS: [&str; 3] = ["from_utc_timestamp", "to_utc_timestamp", "to_timestamp_ltz"];
const NULL_FALLBACKS: [&str; 3] = ["coalesce", "nvl", "ifnull"];

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Evidence {
    Wall,
    Timestamp,
    NamedInstant,
    None,
}

pub(super) fn values_cell_evidence(pre: &Expr) -> Evidence {
    cell_evidence(pre, true)
}

pub(super) fn widen_columns(schema: &DFSchema, ntz_default: bool) -> Vec<(usize, bool)> {
    let wall = DataType::Timestamp(TimeUnit::Microsecond, None);
    let wall_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let instant = crate::instant_ts::ltz_timestamp_type();
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(column, field)| {
            if field.data_type() == &wall || (ntz_default && field.data_type() == &instant) {
                Some((column, false))
            } else if field.data_type() == &wall_ns {
                Some((column, true))
            } else {
                None
            }
        })
        .collect()
}

pub(super) fn instant_cell_source(pre: &Expr, ntz_default: bool) -> &Expr {
    if !ntz_default || values_cell_evidence(pre) != Evidence::NamedInstant {
        return pre;
    }
    match strip_values_wrappers(pre) {
        Expr::Cast(cast)
            if cast.field.data_type() == &DataType::Timestamp(TimeUnit::Microsecond, None) =>
        {
            &cast.expr
        }
        _ => pre,
    }
}

fn cell_evidence(pre: &Expr, branches: bool) -> Evidence {
    let core = strip_values_wrappers(pre);
    let evidence = core_evidence(core, branches);
    if evidence != Evidence::None {
        return evidence;
    }
    match core {
        Expr::Cast(cast) if matches!(cast.field.data_type(), DataType::Timestamp(_, None)) => {
            core_evidence(strip_values_wrappers(&cast.expr), branches)
        }
        _ => Evidence::None,
    }
}

fn core_evidence(expr: &Expr, branches: bool) -> Evidence {
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
            } else if NAMED_INSTANTS.contains(&name) {
                Evidence::NamedInstant
            } else if shifted {
                Evidence::None
            } else if NAMED_WALLS.contains(&name)
                && matches!(
                    base.get_type(&DFSchema::empty()),
                    Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
                )
            {
                Evidence::Wall
            } else if branches && NULL_FALLBACKS.contains(&name) {
                instant_branch_evidence(&function.args)
            } else if branches && name == "if" && function.args.len() == 3 {
                instant_branch_evidence(&function.args[1..])
            } else {
                Evidence::None
            }
        }
        Expr::Case(case) if branches && !shifted => instant_branch_evidence(
            case.when_then_expr
                .iter()
                .map(|(_, then)| then.as_ref())
                .chain(case.else_expr.as_deref()),
        ),
        Expr::Literal(ScalarValue::TimestampMicrosecond(_, None), _) => Evidence::Wall,
        Expr::TryCast(cast) if is_written_timestamp(cast.field.data_type()) && !shifted => {
            Evidence::Timestamp
        }
        Expr::Cast(cast) if is_written_timestamp(cast.field.data_type()) => {
            match strip_values_wrappers(&cast.expr) {
                Expr::Literal(
                    ScalarValue::Utf8(Some(_))
                    | ScalarValue::LargeUtf8(Some(_))
                    | ScalarValue::Utf8View(Some(_)),
                    _,
                ) => Evidence::Timestamp,
                Expr::Cast(inner) if is_written_timestamp(inner.field.data_type()) => {
                    Evidence::Timestamp
                }
                _ if shifted => Evidence::Timestamp,
                _ => Evidence::None,
            }
        }
        _ => Evidence::None,
    }
}

fn instant_branch_evidence<'a>(branches: impl IntoIterator<Item = &'a Expr>) -> Evidence {
    let mut found = Evidence::None;
    for branch in branches {
        if is_null_branch(branch) {
            continue;
        }
        let evidence = cell_evidence(branch, false);
        if !matches!(evidence, Evidence::Timestamp | Evidence::NamedInstant)
            || (found != Evidence::None && found != evidence)
        {
            return Evidence::None;
        }
        found = evidence;
    }
    found
}

fn is_null_branch(mut expr: &Expr) -> bool {
    loop {
        match strip_values_wrappers(expr) {
            Expr::Cast(cast) => expr = cast.expr.as_ref(),
            Expr::TryCast(cast) => expr = cast.expr.as_ref(),
            Expr::Literal(scalar, _) => return scalar.is_null(),
            _ => return false,
        }
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

fn is_written_timestamp(data_type: &DataType) -> bool {
    data_type == &DataType::Timestamp(TimeUnit::Nanosecond, None)
        || crate::instant_ts::is_ltz_timestamp(data_type)
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
