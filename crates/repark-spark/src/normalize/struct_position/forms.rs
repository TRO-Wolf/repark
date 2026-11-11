use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::{Between, Case, ScalarFunction};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{Expr, ExprSchemable, Operator, ScalarUDF};
use repark_functions::cast_map::spark_sql_name;

use super::{
    PLANNED_BY_POSITION, Seat, aligned_operand, binary_op_diff_types, by_position,
    by_position_named, changed, clashes, has_duplicate_names, positional_type, read_type,
    renamed_type, render, struct_pair,
};

pub(super) const NULLIF: &str = "nullif";

struct Partners {
    target: DataType,
    paired: bool,
    duplicate: bool,
}

impl Partners {
    fn new(target: DataType) -> Self {
        let duplicate = has_duplicate_names(&target);
        Self {
            target,
            paired: false,
            duplicate,
        }
    }

    fn meet(&mut self, partner: &DataType, case_sensitive: bool) -> Option<bool> {
        if !struct_pair(&self.target, partner) {
            return Some(false);
        }
        if clashes(&self.target, partner, Seat::Comparison, case_sensitive) {
            return None;
        }
        self.paired = true;
        self.duplicate |= has_duplicate_names(partner);
        Some(true)
    }

    fn anchor(&self, operand: Expr) -> (Expr, DataType) {
        if self.duplicate {
            let positional = positional_type(&self.target);
            (by_position(operand, positional.clone()), positional)
        } else {
            (operand, self.target.clone())
        }
    }
}

fn common_ref_mismatch(
    symbol: &str,
    operand: &Expr,
    target: &DataType,
    current: &DataType,
) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve \"(commonexpressionref() {symbol} \
         {})\" due to data type mismatch: the left and right operands of the binary operator have \
         incompatible types (\"{}\" and \"{}\"). SQLSTATE: 42K09",
        render(operand),
        spark_sql_name(target),
        spark_sql_name(current)
    ))
}

pub(super) fn align_between(
    between: Between,
    schema: &DFSchema,
    case_sensitive: bool,
) -> Result<Transformed<Expr>> {
    let Between {
        expr,
        negated,
        low,
        high,
    } = between;
    let rebuild = |expr: Expr, low: Expr, high: Expr| {
        Expr::Between(Between::new(
            Box::new(expr),
            negated,
            Box::new(low),
            Box::new(high),
        ))
    };
    let Ok(target) = expr.get_type(schema) else {
        return Ok(Transformed::no(rebuild(*expr, *low, *high)));
    };
    let mut partners = Partners::new(target);
    for (bound, symbol) in [(low.as_ref(), ">="), (high.as_ref(), "<=")] {
        let Ok(current) = read_type(bound, schema) else {
            continue;
        };
        if partners.meet(&current, case_sensitive).is_none() {
            return Err(common_ref_mismatch(
                symbol,
                bound,
                &partners.target,
                &current,
            ));
        }
    }
    if !partners.paired {
        return Ok(Transformed::no(rebuild(*expr, *low, *high)));
    }
    let (expr, target) = partners.anchor(*expr);
    let (low, low_moved) = aligned_operand(&target, *low, schema)?;
    let (high, high_moved) = aligned_operand(&target, *high, schema)?;
    Ok(changed(
        rebuild(expr, low, high),
        partners.duplicate || low_moved || high_moved,
    ))
}

pub(super) fn align_case(
    case: Case,
    schema: &DFSchema,
    case_sensitive: bool,
) -> Result<Transformed<Expr>> {
    let Case {
        expr,
        when_then_expr,
        else_expr,
    } = case;
    let Some(operand) = expr else {
        return Ok(Transformed::no(Expr::Case(Case::new(
            None,
            when_then_expr,
            else_expr,
        ))));
    };
    let rebuild = |operand: Expr, when_then_expr| {
        Expr::Case(Case::new(
            Some(Box::new(operand)),
            when_then_expr,
            else_expr.clone(),
        ))
    };
    let Ok(target) = operand.get_type(schema) else {
        return Ok(Transformed::no(rebuild(*operand, when_then_expr)));
    };
    let mut partners = Partners::new(target);
    for (when, _) in &when_then_expr {
        let Ok(current) = read_type(when, schema) else {
            continue;
        };
        if partners.meet(&current, case_sensitive).is_none() {
            return Err(binary_op_diff_types(
                &operand,
                Operator::Eq,
                when,
                &partners.target,
                &current,
            ));
        }
    }
    if !partners.paired {
        return Ok(Transformed::no(rebuild(*operand, when_then_expr)));
    }
    let (operand, target) = partners.anchor(*operand);
    let mut renamed = partners.duplicate;
    let mut pairs = Vec::with_capacity(when_then_expr.len());
    for (when, then) in when_then_expr {
        let (when, moved) = aligned_operand(&target, *when, schema)?;
        renamed |= moved;
        pairs.push((Box::new(when), then));
    }
    Ok(changed(rebuild(operand, pairs), renamed))
}

pub(super) fn align_nullif(
    function: ScalarFunction,
    schema: &DFSchema,
    case_sensitive: bool,
) -> Result<Transformed<Expr>> {
    let ScalarFunction { func, args } = function;
    let rebuild =
        |args: Vec<Expr>| Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(&func), args));
    let [first, second] = args.as_slice() else {
        return Ok(Transformed::no(rebuild(args)));
    };
    let (Ok(target), Ok(current)) = (first.get_type(schema), read_type(second, schema)) else {
        return Ok(Transformed::no(rebuild(args)));
    };
    let mut partners = Partners::new(target);
    match partners.meet(&current, case_sensitive) {
        None => {
            return Err(common_ref_mismatch("=", second, &partners.target, &current));
        }
        Some(false) => return Ok(Transformed::no(rebuild(args))),
        Some(true) => {}
    }
    let [first, second]: [Expr; 2] = args
        .try_into()
        .map_err(|_| DataFusionError::Internal(format!("{NULLIF} takes two arguments")))?;
    let (first, target) = partners.anchor(first);
    let (second, renamed) = aligned_operand(&target, second, schema)?;
    if !partners.duplicate {
        return Ok(changed(rebuild(vec![first, second]), renamed));
    }
    let output = relabelled_output(&partners.target, &target, &second.get_type(schema)?)
        .ok_or_else(|| common_ref_mismatch("=", &second, &partners.target, &current))?;
    Ok(Transformed::yes(by_position(
        rebuild(vec![first, second]),
        output,
    )))
}

fn relabelled_output(original: &DataType, left: &DataType, right: &DataType) -> Option<DataType> {
    let coerced = comparison_coercion(left, right)?;
    Some(renamed_type(original, &coerced).unwrap_or(coerced))
}

pub(super) fn planned_nullif(
    function: ScalarFunction,
    schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    let unchanged = |function: ScalarFunction| Ok(Transformed::no(Expr::ScalarFunction(function)));
    let [first, second] = function.args.as_slice() else {
        return unchanged(function);
    };
    let (Ok(left_type), Ok(right_type)) = (first.get_type(schema), second.get_type(schema)) else {
        return unchanged(function);
    };
    if !struct_pair(&left_type, &right_type)
        || !(has_duplicate_names(&left_type) || has_duplicate_names(&right_type))
    {
        return unchanged(function);
    }
    let left_positional = positional_type(&left_type);
    let right_positional = positional_type(&right_type);
    let Some(output) = relabelled_output(&left_type, &left_positional, &right_positional) else {
        return Err(common_ref_mismatch("=", second, &left_type, &right_type));
    };
    let args = vec![
        by_position_named(PLANNED_BY_POSITION, first.clone(), left_positional),
        by_position_named(PLANNED_BY_POSITION, second.clone(), right_positional),
    ];
    let planned = Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::<ScalarUDF>::clone(&function.func),
        args,
    ));
    Ok(Transformed::yes(by_position(planned, output)))
}
