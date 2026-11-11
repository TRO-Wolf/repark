use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, NullEquality, Result};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Expr, ExprSchemable, Join, JoinConstraint, JoinType, Operator, ScalarUDF,
};
use repark_core::struct_walk::{holds_struct, list_element, peel_encoding};
use repark_functions::cast_map::spark_sql_name;

use super::udf::StructByPosition;
use super::{
    Pairing, Seat, aligned_operand, binary_op_diff_types, by_position, clashes,
    has_duplicate_names, names_match, pairing, positional_type, read_type, render, struct_pair,
};

pub(super) fn keyed(join: &Join) -> bool {
    join.null_equality == NullEquality::NullEqualsNothing
        && join.on.iter().any(|(first, second)| {
            resolved(first, second, join.left.schema(), join.right.schema()).is_some_and(
                |(_, (target, current))| {
                    [target, current]
                        .iter()
                        .any(|key| holds_struct(key) || holds_map(key))
                },
            )
        })
}

pub(super) fn align_join(mut join: Join, case_sensitive: bool) -> Result<(Join, bool)> {
    if join.null_equality != NullEquality::NullEqualsNothing {
        return Ok((join, false));
    }
    let left = Arc::clone(join.left.schema());
    let right = Arc::clone(join.right.schema());
    let full = join.join_type == JoinType::Full;
    let mut renamed = false;
    let mut on = Vec::with_capacity(join.on.len());
    for (first, second) in std::mem::take(&mut join.on) {
        let (pair, moved) = align_key(first, second, &left, &right, full, case_sensitive)?;
        renamed |= moved;
        on.push(pair);
    }
    join.on = on;
    if renamed {
        join.join_constraint = JoinConstraint::On;
    }
    Ok((join, renamed))
}

fn key_types(
    left_key: &Expr,
    right_key: &Expr,
    left: &DFSchema,
    right: &DFSchema,
) -> Option<(DataType, DataType)> {
    Some((
        left_key.get_type(left).ok()?,
        read_type(right_key, right).ok()?,
    ))
}

fn resolved(
    first: &Expr,
    second: &Expr,
    left: &DFSchema,
    right: &DFSchema,
) -> Option<(bool, (DataType, DataType))> {
    key_types(first, second, left, right)
        .map(|types| (false, types))
        .or_else(|| key_types(second, first, left, right).map(|types| (true, types)))
}

fn align_key(
    first: Expr,
    second: Expr,
    left: &DFSchema,
    right: &DFSchema,
    full: bool,
    case_sensitive: bool,
) -> Result<((Expr, Expr), bool)> {
    let Some((swapped, (target, current))) = resolved(&first, &second, left, right) else {
        return Ok(((first, second), false));
    };
    let (left_key, right_key) = if swapped {
        (second, first)
    } else {
        (first, second)
    };
    let key = Key {
        target: &target,
        current: &current,
        right,
        full,
        case_sensitive,
    };
    let (left_key, right_key, moved) = key.align(left_key, right_key)?;
    let pair = if swapped {
        (right_key, left_key)
    } else {
        (left_key, right_key)
    };
    Ok((pair, moved))
}

struct Key<'a> {
    target: &'a DataType,
    current: &'a DataType,
    right: &'a DFSchema,
    full: bool,
    case_sensitive: bool,
}

impl Key<'_> {
    fn align(&self, left_key: Expr, right_key: Expr) -> Result<(Expr, Expr, bool)> {
        let (target, current) = (self.target, self.current);
        if let Some(map_type) = [target, current].into_iter().find(|key| holds_map(key)) {
            if pairing(target, current, Seat::Comparison, self.case_sensitive) == Pairing::Clash {
                return Err(binary_op_diff_types(
                    &left_key,
                    Operator::Eq,
                    &right_key,
                    target,
                    current,
                ));
            }
            return Err(invalid_ordering(
                &render(&left_key),
                &render(&right_key),
                map_type,
            ));
        }
        if !struct_pair(target, current) {
            return Ok((left_key, right_key, false));
        }
        if clashes(target, current, Seat::Comparison, self.case_sensitive) {
            return Err(binary_op_diff_types(
                &left_key,
                Operator::Eq,
                &right_key,
                target,
                current,
            ));
        }
        if self.full && !names_match(target, current, self.case_sensitive) {
            return Err(coalesce_diff_types(&left_key, &right_key, target, current));
        }
        if has_duplicate_names(target) || has_duplicate_names(current) {
            return Ok((
                by_position(left_key, positional_type(target)),
                by_position(right_key, positional_type(current)),
                true,
            ));
        }
        let (right_key, moved) = aligned_operand(target, right_key, self.right)?;
        Ok((left_key, right_key, moved))
    }
}

pub(super) fn holds_map(data_type: &DataType) -> bool {
    match peel_encoding(data_type) {
        DataType::Map(_, _) => true,
        DataType::Struct(fields) => fields.iter().any(|field| holds_map(field.data_type())),
        peeled => list_element(peeled).is_some_and(|element| holds_map(element.data_type())),
    }
}

pub(super) fn invalid_ordering(left: &str, right: &str, map_type: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve \"({left} = {right})\" due to \
         data type mismatch: The `=` does not support ordering on type \"{}\". SQLSTATE: 42K09",
        spark_sql_name(map_type)
    ))
}

fn coalesce_diff_types(
    left_key: &Expr,
    right_key: &Expr,
    target: &DataType,
    current: &DataType,
) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve \"coalesce({}, {})\" due to data type \
         mismatch: Input to `coalesce` should all be the same type, but it's (\"{}\" or \"{}\"). \
         SQLSTATE: 42K09",
        render(left_key),
        render(right_key),
        spark_sql_name(target),
        spark_sql_name(current)
    ))
}

pub(super) fn opaque(expr: Expr) -> Result<Transformed<Expr>> {
    expr.transform_up(|leaf| {
        let Expr::ScalarFunction(function) = leaf else {
            return Ok(Transformed::no(leaf));
        };
        let Some((name, target, true)) = function
            .func
            .inner()
            .downcast_ref::<StructByPosition>()
            .map(StructByPosition::parts)
        else {
            return Ok(Transformed::no(Expr::ScalarFunction(function)));
        };
        let relabel = StructByPosition::new(name, target.clone()).opaque();
        Ok(Transformed::yes(Expr::ScalarFunction(
            ScalarFunction::new_udf(Arc::new(ScalarUDF::new_from_impl(relabel)), function.args),
        )))
    })
}
