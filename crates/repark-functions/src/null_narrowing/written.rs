use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::FieldRef;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::expr::{Cast, ScalarFunction, TryCast};
use datafusion::logical_expr::expr_rewriter::SavedName;
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF};

use super::{NarrowedBeside, sql_timestamp};

pub const WRITTEN_CAST_NAME: &str = "__repark_written_timestamp_cast__";
const WRITTEN_SIGN: &str = "repark.written";

static WRITTEN_CAST: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(NarrowedBeside::new(WRITTEN_CAST_NAME))));

#[must_use]
pub fn written_cast_udf() -> Arc<ScalarUDF> {
    Arc::clone(&WRITTEN_CAST)
}

#[must_use]
pub fn is_written_cast(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(function) if function.func.name() == WRITTEN_CAST_NAME
    )
}

pub(super) fn written_cast(value: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(written_cast_udf(), vec![value]))
}

pub(crate) fn restored(expr: Expr, saved: SavedName) -> Expr {
    match expr {
        Expr::ScalarFunction(mut kept) if kept.func.name() == WRITTEN_CAST_NAME => {
            let Some(value) = kept.args.pop() else {
                return Expr::ScalarFunction(kept);
            };
            match saved.restore(value) {
                Expr::Alias(mut alias) => {
                    alias.expr = Box::new(written_cast(*alias.expr));
                    Expr::Alias(alias)
                }
                value => written_cast(value),
            }
        }
        other => saved.restore(other),
    }
}

fn named(expr: Expr, name: &str) -> Expr {
    let renamed = |field: &FieldRef| Arc::new(field.as_ref().clone().with_name(name));
    match expr {
        Expr::Cast(cast) => {
            let field = renamed(&cast.field);
            Expr::Cast(Cast::new_from_field(cast.expr, field))
        }
        Expr::TryCast(cast) => {
            let field = renamed(&cast.field);
            Expr::TryCast(TryCast::new_from_field(cast.expr, field))
        }
        other => other,
    }
}

pub(super) fn signed(expr: Expr) -> Expr {
    named(expr, WRITTEN_SIGN)
}

#[allow(clippy::missing_errors_doc)]
pub fn unsigned(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            found = found || expr.exists(|inner| Ok(is_signed(inner)))?;
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
    })?;
    if !found {
        return Ok(None);
    }
    let plain = plan.clone().transform_down_with_subqueries(|node| {
        node.map_expressions(|expr| {
            expr.transform_up(|inner| {
                Ok(if is_signed(&inner) {
                    Transformed::yes(named(inner, ""))
                } else {
                    Transformed::no(inner)
                })
            })
        })
    })?;
    Ok(Some(plain.data))
}

pub(super) fn is_signed(expr: &Expr) -> bool {
    match expr {
        Expr::Cast(cast) => cast.field.name() == WRITTEN_SIGN,
        Expr::TryCast(cast) => cast.field.name() == WRITTEN_SIGN,
        _ => false,
    }
}

pub(super) fn wire_cast_source(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::Cast(cast) if sql_timestamp(cast.field.data_type()) => Some(&cast.expr),
        Expr::TryCast(cast) if sql_timestamp(cast.field.data_type()) => Some(&cast.expr),
        _ => None,
    }
}
