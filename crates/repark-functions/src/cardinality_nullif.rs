use datafusion::logical_expr::Expr;

use crate::cardinality::const_i128;

pub(crate) fn nullif_const_int(first: &Expr, _second: &Expr, depth: u32) -> Option<i128> {
    const_i128(first, depth)
}
