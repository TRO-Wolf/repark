#[cfg(test)]
mod tests;
mod udf;

use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;

pub use udf::repark_isnan_udf;

#[must_use]
pub fn repark_isnan_call(arg: Expr) -> Expr {
    repark_isnan_udf().call(vec![arg])
}

pub fn register_repark_isnan(context: &SessionContext) {
    context.register_udf(repark_isnan_udf().as_ref().clone());
}
