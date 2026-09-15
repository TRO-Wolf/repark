#[cfg(test)]
mod tests;
mod udf;

use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;

pub(crate) use udf::spark_sql_type;
pub use udf::update_fields_udf;

#[must_use]
pub fn update_fields_call(args: Vec<Expr>) -> Expr {
    update_fields_udf().call(args)
}

pub fn register_update_fields(context: &SessionContext) {
    context.register_udf(update_fields_udf().as_ref().clone());
}
