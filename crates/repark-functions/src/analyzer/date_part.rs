use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::ScalarFunction;

pub(super) fn rewrite(expr: Expr) -> Transformed<Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return Transformed::no(expr);
    };
    if function.func.name() != "date_part" {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        crate::spark_date_part::date_part_udf(),
        function.args,
    )))
}
