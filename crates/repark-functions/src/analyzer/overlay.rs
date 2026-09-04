use datafusion::common::ScalarValue;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::ScalarFunction;

pub(super) fn rewrite(function: ScalarFunction) -> Transformed<Expr> {
    if function.args.len() >= 4 && is_negative_one_literal(&function.args[3]) {
        let mut args = function.args;
        args.truncate(3);
        return Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
            function.func,
            args,
        )));
    }
    Transformed::no(Expr::ScalarFunction(function))
}

fn is_negative_one_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(
            ScalarValue::Int64(Some(-1))
                | ScalarValue::Int32(Some(-1))
                | ScalarValue::Int16(Some(-1))
                | ScalarValue::Int8(Some(-1)),
            _
        )
    )
}
