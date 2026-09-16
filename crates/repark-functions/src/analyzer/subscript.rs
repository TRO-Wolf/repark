use datafusion::common::DFSchema;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, ExprSchemable};

use datafusion::arrow::datatypes::DataType;

pub(super) fn rewrite_array_subscript(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return Transformed::no(expr);
    };
    if function
        .func
        .inner()
        .downcast_ref::<crate::collection::spark_array::SparkArrayElement>()
        .is_some()
    {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    let types = (
        function.args[0].get_type(schema),
        function.args[1].get_type(schema),
    );
    let (Ok(array_type), Ok(index_type)) = types else {
        return Transformed::no(Expr::ScalarFunction(function));
    };
    if list_element_type(&array_type).is_none() || !index_type.is_integer() {
        return Transformed::no(Expr::ScalarFunction(function));
    }
    let mut args = function.args;
    args[0] = strip_list_coercion_cast(args[0].clone(), schema);
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        crate::collection::spark_array_get_udf(),
        args,
    )))
}

fn strip_list_coercion_cast(expr: Expr, schema: &DFSchema) -> Expr {
    let Expr::Cast(cast) = &expr else {
        return expr;
    };
    let Some(target) = list_element_field(cast.field.data_type()) else {
        return expr;
    };
    let Ok(inner_type) = cast.expr.get_type(schema) else {
        return expr;
    };
    let Some(source) = list_element_field(&inner_type) else {
        return expr;
    };
    if target.name() == "item" && target.is_nullable() == source.is_nullable() {
        *cast.expr.clone()
    } else {
        expr
    }
}

fn list_element_field(data_type: &DataType) -> Option<&arrow::datatypes::FieldRef> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field)
        }
        _ => None,
    }
}

fn list_element_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.data_type().clone())
        }
        _ => None,
    }
}
