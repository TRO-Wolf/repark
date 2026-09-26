use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::{InList, ScalarFunction};
use datafusion::logical_expr::{BinaryExpr, Distinct, Expr, ExprSchemable, LogicalPlan, Operator};
use repark_functions::cast_map::spark_sql_name;

pub(crate) fn refuse_map_ordering(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.apply_with_subqueries(|node| {
        refuse_in_node(node)?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(plan)
}

fn refuse_in_node(node: &LogicalPlan) -> Result<()> {
    let schema = input_schema(node);
    if let LogicalPlan::Distinct(Distinct::All(input)) = node
        && let Some(field) = input
            .schema()
            .fields()
            .iter()
            .find(|field| is_map(field.data_type()))
    {
        return Err(DataFusionError::Plan(format!(
            "[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: \
             Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, \
             EXCEPT, etc.), but the type of column `{}` is \"{}\". SQLSTATE: 0A000",
            field.name(),
            spark_sql_name(field.data_type())
        )));
    }
    if let LogicalPlan::Sort(sort) = node {
        for order in &sort.expr {
            if let Some(map_type) = map_type_of(&order.expr, &schema) {
                let direction = if order.asc { "ASC" } else { "DESC" };
                let nulls = if order.nulls_first { "FIRST" } else { "LAST" };
                return Err(invalid_ordering(
                    &format!("{} {direction} NULLS {nulls}", render(&order.expr)),
                    "sortorder",
                    &map_type,
                ));
            }
        }
    }
    node.apply_expressions(|expr| {
        expr.apply(|inner| {
            refuse_in_expr(inner, &schema)?;
            Ok(TreeNodeRecursion::Continue)
        })
    })
    .map(|_| ())
}

fn refuse_in_expr(expr: &Expr, schema: &DFSchema) -> Result<()> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let Some(symbol) = ordering_symbol(*op) else {
                return Ok(());
            };
            let Some(map_type) = map_type_of(left, schema).or_else(|| map_type_of(right, schema))
            else {
                return Ok(());
            };
            Err(invalid_ordering(
                &format!("({} {symbol} {})", render(left), render(right)),
                symbol,
                &map_type,
            ))
        }
        Expr::InList(InList { expr, list, .. }) => {
            let Some(map_type) = map_type_of(expr, schema)
                .or_else(|| list.iter().find_map(|item| map_type_of(item, schema)))
            else {
                return Ok(());
            };
            let items = list.iter().map(render).collect::<Vec<_>>().join(", ");
            Err(invalid_ordering(
                &format!("({} IN ({items}))", render(expr)),
                "in",
                &map_type,
            ))
        }
        _ => Ok(()),
    }
}

fn ordering_symbol(op: Operator) -> Option<&'static str> {
    match op {
        Operator::Eq | Operator::NotEq => Some("="),
        Operator::Lt => Some("<"),
        Operator::LtEq => Some("<="),
        Operator::Gt => Some(">"),
        Operator::GtEq => Some(">="),
        Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => Some("<=>"),
        _ => None,
    }
}

fn invalid_ordering(resolved: &str, symbol: &str, map_type: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve \"{resolved}\" due to data \
         type mismatch: The `{symbol}` does not support ordering on type \"{}\". SQLSTATE: 42K09",
        spark_sql_name(map_type)
    ))
}

fn input_schema(node: &LogicalPlan) -> DFSchema {
    let mut merged = DFSchema::empty();
    for input in node.inputs() {
        merged.merge(input.schema());
    }
    merged
}

fn is_map(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Map(_, _))
}

fn map_type_of(expr: &Expr, schema: &DFSchema) -> Option<DataType> {
    expr.get_type(schema).ok().filter(is_map)
}

fn render(expr: &Expr) -> String {
    match expr {
        Expr::Column(column) => column.name.clone(),
        Expr::Alias(alias) => render(&alias.expr),
        Expr::Cast(cast) => render(&cast.expr),
        Expr::TryCast(cast) => render(&cast.expr),
        Expr::Literal(value, _) => render_literal(value),
        Expr::ScalarFunction(function) => render_function(function),
        other => other.to_string(),
    }
}

fn render_literal(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::Utf8View(Some(text))
        | ScalarValue::LargeUtf8(Some(text)) => text.clone(),
        other if other.is_null() => "NULL".to_owned(),
        other => other.to_string(),
    }
}

fn render_function(function: &ScalarFunction) -> String {
    let name = function.name();
    if name == "map"
        && let [Expr::ScalarFunction(keys), Expr::ScalarFunction(values)] = function.args.as_slice()
        && keys.name() == "make_array"
        && values.name() == "make_array"
        && keys.args.len() == values.args.len()
    {
        let pairs = keys
            .args
            .iter()
            .zip(&values.args)
            .map(|(key, value)| format!("{}, {}", render(key), render(value)))
            .collect::<Vec<_>>()
            .join(", ");
        return format!("map({pairs})");
    }
    let args = function
        .args
        .iter()
        .map(render)
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}
