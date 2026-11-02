use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue, internal_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, lit};
use datafusion::optimizer::AnalyzerRule;

use super::key_by_position::key_by_position_udf;
use super::{element_at_udf, spark_type_name};

const SUBSCRIPT_FUNCTIONS: [&str; 3] = ["get_field", "array_element", "__repark_get_item__"];

#[derive(Debug, Default)]
pub struct SparkMapSubscript;

impl AnalyzerRule for SparkMapSubscript {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_map_subscript"
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn insert_map_subscript_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return plan_err!(
            "the map subscript rule requires the default type_coercion analyzer rule"
        );
    };
    rules.insert(position, Arc::new(SparkMapSubscript));
    Ok(rules)
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| rewrite_expr(node, &schema))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Some((key_type, lookup_type)) = nested_key_subscript(&expr, schema) else {
        return Ok(Transformed::no(expr));
    };
    let Expr::ScalarFunction(function) = expr else {
        return Ok(Transformed::no(expr));
    };
    let Ok([map, lookup]) = <[Expr; 2]>::try_from(function.args) else {
        return internal_err!("a map subscript takes a map and one key");
    };
    if !accepts(&lookup_type, &key_type) {
        return plan_err!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve the map subscript due to data \
             type mismatch: The second parameter requires the \"{}\" type, however the key has the \
             type \"{}\". SQLSTATE: 42K09",
            spark_type_text(&key_type),
            spark_type_text(&lookup_type)
        );
    }
    let key = if lookup_type == key_type {
        lookup
    } else {
        key_by_position_udf().call(vec![lookup, lit(ScalarValue::try_from(&key_type)?)])
    };
    Ok(Transformed::yes(Expr::ScalarFunction(
        ScalarFunction::new_udf(element_at_udf(), vec![map, key]),
    )))
}

fn nested_key_subscript(expr: &Expr, schema: &DFSchema) -> Option<(DataType, DataType)> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    let [map, lookup] = function.args.as_slice() else {
        return None;
    };
    if !SUBSCRIPT_FUNCTIONS.contains(&function.func.name()) {
        return None;
    }
    let DataType::Map(entries, _) = map.get_type(schema).ok()? else {
        return None;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    let key_type = fields.first()?.data_type().clone();
    if !key_type.is_nested() {
        return None;
    }
    Some((key_type, lookup.get_type(schema).ok()?))
}

fn accepts(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Null, _) => true,
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            from_fields.len() == to_fields.len()
                && from_fields
                    .iter()
                    .zip(to_fields.iter())
                    .all(|(from_field, to_field)| {
                        accepts(from_field.data_type(), to_field.data_type())
                    })
        }
        (
            DataType::List(from_field)
            | DataType::LargeList(from_field)
            | DataType::FixedSizeList(from_field, _),
            DataType::List(to_field) | DataType::LargeList(to_field),
        ) => accepts(from_field.data_type(), to_field.data_type()),
        (DataType::Map(from_entries, _), DataType::Map(to_entries, _)) => {
            accepts(from_entries.data_type(), to_entries.data_type())
        }
        _ if from.is_nested() || to.is_nested() => false,
        _ => {
            from == to
                || (is_string(from) && is_string(to))
                || (from.is_numeric() && (to.is_numeric() || is_string(to)))
        }
    }
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn spark_type_text(data_type: &DataType) -> String {
    match data_type {
        DataType::Struct(fields) => {
            let members: Vec<String> = fields
                .iter()
                .map(|field| {
                    let nullability = if field.is_nullable() { "" } else { " NOT NULL" };
                    format!(
                        "{}: {}{nullability}",
                        field.name(),
                        spark_type_text(field.data_type())
                    )
                })
                .collect();
            format!("STRUCT<{}>", members.join(", "))
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_type_text(field.data_type()))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => format!(
                "MAP<{}, {}>",
                spark_type_text(fields[0].data_type()),
                spark_type_text(fields[1].data_type())
            ),
            other => spark_type_name(other),
        },
        other => spark_type_name(other),
    }
}
