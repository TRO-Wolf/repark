use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, internal_err, plan_err};
use datafusion::functions::core::expr_fn::{get_field, named_struct};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Cast, Expr, ExprSchemable, LogicalPlan, lit, when};
use datafusion::optimizer::AnalyzerRule;

use super::{element_at_udf, spark_type_name};

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
    if !same_shape(&lookup_type, &key_type) {
        return plan_err!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve the map subscript due to data \
             type mismatch: The second parameter requires the \"{}\" type, however the key has the \
             type \"{}\". SQLSTATE: 42K09",
            spark_struct_name(&key_type),
            spark_struct_name(&lookup_type)
        );
    }
    let key = by_position(lookup, &lookup_type, &key_type)?;
    let key = if lookup_type == key_type {
        key
    } else {
        Expr::Cast(Cast::new(Box::new(key), key_type))
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
    if function.func.name() != "get_field" {
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

fn by_position(expr: Expr, from: &DataType, to: &DataType) -> Result<Expr> {
    match (from, to) {
        (DataType::Struct(from_fields), DataType::Struct(to_fields))
            if needs_position(from, to) =>
        {
            rebuild(expr, from_fields, to_fields)
        }
        _ => Ok(expr),
    }
}

fn needs_position(from: &DataType, to: &DataType) -> bool {
    let (DataType::Struct(from_fields), DataType::Struct(to_fields)) = (from, to) else {
        return false;
    };
    from_fields.len() == to_fields.len()
        && from_fields
            .iter()
            .zip(to_fields.iter())
            .any(|(from_field, to_field)| {
                from_field.name() != to_field.name()
                    || needs_position(from_field.data_type(), to_field.data_type())
            })
}

fn rebuild(expr: Expr, from_fields: &Fields, to_fields: &Fields) -> Result<Expr> {
    let mut parts = Vec::with_capacity(to_fields.len() * 2);
    for (from_field, to_field) in from_fields.iter().zip(to_fields.iter()) {
        let child = get_field(expr.clone(), from_field.name().as_str());
        parts.push(lit(to_field.name().as_str()));
        parts.push(by_position(
            child,
            from_field.data_type(),
            to_field.data_type(),
        )?);
    }
    when(expr.is_not_null(), named_struct(parts)).end()
}

fn same_shape(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Null, _) => true,
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            from_fields.len() == to_fields.len()
                && from_fields
                    .iter()
                    .zip(to_fields.iter())
                    .all(|(from_field, to_field)| {
                        same_shape(from_field.data_type(), to_field.data_type())
                    })
        }
        (DataType::Struct(_), _) | (_, DataType::Struct(_)) => false,
        (
            DataType::List(from_field) | DataType::LargeList(from_field),
            DataType::List(to_field) | DataType::LargeList(to_field),
        ) => same_shape(from_field.data_type(), to_field.data_type()),
        _ => true,
    }
}

fn spark_struct_name(data_type: &DataType) -> String {
    let DataType::Struct(fields) = data_type else {
        return spark_type_name(data_type);
    };
    let members: Vec<String> = fields
        .iter()
        .map(|field| {
            let nullability = if field.is_nullable() { "" } else { " NOT NULL" };
            format!(
                "{}: {}{nullability}",
                field.name(),
                spark_struct_name(field.data_type())
            )
        })
        .collect();
    format!("STRUCT<{}>", members.join(", "))
}
