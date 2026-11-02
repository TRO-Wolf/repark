use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue, internal_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, lit};
use datafusion::optimizer::AnalyzerRule;

use super::key_by_position::{KeyPlan, LeafCaster, key_by_position_udf};
use super::{element_at_udf, spark_type_name};

const SUBSCRIPT_FUNCTIONS: [&str; 3] = ["get_field", "array_element", "__repark_get_item__"];

#[derive(Debug, Default)]
pub struct SparkMapSubscript;

impl AnalyzerRule for SparkMapSubscript {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|node| rewrite_plan(node, config))
            .data()
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

#[allow(clippy::missing_errors_doc)]
pub fn map_array_subscripts_as_lookups(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    expr.transform_up(|node| {
        let Expr::ScalarFunction(function) = &node else {
            return Ok(Transformed::no(node));
        };
        let [map, key] = function.args.as_slice() else {
            return Ok(Transformed::no(node));
        };
        if function.func.name() != "array_element" {
            return Ok(Transformed::no(node));
        }
        let Ok(DataType::Map(entries, _)) = map.get_type(schema) else {
            return Ok(Transformed::no(node));
        };
        let nested = map_key_type(entries.data_type()).is_some_and(DataType::is_nested);
        let args = vec![map.clone(), key.clone()];
        let rewritten = if nested {
            datafusion::functions::core::get_field().call(args)
        } else {
            Expr::ScalarFunction(ScalarFunction::new_udf(element_at_udf(), args))
        };
        Ok(Transformed::yes(rewritten))
    })
    .data()
}

fn rewrite_plan(plan: LogicalPlan, config: &ConfigOptions) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| rewrite_expr(node, &schema, config))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_expr(
    expr: Expr,
    schema: &DFSchema,
    config: &ConfigOptions,
) -> Result<Transformed<Expr>> {
    let Some((map_type, lookup_type)) = nested_key_subscript(&expr, schema) else {
        return Ok(Transformed::no(expr));
    };
    let DataType::Map(entries, sorted) = &map_type else {
        return Ok(Transformed::no(expr));
    };
    let Some(key_type) = map_key_type(entries.data_type()).cloned() else {
        return Ok(Transformed::no(expr));
    };
    let Expr::ScalarFunction(function) = expr else {
        return Ok(Transformed::no(expr));
    };
    let Ok([map, lookup]) = <[Expr; 2]>::try_from(function.args) else {
        return internal_err!("a map subscript takes a map and one key");
    };
    let ansi = crate::ansi::spark_ansi_enabled_from_options(config);
    if !accepts(&lookup_type, &key_type) || (!ansi && !legacy_accepts(&lookup_type, &key_type)) {
        return plan_err!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve the map subscript due to data \
             type mismatch: The second parameter requires the \"{}\" type, however the key has the \
             type \"{}\". SQLSTATE: 42K09",
            spark_type_text(&key_type),
            spark_type_text(&lookup_type)
        );
    }
    let target = shape(&key_type, &key_type);
    let caster = LeafCaster::new(config)?;
    let map = if target == key_type && !key_plan_normalises(&caster, &key_type)? {
        map
    } else {
        let normal_map = DataType::Map(normal_entries(entries, &target)?, *sorted);
        let plan = KeyPlan::Map(Box::new(KeyPlan::Struct(vec![
            caster.plan(&key_type, &target, true)?,
            KeyPlan::Keep,
        ])));
        by_position_call(map, &normal_map, plan)?
    };
    let plan = caster.plan(&lookup_type, &target, true)?;
    let key = if plan == KeyPlan::Keep {
        lookup
    } else {
        by_position_call(lookup, &target, plan)?
    };
    Ok(Transformed::yes(Expr::ScalarFunction(
        ScalarFunction::new_udf(element_at_udf(), vec![map, key]),
    )))
}

fn key_plan_normalises(caster: &LeafCaster, key_type: &DataType) -> Result<bool> {
    Ok(caster.plan(key_type, key_type, true)? != KeyPlan::Keep)
}

fn by_position_call(expr: Expr, target: &DataType, plan: KeyPlan) -> Result<Expr> {
    Ok(key_by_position_udf(plan).call(vec![expr, lit(ScalarValue::try_from(target)?)]))
}

fn map_key_type(entries: &DataType) -> Option<&DataType> {
    let DataType::Struct(fields) = entries else {
        return None;
    };
    fields.first().map(|field| field.data_type())
}

fn normal_entries(entries: &FieldRef, key: &DataType) -> Result<FieldRef> {
    let DataType::Struct(fields) = entries.data_type() else {
        return internal_err!("map entries must be a struct");
    };
    let [key_field, value_field] = fields.iter().collect::<Vec<_>>()[..] else {
        return internal_err!("map entries must hold a key and a value");
    };
    let entry_fields = Fields::from(vec![
        Arc::new(Field::new(key_field.name(), key.clone(), false)),
        Arc::clone(value_field),
    ]);
    Ok(Arc::new(Field::new(
        entries.name(),
        DataType::Struct(entry_fields),
        false,
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
    let map_type = map.get_type(schema).ok()?;
    let DataType::Map(entries, _) = &map_type else {
        return None;
    };
    if !map_key_type(entries.data_type())?.is_nested() {
        return None;
    }
    Some((map_type, lookup.get_type(schema).ok()?))
}

fn list_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => Some(element),
        _ => None,
    }
}

fn shape(from: &DataType, to: &DataType) -> DataType {
    match (from, to) {
        (DataType::Struct(from_fields), DataType::Struct(to_fields))
            if from_fields.len() == to_fields.len() =>
        {
            DataType::Struct(
                from_fields
                    .iter()
                    .zip(to_fields.iter())
                    .map(|(from_field, to_field)| {
                        Field::new(
                            to_field.name(),
                            shape(from_field.data_type(), to_field.data_type()),
                            true,
                        )
                    })
                    .collect(),
            )
        }
        (DataType::Map(from_entries, _), DataType::Map(to_entries, sorted)) => {
            let shaped = shape(from_entries.data_type(), to_entries.data_type());
            let DataType::Struct(fields) = &shaped else {
                return from.clone();
            };
            let entry_fields: Fields = fields
                .iter()
                .enumerate()
                .map(|(index, field)| Arc::new(field.as_ref().clone().with_nullable(index != 0)))
                .collect();
            DataType::Map(
                Arc::new(Field::new(
                    to_entries.name(),
                    DataType::Struct(entry_fields),
                    false,
                )),
                *sorted,
            )
        }
        _ => match (list_element(from), list_element(to)) {
            (Some(from_element), Some(to_element)) => {
                DataType::List(Arc::new(Field::new_list_field(
                    shape(from_element.data_type(), to_element.data_type()),
                    true,
                )))
            }
            _ => from.clone(),
        },
    }
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
        (DataType::Map(from_entries, _), DataType::Map(to_entries, _)) => {
            accepts(from_entries.data_type(), to_entries.data_type())
        }
        _ => match (list_element(from), list_element(to)) {
            (Some(from_element), Some(to_element)) => {
                accepts(from_element.data_type(), to_element.data_type())
            }
            (None, None) if !from.is_nested() && !to.is_nested() => leaf_accepts(from, to),
            _ => false,
        },
    }
}

fn legacy_accepts(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Null, _) => true,
        (DataType::Struct(_) | DataType::Map(..), _)
        | (_, DataType::Struct(_) | DataType::Map(..)) => same_spark_type(from, to),
        _ => match (list_element(from), list_element(to)) {
            (Some(from_element), Some(to_element)) => {
                legacy_accepts(from_element.data_type(), to_element.data_type())
            }
            _ => true,
        },
    }
}

fn same_spark_type(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            from_fields.len() == to_fields.len()
                && from_fields
                    .iter()
                    .zip(to_fields.iter())
                    .all(|(from_field, to_field)| {
                        from_field.name() == to_field.name()
                            && same_spark_type(from_field.data_type(), to_field.data_type())
                    })
        }
        (DataType::Map(from_entries, _), DataType::Map(to_entries, _)) => {
            same_spark_type(from_entries.data_type(), to_entries.data_type())
        }
        _ => match (list_element(from), list_element(to)) {
            (Some(from_element), Some(to_element)) => {
                same_spark_type(from_element.data_type(), to_element.data_type())
            }
            (None, None) => {
                from == to
                    || (is_string(from) && is_string(to))
                    || (is_binary(from) && is_binary(to))
                    || matches!(
                        (from, to),
                        (
                            DataType::Timestamp(_, Some(_)),
                            DataType::Timestamp(_, Some(_))
                        ) | (DataType::Timestamp(_, None), DataType::Timestamp(_, None))
                            | (
                                DataType::Date32 | DataType::Date64,
                                DataType::Date32 | DataType::Date64
                            )
                    )
            }
            _ => false,
        },
    }
}

fn leaf_accepts(from: &DataType, to: &DataType) -> bool {
    from == to
        || is_string(to)
        || (from.is_numeric() && to.is_numeric())
        || (is_datetime(from) && is_datetime(to))
        || (is_binary(from) && is_binary(to))
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_datetime(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
    )
}

fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
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
