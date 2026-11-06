use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue, plan_err};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct SparkFieldAmbiguity;

impl AnalyzerRule for SparkFieldAmbiguity {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let case_sensitive = crate::case_sensitive::spark_case_sensitive_from_options(config);
        plan.apply_with_subqueries(|node| {
            refuse_in_node(node, case_sensitive)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }

    fn name(&self) -> &'static str {
        "spark_field_ambiguity"
    }
}

fn refuse_in_node(node: &LogicalPlan, case_sensitive: bool) -> Result<()> {
    let mut schema: Option<DFSchema> = None;
    node.apply_expressions(|expr| {
        expr.apply(|leaf| {
            let Expr::ScalarFunction(function) = leaf else {
                return Ok(TreeNodeRecursion::Continue);
            };
            let [base, path @ ..] = function.args.as_slice() else {
                return Ok(TreeNodeRecursion::Continue);
            };
            if function.func.name() != "get_field" || path.is_empty() {
                return Ok(TreeNodeRecursion::Continue);
            }
            let schema = schema.get_or_insert_with(|| merged_inputs(node));
            let Ok(data_type) = base.get_type(schema) else {
                return Ok(TreeNodeRecursion::Continue);
            };
            refuse_path(data_type, path, case_sensitive)?;
            Ok(TreeNodeRecursion::Continue)
        })
    })?;
    Ok(())
}

fn merged_inputs(node: &LogicalPlan) -> DFSchema {
    let mut schema = DFSchema::empty();
    for input in node.inputs() {
        schema.merge(input.schema());
    }
    schema
}

fn refuse_path(mut data_type: DataType, path: &[Expr], case_sensitive: bool) -> Result<()> {
    for key in path {
        let Some(name) = literal_text(key) else {
            return Ok(());
        };
        data_type = match data_type {
            DataType::Struct(fields) => {
                let mut found = fields.iter().filter(|field| {
                    if case_sensitive {
                        field.name() == name
                    } else {
                        field.name().eq_ignore_ascii_case(name)
                    }
                });
                let Some(first) = found.next() else {
                    return Ok(());
                };
                let count = 1 + found.count();
                if count > 1 {
                    return plan_err!(
                        "[AMBIGUOUS_REFERENCE_TO_FIELDS] Ambiguous reference to the field `{}`. \
                         It appears {count} times in the schema. SQLSTATE: 42000",
                        name.replace('`', "``")
                    );
                }
                first.data_type().clone()
            }
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(pair) if pair.len() == 2 => pair[1].data_type().clone(),
                _ => return Ok(()),
            },
            _ => return Ok(()),
        };
    }
    Ok(())
}

fn literal_text(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::Utf8View(Some(text))
            | ScalarValue::LargeUtf8(Some(text)),
            _,
        ) => Some(text.as_str()),
        _ => None,
    }
}
