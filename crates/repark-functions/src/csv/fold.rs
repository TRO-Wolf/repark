use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use super::from_csv::FROM_CSV_UDF;
use super::{options_from_entries, string_map_entries};

#[derive(Debug)]
pub(crate) struct CsvFold;

impl CsvFold {
    pub(crate) fn rule() -> Arc<dyn AnalyzerRule + Send + Sync> {
        Arc::new(Self)
    }
}

fn literal_text(scalar: &datafusion::common::ScalarValue) -> Option<String> {
    match scalar {
        datafusion::common::ScalarValue::Utf8(value)
        | datafusion::common::ScalarValue::LargeUtf8(value) => value.clone(),
        datafusion::common::ScalarValue::Utf8View(Some(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn options_pairs(expr: &Expr) -> Option<HashMap<String, String>> {
    match expr {
        Expr::Literal(datafusion::common::ScalarValue::Map(map), _) => {
            if map.is_empty() {
                return Some(HashMap::new());
            }
            let entries = map.value(0);
            string_map_entries(entries.column(0), entries.column(1)).ok()
        }
        Expr::ScalarFunction(function) if function.func.name() == "create_map" => {
            let mut args = function.args.iter();
            let mut raw = HashMap::new();
            loop {
                match (args.next(), args.next()) {
                    (Some(key), Some(value)) => {
                        let Expr::Literal(key, _) = key else {
                            return None;
                        };
                        let Expr::Literal(value, _) = value else {
                            return None;
                        };
                        raw.insert(literal_text(key)?, literal_text(value)?);
                    }
                    (None, None) => break,
                    _ => return None,
                }
            }
            Some(raw)
        }
        Expr::ScalarFunction(function)
            if function.func.name() == "map" && function.args.len() == 2 =>
        {
            let mut lists = Vec::with_capacity(2);
            for arg in &function.args {
                let Expr::ScalarFunction(inner) = arg else {
                    return None;
                };
                if inner.func.name() != "make_array" {
                    return None;
                }
                let mut values = Vec::with_capacity(inner.args.len());
                for item in &inner.args {
                    let Expr::Literal(scalar, _) = item else {
                        return None;
                    };
                    values.push(literal_text(scalar)?);
                }
                lists.push(values);
            }
            if lists[0].len() != lists[1].len() {
                return None;
            }
            Some(
                lists[0]
                    .iter()
                    .cloned()
                    .zip(lists[1].iter().cloned())
                    .collect(),
            )
        }
        _ => None,
    }
}

fn validate_call(expr: &Expr) -> Result<()> {
    let Expr::ScalarFunction(function) = expr else {
        return Ok(());
    };
    if function.func.name() != FROM_CSV_UDF || function.args.len() != 3 {
        return Ok(());
    }
    if let Some(raw) = options_pairs(&function.args[2]) {
        options_from_entries(&raw)?;
    }
    Ok(())
}

impl AnalyzerRule for CsvFold {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let transformed = plan.transform_up(|plan| {
            plan.map_expressions(|expr| {
                expr.transform_up(|expr| {
                    validate_call(&expr)?;
                    Ok(Transformed::no(expr))
                })
            })
        })?;
        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "csv_fold"
    }
}
