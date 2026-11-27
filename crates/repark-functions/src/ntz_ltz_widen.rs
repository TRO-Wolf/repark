use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use crate::instant_ts::is_ltz_timestamp;

#[derive(Debug, Default)]
pub struct SparkNtzLtzWiden;

#[must_use]
pub fn ntz_ltz_widen_rule() -> Arc<dyn AnalyzerRule + Send + Sync> {
    Arc::new(SparkNtzLtzWiden)
}

impl AnalyzerRule for SparkNtzLtzWiden {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(localize_node).data()
    }

    fn name(&self) -> &'static str {
        "spark_ntz_ltz_widen"
    }
}

fn widens_a_wall(source: &DataType, target: &DataType) -> Option<bool> {
    match (source, target) {
        (DataType::Timestamp(TimeUnit::Microsecond, None), target) if is_ltz_timestamp(target) => {
            Some(true)
        }
        (DataType::List(source), DataType::List(target))
        | (DataType::LargeList(source), DataType::LargeList(target))
        | (DataType::Map(source, _), DataType::Map(target, _)) => {
            widens_a_wall(source.data_type(), target.data_type())
        }
        (DataType::FixedSizeList(source, width), DataType::FixedSizeList(target, other))
            if width == other =>
        {
            widens_a_wall(source.data_type(), target.data_type())
        }
        (DataType::Struct(sources), DataType::Struct(targets))
            if sources.len() == targets.len() =>
        {
            let mut widened = false;
            for (source, target) in sources.iter().zip(targets.iter()) {
                widened |= widens_a_wall(source.data_type(), target.data_type())?;
            }
            Some(widened)
        }
        (source, target) if source == target => Some(false),
        _ => None,
    }
}

fn localize_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    let Ok(source) = cast.expr.get_type(schema) else {
        return Transformed::no(expr);
    };
    if widens_a_wall(&source, cast.field.data_type()) != Some(true) {
        return Transformed::no(expr);
    }
    match crate::zone_seam::seam_call(*cast.expr.clone(), cast.field.data_type(), false) {
        Some(localized) => Transformed::yes(localized),
        None => Transformed::no(expr),
    }
}

fn localize_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if matches!(plan, LogicalPlan::Values(_)) {
        return Ok(Transformed::no(plan));
    }
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| Ok(localize_cast(node, &schema)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    if transformed.transformed {
        transformed.map_data(LogicalPlan::recompute_schema)
    } else {
        Ok(transformed)
    }
}
