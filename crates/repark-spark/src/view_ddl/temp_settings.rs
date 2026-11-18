use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema};
use datafusion::error::Result;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{
    Between, BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator,
    TryCast, lit,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExecBuilder;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, PhysicalExpr};
use datafusion::sql::TableReference;
use repark_functions::ansi::SparkAnsiConfig;
use repark_functions::decimal_legacy::SparkDecimalTruncateLegacy;
use repark_functions::session_time_zone::{
    SessionTimeZoneConfig, session_time_zone_display_from_options,
};
use repark_functions::timestamp_type::SparkTimestampTypeConfig;

const CURRENT_TIMEZONE: &str = "current_timezone";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TempViewSettings {
    ansi: Option<SparkAnsiConfig>,
    zone: Option<SessionTimeZoneConfig>,
    decimal_legacy: Option<SparkDecimalTruncateLegacy>,
    timestamp_type: Option<SparkTimestampTypeConfig>,
}

#[derive(Debug)]
struct ReaderZone(String);

impl TempViewSettings {
    pub(crate) fn capture(options: &ConfigOptions) -> Self {
        let extensions = &options.extensions;
        Self {
            ansi: extensions.get::<SparkAnsiConfig>().cloned(),
            zone: extensions.get::<SessionTimeZoneConfig>().cloned(),
            decimal_legacy: extensions.get::<SparkDecimalTruncateLegacy>().cloned(),
            timestamp_type: extensions.get::<SparkTimestampTypeConfig>().cloned(),
        }
    }

    fn same_as(&self, options: &ConfigOptions) -> bool {
        let extensions = &options.extensions;
        extensions.get::<SparkAnsiConfig>() == self.ansi.as_ref()
            && extensions.get::<SessionTimeZoneConfig>() == self.zone.as_ref()
            && extensions.get::<SparkDecimalTruncateLegacy>() == self.decimal_legacy.as_ref()
            && extensions.get::<SparkTimestampTypeConfig>() == self.timestamp_type.as_ref()
    }

    fn apply(&self, options: &mut ConfigOptions) {
        let extensions = &mut options.extensions;
        if let Some(ansi) = &self.ansi {
            extensions.insert(ansi.clone());
        }
        if let Some(zone) = &self.zone {
            extensions.insert(zone.clone());
        }
        if let Some(decimal_legacy) = &self.decimal_legacy {
            extensions.insert(decimal_legacy.clone());
        }
        if let Some(timestamp_type) = &self.timestamp_type {
            extensions.insert(timestamp_type.clone());
        }
    }

    pub(crate) fn overlay(
        &self,
        reader: &dyn Session,
        live: &ConfigOptions,
    ) -> Option<SessionState> {
        let outer_zone = reader.config().get_extension::<ReaderZone>();
        let zone_kept = outer_zone.as_ref().is_none_or(|zone| {
            Some(zone.0.as_str()) == self.zone.as_ref().map(SessionTimeZoneConfig::display)
        });
        if zone_kept && self.same_as(reader.config_options()) && self.same_as(live) {
            return None;
        }
        let reader = reader.as_any().downcast_ref::<SessionState>()?;
        let zone = outer_zone.map_or_else(
            || session_time_zone_display_from_options(reader.config_options()).to_string(),
            |zone| zone.0.clone(),
        );
        let mut overlay = reader.clone();
        let config = overlay.config_mut();
        self.apply(config.options_mut());
        config.set_extension(Arc::new(ReaderZone(zone)));
        let options = Arc::clone(overlay.config_options());
        overlay.execution_props_mut().config_options = Some(options);
        Some(overlay)
    }
}

pub(crate) fn settings_free(expr: &Expr, schema: Option<&DFSchema>) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(..) => true,
        Expr::Alias(alias) => settings_free(&alias.expr, schema),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            free_operator(*op) && settings_free(left, schema) && settings_free(right, schema)
        }
        Expr::Not(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsNotUnknown(inner) => settings_free(inner, schema),
        Expr::InList(InList { expr, list, .. }) => {
            settings_free(expr, schema) && list.iter().all(|item| settings_free(item, schema))
        }
        Expr::Between(Between {
            expr, low, high, ..
        }) => {
            settings_free(expr, schema) && settings_free(low, schema) && settings_free(high, schema)
        }
        Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field }) => {
            numeric_cast(expr, field.data_type(), schema) && settings_free(expr, schema)
        }
        _ => false,
    }
}

fn free_operator(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
            | Operator::And
            | Operator::Or
    )
}

fn numeric_cast(operand: &Expr, target: &DataType, schema: Option<&DFSchema>) -> bool {
    target.is_numeric()
        && schema
            .and_then(|schema| operand.get_type(schema).ok())
            .is_some_and(|source| source.is_numeric())
}

pub(crate) fn inlines_settings_reading_frame(
    body: &LogicalPlan,
    homes: &[TableReference],
) -> Result<bool> {
    let mut found = false;
    body.apply_with_subqueries(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node
            && (!matches!(alias.alias, TableReference::Bare { .. }) || homes.contains(&alias.alias))
            && reads_settings(alias.input.as_ref())?
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

fn reads_settings(plan: &LogicalPlan) -> Result<bool> {
    let mut reads = false;
    plan.apply_with_subqueries(|node| {
        if column_only_node(node)? {
            Ok(TreeNodeRecursion::Continue)
        } else {
            reads = true;
            Ok(TreeNodeRecursion::Stop)
        }
    })?;
    Ok(reads)
}

fn column_only_node(node: &LogicalPlan) -> Result<bool> {
    let shape = match node {
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::EmptyRelation(_) => true,
        LogicalPlan::TableScan(scan) => match scan.source.get_logical_plan() {
            Some(inner) => !reads_settings(&inner)?,
            None => true,
        },
        LogicalPlan::Union(union) => union.inputs.iter().all(|input| {
            input.schema().fields().len() == union.schema.fields().len()
                && input
                    .schema()
                    .fields()
                    .iter()
                    .zip(union.schema.fields())
                    .all(|(field, out)| field.data_type() == out.data_type())
        }),
        LogicalPlan::Join(join) => join.on.iter().all(|(left, right)| {
            matches!(
                (left.get_type(join.left.schema()), right.get_type(join.right.schema())),
                (Ok(left), Ok(right)) if left == right
            )
        }),
        _ => false,
    };
    if !shape {
        return Ok(false);
    }
    let mut free = true;
    node.apply_expressions(|expr| {
        free = bare_column(expr);
        Ok(if free {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    })?;
    Ok(free)
}

fn bare_column(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Alias(alias) => bare_column(&alias.expr),
        _ => false,
    }
}

pub(crate) fn reader_zone(overlay: &SessionState) -> String {
    overlay.config().get_extension::<ReaderZone>().map_or_else(
        || session_time_zone_display_from_options(overlay.config_options()).to_string(),
        |zone| zone.0.clone(),
    )
}

pub(crate) fn rewrite_current_timezone(plan: LogicalPlan, zone: &str) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|node| {
        let names = NamePreserver::new(&node);
        node.map_expressions(|expr| {
            let saved = names.save(&expr);
            let rewritten = expr.transform_up(|inner| match &inner {
                Expr::ScalarFunction(call)
                    if call.name() == CURRENT_TIMEZONE && call.args.is_empty() =>
                {
                    Ok(Transformed::yes(lit(zone)))
                }
                _ => Ok(Transformed::no(inner)),
            })?;
            Ok(if rewritten.transformed {
                rewritten.update_data(|expr| saved.restore(expr))
            } else {
                rewritten
            })
        })
    })
    .map(|rewritten| rewritten.data)
}

pub(crate) async fn plan_under_overlay(
    overlay: &SessionState,
    reader: &dyn Session,
    body: LogicalPlan,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let body = overlay
        .analyzer()
        .execute_and_check(body, overlay.config_options(), |_, _| {})?;
    let body = rewrite_current_timezone(body, &reader_zone(overlay))?;
    let view_schema = Arc::clone(body.schema());
    let (free, sensitive): (Vec<Expr>, Vec<Expr>) = filters
        .iter()
        .cloned()
        .flat_map(split_conjunction_owned)
        .partition(|conjunct| settings_free(conjunct, Some(view_schema.as_ref())));
    let mut builder = LogicalPlanBuilder::from(body);
    if let Some(free) = conjunction(free) {
        builder = builder.filter(free)?;
    }
    let width = view_schema.fields().len();
    let wanted = projection.map_or_else(|| (0..width).collect(), Clone::clone);
    let needed = if sensitive.is_empty() {
        wanted.clone()
    } else {
        needed_columns(&view_schema, &wanted, &sensitive)
    };
    if !needed.iter().copied().eq(0..width) {
        let fields = needed
            .iter()
            .map(|index| Expr::Column(Column::from(view_schema.qualified_field(*index))))
            .collect::<Vec<_>>();
        builder = builder.project(fields)?;
    }
    if sensitive.is_empty()
        && let Some(limit) = limit
    {
        builder = builder.limit(0, Some(limit))?;
    }
    let plan = builder.build()?;
    let below = Arc::clone(plan.schema());
    let plan = overlay.optimizer().optimize(plan, overlay, |_, _| {})?;
    let input = overlay
        .query_planner()
        .create_physical_plan(&plan, overlay)
        .await?;
    match conjunction(sensitive) {
        None => Ok(input),
        Some(predicate) => {
            let predicate = reader.create_physical_expr(predicate, below.as_ref())?;
            let kept = wanted
                .iter()
                .filter_map(|index| needed.iter().position(|column| column == index))
                .collect::<Vec<_>>();
            let kept = (!kept.iter().copied().eq(0..needed.len())).then_some(kept);
            stack_above(predicate, input, kept, limit)
        }
    }
}

fn needed_columns(schema: &DFSchema, wanted: &[usize], sensitive: &[Expr]) -> Vec<usize> {
    let mut needed = wanted.iter().copied().collect::<BTreeSet<_>>();
    for conjunct in sensitive {
        for column in conjunct.column_refs() {
            match schema.index_of_column_by_name(column.relation.as_ref(), &column.name) {
                Some(index) => {
                    needed.insert(index);
                }
                None => return (0..schema.fields().len()).collect(),
            }
        }
    }
    needed.into_iter().collect()
}

pub(crate) fn stack_above(
    predicate: Arc<dyn PhysicalExpr>,
    input: Arc<dyn ExecutionPlan>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let filtered: Arc<dyn ExecutionPlan> = Arc::new(
        FilterExecBuilder::new(predicate, input)
            .apply_projection(projection)?
            .build()?,
    );
    let Some(limit) = limit else {
        return Ok(filtered);
    };
    if filtered.output_partitioning().partition_count() == 1 {
        return Ok(Arc::new(GlobalLimitExec::new(filtered, 0, Some(limit))));
    }
    let local: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(filtered, limit));
    Ok(Arc::new(GlobalLimitExec::new(
        Arc::new(CoalescePartitionsExec::new(local)),
        0,
        Some(limit),
    )))
}

#[cfg(test)]
mod tests;
