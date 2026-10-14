use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::logical_expr::{Cast, Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{DataType as SqlDataType, TimezoneInfo};

use crate::instant_ts::{is_ltz_timestamp, ltz_timestamp_type, ntz_timestamp_type};

const TIMESTAMP_LTZ: &str = "timestamp_ltz";
const TIMESTAMP_NS: &str = "timestamp_ns";
const TIMESTAMPTZ_NS: &str = "timestamptz_ns";

#[derive(Debug, Default, Clone, Copy)]
pub struct SparkTypePlanner;

#[must_use]
pub fn spark_type_planner() -> Arc<dyn TypePlanner> {
    Arc::new(SparkTypePlanner)
}

#[must_use]
pub fn timestamp_ns_type(zoned: bool) -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, zoned.then(|| Arc::<str>::from("UTC")))
}

fn planned_type(sql_type: &SqlDataType) -> Option<DataType> {
    match sql_type {
        SqlDataType::Timestamp(None, TimezoneInfo::None) => Some(ltz_timestamp_type()),
        SqlDataType::TimestampNtz(None) => Some(ntz_timestamp_type()),
        SqlDataType::Custom(name, modifiers) if modifiers.is_empty() => {
            let spelled = name.to_string();
            if spelled.eq_ignore_ascii_case(TIMESTAMP_LTZ) {
                Some(ltz_timestamp_type())
            } else if spelled.eq_ignore_ascii_case(TIMESTAMP_NS) {
                Some(timestamp_ns_type(false))
            } else if spelled.eq_ignore_ascii_case(TIMESTAMPTZ_NS) {
                Some(timestamp_ns_type(true))
            } else {
                None
            }
        }
        _ => None,
    }
}

impl TypePlanner for SparkTypePlanner {
    fn plan_type_field(&self, sql_type: &SqlDataType) -> Result<Option<FieldRef>> {
        Ok(planned_type(sql_type)
            .map(|data_type| Arc::new(Field::new("", data_type, true)) as FieldRef))
    }
}

fn retargeted(expr: Box<Expr>, target: DataType, nullable: bool) -> Expr {
    Expr::Cast(Cast::new_from_field(
        expr,
        Arc::new(Field::new("ts", target, nullable)),
    ))
}

pub(crate) fn planned_instant_cast(
    cast: &Cast,
    source: &DataType,
    schema: &DFSchema,
    session_ntz: bool,
) -> Option<Expr> {
    if !is_ltz_timestamp(cast.field.data_type()) {
        return None;
    }
    let nullable = cast.expr.nullable(schema).unwrap_or(true);
    let target = if session_ntz {
        ntz_timestamp_type()
    } else {
        ltz_timestamp_type()
    };
    if source.is_integer() {
        let unit = if matches!(source, DataType::Int64) {
            TimeUnit::Second
        } else {
            TimeUnit::Nanosecond
        };
        let ticks = retargeted(cast.expr.clone(), DataType::Timestamp(unit, None), nullable);
        return Some(retargeted(Box::new(ticks), target, nullable));
    }
    if !session_ntz {
        return None;
    }
    match source {
        DataType::Null => Some(retargeted(cast.expr.clone(), target, nullable)),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(*cast.expr.clone()),
        _ => None,
    }
}

#[derive(Debug, Default)]
pub struct PlannedTimestampCast;

impl AnalyzerRule for PlannedTimestampCast {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    fn name(&self) -> &'static str {
        "planned_timestamp_cast"
    }
}

pub fn install_planned_timestamp_cast(ctx: &SessionContext) {
    ctx.add_analyzer_rule(Arc::new(PlannedTimestampCast));
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| Ok(rewrite_cast(node, &schema)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    if transformed.transformed {
        transformed.map_data(LogicalPlan::recompute_schema)
    } else {
        Ok(transformed)
    }
}

fn rewrite_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    let Ok(source) = cast.expr.get_type(schema) else {
        return Transformed::no(expr);
    };
    if !matches!(source, DataType::Int64) {
        return Transformed::no(expr);
    }
    match planned_instant_cast(cast, &source, schema, false) {
        Some(planned) => Transformed::yes(planned),
        None => Transformed::no(expr),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::ast::{Ident, ObjectName};

    use super::*;

    fn custom(name: &str) -> SqlDataType {
        SqlDataType::Custom(ObjectName::from(vec![Ident::new(name)]), Vec::new())
    }

    fn planned(sql_type: &SqlDataType) -> Option<DataType> {
        SparkTypePlanner
            .plan_type_field(sql_type)
            .unwrap_or_else(|error| panic!("plan {sql_type}: {error}"))
            .map(|field| field.data_type().clone())
    }

    #[test]
    fn timestamp_and_timestamp_ltz_plan_to_the_microsecond_instant() {
        let bare = SqlDataType::Timestamp(None, TimezoneInfo::None);
        assert_eq!(planned(&bare), Some(ltz_timestamp_type()));
        assert_eq!(
            planned(&custom("TIMESTAMP_LTZ")),
            Some(ltz_timestamp_type())
        );
    }

    #[test]
    fn timestamp_ntz_plans_to_the_naive_microsecond_wall() {
        assert_eq!(
            planned(&SqlDataType::TimestampNtz(None)),
            Some(ntz_timestamp_type())
        );
    }

    #[test]
    fn the_two_nanosecond_names_plan_to_nanoseconds() {
        assert_eq!(
            planned(&custom("timestamp_ns")),
            Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
        );
        assert_eq!(
            planned(&custom("TIMESTAMPTZ_NS")),
            Some(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::<str>::from("UTC"))
            ))
        );
    }

    #[test]
    fn every_other_name_is_left_to_datafusion() {
        for sql_type in [
            SqlDataType::Date,
            SqlDataType::Timestamp(Some(3), TimezoneInfo::None),
            SqlDataType::Timestamp(None, TimezoneInfo::WithTimeZone),
            SqlDataType::Timestamp(None, TimezoneInfo::Tz),
            SqlDataType::TimestampNtz(Some(6)),
            custom("timestamp_s"),
        ] {
            assert_eq!(planned(&sql_type), None, "{sql_type}");
        }
    }
}
