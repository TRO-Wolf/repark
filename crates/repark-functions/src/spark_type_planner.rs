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

#[derive(Debug, Default, Clone, Copy)]
pub struct AnsiTypePlanner;

#[must_use]
pub fn ansi_type_planner() -> Arc<dyn TypePlanner> {
    Arc::new(AnsiTypePlanner)
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

fn ansi_planned_type(sql_type: &SqlDataType) -> Option<DataType> {
    match sql_type {
        SqlDataType::Timestamp(None, TimezoneInfo::None) => Some(ntz_timestamp_type()),
        SqlDataType::Timestamp(None, TimezoneInfo::Tz) => Some(ltz_timestamp_type()),
        _ => planned_type(sql_type),
    }
}

fn planned_field(data_type: Option<DataType>) -> Option<FieldRef> {
    data_type.map(|data_type| Arc::new(Field::new("", data_type, true)) as FieldRef)
}

impl TypePlanner for SparkTypePlanner {
    fn plan_type_field(&self, sql_type: &SqlDataType) -> Result<Option<FieldRef>> {
        Ok(planned_field(planned_type(sql_type)))
    }
}

impl TypePlanner for AnsiTypePlanner {
    fn plan_type_field(&self, sql_type: &SqlDataType) -> Result<Option<FieldRef>> {
        Ok(planned_field(ansi_planned_type(sql_type)))
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
        let seconds = retargeted(
            cast.expr.clone(),
            DataType::Timestamp(TimeUnit::Second, None),
            nullable,
        );
        return Some(retargeted(Box::new(seconds), target, nullable));
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
pub struct PlannedTimestampCast {
    naive_seconds: bool,
}

impl AnalyzerRule for PlannedTimestampCast {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|node| rewrite_plan(node, self.naive_seconds))
            .data()
    }

    fn name(&self) -> &'static str {
        "planned_timestamp_cast"
    }
}

pub fn install_planned_timestamp_cast(ctx: &SessionContext) {
    ctx.add_analyzer_rule(Arc::new(PlannedTimestampCast::default()));
}

pub fn install_ansi_planned_timestamp_cast(ctx: &SessionContext) {
    ctx.add_analyzer_rule(Arc::new(PlannedTimestampCast {
        naive_seconds: true,
    }));
}

fn rewrite_plan(plan: LogicalPlan, naive_seconds: bool) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| {
            rewrite_cast(node, &schema, naive_seconds).transform_data(|typed| {
                crate::timestamp_ns_cast::wall_kernel_by_target(typed, &schema)
            })
        })?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    if transformed.transformed {
        transformed.map_data(LogicalPlan::recompute_schema)
    } else {
        Ok(transformed)
    }
}

fn naive_seconds_cast(cast: &Cast, schema: &DFSchema) -> Option<Expr> {
    if cast.field.data_type() != &ntz_timestamp_type() {
        return None;
    }
    let nullable = cast.expr.nullable(schema).unwrap_or(true);
    let seconds = retargeted(
        cast.expr.clone(),
        DataType::Timestamp(TimeUnit::Second, None),
        nullable,
    );
    Some(retargeted(
        Box::new(seconds),
        ntz_timestamp_type(),
        nullable,
    ))
}

fn rewrite_cast(expr: Expr, schema: &DFSchema, naive_seconds: bool) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    let Ok(source) = cast.expr.get_type(schema) else {
        return Transformed::no(expr);
    };
    if !matches!(source, DataType::Int64) {
        return Transformed::no(expr);
    }
    let planned = planned_instant_cast(cast, &source, schema, false).or_else(|| {
        naive_seconds
            .then(|| naive_seconds_cast(cast, schema))
            .flatten()
    });
    match planned {
        Some(planned) => Transformed::yes(planned),
        None => Transformed::no(expr),
    }
}

#[cfg(test)]
pub(crate) fn planned_context(config: datafusion::prelude::SessionConfig) -> SessionContext {
    let state = datafusion::execution::SessionStateBuilder::new()
        .with_type_planner(spark_type_planner())
        .with_config(config)
        .with_default_features()
        .build();
    SessionContext::new_with_state(state)
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

    fn ansi_planned(sql_type: &SqlDataType) -> Option<DataType> {
        AnsiTypePlanner
            .plan_type_field(sql_type)
            .unwrap_or_else(|error| panic!("plan {sql_type}: {error}"))
            .map(|field| field.data_type().clone())
    }

    #[test]
    fn the_ansi_variant_plans_timestamp_as_the_wall_and_timestamptz_as_the_instant() {
        assert_eq!(
            ansi_planned(&SqlDataType::Timestamp(None, TimezoneInfo::None)),
            Some(ntz_timestamp_type())
        );
        assert_eq!(
            ansi_planned(&SqlDataType::Timestamp(None, TimezoneInfo::Tz)),
            Some(ltz_timestamp_type())
        );
        for sql_type in [
            SqlDataType::TimestampNtz(None),
            custom("TIMESTAMP_LTZ"),
            custom("timestamp_ns"),
            custom("timestamptz_ns"),
            SqlDataType::Timestamp(Some(3), TimezoneInfo::None),
            SqlDataType::Timestamp(None, TimezoneInfo::WithTimeZone),
            SqlDataType::Date,
        ] {
            assert_eq!(ansi_planned(&sql_type), planned(&sql_type), "{sql_type}");
        }
    }
}
