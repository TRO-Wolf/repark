use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Distinct, Expr, ExprSchemable, JoinType, LogicalPlan, TableScan};

use super::{is_timestamp_ns_cast, is_written_timestamp_ns_cast};

const MAX_DEPTH: usize = 1024;
const VIEW_DEPTH: usize = 32;

pub type ViewPlans<'a> = &'a dyn Fn(&TableScan) -> Option<LogicalPlan>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origins {
    Absent,
    Asked,
    Unasked,
}

impl Origins {
    fn with(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unasked, _) | (_, Self::Unasked) => Self::Unasked,
            (Self::Asked, _) | (_, Self::Asked) => Self::Asked,
            (Self::Absent, Self::Absent) => Self::Absent,
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn asked_nanosecond_columns(plan: &LogicalPlan, views: ViewPlans<'_>) -> Result<Vec<bool>> {
    plan.schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if holds_nanoseconds(field.data_type()) {
                Ok(column_origins(plan, index, 0, views)? == Origins::Asked)
            } else {
                Ok(false)
            }
        })
        .collect()
}

#[allow(clippy::missing_errors_doc)]
pub fn asked_nanosecond_columns_in(
    state: &SessionState,
    plan: &LogicalPlan,
    views: ViewPlans<'_>,
) -> Result<Vec<bool>> {
    let fields = plan.schema().fields();
    if !fields
        .iter()
        .any(|field| holds_nanoseconds(field.data_type()))
    {
        return Ok(vec![false; fields.len()]);
    }
    asked_nanosecond_columns(&crate::analyze_eagerly(state, plan.clone())?, views)
}

fn holds_nanoseconds(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => holds_nanoseconds(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| holds_nanoseconds(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| holds_nanoseconds(field.data_type())),
        DataType::Dictionary(_, values) => holds_nanoseconds(values),
        DataType::RunEndEncoded(_, values) => holds_nanoseconds(values.data_type()),
        _ => false,
    }
}

fn column_origins(
    plan: &LogicalPlan,
    index: usize,
    depth: usize,
    views: ViewPlans<'_>,
) -> Result<Origins> {
    if depth >= MAX_DEPTH {
        return Ok(Origins::Unasked);
    }
    let below = depth + 1;
    match plan {
        LogicalPlan::Projection(projection) => output_origins(
            projection.expr.get(index),
            Some(&projection.input),
            below,
            views,
        ),
        LogicalPlan::Aggregate(aggregate) => {
            if aggregate
                .group_expr
                .iter()
                .any(|expr| matches!(expr, Expr::GroupingSet(_)))
            {
                return Ok(Origins::Unasked);
            }
            let mut output = aggregate.group_expr.iter().chain(&aggregate.aggr_expr);
            output_origins(output.nth(index), Some(&aggregate.input), below, views)
        }
        LogicalPlan::Window(window) => {
            let carried = window.input.schema().fields().len();
            match index.checked_sub(carried) {
                Some(added) => output_origins(
                    window.window_expr.get(added),
                    Some(&window.input),
                    below,
                    views,
                ),
                None => column_origins(&window.input, index, below, views),
            }
        }
        LogicalPlan::SubqueryAlias(alias) => column_origins(&alias.input, index, below, views),
        LogicalPlan::Filter(filter) => column_origins(&filter.input, index, below, views),
        LogicalPlan::Sort(sort) => column_origins(&sort.input, index, below, views),
        LogicalPlan::Limit(limit) => column_origins(&limit.input, index, below, views),
        LogicalPlan::Distinct(Distinct::All(input)) => column_origins(input, index, below, views),
        LogicalPlan::Repartition(repartition) => {
            column_origins(&repartition.input, index, below, views)
        }
        LogicalPlan::Union(union) => {
            let mut origins = Origins::Absent;
            for input in &union.inputs {
                origins = origins.with(column_origins(input, index, below, views)?);
            }
            Ok(origins)
        }
        LogicalPlan::Values(values) => {
            let mut origins = Origins::Absent;
            for row in &values.values {
                origins = origins.with(output_origins(row.get(index), None, below, views)?);
            }
            Ok(origins)
        }
        LogicalPlan::Join(join) => {
            let left = match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    join.left.schema().fields().len()
                }
                JoinType::LeftSemi | JoinType::LeftAnti => usize::MAX,
                JoinType::RightSemi | JoinType::RightAnti => 0,
                JoinType::LeftMark | JoinType::RightMark => return Ok(Origins::Unasked),
            };
            match index.checked_sub(left) {
                Some(right) => column_origins(&join.right, right, below, views),
                None => column_origins(&join.left, index, below, views),
            }
        }
        LogicalPlan::TableScan(scan) => scan_origins(scan, index, depth, views),
        LogicalPlan::Distinct(Distinct::On(_))
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::Statement(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Extension(_)
        | LogicalPlan::Dml(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Copy(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::Unnest(_)
        | LogicalPlan::RecursiveQuery(_) => Ok(Origins::Unasked),
    }
}

#[inline(never)]
fn scan_origins(
    scan: &TableScan,
    index: usize,
    depth: usize,
    views: ViewPlans<'_>,
) -> Result<Origins> {
    let column = match &scan.projection {
        Some(projection) => projection.get(index).copied(),
        None => Some(index),
    };
    let view = match scan.source.get_logical_plan() {
        Some(plan) => Some(Box::new(plan.into_owned())),
        None => views(scan).map(Box::new),
    };
    match (view, column) {
        (Some(view), Some(column)) => column_origins(&view, column, depth + VIEW_DEPTH, views),
        _ => Ok(Origins::Unasked),
    }
}

fn output_origins(
    expr: Option<&Expr>,
    input: Option<&LogicalPlan>,
    depth: usize,
    views: ViewPlans<'_>,
) -> Result<Origins> {
    match expr {
        Some(expr) => expr_origins(expr, input, depth, views),
        None => Ok(Origins::Unasked),
    }
}

fn expr_origins(
    expr: &Expr,
    input: Option<&LogicalPlan>,
    depth: usize,
    views: ViewPlans<'_>,
) -> Result<Origins> {
    if depth >= MAX_DEPTH {
        return Ok(Origins::Unasked);
    }
    let empty = DFSchema::empty();
    let schema = input.map_or(&empty, |plan| plan.schema().as_ref());
    let Ok(data_type) = expr.get_type(schema) else {
        return Ok(Origins::Unasked);
    };
    if !holds_nanoseconds(&data_type) {
        return Ok(Origins::Absent);
    }
    match expr {
        Expr::Alias(alias) => expr_origins(&alias.expr, input, depth + 1, views),
        Expr::Column(column) => match input {
            Some(plan) => column_origins(
                plan,
                plan.schema().index_of_column(column)?,
                depth + 1,
                views,
            ),
            None => Ok(Origins::Unasked),
        },
        _ if is_written_timestamp_ns_cast(expr) => Ok(Origins::Asked),
        _ if is_timestamp_ns_cast(expr) => child_origins(expr, input, depth + 1, views),
        _ => match child_origins(expr, input, depth + 1, views)? {
            Origins::Absent => Ok(Origins::Unasked),
            found => Ok(found),
        },
    }
}

fn child_origins(
    expr: &Expr,
    input: Option<&LogicalPlan>,
    depth: usize,
    views: ViewPlans<'_>,
) -> Result<Origins> {
    let mut origins = Origins::Absent;
    expr.apply_children(|child| {
        origins = origins.with(expr_origins(child, input, depth, views)?);
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(origins)
}
