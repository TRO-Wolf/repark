use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema};
use datafusion::datasource::source_as_provider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::utils::grouping_set_to_exprlist;
use datafusion::logical_expr::{
    Aggregate, Cast, Distinct, Expr, ExprSchemable, Join, JoinType, LogicalPlan, Operator,
    TableScan,
};
use datafusion::prelude::SessionContext;

use super::negated_null_store::ViewDefinitionPlans;

mod text;

pub const NARROWED_BESIDE_NULL_UDF_NAME: &str = "__repark_narrowed_beside_null__";
pub const NARROWED_BESIDE_VALUE_UDF_NAME: &str = "__repark_narrowed_beside_value__";
pub const NARROW_TIMESTAMP_NS_UDF_NAME: &str = "__repark_narrow_timestamp_ns__";
pub const WRITTEN_CAST_UDF_NAME: &str = "__repark_written_timestamp_cast__";
const DATE_TRUNC_UDF_NAME: &str = "date_trunc";
const TIMESTAMP_TO_DATE_UDF_NAME: &str = "__repark_timestamp_to_date__";
const COALESCE_UDF_NAME: &str = "coalesce";
const MAP_UDF_NAMES: [&str; 2] = ["map", "map_from_arrays"];
const WRAPPING_UDF_NAMES: [&str; 2] = ["__repark_spark_nonnull__", "__repark_hof_array_field__"];
const MAP_KEYS_UDF_NAME: &str = "map_keys";
const MAP_VALUES_UDF_NAME: &str = "map_values";
const DATE_UDF_NAMES: [&str; 2] = ["to_date", "date"];
const UNNAMED_TABLE: &str = "the table ``: Cannot safely cast";
const NARROWED: &str = "narrowed from nanoseconds to microseconds";
const SELECTING_UDF_NAMES: [&str; 7] = [
    "array_element",
    "__repark_array_get__",
    "element_at",
    "try_element_at",
    "get_field",
    "get",
    "map_extract",
];

#[allow(clippy::missing_errors_doc)]
pub fn refuse_narrowed_ns_writes<'a>(
    ctx: &SessionContext,
    table: &str,
    plan: &LogicalPlan,
    targets: impl IntoIterator<Item = (&'a str, &'a DataType)>,
) -> Result<()> {
    let targets: Vec<(&str, &DataType)> = targets.into_iter().collect();
    if !targets
        .iter()
        .any(|(_, target)| nanosecond_target(target).is_some())
    {
        return Ok(());
    }
    let state = ctx.state();
    let Ok(analyzed) =
        state
            .analyzer()
            .execute_and_check(plan.clone(), state.config_options(), |_, _| {})
    else {
        return Ok(());
    };
    let views = state.config().get_extension::<ViewDefinitionPlans>();
    refuse_narrowed_ns_columns(table, &analyzed, targets, views.as_deref())
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_narrowed_ns_columns<'a>(
    table: &str,
    analyzed: &LogicalPlan,
    targets: impl IntoIterator<Item = (&'a str, &'a DataType)>,
    views: Option<&ViewDefinitionPlans>,
) -> Result<()> {
    let targets: Vec<(&str, &DataType)> = targets.into_iter().collect();
    if analyzed.schema().fields().len() != targets.len() {
        return Ok(());
    }
    for (index, (column, target)) in targets.into_iter().enumerate() {
        let Some(zoned) = nanosecond_target(target) else {
            continue;
        };
        if let Some(narrowing) = column_narrowed(analyzed, index, views) {
            return Err(narrowed_refusal(table, column, zoned, &narrowing));
        }
    }
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_narrowed_ns_inserts<'a>(
    table: &str,
    analyzed: &LogicalPlan,
    targets: impl IntoIterator<Item = (&'a str, &'a DataType)>,
    views: Option<&ViewDefinitionPlans>,
) -> Result<()> {
    let LogicalPlan::Projection(conforming) = analyzed else {
        return refuse_narrowed_ns_columns(table, analyzed, targets, views);
    };
    let targets: Vec<(&str, &DataType)> = targets.into_iter().collect();
    if conforming.expr.len() != targets.len() {
        return Ok(());
    }
    for (index, (column, target)) in targets.into_iter().enumerate() {
        let Some(zoned) = nanosecond_target(target) else {
            continue;
        };
        let narrowing = match conformed(&conforming.expr[index]) {
            Some(source) => (conforming.input.schema().index_of_column(source).ok())
                .and_then(|position| column_narrowed(&conforming.input, position, views)),
            None => column_narrowed(analyzed, index, views),
        };
        if let Some(narrowing) = narrowing {
            return Err(narrowed_refusal(table, column, zoned, &narrowing));
        }
    }
    Ok(())
}

fn peeled(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => peeled(&alias.expr),
        other => other,
    }
}

fn conformed(expr: &Expr) -> Option<&Column> {
    let source = match expr {
        Expr::Alias(alias) => return conformed(&alias.expr),
        Expr::Cast(cast) => cast.expr.as_ref(),
        Expr::ScalarFunction(function) if written_cast(function) => function.args.first()?,
        _ => return None,
    };
    match source {
        Expr::Column(column) => Some(column),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Narrowing {
    pub value: String,
    pub element_of: Option<String>,
    pub beside_untyped_null: bool,
    pub zoned: bool,
}

#[must_use]
pub fn name_table(error: DataFusionError, table: &str) -> DataFusionError {
    match error {
        DataFusionError::Plan(text) if text.contains(UNNAMED_TABLE) && text.contains(NARROWED) => {
            let named = format!("the table {table}: Cannot safely cast");
            DataFusionError::Plan(text.replacen(UNNAMED_TABLE, &named, 1))
        }
        DataFusionError::Context(context, inner) => {
            DataFusionError::Context(context, Box::new(name_table(*inner, table)))
        }
        other => other,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    Opaque,
    Timestamp,
    Trunc,
    Date,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cut {
    Instant,
    Naive,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    Whole,
    Keys,
    Values,
}

#[derive(Clone, Copy)]
struct Scope<'a> {
    plan: &'a LogicalPlan,
    outer: Option<&'a Scope<'a>>,
}

#[derive(Clone, Copy)]
struct Walk<'a> {
    views: Option<&'a ViewDefinitionPlans>,
    outer: Option<&'a Scope<'a>>,
    ranged: Option<&'a Expr>,
    nearest: Call,
    part: Part,
}

impl<'a> Walk<'a> {
    fn under(self, nearest: Call) -> Self {
        Self { nearest, ..self }
    }

    fn of(self, part: Part) -> Self {
        Self { part, ..self }
    }

    fn within(self, outer: Option<&'a Scope<'a>>) -> Self {
        Self { outer, ..self }
    }
}

#[must_use]
pub fn narrowed_refusal(
    table: &str,
    column: &str,
    zoned: bool,
    narrowing: &Narrowing,
) -> DataFusionError {
    let (upper, lower) = if zoned {
        ("TIMESTAMPTZ_NS", "timestamptz_ns")
    } else {
        ("TIMESTAMP_NS", "timestamp_ns")
    };
    let head = format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table {table}: Cannot safely cast `{column}` \"TIMESTAMP\" to \"{upper}\"."
    );
    if narrowing.beside_untyped_null {
        return DataFusionError::Plan(format!(
            "{head} The value was narrowed from nanoseconds to microseconds before the store; \
             give the NULL beside it the type {lower}. SQLSTATE: KD000"
        ));
    }
    let value = &narrowing.value;
    let named = match &narrowing.element_of {
        Some(ranged) => format!("{value}, an element of {ranged},"),
        None => value.clone(),
    };
    DataFusionError::Plan(format!(
        "{head} The value {named} was narrowed from nanoseconds to microseconds before the \
         store, to match the microsecond value beside it; write CAST({value} AS TIMESTAMP) if \
         microseconds are intended, or give the value beside it a nanosecond type. SQLSTATE: \
         KD000"
    ))
}

fn nanosecond_target(target: &DataType) -> Option<bool> {
    match target {
        DataType::Timestamp(TimeUnit::Nanosecond, zone) => Some(zone.is_some()),
        _ => None,
    }
}

#[must_use]
pub fn column_narrowed(
    plan: &LogicalPlan,
    index: usize,
    views: Option<&ViewDefinitionPlans>,
) -> Option<Narrowing> {
    let walk = Walk {
        views,
        outer: None,
        ranged: None,
        nearest: Call::Opaque,
        part: Part::Whole,
    };
    column_lineage(plan, index, walk)
}

fn column_lineage(plan: &LogicalPlan, index: usize, walk: Walk<'_>) -> Option<Narrowing> {
    let below = |input: &LogicalPlan, index: usize| column_lineage(input, index, walk);
    let of = |expr: &Expr, input: &LogicalPlan| expr_lineage(expr, Some(input), walk);
    match plan {
        LogicalPlan::Projection(projection) => of(projection.expr.get(index)?, &projection.input),
        LogicalPlan::SubqueryAlias(alias) => below(&alias.input, index),
        LogicalPlan::Subquery(subquery) => below(&subquery.subquery, index),
        LogicalPlan::Filter(filter) => below(&filter.input, index),
        LogicalPlan::Sort(sort) => below(&sort.input, index),
        LogicalPlan::Limit(limit) => below(&limit.input, index),
        LogicalPlan::Repartition(repartition) => below(&repartition.input, index),
        LogicalPlan::Distinct(Distinct::All(input)) => below(input, index),
        LogicalPlan::Distinct(Distinct::On(on)) => of(on.select_expr.get(index)?, &on.input),
        LogicalPlan::Values(values) => values.values.iter().find_map(|row| {
            row.get(index)
                .and_then(|expr| expr_lineage(expr, None, walk))
        }),
        LogicalPlan::Union(union) => {
            let mut narrowed = union.inputs.iter().filter_map(|input| below(input, index));
            let first = narrowed.next()?;
            if first.beside_untyped_null {
                return Some(first);
            }
            narrowed
                .find(|narrowing| narrowing.beside_untyped_null)
                .or(Some(first))
        }
        LogicalPlan::Join(join) => join_lineage(join, index, walk),
        LogicalPlan::Aggregate(aggregate) => aggregate_lineage(aggregate, index, walk),
        LogicalPlan::Window(window) => {
            let carried = window.input.schema().fields().len();
            if index < carried {
                below(&window.input, index)
            } else {
                of(window.window_expr.get(index - carried)?, &window.input)
            }
        }
        LogicalPlan::Unnest(unnest) => below(
            &unnest.input,
            unnest.dependency_indices.get(index).copied()?,
        ),
        LogicalPlan::RecursiveQuery(recursive) => {
            below(&recursive.static_term, index).or_else(|| below(&recursive.recursive_term, index))
        }
        LogicalPlan::TableScan(scan) => scan_lineage(scan, index, walk),
        other => {
            let schema = other.schema();
            if index >= schema.fields().len() {
                return None;
            }
            let (qualifier, field) = schema.qualified_field(index);
            other.inputs().into_iter().find_map(|input| {
                let position = input
                    .schema()
                    .index_of_column_by_name(qualifier, field.name())?;
                below(input, position)
            })
        }
    }
}

fn join_lineage(join: &Join, index: usize, walk: Walk<'_>) -> Option<Narrowing> {
    let left_width = join.left.schema().fields().len();
    let beside_left = Scope {
        plan: &join.left,
        outer: walk.outer,
    };
    let left = |index: usize| column_lineage(&join.left, index, walk);
    let right = |index: usize| column_lineage(&join.right, index, walk.within(Some(&beside_left)));
    match join.join_type {
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            if index < left_width {
                left(index)
            } else {
                right(index - left_width)
            }
        }
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            (index < left_width).then(|| left(index))?
        }
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            (index < join.right.schema().fields().len()).then(|| right(index))?
        }
    }
}

fn aggregate_lineage(aggregate: &Aggregate, index: usize, walk: Walk<'_>) -> Option<Narrowing> {
    let of = |expr: &Expr| expr_lineage(expr, Some(&aggregate.input), walk);
    let grouped = grouping_set_to_exprlist(&aggregate.group_expr).ok()?;
    let grouping_sets = matches!(aggregate.group_expr.as_slice(), [Expr::GroupingSet(_)]);
    let computed = grouped.len() + usize::from(grouping_sets);
    if index < grouped.len() {
        of(grouped[index])
    } else if index < computed {
        None
    } else {
        of(aggregate.aggr_expr.get(index - computed)?)
    }
}

fn scan_lineage(scan: &TableScan, index: usize, walk: Walk<'_>) -> Option<Narrowing> {
    let source_index = match &scan.projection {
        Some(projection) => projection.get(index).copied()?,
        None => index,
    };
    if let Some(source) = scan.source.get_logical_plan() {
        return column_lineage(&source, source_index, walk);
    }
    let source = (walk.views)
        .zip(source_as_provider(&scan.source).ok())
        .and_then(|(views, provider)| views.definition_plan(provider.as_ref()))?;
    column_lineage(&source, source_index, walk)
}

fn expr_lineage(expr: &Expr, input: Option<&LogicalPlan>, walk: Walk<'_>) -> Option<Narrowing> {
    let carried = |branch: &Expr| expr_lineage(branch, input, walk);
    let whole = walk.of(Part::Whole);
    let opaque = |branch: &Expr| expr_lineage(branch, input, whole.under(Call::Opaque));
    match expr {
        Expr::Alias(alias) => carried(&alias.expr),
        Expr::Column(column) => {
            let input = input?;
            let position = input.schema().index_of_column(column).ok()?;
            column_lineage(input, position, walk)
        }
        Expr::OuterReferenceColumn(_, column) => outer_lineage(column, walk),
        Expr::ScalarSubquery(subquery) => {
            let enclosing = input.map(|plan| Scope {
                plan,
                outer: walk.outer,
            });
            let outer = enclosing.as_ref().or(walk.outer);
            column_lineage(&subquery.subquery, 0, walk.within(outer))
        }
        Expr::Cast(cast) => cast_lineage(cast, input, walk),
        Expr::TryCast(cast) => {
            let nearest = written_call(cast.field.data_type());
            expr_lineage(&cast.expr, input, walk.under(nearest))
        }
        Expr::ScalarFunction(function) => function_lineage(expr, function, input, walk),
        Expr::Case(case) => (case.when_then_expr.iter())
            .find_map(|(_, then)| carried(then))
            .or_else(|| case.else_expr.as_deref().and_then(carried)),
        Expr::HigherOrderFunction(function) => {
            let ranged = function
                .args
                .iter()
                .find(|argument| !matches!(argument, Expr::Lambda(_)));
            let within = Walk {
                ranged,
                nearest: Call::Opaque,
                ..whole
            };
            function.args.iter().find_map(|argument| match argument {
                Expr::Lambda(lambda) => expr_lineage(&lambda.body, input, within),
                value => opaque(value),
            })
        }
        Expr::AggregateFunction(aggregate) => aggregate.params.args.iter().find_map(opaque),
        Expr::WindowFunction(window) => window.params.args.iter().find_map(opaque),
        Expr::BinaryExpr(binary) => {
            if compares(binary.op) {
                return None;
            }
            let point = |side: &Expr| expr_type(side, input).is_ok_and(|kind| dated(&kind));
            match (point(&binary.left), point(&binary.right)) {
                (true, false) => opaque(&binary.left),
                (false, true) => opaque(&binary.right),
                _ => opaque(&binary.left).or_else(|| opaque(&binary.right)),
            }
        }
        Expr::Negative(value) => opaque(value),
        Expr::Unnest(unnest) => opaque(&unnest.expr),
        Expr::Not(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Between(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_)
        | Expr::InList(_)
        | Expr::InSubquery(_)
        | Expr::Exists(_)
        | Expr::Literal(_, _) => None,
        other => {
            let mut narrowed = None;
            let walked = other.apply_children(|branch| {
                narrowed = opaque(branch);
                Ok(if narrowed.is_some() {
                    TreeNodeRecursion::Stop
                } else {
                    TreeNodeRecursion::Continue
                })
            });
            walked.ok().and(narrowed)
        }
    }
}

fn written_call(target: &DataType) -> Call {
    match target {
        DataType::Timestamp(unit, Some(_)) if *unit != TimeUnit::Nanosecond => Call::Timestamp,
        DataType::Date32 => Call::Date,
        _ => Call::Opaque,
    }
}

fn dated(kind: &DataType) -> bool {
    matches!(
        kind,
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
    )
}

fn cast_lineage(cast: &Cast, input: Option<&LogicalPlan>, walk: Walk<'_>) -> Option<Narrowing> {
    let target = cast.field.data_type();
    if let Some(marked) = mark_of(&cast.expr) {
        let source = expr_type(marked.0, input).ok();
        if source.is_some_and(|source| part_kept(&source, target, walk.part)) {
            return expr_lineage(marked.0, input, walk);
        }
        return found(marked, cut_of(target), input, walk);
    }
    expr_lineage(&cast.expr, input, walk.under(written_call(target)))
}

fn part_kept(source: &DataType, target: &DataType, part: Part) -> bool {
    let (DataType::Map(from, _), DataType::Map(to, _)) = (source, target) else {
        return false;
    };
    let (DataType::Struct(from), DataType::Struct(to)) = (from.data_type(), to.data_type()) else {
        return false;
    };
    let index = match part {
        Part::Whole => return false,
        Part::Keys => 0,
        Part::Values => 1,
    };
    match (from.get(index), to.get(index)) {
        (Some(from), Some(to)) => from.data_type() == to.data_type(),
        _ => false,
    }
}

fn function_lineage(
    expr: &Expr,
    function: &ScalarFunction,
    input: Option<&LogicalPlan>,
    walk: Walk<'_>,
) -> Option<Narrowing> {
    let whole = walk.of(Part::Whole);
    let through = |branch: &Expr, nearest: Call| expr_lineage(branch, input, whole.under(nearest));
    let name = function.func.name();
    let args = function.args.as_slice();
    if let Some(marked) = mark_of(expr) {
        return found(marked, Cut::Other, input, walk);
    }
    if name == NARROW_TIMESTAMP_NS_UDF_NAME {
        let value = args.first()?;
        if let Some(beside_untyped_null) = narrowed_beside(function) {
            return found((value, beside_untyped_null), Cut::Instant, input, walk);
        }
        return match mark_of(value) {
            Some(marked) => found(marked, Cut::Instant, input, walk),
            None => through(value, Call::Timestamp),
        };
    }
    if name == WRITTEN_CAST_UDF_NAME {
        return through(args.first()?, Call::Timestamp);
    }
    if WRAPPING_UDF_NAMES.contains(&name) {
        return expr_lineage(args.first()?, input, walk);
    }
    if SELECTING_UDF_NAMES.contains(&name) {
        return expr_lineage(
            args.first()?,
            input,
            walk.under(Call::Opaque).of(Part::Values),
        );
    }
    if name == MAP_KEYS_UDF_NAME || name == MAP_VALUES_UDF_NAME {
        let part = if name == MAP_KEYS_UDF_NAME {
            Part::Keys
        } else {
            Part::Values
        };
        return expr_lineage(args.first()?, input, walk.under(Call::Opaque).of(part));
    }
    if let (true, [keys, values]) = (MAP_UDF_NAMES.contains(&name), args) {
        return match walk.part {
            Part::Keys => through(keys, Call::Opaque),
            Part::Values => through(values, Call::Opaque),
            Part::Whole => through(keys, Call::Opaque).or_else(|| through(values, Call::Opaque)),
        };
    }
    let kind = expr_type(expr, input);
    if matches!(kind, Ok(DataType::Boolean)) {
        return None;
    }
    let nearest = match name {
        DATE_TRUNC_UDF_NAME => Call::Trunc,
        TIMESTAMP_TO_DATE_UDF_NAME => Call::Date,
        name if DATE_UDF_NAMES.contains(&name) && args.len() == 1 => Call::Date,
        COALESCE_UDF_NAME => walk.nearest,
        _ => Call::Opaque,
    };
    let point = |value: &Expr| expr_type(value, input).is_ok_and(|kind| dated(&kind));
    let passes = kind.is_ok_and(|kind| dated(&kind)) && args.iter().any(point);
    args.iter()
        .filter(|value| !passes || point(value))
        .find_map(|value| through(value, nearest))
}

fn outer_lineage(column: &Column, walk: Walk<'_>) -> Option<Narrowing> {
    let mut scope = walk.outer;
    while let Some(enclosing) = scope {
        if let Ok(position) = enclosing.plan.schema().index_of_column(column) {
            return column_lineage(enclosing.plan, position, walk.within(enclosing.outer));
        }
        scope = enclosing.outer;
    }
    None
}

fn compares(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::And
            | Operator::Or
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
            | Operator::LikeMatch
            | Operator::ILikeMatch
            | Operator::NotLikeMatch
            | Operator::NotILikeMatch
            | Operator::RegexMatch
            | Operator::RegexIMatch
            | Operator::RegexNotMatch
            | Operator::RegexNotIMatch
    )
}

fn written_cast(function: &ScalarFunction) -> bool {
    match function.func.name() {
        WRITTEN_CAST_UDF_NAME => true,
        NARROW_TIMESTAMP_NS_UDF_NAME => narrowed_beside(function).is_none(),
        _ => false,
    }
}

fn narrowed_beside(kernel: &ScalarFunction) -> Option<bool> {
    match kernel.func.aliases().first().map(String::as_str) {
        Some(NARROWED_BESIDE_NULL_UDF_NAME) => Some(true),
        Some(NARROWED_BESIDE_VALUE_UDF_NAME) => Some(false),
        _ => None,
    }
}

fn mark_of(expr: &Expr) -> Option<(&Expr, bool)> {
    let Expr::ScalarFunction(mark) = expr else {
        return None;
    };
    let beside_untyped_null = match mark.func.name() {
        NARROWED_BESIDE_NULL_UDF_NAME => true,
        NARROWED_BESIDE_VALUE_UDF_NAME => false,
        _ => return None,
    };
    Some((mark.args.first()?, beside_untyped_null))
}

fn cut_of(target: &DataType) -> Cut {
    match target {
        DataType::Timestamp(TimeUnit::Nanosecond, None) | DataType::Timestamp(_, Some(_)) => {
            Cut::Instant
        }
        DataType::Timestamp(_, None) => Cut::Naive,
        _ => Cut::Other,
    }
}

fn found(
    (value, beside_untyped_null): (&Expr, bool),
    cut: Cut,
    input: Option<&LogicalPlan>,
    walk: Walk<'_>,
) -> Option<Narrowing> {
    let zoned = match expr_type(value, input) {
        Ok(DataType::Timestamp(_, zone)) => Some(zone.is_some()),
        _ => None,
    };
    let idle = cuts_nothing(value, input, walk);
    let forgiven = matches!(
        (walk.nearest, cut, zoned),
        (Call::Timestamp, Cut::Instant, Some(_))
            | (Call::Trunc, Cut::Instant, Some(true))
            | (Call::Date, Cut::Instant, Some(false))
    );
    if idle || forgiven {
        return expr_lineage(value, input, walk);
    }
    let element_of = match value {
        Expr::LambdaVariable(_) => walk.ranged.map(text::sql_text),
        _ => None,
    };
    Some(Narrowing {
        value: text::sql_text(value),
        element_of,
        beside_untyped_null,
        zoned: zoned.unwrap_or(false),
    })
}

fn cuts_nothing(value: &Expr, input: Option<&LogicalPlan>, walk: Walk<'_>) -> bool {
    if stated_type(value, input).is_some_and(|kind| !nanoseconds(&kind)) {
        return true;
    }
    let (Expr::Column(column), Some(plan)) = (peeled(value), input) else {
        return false;
    };
    (plan.schema().index_of_column(column).ok())
        .is_some_and(|index| column_cuts_nothing(plan, index, walk))
}

fn column_cuts_nothing(plan: &LogicalPlan, index: usize, walk: Walk<'_>) -> bool {
    let below = |input: &LogicalPlan| column_cuts_nothing(input, index, walk);
    match plan {
        LogicalPlan::Projection(projection) => (projection.expr.get(index))
            .is_some_and(|expr| cuts_nothing(expr, Some(&projection.input), walk)),
        LogicalPlan::SubqueryAlias(alias) => below(&alias.input),
        LogicalPlan::Filter(filter) => below(&filter.input),
        LogicalPlan::Sort(sort) => below(&sort.input),
        LogicalPlan::Limit(limit) => below(&limit.input),
        LogicalPlan::Distinct(Distinct::All(input)) => below(input),
        LogicalPlan::TableScan(scan) => {
            let source_index = match &scan.projection {
                Some(projection) => projection.get(index).copied(),
                None => Some(index),
            };
            let held = scan.source.get_logical_plan().map(|plan| plan.into_owned());
            let defined = || {
                (walk.views)
                    .zip(source_as_provider(&scan.source).ok())
                    .and_then(|(views, provider)| views.definition_plan(provider.as_ref()))
            };
            let view = held.or_else(defined);
            (source_index.zip(view))
                .is_some_and(|(index, view)| column_cuts_nothing(&view, index, walk))
        }
        _ => false,
    }
}

fn stated_type(value: &Expr, input: Option<&LogicalPlan>) -> Option<DataType> {
    let tried = |node: &Expr| {
        matches!(node, Expr::TryCast(cast)
            if cast.field.data_type() == &DataType::Timestamp(TimeUnit::Nanosecond, None))
    };
    if !value.exists(|node| Ok(tried(node))).unwrap_or(false) {
        return expr_type(value, input).ok();
    }
    let read = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let stated = value.clone().transform_up(|node| {
        Ok(match node {
            Expr::TryCast(cast) if tried(&Expr::TryCast(cast.clone())) => {
                Transformed::yes(Expr::Cast(Cast::new(cast.expr, read.clone())))
            }
            other => Transformed::no(other),
        })
    });
    expr_type(&stated.ok()?.data, input).ok()
}

fn nanoseconds(kind: &DataType) -> bool {
    match kind {
        DataType::Timestamp(unit, _) => *unit == TimeUnit::Nanosecond,
        DataType::Struct(fields) => fields.iter().any(|field| nanoseconds(field.data_type())),
        DataType::Map(field, _)
        | DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _) => nanoseconds(field.data_type()),
        _ => false,
    }
}

fn expr_type(expr: &Expr, input: Option<&LogicalPlan>) -> Result<DataType> {
    match input {
        Some(input) => expr.get_type(input.schema().as_ref()),
        None => expr.get_type(&DFSchema::empty()),
    }
}

#[cfg(test)]
mod tests;
