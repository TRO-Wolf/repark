use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Column, DFSchema, Result, internal_datafusion_err, plan_err};
use datafusion::logical_expr::expr::{
    Between, BinaryExpr, Case, HigherOrderFunction, InList, ScalarFunction,
};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Expr, ExprSchemable, LogicalPlan, Operator, Projection, Union, Values,
};
use datafusion::optimizer::AnalyzerRule;

const SHARED_ARGUMENTS: [&str; 7] = [
    "coalesce",
    "nvl",
    "ifnull",
    "greatest",
    "least",
    "array",
    "make_array",
];
const SHARED_AFTER_FIRST: [&str; 2] = ["nvl2", "if"];
const LIST_AND_ELEMENT: [&str; 6] = [
    "array_append",
    "array_prepend",
    "array_contains",
    "array_position",
    "array_remove",
    "element_at",
];
const LISTS: [&str; 4] = ["array_concat", "concat", "array_except", "array_intersect"];
const TYPE_COERCION_RULE: &str = "type_coercion";

#[derive(Debug, Default)]
pub struct SparkTimestampUnitWiden;

impl AnalyzerRule for SparkTimestampUnitWiden {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut retyped = false;
        plan.transform_up_with_subqueries(|node| widen_node(node, &mut retyped))
            .data()
    }

    fn name(&self) -> &'static str {
        "spark_timestamp_unit_widen"
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn analyzer_rules_with_timestamp_unit_widen(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules
        .iter()
        .position(|rule| rule.name() == TYPE_COERCION_RULE)
    else {
        return plan_err!(
            "the timestamp unit rule requires the default type_coercion analyzer rule"
        );
    };
    rules.insert(position, Arc::new(SparkTimestampUnitWiden));
    Ok(rules)
}

const fn unit_rank(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

fn timestamp_unit(data_type: &DataType) -> Option<TimeUnit> {
    match data_type {
        DataType::Timestamp(unit, _) => Some(*unit),
        _ => None,
    }
}

fn list_unit(data_type: &DataType) -> Option<TimeUnit> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            timestamp_unit(field.data_type())
        }
        _ => None,
    }
}

fn with_unit(data_type: &DataType, unit: TimeUnit) -> DataType {
    let element = |field: &Arc<Field>| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(with_unit(field.data_type(), unit)),
        )
    };
    match data_type {
        DataType::Timestamp(_, zone) => DataType::Timestamp(unit, zone.clone()),
        DataType::List(field) => DataType::List(element(field)),
        DataType::LargeList(field) => DataType::LargeList(element(field)),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(element(field), *size),
        other => other.clone(),
    }
}

fn timestamp_zone(data_type: &DataType) -> Option<Arc<str>> {
    match data_type {
        DataType::Timestamp(_, zone) => zone.clone(),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            timestamp_zone(field.data_type())
        }
        _ => None,
    }
}

const fn is_date(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

const fn joins_a_zoned_timestamp(data_type: &DataType) -> bool {
    is_date(data_type)
        || matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
}

struct SharedTarget {
    unit: TimeUnit,
    zone: Option<Arc<str>>,
    dated: bool,
}

impl SharedTarget {
    fn of(types: &[DataType], units: &[Option<TimeUnit>], peers_join: bool) -> Option<Self> {
        let zone = types
            .iter()
            .zip(units)
            .filter(|(_, unit)| unit.is_some())
            .find_map(|(data_type, _)| timestamp_zone(data_type));
        let dated = peers_join && zone.is_some() && types.iter().any(joins_a_zoned_timestamp);
        let unit = match finest(units) {
            Some(unit) => unit,
            None if dated => units
                .iter()
                .flatten()
                .copied()
                .max_by_key(|unit| unit_rank(*unit))?,
            None => return None,
        };
        Some(Self { unit, zone, dated })
    }

    fn operand_type(&self, data_type: &DataType, unit: Option<TimeUnit>) -> Option<DataType> {
        if self.dated && joins_a_zoned_timestamp(data_type) {
            return Some(DataType::Timestamp(self.unit, self.zone.clone()));
        }
        if unit.is_none_or(|unit| unit == self.unit) {
            return None;
        }
        Some(match (data_type, &self.zone) {
            (DataType::Timestamp(_, None), Some(zone)) if self.unit == TimeUnit::Nanosecond => {
                DataType::Timestamp(self.unit, Some(Arc::clone(zone)))
            }
            _ => with_unit(data_type, self.unit),
        })
    }
}

fn finest(units: &[Option<TimeUnit>]) -> Option<TimeUnit> {
    let mut present = units.iter().flatten().copied();
    let first = present.next()?;
    let mut widest = first;
    let mut mixed = false;
    for unit in present {
        if unit != first {
            mixed = true;
        }
        if unit_rank(unit) > unit_rank(widest) {
            widest = unit;
        }
    }
    mixed.then_some(widest)
}

fn share_unit(
    operands: Vec<Expr>,
    schema: &DFSchema,
    unit_of: impl Fn(usize, &DataType) -> Option<TimeUnit>,
    dates: bool,
) -> Result<Transformed<Vec<Expr>>> {
    let mut types = Vec::with_capacity(operands.len());
    for operand in &operands {
        let Ok(data_type) = operand.get_type(schema) else {
            return Ok(Transformed::no(operands));
        };
        types.push(data_type);
    }
    let units: Vec<Option<TimeUnit>> = types
        .iter()
        .enumerate()
        .map(|(index, data_type)| unit_of(index, data_type))
        .collect();
    let Some(target) = SharedTarget::of(&types, &units, dates) else {
        return Ok(Transformed::no(operands));
    };
    let mut widened = Vec::with_capacity(operands.len());
    for ((operand, data_type), unit) in operands.into_iter().zip(&types).zip(&units) {
        match target.operand_type(data_type, *unit) {
            Some(widened_type) => widened.push(operand.cast_to(&widened_type, schema)?),
            None => widened.push(operand),
        }
    }
    Ok(Transformed::yes(widened))
}

fn scalar_unit(_index: usize, data_type: &DataType) -> Option<TimeUnit> {
    timestamp_unit(data_type)
}

fn list_or_element_unit(_index: usize, data_type: &DataType) -> Option<TimeUnit> {
    timestamp_unit(data_type).or_else(|| list_unit(data_type))
}

fn only_list_unit(_index: usize, data_type: &DataType) -> Option<TimeUnit> {
    list_unit(data_type)
}

fn widen_function(function: ScalarFunction, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let name = function.func.name().to_ascii_lowercase();
    let ScalarFunction { func, args } = function;
    let rebuilt = if SHARED_ARGUMENTS.contains(&name.as_str()) {
        share_unit(args, schema, scalar_unit, true)?
    } else if SHARED_AFTER_FIRST.contains(&name.as_str()) {
        share_unit(
            args,
            schema,
            |index, data_type| (index > 0).then(|| timestamp_unit(data_type)).flatten(),
            true,
        )?
    } else if LIST_AND_ELEMENT.contains(&name.as_str()) {
        share_unit(args, schema, list_or_element_unit, false)?
    } else if LISTS.contains(&name.as_str()) {
        share_unit(args, schema, only_list_unit, false)?
    } else {
        Transformed::no(args)
    };
    Ok(rebuilt.update_data(|args| Expr::ScalarFunction(ScalarFunction { func, args })))
}

fn aggregate_zero_unit(index: usize, data_type: &DataType) -> Option<TimeUnit> {
    if index == 0 {
        list_unit(data_type)
    } else {
        timestamp_unit(data_type)
    }
}

fn widen_aggregate_zero(
    function: HigherOrderFunction,
    schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    let HigherOrderFunction { func, mut args } = function;
    if args.len() < 2 || matches!(args[1], Expr::Lambda(_)) {
        return Ok(Transformed::no(Expr::HigherOrderFunction(
            HigherOrderFunction::new(func, args),
        )));
    }
    let lambdas = args.split_off(2);
    share_unit(args, schema, aggregate_zero_unit, false)?.map_data(|mut shared| {
        shared.extend(lambdas);
        Ok(Expr::HigherOrderFunction(HigherOrderFunction::new(
            func, shared,
        )))
    })
}

fn widen_case(case: Case, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Case {
        expr,
        when_then_expr,
        else_expr,
    } = case;
    let branches = when_then_expr.len();
    let mut results: Vec<Expr> = Vec::with_capacity(branches + 1);
    let mut subjects: Vec<Expr> = Vec::with_capacity(branches + 1);
    let simple = expr.is_some();
    if let Some(subject) = expr {
        subjects.push(*subject);
    }
    for (when, then) in when_then_expr {
        subjects.push(*when);
        results.push(*then);
    }
    let has_else = else_expr.is_some();
    if let Some(otherwise) = else_expr {
        results.push(*otherwise);
    }
    let results = share_unit(results, schema, scalar_unit, true)?;
    let subjects = if simple {
        share_unit(subjects, schema, scalar_unit, true)?
    } else {
        Transformed::no(subjects)
    };
    let changed = results.transformed || subjects.transformed;
    let mut subjects = subjects.data.into_iter();
    let mut results = results.data.into_iter();
    let expr = if simple {
        subjects.next().map(Box::new)
    } else {
        None
    };
    let mut when_then_expr = Vec::with_capacity(branches);
    for (when, then) in subjects.zip(results.by_ref()).take(branches) {
        when_then_expr.push((Box::new(when), Box::new(then)));
    }
    let else_expr = if has_else {
        results.next().map(Box::new)
    } else {
        None
    };
    let rebuilt = Expr::Case(Case {
        expr,
        when_then_expr,
        else_expr,
    });
    Ok(if changed {
        Transformed::yes(rebuilt)
    } else {
        Transformed::no(rebuilt)
    })
}

const fn is_comparison(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

fn exactly<const N: usize>(operands: Vec<Expr>) -> Result<[Expr; N]> {
    <[Expr; N]>::try_from(operands).map_err(|operands| {
        internal_datafusion_err!(
            "the timestamp unit rule rebuilt {} operands where {N} were taken",
            operands.len()
        )
    })
}

fn widen_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    match expr {
        Expr::ScalarFunction(function) => widen_function(function, schema),
        Expr::Case(case) => widen_case(case, schema),
        Expr::HigherOrderFunction(function) if function.func.name() == "aggregate" => {
            widen_aggregate_zero(function, schema)
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
            share_unit(vec![*left, *right], schema, scalar_unit, true)?.map_data(|operands| {
                let [left, right] = exactly(operands)?;
                Ok(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                }))
            })
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            share_unit(vec![*expr, *low, *high], schema, scalar_unit, true)?.map_data(|operands| {
                let [expr, low, high] = exactly(operands)?;
                Ok(Expr::Between(Between {
                    expr: Box::new(expr),
                    negated,
                    low: Box::new(low),
                    high: Box::new(high),
                }))
            })
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let mut operands = Vec::with_capacity(list.len() + 1);
            operands.push(*expr);
            operands.extend(list);
            share_unit(operands, schema, scalar_unit, true)?.map_data(|mut operands| {
                let list = operands.split_off(1);
                let [expr] = exactly(operands)?;
                Ok(Expr::InList(InList {
                    expr: Box::new(expr),
                    list,
                    negated,
                }))
            })
        }
        other => Ok(Transformed::no(other)),
    }
}

fn union_targets(union: &Union) -> Option<Vec<Option<SharedTarget>>> {
    let width = union.schema.fields().len();
    let mut targets = Vec::with_capacity(width);
    for index in 0..width {
        let mut types = Vec::with_capacity(union.inputs.len());
        for input in &union.inputs {
            let fields = input.schema().fields();
            if fields.len() != width {
                return None;
            }
            types.push(fields[index].data_type().clone());
        }
        let units: Vec<Option<TimeUnit>> = types.iter().map(timestamp_unit).collect();
        targets.push(SharedTarget::of(&types, &units, true));
    }
    targets.iter().any(Option::is_some).then_some(targets)
}

fn widen_union_input(
    input: &Arc<LogicalPlan>,
    targets: &[Option<SharedTarget>],
) -> Result<Arc<LogicalPlan>> {
    let schema = input.schema();
    let mut changed = false;
    let mut columns = Vec::with_capacity(targets.len());
    for ((qualifier, field), target) in schema.iter().zip(targets) {
        let column = Expr::Column(Column::from((qualifier, field)));
        let widened_type = target.as_ref().and_then(|target| {
            target.operand_type(field.data_type(), timestamp_unit(field.data_type()))
        });
        let widened = match widened_type {
            Some(widened_type) => {
                changed = true;
                column
                    .cast_to(&widened_type, schema)?
                    .alias_qualified(qualifier.cloned(), field.name())
            }
            None => column,
        };
        columns.push(widened);
    }
    if !changed {
        return Ok(Arc::clone(input));
    }
    Ok(Arc::new(LogicalPlan::Projection(Projection::try_new(
        columns,
        Arc::clone(input),
    )?)))
}

fn widen_union(union: &Union) -> Result<Option<LogicalPlan>> {
    let Some(targets) = union_targets(union) else {
        return Ok(None);
    };
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in &union.inputs {
        inputs.push(widen_union_input(input, &targets)?);
    }
    Ok(Some(LogicalPlan::Union(Union::try_new_with_loose_types(
        inputs,
    )?)))
}

fn values_cell_source(cell: &Expr, declared: &DataType) -> Expr {
    match cell {
        Expr::Cast(cast) if cast.field.data_type() == declared => cast.expr.as_ref().clone(),
        other => other.clone(),
    }
}

fn retyped_values_column(values: &Values, column: usize) -> Option<(DataType, Vec<Expr>)> {
    let declared = values.schema.field(column).data_type();
    if declared != &DataType::Timestamp(TimeUnit::Nanosecond, None) {
        return None;
    }
    let empty = DFSchema::empty();
    let mut sources = Vec::with_capacity(values.values.len());
    let mut types = Vec::with_capacity(values.values.len());
    for row in &values.values {
        let source = values_cell_source(row.get(column)?, declared);
        let data_type = source.get_type(&empty).ok()?;
        let joins = matches!(data_type, DataType::Null)
            || is_date(&data_type)
            || matches!(data_type, DataType::Timestamp(unit, _) if unit != TimeUnit::Nanosecond);
        if !joins {
            return None;
        }
        sources.push(source);
        types.push(data_type);
    }
    let units: Vec<Option<TimeUnit>> = types.iter().map(timestamp_unit).collect();
    let target = SharedTarget::of(&types, &units, true)
        .filter(|target| target.dated)
        .or_else(|| naive_expression_beside_a_date(&sources, &types, &units))?;
    let retyped = DataType::Timestamp(target.unit, target.zone);
    let mut cells = Vec::with_capacity(sources.len());
    for (source, data_type) in sources.into_iter().zip(&types) {
        cells.push(if data_type == &retyped {
            source
        } else {
            source.cast_to(&retyped, &empty).ok()?
        });
    }
    Some((retyped, cells))
}

fn naive_expression_beside_a_date(
    sources: &[Expr],
    types: &[DataType],
    units: &[Option<TimeUnit>],
) -> Option<SharedTarget> {
    let read_as_instant = types.iter().any(is_date)
        && sources.iter().zip(types).any(|(source, data_type)| {
            matches!(data_type, DataType::Timestamp(_, None))
                && !crate::timestamp_ns_cast::is_values_wall_cell(source)
        });
    if !read_as_instant {
        return None;
    }
    let unit = units
        .iter()
        .flatten()
        .copied()
        .max_by_key(|unit| unit_rank(*unit))?;
    Some(SharedTarget {
        unit,
        zone: Some(Arc::<str>::from("UTC")),
        dated: true,
    })
}

fn widen_values_dates(values: Values) -> Result<Transformed<LogicalPlan>> {
    let width = values.schema.fields().len();
    let retyped: Vec<(usize, DataType, Vec<Expr>)> = (0..width)
        .filter_map(|column| {
            retyped_values_column(&values, column)
                .map(|(data_type, cells)| (column, data_type, cells))
        })
        .collect();
    if retyped.is_empty() {
        return Ok(Transformed::no(LogicalPlan::Values(values)));
    }
    let Values { schema, mut values } = values;
    let mut fields: Vec<_> = schema
        .iter()
        .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
        .collect();
    for (column, data_type, cells) in retyped {
        for (row, cell) in values.iter_mut().zip(cells) {
            row[column] = cell;
        }
        let field = &mut fields[column].1;
        *field = Arc::new(field.as_ref().clone().with_data_type(data_type));
    }
    let schema = DFSchema::new_with_metadata(fields, schema.metadata().clone())?;
    Ok(Transformed::yes(LogicalPlan::Values(Values {
        schema: Arc::new(schema),
        values,
    })))
}

fn widen_node(plan: LogicalPlan, retyped: &mut bool) -> Result<Transformed<LogicalPlan>> {
    let (plan, dated) = match plan {
        LogicalPlan::Values(values) => {
            let widened = widen_values_dates(values)?;
            (widened.data, widened.transformed)
        }
        other => (other, false),
    };
    *retyped |= dated;
    let widened = widen_plan(plan)?;
    if widened.transformed || dated {
        return Ok(Transformed::yes(widened.data));
    }
    if !*retyped {
        return Ok(widened);
    }
    widened.data.recompute_schema().map(Transformed::yes)
}

fn widen_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Union(union) = &plan {
        return Ok(match widen_union(union)? {
            Some(widened) => Transformed::yes(widened),
            None => Transformed::no(plan),
        });
    }
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| widen_expr(node, &schema))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    if transformed.transformed {
        transformed
            .map_data(LogicalPlan::recompute_schema)?
            .map_data(|plan| plan.resolve_lambda_variables().data())
    } else {
        Ok(transformed)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        Array, ArrayRef, AsArray, BooleanArray, RecordBatch, TimestampMicrosecondArray,
        TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{Schema, TimestampNanosecondType};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::optimizer::Analyzer;
    use datafusion::prelude::SessionContext;

    use super::*;

    const FAR_MICROS: i64 = 253_402_214_400_000_000;

    fn zone() -> Arc<str> {
        Arc::<str>::from("UTC")
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "us_tz",
                DataType::Timestamp(TimeUnit::Microsecond, Some(zone())),
                true,
            ),
            Field::new("us", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("ns", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "ns_tz",
                DataType::Timestamp(TimeUnit::Nanosecond, Some(zone())),
                true,
            ),
            Field::new(
                "far",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("flag", DataType::Boolean, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1_000_001i64, -1]).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(vec![1_000_001i64, -1])),
            Arc::new(TimestampNanosecondArray::from(vec![1_000_001_789i64, -1])),
            Arc::new(
                TimestampNanosecondArray::from(vec![1_000_001_789i64, -1]).with_timezone("UTC"),
            ),
            Arc::new(TimestampMicrosecondArray::from(vec![
                FAR_MICROS, FAR_MICROS,
            ])),
            Arc::new(BooleanArray::from(vec![true, false])),
        ];
        RecordBatch::try_new(schema, columns).unwrap_or_else(|error| panic!("batch: {error}"))
    }

    fn context(with_rule: bool) -> SessionContext {
        let rules = Analyzer::new().rules;
        let rules = if with_rule {
            analyzer_rules_with_timestamp_unit_widen(rules)
                .unwrap_or_else(|error| panic!("place the rule: {error}"))
        } else {
            rules
        };
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rules(rules)
            .build();
        let context = SessionContext::new_with_state(state);
        for table in ["t", "u"] {
            context
                .register_batch(table, batch())
                .unwrap_or_else(|error| panic!("register {table}: {error}"));
        }
        context
    }

    async fn result_type(context: &SessionContext, sql: &str) -> DataType {
        context
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
            .into_optimized_plan()
            .unwrap_or_else(|error| panic!("optimize `{sql}`: {error}"))
            .schema()
            .field(0)
            .data_type()
            .clone()
    }

    async fn nanos(context: &SessionContext, sql: &str) -> Vec<Option<i64>> {
        let batches = context
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("run `{sql}`: {error}"));
        let mut ticks = Vec::new();
        for batch in &batches {
            let column = batch.column(0).as_primitive::<TimestampNanosecondType>();
            for index in 0..column.len() {
                ticks.push(column.is_valid(index).then(|| column.value(index)));
            }
        }
        ticks
    }

    fn mixed_pairs() -> Vec<(&'static str, &'static str)> {
        let micros = ["us_tz", "us"];
        let nanoseconds = ["ns", "ns_tz"];
        let mut pairs = Vec::new();
        for left in micros {
            for right in nanoseconds {
                pairs.push((left, right));
                pairs.push((right, left));
            }
        }
        pairs
    }

    fn element_unit(data_type: &DataType) -> Option<TimeUnit> {
        timestamp_unit(data_type).or_else(|| list_unit(data_type))
    }

    #[tokio::test]
    async fn every_listed_position_takes_the_finest_unit() {
        let context = context(true);
        for (left, right) in mixed_pairs() {
            let cells = [
                format!("SELECT coalesce({left}, {right}) FROM t"),
                format!("SELECT greatest({left}, {right}) FROM t"),
                format!("SELECT least({left}, {right}) FROM t"),
                format!("SELECT make_array({left}, {right}) FROM t"),
                format!("SELECT array_concat(make_array({left}), make_array({right})) FROM t"),
                format!("SELECT CASE WHEN flag THEN {left} ELSE {right} END FROM t"),
                format!("SELECT {left} FROM t UNION ALL SELECT {right} FROM t"),
                format!("SELECT {left} FROM t UNION SELECT {right} FROM t"),
            ];
            for sql in &cells {
                let got = result_type(&context, sql).await;
                assert_eq!(
                    element_unit(&got),
                    Some(TimeUnit::Nanosecond),
                    "`{sql}` answered {got:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn without_the_rule_datafusion_joins_to_a_coarser_unit() {
        let context = context(false);
        let got = result_type(&context, "SELECT coalesce(us, ns) FROM t").await;
        assert_eq!(timestamp_unit(&got), Some(TimeUnit::Microsecond));
    }

    #[tokio::test]
    async fn a_widened_microsecond_value_is_exact() {
        let context = context(true);
        let ticks = nanos(
            &context,
            "SELECT coalesce(us, ns) FROM t ORDER BY flag DESC",
        )
        .await;
        assert_eq!(ticks, vec![Some(1_000_001_000), Some(-1_000)]);
        let ticks = nanos(
            &context,
            "SELECT CASE WHEN flag THEN ns ELSE us END FROM t ORDER BY flag DESC",
        )
        .await;
        assert_eq!(ticks, vec![Some(1_000_001_789), Some(-1_000)]);
    }

    #[tokio::test]
    async fn a_microsecond_value_outside_the_nanosecond_range_raises() {
        let context = context(true);
        let frame = context
            .sql("SELECT greatest(far, ns) FROM t")
            .await
            .unwrap_or_else(|error| panic!("plan: {error}"));
        assert!(frame.collect().await.is_err());
    }

    #[tokio::test]
    async fn comparisons_and_join_keys_share_the_finest_unit() {
        let context = context(true);
        for (left, right) in mixed_pairs() {
            for sql in [
                format!("SELECT {left} = {right} FROM t"),
                format!("SELECT {left} < {right} FROM t"),
                format!("SELECT {left} BETWEEN {right} AND {right} FROM t"),
                format!("SELECT {left} IN ({right}) FROM t"),
                format!("SELECT {left} IS NOT DISTINCT FROM {right} FROM t"),
                format!("SELECT a.flag FROM t a JOIN u b ON a.{left} = b.{right}"),
            ] {
                let plan = context
                    .sql(&sql)
                    .await
                    .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
                    .into_optimized_plan()
                    .unwrap_or_else(|error| panic!("optimize `{sql}`: {error}"));
                let rendered = plan.display_indent().to_string();
                assert!(
                    !rendered.contains("Timestamp(µs")
                        && !rendered.contains("Timestamp(Microsecond"),
                    "`{sql}` casts to microseconds:\n{rendered}"
                );
            }
        }
    }

    #[tokio::test]
    async fn equal_units_and_single_timestamps_are_left_alone() {
        let with_rule = context(true);
        let without = context(false);
        for sql in [
            "SELECT coalesce(us, us_tz) FROM t",
            "SELECT coalesce(ns, ns_tz) FROM t",
            "SELECT coalesce(ns, NULL) FROM t",
            "SELECT ns = NULL FROM t",
            "SELECT us > '2020-01-01' FROM t",
            "SELECT us FROM t UNION ALL SELECT us_tz FROM t",
            "SELECT a.flag FROM t a JOIN u b ON a.us = b.us_tz",
        ] {
            let mut plans = Vec::new();
            for context in [&with_rule, &without] {
                let plan = context
                    .sql(sql)
                    .await
                    .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
                    .into_optimized_plan()
                    .unwrap_or_else(|error| panic!("optimize `{sql}`: {error}"));
                plans.push(plan.display_indent_schema().to_string());
            }
            assert_eq!(plans[0], plans[1], "the rule moved `{sql}`");
        }
    }

    #[tokio::test]
    async fn a_null_beside_a_nanosecond_value_is_nanoseconds() {
        let context = context(true);
        for sql in [
            "SELECT coalesce(ns, NULL) FROM t",
            "SELECT CASE WHEN flag THEN ns ELSE NULL END FROM t",
            "SELECT ns FROM t UNION ALL SELECT NULL",
        ] {
            let got = result_type(&context, sql).await;
            assert_eq!(
                got,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                "`{sql}`"
            );
        }
    }
}
