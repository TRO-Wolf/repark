use std::sync::Arc;

use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

use datafusion::logical_expr::expr::{AggregateFunction, Alias};
use datafusion::logical_expr::{
    Expr, GroupingSet,
    logical_plan::{Aggregate, LogicalPlan},
};
use datafusion::optimizer::AnalyzerRule;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Column, TableReference};
use datafusion::logical_expr::{
    bitwise_and, bitwise_or, bitwise_shift_left, bitwise_shift_right, cast,
};

use crate::max_min_by::unqualified_name;

#[must_use]
pub fn grouping_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkGrouping::new()).with_aliases(["grouping"]))
}

#[must_use]
pub fn grouping_id_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkGroupingId::new()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SparkGrouping {
    signature: Signature,
}

impl SparkGrouping {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn grouping_display(args: &[Expr]) -> String {
    let value = args.first().map(unqualified_name).unwrap_or_default();
    format!("grouping({value})")
}

impl AggregateUDFImpl for SparkGrouping {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "__repark_grouping"
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(grouping_display(&params.args))
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.schema_name(params)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int8)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(GroupingAccumulator))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(Vec::new())
    }
}

fn unsupported_grouping_id(display: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[UNSUPPORTED_GROUPING_EXPRESSION] {display} is only supported with GROUP BY CUBE, \
         ROLLUP or GROUPING SETS"
    ))
}

fn grouping_id_column_mismatch(display: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[GROUPING_ID_COLUMN_MISMATCH] {display} columns do not match any grouping set"
    ))
}

fn grouping_parts(group_expr: &[Expr]) -> Option<(Vec<String>, Vec<Vec<String>>)> {
    let [Expr::GroupingSet(set)] = group_expr else {
        return None;
    };
    let order = set
        .distinct_expr()
        .iter()
        .map(|expr| unqualified_name(expr))
        .collect::<Vec<_>>();
    let names = |exprs: &[Expr]| exprs.iter().map(unqualified_name).collect::<Vec<_>>();
    match set {
        GroupingSet::Rollup(cols) => {
            let sets = (0..=cols.len())
                .map(|prefix| names(&cols[..prefix]))
                .collect();
            Some((order, sets))
        }
        GroupingSet::Cube(cols) => {
            let width = u32::try_from(cols.len()).ok()?;
            let total = 1usize.checked_shl(width)?;
            let mut sets = Vec::with_capacity(total);
            for mask in 0..total {
                let mut set = Vec::new();
                for (index, column) in cols.iter().enumerate() {
                    if mask & (1usize << index) != 0 {
                        set.push(unqualified_name(column));
                    }
                }
                sets.push(set);
            }
            Some((order, sets))
        }
        GroupingSet::GroupingSets(sets) => {
            let sets = sets.iter().map(|set| names(set)).collect();
            Some((order, sets))
        }
    }
}

fn grouping_id_bits(order: &[String], args: &[String]) -> Result<Expr> {
    let identifier = Expr::Column(Column::from(Aggregate::INTERNAL_GROUPING_ID));
    let wide = cast(identifier, DataType::UInt64);
    let width = u32::try_from(order.len())
        .map_err(|err| DataFusionError::Plan(format!("grouping sets are too wide: {err}")))?;
    let mask = 1u64
        .checked_shl(width)
        .map_or(u64::MAX, |shifted| shifted - 1);
    let masked = bitwise_and(wide, Expr::Literal(ScalarValue::UInt64(Some(mask)), None));
    if args.len() == order.len() && args.iter().zip(order.iter()).all(|(arg, slot)| arg == slot) {
        return Ok(cast(masked, DataType::Int64));
    }
    let mut placed: Option<Expr> = None;
    for (rank, name) in args.iter().enumerate() {
        let Some(slot) = order.iter().position(|item| item == name) else {
            return Err(DataFusionError::Plan(format!(
                "grouping_id() argument {name} is not a grouping column"
            )));
        };
        let from_right = order.len() - 1 - slot;
        let target = args.len() - 1 - rank;
        let distance = u32::try_from(from_right.abs_diff(target))
            .map_err(|err| DataFusionError::Plan(format!("grouping sets are too wide: {err}")))?;
        let literal = |value: u64| Expr::Literal(ScalarValue::UInt64(Some(value)), None);
        let source =
            1u64.checked_shl(u32::try_from(from_right).map_err(|err| {
                DataFusionError::Plan(format!("grouping sets are too wide: {err}"))
            })?);
        let Some(origin) = source else {
            return Err(DataFusionError::Plan(
                "grouping sets are too wide".to_string(),
            ));
        };
        let isolated = bitwise_and(masked.clone(), literal(origin));
        let moved = match target.cmp(&from_right) {
            std::cmp::Ordering::Less => bitwise_shift_right(isolated, literal(u64::from(distance))),
            std::cmp::Ordering::Greater => {
                bitwise_shift_left(isolated, literal(u64::from(distance)))
            }
            std::cmp::Ordering::Equal => isolated,
        };
        placed = Some(match placed {
            Some(accumulated) => bitwise_or(accumulated, moved),
            None => moved,
        });
    }
    placed
        .map(|bits| cast(bits, DataType::Int64))
        .ok_or_else(|| {
            DataFusionError::Plan("grouping_id() requires a grouping column".to_string())
        })
}

#[derive(Debug, Default)]
pub struct ResolveGroupingId;

impl AnalyzerRule for ResolveGroupingId {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(rewrite_grouping_nodes).data()
    }

    fn name(&self) -> &'static str {
        "resolve_grouping_id"
    }
}

fn aggregate_function_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::AggregateFunction(function) => Some(function.func.name()),
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::AggregateFunction(function) => Some(function.func.name()),
            _ => None,
        },
        _ => None,
    }
}

fn aggregate_uses_grouping(exprs: &[Expr]) -> bool {
    exprs.iter().any(|expr| {
        matches!(
            aggregate_function_name(expr),
            Some("__repark_grouping" | "grouping_id")
        )
    })
}

fn rewrite_grouping_nodes(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => {
            if !aggregate_uses_grouping(&aggregate.aggr_expr) {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            }
            Ok(Transformed::yes(rewrite_aggregate(aggregate)?))
        }
        LogicalPlan::Projection(projection) => {
            if !projection
                .expr
                .iter()
                .any(|expr| qualified_grouping_alias(expr).is_some())
            {
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            }
            let mut rewritten = Vec::with_capacity(projection.expr.len());
            for expr in &projection.expr {
                if let Some(replacement) = qualified_grouping_alias(expr) {
                    rewritten.push(replacement);
                } else {
                    rewritten.push(expr.clone());
                }
            }
            Ok(Transformed::yes(LogicalPlan::Projection(
                datafusion::logical_expr::logical_plan::Projection::try_new(
                    rewritten,
                    projection.input.clone(),
                )?,
            )))
        }
        _ => Ok(Transformed::no(plan)),
    }
}

fn qualified_grouping_alias(expr: &Expr) -> Option<Expr> {
    let Expr::Alias(alias) = expr else {
        return None;
    };
    let mut inner = alias.expr.as_ref();
    while let Expr::Alias(nested) = inner {
        inner = nested.expr.as_ref();
    }
    let Expr::Column(column) = inner else {
        return None;
    };
    let (head, args) = parse_call_name(&column.name)?;
    if !matches!(head.as_str(), "grouping" | "grouping_id") {
        return None;
    }
    let (outer_head, outer_args) = parse_call_name(&alias.name)?;
    if outer_head != head && !(head == "grouping" && outer_head == "__repark_grouping") {
        return None;
    }
    if outer_args != args || alias.name == column.name {
        return None;
    }
    Some(Expr::Alias(Alias::new(
        Expr::Column(column.clone()),
        None::<TableReference>,
        column.name.clone(),
    )))
}

fn parse_call_name(name: &str) -> Option<(String, Vec<String>)> {
    let open = name.find('(')?;
    if !name.ends_with(')') {
        return None;
    }
    let args = split_alias_args(&name[open + 1..name.len() - 1])
        .into_iter()
        .map(|part| {
            part.rsplit('.')
                .next()
                .unwrap_or_default()
                .trim()
                .to_string()
        })
        .collect::<Vec<_>>();
    Some((name[..open].trim().to_string(), args))
}

fn split_alias_args(inner: &str) -> Vec<String> {
    if inner.trim().is_empty() {
        return Vec::new();
    }
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut current = String::new();
    for part in inner.split(',') {
        depth += part.matches('(').count();
        if !current.is_empty() {
            current.push(',');
        }
        current.push_str(part);
        depth = depth.saturating_sub(part.matches(')').count());
        if depth == 0 {
            parts.push(current.trim().to_string());
            current = String::new();
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

type OuterName = Option<(Option<TableReference>, String)>;

fn sorted_names(mut names: Vec<String>) -> Vec<String> {
    names.sort();
    names
}

fn resolve_grouping_id_call(
    function: &AggregateFunction,
    order: &[String],
    sets: &[Vec<String>],
    outer: OuterName,
) -> Result<Expr> {
    let display = grouping_id_display(&function.params.args);
    let rendered = function
        .params
        .args
        .iter()
        .map(unqualified_name)
        .collect::<Vec<_>>();
    let names = if rendered.is_empty() {
        order.to_vec()
    } else {
        rendered
    };
    let wanted = sorted_names(names.clone());
    let matched = sets.iter().any(|set| sorted_names(set.clone()) == wanted);
    if !matched {
        return Err(grouping_id_column_mismatch(&display));
    }
    let bits = grouping_id_bits(order, &names)?;
    let (relation, name) = outer.unwrap_or((None, display));
    Ok(Expr::Alias(Alias::new(bits, relation, name)))
}

fn first_grouping_id_display(aggr_expr: &[Expr]) -> String {
    for expr in aggr_expr {
        let candidate = match expr {
            Expr::AggregateFunction(function) => Some(function),
            Expr::Alias(alias) => match alias.expr.as_ref() {
                Expr::AggregateFunction(function) => Some(function),
                _ => None,
            },
            _ => None,
        };
        if let Some(function) = candidate.filter(|function| function.func.name() == "grouping_id") {
            return grouping_id_display(&function.params.args);
        }
    }
    "grouping_id()".to_string()
}

fn grouping_function(expr: &Expr) -> Option<(&AggregateFunction, OuterName)> {
    match expr {
        Expr::AggregateFunction(function)
            if matches!(function.func.name(), "__repark_grouping" | "grouping_id") =>
        {
            Some((function, None))
        }
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::AggregateFunction(function)
                if matches!(function.func.name(), "__repark_grouping" | "grouping_id") =>
            {
                Some((function, Some((alias.relation.clone(), alias.name.clone()))))
            }
            _ => None,
        },
        _ => None,
    }
}

fn resolve_grouping_call(
    function: &AggregateFunction,
    order: Option<&[String]>,
    outer: OuterName,
) -> Result<Expr> {
    let display = grouping_display(&function.params.args);
    let (relation, alias) = outer.unwrap_or((None, display.clone()));
    let Some(order) = order else {
        return Ok(Expr::Alias(Alias::new(
            Expr::Literal(ScalarValue::Int8(Some(0)), None),
            relation,
            alias,
        )));
    };
    let [arg] = function.params.args.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "grouping() requires exactly one grouping column, got {display}"
        )));
    };
    let Expr::Column(column) = arg else {
        return Err(DataFusionError::Plan(format!(
            "grouping() requires exactly one grouping column, got {display}"
        )));
    };
    let name = unqualified_name(&Expr::Column(column.clone()));
    if !order.iter().any(|slot| slot == &name) {
        return Err(DataFusionError::Plan(format!(
            "grouping() argument {name} is not a grouping column"
        )));
    }
    let bits = grouping_id_bits(order, std::slice::from_ref(&name))?;
    Ok(Expr::Alias(Alias::new(
        cast(bits, DataType::Int8),
        relation,
        alias,
    )))
}

fn rewrite_aggregate(aggregate: Aggregate) -> Result<LogicalPlan> {
    let Aggregate {
        input,
        group_expr,
        aggr_expr,
        schema,
        ..
    } = aggregate;
    let parts = grouping_parts(&group_expr);
    if parts.is_none()
        && aggr_expr
            .iter()
            .any(|expr| aggregate_function_name(expr) == Some("grouping_id"))
    {
        return Err(unsupported_grouping_id(&first_grouping_id_display(
            &aggr_expr,
        )));
    }
    let gid_len = usize::from(parts.is_some());
    let (order, sets) = parts.unwrap_or_default();
    let columns = schema.columns();
    let group_len = columns
        .len()
        .saturating_sub(aggr_expr.len())
        .saturating_sub(gid_len);
    let mut projection_exprs = columns
        .iter()
        .take(group_len)
        .map(|column| Expr::Column(column.clone()))
        .collect::<Vec<_>>();
    let mut new_agg_expr = Vec::with_capacity(aggr_expr.len());
    for (expr, column) in aggr_expr
        .into_iter()
        .zip(columns.into_iter().skip(group_len + gid_len))
    {
        let Some((function, outer)) = grouping_function(&expr) else {
            new_agg_expr.push(expr);
            projection_exprs.push(Expr::Column(column));
            continue;
        };
        let outer = outer.or(Some((column.relation, column.name)));
        let bits = if function.func.name() == "grouping_id" {
            resolve_grouping_id_call(function, &order, &sets, outer)?
        } else {
            let with_sets = (gid_len == 1).then_some(order.as_slice());
            resolve_grouping_call(function, with_sets, outer)?
        };
        projection_exprs.push(bits);
    }
    let narrowed = Aggregate::try_new(input, group_expr, new_agg_expr)?;
    Ok(LogicalPlan::Projection(
        datafusion::logical_expr::logical_plan::Projection::try_new(
            projection_exprs,
            Arc::new(LogicalPlan::Aggregate(narrowed)),
        )?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int8Array, Int64Array, RecordBatch};
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::optimizer::Analyzer;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx() -> SessionContext {
        let mut rules = Analyzer::new().rules;
        rules.push(Arc::new(ResolveGroupingId));
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_analyzer_rules(rules)
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_udaf(grouping_udaf().as_ref().clone());
        ctx.register_udaf(grouping_id_udaf().as_ref().clone());
        ctx
    }

    async fn run(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        let schema = batches.first().expect("at least one batch").schema();
        datafusion::arrow::compute::concat_batches(&schema, &batches).expect("concat")
    }

    fn longs(batch: &RecordBatch, column: usize) -> Vec<Option<i64>> {
        let values = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        let mut answered = (0..values.len())
            .map(|row| values.is_valid(row).then(|| values.value(row)))
            .collect::<Vec<_>>();
        answered.sort();
        answered
    }

    async fn failure(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(failure) => failure.to_string(),
            Ok(frame) => frame.collect().await.expect_err("must fail").to_string(),
        }
    }

    #[tokio::test]
    async fn cube_bitmask_matches_spark() {
        let batch = run(
            &ctx(),
            "SELECT grouping_id(), grouping_id(g, k) FROM (VALUES (1, 'a'), (1, 'b'), (2, NULL)) AS t(g, k) GROUP BY CUBE(g, k)",
        )
        .await;
        assert_eq!(
            longs(&batch, 0),
            vec![
                Some(0),
                Some(0),
                Some(0),
                Some(1),
                Some(1),
                Some(2),
                Some(2),
                Some(2),
                Some(3)
            ]
        );
        assert_eq!(longs(&batch, 1), longs(&batch, 0));
    }

    #[tokio::test]
    async fn rollup_subset_matches_its_set() {
        let batch = run(
            &ctx(),
            "SELECT grouping_id(g) FROM (VALUES (1, 'a'), (2, 'b')) AS t(g, k) GROUP BY ROLLUP(g, k)",
        )
        .await;
        assert_eq!(
            longs(&batch, 0),
            vec![Some(0), Some(0), Some(0), Some(0), Some(1)]
        );
    }

    #[tokio::test]
    async fn grouping_sets_syntax_reaches_resolve_grouping_id() {
        let batch = run(
            &ctx(),
            "SELECT g, k, grouping(g), grouping_id(g, k) FROM (VALUES (1, 'a'), (2, 'b')) AS t(g, k) GROUP BY GROUPING SETS ((g, k), (g), ())",
        )
        .await;
        assert_eq!(batch.schema().field(2).name(), "grouping(g)");
        assert_eq!(batch.schema().field(3).name(), "grouping_id(g, k)");
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Int8);
        assert_eq!(batch.schema().field(3).data_type(), &DataType::Int64);
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("Int8Array");
        let mut answered = (0..values.len())
            .map(|row| values.value(row))
            .collect::<Vec<_>>();
        answered.sort_unstable();
        assert_eq!(answered, vec![0, 0, 0, 0, 1]);
        assert_eq!(
            longs(&batch, 3),
            vec![Some(0), Some(0), Some(1), Some(1), Some(3)]
        );
    }

    #[tokio::test]
    async fn grouping_answers_tinyint() {
        let batch = run(
            &ctx(),
            "SELECT g, k, grouping(g) AS gg, grouping(g) FROM (VALUES (1, 'a'), (2, 'b')) AS t(g, k) GROUP BY CUBE(g, k)",
        )
        .await;
        assert_eq!(batch.schema().field(2).name(), "gg");
        assert_eq!(batch.schema().field(3).name(), "grouping(g)");
        assert_eq!(batch.schema().field(3).data_type(), &DataType::Int8);
        let values = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("Int8Array");
        let mut answered = (0..values.len())
            .map(|row| values.value(row))
            .collect::<Vec<_>>();
        answered.sort_unstable();
        assert_eq!(answered, vec![0, 0, 0, 0, 1, 1, 1]);
    }

    #[tokio::test]
    async fn non_set_columns_are_refused() {
        let refused = failure(
            &ctx(),
            "SELECT grouping_id(k) FROM (VALUES (1, 'a'), (2, 'b')) AS t(g, k) GROUP BY ROLLUP(g, k)",
        )
        .await;
        assert!(
            refused.contains("[GROUPING_ID_COLUMN_MISMATCH]"),
            "{refused}"
        );
    }

    #[tokio::test]
    async fn plain_group_by_is_refused() {
        let refused = failure(
            &ctx(),
            "SELECT grouping_id() FROM (VALUES (1, 'a'), (2, 'b')) AS t(g, k) GROUP BY g",
        )
        .await;
        assert!(
            refused.contains("[UNSUPPORTED_GROUPING_EXPRESSION]"),
            "{refused}"
        );
    }

    #[tokio::test]
    async fn grouping_survives_outer_cast_with_alias() {
        let batch = run(
            &ctx(),
            "SELECT CAST(\"gg\" AS TINYINT) AS \"grouping(g)\" FROM (SELECT grouping(g) AS gg FROM (VALUES (1, 'a'), (2, 'b')) AS t(g, k) GROUP BY CUBE(g, k))",
        )
        .await;
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int8);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("Int8Array");
        let mut answered = (0..values.len())
            .map(|row| values.value(row))
            .collect::<Vec<_>>();
        answered.sort_unstable();
        assert_eq!(answered, vec![0, 0, 0, 0, 1, 1, 1]);
    }

    #[tokio::test]
    async fn constant_accumulator_merges_cleanly() {
        let single = {
            let mut accumulator = GroupingAccumulator;
            accumulator.evaluate().expect("evaluate")
        };
        let merged = {
            let mut accumulator = GroupingAccumulator;
            accumulator.merge_batch(&[]).expect("merge");
            accumulator.evaluate().expect("evaluate")
        };
        assert_eq!(single, merged);
        assert_eq!(single, ScalarValue::Int8(Some(0)));
    }
}

#[derive(Debug)]
struct GroupingAccumulator;

impl Accumulator for GroupingAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(Vec::new())
    }

    fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        Ok(())
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int8(Some(0)))
    }

    fn size(&self) -> usize {
        8
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SparkGroupingId {
    signature: Signature,
}

impl SparkGroupingId {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

pub(crate) fn grouping_id_display(args: &[Expr]) -> String {
    let rendered = args
        .iter()
        .map(unqualified_name)
        .collect::<Vec<_>>()
        .join(", ");
    format!("grouping_id({rendered})")
}

impl AggregateUDFImpl for SparkGroupingId {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "grouping_id"
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(grouping_id_display(&params.args))
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.schema_name(params)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Err(DataFusionError::Plan(
            "grouping_id() is only supported with CUBE, ROLLUP or GROUPING SETS".to_string(),
        ))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(Vec::new())
    }
}
