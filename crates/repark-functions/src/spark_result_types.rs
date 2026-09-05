use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion::functions_aggregate::regr::regr_count_udaf;
use datafusion::functions_window::ntile::ntile_udwf;
use datafusion::functions_window::rank::{dense_rank_udwf, rank_udwf};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::function::{
    AccumulatorArgs, PartitionEvaluatorArgs, StateFieldsArgs, WindowUDFFieldArgs,
};
use datafusion::logical_expr::window_state::WindowAggState;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, LogicalPlan, LogicalPlanBuilder,
    PartitionEvaluator, Signature, Values, WindowUDF, WindowUDFImpl,
};
use datafusion::optimizer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct SparkIntegerLiteral;

impl AnalyzerRule for SparkIntegerLiteral {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_integer_literal"
    }
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Values(values) = plan {
        return rewrite_values(values);
    }
    if matches!(plan, LogicalPlan::Limit(_)) {
        return Ok(Transformed::no(plan));
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = narrow_expr(expr)?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_values(values: Values) -> Result<Transformed<LogicalPlan>> {
    let mut changed = false;
    let mut rows = Vec::with_capacity(values.values.len());
    for row in &values.values {
        let mut narrowed_row = Vec::with_capacity(row.len());
        for expr in row {
            let narrowed = narrow_expr(expr.clone())?;
            changed |= narrowed.transformed;
            narrowed_row.push(narrowed.data);
        }
        rows.push(narrowed_row);
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Values(values)));
    }
    let rebuilt = LogicalPlanBuilder::values(rows)?.build()?;
    Ok(Transformed::yes(rebuilt))
}

fn narrow_expr(expr: Expr) -> Result<Transformed<Expr>> {
    expr.transform_up(|node| Ok(narrow_node(node)))
}

fn narrow_node(expr: Expr) -> Transformed<Expr> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), meta) => {
            if let Ok(narrow) = i32::try_from(value) {
                Transformed::yes(Expr::Literal(ScalarValue::Int32(Some(narrow)), meta))
            } else {
                Transformed::no(Expr::Literal(ScalarValue::Int64(Some(value)), meta))
            }
        }
        Expr::Literal(ScalarValue::Int64(None), meta) => {
            Transformed::yes(Expr::Literal(ScalarValue::Int32(None), meta))
        }
        Expr::Negative(inner) => fold_negative_int_min(*inner),
        other => Transformed::no(other),
    }
}

fn fold_negative_int_min(inner: Expr) -> Transformed<Expr> {
    if let Expr::Literal(ScalarValue::Int64(Some(value)), meta) = &inner
        && *value == i64::from(i32::MAX) + 1
    {
        return Transformed::yes(Expr::Literal(
            ScalarValue::Int32(Some(i32::MIN)),
            meta.clone(),
        ));
    }
    Transformed::no(Expr::Negative(Box::new(inner)))
}

#[must_use]
pub fn signed_aggregate_functions() -> Vec<Arc<AggregateUDF>> {
    vec![
        Arc::new(AggregateUDF::new_from_impl(SignedAggregate::new(
            regr_count_udaf(),
        ))),
        Arc::new(AggregateUDF::new_from_impl(SignedAggregate::new(Arc::new(
            approx_distinct_udaf()
                .as_ref()
                .clone()
                .with_aliases(["approx_count_distinct"]),
        )))),
    ]
}

#[derive(Debug)]
struct SignedAggregate {
    inner: Arc<AggregateUDF>,
}

impl SignedAggregate {
    fn new(inner: Arc<AggregateUDF>) -> Self {
        Self { inner }
    }
}

impl PartialEq for SignedAggregate {
    fn eq(&self, other: &Self) -> bool {
        self.inner.name() == other.inner.name()
    }
}

impl Eq for SignedAggregate {}

impl Hash for SignedAggregate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.name().hash(state);
    }
}

impl AggregateUDFImpl for SignedAggregate {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SignedAccumulator {
            inner: self.inner.accumulator(acc_args)?,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }
}

#[derive(Debug)]
struct SignedAccumulator {
    inner: Box<dyn Accumulator>,
}

fn signed_scalar(value: ScalarValue) -> Result<ScalarValue> {
    match value {
        ScalarValue::UInt64(Some(count)) => {
            let narrowed = i64::try_from(count).map_err(|_| {
                DataFusionError::Execution("unsigned count result does not fit Int64".to_string())
            })?;
            Ok(ScalarValue::Int64(Some(narrowed)))
        }
        ScalarValue::UInt64(None) => Ok(ScalarValue::Int64(None)),
        other => Err(DataFusionError::Internal(format!(
            "signed count wrapper expected UInt64, got {other}"
        ))),
    }
}

impl Accumulator for SignedAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        signed_scalar(self.inner.evaluate()?)
    }

    fn size(&self) -> usize {
        self.inner.size() + std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(values)
    }

    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
}

#[must_use]
pub fn signed_window_functions() -> Vec<Arc<WindowUDF>> {
    vec![
        Arc::new(WindowUDF::new_from_impl(SignedWindow::new(rank_udwf()))),
        Arc::new(WindowUDF::new_from_impl(SignedWindow::new(
            dense_rank_udwf(),
        ))),
        Arc::new(WindowUDF::new_from_impl(SignedWindow::new(
            row_number_udwf(),
        ))),
        Arc::new(WindowUDF::new_from_impl(SignedWindow::new(ntile_udwf()))),
    ]
}

#[derive(Debug)]
struct SignedWindow {
    inner: Arc<WindowUDF>,
}

impl SignedWindow {
    fn new(inner: Arc<WindowUDF>) -> Self {
        Self { inner }
    }
}

impl PartialEq for SignedWindow {
    fn eq(&self, other: &Self) -> bool {
        self.inner.name() == other.inner.name()
    }
}

impl Eq for SignedWindow {}

impl Hash for SignedWindow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.name().hash(state);
    }
}

impl WindowUDFImpl for SignedWindow {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(SignedPartitionEvaluator {
            inner: self.inner.inner().partition_evaluator(args)?,
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let field = self.inner.field(field_args)?;
        Ok(Arc::new(Field::new(
            field.name(),
            DataType::Int32,
            field.is_nullable(),
        )))
    }
}

#[derive(Debug)]
struct SignedPartitionEvaluator {
    inner: Box<dyn PartitionEvaluator>,
}

fn signed_rank_scalar(value: ScalarValue) -> Result<ScalarValue> {
    match value {
        ScalarValue::UInt64(Some(rank)) => {
            let narrowed = i32::try_from(rank).map_err(|_| {
                DataFusionError::Execution("unsigned rank result does not fit Int32".to_string())
            })?;
            Ok(ScalarValue::Int32(Some(narrowed)))
        }
        ScalarValue::UInt64(None) => Ok(ScalarValue::Int32(None)),
        other => Err(DataFusionError::Internal(format!(
            "signed rank wrapper expected UInt64, got {other}"
        ))),
    }
}

fn signed_array(values: ArrayRef) -> Result<ArrayRef> {
    if values.data_type() == &DataType::UInt64 {
        return Ok(cast(&values, &DataType::Int32)?);
    }
    Ok(values)
}

impl PartitionEvaluator for SignedPartitionEvaluator {
    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()> {
        self.inner.memoize(state)
    }

    fn get_range(&self, idx: usize, n_rows: usize) -> Result<Range<usize>> {
        self.inner.get_range(idx, n_rows)
    }

    fn is_causal(&self) -> bool {
        self.inner.is_causal()
    }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        signed_array(self.inner.evaluate_all(values, num_rows)?)
    }

    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue> {
        signed_rank_scalar(self.inner.evaluate(values, range)?)
    }

    fn evaluate_all_with_rank(
        &self,
        num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        signed_array(
            self.inner
                .evaluate_all_with_rank(num_rows, ranks_in_partition)?,
        )
    }

    fn supports_bounded_execution(&self) -> bool {
        self.inner.supports_bounded_execution()
    }

    fn uses_window_frame(&self) -> bool {
        self.inner.uses_window_frame()
    }

    fn include_rank(&self) -> bool {
        self.inner.include_rank()
    }
}

#[cfg(test)]
mod tests;
