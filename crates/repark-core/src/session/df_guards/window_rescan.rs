use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::compute::filter;
use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::function::{
    AccumulatorArgs, PartitionEvaluatorArgs, WindowUDFFieldArgs,
};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, Expr, ExprSchemable, LogicalPlan, PartitionEvaluator, Signature,
    Volatility, WindowFunctionDefinition, WindowUDF, WindowUDFImpl,
};
use datafusion::optimizer::{Analyzer, AnalyzerRule};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};

pub(super) fn analyzer_rules_with_sliding_rescan()
-> Vec<Arc<dyn AnalyzerRule + Send + Sync + 'static>> {
    let mut rules = Analyzer::new().rules;
    rules.push(Arc::new(SlidingFrameReScan));
    rules
}

#[derive(Debug, Default)]
pub struct SlidingFrameReScan;

impl AnalyzerRule for SlidingFrameReScan {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DataFusionResult<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "sliding_frame_rescan"
    }
}

fn rewrite_plan(plan: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
    if !matches!(plan, LogicalPlan::Window(_)) {
        return Ok(Transformed::no(plan));
    }
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved = preserver.save(&expr);
        let rewritten = expr.transform_up(|node| Ok(rewrite_expr(node, &schema)))?;
        Ok(rewritten.update_data(|node| saved.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::WindowFunction(ref window) = expr else {
        return Transformed::no(expr);
    };
    let WindowFunctionDefinition::AggregateUDF(ref aggregate) = window.fun else {
        return Transformed::no(expr);
    };
    if window.params.window_frame.is_ever_expanding() {
        return Transformed::no(expr);
    }
    let Some(fields) = argument_fields(&window.params.args, schema) else {
        return Transformed::no(expr);
    };
    let literals = argument_literals(&window.params.args);
    if !needs_frame_rescan(aggregate, &literals, &fields, window.params.distinct) {
        return Transformed::no(expr);
    }
    let wrapper = ReScanAggregateWindow::new(
        Arc::clone(aggregate),
        window.params.distinct,
        literals,
        window.params.filter.is_some(),
    );
    let mut rewritten = window.as_ref().clone();
    if let Some(predicate) = rewritten.params.filter.take() {
        rewritten.params.args.push(*predicate);
    }
    rewritten.fun =
        WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::new_from_impl(wrapper)));
    Transformed::yes(Expr::WindowFunction(Box::new(rewritten)))
}

fn argument_fields(args: &[Expr], schema: &DFSchema) -> Option<Vec<FieldRef>> {
    args.iter()
        .map(|arg| arg.to_field(schema).ok().map(|(_, field)| field))
        .collect()
}

fn argument_literals(args: &[Expr]) -> Vec<Option<ScalarValue>> {
    args.iter()
        .map(|arg| match arg {
            Expr::Literal(scalar, _) => Some(scalar.clone()),
            _ => None,
        })
        .collect()
}

fn synthetic_expressions(
    literals: &[Option<ScalarValue>],
    fields: &[FieldRef],
) -> Vec<Arc<dyn PhysicalExpr>> {
    fields
        .iter()
        .enumerate()
        .map(
            |(index, field)| match literals.get(index).and_then(Option::as_ref) {
                Some(scalar) => Arc::new(Literal::new(scalar.clone())) as Arc<dyn PhysicalExpr>,
                None => Arc::new(Column::new(field.name(), index)),
            },
        )
        .collect()
}

fn accumulator_arguments<'a>(
    aggregate: &'a AggregateUDF,
    fields: &'a [FieldRef],
    exprs: &'a [Arc<dyn PhysicalExpr>],
    schema: &'a Schema,
    return_field: FieldRef,
    distinct: bool,
    ignore_nulls: bool,
) -> AccumulatorArgs<'a> {
    AccumulatorArgs {
        return_field,
        schema,
        ignore_nulls,
        order_bys: &[],
        is_reversed: false,
        name: aggregate.name(),
        is_distinct: distinct,
        exprs,
        expr_fields: fields,
    }
}

fn build_accumulator(
    aggregate: &AggregateUDF,
    fields: &[FieldRef],
    exprs: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
    distinct: bool,
    ignore_nulls: bool,
) -> DataFusionResult<Box<dyn Accumulator>> {
    let return_field = aggregate.return_field(fields)?;
    aggregate.accumulator(accumulator_arguments(
        aggregate,
        fields,
        exprs,
        schema,
        return_field,
        distinct,
        ignore_nulls,
    ))
}

fn build_sliding_accumulator(
    aggregate: &AggregateUDF,
    fields: &[FieldRef],
    exprs: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
    distinct: bool,
) -> DataFusionResult<Box<dyn Accumulator>> {
    let return_field = aggregate.return_field(fields)?;
    aggregate.create_sliding_accumulator(accumulator_arguments(
        aggregate,
        fields,
        exprs,
        schema,
        return_field,
        distinct,
        false,
    ))
}

fn needs_frame_rescan(
    aggregate: &AggregateUDF,
    literals: &[Option<ScalarValue>],
    fields: &[FieldRef],
    distinct: bool,
) -> bool {
    let exprs = synthetic_expressions(literals, fields);
    let schema = Schema::new(fields.to_vec());
    let Ok(accumulator) = build_sliding_accumulator(aggregate, fields, &exprs, &schema, distinct)
    else {
        return false;
    };
    !accumulator.supports_retract_batch()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReScanAggregateWindow {
    aggregate: Arc<AggregateUDF>,
    signature: Signature,
    distinct: bool,
    literals: Vec<Option<ScalarValue>>,
    filtered: bool,
}

impl ReScanAggregateWindow {
    fn new(
        aggregate: Arc<AggregateUDF>,
        distinct: bool,
        literals: Vec<Option<ScalarValue>>,
        filtered: bool,
    ) -> Self {
        Self {
            aggregate,
            signature: Signature::variadic_any(Volatility::Immutable),
            distinct,
            literals,
            filtered,
        }
    }

    fn aggregate_fields<'a>(&self, input_fields: &'a [FieldRef]) -> &'a [FieldRef] {
        if self.filtered && !input_fields.is_empty() {
            &input_fields[..input_fields.len() - 1]
        } else {
            input_fields
        }
    }
}

impl WindowUDFImpl for ReScanAggregateWindow {
    fn name(&self) -> &str {
        self.aggregate.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> DataFusionResult<Box<dyn PartitionEvaluator>> {
        let fields: Vec<FieldRef> = self.aggregate_fields(args.input_fields()).to_vec();
        let exprs = synthetic_expressions(&self.literals, &fields);
        let schema = Arc::new(Schema::new(fields.clone()));
        let evaluator = FrameReScanEvaluator {
            aggregate: Arc::clone(&self.aggregate),
            argument_count: fields.len(),
            fields,
            exprs,
            schema,
            distinct: self.distinct,
            ignore_nulls: args.ignore_nulls(),
            filtered: self.filtered,
        };
        evaluator.accumulator()?;
        Ok(Box::new(evaluator))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DataFusionResult<FieldRef> {
        let field = self
            .aggregate
            .return_field(self.aggregate_fields(field_args.input_fields()))?;
        Ok(Arc::new(
            field.as_ref().clone().with_name(field_args.name()),
        ))
    }
}

#[derive(Debug)]
struct FrameReScanEvaluator {
    aggregate: Arc<AggregateUDF>,
    fields: Vec<FieldRef>,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    schema: Arc<Schema>,
    distinct: bool,
    ignore_nulls: bool,
    filtered: bool,
    argument_count: usize,
}

impl FrameReScanEvaluator {
    fn apply_frame_filter(
        &self,
        values: &[ArrayRef],
        range: &Range<usize>,
        frame: &[ArrayRef],
    ) -> DataFusionResult<Vec<ArrayRef>> {
        let predicate = values.get(self.argument_count).ok_or_else(|| {
            DataFusionError::Internal(
                "the frame re-scan evaluator lost its FILTER predicate column".to_string(),
            )
        })?;
        let sliced = predicate.slice(range.start, range.end - range.start);
        let Some(mask) = sliced.as_any().downcast_ref::<BooleanArray>() else {
            return Err(DataFusionError::Internal(
                "a window FILTER predicate must evaluate to BOOLEAN".to_string(),
            ));
        };
        frame
            .iter()
            .map(|column| filter(column.as_ref(), mask).map_err(DataFusionError::from))
            .collect()
    }

    fn accumulator(&self) -> DataFusionResult<Box<dyn Accumulator>> {
        build_accumulator(
            &self.aggregate,
            &self.fields,
            &self.exprs,
            &self.schema,
            self.distinct,
            self.ignore_nulls,
        )
    }
}

impl PartitionEvaluator for FrameReScanEvaluator {
    fn uses_window_frame(&self) -> bool {
        true
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> DataFusionResult<ScalarValue> {
        let mut accumulator = self.accumulator()?;
        if range.end > range.start {
            let length = range.end - range.start;
            let frame: Vec<ArrayRef> = values
                .iter()
                .take(self.argument_count)
                .map(|column| column.slice(range.start, length))
                .collect();
            let frame = if self.filtered {
                self.apply_frame_filter(values, range, &frame)?
            } else {
                frame
            };
            accumulator.update_batch(&frame)?;
        }
        accumulator.evaluate()
    }
}
