use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Schema};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, LogicalPlan, Signature, Volatility,
    WindowFunctionDefinition,
};
use datafusion::prelude::SessionContext;

use crate::ReparkSession;

const FRAME: &str = "ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW";
const EMPTY_FRAME_SENTINEL: f64 = -1.0;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ThrowawayNonRetractableSum {
    signature: Signature,
}

impl ThrowawayNonRetractableSum {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ThrowawayNonRetractableSum {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "winslide_probe_sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> DataFusionResult<Box<dyn Accumulator>> {
        Ok(Box::new(ThrowawaySumAccumulator::default()))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DataFusionResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[total]", args.name),
            DataType::Float64,
            true,
        ))])
    }

    fn default_value(&self, _data_type: &DataType) -> DataFusionResult<ScalarValue> {
        Ok(ScalarValue::Float64(Some(EMPTY_FRAME_SENTINEL)))
    }
}

#[derive(Debug, Default)]
struct ThrowawaySumAccumulator {
    total: f64,
    seen: bool,
}

impl Accumulator for ThrowawaySumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        let column = values[0].as_primitive::<Float64Type>();
        for index in 0..column.len() {
            if column.is_valid(index) {
                self.total += column.value(index);
                self.seen = true;
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float64(self.seen.then_some(self.total))])
    }

    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        Ok(ScalarValue::Float64(self.seen.then_some(self.total)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

fn probe_context() -> SessionContext {
    let session = ReparkSession::new().unwrap();
    let context = session.context().clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(2.0),
                None,
                Some(4.0),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    context.register_batch("winslide_probe", batch).unwrap();
    context
}

fn window_function_kinds(plan: &LogicalPlan) -> Vec<String> {
    let mut kinds = Vec::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::Window(window) = node {
            for expr in &window.window_expr {
                expr.apply(|inner| {
                    if let Expr::WindowFunction(function) = inner {
                        kinds.push(match &function.fun {
                            WindowFunctionDefinition::AggregateUDF(udaf) => {
                                format!("aggregate:{}", udaf.name())
                            }
                            WindowFunctionDefinition::WindowUDF(udwf) => {
                                format!("rescan:{}", udwf.name())
                            }
                        });
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .unwrap();
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    kinds
}

#[tokio::test]
async fn an_unregistered_non_retractable_aggregate_answers_over_a_sliding_frame() {
    let context = probe_context();
    context.register_udaf(AggregateUDF::new_from_impl(
        ThrowawayNonRetractableSum::new(),
    ));
    let query =
        format!("SELECT winslide_probe_sum(v) OVER ({FRAME}) AS w FROM winslide_probe ORDER BY id");
    let frame = context.sql(&query).await.unwrap();
    let batches = frame.collect().await.unwrap();
    let column = batches[0].column(0).as_primitive::<Float64Type>();
    let answers: Vec<Option<f64>> = (0..column.len())
        .map(|index| column.is_valid(index).then(|| column.value(index)))
        .collect();
    assert_eq!(
        answers,
        vec![Some(1.0), Some(3.0), Some(2.0), Some(4.0)],
        "a freshly registered aggregate with no retract_batch must fall back to the frame re-scan"
    );
}

#[tokio::test]
async fn a_non_retractable_aggregate_is_planned_as_a_frame_rescan() {
    let context = probe_context();
    context.register_udaf(AggregateUDF::new_from_impl(
        ThrowawayNonRetractableSum::new(),
    ));
    let query = format!("SELECT winslide_probe_sum(v) OVER ({FRAME}) AS w FROM winslide_probe");
    let plan = context
        .sql(&query)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    assert_eq!(
        window_function_kinds(&plan),
        vec!["rescan:winslide_probe_sum".to_string()]
    );
}

#[tokio::test]
async fn a_retractable_aggregate_keeps_datafusions_sliding_accumulator() {
    let context = probe_context();
    let query = format!("SELECT sum(v) OVER ({FRAME}) AS w FROM winslide_probe");
    let plan = context
        .sql(&query)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    assert_eq!(
        window_function_kinds(&plan),
        vec!["aggregate:sum".to_string()],
        "sum retracts; the fallback must leave DataFusion's sliding accumulator in place"
    );
}

#[tokio::test]
async fn an_ever_expanding_frame_is_never_rewritten() {
    let context = probe_context();
    context.register_udaf(AggregateUDF::new_from_impl(
        ThrowawayNonRetractableSum::new(),
    ));
    let query = "SELECT winslide_probe_sum(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED \
         PRECEDING AND CURRENT ROW) AS w FROM winslide_probe";
    let plan = context
        .sql(query)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    assert_eq!(
        window_function_kinds(&plan),
        vec!["aggregate:winslide_probe_sum".to_string()],
        "an ever-expanding frame never asks for retraction, so it keeps the plain accumulator"
    );
}

#[tokio::test]
async fn an_empty_frame_answers_a_fresh_accumulator_not_the_aggregate_default() {
    let context = probe_context();
    context.register_udaf(AggregateUDF::new_from_impl(
        ThrowawayNonRetractableSum::new(),
    ));
    let query = "SELECT winslide_probe_sum(v) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 \
         PRECEDING) AS w FROM winslide_probe ORDER BY id";
    let frame = context.sql(query).await.unwrap();
    let batches = frame.collect().await.unwrap();
    let column = batches[0].column(0).as_primitive::<Float64Type>();
    assert!(
        !column.is_valid(0),
        "an empty frame evaluates a fresh accumulator, never the aggregate default value"
    );
    assert!(column.is_valid(3));
    assert!((column.value(3) - 3.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn a_filtered_non_retractable_aggregate_answers_the_masked_frame() {
    let context = probe_context();
    context.register_udaf(AggregateUDF::new_from_impl(
        ThrowawayNonRetractableSum::new(),
    ));
    let query = format!(
        "SELECT winslide_probe_sum(v) FILTER (WHERE v > 1.5) OVER ({FRAME}) AS w \
         FROM winslide_probe ORDER BY id"
    );
    let frame = context.sql(&query).await.unwrap();
    let batches = frame.collect().await.unwrap();
    let column = batches[0].column(0).as_primitive::<Float64Type>();
    let answers: Vec<Option<f64>> = (0..column.len())
        .map(|index| column.is_valid(index).then(|| column.value(index)))
        .collect();
    assert_eq!(
        answers,
        vec![None, Some(2.0), Some(2.0), Some(4.0)],
        "FILTER (WHERE ...) rides through the frame re-scan as a per-frame mask"
    );
}
