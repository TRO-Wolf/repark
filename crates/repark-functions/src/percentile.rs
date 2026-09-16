use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::Literal;

use crate::max_min_by::unqualified_name;

#[must_use]
pub fn percentile_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkPercentile::new()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SparkPercentile {
    signature: Signature,
}

impl SparkPercentile {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn render_double(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{value:.1}")
    } else {
        format!("{value}")
    }
}

#[allow(clippy::cast_precision_loss)]
fn scalar_to_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float64(Some(value)) => Some(*value),
        ScalarValue::Float32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Decimal128(Some(value), _, scale) => {
            Some(*value as f64 / 10f64.powi(i32::from(*scale)))
        }
        ScalarValue::Int8(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value as f64),
        ScalarValue::UInt8(Some(value)) => Some(f64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(f64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(*value as f64),
        _ => None,
    }
}

fn scalar_to_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value),
        ScalarValue::UInt8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => value.to_owned().try_into().ok(),
        _ => None,
    }
}

fn list_to_f64s(list: &ListArray) -> Option<Vec<f64>> {
    let values = cast(list.values(), &DataType::Float64).ok()?;
    let doubles = values.as_any().downcast_ref::<Float64Array>()?;
    if (0..doubles.len()).any(|row| doubles.is_null(row)) {
        return None;
    }
    Some((0..doubles.len()).map(|row| doubles.value(row)).collect())
}

fn render_logical_percentage(expr: &Expr) -> String {
    match expr {
        Expr::Literal(value, _) => match value {
            ScalarValue::List(list) => {
                let items = list_to_f64s(list)
                    .unwrap_or_default()
                    .iter()
                    .map(|percentage| render_double(*percentage))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("array({items})")
            }
            _ => render_scalar_double(value),
        },
        Expr::ScalarFunction(function) if is_array_constructor(function.name()) => {
            let items = function
                .args
                .iter()
                .map(render_logical_percentage)
                .collect::<Vec<_>>()
                .join(", ");
            format!("array({items})")
        }
        Expr::Column(column) => column.name.clone(),
        other => other.schema_name().to_string(),
    }
}

fn render_scalar_double(value: &ScalarValue) -> String {
    scalar_to_f64(value).map(render_double).unwrap_or_default()
}

fn is_array_constructor(name: &str) -> bool {
    matches!(name, "array" | "make_array" | "array_repeat" | "list")
}

fn render_logical_frequency(args: &[Expr]) -> String {
    if args.len() < 3 {
        return "1".to_string();
    }
    match &args[2] {
        Expr::Literal(value, _) => scalar_to_i64(value)
            .map(|frequency| frequency.to_string())
            .unwrap_or_default(),
        Expr::Column(column) => column.name.clone(),
        other => other.schema_name().to_string(),
    }
}

fn percentile_name(args: &[Expr]) -> String {
    let value = args.first().map(unqualified_name).unwrap_or_default();
    let percentage = args
        .get(1)
        .map(render_logical_percentage)
        .unwrap_or_default();
    let frequency = render_logical_frequency(args);
    format!("percentile({value}, {percentage}, {frequency})")
}

fn physical_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    let erased: &dyn std::any::Any = expr.as_ref();
    erased
        .downcast_ref::<Literal>()
        .map(|literal| literal.value().clone())
}

fn physical_array_items(expr: &Arc<dyn PhysicalExpr>) -> Option<Vec<ScalarValue>> {
    let erased: &dyn std::any::Any = expr.as_ref();
    let function = erased.downcast_ref::<ScalarFunctionExpr>()?;
    if !is_array_constructor(function.name()) {
        return None;
    }
    function
        .args()
        .iter()
        .map(physical_literal)
        .collect::<Option<Vec<_>>>()
}

fn out_of_range(name: &str, value: f64) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] Cannot resolve \"{name}\" due to data type \
         mismatch: The percentage must be between [0.0, 1.0] (current value = {}D). \
         SQLSTATE: 42K09",
        render_double(value)
    ))
}

fn negative_frequency(context: &str, value: i64) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[NEGATIVE_VALUES_IN_FREQUENCY_EXPRESSION] Found the negative value in \"{context}\": \
         {value}L, but expected a positive integral value. SQLSTATE: 22003"
    ))
}

fn physical_percentage_parts(expr: &Arc<dyn PhysicalExpr>) -> Option<(Vec<f64>, bool)> {
    if let Some(literal) = physical_literal(expr) {
        if let ScalarValue::List(list) = &literal {
            return list_to_f64s(list).map(|percentages| (percentages, true));
        }
        return scalar_to_f64(&literal).map(|percentage| (vec![percentage], false));
    }
    let items = physical_array_items(expr)?;
    let percentages = items
        .iter()
        .map(scalar_to_f64)
        .collect::<Option<Vec<_>>>()?;
    Some((percentages, true))
}

fn render_physical_percentage(expr: &Arc<dyn PhysicalExpr>) -> String {
    let Some((percentages, is_array)) = physical_percentage_parts(expr) else {
        return String::new();
    };
    if !is_array {
        return percentages
            .first()
            .map(|percentage| render_double(*percentage))
            .unwrap_or_default();
    }
    let items = percentages
        .iter()
        .map(|percentage| render_double(*percentage))
        .collect::<Vec<_>>()
        .join(", ");
    format!("array({items})")
}

fn resolve_percentage(
    expr: &Arc<dyn PhysicalExpr>,
    display: &dyn Fn() -> String,
) -> Result<(Vec<f64>, bool)> {
    let Some((percentages, is_array)) = physical_percentage_parts(expr) else {
        return Err(DataFusionError::Plan(
            "percentile percentage must be a numeric literal or an array of them".to_string(),
        ));
    };
    for percentage in &percentages {
        if !(0.0..=1.0).contains(percentage) {
            return Err(out_of_range(&display(), *percentage));
        }
    }
    Ok((percentages, is_array))
}

pub(crate) fn physical_column_name(expr: &Arc<dyn PhysicalExpr>) -> String {
    let erased: &dyn std::any::Any = expr.as_ref();
    erased
        .downcast_ref::<datafusion::physical_expr::expressions::Column>()
        .map_or_else(
            || "frequency".to_string(),
            |column| column.name().to_string(),
        )
}

impl AggregateUDFImpl for SparkPercentile {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "percentile"
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(percentile_name(&params.args))
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.schema_name(params)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return Err(DataFusionError::Plan(format!(
                "percentile requires 2 or 3 parameters but got {}",
                arg_types.len()
            )));
        }
        let mut coerced = arg_types.to_vec();
        if matches!(
            coerced[0],
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            coerced[0] = DataType::Float64;
        }
        Ok(coerced)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let percentage = arg_types.get(1).ok_or_else(|| {
            DataFusionError::Plan("percentile requires 2 or 3 parameters but got 0".to_string())
        })?;
        if matches!(
            percentage,
            DataType::List(_) | DataType::FixedSizeList(_, _)
        ) {
            return Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Float64,
                true,
            ))));
        }
        Ok(DataType::Float64)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.exprs.len() < 2 || acc_args.exprs.len() > 3 {
            return Err(DataFusionError::Plan(format!(
                "percentile requires 2 or 3 parameters but got {}",
                acc_args.exprs.len()
            )));
        }
        let display = || {
            let value = physical_column_name(&acc_args.exprs[0]);
            let frequency = if acc_args.exprs.len() < 3 {
                "1".to_string()
            } else if let Some(literal) = physical_literal(&acc_args.exprs[2]) {
                scalar_to_i64(&literal)
                    .map(|frequency| frequency.to_string())
                    .unwrap_or_default()
            } else {
                physical_column_name(&acc_args.exprs[2])
            };
            let percentage = render_physical_percentage(&acc_args.exprs[1]);
            format!("percentile({value}, {percentage}, {frequency})")
        };
        let (percentages, is_array) = resolve_percentage(&acc_args.exprs[1], &display)?;
        let literal_frequency = if acc_args.exprs.len() < 3 {
            Some(1)
        } else if let Some(literal) = physical_literal(&acc_args.exprs[2]) {
            if matches!(literal, ScalarValue::Null) {
                Some(0)
            } else if let Some(frequency) = scalar_to_i64(&literal) {
                if frequency < 0 {
                    return Err(negative_frequency(
                        &physical_column_name(&acc_args.exprs[2]),
                        frequency,
                    ));
                }
                Some(frequency)
            } else {
                return Err(DataFusionError::Plan(
                    "percentile frequency must be an integral literal or a column".to_string(),
                ));
            }
        } else {
            None
        };
        Ok(Box::new(PercentileAccumulator::new(
            percentages,
            is_array,
            literal_frequency,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let name = args.name;
        Ok(vec![
            Field::new(
                format_state_name(name, "values"),
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )
            .into(),
            Field::new(
                format_state_name(name, "frequencies"),
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            )
            .into(),
        ])
    }
}

#[derive(Debug)]
struct PercentileAccumulator {
    percentages: Vec<f64>,
    is_array: bool,
    literal_frequency: Option<i64>,
    pairs: Vec<(f64, i64)>,
}

impl PercentileAccumulator {
    fn new(percentages: Vec<f64>, is_array: bool, literal_frequency: Option<i64>) -> Self {
        Self {
            percentages,
            is_array,
            literal_frequency,
            pairs: Vec::new(),
        }
    }

    fn push(&mut self, value: f64, frequency: i64, frequency_context: &str) -> Result<()> {
        if frequency < 0 {
            return Err(negative_frequency(frequency_context, frequency));
        }
        if frequency == 0 {
            return Ok(());
        }
        match self.pairs.binary_search_by(|pair| pair.0.total_cmp(&value)) {
            Ok(index) => {
                self.pairs[index].1 += frequency;
            }
            Err(index) => {
                self.pairs.insert(index, (value, frequency));
            }
        }
        Ok(())
    }

    fn sorted_counts(&self) -> (Vec<f64>, Vec<i64>, i64) {
        let mut values = Vec::with_capacity(self.pairs.len());
        let mut counts = Vec::with_capacity(self.pairs.len());
        let mut total = 0;
        for (value, frequency) in &self.pairs {
            total += frequency;
            values.push(*value);
            counts.push(total);
        }
        (values, counts, total)
    }

    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_possible_wrap
    )]
    fn interpolate(values: &[f64], counts: &[i64], total: i64, percentage: f64) -> f64 {
        let position = percentage * (total - 1) as f64;
        let lower = position.floor() as usize;
        let fraction = position - lower as f64;
        let at = |index: usize| {
            let mut slot = 0;
            while counts[slot] <= index as i64 {
                slot += 1;
            }
            values[slot]
        };
        if fraction == 0.0 || lower + 1 >= total as usize {
            return at(lower);
        }
        at(lower) + fraction * (at(lower + 1) - at(lower))
    }
}

fn frequencies_column(values: &[ArrayRef]) -> Result<ArrayRef> {
    cast(&values[2], &DataType::Int64).map_err(|err| {
        DataFusionError::Plan(format!("percentile frequency must be integral: {err}"))
    })
}

impl Accumulator for PercentileAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values = Float64Array::from(self.pairs.iter().map(|pair| pair.0).collect::<Vec<_>>());
        let frequencies =
            Int64Array::from(self.pairs.iter().map(|pair| pair.1).collect::<Vec<_>>());
        Ok(vec![
            ScalarValue::List(Arc::new(single_row_list(
                Arc::new(values),
                DataType::Float64,
            ))),
            ScalarValue::List(Arc::new(single_row_list(
                Arc::new(frequencies),
                DataType::Int64,
            ))),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let doubles = cast(&values[0], &DataType::Float64).map_err(|err| {
            DataFusionError::Plan(format!("percentile value must be numeric: {err}"))
        })?;
        let doubles = doubles
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Plan("percentile value must be numeric".to_string()))?;
        let counts = match self.literal_frequency {
            Some(_) => None,
            None => Some(
                frequencies_column(values)?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Plan("percentile frequency must be integral".to_string())
                    })?
                    .clone(),
            ),
        };
        for row in 0..doubles.len() {
            if doubles.is_null(row) {
                continue;
            }
            let frequency = match (&counts, self.literal_frequency) {
                (Some(counts), _) if counts.is_null(row) => continue,
                (Some(counts), _) => counts.value(row),
                (None, Some(frequency)) => frequency,
                (None, None) => 1,
            };
            self.push(doubles.value(row), frequency, "frequency")?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let values = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("percentile state must hold lists".to_string()))?;
        let frequencies = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("percentile state must hold lists".to_string()))?;
        for group in 0..values.len() {
            if values.is_null(group) || frequencies.is_null(group) {
                continue;
            }
            let group_values = values.value(group);
            let group_frequencies = frequencies.value(group);
            let group_values = group_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Plan("percentile state must hold doubles".to_string())
                })?;
            let group_frequencies = group_frequencies
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Plan("percentile state must hold int64".to_string())
                })?;
            for row in 0..group_values.len() {
                if group_values.is_null(row) || group_frequencies.is_null(row) {
                    continue;
                }
                self.push(
                    group_values.value(row),
                    group_frequencies.value(row),
                    "frequency",
                )?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.pairs.is_empty() {
            if self.is_array {
                return Ok(ScalarValue::List(Arc::new(ListArray::new_null(
                    Arc::new(Field::new("item", DataType::Float64, true)),
                    1,
                ))));
            }
            return Ok(ScalarValue::Float64(None));
        }
        let (values, counts, total) = self.sorted_counts();
        let answers = self
            .percentages
            .iter()
            .map(|percentage| Self::interpolate(&values, &counts, total, *percentage))
            .collect::<Vec<_>>();
        if !self.is_array {
            return Ok(ScalarValue::Float64(Some(answers[0])));
        }
        let array = Float64Array::from(answers);
        Ok(ScalarValue::List(Arc::new(single_row_list(
            Arc::new(array),
            DataType::Float64,
        ))))
    }

    fn size(&self) -> usize {
        self.pairs.len() * 16 + self.percentages.len() * 8 + 64
    }
}

pub(crate) fn single_row_list(values: ArrayRef, data_type: DataType) -> ListArray {
    let length = values.len();
    ListArray::new(
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::new(vec![0, i32::try_from(length).unwrap_or(i32::MAX)].into()),
        values,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udaf(percentile_udaf().as_ref().clone());
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
        assert_eq!(batches.len(), 1, "expected a single batch for {sql}");
        batches.into_iter().next().expect("one batch")
    }

    fn double(batch: &RecordBatch, row: usize) -> Option<f64> {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        values.is_valid(row).then(|| values.value(row))
    }

    async fn failure(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(failure) => failure.to_string(),
            Ok(frame) => frame.collect().await.expect_err("must fail").to_string(),
        }
    }

    #[tokio::test]
    async fn scalar_median_interpolates() {
        let batch = run(
            &ctx(),
            "SELECT percentile(v, 0.5) FROM (VALUES (10), (20), (30)) AS t(v)",
        )
        .await;
        assert_eq!(double(&batch, 0), Some(20.0));
        let batch = run(
            &ctx(),
            "SELECT percentile(v, 0.25) FROM (VALUES (10), (20), (30)) AS t(v)",
        )
        .await;
        assert_eq!(double(&batch, 0), Some(15.0));
    }

    #[tokio::test]
    async fn array_percentage_answers_per_element() {
        use datafusion::logical_expr::{Expr as LogicalExpr, col, lit};
        let ctx = ctx();
        let frame = ctx
            .sql("SELECT * FROM (VALUES (10), (20), (30)) AS t(v)")
            .await
            .expect("plan")
            .into_optimized_plan()
            .expect("optimized");
        let items = Float64Array::from(vec![0.25, 0.75]);
        let list = ScalarValue::List(Arc::new(single_row_list(
            Arc::new(items),
            DataType::Float64,
        )));
        let call = percentile_udaf().call(vec![col("v"), lit(list)]);
        let plan = datafusion::logical_expr::LogicalPlanBuilder::from(frame)
            .aggregate(Vec::<LogicalExpr>::new(), vec![call])
            .expect("aggregate")
            .build()
            .expect("plan");
        let batch = ctx
            .execute_logical_plan(plan)
            .await
            .expect("execute")
            .collect()
            .await
            .expect("run");
        let values = batch[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("ListArray");
        let row = values.value(0);
        let row = row
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("doubles");
        assert_eq!(row.values(), &[15.0, 25.0]);
    }

    #[tokio::test]
    async fn literal_frequency_repeats_values() {
        let batch = run(
            &ctx(),
            "SELECT percentile(v, 0.5, 2) FROM (VALUES (10), (20)) AS t(v)",
        )
        .await;
        assert_eq!(double(&batch, 0), Some(15.0));
    }

    #[tokio::test]
    async fn column_frequency_skips_nulls() {
        let batch = run(
            &ctx(),
            "SELECT percentile(v, 0.5, f) FROM (VALUES (10, 1), (20, 1), (30, NULL)) AS t(v, f)",
        )
        .await;
        assert_eq!(double(&batch, 0), Some(15.0));
    }

    #[tokio::test]
    async fn out_of_range_names_spark_condition() {
        let refused = failure(
            &ctx(),
            "SELECT percentile(v, 1.5) FROM (VALUES (10), (20)) AS t(v)",
        )
        .await;
        assert!(
            refused.contains("[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE]"),
            "{refused}"
        );
        assert!(refused.contains("percentile(v, 1.5, 1)"), "{refused}");
        assert!(refused.contains("1.5D"), "{refused}");
    }

    #[tokio::test]
    async fn negative_frequency_is_refused() {
        let refused = failure(
            &ctx(),
            "SELECT percentile(v, 0.5, -2) FROM (VALUES (10), (20)) AS t(v)",
        )
        .await;
        assert!(
            refused.contains("[NEGATIVE_VALUES_IN_FREQUENCY_EXPRESSION]"),
            "{refused}"
        );
    }

    #[tokio::test]
    async fn merge_matches_single_partition() {
        let values = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);
        let single = {
            let mut accumulator = PercentileAccumulator::new(vec![0.5], false, Some(1));
            accumulator
                .update_batch(&[Arc::new(values.clone()) as ArrayRef])
                .expect("update");
            accumulator.evaluate().expect("evaluate")
        };
        let merged = {
            let mut first = PercentileAccumulator::new(vec![0.5], false, Some(1));
            let mut second = PercentileAccumulator::new(vec![0.5], false, Some(1));
            first
                .update_batch(&[Arc::new(values.slice(0, 2)) as ArrayRef])
                .expect("update");
            second
                .update_batch(&[Arc::new(values.slice(2, 2)) as ArrayRef])
                .expect("update");
            let first_state = first.state().expect("state");
            let second_state = second.state().expect("state");
            let mut merged = PercentileAccumulator::new(vec![0.5], false, Some(1));
            for state in [first_state, second_state] {
                let arrays = state
                    .iter()
                    .map(|scalar| scalar.to_array_of_size(1).expect("state array"))
                    .collect::<Vec<_>>();
                merged.merge_batch(&arrays).expect("merge");
            }
            merged.evaluate().expect("evaluate")
        };
        assert_eq!(single, merged);
        assert_eq!(single, ScalarValue::Float64(Some(25.0)));
    }
}
