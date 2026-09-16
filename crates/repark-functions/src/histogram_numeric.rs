use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, ListArray, StructArray};
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
use datafusion::physical_expr::expressions::Literal;

use crate::max_min_by::unqualified_name;
use crate::percentile::{physical_column_name, single_row_list};

#[must_use]
pub fn histogram_numeric_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkHistogramNumeric::new()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SparkHistogramNumeric {
    signature: Signature,
}

impl SparkHistogramNumeric {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn render_logical_bins(expr: &Expr) -> String {
    match expr {
        Expr::Literal(value, _) => match value {
            ScalarValue::Int8(Some(bins)) => bins.to_string(),
            ScalarValue::Int16(Some(bins)) => bins.to_string(),
            ScalarValue::Int32(Some(bins)) => bins.to_string(),
            ScalarValue::Int64(Some(bins)) => bins.to_string(),
            ScalarValue::UInt8(Some(bins)) => bins.to_string(),
            ScalarValue::UInt16(Some(bins)) => bins.to_string(),
            ScalarValue::UInt32(Some(bins)) => bins.to_string(),
            ScalarValue::UInt64(Some(bins)) => bins.to_string(),
            _ => value.to_string(),
        },
        Expr::Column(column) => column.name.clone(),
        other => other.schema_name().to_string(),
    }
}

fn histogram_name(args: &[Expr]) -> String {
    let value = args.first().map(unqualified_name).unwrap_or_default();
    let bins = args.get(1).map(render_logical_bins).unwrap_or_default();
    format!("histogram_numeric({value}, {bins})")
}

fn physical_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    let erased: &dyn std::any::Any = expr.as_ref();
    erased
        .downcast_ref::<Literal>()
        .map(|literal| literal.value().clone())
}

fn literal_bins(literal: &ScalarValue) -> Option<i64> {
    match literal {
        ScalarValue::Int8(Some(bins)) => Some(i64::from(*bins)),
        ScalarValue::Int16(Some(bins)) => Some(i64::from(*bins)),
        ScalarValue::Int32(Some(bins)) => Some(i64::from(*bins)),
        ScalarValue::Int64(Some(bins)) => Some(*bins),
        ScalarValue::UInt8(Some(bins)) => Some(i64::from(*bins)),
        ScalarValue::UInt16(Some(bins)) => Some(i64::from(*bins)),
        ScalarValue::UInt32(Some(bins)) => Some(i64::from(*bins)),
        ScalarValue::UInt64(Some(bins)) => i64::try_from(*bins).ok(),
        _ => None,
    }
}

fn out_of_range(display: &str, bins: i64) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] Cannot resolve \"{display}\" due to data type \
         mismatch: The number of bins must be at least 2 (current value = {bins}). \
         SQLSTATE: 42K09"
    ))
}

fn resolve_bins(expr: &Arc<dyn PhysicalExpr>, display: &dyn Fn() -> String) -> Result<usize> {
    let Some(literal) = physical_literal(expr) else {
        return Err(DataFusionError::Plan(
            "histogram_numeric nBins must be an integral literal".to_string(),
        ));
    };
    let Some(bins) = literal_bins(&literal) else {
        return Err(DataFusionError::Plan(
            "histogram_numeric nBins must be an integral literal".to_string(),
        ));
    };
    if bins < 2 {
        return Err(out_of_range(&display(), bins));
    }
    usize::try_from(bins).map_err(|err| {
        DataFusionError::Plan(format!("histogram_numeric nBins is out of range: {err}"))
    })
}

fn bin_value_type(input: &DataType) -> DataType {
    match input {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => input.clone(),
        _ => DataType::Float64,
    }
}

#[derive(Debug, Clone, Copy)]
struct Bin {
    center: f64,
    count: f64,
}

impl Bin {
    fn merged(self, other: Self) -> Self {
        let total = self.count + other.count;
        Self {
            center: self.center * (self.count / total) + other.center * (other.count / total),
            count: total,
        }
    }
}

fn insert_bin(bins: &mut Vec<Bin>, center: f64, count: f64, nbins: usize) {
    let position = bins
        .iter()
        .position(|bin| center.total_cmp(&bin.center).is_le())
        .unwrap_or(bins.len());
    bins.insert(position, Bin { center, count });
    while bins.len() > nbins {
        let mut best = 0;
        let mut best_distance = f64::INFINITY;
        for index in 0..bins.len() - 1 {
            let distance = (bins[index + 1].center - bins[index].center).abs();
            if distance <= best_distance {
                best_distance = distance;
                best = index;
            }
        }
        let merged = bins[best].merged(bins[best + 1]);
        bins[best] = merged;
        bins.remove(best + 1);
    }
}

impl AggregateUDFImpl for SparkHistogramNumeric {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "histogram_numeric"
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(histogram_name(&params.args))
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.schema_name(params)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "histogram_numeric requires 2 parameters but got {}",
                arg_types.len()
            )));
        }
        Ok(vec![bin_value_type(&arg_types[0]), arg_types[1].clone()])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let input = arg_types.first().ok_or_else(|| {
            DataFusionError::Plan("histogram_numeric requires 2 parameters but got 0".to_string())
        })?;
        let value_type = bin_value_type(input);
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("x", value_type, true),
                    Field::new("y", DataType::Float64, true),
                ]
                .into(),
            ),
            true,
        ))))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.exprs.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "histogram_numeric requires 2 parameters but got {}",
                acc_args.exprs.len()
            )));
        }
        let display = || {
            let value = acc_args
                .exprs
                .first()
                .map(physical_column_name)
                .unwrap_or_default();
            let bins = acc_args
                .exprs
                .get(1)
                .and_then(physical_literal)
                .and_then(|literal| literal_bins(&literal))
                .map(|bins| bins.to_string())
                .unwrap_or_default();
            format!("histogram_numeric({value}, {bins})")
        };
        let nbins = resolve_bins(&acc_args.exprs[1], &display)?;
        let value_type = acc_args
            .expr_fields
            .first()
            .map_or(DataType::Float64, |field| bin_value_type(field.data_type()));
        Ok(Box::new(HistogramAccumulator::new(nbins, value_type)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let bin = DataType::Struct(
            vec![
                Field::new("x", DataType::Float64, true),
                Field::new("y", DataType::Float64, true),
            ]
            .into(),
        );
        let items = Arc::new(Field::new("item", bin, true));
        Ok(vec![Arc::new(Field::new(
            format_state_name(self.name(), "bins"),
            DataType::List(items),
            true,
        ))])
    }
}

#[derive(Debug)]
struct HistogramAccumulator {
    bins: Vec<Bin>,
    nbins: usize,
    value_type: DataType,
}

impl HistogramAccumulator {
    fn new(nbins: usize, value_type: DataType) -> Self {
        Self {
            bins: Vec::new(),
            nbins,
            value_type,
        }
    }
}

impl Accumulator for HistogramAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let centers =
            Float64Array::from(self.bins.iter().map(|bin| bin.center).collect::<Vec<_>>());
        let counts = Float64Array::from(self.bins.iter().map(|bin| bin.count).collect::<Vec<_>>());
        let bins = StructArray::new(
            vec![
                Arc::new(Field::new("x", DataType::Float64, true)),
                Arc::new(Field::new("y", DataType::Float64, true)),
            ]
            .into(),
            vec![Arc::new(centers), Arc::new(counts)],
            None,
        );
        Ok(vec![ScalarValue::List(Arc::new(single_row_list(
            Arc::new(bins),
            DataType::Struct(
                vec![
                    Field::new("x", DataType::Float64, true),
                    Field::new("y", DataType::Float64, true),
                ]
                .into(),
            ),
        )))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let doubles = cast(&values[0], &DataType::Float64).map_err(|err| {
            DataFusionError::Plan(format!("histogram_numeric value must be numeric: {err}"))
        })?;
        let doubles = doubles
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Plan("histogram_numeric value must be numeric".to_string())
            })?;
        for row in 0..doubles.len() {
            if doubles.is_null(row) {
                continue;
            }
            insert_bin(&mut self.bins, doubles.value(row), 1.0, self.nbins);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let values = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("histogram_numeric state must hold lists".to_string())
            })?;
        for group in 0..values.len() {
            if values.is_null(group) {
                continue;
            }
            let group_values = values.value(group);
            let group_values = group_values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan("histogram_numeric state must hold bins".to_string())
                })?;
            let centers = group_values
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Plan("histogram_numeric state must hold doubles".to_string())
                })?;
            let counts = group_values
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Plan("histogram_numeric state must hold doubles".to_string())
                })?;
            for row in 0..centers.len() {
                if centers.is_null(row) || counts.is_null(row) {
                    continue;
                }
                insert_bin(
                    &mut self.bins,
                    centers.value(row),
                    counts.value(row),
                    self.nbins,
                );
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.bins.is_empty() {
            return Ok(ScalarValue::List(Arc::new(ListArray::new_null(
                Arc::new(Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new("x", self.value_type.clone(), true),
                            Field::new("y", DataType::Float64, true),
                        ]
                        .into(),
                    ),
                    true,
                )),
                1,
            ))));
        }
        let centers =
            Float64Array::from(self.bins.iter().map(|bin| bin.center).collect::<Vec<_>>());
        let heights = Float64Array::from(self.bins.iter().map(|bin| bin.count).collect::<Vec<_>>());
        let centers: ArrayRef = Arc::new(centers);
        let axis = cast(&centers, &self.value_type).map_err(|err| {
            DataFusionError::Plan(format!("histogram_numeric axis must cast back: {err}"))
        })?;
        let histogram = StructArray::new(
            vec![
                Arc::new(Field::new("x", self.value_type.clone(), true)),
                Arc::new(Field::new("y", DataType::Float64, true)),
            ]
            .into(),
            vec![axis, Arc::new(heights)],
            None,
        );
        let height_type = DataType::Struct(
            vec![
                Field::new("x", self.value_type.clone(), true),
                Field::new("y", DataType::Float64, true),
            ]
            .into(),
        );
        Ok(ScalarValue::List(Arc::new(single_row_list(
            Arc::new(histogram),
            height_type,
        ))))
    }

    fn size(&self) -> usize {
        self.bins.len() * 16 + 64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udaf(histogram_numeric_udaf().as_ref().clone());
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

    async fn failure(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(failure) => failure.to_string(),
            Ok(frame) => frame.collect().await.expect_err("must fail").to_string(),
        }
    }

    fn centers(batch: &RecordBatch) -> Vec<Option<f64>> {
        let lists = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("ListArray");
        let row = lists.value(0);
        let structs = row
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("StructArray");
        let axis = structs
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("ints");
        let heights = structs
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("doubles");
        assert_eq!(
            heights.values(),
            &[1.0, 2.0],
            "expected the fixture merge shape"
        );
        (0..axis.len())
            .map(|row| axis.is_valid(row).then(|| f64::from(axis.value(row))))
            .collect()
    }

    #[tokio::test]
    async fn two_bins_merge_closest_pair() {
        let batch = run(
            &ctx(),
            "SELECT histogram_numeric(v, 2) FROM (VALUES (CAST(10 AS INT)), (CAST(20 AS INT)), (CAST(30 AS INT))) AS t(v)",
        )
        .await;
        assert_eq!(centers(&batch), vec![Some(10.0), Some(25.0)]);
    }

    #[tokio::test]
    async fn single_bin_per_value_below_budget() {
        let batch = run(
            &ctx(),
            "SELECT histogram_numeric(v, 3) FROM (VALUES (1.5), (2.5), (-4.0)) AS t(v)",
        )
        .await;
        let lists = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("ListArray");
        let row = lists.value(0);
        let structs = row
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("StructArray");
        let axis = structs
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("doubles");
        assert_eq!(axis.values(), &[-4.0, 1.5, 2.5]);
    }

    #[tokio::test]
    async fn merge_inserts_weighted_bins_in_order() {
        let mut first = HistogramAccumulator::new(2, DataType::Float64);
        first
            .update_batch(&[Arc::new(Float64Array::from(vec![30.0])) as ArrayRef])
            .expect("update");
        let mut second = HistogramAccumulator::new(2, DataType::Float64);
        second
            .update_batch(&[Arc::new(Float64Array::from(vec![10.0, 20.0])) as ArrayRef])
            .expect("update");
        let mut merged = HistogramAccumulator::new(2, DataType::Float64);
        for state in [
            first.state().expect("state"),
            second.state().expect("state"),
        ] {
            let arrays = state
                .iter()
                .map(|scalar| scalar.to_array_of_size(1).expect("state array"))
                .collect::<Vec<_>>();
            merged.merge_batch(&arrays).expect("merge");
        }
        assert_eq!(
            merged
                .bins
                .iter()
                .map(|bin| (bin.center, bin.count))
                .collect::<Vec<_>>(),
            vec![(10.0, 1.0), (25.0, 2.0)]
        );
    }

    #[tokio::test]
    async fn int_axis_truncates_toward_zero() {
        let batch = run(
            &ctx(),
            "SELECT histogram_numeric(v, 3) FROM (VALUES (5), (1), (9), (3), (7), (2), (8), (4), (6), (0)) AS t(v)",
        )
        .await;
        let lists = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("ListArray");
        let row = lists.value(0);
        let structs = row
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("StructArray");
        let axis = structs
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("ints");
        let heights = structs
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("doubles");
        assert_eq!(
            (0..axis.len())
                .map(|row| axis.value(row))
                .collect::<Vec<_>>(),
            vec![1, 4, 7]
        );
        assert_eq!(heights.values(), &[3.0, 3.0, 4.0]);
    }

    #[tokio::test]
    async fn one_bin_is_out_of_range() {
        let refused = failure(
            &ctx(),
            "SELECT histogram_numeric(v, 1) FROM (VALUES (10), (20)) AS t(v)",
        )
        .await;
        assert!(
            refused.contains("[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE]"),
            "{refused}"
        );
    }

    #[tokio::test]
    async fn merge_matches_single_partition() {
        let values = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let single = {
            let mut accumulator = HistogramAccumulator::new(2, DataType::Float64);
            accumulator
                .update_batch(&[Arc::new(values.clone()) as ArrayRef])
                .expect("update");
            accumulator.evaluate().expect("evaluate")
        };
        let merged = {
            let mut first = HistogramAccumulator::new(2, DataType::Float64);
            let mut second = HistogramAccumulator::new(2, DataType::Float64);
            first
                .update_batch(&[Arc::new(values.slice(0, 2)) as ArrayRef])
                .expect("update");
            second
                .update_batch(&[Arc::new(values.slice(2, 1)) as ArrayRef])
                .expect("update");
            let first_state = first.state().expect("state");
            let second_state = second.state().expect("state");
            let mut merged = HistogramAccumulator::new(2, DataType::Float64);
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
    }
}
