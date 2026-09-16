use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

use crate::max_min_by::unqualified_name;

#[must_use]
pub fn moment_udaf(kind: &str) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(CentralMoments::new(
        kind == "kurtosis",
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CentralMoments {
    signature: Signature,
    is_kurtosis: bool,
}

impl CentralMoments {
    fn new(is_kurtosis: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            is_kurtosis,
        }
    }

    fn spark_display(&self, params: &AggregateFunctionParams) -> String {
        let arg = params
            .args
            .first()
            .map(unqualified_name)
            .unwrap_or_default();
        format!("{}({arg})", self.name())
    }
}

impl AggregateUDFImpl for CentralMoments {
    fn name(&self) -> &str {
        if self.is_kurtosis {
            "kurtosis"
        } else {
            "skewness"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(self.spark_display(params))
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(self.spark_display(params))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return Err(DataFusionError::Plan(format!(
                "{} does not support DISTINCT",
                self.name()
            )));
        }
        Ok(Box::new(MomentsAccumulator::new(self.is_kurtosis)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(self.name(), "n"),
                DataType::Int64,
                false,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "mean"),
                DataType::Float64,
                false,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "m2"),
                DataType::Float64,
                false,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "m3"),
                DataType::Float64,
                false,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "m4"),
                DataType::Float64,
                false,
            )),
        ])
    }
}

#[derive(Debug, Default)]
struct MomentsAccumulator {
    is_kurtosis: bool,
    n: u64,
    mean: f64,
    m2: f64,
    m3: f64,
    m4: f64,
}

impl MomentsAccumulator {
    fn new(is_kurtosis: bool) -> Self {
        Self {
            is_kurtosis,
            ..Self::default()
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn observe(&mut self, value: f64) {
        let previous = self.n as f64;
        self.n += 1;
        let count = self.n as f64;
        let delta = value - self.mean;
        let delta_n = delta / count;
        let delta_n2 = delta_n * delta_n;
        let term = delta * delta_n * previous;
        self.mean += delta_n;
        self.m4 += term * delta_n2 * (count * count - 3.0 * count + 3.0) + 6.0 * delta_n2 * self.m2
            - 4.0 * delta_n * self.m3;
        self.m3 += term * delta_n * (count - 2.0) - 3.0 * delta_n * self.m2;
        self.m2 += term;
    }

    #[allow(clippy::cast_precision_loss)]
    fn ingest(&mut self, other: &Self) {
        if other.n == 0 {
            return;
        }
        if self.n == 0 {
            *self = Self {
                is_kurtosis: self.is_kurtosis,
                n: other.n,
                mean: other.mean,
                m2: other.m2,
                m3: other.m3,
                m4: other.m4,
            };
            return;
        }
        let left_n = self.n as f64;
        let right_n = other.n as f64;
        let count = left_n + right_n;
        let delta = other.mean - self.mean;
        let delta2 = delta * delta;
        let delta3 = delta2 * delta;
        let delta4 = delta2 * delta2;
        self.m4 += other.m4
            + delta4 * left_n * right_n * (left_n * left_n - left_n * right_n + right_n * right_n)
                / (count * count * count)
            + 6.0 * delta2 * (left_n * left_n * other.m2 + right_n * right_n * self.m2)
                / (count * count)
            + 4.0 * delta * (left_n * other.m3 - right_n * self.m3) / count;
        self.m3 += other.m3
            + delta3 * left_n * right_n * (left_n - right_n) / (count * count)
            + 3.0 * delta * (left_n * other.m2 - right_n * self.m2) / count;
        self.m2 += other.m2 + delta2 * left_n * right_n / count;
        self.mean += delta * right_n / count;
        self.n += other.n;
    }

    #[allow(clippy::cast_precision_loss)]
    fn result(&self) -> Option<f64> {
        if self.n < 2 || self.m2 == 0.0 {
            return None;
        }
        let count = self.n as f64;
        if self.is_kurtosis {
            Some(count * self.m4 / (self.m2 * self.m2) - 3.0)
        } else {
            Some(count.sqrt() * self.m3 / self.m2.powf(1.5))
        }
    }
}

impl Accumulator for MomentsAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let casted = cast(&values[0], &DataType::Float64)?;
        let floats = casted
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("kurtosis/skewness input mistyped".to_string())
            })?;
        for index in 0..floats.len() {
            if !floats.is_null(index) {
                self.observe(floats.value(index));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(self.result()))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let count = i64::try_from(self.n).map_err(|_| {
            DataFusionError::Execution("kurtosis/skewness state count out of range".to_string())
        })?;
        Ok(vec![
            ScalarValue::Int64(Some(count)),
            ScalarValue::Float64(Some(self.mean)),
            ScalarValue::Float64(Some(self.m2)),
            ScalarValue::Float64(Some(self.m3)),
            ScalarValue::Float64(Some(self.m4)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = states[0]
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("kurtosis/skewness state count mistyped".to_string())
            })?;
        let moment = |index: usize| -> Result<Vec<f64>> {
            let floats = states[index]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "kurtosis/skewness state moment mistyped".to_string(),
                    )
                })?;
            Ok((0..floats.len()).map(|row| floats.value(row)).collect())
        };
        let means = moment(1)?;
        let seconds = moment(2)?;
        let thirds = moment(3)?;
        let fourths = moment(4)?;
        for row in 0..counts.len() {
            let other = MomentsAccumulator {
                is_kurtosis: self.is_kurtosis,
                n: u64::try_from(counts.value(row).max(0)).map_err(|_| {
                    DataFusionError::Execution(
                        "kurtosis/skewness state count out of range".to_string(),
                    )
                })?,
                mean: means[row],
                m2: seconds[row],
                m3: thirds[row],
                m4: fourths[row],
            };
            self.ingest(&other);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udaf(moment_udaf("kurtosis").as_ref().clone());
        ctx.register_udaf(moment_udaf("skewness").as_ref().clone());
        ctx
    }

    async fn batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
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

    fn double(batch: &RecordBatch, column: usize, row: usize) -> Option<f64> {
        let values = batch
            .column(column)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        values.is_valid(row).then(|| values.value(row))
    }

    #[tokio::test]
    async fn grouped_moments_match_spark_oracle_cells() {
        let batch = batch(
            &ctx(),
            "SELECT kurtosis(v), kurtosis(d), skewness(v), skewness(d) FROM (VALUES \
             (10, 1.5), (20, 2.5), (NULL, NULL), (30, -4.0)) AS t(v, d)",
        )
        .await;
        assert_eq!(double(&batch, 0, 0), Some(-1.5));
        assert_eq!(double(&batch, 1, 0), Some(-1.5));
        assert_eq!(double(&batch, 2, 0), Some(0.0));
        assert_eq!(double(&batch, 3, 0), Some(-0.642_723_256_123_866));
    }

    #[tokio::test]
    async fn global_moments_match_spark_oracle_cells() {
        let batch = batch(
            &ctx(),
            "SELECT kurtosis(v), skewness(v) FROM (VALUES (10), (20), (NULL), (30), (5), (NULL)) AS t(v)",
        )
        .await;
        assert_eq!(double(&batch, 0, 0), Some(-1.426_601_551_278_368_3));
        assert_eq!(double(&batch, 1, 0), Some(0.278_030_555_653_962_84));
    }

    #[tokio::test]
    async fn single_row_and_empty_groups_answer_null() {
        let single = batch(
            &ctx(),
            "SELECT kurtosis(v), skewness(v) FROM (VALUES (5)) AS t(v)",
        )
        .await;
        assert_eq!(double(&single, 0, 0), None);
        assert_eq!(double(&single, 1, 0), None);
        let empty = batch(
            &ctx(),
            "SELECT kurtosis(v), skewness(v) FROM (VALUES (CAST(NULL AS INT))) AS t(v)",
        )
        .await;
        assert_eq!(double(&empty, 0, 0), None);
        assert_eq!(double(&empty, 1, 0), None);
    }

    fn split_merged(values: &[f64], is_kurtosis: bool) -> Option<f64> {
        let arrays: Vec<ArrayRef> = values
            .chunks(2)
            .map(|chunk| Arc::new(Float64Array::from(chunk.to_vec())) as ArrayRef)
            .collect();
        let mut partials = Vec::new();
        for array in &arrays {
            let mut accumulator = MomentsAccumulator::new(is_kurtosis);
            accumulator
                .update_batch(std::slice::from_ref(array))
                .expect("partial update");
            partials.push(accumulator.state().expect("partial state"));
        }
        let states: Vec<ArrayRef> = (0..5)
            .map(|column| {
                ScalarValue::iter_to_array(
                    partials
                        .iter()
                        .map(|state| state[column].clone())
                        .collect::<Vec<_>>(),
                )
                .expect("state column")
            })
            .collect();
        let mut merged = MomentsAccumulator::new(is_kurtosis);
        merged.merge_batch(&states).expect("merge");
        floated(merged.evaluate().expect("evaluate"))
    }

    fn single(values: &[f64], is_kurtosis: bool) -> Option<f64> {
        let array: ArrayRef = Arc::new(Float64Array::from(values.to_vec()));
        let mut accumulator = MomentsAccumulator::new(is_kurtosis);
        accumulator
            .update_batch(std::slice::from_ref(&array))
            .expect("update");
        floated(accumulator.evaluate().expect("evaluate"))
    }

    fn floated(value: ScalarValue) -> Option<f64> {
        match value {
            ScalarValue::Float64(value) => value,
            other => panic!("moments result mistyped: {other}"),
        }
    }

    #[tokio::test]
    async fn merge_batch_matches_single_partition() {
        let values = vec![10.0, 20.0, 30.0, 5.0, 7.0, -3.0];
        assert_eq!(split_merged(&values, true), single(&values, true));
        assert_eq!(split_merged(&values, false), single(&values, false));
    }
}
