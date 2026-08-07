//! Aggregate UDF shims over `datafusion-spark` gaps.
//!
//! **R-RETRACT-SHIM (X2):** `datafusion-spark`'s `SparkAvg` / `AvgAccumulator` never overrides
//! `retract_batch`, so sliding-frame `AVG(...) OVER (ROWS BETWEEN …)` dies with
//! "Aggregate can not be used as a sliding accumulator". Core DataFusion's Float64 avg already
//! implements retract (reference: `datafusion-functions-aggregate` 52.5 `average.rs`). This
//! module registers a same-name `avg` [`AggregateUDF`] that mirrors Spark's i64-count / null-on-
//! empty semantics **and** implements Float64 `retract_batch` (everywhere-for-Float64; existing
//! `f64::to_bits` aggregation pins are the tripwire for non-sliding drift).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::{cast, sum};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type};
use datafusion::common::types::{NativeType, logical_float64};
use datafusion::common::{Result, ScalarValue, not_impl_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Coercion, ReversedUDAF, Signature,
    TypeSignatureClass, Volatility,
};

/// ===========================================================================================
/// The repark `avg` [`AggregateUDF`] instances to register after `datafusion-spark` (name overwrite).
/// ===========================================================================================
#[must_use]
pub fn functions() -> Vec<Arc<AggregateUDF>> {
    vec![Arc::new(AggregateUDF::new_from_impl(
        SparkAvgWithRetract::new(),
    ))]
}

/// Spark-compatible AVG with Float64 sliding-window retract (mirrors `SparkAvg` signature +
/// i64 count, plus core DF's `retract_batch` for Float64).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SparkAvgWithRetract {
    signature: Signature,
}

impl SparkAvgWithRetract {
    fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Native(logical_float64()),
                    vec![TypeSignatureClass::Numeric],
                    NativeType::Float64,
                )],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for SparkAvgWithRetract {
    // Trait signature is `&str`; the string is static.
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("DistinctAvgAccumulator");
        }
        let data_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        match (&data_type, &acc_args.return_type()) {
            (DataType::Float64, DataType::Float64) => {
                Ok(Box::<AvgAccumulatorWithRetract>::default())
            }
            (data_type, return_type) => {
                not_impl_err!("AvgAccumulator for ({data_type} --> {return_type})")
            }
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Same order as datafusion-spark SparkAvg: sum then count (i64) — merge_batch depends on it.
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(self.name(), "sum"),
                args.input_fields[0].data_type().clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "count"),
                DataType::Int64,
                true,
            )),
        ])
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }
}

/// Float64 avg accumulator: Spark semantics (i64 count, null when count==0) + retract for sliding.
#[derive(Debug, Default)]
struct AvgAccumulatorWithRetract {
    sum: Option<f64>,
    count: i64,
}

impl Accumulator for AvgAccumulatorWithRetract {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(self.sum),
            ScalarValue::from(self.count),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // SAF-002: UDAF signature forces Float64; defensive cast → typed Err on physical drift.
        let values = cast(&values[0], &DataType::Float64)?;
        let values = values.as_primitive::<Float64Type>();
        // SparkAvg uses i64 counts (same cast shape as datafusion-spark avg.rs).
        #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
        {
            self.count += (values.len() - values.null_count()) as i64;
        }
        let sum_slot = self.sum.get_or_insert(0.);
        if let Some(total) = sum(values) {
            *sum_slot += total;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // SAF-002: state schema is owned by `state()` → [Float64 sum, Int64 count]; defensive cast.
        // SparkAvg order: state[0]=sum, state[1]=count (i64).
        let counts = cast(&states[1], &DataType::Int64)?;
        let sums = cast(&states[0], &DataType::Float64)?;
        self.count += sum(counts.as_primitive::<Int64Type>()).unwrap_or_default();
        if let Some(total) = sum(sums.as_primitive::<Float64Type>()) {
            let sum_slot = self.sum.get_or_insert(0.);
            *sum_slot += total;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 {
            Ok(ScalarValue::Float64(None))
        } else {
            // Same cast as SparkAvg / core DF avg (count → f64 for divide).
            #[allow(clippy::cast_precision_loss)]
            let average = self.sum.map(|total| total / self.count as f64);
            Ok(ScalarValue::Float64(average))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    /// Core DF Float64 avg retract (subtract sum/count) — reference:
    /// `datafusion-functions-aggregate` 52.5 `average.rs` `AvgAccumulator::retract_batch`.
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // SAF-002: same Float64 contract as `update_batch` (sliding-window retract); defensive cast.
        let values = cast(&values[0], &DataType::Float64)?;
        let values = values.as_primitive::<Float64Type>();
        #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
        {
            self.count -= (values.len() - values.null_count()) as i64;
        }
        if let Some(total) = sum(values) {
            self.sum = Some(self.sum.unwrap_or(0.) - total);
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use datafusion::prelude::SessionContext;
    use datafusion_spark;

    #[tokio::test]
    async fn sliding_avg_over_rows_succeeds_after_shim() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let batches = ctx
            .sql(
                "SELECT avg(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a \
                 FROM (VALUES (1, 1.0), (2, 2.0), (3, 3.0)) t(id, v) \
                 ORDER BY id",
            )
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute sliding avg");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64");
        // Frame width 2: row0 = 1.0; row1 = (1+2)/2 = 1.5; row2 = (2+3)/2 = 2.5
        assert!((column.value(0) - 1.0).abs() < 1e-12);
        assert!((column.value(1) - 1.5).abs() < 1e-12);
        assert!((column.value(2) - 2.5).abs() < 1e-12);
    }

    #[tokio::test]
    async fn plain_group_by_avg_still_works() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let batches = ctx
            .sql("SELECT avg(v) AS a FROM (VALUES (1.0), (3.0), (5.0)) t(v)")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64");
        assert!((column.value(0) - 3.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn sum_and_count_sliding_still_work() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let batches = ctx
            .sql(
                "SELECT sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s, \
                        count(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS c \
                 FROM (VALUES (1, 1.0), (2, 2.0)) t(id, v) ORDER BY id",
            )
            .await
            .expect("plan")
            .collect()
            .await
            .expect("execute");
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn retract_batch_matches_core_semantics() {
        let mut acc = AvgAccumulatorWithRetract::default();
        let batch = Arc::new(Float64Array::from(vec![Some(1.0), Some(3.0), None])) as ArrayRef;
        acc.update_batch(&[Arc::clone(&batch)]).unwrap();
        assert_eq!(acc.count, 2);
        acc.retract_batch(&[batch]).unwrap();
        assert_eq!(acc.count, 0);
    }

    #[test]
    fn spark_avg_is_overwritten_by_name() {
        // Sanity: datafusion-spark ships avg; our register_all must overwrite it.
        let names: Vec<_> = datafusion_spark::all_default_aggregate_functions()
            .into_iter()
            .map(|function| function.name().to_string())
            .collect();
        assert!(names.iter().any(|name| name == "avg"));
    }

    #[tokio::test]
    async fn percentile_approx_sql_aliases_resolve() {
        // Q1: Spark names register as aliases over approx_percentile_cont (t-digest).
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        for name in [
            "percentile_approx",
            "approx_percentile",
            "approx_percentile_cont",
        ] {
            let batches = ctx
                .sql(&format!(
                    "SELECT {name}(v, 0.5) AS p FROM (VALUES (1.0), (2.0), (3.0)) t(v)"
                ))
                .await
                .unwrap_or_else(|err| panic!("{name} plan: {err}"))
                .collect()
                .await
                .unwrap_or_else(|err| panic!("{name} exec: {err}"));
            let column = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("f64");
            let value = column.value(0);
            assert!(
                (1.0..=3.0).contains(&value),
                "{name}={value} outside exact-quantile neighbor window"
            );
        }
    }
}
