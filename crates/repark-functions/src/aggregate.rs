//! Aggregate UDF shims over `datafusion-spark` gaps.
//!
//! **R-RETRACT-SHIM (X2):** `datafusion-spark`'s `SparkAvg` / `AvgAccumulator` never overrides
//! `retract_batch`, so sliding-frame `AVG(...) OVER (ROWS BETWEEN …)` dies with
//! "Aggregate can not be used as a sliding accumulator". Core DataFusion's Float64 avg already
//! implements retract (reference: `datafusion-functions-aggregate` 52.5 `average.rs`). This
//! module registers a same-name `avg` [`AggregateUDF`] that mirrors Spark's i64-count / null-on-
//! empty semantics **and** implements Float64 `retract_batch` (everywhere-for-Float64; existing
//! `f64::to_bits` aggregation pins are the tripwire for non-sliding drift).
//!
//! **DEC-5 / Z-3 U1:** `SparkAvg` (and the X2 overwrite) coerced every Numeric — including
//! `DECIMAL` — to `Float64`. DataFusion's native `Avg` already implements Spark's
//! `DECIMAL(p,s) → DECIMAL(min(38,p+4), min(38,s+4))` rule *and* decimal `retract_batch`.
//! This overwrite now keeps that decimal arm (a small copy of DF's `DecimalAvgAccumulator` +
//! `DecimalAverager`) so sliding `avg(DECIMAL)` does not silently ride the float path.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, AsArray};
use arrow::compute::{cast, sum};
use arrow::datatypes::{
    ArrowNativeType, DECIMAL32_MAX_PRECISION, DECIMAL32_MAX_SCALE, DECIMAL64_MAX_PRECISION,
    DECIMAL64_MAX_SCALE, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION,
    DECIMAL256_MAX_SCALE, DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    DecimalType, Field, FieldRef, Float64Type, Int64Type,
};
use datafusion::common::types::{NativeType, logical_float64};
use datafusion::common::{Result, ScalarValue, exec_err, not_impl_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Coercion, ReversedUDAF, Signature, TypeSignature,
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

/// Spark-compatible AVG: Float64 retract (X2) plus Spark-typed decimal avg with retract (DEC-5).
///
/// Signature is DF `Avg`'s shape, not `SparkAvg`'s: `DECIMAL` stays decimal (exact), integers
/// and floats still coerce to `Float64`. Coercing `TypeSignatureClass::Numeric` would send
/// money through the float path again.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SparkAvgWithRetract {
    signature: Signature,
}

impl SparkAvgWithRetract {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Decimal,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_float64()),
                        vec![TypeSignatureClass::Integer, TypeSignatureClass::Float],
                        NativeType::Float64,
                    )]),
                ],
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(DataType::Decimal32(precision, scale)) => Ok(DataType::Decimal32(
                DECIMAL32_MAX_PRECISION.min(*precision + 4),
                DECIMAL32_MAX_SCALE.min(*scale + 4),
            )),
            Some(DataType::Decimal64(precision, scale)) => Ok(DataType::Decimal64(
                DECIMAL64_MAX_PRECISION.min(*precision + 4),
                DECIMAL64_MAX_SCALE.min(*scale + 4),
            )),
            Some(DataType::Decimal128(precision, scale)) => Ok(DataType::Decimal128(
                DECIMAL128_MAX_PRECISION.min(*precision + 4),
                DECIMAL128_MAX_SCALE.min(*scale + 4),
            )),
            Some(DataType::Decimal256(precision, scale)) => Ok(DataType::Decimal256(
                DECIMAL256_MAX_PRECISION.min(*precision + 4),
                DECIMAL256_MAX_SCALE.min(*scale + 4),
            )),
            _ => Ok(DataType::Float64),
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("DistinctAvgAccumulator");
        }
        let data_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        match (&data_type, acc_args.return_type()) {
            (DataType::Float64, DataType::Float64) => {
                Ok(Box::<AvgAccumulatorWithRetract>::default())
            }
            (
                DataType::Decimal32(sum_precision, sum_scale),
                DataType::Decimal32(target_precision, target_scale),
            ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal32Type> {
                sum: None,
                count: 0,
                sum_scale: *sum_scale,
                sum_precision: *sum_precision,
                target_precision: *target_precision,
                target_scale: *target_scale,
            })),
            (
                DataType::Decimal64(sum_precision, sum_scale),
                DataType::Decimal64(target_precision, target_scale),
            ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal64Type> {
                sum: None,
                count: 0,
                sum_scale: *sum_scale,
                sum_precision: *sum_precision,
                target_precision: *target_precision,
                target_scale: *target_scale,
            })),
            (
                DataType::Decimal128(sum_precision, sum_scale),
                DataType::Decimal128(target_precision, target_scale),
            ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal128Type> {
                sum: None,
                count: 0,
                sum_scale: *sum_scale,
                sum_precision: *sum_precision,
                target_precision: *target_precision,
                target_scale: *target_scale,
            })),
            (
                DataType::Decimal256(sum_precision, sum_scale),
                DataType::Decimal256(target_precision, target_scale),
            ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal256Type> {
                sum: None,
                count: 0,
                sum_scale: *sum_scale,
                sum_precision: *sum_precision,
                target_precision: *target_precision,
                target_scale: *target_scale,
            })),
            (data_type, return_type) => {
                not_impl_err!("AvgAccumulator for ({data_type} --> {return_type})")
            }
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.input_fields[0].data_type().is_decimal() {
            // DF native decimal avg: count (u64) then sum — merge_batch depends on it.
            Ok(vec![
                Arc::new(Field::new(
                    format_state_name(self.name(), "count"),
                    DataType::UInt64,
                    true,
                )),
                Arc::new(Field::new(
                    format_state_name(self.name(), "sum"),
                    args.input_fields[0].data_type().clone(),
                    true,
                )),
            ])
        } else {
            // datafusion-spark SparkAvg: sum then count (i64) — merge_batch depends on it.
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
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        // Empty-group null must match the return type (decimal or float), not always Float64.
        ScalarValue::try_from(data_type)
    }
}

/// Small copy of DF `DecimalAverager` (`datafusion-functions-aggregate-common` 54.1 `utils.rs`).
/// Rescales `sum` (at `sum_scale`) into `target_scale` then divides by `count`.
struct DecimalAverager<T: DecimalType> {
    sum_mul: T::Native,
    target_mul: T::Native,
    target_precision: u8,
    target_scale: i8,
}

impl<T: DecimalType> DecimalAverager<T> {
    fn try_new(sum_scale: i8, target_precision: u8, target_scale: i8) -> Result<Self> {
        let ten = T::Native::from_usize(10).ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "DecimalAverager: 10 does not fit the decimal native type".to_string(),
            )
        })?;
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let sum_mul = ten.pow_wrapping(sum_scale as u32);
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let target_mul = ten.pow_wrapping(target_scale as u32);
        if target_mul >= sum_mul {
            Ok(Self {
                sum_mul,
                target_mul,
                target_precision,
                target_scale,
            })
        } else {
            exec_err!("Arithmetic Overflow in AvgAccumulator")
        }
    }

    fn avg(&self, sum: T::Native, count: T::Native) -> Result<T::Native> {
        let Ok(value) = sum.mul_checked(self.target_mul.div_wrapping(self.sum_mul)) else {
            return exec_err!("Arithmetic Overflow in AvgAccumulator");
        };
        let new_value = value.div_wrapping(count);
        if T::validate_decimal_precision(new_value, self.target_precision, self.target_scale)
            .is_ok()
        {
            Ok(new_value)
        } else {
            exec_err!("Arithmetic Overflow in AvgAccumulator")
        }
    }
}

/// Small copy of DF `DecimalAvgAccumulator` (54.1 `average.rs`) — includes `retract_batch`.
#[derive(Debug)]
struct DecimalAvgAccumulator<T: DecimalType + ArrowNumericType + std::fmt::Debug> {
    sum: Option<T::Native>,
    count: u64,
    sum_scale: i8,
    sum_precision: u8,
    target_precision: u8,
    target_scale: i8,
}

impl<T> Accumulator for DecimalAvgAccumulator<T>
where
    T: DecimalType + ArrowNumericType + std::fmt::Debug,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.count += u64::try_from(values.len() - values.null_count()).map_err(|_| {
            datafusion::common::DataFusionError::Internal(
                "avg update_batch: batch length does not fit u64".to_string(),
            )
        })?;
        if let Some(total) = sum(values) {
            let slot = self.sum.get_or_insert_with(T::Native::default);
            self.sum = Some(slot.add_wrapping(total));
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = if self.count == 0 {
            None
        } else {
            self.sum
                .map(|total| {
                    let count_native = usize::try_from(self.count)
                        .ok()
                        .and_then(T::Native::from_usize)
                        .ok_or_else(|| {
                            datafusion::common::DataFusionError::Execution(
                                "avg count does not fit the decimal native type".to_string(),
                            )
                        })?;
                    DecimalAverager::<T>::try_new(
                        self.sum_scale,
                        self.target_precision,
                        self.target_scale,
                    )?
                    .avg(total, count_native)
                })
                .transpose()?
        };
        ScalarValue::new_primitive::<T>(
            value,
            &T::TYPE_CONSTRUCTOR(self.target_precision, self.target_scale),
        )
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::new_primitive::<T>(
                self.sum,
                &T::TYPE_CONSTRUCTOR(self.sum_precision, self.sum_scale),
            )?,
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        use arrow::datatypes::UInt64Type;
        self.count += sum(states[0].as_primitive::<UInt64Type>()).unwrap_or_default();
        if let Some(total) = sum(states[1].as_primitive::<T>()) {
            let slot = self.sum.get_or_insert_with(T::Native::default);
            self.sum = Some(slot.add_wrapping(total));
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.count =
            self.count
                .saturating_sub(u64::try_from(values.len() - values.null_count()).map_err(
                    |_| {
                        datafusion::common::DataFusionError::Internal(
                            "avg retract_batch: batch length does not fit u64".to_string(),
                        )
                    },
                )?);
        if let Some(total) = sum(values) {
            let current = self.sum.unwrap_or_default();
            self.sum = Some(current.sub_wrapping(total));
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
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
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
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
    async fn group_avg_decimal128_stays_decimal_14_6_i128() {
        // DEC-5 / Z-3 U1: facade `avg(DECIMAL(10,2))` must keep Spark's (14,6), i128=1_650_000.
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let batches = ctx
            .sql(
                "SELECT avg(v) AS a FROM (\
                   SELECT CAST('1.10' AS DECIMAL(10,2)) AS v \
                   UNION ALL SELECT CAST('2.20' AS DECIMAL(10,2))\
                 ) t",
            )
            .await
            .expect("plan decimal avg")
            .collect()
            .await
            .expect("execute decimal avg");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .expect("decimal128, not float64");
        assert_eq!(column.precision(), 14);
        assert_eq!(column.scale(), 6);
        assert!(
            batches[0].schema().field(0).is_nullable(),
            "avg is nullable"
        );
        assert_eq!(
            column.value(0),
            1_650_000,
            "i128 scaled 1.650000 at scale 6"
        );
    }

    #[tokio::test]
    async fn empty_group_decimal_avg_is_null_at_14_6() {
        // default_value / empty-group path must be decimal NULL, not Float64(None).
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let batches = ctx
            .sql(
                "SELECT avg(v) AS a FROM (\
                   SELECT CAST('1.10' AS DECIMAL(10,2)) AS v WHERE false\
                 ) t",
            )
            .await
            .expect("plan empty decimal avg")
            .collect()
            .await
            .expect("execute empty decimal avg");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .expect("decimal128 empty avg");
        assert_eq!(column.precision(), 14);
        assert_eq!(column.scale(), 6);
        assert!(column.is_null(0), "empty group avg is NULL");
    }

    #[tokio::test]
    async fn sliding_avg_decimal128_retracts() {
        // Sliding decimal avg must use decimal retract, never the float path.
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let batches = ctx
            .sql(
                "SELECT avg(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a \
                 FROM (VALUES \
                   (1, CAST('1.00' AS DECIMAL(10,2))), \
                   (2, CAST('3.00' AS DECIMAL(10,2))), \
                   (3, CAST('5.00' AS DECIMAL(10,2)))\
                 ) t(id, v) \
                 ORDER BY id",
            )
            .await
            .expect("plan sliding decimal avg")
            .collect()
            .await
            .expect("execute sliding decimal avg");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .expect("decimal128 sliding avg");
        assert_eq!(column.precision(), 14);
        assert_eq!(column.scale(), 6);
        // Frame width 2: 1.00; (1+3)/2=2.00; (3+5)/2=4.00 at scale 6.
        assert_eq!(column.value(0), 1_000_000);
        assert_eq!(column.value(1), 2_000_000);
        assert_eq!(column.value(2), 4_000_000);
    }

    #[test]
    fn decimal_retract_batch_returns_to_empty() {
        let mut acc = DecimalAvgAccumulator::<Decimal128Type> {
            sum: None,
            count: 0,
            sum_scale: 2,
            sum_precision: 10,
            target_precision: 14,
            target_scale: 6,
        };
        let batch = Arc::new(
            arrow::array::Decimal128Array::from(vec![Some(110), Some(220), None])
                .with_precision_and_scale(10, 2)
                .expect("fixture scale"),
        ) as ArrayRef;
        acc.update_batch(&[Arc::clone(&batch)]).expect("update");
        assert_eq!(acc.count, 2);
        acc.retract_batch(&[batch]).expect("retract");
        assert_eq!(acc.count, 0);
        let evaluated = acc.evaluate().expect("empty frame is null");
        assert!(evaluated.is_null(), "empty retracted frame must be NULL");
    }

    fn register_decimal_column(ctx: &SessionContext, column: ArrayRef, data_type: DataType) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", data_type, true)]));
        let batch = RecordBatch::try_new(schema, vec![column]).expect("decimal avg fixture");
        ctx.register_batch("t", batch)
            .expect("register decimal avg fixture");
    }

    /// Z-3 S3 residual: Decimal32 accumulator arm. Mutation-red if the match arm is dropped
    /// (`not_impl` at plan/exec, or a silent coerce to Float64).
    #[tokio::test]
    async fn group_avg_decimal32_stays_decimal_9_6_i32() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let array = arrow::array::Decimal32Array::from(vec![Some(110), Some(220)])
            .with_precision_and_scale(5, 2)
            .expect("decimal32 fixture");
        register_decimal_column(&ctx, Arc::new(array), DataType::Decimal32(5, 2));
        let batches = ctx
            .sql("SELECT avg(v) AS a FROM t")
            .await
            .expect("plan decimal32 avg")
            .collect()
            .await
            .expect("execute decimal32 avg");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal32Array>()
            .expect("decimal32, not float64");
        assert_eq!(column.precision(), 9);
        assert_eq!(column.scale(), 6);
        assert_eq!(column.value(0), 1_650_000, "i32 scaled 1.650000 at scale 6");
    }

    /// Z-3 S3 residual: Decimal64 accumulator arm.
    #[tokio::test]
    async fn group_avg_decimal64_stays_decimal_14_6_i64() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let array = arrow::array::Decimal64Array::from(vec![Some(110), Some(220)])
            .with_precision_and_scale(10, 2)
            .expect("decimal64 fixture");
        register_decimal_column(&ctx, Arc::new(array), DataType::Decimal64(10, 2));
        let batches = ctx
            .sql("SELECT avg(v) AS a FROM t")
            .await
            .expect("plan decimal64 avg")
            .collect()
            .await
            .expect("execute decimal64 avg");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal64Array>()
            .expect("decimal64, not float64");
        assert_eq!(column.precision(), 14);
        assert_eq!(column.scale(), 6);
        assert_eq!(column.value(0), 1_650_000, "i64 scaled 1.650000 at scale 6");
    }

    /// Z-3 S3 residual: Decimal256 accumulator arm.
    #[tokio::test]
    async fn group_avg_decimal256_stays_decimal_14_6_i256() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let array = arrow::array::Decimal256Array::from(vec![
            Some(arrow::datatypes::i256::from_i128(110)),
            Some(arrow::datatypes::i256::from_i128(220)),
        ])
        .with_precision_and_scale(10, 2)
        .expect("decimal256 fixture");
        register_decimal_column(&ctx, Arc::new(array), DataType::Decimal256(10, 2));
        let batches = ctx
            .sql("SELECT avg(v) AS a FROM t")
            .await
            .expect("plan decimal256 avg")
            .collect()
            .await
            .expect("execute decimal256 avg");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal256Array>()
            .expect("decimal256, not float64");
        assert_eq!(column.precision(), 14);
        assert_eq!(column.scale(), 6);
        assert_eq!(
            column.value(0),
            arrow::datatypes::i256::from_i128(1_650_000),
            "i256 scaled 1.650000 at scale 6"
        );
    }

    #[tokio::test]
    async fn integer_avg_still_returns_float64() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let batches = ctx
            .sql("SELECT avg(v) AS a FROM (VALUES (1), (3), (5)) t(v)")
            .await
            .expect("plan int avg")
            .collect()
            .await
            .expect("execute int avg");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("integer avg stays Float64");
        assert!((column.value(0) - 3.0).abs() < 1e-12);
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
