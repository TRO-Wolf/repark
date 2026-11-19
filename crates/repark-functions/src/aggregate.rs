//! Aggregate UDF shims over `datafusion-spark` gaps.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::{cast, sum};
use arrow::datatypes::{
    DECIMAL32_MAX_PRECISION, DECIMAL32_MAX_SCALE, DECIMAL64_MAX_PRECISION, DECIMAL64_MAX_SCALE,
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Field, FieldRef,
    Float64Type, Int64Type, IntervalUnit, TimeUnit,
};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::types::{NativeType, logical_float64};
use datafusion::common::{Result, ScalarValue, not_impl_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Coercion, Expr, GroupsAccumulator, LogicalPlan,
    ReversedUDAF, Signature, TypeSignature, TypeSignatureClass, Volatility,
    WindowFunctionDefinition,
};
use datafusion::optimizer::AnalyzerRule;

use crate::avg_decimal::DecimalAvgAccumulator;
use crate::avg_groups;

mod spark_sum;
mod sum_decimal;
#[cfg(test)]
mod sum_precision_tests;
#[cfg(test)]
mod sum_tests;

pub use crate::grouping::{grouping_id_udaf, grouping_udaf};

/// Register repark `avg` [`AggregateUDF`] instances after `datafusion-spark` (name overwrite).
#[must_use]
pub fn functions() -> Vec<Arc<AggregateUDF>> {
    let mut functions = vec![
        avg_udaf(),
        mean_udaf(),
        try_avg_udaf(),
        crate::count_if::count_if_udaf(),
        sum_udaf(),
        try_sum_udaf(),
    ];
    functions.push(crate::grouping::grouping_udaf());
    functions.push(crate::grouping::grouping_id_udaf());
    functions.extend(crate::bitmap_agg::functions());
    functions.extend(crate::spark_result_types::signed_aggregate_functions());
    functions
}

/// Spark `try_sum` — overflow yields NULL. Reuses the datafusion-spark kernel.
#[must_use]
pub fn try_sum_udaf() -> Arc<AggregateUDF> {
    spark_sum::try_sum_udaf()
}

#[must_use]
pub fn sum_udaf() -> Arc<AggregateUDF> {
    spark_sum::sum_udaf()
}

/// Spark `try_avg` — decimal overflow yields NULL; interval input refuses (FNP-11).
#[must_use]
pub fn try_avg_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkAvgWithRetract::new(
        "try_avg",
        AvgMode::Try,
    )))
}

/// Return Spark-compatible `avg` with Spark's null/count and argument contracts.
#[must_use]
pub fn avg_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkAvgWithRetract::new(
        "avg",
        AvgMode::Ansi,
    )))
}

fn mean_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkAvgWithRetract::new(
        "mean",
        AvgMode::Ansi,
    )))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AvgMode {
    Ansi,
    Legacy,
    Try,
}

/// Spark AVG keeps Float64 retract and exact decimal retract; integers and floats become `Float64`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SparkAvgWithRetract {
    signature: Signature,
    name: &'static str,
    mode: AvgMode,
}

fn numeric_avg_signatures() -> Vec<TypeSignature> {
    vec![
        TypeSignature::Coercible(vec![Coercion::new_exact(TypeSignatureClass::Decimal)]),
        TypeSignature::Coercible(vec![Coercion::new_implicit(
            TypeSignatureClass::Native(logical_float64()),
            vec![TypeSignatureClass::Integer, TypeSignatureClass::Float],
            NativeType::Float64,
        )]),
    ]
}

fn interval_avg_signatures() -> Vec<TypeSignature> {
    vec![
        TypeSignature::Exact(vec![DataType::Interval(IntervalUnit::YearMonth)]),
        TypeSignature::Exact(vec![DataType::Interval(IntervalUnit::DayTime)]),
        TypeSignature::Exact(vec![DataType::Interval(IntervalUnit::MonthDayNano)]),
        TypeSignature::Exact(vec![DataType::Duration(TimeUnit::Second)]),
        TypeSignature::Exact(vec![DataType::Duration(TimeUnit::Millisecond)]),
        TypeSignature::Exact(vec![DataType::Duration(TimeUnit::Microsecond)]),
        TypeSignature::Exact(vec![DataType::Duration(TimeUnit::Nanosecond)]),
    ]
}

impl SparkAvgWithRetract {
    fn new(name: &'static str, mode: AvgMode) -> Self {
        let mut signatures = numeric_avg_signatures();
        signatures.extend(interval_avg_signatures());
        Self {
            signature: Signature::one_of(signatures, Volatility::Immutable),
            name,
            mode,
        }
    }

    fn decimal_null_on_overflow(&self) -> bool {
        self.mode != AvgMode::Ansi
    }

    fn interval_null_on_overflow(&self) -> bool {
        self.mode == AvgMode::Try
    }

    fn keeps_core_distinct(&self, data_type: &DataType) -> bool {
        self.name == "mean" && data_type == &DataType::Float64
    }
}

impl AggregateUDFImpl for SparkAvgWithRetract {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if matches!(
            arg_types.first(),
            Some(DataType::Interval(_) | DataType::Duration(_))
        ) {
            return Ok(DataType::Interval(IntervalUnit::MonthDayNano));
        }
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
        let data_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        if acc_args.is_distinct {
            if self.keeps_core_distinct(&data_type) {
                return datafusion::functions_aggregate::average::avg_udaf().accumulator(acc_args);
            }
            return not_impl_err!("DistinctAvgAccumulator");
        }
        match (&data_type, acc_args.return_type()) {
            (DataType::Float64, DataType::Float64) => {
                Ok(Box::<AvgAccumulatorWithRetract>::default())
            }
            (
                DataType::Decimal32(sum_precision, sum_scale),
                DataType::Decimal32(target_precision, target_scale),
            ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal32Type>::try_new(
                (*sum_precision, *sum_scale),
                (*target_precision, *target_scale),
                self.decimal_null_on_overflow(),
            )?)),
            (
                DataType::Decimal64(sum_precision, sum_scale),
                DataType::Decimal64(target_precision, target_scale),
            ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal64Type>::try_new(
                (*sum_precision, *sum_scale),
                (*target_precision, *target_scale),
                self.decimal_null_on_overflow(),
            )?)),
            (
                DataType::Decimal128(sum_precision, sum_scale),
                DataType::Decimal128(target_precision, target_scale),
            ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal128Type>::try_new(
                (*sum_precision, *sum_scale),
                (*target_precision, *target_scale),
                self.decimal_null_on_overflow(),
            )?)),
            (
                DataType::Decimal256(sum_precision, sum_scale),
                DataType::Decimal256(target_precision, target_scale),
            ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal256Type>::try_new(
                (*sum_precision, *sum_scale),
                (*target_precision, *target_scale),
                self.decimal_null_on_overflow(),
            )?)),
            (DataType::Interval(_) | DataType::Duration(_), _) => Ok(Box::new(
                crate::interval_avg::IntervalAvgAccumulator::with_null_on_overflow(
                    self.interval_null_on_overflow(),
                ),
            )),
            (data_type, return_type) => {
                not_impl_err!("AvgAccumulator for ({data_type} --> {return_type})")
            }
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        avg_groups::groups_supported(args.return_field.data_type(), args.is_distinct)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let data_type = args.exprs[0].data_type(args.schema)?;
        avg_groups::create_groups(
            &data_type,
            args.return_type(),
            self.decimal_null_on_overflow(),
        )
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.is_distinct && self.keeps_core_distinct(args.input_fields[0].data_type()) {
            return datafusion::functions_aggregate::average::avg_udaf().state_fields(args);
        }
        if matches!(
            args.input_fields[0].data_type(),
            DataType::Interval(_) | DataType::Duration(_)
        ) {
            return Ok(crate::interval_avg::interval_state_fields());
        }
        if args.input_fields[0].data_type().is_decimal() {
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
        ScalarValue::try_from(data_type)
    }
}

#[derive(Debug, Default)]
pub(crate) struct SparkAvgAnsiMode;

impl AnalyzerRule for SparkAvgAnsiMode {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let ansi = crate::ansi::spark_ansi_enabled_from_options(config);
        plan.transform_up_with_subqueries(|node| {
            let place = spark_sum::place_of(spark_sum::grouped(&node));
            node.map_expressions(|expr| {
                expr.transform_up(|leaf| Ok(legacy_avg_expr(leaf, ansi, place)))
            })
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "spark_avg_ansi_mode"
    }
}

fn ansi_mode_udaf(
    func: &Arc<AggregateUDF>,
    ansi: bool,
    place: sum_decimal::Place,
) -> Option<Arc<AggregateUDF>> {
    if ansi {
        return spark_sum::placed(func, place);
    }
    legacy_avg_udaf(func).or_else(|| spark_sum::legacy(func))
}

fn legacy_avg_expr(expr: Expr, ansi: bool, place: sum_decimal::Place) -> Transformed<Expr> {
    match expr {
        Expr::AggregateFunction(mut function) => {
            match ansi_mode_udaf(&function.func, ansi, place) {
                Some(func) => {
                    function.func = func;
                    Transformed::yes(Expr::AggregateFunction(function))
                }
                None => Transformed::no(Expr::AggregateFunction(function)),
            }
        }
        Expr::WindowFunction(mut window) => {
            let legacy = match &window.fun {
                WindowFunctionDefinition::AggregateUDF(udaf) => {
                    ansi_mode_udaf(udaf, ansi, sum_decimal::Place::Window)
                }
                WindowFunctionDefinition::WindowUDF(_) => None,
            };
            match legacy {
                Some(func) => {
                    window.fun = WindowFunctionDefinition::AggregateUDF(func);
                    Transformed::yes(Expr::WindowFunction(window))
                }
                None => Transformed::no(Expr::WindowFunction(window)),
            }
        }
        other => Transformed::no(other),
    }
}

fn legacy_avg_udaf(func: &Arc<AggregateUDF>) -> Option<Arc<AggregateUDF>> {
    let avg = func.inner().downcast_ref::<SparkAvgWithRetract>()?;
    (avg.mode == AvgMode::Ansi).then(|| {
        Arc::new(AggregateUDF::new_from_impl(SparkAvgWithRetract {
            mode: AvgMode::Legacy,
            ..avg.clone()
        }))
    })
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
        let values = cast(&values[0], &DataType::Float64)?;
        let values = values.as_primitive::<Float64Type>();
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
            #[allow(clippy::cast_precision_loss)]
            let average = self.sum.map(|total| total / self.count as f64);
            Ok(ScalarValue::Float64(average))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    /// Retract Float64 avg by subtracting sum and count, matching DataFusion `AvgAccumulator`.
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
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
        let names: Vec<_> = datafusion_spark::all_default_aggregate_functions()
            .into_iter()
            .map(|function| function.name().to_string())
            .collect();
        assert!(names.iter().any(|name| name == "avg"));
    }

    #[tokio::test]
    async fn group_avg_decimal128_stays_decimal_14_6_i128() {
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
        assert_eq!(column.value(0), 1_000_000);
        assert_eq!(column.value(1), 2_000_000);
        assert_eq!(column.value(2), 4_000_000);
    }

    #[test]
    fn decimal_retract_batch_returns_to_empty() {
        let mut acc = DecimalAvgAccumulator::<Decimal128Type>::try_new((10, 2), (14, 6), false)
            .expect("averager");
        let batch = Arc::new(
            arrow::array::Decimal128Array::from(vec![Some(110), Some(220), None])
                .with_precision_and_scale(10, 2)
                .expect("fixture scale"),
        ) as ArrayRef;
        acc.update_batch(&[Arc::clone(&batch)]).expect("update");
        assert_eq!(
            acc.evaluate().expect("two rows"),
            ScalarValue::Decimal128(Some(1_650_000), 14, 6)
        );
        acc.retract_batch(&[batch]).expect("retract");
        let evaluated = acc.evaluate().expect("empty frame is null");
        assert!(evaluated.is_null(), "empty retracted frame must be NULL");
    }

    fn register_decimal_column(ctx: &SessionContext, column: ArrayRef, data_type: DataType) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", data_type, true)]));
        let batch = RecordBatch::try_new(schema, vec![column]).expect("decimal avg fixture");
        ctx.register_batch("t", batch)
            .expect("register decimal avg fixture");
    }

    /// Z-3 S3 residual: Decimal32 accumulator arm.
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

    #[tokio::test]
    async fn try_avg_decimal_overflow_is_null() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let sql = "SELECT try_avg(v) AS a FROM (\
             SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38, 0)) AS v \
             UNION ALL \
             SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38, 0))\
           ) t";
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan try_avg decimal overflow")
            .collect()
            .await
            .expect("execute try_avg decimal overflow");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .expect("decimal128 try_avg");
        assert!(column.is_null(0), "try_avg overflow is NULL");
        assert_eq!(column.precision(), 38);
        assert_eq!(column.scale(), 4);
    }

    #[tokio::test]
    async fn avg_decimal_overflow_still_raises() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let sql = "SELECT avg(v) AS a FROM (\
             SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38, 0)) AS v \
             UNION ALL \
             SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38, 0))\
           ) t";
        let error = match ctx.sql(sql).await {
            Ok(frame) => match frame.collect().await {
                Ok(_) => "executed".to_string(),
                Err(error) => error.to_string(),
            },
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("Arithmetic Overflow") || error.contains("ARITHMETIC_OVERFLOW"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn try_avg_interval_answers_spark_average() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let batches = ctx
            .sql("SELECT try_avg(v) AS a FROM (VALUES (INTERVAL 1 DAY), (INTERVAL 2 DAY)) AS x(v)")
            .await
            .expect("plan try_avg interval")
            .collect()
            .await
            .expect("execute try_avg interval");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::IntervalMonthDayNanoArray>()
            .expect("interval try_avg");
        assert!(!column.is_null(0));
        let value = column.value(0);
        assert_eq!(
            (value.months, value.days, value.nanoseconds),
            (0, 1, 43_200_000_000_000)
        );
    }

    #[tokio::test]
    async fn try_avg_interval_overflow_is_null_and_avg_raises() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let sql = "SELECT try_avg(v) AS a FROM (VALUES (INTERVAL '106751991' DAY), \
             (INTERVAL '106751991' DAY)) AS x(v)";
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan try_avg interval overflow")
            .collect()
            .await
            .expect("execute try_avg interval overflow");
        assert!(batches[0].column(0).is_null(0));
        let error = match ctx.sql(sql.replace("try_avg", "avg").as_str()).await {
            Err(error) => error.to_string(),
            Ok(frame) => match frame.collect().await {
                Ok(_) => "executed".to_string(),
                Err(error) => error.to_string(),
            },
        };
        assert!(error.contains("[INTERVAL_ARITHMETIC_OVERFLOW"), "{error}");
    }

    fn ansi_ctx(ansi: bool) -> SessionContext {
        let config =
            crate::ansi::with_spark_ansi_config(datafusion::prelude::SessionConfig::new(), ansi);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        ctx.add_analyzer_rule(Arc::new(SparkAvgAnsiMode));
        ctx
    }

    const NINES38: &str = "99999999999999999999999999999999999999";

    async fn first_text(ctx: &SessionContext, sql: &str) -> std::result::Result<String, String> {
        let frame = ctx.sql(sql).await.map_err(|error| error.to_string())?;
        let batches = frame.collect().await.map_err(|error| error.to_string())?;
        let column = batches[0].column(0);
        Ok(arrow::util::display::array_value_to_string(column, 0).expect("display"))
    }

    #[tokio::test]
    async fn avg_and_mean_overflow_is_null_with_ansi_off_and_raises_with_ansi_on() {
        let source = format!(
            "(SELECT CAST(x AS DECIMAL(38,0)) AS v, x AS w FROM (VALUES ('{NINES38}'), ('{NINES38}')) t(x))"
        );
        for function in ["avg", "mean"] {
            for shape in [
                format!("SELECT {function}(v) FROM {source} s"),
                format!("SELECT {function}(v) OVER () FROM {source} s"),
                format!("SELECT {function}(v), w FROM {source} s GROUP BY w"),
            ] {
                let off = first_text(&ansi_ctx(false), &shape).await;
                assert_eq!(off.as_deref(), Ok(""), "{shape}");
                let on = first_text(&ansi_ctx(true), &shape)
                    .await
                    .expect_err("ANSI raises");
                assert!(
                    on.contains("[ARITHMETIC_OVERFLOW] Overflow in sum of decimals"),
                    "{on}"
                );
            }
        }
        let try_avg = format!("SELECT try_avg(v) FROM {source} s");
        for ansi in [true, false] {
            assert_eq!(
                first_text(&ansi_ctx(ansi), &try_avg).await.as_deref(),
                Ok("")
            );
        }
    }

    #[tokio::test]
    async fn mean_is_spark_avg_and_keeps_its_name() {
        let ctx = ansi_ctx(true);
        let source = "(SELECT CAST(x AS DECIMAL(10,2)) AS v FROM (VALUES ('2.51'), ('0.00'), ('0.00')) t(x))";
        let frame = ctx
            .sql(&format!("SELECT avg(v), mean(v) FROM {source} s"))
            .await
            .expect("plan avg beside mean");
        let names: Vec<String> = frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(names, ["avg(s.v)", "mean(s.v)"]);
        let batches = frame.collect().await.expect("avg beside mean");
        for index in 0..2 {
            assert_eq!(
                arrow::util::display::array_value_to_string(batches[0].column(index), 0)
                    .expect("display"),
                "0.836667"
            );
        }
        let floats = "SELECT mean(DISTINCT x), count(x) FROM (VALUES (1.5), (1.5), (2.5)) t(x)";
        let floats = ctx
            .sql(floats)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("float DISTINCT");
        assert_eq!(
            arrow::util::display::array_value_to_string(floats[0].column(0), 0).expect("display"),
            "2.0"
        );
        let mixed = format!("SELECT mean(DISTINCT v), count(v) FROM {source} s");
        let error = first_text(&ctx, &mixed)
            .await
            .expect_err("mixed DISTINCT refuses");
        assert!(error.contains("DistinctAvgAccumulator"), "{error}");
        let registered = datafusion::execution::FunctionRegistry::udaf(&ctx.state(), "mean")
            .expect("mean registered");
        assert!(
            registered
                .inner()
                .downcast_ref::<SparkAvgWithRetract>()
                .is_some()
        );
    }
}
