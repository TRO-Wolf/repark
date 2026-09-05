use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::{cast, sum};
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

#[must_use]
pub fn count_if_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(CountIf::new()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CountIf {
    signature: Signature,
}

impl CountIf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CountIf {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "count_if"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return Err(DataFusionError::Plan(
                "count_if(DISTINCT ...) is not supported".to_string(),
            ));
        }
        Ok(Box::<CountIfAccumulator>::default())
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(self.name(), "count"),
            DataType::Int64,
            true,
        ))])
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(0)))
    }
}

#[derive(Debug, Default)]
struct CountIfAccumulator {
    count: i64,
}

impl CountIfAccumulator {
    fn true_count(values: &[ArrayRef]) -> Result<i64> {
        let booleans = cast(&values[0], &DataType::Boolean)?;
        let booleans = booleans.as_boolean();
        i64::try_from(booleans.iter().flatten().filter(|value| *value).count()).map_err(|_| {
            DataFusionError::Internal(
                "count_if update_batch: batch length does not fit i64".to_string(),
            )
        })
    }
}

impl Accumulator for CountIfAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.count += Self::true_count(values)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let merged = cast(&states[0], &DataType::Int64)?;
        self.count += sum(merged.as_primitive::<Int64Type>()).unwrap_or_default();
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.count -= Self::true_count(values)?;
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

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

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udaf(count_if_udaf().as_ref().clone());
        ctx
    }

    fn int64_cell(batch: &RecordBatch) -> Option<i64> {
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        array.is_valid(0).then(|| array.value(0))
    }

    #[tokio::test]
    async fn counts_true_skips_false_and_null() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT count_if(x > 1) AS v FROM (VALUES (1), (2), (NULL)) AS t(x)",
        )
        .await;
        assert_eq!(int64_cell(&batch), Some(1));
        assert!(!batch.schema().field(0).is_nullable());
    }

    #[tokio::test]
    async fn empty_input_answers_zero() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT count_if(x > 100) AS v FROM (VALUES (1), (2)) AS t(x)",
        )
        .await;
        assert_eq!(int64_cell(&batch), Some(0));
    }

    #[tokio::test]
    async fn non_boolean_argument_refuses() {
        let ctx = ctx();
        let error = ctx
            .sql("SELECT count_if(x) AS v FROM (VALUES (1)) AS t(x)")
            .await
            .expect_err("an int argument must not plan");
        assert!(error.to_string().contains("count_if"), "got {error}");
    }

    #[tokio::test]
    async fn distinct_refuses() {
        let ctx = ctx();
        let frame = ctx
            .sql("SELECT count_if(DISTINCT x > 0), count(DISTINCT x) FROM (VALUES (1)) AS t(x)")
            .await
            .expect("logical plan");
        let error = frame.collect().await.expect_err("distinct must refuse");
        assert!(error.to_string().contains("DISTINCT"), "got {error}");
    }
}
