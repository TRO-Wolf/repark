use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

#[must_use]
pub fn product_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkProduct::new()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SparkProduct {
    signature: Signature,
}

impl SparkProduct {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for SparkProduct {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "__repark_product"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::<ProductAccumulator>::default())
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(self.name(), "product"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "has_value"),
                DataType::Boolean,
                false,
            )),
        ])
    }
}

#[derive(Debug, Default)]
struct ProductAccumulator {
    product: f64,
    has_value: bool,
}

impl ProductAccumulator {
    fn observe(&mut self, value: f64) {
        if self.has_value {
            self.product *= value;
        } else {
            self.product = value;
            self.has_value = true;
        }
    }
}

impl Accumulator for ProductAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let casted = cast(&values[0], &DataType::Float64)?;
        let floats = casted
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("product input mistyped".to_string()))?;
        for index in 0..floats.len() {
            if !floats.is_null(index) {
                self.observe(floats.value(index));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.has_value {
            Ok(ScalarValue::Float64(Some(self.product)))
        } else {
            Ok(ScalarValue::Float64(None))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(self.has_value.then_some(self.product)),
            ScalarValue::Boolean(Some(self.has_value)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let products = states[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("product state mistyped".to_string()))?;
        let flags = states[1]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| DataFusionError::Execution("product state flag mistyped".to_string()))?;
        for row in 0..products.len() {
            if flags.is_null(row) || !flags.value(row) || products.is_null(row) {
                continue;
            }
            self.observe(products.value(row));
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
        ctx.register_udaf(product_udaf().as_ref().clone());
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

    fn double(batch: &RecordBatch, row: usize) -> Option<f64> {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        values.is_valid(row).then(|| values.value(row))
    }

    #[tokio::test]
    async fn product_skips_nulls_and_answers_double() {
        let batch = batch(
            &ctx(),
            "SELECT __repark_product(v) FROM (VALUES (10), (20), (NULL), (30)) AS t(v)",
        )
        .await;
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
        assert_eq!(double(&batch, 0), Some(6000.0));
    }

    #[tokio::test]
    async fn all_null_product_is_null() {
        let batch = batch(
            &ctx(),
            "SELECT __repark_product(v) FROM (VALUES (CAST(NULL AS INT))) AS t(v)",
        )
        .await;
        assert_eq!(double(&batch, 0), None);
    }

    fn split_merged(values: &[Option<f64>]) -> Option<f64> {
        let arrays: Vec<ArrayRef> = values
            .chunks(2)
            .map(|chunk| Arc::new(Float64Array::from(chunk.to_vec())) as ArrayRef)
            .collect();
        let mut partials = Vec::new();
        for array in &arrays {
            let mut accumulator = ProductAccumulator::default();
            accumulator
                .update_batch(std::slice::from_ref(array))
                .expect("update");
            partials.push(accumulator.state().expect("state"));
        }
        let states: Vec<ArrayRef> = (0..2)
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
        let mut merged = ProductAccumulator::default();
        merged.merge_batch(&states).expect("merge");
        match merged.evaluate().expect("evaluate") {
            ScalarValue::Float64(value) => value,
            other => panic!("product result mistyped: {other}"),
        }
    }

    #[tokio::test]
    async fn merge_batch_matches_single_partition() {
        assert_eq!(
            split_merged(&[Some(10.0), Some(20.0), None, Some(30.0)]),
            Some(6000.0)
        );
        assert_eq!(split_merged(&[None, None]), None);
    }
}
