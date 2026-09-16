use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::max_min_by::unqualified_name;

#[must_use]
pub fn any_value_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkAnyValue::new()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SparkAnyValue {
    signature: Signature,
}

impl SparkAnyValue {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

fn ignore_nulls(values: &[ArrayRef]) -> Result<bool> {
    if values.len() < 2 {
        return Ok(false);
    }
    if values[1].data_type() == &DataType::Null {
        return Ok(false);
    }
    let flags = values[1]
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            DataFusionError::Plan("any_value ignoreNulls flag must be boolean".to_string())
        })?;
    if flags.is_empty() {
        return Ok(false);
    }
    Ok(flags.value(0))
}

impl AggregateUDFImpl for SparkAnyValue {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "any_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            DataFusionError::Plan("any_value requires 1 or 2 parameters but got 0".to_string())
        })
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let fields = acc_args.expr_fields;
        if fields.is_empty() || fields.len() > 2 {
            return Err(DataFusionError::Plan(format!(
                "any_value requires 1 or 2 parameters but got {}",
                fields.len()
            )));
        }
        Ok(Box::new(AnyValueAccumulator::new(
            fields[0].data_type().clone(),
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.input_fields.is_empty() || args.input_fields.len() > 2 {
            return Err(DataFusionError::Plan(format!(
                "any_value requires 1 or 2 parameters but got {}",
                args.input_fields.len()
            )));
        }
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(self.name(), "value"),
                args.input_fields[0].data_type().clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "occupied"),
                DataType::Boolean,
                false,
            )),
        ])
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        let arg = params
            .args
            .first()
            .map(unqualified_name)
            .unwrap_or_default();
        Ok(format!("any_value({arg})"))
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        let arg = params
            .args
            .first()
            .map(unqualified_name)
            .unwrap_or_default();
        Ok(format!("any_value({arg})"))
    }
}

#[derive(Debug)]
struct AnyValueAccumulator {
    value_type: DataType,
    best: Option<ScalarValue>,
    occupied: bool,
}

impl AnyValueAccumulator {
    fn new(value_type: DataType) -> Self {
        Self {
            value_type,
            best: None,
            occupied: false,
        }
    }
}

impl Accumulator for AnyValueAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let ignore = ignore_nulls(values)?;
        for row in 0..values[0].len() {
            if self.occupied {
                break;
            }
            if values[0].is_null(row) && ignore {
                continue;
            }
            self.best = Some(ScalarValue::try_from_array(&values[0], row)?);
            self.occupied = true;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match &self.best {
            Some(value) => Ok(value.clone()),
            None => Ok(ScalarValue::try_from(&self.value_type)?),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            self.best
                .clone()
                .unwrap_or(ScalarValue::try_from(&self.value_type)?),
            ScalarValue::Boolean(Some(self.occupied)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let flags = states[1]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("any_value state flag mistyped".to_string())
            })?;
        for row in 0..states[0].len() {
            if self.occupied {
                break;
            }
            if flags.is_null(row) || !flags.value(row) {
                continue;
            }
            self.best = Some(ScalarValue::try_from_array(&states[0], row)?);
            self.occupied = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udaf(any_value_udaf().as_ref().clone());
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

    #[tokio::test]
    async fn both_arg_shapes_answer_first_value() {
        let plain = batch(
            &ctx(),
            "SELECT any_value(v) FROM (VALUES (10), (20), (NULL), (30)) AS t(v)",
        )
        .await;
        assert_eq!(plain.schema().field(0).name(), "any_value(v)");
        let first = ScalarValue::try_from_array(plain.column(0), 0).expect("first");
        assert_eq!(first, ScalarValue::Int64(Some(10)));
        let flagged = batch(
            &ctx(),
            "SELECT any_value(v, true) AS flagged FROM (VALUES (10), (20), (NULL), (30)) AS t(v)",
        )
        .await;
        let value = ScalarValue::try_from_array(flagged.column(0), 0).expect("flagged");
        assert_eq!(value, ScalarValue::Int64(Some(10)));
    }

    #[tokio::test]
    async fn ignore_nulls_skips_leading_nulls() {
        let plain = batch(
            &ctx(),
            "SELECT any_value(v) FROM (VALUES (CAST(NULL AS INT)), (7)) AS t(v)",
        )
        .await;
        assert!(plain.column(0).is_null(0));
        let flagged = batch(
            &ctx(),
            "SELECT any_value(v, true) AS flagged FROM (VALUES (CAST(NULL AS INT)), (7)) AS t(v)",
        )
        .await;
        let value = ScalarValue::try_from_array(flagged.column(0), 0).expect("flagged");
        assert_eq!(value, ScalarValue::Int64(Some(7)));
    }

    fn states_of(values: Vec<Option<i32>>, ignore: bool) -> Vec<ScalarValue> {
        let array: ArrayRef = Arc::new(Int32Array::from(values));
        let inputs = if ignore {
            let flags: ArrayRef = Arc::new(BooleanArray::from(vec![true; array.len()]));
            vec![array, flags]
        } else {
            vec![array]
        };
        let mut accumulator = AnyValueAccumulator::new(DataType::Int32);
        accumulator.update_batch(&inputs).expect("update");
        accumulator.state().expect("state")
    }

    fn merge_all(parts: &[Vec<ScalarValue>]) -> ScalarValue {
        let states: Vec<ArrayRef> = (0..2)
            .map(|column| {
                ScalarValue::iter_to_array(
                    parts
                        .iter()
                        .map(|state| state[column].clone())
                        .collect::<Vec<_>>(),
                )
                .expect("state column")
            })
            .collect();
        let mut merged = AnyValueAccumulator::new(DataType::Int32);
        merged.merge_batch(&states).expect("merge");
        merged.evaluate().expect("evaluate")
    }

    #[tokio::test]
    async fn merge_keeps_first_decided_partial() {
        let left = states_of(vec![None, None], true);
        let right = states_of(vec![Some(4)], true);
        assert_eq!(merge_all(&[left, right]), ScalarValue::Int32(Some(4)));
        let frozen = states_of(vec![None], false);
        let late = states_of(vec![Some(9)], false);
        assert_eq!(merge_all(&[frozen, late]), ScalarValue::Int32(None));
    }
}
