use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::prelude::{Column, DataFrame, Expr};

use crate::{Result, engine_err};

pub const FREQ_ITEMS_OUTPUT_PREFIX: &str = "__repark_freq_items_";

#[expect(
    clippy::missing_errors_doc,
    reason = "errors are Spark AnalysisException text; the reason lives in this module's map.md row"
)]
pub fn freq_items(frame: DataFrame, column_names: &[String], capacity: usize) -> Result<DataFrame> {
    let udaf = AggregateUDF::new_from_impl(FreqItems::new(capacity));
    let aggregates = column_names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            udaf.call(vec![Expr::Column(Column::new_unqualified(name.clone()))])
                .alias(format!("{FREQ_ITEMS_OUTPUT_PREFIX}{index}"))
        })
        .collect();
    frame.aggregate(vec![], aggregates).map_err(engine_err)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FreqItems {
    signature: Signature,
    capacity: usize,
}

impl FreqItems {
    fn new(capacity: usize) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            capacity,
        }
    }
}

impl AggregateUDFImpl for FreqItems {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "collect_frequent_items"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Err(DataFusionError::Plan(
            "collect_frequent_items resolves its return type through return_field".to_string(),
        ))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> DataFusionResult<FieldRef> {
        let input = arg_fields[0].data_type().clone();
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(Arc::new(Field::new_list_field(input, true))),
            false,
        )))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs<'_>) -> DataFusionResult<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return Err(DataFusionError::Plan(
                "collect_frequent_items(DISTINCT ...) is not supported".to_string(),
            ));
        }
        Ok(Box::new(FreqItemsAccumulator {
            datatype: acc_args.expr_fields[0].data_type().clone(),
            capacity: self.capacity,
            counts: HashMap::new(),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DataFusionResult<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new_list(
                format_state_name(self.name(), "keys"),
                Arc::new(Field::new_list_field(
                    args.input_fields[0].data_type().clone(),
                    true,
                )),
                true,
            )),
            Arc::new(Field::new_list(
                format_state_name(self.name(), "counts"),
                Arc::new(Field::new_list_field(DataType::Int64, true)),
                true,
            )),
        ])
    }

    fn is_nullable(&self) -> bool {
        false
    }
}

struct FreqItemsAccumulator {
    datatype: DataType,
    capacity: usize,
    counts: HashMap<ScalarValue, i64>,
}

impl fmt::Debug for FreqItemsAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FreqItemsAccumulator")
            .field("datatype", &self.datatype)
            .field("capacity", &self.capacity)
            .field("counts_len", &self.counts.len())
            .finish()
    }
}

impl FreqItemsAccumulator {
    fn add(&mut self, key: ScalarValue, count: i64) {
        if let Some(existing) = self.counts.get_mut(&key) {
            *existing += count;
            return;
        }
        if self.counts.len() < self.capacity {
            self.counts.insert(key, count);
            return;
        }
        let Some(minimum) = self.counts.values().copied().min() else {
            return;
        };
        if count >= minimum {
            self.counts.insert(key, count);
            self.counts.retain(|_, value| *value > minimum);
            for value in self.counts.values_mut() {
                *value -= minimum;
            }
        } else {
            for value in self.counts.values_mut() {
                *value -= count;
            }
        }
    }
}

impl Accumulator for FreqItemsAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        let values = &values[0];
        for index in 0..values.len() {
            let key = ScalarValue::try_from_array(values.as_ref(), index)?.compacted();
            self.add(key, 1);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        let keys = states[0].as_list::<i32>();
        let counts = states[1].as_list::<i32>();
        for row in 0..keys.len() {
            if keys.is_null(row) {
                continue;
            }
            let key_values = keys.value(row);
            let count_values = counts.value(row);
            for index in 0..key_values.len() {
                let key = ScalarValue::try_from_array(key_values.as_ref(), index)?.compacted();
                let ScalarValue::Int64(Some(count)) =
                    ScalarValue::try_from_array(count_values.as_ref(), index)?
                else {
                    continue;
                };
                self.add(key, count);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        let keys: Vec<ScalarValue> = self.counts.keys().cloned().collect();
        let counts: Vec<ScalarValue> = self
            .counts
            .values()
            .map(|count| ScalarValue::Int64(Some(*count)))
            .collect();
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list(&keys, &self.datatype, true)),
            ScalarValue::List(ScalarValue::new_list(&counts, &DataType::Int64, true)),
        ])
    }

    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        let keys: Vec<ScalarValue> = self.counts.keys().cloned().collect();
        Ok(ScalarValue::List(ScalarValue::new_list(
            &keys,
            &self.datatype,
            true,
        )))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.counts.capacity() * size_of::<(ScalarValue, i64)>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accumulator(capacity: usize) -> FreqItemsAccumulator {
        FreqItemsAccumulator {
            datatype: DataType::Int32,
            capacity,
            counts: HashMap::new(),
        }
    }

    fn int(value: i32) -> ScalarValue {
        ScalarValue::Int32(Some(value))
    }

    #[test]
    fn add_inserts_until_capacity() {
        let mut acc = accumulator(3);
        acc.add(int(1), 1);
        acc.add(int(2), 1);
        assert_eq!(acc.counts.len(), 2);
    }

    #[test]
    fn add_full_zero_remainder_inserts_then_evicts() {
        let mut acc = accumulator(2);
        acc.add(int(1), 3);
        acc.add(int(2), 1);
        acc.add(int(3), 1);
        assert_eq!(acc.counts, HashMap::from([(int(1), 2)]));
    }

    #[test]
    fn add_full_negative_remainder_shrinks_all() {
        let mut acc = accumulator(2);
        acc.add(int(1), 4);
        acc.add(int(2), 3);
        acc.add(int(3), 1);
        assert_eq!(acc.counts, HashMap::from([(int(1), 3), (int(2), 2)]));
    }

    #[test]
    fn existing_key_increments() {
        let mut acc = accumulator(1);
        acc.add(int(7), 1);
        acc.add(int(7), 4);
        assert_eq!(acc.counts, HashMap::from([(int(7), 5)]));
    }

    #[test]
    fn oracle_support_05_trace() {
        let mut acc = accumulator(2);
        for value in [1i32, 1, 2, 1, 3] {
            acc.add(int(value), 1);
        }
        assert_eq!(acc.counts, HashMap::from([(int(1), 2)]));
    }

    #[test]
    fn oracle_many_distinct_empties() {
        let mut acc = accumulator(3);
        for value in 0..1000i32 {
            acc.add(int(value), 1);
        }
        assert!(acc.counts.is_empty());
    }
}
