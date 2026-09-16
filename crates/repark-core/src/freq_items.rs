use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
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

#[derive(Debug)]
struct FreqKey(ScalarValue);

impl PartialEq for FreqKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (ScalarValue::Float16(Some(left)), ScalarValue::Float16(Some(right))) => {
                left.to_f64() == right.to_f64()
            }
            (ScalarValue::Float32(Some(left)), ScalarValue::Float32(Some(right))) => left == right,
            (ScalarValue::Float64(Some(left)), ScalarValue::Float64(Some(right))) => left == right,
            (ScalarValue::Map(_), _) | (_, ScalarValue::Map(_))
                if !self.0.is_null() || !other.0.is_null() =>
            {
                false
            }
            _ => self.0 == other.0,
        }
    }
}

impl Eq for FreqKey {}

impl Hash for FreqKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            ScalarValue::Float16(Some(value)) => {
                canonical_float_bits(value.to_f64()).hash(state);
            }
            ScalarValue::Float32(Some(value)) => {
                canonical_float_bits(f64::from(*value)).hash(state);
            }
            ScalarValue::Float64(Some(value)) => {
                canonical_float_bits(*value).hash(state);
            }
            _ => self.0.hash(state),
        }
    }
}

fn canonical_float_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0f64.to_bits()
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

struct FreqItemsAccumulator {
    datatype: DataType,
    capacity: usize,
    counts: HashMap<FreqKey, i64>,
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
    fn add(&mut self, key: FreqKey, count: i64) {
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
            self.add(FreqKey(key), 1);
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
                self.add(FreqKey(key), count);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        let keys: Vec<ScalarValue> = self.counts.keys().map(|key| key.0.clone()).collect();
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
        let keys: Vec<ScalarValue> = self.counts.keys().map(|key| key.0.clone()).collect();
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

    fn int(value: i32) -> FreqKey {
        FreqKey(ScalarValue::Int32(Some(value)))
    }

    fn double(value: f64) -> FreqKey {
        FreqKey(ScalarValue::Float64(Some(value)))
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

    #[test]
    fn signed_zero_keys_collapse_to_first_inserted() {
        let mut acc = accumulator(4);
        acc.add(double(-0.0), 1);
        acc.add(double(0.0), 1);
        assert_eq!(acc.counts.len(), 1);
        assert_eq!(acc.counts.get(&double(0.0)), Some(&2));
        assert_eq!(acc.counts.get(&double(-0.0)), Some(&2));
        assert!(
            acc.counts
                .keys()
                .any(|key| matches!(key.0, ScalarValue::Float64(Some(v)) if v.is_sign_negative()))
        );
    }

    #[test]
    fn signed_zero_keys_collapse_at_capacity() {
        let mut acc = accumulator(1);
        acc.add(double(0.0), 1);
        acc.add(double(-0.0), 1);
        assert_eq!(acc.counts, HashMap::from([(double(0.0), 2)]));
    }

    #[test]
    fn float32_signed_zero_keys_collapse() {
        let mut acc = accumulator(1);
        acc.add(FreqKey(ScalarValue::Float32(Some(0.0))), 1);
        acc.add(FreqKey(ScalarValue::Float32(Some(-0.0))), 1);
        assert_eq!(acc.counts.len(), 1);
    }

    #[test]
    fn nan_keys_never_equal() {
        let mut acc = accumulator(4);
        acc.add(double(f64::NAN), 1);
        acc.add(double(f64::NAN), 1);
        assert_eq!(acc.counts.len(), 2);
    }

    #[test]
    fn nan_keys_evict_each_other_at_capacity() {
        let mut acc = accumulator(1);
        acc.add(double(f64::NAN), 1);
        acc.add(double(f64::NAN), 1);
        assert!(acc.counts.is_empty());
    }

    #[test]
    fn null_float_keys_dedupe() {
        let mut acc = accumulator(4);
        acc.add(FreqKey(ScalarValue::Float64(None)), 1);
        acc.add(FreqKey(ScalarValue::Float64(None)), 1);
        assert_eq!(acc.counts.len(), 1);
    }

    fn map_key() -> FreqKey {
        let map = arrow::array::MapArray::new_from_strings(
            ["a"].into_iter(),
            &arrow::array::Int32Array::from(vec![Some(1)]),
            &[0, 1],
        )
        .expect("map");
        FreqKey(ScalarValue::Map(Arc::new(map)))
    }

    #[test]
    fn map_keys_never_equal_at_default_capacity() {
        let mut acc = accumulator(4);
        acc.add(map_key(), 1);
        acc.add(map_key(), 1);
        assert_eq!(acc.counts.len(), 2);
    }

    #[test]
    fn map_keys_evict_each_other_at_capacity_one() {
        let mut acc = accumulator(1);
        acc.add(map_key(), 1);
        acc.add(map_key(), 1);
        assert!(acc.counts.is_empty());
    }

    fn null_map_key() -> FreqKey {
        let map = arrow::array::MapArray::new_from_strings(
            ["a"].into_iter(),
            &arrow::array::Int32Array::from(vec![Some(1)]),
            &[0, 1],
        )
        .expect("map");
        let (field, offsets, entries, _, ordered) = map.into_parts();
        let null_map = arrow::array::MapArray::try_new(
            field,
            offsets,
            entries,
            Some(arrow::buffer::NullBuffer::from(vec![false])),
            ordered,
        )
        .expect("null map");
        FreqKey(ScalarValue::Map(Arc::new(null_map)))
    }

    #[test]
    fn null_map_keys_dedupe() {
        let mut acc = accumulator(4);
        acc.add(null_map_key(), 1);
        acc.add(null_map_key(), 1);
        assert_eq!(acc.counts.len(), 1);
        assert_eq!(acc.counts.get(&null_map_key()), Some(&2));
    }

    #[test]
    fn null_map_keys_dedupe_at_capacity_one() {
        let mut acc = accumulator(1);
        acc.add(null_map_key(), 1);
        acc.add(null_map_key(), 1);
        assert_eq!(acc.counts.len(), 1);
    }

    #[test]
    fn null_map_key_differs_from_value_map() {
        let mut acc = accumulator(4);
        acc.add(null_map_key(), 1);
        acc.add(map_key(), 1);
        assert_eq!(acc.counts.len(), 2);
    }
}
