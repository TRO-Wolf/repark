use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, ListArray, new_empty_array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, TypeSignature, Volatility,
};

use crate::max_min_by::{OrdKey, unqualified_name};

#[must_use]
pub fn mode_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkMode::new()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SparkMode {
    signature: Signature,
}

impl SparkMode {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }

    fn spark_display(&self, params: &AggregateFunctionParams) -> String {
        let name = self.name();
        let arg = params
            .args
            .first()
            .map(unqualified_name)
            .unwrap_or_default();
        if !params.order_by.is_empty() {
            format!("{name}() WITHIN GROUP (ORDER BY {arg})")
        } else if params.args.len() > 1 && is_true_literal(&params.args[1]) {
            format!("{name}() WITHIN GROUP (ORDER BY {arg} DESC)")
        } else {
            format!("{name}({arg})")
        }
    }
}

fn is_true_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Boolean(Some(true)), _))
}

impl AggregateUDFImpl for SparkMode {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "mode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            DataFusionError::Plan("mode requires 1 or 2 parameters but got 0".to_string())
        })
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let fields = acc_args.expr_fields;
        if fields.is_empty() || fields.len() > 2 {
            return Err(DataFusionError::Plan(format!(
                "mode requires 1 or 2 parameters but got {}",
                fields.len()
            )));
        }
        let order = acc_args
            .order_bys
            .first()
            .map(|sort| sort.options.descending);
        Ok(Box::new(ModeAccumulator::new(
            fields[0].data_type().clone(),
            order,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.input_fields.is_empty() || args.input_fields.len() > 2 {
            return Err(DataFusionError::Plan(format!(
                "mode requires 1 or 2 parameters but got {}",
                args.input_fields.len()
            )));
        }
        let keys = Arc::new(Field::new(
            "item",
            args.input_fields[0].data_type().clone(),
            true,
        ));
        let tallies = Arc::new(Field::new("item", DataType::Int64, true));
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(self.name(), "values"),
                DataType::List(keys),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "counts"),
                DataType::List(tallies),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "deterministic"),
                DataType::Boolean,
                false,
            )),
        ])
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(self.spark_display(params))
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(self.spark_display(params))
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct ModeAccumulator {
    value_type: DataType,
    descending: Option<bool>,
    deterministic: bool,
    counts: Vec<(ScalarValue, u64)>,
    leader: Option<ScalarValue>,
    max_count: u64,
}

impl ModeAccumulator {
    fn new(value_type: DataType, descending: Option<bool>) -> Self {
        Self {
            value_type,
            descending,
            deterministic: false,
            counts: Vec::new(),
            leader: None,
            max_count: 0,
        }
    }

    fn add(&mut self, value: ScalarValue, increment: u64) {
        let total = if let Some(slot) = self.counts.iter_mut().find(|(key, _)| *key == value) {
            slot.1 += increment;
            slot.1
        } else {
            self.counts.push((value.clone(), increment));
            increment
        };
        if total > self.max_count {
            self.max_count = total;
        }
        if self.descending.is_none() && !self.deterministic && total >= self.max_count {
            self.leader = Some(value);
        }
    }

    fn flag(values: &[ArrayRef]) -> Result<bool> {
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
                DataFusionError::Plan(format!(
                    "mode deterministic flag must be a boolean literal, got {}",
                    values[1].data_type()
                ))
            })?;
        if flags.is_empty() {
            return Ok(false);
        }
        Ok(flags.value(0))
    }

    fn extremum(&self, descending: bool) -> Result<ScalarValue> {
        let mut best: Option<&ScalarValue> = None;
        let mut best_key: Option<OrdKey> = None;
        for (value, count) in &self.counts {
            if *count != self.max_count {
                continue;
            }
            let key = OrdKey::from_scalar(value)?
                .ok_or_else(|| DataFusionError::Internal("mode candidate is null".to_string()))?;
            let take = match &best_key {
                None => true,
                Some(current) => {
                    let order = key.cmp(current)?;
                    (descending && order == std::cmp::Ordering::Greater)
                        || (!descending && order == std::cmp::Ordering::Less)
                }
            };
            if take {
                best = Some(value);
                best_key = Some(key);
            }
        }
        best.cloned().ok_or_else(|| {
            DataFusionError::Internal("mode has a max count but no candidate".to_string())
        })
    }
}

impl Accumulator for ModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() > 1 && self.descending.is_none() {
            self.deterministic = Self::flag(values)?;
        }
        for row in 0..values[0].len() {
            if values[0].is_null(row) {
                continue;
            }
            let value = ScalarValue::try_from_array(&values[0], row)?;
            self.add(value, 1);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.counts.is_empty() {
            return ScalarValue::try_from(&self.value_type);
        }
        if let Some(descending) = self.descending {
            return self.extremum(descending);
        }
        if self.deterministic {
            return self.extremum(false);
        }
        self.leader
            .clone()
            .ok_or_else(|| DataFusionError::Internal("mode has counts but no leader".to_string()))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values = if self.counts.is_empty() {
            new_empty_array(&self.value_type)
        } else {
            ScalarValue::iter_to_array(self.counts.iter().map(|(value, _)| value.clone()))?
        };
        let counts: ArrayRef = Arc::new(Int64Array::from(
            self.counts
                .iter()
                .map(|(_, count)| {
                    i64::try_from(*count).map_err(|_| {
                        DataFusionError::Execution("mode state count out of range".to_string())
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        ));
        Ok(vec![
            ScalarValue::List(single_row_list(&values, &self.value_type)),
            ScalarValue::List(single_row_list(&counts, &DataType::Int64)),
            ScalarValue::Boolean(Some(self.deterministic)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let values = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("mode state values mistyped".to_string()))?;
        let tallies = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("mode state counts mistyped".to_string()))?;
        let flags = states[2]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| DataFusionError::Execution("mode state flag mistyped".to_string()))?;
        for row in 0..values.len() {
            if !flags.is_null(row) && flags.value(row) {
                self.deterministic = true;
            }
            if values.is_null(row) || tallies.is_null(row) {
                continue;
            }
            let keys = values.value(row);
            let counts = tallies.value(row);
            let counts = counts
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("mode state counts mistyped".to_string())
                })?;
            for index in 0..keys.len() {
                if keys.is_null(index) || counts.is_null(index) {
                    continue;
                }
                let increment = u64::try_from(counts.value(index).max(0)).map_err(|_| {
                    DataFusionError::Execution("mode state count out of range".to_string())
                })?;
                let value = ScalarValue::try_from_array(&keys, index)?;
                self.add(value, increment);
            }
        }
        Ok(())
    }
}

fn single_row_list(values: &ArrayRef, data_type: &DataType) -> Arc<ListArray> {
    let field = Arc::new(Field::new("item", data_type.clone(), true));
    let list = ListArray::new(
        field,
        arrow::buffer::OffsetBuffer::from_lengths([values.len()]),
        Arc::clone(values),
        None,
    );
    Arc::new(list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udaf(mode_udaf().as_ref().clone());
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

    fn names(batch: &RecordBatch) -> Vec<String> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    fn text(batch: &RecordBatch, column: usize, row: usize) -> Option<String> {
        match ScalarValue::try_from_array(batch.column(column), row).expect("value") {
            ScalarValue::Utf8(value) => value,
            ScalarValue::Int32(value) => value.map(|v| v.to_string()),
            ScalarValue::Int64(value) => value.map(|v| v.to_string()),
            other => panic!("mode result mistyped: {other}"),
        }
    }

    #[tokio::test]
    async fn within_group_form_aggregates_sort_key() {
        let batch = batch(
            &ctx(),
            "SELECT mode() WITHIN GROUP (ORDER BY v DESC) FROM (VALUES (10), (20)) AS t(v)",
        )
        .await;
        assert_eq!(
            batch.schema().field(0).name(),
            "mode() WITHIN GROUP (ORDER BY v)"
        );
        let value = ScalarValue::try_from_array(batch.column(0), 0).expect("value");
        assert_eq!(value, ScalarValue::Int64(Some(20)));
    }

    #[tokio::test]
    async fn sql_names_follow_spark_display() {
        let batch = batch(
            &ctx(),
            "SELECT mode(k), mode(s, true), mode() WITHIN GROUP (ORDER BY v DESC) FROM \
             (VALUES ('b', 'x', 10), ('b', 'y', 20), ('a', 'x', 30)) AS t(k, s, v)",
        )
        .await;
        assert_eq!(
            names(&batch),
            vec![
                "mode(k)".to_string(),
                "mode() WITHIN GROUP (ORDER BY s DESC)".to_string(),
                "mode() WITHIN GROUP (ORDER BY v)".to_string(),
            ]
        );
        assert_eq!(text(&batch, 0, 0), Some("b".to_string()));
        assert_eq!(text(&batch, 1, 0), Some("x".to_string()));
        assert_eq!(text(&batch, 2, 0), Some("30".to_string()));
    }

    #[tokio::test]
    async fn ties_keep_last_seen_without_deterministic() {
        let batch = batch(
            &ctx(),
            "SELECT mode(v) FROM (VALUES (10), (20), (30)) AS t(v)",
        )
        .await;
        let value = ScalarValue::try_from_array(batch.column(0), 0).expect("value");
        assert_eq!(value, ScalarValue::Int64(Some(30)));
    }

    #[tokio::test]
    async fn deterministic_ties_pick_smallest() {
        let batch = batch(
            &ctx(),
            "SELECT mode(v, true) FROM (VALUES (20), (10), (30)) AS t(v)",
        )
        .await;
        let value = ScalarValue::try_from_array(batch.column(0), 0).expect("value");
        assert_eq!(value, ScalarValue::Int64(Some(10)));
    }

    #[tokio::test]
    async fn nulls_are_ignored_and_empty_is_null() {
        let batch = batch(
            &ctx(),
            "SELECT mode(v) FROM (VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT))) AS t(v)",
        )
        .await;
        assert!(batch.column(0).is_null(0));
    }

    fn input(values: Vec<Option<i32>>) -> Vec<ArrayRef> {
        vec![Arc::new(Int32Array::from(values)) as ArrayRef]
    }

    fn states_of(values: Vec<Option<i32>>, flag: bool) -> Vec<ScalarValue> {
        let arrays = input(values);
        let mut accumulator = ModeAccumulator::new(DataType::Int32, None);
        if flag {
            let flags: ArrayRef = Arc::new(BooleanArray::from(vec![true; arrays[0].len()]));
            accumulator
                .update_batch(&[arrays[0].clone(), flags])
                .expect("update");
        } else {
            accumulator.update_batch(&arrays).expect("update");
        }
        accumulator.state().expect("state")
    }

    fn merge_all(parts: &[Vec<ScalarValue>]) -> ScalarValue {
        let states: Vec<ArrayRef> = (0..3)
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
        let mut merged = ModeAccumulator::new(DataType::Int32, None);
        merged.merge_batch(&states).expect("merge");
        merged.evaluate().expect("evaluate")
    }

    #[tokio::test]
    async fn merge_matches_single_partition_for_unique_max() {
        let left = states_of(vec![Some(1), Some(2)], false);
        let right = states_of(vec![Some(2), Some(3)], false);
        assert_eq!(merge_all(&[left, right]), ScalarValue::Int32(Some(2)));
    }

    #[tokio::test]
    async fn merge_carries_deterministic_across_partitions() {
        let left = states_of(vec![Some(20), Some(10)], true);
        let right = states_of(vec![Some(30)], true);
        assert_eq!(merge_all(&[left, right]), ScalarValue::Int32(Some(10)));
    }
}
