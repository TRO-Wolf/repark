use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

#[must_use]
pub fn percentile_approx_udaf() -> Arc<AggregateUDF> {
    Arc::new(
        AggregateUDF::new_from_impl(SparkPercentileApprox::new())
            .with_aliases(["approx_percentile"]),
    )
}

#[derive(Debug)]
struct SparkPercentileApprox {
    signature: Signature,
}

impl SparkPercentileApprox {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl PartialEq for SparkPercentileApprox {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkPercentileApprox {}

impl Hash for SparkPercentileApprox {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn percentages_are_list(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::LargeList(_)
    )
}

fn return_data_type(value_type: &DataType, percentage_type: &DataType) -> DataType {
    if percentages_are_list(percentage_type) {
        DataType::List(Arc::new(Field::new("item", value_type.clone(), true)))
    } else {
        value_type.clone()
    }
}

fn percentages_from_scalar(value: &ScalarValue) -> Result<Vec<f64>> {
    match value {
        ScalarValue::List(list) => {
            if list.is_empty() || list.is_null(0) {
                return Ok(Vec::new());
            }
            let inner = list.value(0);
            let mut percentages = Vec::with_capacity(inner.len());
            for index in 0..inner.len() {
                if inner.is_null(index) {
                    continue;
                }
                percentages.push(scalar_as_f64(&ScalarValue::try_from_array(&inner, index)?)?);
            }
            Ok(percentages)
        }
        other => Ok(vec![scalar_as_f64(other)?]),
    }
}

fn extract_percentages(array: &ArrayRef) -> Result<Vec<f64>> {
    if array.is_empty() {
        return Ok(Vec::new());
    }
    percentages_from_scalar(&ScalarValue::try_from_array(array, 0)?)
}

#[allow(clippy::cast_precision_loss)]
fn scalar_as_f64(value: &ScalarValue) -> Result<f64> {
    match value {
        ScalarValue::Float64(Some(number)) => Ok(*number),
        ScalarValue::Float32(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Decimal128(Some(number), _, scale) => {
            let scale = i32::from(*scale);
            Ok((*number as f64) / 10f64.powi(scale))
        }
        ScalarValue::Int32(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Int16(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Int8(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::UInt32(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::UInt16(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::UInt8(Some(number)) => Ok(f64::from(*number)),
        ScalarValue::Int64(Some(number)) => {
            let Ok(narrow) = i32::try_from(*number) else {
                return exec_err!("percentile_approx Int64 percentage does not fit i32");
            };
            Ok(f64::from(narrow))
        }
        ScalarValue::UInt64(Some(number)) => {
            let Ok(narrow) = u32::try_from(*number) else {
                return exec_err!("percentile_approx UInt64 percentage does not fit u32");
            };
            Ok(f64::from(narrow))
        }
        other => exec_err!("percentile_approx percentage must be numeric, got {other}"),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn discrete_index(percentage: f64, count: usize) -> Result<usize> {
    if count == 0 {
        return Ok(0);
    }
    let Ok(count_u32) = u32::try_from(count) else {
        return exec_err!("percentile_approx group is too large");
    };
    let count_float = f64::from(count_u32);
    let rank = (percentage * count_float).ceil();
    if !rank.is_finite() {
        return exec_err!("percentile_approx percentage produced a non-finite rank");
    }
    if rank <= 1.0 {
        return Ok(0);
    }
    let rank_u32 = u32::try_from(rank as i64).unwrap_or(u32::MAX);
    let index = usize::try_from(rank_u32)
        .unwrap_or(usize::MAX)
        .saturating_sub(1);
    Ok(index.min(count - 1))
}

fn pick(values: &[ScalarValue], percentages: &[f64], return_list: bool) -> Result<ScalarValue> {
    let mut working = values.to_vec();
    let count = working.len();
    let default = [0.5_f64];
    let used = if percentages.is_empty() {
        &default
    } else {
        percentages
    };
    let mut picked = Vec::with_capacity(used.len());
    for percentage in used {
        let index = discrete_index(*percentage, count)?;
        working.select_nth_unstable_by(index, |left, right| {
            left.partial_cmp(right).unwrap_or(Ordering::Equal)
        });
        picked.push(working[index].clone());
    }
    let Some(first) = picked.first() else {
        return Ok(ScalarValue::Null);
    };
    let value_type = first.data_type();
    if return_list {
        Ok(ScalarValue::List(ScalarValue::new_list_nullable(
            &picked,
            &value_type,
        )))
    } else {
        Ok(first.clone())
    }
}

#[derive(Debug)]
struct PercentileAccumulator {
    values: Vec<ScalarValue>,
    percentages: Vec<f64>,
    value_type: DataType,
    return_list: bool,
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(column) = values.first() else {
            return Ok(());
        };
        if self.percentages.is_empty()
            && let Some(percentage_column) = values.get(1)
        {
            self.percentages = extract_percentages(percentage_column)?;
        }
        for row in 0..column.len() {
            if column.is_null(row) {
                continue;
            }
            self.values.push(ScalarValue::try_from_array(column, row)?);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            if self.return_list {
                return Ok(ScalarValue::new_null_list(self.value_type.clone(), true, 1));
            }
            return ScalarValue::try_from(&self.value_type);
        }
        pick(&self.values, &self.percentages, self.return_list)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.len() * size_of::<ScalarValue>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let percentages: Vec<ScalarValue> = self
            .percentages
            .iter()
            .map(|percentage| ScalarValue::Float64(Some(*percentage)))
            .collect();
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list_nullable(
                &self.values,
                &self.value_type,
            )),
            ScalarValue::List(ScalarValue::new_list_nullable(
                &percentages,
                &DataType::Float64,
            )),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(values_state) = states.first() else {
            return Ok(());
        };
        let list = values_state.as_list::<i32>();
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let inner = list.value(row);
            for index in 0..inner.len() {
                if inner.is_null(index) {
                    continue;
                }
                self.values
                    .push(ScalarValue::try_from_array(&inner, index)?);
            }
        }
        if self.percentages.is_empty()
            && let Some(percentage_state) = states.get(1)
        {
            self.percentages = extract_percentages(percentage_state)?;
        }
        Ok(())
    }
}

impl AggregateUDFImpl for SparkPercentileApprox {
    fn name(&self) -> &'static str {
        "percentile_approx"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let Some(value_type) = arg_types.first() else {
            return exec_err!("percentile_approx requires a value column");
        };
        let percentage_type = arg_types.get(1).ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "percentile_approx requires a percentage".to_string(),
            )
        })?;
        Ok(return_data_type(value_type, percentage_type))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let value_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        let percentage_type = acc_args.exprs[1].data_type(acc_args.schema)?;
        Ok(Box::new(PercentileAccumulator {
            values: Vec::new(),
            percentages: Vec::new(),
            value_type,
            return_list: percentages_are_list(&percentage_type),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        let value_type = args.input_fields[0].data_type().clone();
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "values"),
                DataType::List(Arc::new(Field::new("item", value_type, true))),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "percentages"),
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )),
        ])
    }
}
