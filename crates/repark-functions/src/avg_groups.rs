use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, AsArray, BooleanArray, Int64Array,
    PrimitiveArray, PrimitiveBuilder, UInt64Array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::cast;
use arrow::datatypes::{
    ArrowNativeType, DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    DecimalType, Float64Type, Int64Type, UInt64Type,
};
use datafusion::common::{DataFusionError, Result, exec_err, not_impl_err};
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};

use crate::aggregate::DecimalAverager;
use crate::groups_null_state::GroupNullState;

pub(crate) fn groups_supported(return_type: &DataType, is_distinct: bool) -> bool {
    matches!(
        return_type,
        DataType::Float64
            | DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    ) && !is_distinct
}

pub(crate) fn create_groups(
    data_type: &DataType,
    return_type: &DataType,
    null_on_overflow: bool,
) -> Result<Box<dyn GroupsAccumulator>> {
    match (data_type, return_type) {
        (DataType::Float64, DataType::Float64) => {
            Ok(Box::new(SparkAvgGroupsAccumulator::<Float64Type, _>::new(
                data_type,
                return_type,
                true,
                float_average,
            )))
        }
        (
            DataType::Decimal32(_, sum_scale),
            DataType::Decimal32(target_precision, target_scale),
        ) => decimal_groups::<Decimal32Type>(
            data_type,
            return_type,
            *sum_scale,
            *target_precision,
            *target_scale,
            null_on_overflow,
        ),
        (
            DataType::Decimal64(_, sum_scale),
            DataType::Decimal64(target_precision, target_scale),
        ) => decimal_groups::<Decimal64Type>(
            data_type,
            return_type,
            *sum_scale,
            *target_precision,
            *target_scale,
            null_on_overflow,
        ),
        (
            DataType::Decimal128(_, sum_scale),
            DataType::Decimal128(target_precision, target_scale),
        ) => decimal_groups::<Decimal128Type>(
            data_type,
            return_type,
            *sum_scale,
            *target_precision,
            *target_scale,
            null_on_overflow,
        ),
        (
            DataType::Decimal256(_, sum_scale),
            DataType::Decimal256(target_precision, target_scale),
        ) => decimal_groups::<Decimal256Type>(
            data_type,
            return_type,
            *sum_scale,
            *target_precision,
            *target_scale,
            null_on_overflow,
        ),
        (data_type, return_type) => {
            not_impl_err!("AvgGroupsAccumulator for ({data_type} --> {return_type})")
        }
    }
}

fn decimal_groups<T>(
    data_type: &DataType,
    return_type: &DataType,
    sum_scale: i8,
    target_precision: u8,
    target_scale: i8,
    null_on_overflow: bool,
) -> Result<Box<dyn GroupsAccumulator>>
where
    T: DecimalType + ArrowNumericType + Send,
{
    let averager = match DecimalAverager::<T>::try_new(sum_scale, target_precision, target_scale) {
        Ok(averager) => Some(averager),
        Err(_) if null_on_overflow => None,
        Err(error) => return Err(error),
    };
    Ok(Box::new(SparkAvgGroupsAccumulator::<T, _>::new(
        data_type,
        return_type,
        false,
        move |sum, count| decimal_average(averager.as_ref(), sum, count, null_on_overflow),
    )))
}

fn decimal_average<T: DecimalType>(
    averager: Option<&DecimalAverager<T>>,
    sum: T::Native,
    count: u64,
    null_on_overflow: bool,
) -> Result<Option<T::Native>> {
    if count == 0 {
        return Ok(None);
    }
    let Some(averager) = averager else {
        return Ok(None);
    };
    let count_native = usize::try_from(count)
        .ok()
        .and_then(T::Native::from_usize)
        .ok_or_else(|| {
            DataFusionError::Execution(
                "avg groups evaluate: group count exceeds the decimal native type".to_string(),
            )
        })?;
    averager.avg(sum, count_native, null_on_overflow)
}

#[allow(clippy::cast_precision_loss, clippy::unnecessary_wraps)]
fn float_average(sum: f64, count: u64) -> Result<Option<f64>> {
    if count == 0 {
        Ok(None)
    } else {
        Ok(Some(sum / count as f64))
    }
}

fn filtered_nulls(opt_filter: Option<&BooleanArray>, input: &dyn Array) -> Option<NullBuffer> {
    let filter_nulls = opt_filter.map(|filter| match filter.nulls() {
        None => NullBuffer::new(filter.values().clone()),
        Some(valid) => NullBuffer::new(filter.values() & valid.inner()),
    });
    NullBuffer::union(filter_nulls.as_ref(), input.nulls())
}

struct SparkAvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<Option<T::Native>> + Send + 'static,
{
    sum_data_type: DataType,
    return_data_type: DataType,
    state_sum_first: bool,
    counts: Vec<u64>,
    sums: Vec<T::Native>,
    null_state: GroupNullState,
    avg_fn: F,
}

impl<T, F> SparkAvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<Option<T::Native>> + Send + 'static,
{
    fn new(
        sum_data_type: &DataType,
        return_data_type: &DataType,
        state_sum_first: bool,
        avg_fn: F,
    ) -> Self {
        Self {
            sum_data_type: sum_data_type.clone(),
            return_data_type: return_data_type.clone(),
            state_sum_first,
            counts: Vec::new(),
            sums: Vec::new(),
            null_state: GroupNullState::new(),
            avg_fn,
        }
    }

    fn coerce_input(&self, values: &[ArrayRef]) -> Result<ArrayRef> {
        if values.len() != 1 {
            return exec_err!("avg groups update: single argument expected");
        }
        if values[0].data_type() == &self.sum_data_type {
            Ok(Arc::clone(&values[0]))
        } else {
            Ok(cast(&values[0], &self.sum_data_type)?)
        }
    }
}

fn check_emitted(counts: usize, sums: usize, nulls: Option<&NullBuffer>) -> Result<()> {
    if counts != sums {
        return exec_err!("avg groups emit: counts and sums disagree");
    }
    if let Some(nulls) = nulls
        && nulls.len() != sums
    {
        return exec_err!("avg groups emit: null mask and sums disagree");
    }
    Ok(())
}

impl<T, F> GroupsAccumulator for SparkAvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<Option<T::Native>> + Send + 'static,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let coerced = self.coerce_input(values)?;
        let values = coerced.as_primitive::<T>();
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, T::default_value());
        let counts = &mut self.counts;
        let sums = &mut self.sums;
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                sums[group_index] = sums[group_index].add_wrapping(new_value);
                counts[group_index] += 1;
            },
        )
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let nulls = self.null_state.build(emit_to)?;
        check_emitted(counts.len(), sums.len(), nulls.as_ref())?;
        let array: PrimitiveArray<T> = match &nulls {
            Some(mask) if mask.null_count() > 0 => {
                let mut builder = PrimitiveBuilder::<T>::with_capacity(sums.len())
                    .with_data_type(self.return_data_type.clone());
                for ((sum, count), valid) in sums.into_iter().zip(counts).zip(mask.iter()) {
                    let average = if valid {
                        (self.avg_fn)(sum, count)?
                    } else {
                        None
                    };
                    match average {
                        Some(value) => builder.append_value(value),
                        None => builder.append_null(),
                    }
                }
                builder.finish()
            }
            _ => {
                let mut averages = Vec::with_capacity(sums.len());
                let mut has_null = false;
                for (sum, count) in sums.into_iter().zip(counts) {
                    let average = (self.avg_fn)(sum, count)?;
                    has_null = has_null || average.is_none();
                    averages.push(average);
                }
                if has_null {
                    let mut builder = PrimitiveBuilder::<T>::with_capacity(averages.len())
                        .with_data_type(self.return_data_type.clone());
                    for average in averages {
                        match average {
                            Some(value) => builder.append_value(value),
                            None => builder.append_null(),
                        }
                    }
                    builder.finish()
                } else {
                    let values: Vec<T::Native> = averages
                        .into_iter()
                        .map(Option::unwrap_or_default)
                        .collect();
                    PrimitiveArray::new(values.into(), nulls)
                        .with_data_type(self.return_data_type.clone())
                }
            }
        };
        Ok(Arc::new(array))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let nulls = self.null_state.build(emit_to)?;
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        check_emitted(counts.len(), sums.len(), nulls.as_ref())?;
        let sums = PrimitiveArray::<T>::new(sums.into(), nulls.clone())
            .with_data_type(self.sum_data_type.clone());
        if self.state_sum_first {
            let mut as_i64 = Vec::with_capacity(counts.len());
            for count in counts {
                as_i64.push(i64::try_from(count).map_err(|_| {
                    DataFusionError::Execution(
                        "avg groups state: group count exceeds i64".to_string(),
                    )
                })?);
            }
            let counts = Int64Array::new(as_i64.into(), nulls);
            Ok(vec![
                Arc::new(sums) as ArrayRef,
                Arc::new(counts) as ArrayRef,
            ])
        } else {
            let counts = UInt64Array::new(counts.into(), nulls);
            Ok(vec![
                Arc::new(counts) as ArrayRef,
                Arc::new(sums) as ArrayRef,
            ])
        }
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        if values.len() != 2 {
            return exec_err!("avg groups merge: two state arguments expected");
        }
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, T::default_value());
        if self.state_sum_first {
            let raw_counts = values[1].as_primitive::<Int64Type>();
            if raw_counts.len() != group_indices.len() {
                return exec_err!("avg groups merge: counts and group indices disagree");
            }
            let mut converted = Vec::with_capacity(raw_counts.len());
            for count in raw_counts {
                match count {
                    None => converted.push(0),
                    Some(count) => converted.push(u64::try_from(count).map_err(|_| {
                        DataFusionError::Execution(
                            "avg groups merge: group count is negative".to_string(),
                        )
                    })?),
                }
            }
            let partial_counts = UInt64Array::new(converted.into(), raw_counts.nulls().cloned());
            let counts = &mut self.counts;
            self.null_state.accumulate(
                group_indices,
                &partial_counts,
                opt_filter,
                total_num_groups,
                |group_index, partial| {
                    counts[group_index] += partial;
                },
            )?;
            let partial_sums = values[0].as_primitive::<T>();
            let sums = &mut self.sums;
            self.null_state.accumulate(
                group_indices,
                partial_sums,
                opt_filter,
                total_num_groups,
                |group_index, partial| {
                    sums[group_index] = sums[group_index].add_wrapping(partial);
                },
            )?;
        } else {
            let partial_counts = values[0].as_primitive::<UInt64Type>();
            let counts = &mut self.counts;
            self.null_state.accumulate(
                group_indices,
                partial_counts,
                opt_filter,
                total_num_groups,
                |group_index, partial| {
                    counts[group_index] += partial;
                },
            )?;
            let partial_sums = values[1].as_primitive::<T>();
            let sums = &mut self.sums;
            self.null_state.accumulate(
                group_indices,
                partial_sums,
                opt_filter,
                total_num_groups,
                |group_index, partial| {
                    sums[group_index] = sums[group_index].add_wrapping(partial);
                },
            )?;
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let coerced = self.coerce_input(values)?;
        let sums = coerced
            .as_primitive::<T>()
            .clone()
            .with_data_type(self.sum_data_type.clone());
        let nulls = filtered_nulls(opt_filter, &sums);
        let sums = PrimitiveArray::<T>::new(sums.into_parts().1, nulls.clone())
            .with_data_type(self.sum_data_type.clone());
        if self.state_sum_first {
            let counts = Int64Array::new(vec![1_i64; sums.len()].into(), nulls);
            Ok(vec![
                Arc::new(sums) as ArrayRef,
                Arc::new(counts) as ArrayRef,
            ])
        } else {
            let counts = UInt64Array::new(vec![1_u64; sums.len()].into(), nulls);
            Ok(vec![
                Arc::new(counts) as ArrayRef,
                Arc::new(sums) as ArrayRef,
            ])
        }
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<u64>()
            + self.sums.capacity() * std::mem::size_of::<T::Native>()
            + self.null_state.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, Float64Array,
    };
    use datafusion::prelude::SessionContext;

    fn float_groups(null_on_overflow: bool) -> Box<dyn GroupsAccumulator> {
        create_groups(&DataType::Float64, &DataType::Float64, null_on_overflow)
            .expect("float groups accumulator")
    }

    fn decimal128_groups(null_on_overflow: bool) -> Box<dyn GroupsAccumulator> {
        create_groups(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(14, 6),
            null_on_overflow,
        )
        .expect("decimal128 groups accumulator")
    }

    #[test]
    fn float_groups_update_then_evaluate_averages_per_group() {
        let mut groups = float_groups(false);
        let values = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(3.0),
            None,
            Some(5.0),
        ])) as ArrayRef;
        groups
            .update_batch(&[values], &[0, 0, 1, 1], None, 3)
            .expect("update");
        let result = groups.evaluate(EmitTo::All).expect("evaluate");
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.len(), 3);
        assert!((result.value(0) - 2.0).abs() < 1e-12);
        assert!((result.value(1) - 5.0).abs() < 1e-12);
        assert!(result.is_null(2));
    }

    #[test]
    fn float_groups_filter_marks_fully_filtered_group_null() {
        let mut groups = float_groups(false);
        let values =
            Arc::new(Float64Array::from(vec![Some(1.0), Some(3.0), Some(5.0)])) as ArrayRef;
        let filter = BooleanArray::from(vec![Some(true), Some(false), None]);
        groups
            .update_batch(&[values], &[0, 1, 1], Some(&filter), 2)
            .expect("update");
        let result = groups.evaluate(EmitTo::All).expect("evaluate");
        let result = result.as_primitive::<Float64Type>();
        assert!((result.value(0) - 1.0).abs() < 1e-12);
        assert!(result.is_null(1));
    }

    #[test]
    fn float_groups_int_input_coerces_to_float() {
        let mut groups = float_groups(false);
        let values =
            Arc::new(arrow::array::Int64Array::from(vec![Some(1), Some(3), None])) as ArrayRef;
        groups
            .update_batch(&[values], &[0, 0, 0], None, 1)
            .expect("update");
        let result = groups.evaluate(EmitTo::All).expect("evaluate");
        let result = result.as_primitive::<Float64Type>();
        assert!((result.value(0) - 2.0).abs() < 1e-12);
    }

    #[test]
    fn float_groups_state_layout_is_sum_then_int64_count() {
        let mut groups = float_groups(false);
        let values = Arc::new(Float64Array::from(vec![Some(1.0), Some(3.0)])) as ArrayRef;
        groups
            .update_batch(&[values], &[0, 1], None, 2)
            .expect("update");
        let state = groups.state(EmitTo::All).expect("state");
        assert_eq!(state.len(), 2);
        assert_eq!(state[0].data_type(), &DataType::Float64);
        assert_eq!(state[1].data_type(), &DataType::Int64);
        let sums = state[0].as_primitive::<Float64Type>();
        let counts = state[1].as_primitive::<Int64Type>();
        assert!((sums.value(0) - 1.0).abs() < 1e-12);
        assert_eq!(counts.value(1), 1);
        let mut merged = float_groups(false);
        merged.merge_batch(&state, &[0, 1], None, 2).expect("merge");
        let result = merged.evaluate(EmitTo::All).expect("evaluate");
        let result = result.as_primitive::<Float64Type>();
        assert!((result.value(0) - 1.0).abs() < 1e-12);
        assert!((result.value(1) - 3.0).abs() < 1e-12);
    }

    #[test]
    fn float_groups_merge_combines_two_partials() {
        let mut first = float_groups(false);
        let values = Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)])) as ArrayRef;
        first
            .update_batch(&[values], &[0, 0], None, 1)
            .expect("update");
        let first_state = first.state(EmitTo::All).expect("state");
        let mut second = float_groups(false);
        let values = Arc::new(Float64Array::from(vec![Some(3.0), None])) as ArrayRef;
        second
            .update_batch(&[values], &[0, 0], None, 1)
            .expect("update");
        let second_state = second.state(EmitTo::All).expect("state");
        let mut merged = float_groups(false);
        merged
            .merge_batch(&first_state, &[0], None, 1)
            .expect("merge first");
        merged
            .merge_batch(&second_state, &[0], None, 1)
            .expect("merge second");
        let result = merged.evaluate(EmitTo::All).expect("evaluate");
        let result = result.as_primitive::<Float64Type>();
        assert!((result.value(0) - 2.0).abs() < 1e-12);
    }

    #[test]
    fn float_groups_emit_first_shifts_remainder() {
        let mut groups = float_groups(false);
        let values = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(3.0),
            Some(5.0),
            Some(7.0),
        ])) as ArrayRef;
        groups
            .update_batch(&[values], &[0, 0, 1, 2], None, 3)
            .expect("update");
        let head = groups.evaluate(EmitTo::First(2)).expect("emit head");
        let head = head.as_primitive::<Float64Type>();
        assert_eq!(head.len(), 2);
        assert!((head.value(0) - 2.0).abs() < 1e-12);
        assert!((head.value(1) - 5.0).abs() < 1e-12);
        let values = Arc::new(Float64Array::from(vec![Some(9.0)])) as ArrayRef;
        groups
            .update_batch(&[values], &[0], None, 1)
            .expect("update shifted");
        let tail = groups.evaluate(EmitTo::All).expect("emit tail");
        let tail = tail.as_primitive::<Float64Type>();
        assert_eq!(tail.len(), 1);
        assert!((tail.value(0) - 8.0).abs() < 1e-12);
    }

    #[test]
    fn float_groups_convert_to_state_marks_filtered_null() {
        let groups = float_groups(false);
        let values = Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])) as ArrayRef;
        let filter = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
        assert!(groups.supports_convert_to_state());
        let state = groups
            .convert_to_state(&[values], Some(&filter))
            .expect("convert");
        assert_eq!(state.len(), 2);
        assert_eq!(state[0].data_type(), &DataType::Float64);
        assert_eq!(state[1].data_type(), &DataType::Int64);
        let sums = state[0].as_primitive::<Float64Type>();
        let counts = state[1].as_primitive::<Int64Type>();
        assert_eq!(sums.null_count(), 2);
        assert_eq!(counts.null_count(), 2);
        assert!((sums.value(0) - 1.0).abs() < 1e-12);
        assert_eq!(counts.value(0), 1);
    }

    #[test]
    fn float_groups_size_grows_with_groups() {
        let mut groups = float_groups(false);
        assert_eq!(groups.size(), 0);
        let values = Arc::new(Float64Array::from(vec![Some(1.0)])) as ArrayRef;
        groups
            .update_batch(&[values], &[0], None, 100)
            .expect("update");
        assert!(groups.size() >= 100 * std::mem::size_of::<u64>() * 2);
    }

    #[test]
    fn decimal128_groups_update_then_evaluate_scales_result() {
        let mut groups = decimal128_groups(false);
        let values = Arc::new(
            Decimal128Array::from(vec![Some(110), Some(220), Some(300)])
                .with_precision_and_scale(10, 2)
                .expect("fixture scale"),
        ) as ArrayRef;
        groups
            .update_batch(&[values], &[0, 0, 1], None, 2)
            .expect("update");
        let result = groups.evaluate(EmitTo::All).expect("evaluate");
        assert_eq!(result.data_type(), &DataType::Decimal128(14, 6));
        let result = result.as_primitive::<Decimal128Type>();
        assert_eq!(result.value(0), 1_650_000);
        assert_eq!(result.value(1), 3_000_000);
    }

    #[test]
    fn decimal128_groups_state_layout_is_count_then_sum() {
        let mut groups = decimal128_groups(false);
        let values = Arc::new(
            Decimal128Array::from(vec![Some(110)])
                .with_precision_and_scale(10, 2)
                .expect("fixture scale"),
        ) as ArrayRef;
        groups
            .update_batch(&[values], &[0], None, 1)
            .expect("update");
        let state = groups.state(EmitTo::All).expect("state");
        assert_eq!(state.len(), 2);
        assert_eq!(state[0].data_type(), &DataType::UInt64);
        assert_eq!(state[1].data_type(), &DataType::Decimal128(10, 2));
        let mut merged = decimal128_groups(false);
        merged.merge_batch(&state, &[0], None, 1).expect("merge");
        let result = merged.evaluate(EmitTo::All).expect("evaluate");
        let result = result.as_primitive::<Decimal128Type>();
        assert_eq!(result.value(0), 1_100_000);
    }

    #[test]
    fn decimal128_groups_merge_combines_three_groups_across_two_partials() {
        let mut first = decimal128_groups(false);
        let values = Arc::new(
            Decimal128Array::from(vec![Some(110), Some(220), Some(330)])
                .with_precision_and_scale(10, 2)
                .expect("fixture scale"),
        ) as ArrayRef;
        first
            .update_batch(&[values], &[0, 1, 2], None, 3)
            .expect("update first");
        let first_state = first.state(EmitTo::All).expect("first state");
        let mut second = decimal128_groups(false);
        let values = Arc::new(
            Decimal128Array::from(vec![Some(110), Some(440), Some(660)])
                .with_precision_and_scale(10, 2)
                .expect("fixture scale"),
        ) as ArrayRef;
        second
            .update_batch(&[values], &[0, 1, 2], None, 3)
            .expect("update second");
        let second_state = second.state(EmitTo::All).expect("second state");
        let mut merged = decimal128_groups(false);
        merged
            .merge_batch(&first_state, &[0, 1, 2], None, 3)
            .expect("merge first");
        merged
            .merge_batch(&second_state, &[0, 1, 2], None, 3)
            .expect("merge second");
        let result = merged.evaluate(EmitTo::All).expect("evaluate");
        let result = result.as_primitive::<Decimal128Type>();
        assert_eq!(result.value(0), 1_100_000);
        assert_eq!(result.value(1), 3_300_000);
        assert_eq!(result.value(2), 4_950_000);
    }

    #[test]
    fn decimal128_groups_empty_group_evaluates_null() {
        let mut groups = decimal128_groups(false);
        let values = Arc::new(
            Decimal128Array::from(vec![Some(110)])
                .with_precision_and_scale(10, 2)
                .expect("fixture scale"),
        ) as ArrayRef;
        groups
            .update_batch(&[values], &[0], None, 2)
            .expect("update");
        let result = groups.evaluate(EmitTo::All).expect("evaluate");
        let result = result.as_primitive::<Decimal128Type>();
        assert_eq!(result.value(0), 1_100_000);
        assert!(result.is_null(1));
    }

    #[test]
    fn decimal128_groups_filtered_group_evaluates_null() {
        let mut groups = decimal128_groups(false);
        let values = Arc::new(
            Decimal128Array::from(vec![Some(110), Some(220)])
                .with_precision_and_scale(10, 2)
                .expect("fixture scale"),
        ) as ArrayRef;
        let filter = BooleanArray::from(vec![Some(true), Some(false)]);
        groups
            .update_batch(&[values], &[0, 1], Some(&filter), 2)
            .expect("update");
        let result = groups.evaluate(EmitTo::All).expect("evaluate");
        let result = result.as_primitive::<Decimal128Type>();
        assert_eq!(result.value(0), 1_100_000);
        assert!(result.is_null(1));
    }

    #[test]
    fn decimal128_groups_try_avg_overflow_is_null_and_avg_raises() {
        let extreme = 99_999_999_999_999_999_999_999_999_999_999_999_i128;
        for null_on_overflow in [false, true] {
            let mut groups = create_groups(
                &DataType::Decimal128(38, 0),
                &DataType::Decimal128(38, 4),
                null_on_overflow,
            )
            .expect("decimal groups");
            let values = Arc::new(
                Decimal128Array::from(vec![Some(extreme), Some(extreme)])
                    .with_precision_and_scale(38, 0)
                    .expect("fixture scale"),
            ) as ArrayRef;
            groups
                .update_batch(&[values], &[0, 1], None, 2)
                .expect("update");
            if null_on_overflow {
                let result = groups.evaluate(EmitTo::All).expect("evaluate");
                let result = result.as_primitive::<Decimal128Type>();
                assert!(result.is_null(0));
                assert!(result.is_null(1));
            } else {
                let error = groups.evaluate(EmitTo::All).expect_err("avg must raise");
                assert!(error.to_string().contains("Arithmetic Overflow"), "{error}");
            }
        }
    }

    #[test]
    fn decimal32_groups_update_then_evaluate() {
        let mut groups = create_groups(
            &DataType::Decimal32(5, 2),
            &DataType::Decimal32(9, 6),
            false,
        )
        .expect("decimal32 groups");
        let values = Arc::new(
            Decimal32Array::from(vec![Some(110), Some(220)])
                .with_precision_and_scale(5, 2)
                .expect("fixture scale"),
        ) as ArrayRef;
        groups
            .update_batch(&[values], &[0, 0], None, 1)
            .expect("update");
        let result = groups.evaluate(EmitTo::All).expect("evaluate");
        assert_eq!(result.data_type(), &DataType::Decimal32(9, 6));
        assert_eq!(result.as_primitive::<Decimal32Type>().value(0), 1_650_000);
    }

    #[test]
    fn decimal64_groups_update_then_evaluate() {
        let mut groups = create_groups(
            &DataType::Decimal64(10, 2),
            &DataType::Decimal64(14, 6),
            false,
        )
        .expect("decimal64 groups");
        let values = Arc::new(
            Decimal64Array::from(vec![Some(110), Some(220)])
                .with_precision_and_scale(10, 2)
                .expect("fixture scale"),
        ) as ArrayRef;
        groups
            .update_batch(&[values], &[0, 0], None, 1)
            .expect("update");
        let result = groups.evaluate(EmitTo::All).expect("evaluate");
        assert_eq!(result.data_type(), &DataType::Decimal64(14, 6));
        assert_eq!(result.as_primitive::<Decimal64Type>().value(0), 1_650_000);
    }

    #[test]
    fn decimal256_groups_update_then_evaluate() {
        let mut groups = create_groups(
            &DataType::Decimal256(10, 2),
            &DataType::Decimal256(14, 6),
            false,
        )
        .expect("decimal256 groups");
        let values = Arc::new(
            Decimal256Array::from(vec![
                Some(arrow::datatypes::i256::from_i128(110)),
                Some(arrow::datatypes::i256::from_i128(220)),
            ])
            .with_precision_and_scale(10, 2)
            .expect("fixture scale"),
        ) as ArrayRef;
        groups
            .update_batch(&[values], &[0, 0], None, 1)
            .expect("update");
        let result = groups.evaluate(EmitTo::All).expect("evaluate");
        assert_eq!(result.data_type(), &DataType::Decimal256(14, 6));
        assert_eq!(
            result.as_primitive::<Decimal256Type>().value(0),
            arrow::datatypes::i256::from_i128(1_650_000)
        );
    }

    #[test]
    fn groups_supported_covers_float_decimals_and_refuses_distinct() {
        assert!(groups_supported(&DataType::Float64, false));
        assert!(groups_supported(&DataType::Decimal32(9, 6), false));
        assert!(groups_supported(&DataType::Decimal64(14, 6), false));
        assert!(groups_supported(&DataType::Decimal128(14, 6), false));
        assert!(groups_supported(&DataType::Decimal256(14, 6), false));
        assert!(!groups_supported(&DataType::Float64, true));
        assert!(!groups_supported(&DataType::Decimal128(14, 6), true));
        assert!(!groups_supported(&DataType::Utf8, false));
        assert!(!groups_supported(
            &DataType::Interval(arrow::datatypes::IntervalUnit::DayTime),
            false
        ));
    }

    #[test]
    fn create_groups_rejects_unsupported_pair() {
        let Err(error) = create_groups(&DataType::Utf8, &DataType::Float64, false) else {
            panic!("unsupported pair must be rejected");
        };
        assert!(
            error.to_string().contains("AvgGroupsAccumulator"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn group_by_avg_sql_answers_through_session() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let batches = ctx
            .sql(
                "SELECT k, avg(v) AS a FROM (VALUES (1, 1.0), (1, 3.0), (2, 5.0)) t(k, v) \
                 GROUP BY k ORDER BY k",
            )
            .await
            .expect("plan grouped avg")
            .collect()
            .await
            .expect("execute grouped avg");
        let column = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64");
        assert!((column.value(0) - 2.0).abs() < 1e-12);
        assert!((column.value(1) - 5.0).abs() < 1e-12);
        let batches = ctx
            .sql(
                "SELECT avg(v) AS a FROM (\
                   SELECT CAST('1.10' AS DECIMAL(10,2)) AS v, 1 AS k \
                   UNION ALL SELECT CAST('2.20' AS DECIMAL(10,2)), 1\
                 ) t GROUP BY k",
            )
            .await
            .expect("plan grouped decimal avg")
            .collect()
            .await
            .expect("execute grouped decimal avg");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal128");
        assert_eq!(column.precision(), 14);
        assert_eq!(column.scale(), 6);
        assert_eq!(column.value(0), 1_650_000);
    }
}
