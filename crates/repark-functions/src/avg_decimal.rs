use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, AsArray, BooleanArray,
    BooleanBufferBuilder, PrimitiveArray, PrimitiveBuilder, UInt64Array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::{cast, sum};
use arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DataType, DecimalType, UInt64Type, i256,
};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{Accumulator, EmitTo, GroupsAccumulator};

use crate::avg_groups::{check_emitted, filtered_nulls};
use crate::decimal_spark::{div_half_up, narrow_div_half_up, numeric_out_of_range_error};
use crate::groups_null_state::GroupNullState;

const SPARK_SUM_EXTRA_DIGITS: u8 = 10;

pub(crate) trait WideDecimal: ArrowNativeTypeOp {
    fn widen(self) -> i256;
    fn narrow(value: i256) -> Option<Self>;
    fn add_carry(self, value: Self, wraps: &mut i64) -> Self;
    fn sub_carry(self, value: Self, wraps: &mut i64) -> Self;
    fn join(self, wraps: i64) -> i256;
    fn to_narrow(self) -> Option<i128>;
    fn from_narrow(value: i128) -> Option<Self>;
}

fn carry(overflowed: bool, negative: bool, wraps: &mut i64) {
    if overflowed {
        *wraps += if negative { -1 } else { 1 };
    }
}

impl WideDecimal for i32 {
    fn widen(self) -> i256 {
        i256::from_i128(i128::from(self))
    }
    fn narrow(value: i256) -> Option<Self> {
        value.to_i128().and_then(|wide| Self::try_from(wide).ok())
    }
    fn add_carry(self, value: Self, wraps: &mut i64) -> Self {
        let (total, overflowed) = self.overflowing_add(value);
        carry(overflowed, value < 0, wraps);
        total
    }
    fn sub_carry(self, value: Self, wraps: &mut i64) -> Self {
        let (total, overflowed) = self.overflowing_sub(value);
        carry(overflowed, value > 0, wraps);
        total
    }
    fn join(self, wraps: i64) -> i256 {
        i256::from_i128(i128::from(self) + (i128::from(wraps) << 32))
    }
    fn to_narrow(self) -> Option<i128> {
        Some(i128::from(self))
    }
    fn from_narrow(value: i128) -> Option<Self> {
        Self::try_from(value).ok()
    }
}

impl WideDecimal for i64 {
    fn widen(self) -> i256 {
        i256::from_i128(i128::from(self))
    }
    fn narrow(value: i256) -> Option<Self> {
        value.to_i128().and_then(|wide| Self::try_from(wide).ok())
    }
    fn add_carry(self, value: Self, wraps: &mut i64) -> Self {
        let (total, overflowed) = self.overflowing_add(value);
        carry(overflowed, value < 0, wraps);
        total
    }
    fn sub_carry(self, value: Self, wraps: &mut i64) -> Self {
        let (total, overflowed) = self.overflowing_sub(value);
        carry(overflowed, value > 0, wraps);
        total
    }
    fn join(self, wraps: i64) -> i256 {
        i256::from_i128(i128::from(self) + (i128::from(wraps) << 64))
    }
    fn to_narrow(self) -> Option<i128> {
        Some(i128::from(self))
    }
    fn from_narrow(value: i128) -> Option<Self> {
        Self::try_from(value).ok()
    }
}

impl WideDecimal for i128 {
    fn widen(self) -> i256 {
        i256::from_i128(self)
    }
    fn narrow(value: i256) -> Option<Self> {
        value.to_i128()
    }
    fn add_carry(self, value: Self, wraps: &mut i64) -> Self {
        let (total, overflowed) = self.overflowing_add(value);
        carry(overflowed, value < 0, wraps);
        total
    }
    fn sub_carry(self, value: Self, wraps: &mut i64) -> Self {
        let (total, overflowed) = self.overflowing_sub(value);
        carry(overflowed, value > 0, wraps);
        total
    }
    fn join(self, wraps: i64) -> i256 {
        i256::from_parts(self.cast_unsigned(), (self >> 127) + i128::from(wraps))
    }
    fn to_narrow(self) -> Option<i128> {
        Some(self)
    }
    fn from_narrow(value: i128) -> Option<Self> {
        Some(value)
    }
}

impl WideDecimal for i256 {
    fn widen(self) -> i256 {
        self
    }
    fn narrow(value: i256) -> Option<Self> {
        Some(value)
    }
    fn add_carry(self, value: Self, _wraps: &mut i64) -> Self {
        self.wrapping_add(value)
    }
    fn sub_carry(self, value: Self, _wraps: &mut i64) -> Self {
        self.wrapping_sub(value)
    }
    fn join(self, _wraps: i64) -> i256 {
        self
    }
    fn to_narrow(self) -> Option<i128> {
        self.to_i128()
    }
    fn from_narrow(value: i128) -> Option<Self> {
        Some(i256::from_i128(value))
    }
}

fn power_of_ten(exponent: u8) -> Result<i256> {
    i256::from_i128(10)
        .checked_pow(u32::from(exponent))
        .ok_or_else(|| DataFusionError::Execution(format!("avg: 10^{exponent} overflowed i256")))
}

fn outside(value: i256, limit: i256) -> bool {
    value
        .checked_abs()
        .is_none_or(|magnitude| magnitude >= limit)
}

fn sum_overflow_error() -> DataFusionError {
    DataFusionError::Execution(
        "[ARITHMETIC_OVERFLOW] Overflow in sum of decimals. Use 'try_avg' to tolerate overflow \
         and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to \
         bypass this error. SQLSTATE: 22003"
            .to_string(),
    )
}

#[derive(Debug, Clone)]
pub(crate) struct SparkDecimalAverage {
    sum_limit: i256,
    scale_up: i256,
    target_limit: i256,
    narrow: Option<(u128, i128, u128)>,
    target_precision: u8,
    target_scale: i8,
    null_on_overflow: bool,
}

impl SparkDecimalAverage {
    pub(crate) fn try_new(
        input: (u8, i8),
        target: (u8, i8),
        null_on_overflow: bool,
    ) -> Result<Self> {
        let max_precision = if input.0 <= DECIMAL128_MAX_PRECISION {
            DECIMAL128_MAX_PRECISION
        } else {
            DECIMAL256_MAX_PRECISION
        };
        let sum_precision = input
            .0
            .saturating_add(SPARK_SUM_EXTRA_DIGITS)
            .min(max_precision);
        let Ok(scale_delta) = u8::try_from(i16::from(target.1) - i16::from(input.1)) else {
            return exec_err!(
                "avg: result scale {} is below the input scale {}",
                target.1,
                input.1
            );
        };
        let sum_limit = power_of_ten(sum_precision)?;
        let scale_up = power_of_ten(scale_delta)?;
        let target_limit = power_of_ten(target.0)?;
        let narrow = sum_limit
            .to_i128()
            .zip(scale_up.to_i128())
            .zip(target_limit.to_i128())
            .map(|((sum, scale), target)| (sum.unsigned_abs(), scale, target.unsigned_abs()));
        Ok(Self {
            sum_limit,
            scale_up,
            target_limit,
            narrow,
            target_precision: target.0,
            target_scale: target.1,
            null_on_overflow,
        })
    }

    pub(crate) fn average(&self, sum: i256, count: u64) -> Result<Option<i256>> {
        if count == 0 {
            return Ok(None);
        }
        if let Some(narrow) = sum
            .to_i128()
            .and_then(|sum| self.narrow_quotient(sum, count))
        {
            return narrow.map(|quotient| quotient.map(i256::from_i128));
        }
        if outside(sum, self.sum_limit) {
            return self.overflow(sum_overflow_error());
        }
        let Some(numerator) = sum.checked_mul(self.scale_up) else {
            return self.overflow(self.out_of_range());
        };
        let quotient = div_half_up(numerator, i256::from_i128(i128::from(count)))?;
        if outside(quotient, self.target_limit) {
            return self.overflow(self.out_of_range());
        }
        Ok(Some(quotient))
    }

    fn narrow_quotient(&self, sum: i128, count: u64) -> Option<Result<Option<i128>>> {
        let (sum_limit, scale_up, target_limit) = self.narrow?;
        if sum.unsigned_abs() >= sum_limit {
            return Some(self.sum_overflowed().map(|_| None));
        }
        let quotient = narrow_div_half_up(sum.checked_mul(scale_up)?, i128::from(count))?;
        if quotient.unsigned_abs() >= target_limit {
            return Some(self.overflow(self.out_of_range()).map(|_| None));
        }
        Some(Ok(Some(quotient)))
    }

    pub(crate) fn average_parts<N: WideDecimal>(
        &self,
        low: N,
        wraps: i64,
        count: u64,
        overflowed: bool,
    ) -> Result<Option<N>> {
        if count == 0 {
            return Ok(None);
        }
        if overflowed {
            return self.sum_overflowed().map(|_| None);
        }
        let narrow = (wraps == 0)
            .then(|| low.to_narrow())
            .flatten()
            .and_then(|sum| self.narrow_quotient(sum, count));
        let quotient = match narrow {
            Some(quotient) => quotient?.map(N::from_narrow),
            None => self.average(low.join(wraps), count)?.map(N::narrow),
        };
        match quotient {
            None => Ok(None),
            Some(Some(value)) => Ok(Some(value)),
            Some(None) => exec_err!("avg: the result does not fit the decimal native type"),
        }
    }

    fn fits_sum(&self, sum: i256) -> bool {
        !outside(sum, self.sum_limit)
    }

    fn sum_overflowed(&self) -> Result<Option<i256>> {
        self.overflow(sum_overflow_error())
    }

    fn out_of_range(&self) -> DataFusionError {
        numeric_out_of_range_error(self.target_precision, self.target_scale)
    }

    fn overflow(&self, error: DataFusionError) -> Result<Option<i256>> {
        if self.null_on_overflow {
            Ok(None)
        } else {
            Err(error)
        }
    }
}

pub(crate) fn safe_rows<N: WideDecimal>(precision: u8) -> usize {
    let Ok(limit) = power_of_ten(precision) else {
        return 0;
    };
    let largest = limit.wrapping_sub(i256::ONE);
    if largest <= i256::ZERO {
        return 0;
    }
    N::MAX_TOTAL_ORDER
        .widen()
        .checked_div(largest)
        .and_then(i256::to_i128)
        .and_then(|rows| usize::try_from(rows).ok())
        .unwrap_or(usize::MAX)
}

pub(crate) fn add_batch<T>(
    values: &PrimitiveArray<T>,
    safe_rows: usize,
    low: &mut T::Native,
    wraps: &mut i64,
) where
    T: ArrowNumericType,
    T::Native: WideDecimal,
{
    let rows = values.len() - values.null_count();
    if rows == 0 {
        return;
    }
    if rows <= safe_rows {
        if let Some(total) = sum(values) {
            *low = low.add_carry(total, wraps);
        }
        return;
    }
    for value in values.iter().flatten() {
        *low = low.add_carry(value, wraps);
    }
}

pub(crate) fn sub_batch<T>(
    values: &PrimitiveArray<T>,
    safe_rows: usize,
    low: &mut T::Native,
    wraps: &mut i64,
) where
    T: ArrowNumericType,
    T::Native: WideDecimal,
{
    let rows = values.len() - values.null_count();
    if rows == 0 {
        return;
    }
    if rows <= safe_rows {
        if let Some(total) = sum(values) {
            *low = low.sub_carry(total, wraps);
        }
        return;
    }
    for value in values.iter().flatten() {
        *low = low.sub_carry(value, wraps);
    }
}

fn row_count(array: &dyn Array) -> Result<u64> {
    u64::try_from(array.len() - array.null_count())
        .map_err(|_| DataFusionError::Internal("avg: batch length does not fit u64".to_string()))
}

#[derive(Debug)]
pub(crate) struct DecimalAvgAccumulator<T: DecimalType + ArrowNumericType + std::fmt::Debug> {
    low: T::Native,
    wraps: i64,
    count: u64,
    overflowed: bool,
    safe_rows: usize,
    input: (u8, i8),
    target: (u8, i8),
    average: SparkDecimalAverage,
    native: PhantomData<T>,
}

impl<T> DecimalAvgAccumulator<T>
where
    T: DecimalType + ArrowNumericType + std::fmt::Debug,
    T::Native: WideDecimal,
{
    pub(crate) fn try_new(
        input: (u8, i8),
        target: (u8, i8),
        null_on_overflow: bool,
    ) -> Result<Self> {
        Ok(Self {
            low: T::Native::ZERO,
            wraps: 0,
            count: 0,
            overflowed: false,
            safe_rows: safe_rows::<T::Native>(input.0),
            input,
            target,
            average: SparkDecimalAverage::try_new(input, target, null_on_overflow)?,
            native: PhantomData,
        })
    }
}

impl<T> Accumulator for DecimalAvgAccumulator<T>
where
    T: DecimalType + ArrowNumericType + std::fmt::Debug,
    T::Native: WideDecimal,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.count += row_count(values)?;
        add_batch(values, self.safe_rows, &mut self.low, &mut self.wraps);
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value =
            self.average
                .average_parts(self.low, self.wraps, self.count, self.overflowed)?;
        ScalarValue::new_primitive::<T>(value, &T::TYPE_CONSTRUCTOR(self.target.0, self.target.1))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let shipped =
            (!self.overflowed && self.wraps == 0 && self.average.fits_sum(self.low.widen()))
                .then_some(self.low);
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::new_primitive::<T>(
                shipped,
                &T::TYPE_CONSTRUCTOR(self.input.0, self.input.1),
            )?,
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            return exec_err!("avg merge: two state arguments expected");
        }
        let counts = states[0].as_primitive::<UInt64Type>();
        let partials = states[1].as_primitive::<T>();
        self.count += sum(counts).unwrap_or_default();
        for partial in partials.iter().flatten() {
            self.low = self.low.add_carry(partial, &mut self.wraps);
        }
        if partials.null_count() > counts.null_count() {
            self.overflowed |= (0..partials.len()).any(|index| {
                partials.is_null(index) && counts.is_valid(index) && counts.value(index) > 0
            });
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.count = self.count.saturating_sub(row_count(values)?);
        sub_batch(values, self.safe_rows, &mut self.low, &mut self.wraps);
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

pub(crate) struct DecimalAvgGroups<T: DecimalType + ArrowNumericType + Send> {
    input_type: DataType,
    return_type: DataType,
    counts: Vec<u64>,
    sums: Vec<T::Native>,
    wraps: Vec<i64>,
    overflowed: Vec<bool>,
    null_state: GroupNullState,
    average: SparkDecimalAverage,
}

impl<T> DecimalAvgGroups<T>
where
    T: DecimalType + ArrowNumericType + Send,
    T::Native: WideDecimal,
{
    pub(crate) fn try_new(
        input_type: &DataType,
        return_type: &DataType,
        input: (u8, i8),
        target: (u8, i8),
        null_on_overflow: bool,
    ) -> Result<Self> {
        Ok(Self {
            input_type: input_type.clone(),
            return_type: return_type.clone(),
            counts: Vec::new(),
            sums: Vec::new(),
            wraps: Vec::new(),
            overflowed: Vec::new(),
            null_state: GroupNullState::new(),
            average: SparkDecimalAverage::try_new(input, target, null_on_overflow)?,
        })
    }

    fn coerce_input(&self, values: &[ArrayRef]) -> Result<ArrayRef> {
        if values.len() != 1 {
            return exec_err!("avg groups update: single argument expected");
        }
        if values[0].data_type() == &self.input_type {
            Ok(Arc::clone(&values[0]))
        } else {
            Ok(cast(&values[0], &self.input_type)?)
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, T::Native::ZERO);
        self.wraps.resize(total_num_groups, 0);
        self.overflowed.resize(total_num_groups, false);
    }
}

impl<T> GroupsAccumulator for DecimalAvgGroups<T>
where
    T: DecimalType + ArrowNumericType + Send,
    T::Native: WideDecimal,
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
        self.resize(total_num_groups);
        let counts = &mut self.counts;
        let sums = &mut self.sums;
        let wraps = &mut self.wraps;
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, value| {
                sums[group_index] = sums[group_index].add_carry(value, &mut wraps[group_index]);
                counts[group_index] += 1;
            },
        )
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let wraps = emit_to.take_needed(&mut self.wraps);
        let overflowed = emit_to.take_needed(&mut self.overflowed);
        let nulls = self.null_state.build(emit_to)?;
        check_emitted(counts.len(), sums.len(), nulls.as_ref())?;
        check_emitted(wraps.len(), sums.len(), None)?;
        check_emitted(overflowed.len(), sums.len(), None)?;
        let mut builder = PrimitiveBuilder::<T>::with_capacity(sums.len())
            .with_data_type(self.return_type.clone());
        let groups = sums.into_iter().zip(wraps).zip(counts).zip(overflowed);
        for (index, (((low, carried), count), overflowed)) in groups.enumerate() {
            let valid = nulls.as_ref().is_none_or(|mask| mask.is_valid(index));
            let average = if valid {
                self.average
                    .average_parts(low, carried, count, overflowed)?
            } else {
                None
            };
            builder.append_option(average);
        }
        Ok(Arc::new(builder.finish()))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let nulls = self.null_state.build(emit_to)?;
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let wraps = emit_to.take_needed(&mut self.wraps);
        let overflowed = emit_to.take_needed(&mut self.overflowed);
        check_emitted(counts.len(), sums.len(), nulls.as_ref())?;
        check_emitted(wraps.len(), sums.len(), None)?;
        check_emitted(overflowed.len(), sums.len(), None)?;
        let mut shipped = Vec::with_capacity(sums.len());
        let mut valid = BooleanBufferBuilder::new(sums.len());
        let mut dropped = false;
        for (index, (low, carried)) in sums.into_iter().zip(wraps).enumerate() {
            let kept = (!overflowed[index] && carried == 0 && self.average.fits_sum(low.widen()))
                .then_some(low);
            dropped |= kept.is_none();
            shipped.push(kept.unwrap_or(low));
            valid.append(kept.is_some() && nulls.as_ref().is_none_or(|mask| mask.is_valid(index)));
        }
        let sum_nulls = if dropped {
            Some(NullBuffer::new(valid.finish()))
        } else {
            nulls.clone()
        };
        let sums = PrimitiveArray::<T>::new(shipped.into(), sum_nulls)
            .with_data_type(self.input_type.clone());
        let counts = UInt64Array::new(counts.into(), nulls);
        Ok(vec![
            Arc::new(counts) as ArrayRef,
            Arc::new(sums) as ArrayRef,
        ])
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
        self.resize(total_num_groups);
        let partial_counts = values[0].as_primitive::<UInt64Type>();
        let partial_sums = values[1].as_primitive::<T>();
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
        let sums = &mut self.sums;
        let wraps = &mut self.wraps;
        self.null_state.accumulate(
            group_indices,
            partial_sums,
            opt_filter,
            total_num_groups,
            |group_index, partial| {
                sums[group_index] = sums[group_index].add_carry(partial, &mut wraps[group_index]);
            },
        )?;
        if partial_sums.null_count() > partial_counts.null_count() {
            for (index, group_index) in group_indices.iter().enumerate() {
                let selected =
                    opt_filter.is_none_or(|filter| filter.is_valid(index) && filter.value(index));
                if selected
                    && partial_sums.is_null(index)
                    && partial_counts.is_valid(index)
                    && partial_counts.value(index) > 0
                {
                    self.overflowed[*group_index] = true;
                }
            }
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let coerced = self.coerce_input(values)?;
        let nulls = filtered_nulls(opt_filter, coerced.as_ref());
        let sums =
            PrimitiveArray::<T>::new(coerced.as_primitive::<T>().values().clone(), nulls.clone())
                .with_data_type(self.input_type.clone());
        let counts = UInt64Array::new(vec![1_u64; sums.len()].into(), nulls);
        Ok(vec![
            Arc::new(counts) as ArrayRef,
            Arc::new(sums) as ArrayRef,
        ])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<u64>()
            + self.sums.capacity() * std::mem::size_of::<T::Native>()
            + self.wraps.capacity() * std::mem::size_of::<i64>()
            + self.overflowed.capacity()
            + self.null_state.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Decimal128Array;
    use arrow::datatypes::Decimal128Type;

    const MAX38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

    fn decimal(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(precision, scale)
                .expect("fixture"),
        )
    }

    fn ungrouped(
        input: (u8, i8),
        target: (u8, i8),
        null: bool,
    ) -> DecimalAvgAccumulator<Decimal128Type> {
        DecimalAvgAccumulator::<Decimal128Type>::try_new(input, target, null).expect("averager")
    }

    #[test]
    fn average_rounds_half_up_at_the_target_scale() {
        let average = SparkDecimalAverage::try_new((10, 2), (14, 6), false).expect("averager");
        let at = |sum: i128, count: u64| average.average(i256::from_i128(sum), count).expect("avg");
        assert_eq!(at(251, 3), Some(i256::from_i128(836_667)));
        assert_eq!(at(-251, 3), Some(i256::from_i128(-836_667)));
        assert_eq!(at(1, 32), Some(i256::from_i128(313)));
        assert_eq!(at(-1, 32), Some(i256::from_i128(-313)));
        assert_eq!(at(2, 3), Some(i256::from_i128(6_667)));
        assert_eq!(at(7, 0), None);
    }

    #[test]
    fn average_holds_a_sum_past_i128_and_checks_the_bound_once() {
        let mut acc = ungrouped((38, 0), (38, 4), false);
        let rows = decimal(
            vec![
                Some(MAX38),
                Some(MAX38),
                Some(-MAX38),
                Some(-MAX38),
                Some(5),
            ],
            38,
            0,
        );
        acc.update_batch(&[rows]).expect("update");
        assert_eq!(
            acc.evaluate().expect("s04"),
            ScalarValue::Decimal128(Some(10_000), 38, 4)
        );
    }

    #[test]
    fn sum_overflow_raises_in_ansi_and_is_null_otherwise() {
        for null in [false, true] {
            let mut acc = ungrouped((38, 0), (38, 4), null);
            acc.update_batch(&[decimal(vec![Some(MAX38), Some(MAX38)], 38, 0)])
                .expect("update");
            match acc.evaluate() {
                Ok(value) => {
                    assert!(null, "ANSI avg must raise");
                    assert!(value.is_null());
                }
                Err(error) => {
                    assert!(!null, "legacy avg must answer NULL");
                    assert!(
                        error.to_string().contains(
                            "[ARITHMETIC_OVERFLOW] Overflow in sum of decimals. Use 'try_avg'"
                        ),
                        "{error}"
                    );
                }
            }
        }
    }

    #[test]
    fn near_limit_sum_answers_where_the_old_scale_up_overflowed() {
        let wide = 9 * 10_i128.pow(33);
        let mut acc = ungrouped((38, 0), (38, 4), false);
        acc.update_batch(&[decimal(vec![Some(wide), Some(wide)], 38, 0)])
            .expect("update");
        assert_eq!(
            acc.evaluate().expect("g22"),
            ScalarValue::Decimal128(Some(wide * 10_000), 38, 4)
        );
    }

    #[test]
    fn result_out_of_range_raises_numeric_value_out_of_range() {
        let value = 9 * 10_i128.pow(35);
        let mut acc = ungrouped((38, 2), (38, 6), false);
        acc.update_batch(&[decimal(vec![Some(value), Some(value)], 38, 2)])
            .expect("update");
        let error = acc.evaluate().expect_err("b25 raises");
        assert!(
            error.to_string().contains("[NUMERIC_VALUE_OUT_OF_RANGE]"),
            "{error}"
        );
    }

    fn merged(parts: &[Vec<Option<i128>>]) -> Result<ScalarValue> {
        let mut merged = ungrouped((38, 0), (38, 4), false);
        for part in parts {
            let mut partial = ungrouped((38, 0), (38, 4), false);
            partial
                .update_batch(&[decimal(part.clone(), 38, 0)])
                .expect("update");
            let state: Vec<ArrayRef> = partial
                .state()
                .expect("state")
                .into_iter()
                .map(|value| value.to_array().expect("state array"))
                .collect();
            assert_eq!(state.len(), 2);
            assert_eq!(state[1].data_type(), &DataType::Decimal128(38, 0));
            merged.merge_batch(&state).expect("merge");
        }
        merged.evaluate()
    }

    #[test]
    fn retract_and_merge_keep_an_exact_sum_and_a_partial_past_the_sum_type_ships_null() {
        let mut acc = ungrouped((38, 0), (38, 4), false);
        let head = decimal(vec![Some(MAX38), Some(MAX38)], 38, 0);
        acc.update_batch(&[Arc::clone(&head)]).expect("update");
        acc.update_batch(&[decimal(vec![Some(5)], 38, 0)])
            .expect("update");
        acc.retract_batch(&[head]).expect("retract");
        assert_eq!(
            acc.evaluate().expect("frame"),
            ScalarValue::Decimal128(Some(50_000), 38, 4)
        );
        let singles = [MAX38, MAX38, -MAX38, -MAX38, 5].map(|value| vec![Some(value)]);
        assert_eq!(
            merged(&singles).expect("merged s04"),
            ScalarValue::Decimal128(Some(10_000), 38, 4)
        );
        let halves = [
            vec![Some(MAX38), Some(MAX38)],
            vec![Some(-MAX38), Some(-MAX38), Some(5)],
        ];
        let error = merged(&halves).expect_err("a partial past 38 digits is Spark's NULL sum");
        assert!(
            error.to_string().contains("[ARITHMETIC_OVERFLOW]"),
            "{error}"
        );
    }

    #[test]
    fn grouped_partials_merge_exactly_and_a_partial_past_the_sum_type_ships_null() {
        let groups = || {
            DecimalAvgGroups::<Decimal128Type>::try_new(
                &DataType::Decimal128(38, 0),
                &DataType::Decimal128(38, 4),
                (38, 0),
                (38, 4),
                true,
            )
            .expect("groups")
        };
        let mut partials = Vec::new();
        for (values, group) in [
            (vec![Some(MAX38), Some(MAX38)], vec![0, 1]),
            (vec![Some(MAX38), Some(MAX38)], vec![0, 1]),
            (vec![Some(-MAX38), Some(-MAX38)], vec![0, 0]),
            (vec![Some(5), Some(MAX38)], vec![0, 1]),
        ] {
            let mut partial = groups();
            partial
                .update_batch(&[decimal(values, 38, 0)], &group, None, 2)
                .expect("update");
            partials.push(partial.state(EmitTo::All).expect("state"));
        }
        assert_eq!(partials[2][1].data_type(), &DataType::Decimal128(38, 0));
        assert!(partials[2][1].is_null(0), "-2 MAX38 leaves the sum type");
        let mut merged = groups();
        for state in &partials {
            merged.merge_batch(state, &[0, 1], None, 2).expect("merge");
        }
        let result = merged.evaluate(EmitTo::All).expect("evaluate");
        let result = result.as_primitive::<Decimal128Type>();
        assert!(result.is_null(0), "group 0 met a NULL partial sum");
        assert!(result.is_null(1), "group 1 sums past 38 digits");
        let mut exact = groups();
        for value in [MAX38, MAX38, -MAX38, -MAX38, 5] {
            let mut partial = groups();
            partial
                .update_batch(&[decimal(vec![Some(value)], 38, 0)], &[0], None, 1)
                .expect("update");
            exact
                .merge_batch(&partial.state(EmitTo::All).expect("state"), &[0], None, 1)
                .expect("merge");
        }
        let result = exact.evaluate(EmitTo::All).expect("evaluate");
        assert_eq!(result.as_primitive::<Decimal128Type>().value(0), 10_000);
        let converted = groups()
            .convert_to_state(&[decimal(vec![Some(7), None], 38, 0)], None)
            .expect("convert");
        assert_eq!(converted.len(), 2);
        assert!(converted[0].is_null(1));
    }

    #[test]
    fn batch_sums_take_the_native_path_only_when_they_cannot_wrap() {
        assert_eq!(safe_rows::<i128>(38), 1);
        assert!(safe_rows::<i128>(10) > 1 << 40);
        let rows = PrimitiveArray::<Decimal128Type>::from(vec![Some(MAX38), None, Some(MAX38)]);
        let expected = i256::from_i128(MAX38).wrapping_mul(i256::from_i128(2));
        let (mut low, mut wraps) = (0_i128, 0_i64);
        add_batch(&rows, 1, &mut low, &mut wraps);
        assert_eq!(low.join(wraps), expected);
        sub_batch(&rows, 1, &mut low, &mut wraps);
        assert_eq!((low, wraps), (0, 0));
        let (mut low, mut wraps) = (0_i128, 0_i64);
        add_batch(&rows, usize::MAX, &mut low, &mut wraps);
        assert_eq!((low, wraps), (MAX38.wrapping_mul(2), 0));
    }

    #[test]
    fn carried_sums_join_back_exactly() {
        let mut wraps = 0;
        let mut total = 0_i128;
        for value in [MAX38, MAX38, MAX38, -MAX38, 5] {
            total = total.add_carry(value, &mut wraps);
        }
        assert_eq!(
            total.join(wraps),
            i256::from_i128(MAX38)
                .wrapping_mul(i256::from_i128(2))
                .wrapping_add(i256::from_i128(5))
        );
    }

    #[test]
    fn the_narrow_division_path_matches_the_wide_one() {
        let cases = [
            (5_i128, 2_i128),
            (-5, 2),
            (5, -2),
            (-5, -2),
            (1, 3),
            (2, 3),
            (-2, 3),
            (7, 7),
        ];
        for (numerator, denominator) in cases {
            let fast = div_half_up(i256::from_i128(numerator), i256::from_i128(denominator))
                .expect("fast");
            let shifted = i256::from_parts(0, 1);
            let slow = div_half_up(
                i256::from_i128(numerator).wrapping_mul(shifted),
                i256::from_i128(denominator).wrapping_mul(shifted),
            )
            .expect("slow");
            assert_eq!(fast, slow, "{numerator} / {denominator}");
        }
        assert_eq!(
            div_half_up(i256::from_i128(5), i256::from_i128(2)).expect("tie"),
            i256::from_i128(3)
        );
        assert_eq!(
            div_half_up(i256::from_i128(-5), i256::from_i128(2)).expect("tie"),
            i256::from_i128(-3)
        );
        assert!(div_half_up(i256::from_i128(1), i256::ZERO).is_err());
    }
}
