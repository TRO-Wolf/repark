use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array, PrimitiveArray};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::compute::{max, min};
use arrow::datatypes::{DataType, Decimal128Type, Int64Type};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{DataFusionError, HashSet, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{Accumulator, EmitTo, GroupsAccumulator};

use crate::avg_decimal::{WideDecimal, add_batch, safe_rows, sub_batch};
use crate::avg_groups::{check_emitted, filtered_nulls};
use crate::decimal_spark::numeric_out_of_range_error;
use crate::groups_null_state::GroupNullState;

pub(crate) const NARROW_PRECISION: u8 = 19;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Place {
    Ungrouped,
    Grouped,
    Window,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Carrier {
    Narrow,
    Wide,
}

impl Carrier {
    pub(crate) fn of(input_precision: u8) -> Self {
        if input_precision <= NARROW_PRECISION {
            Self::Narrow
        } else {
            Self::Wide
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SumCheck {
    limit: u128,
    precision: u8,
    scale: i8,
    raise: bool,
}

impl SumCheck {
    pub(crate) fn new(precision: u8, scale: i8, raise: bool) -> Self {
        Self {
            limit: 10_u128.pow(u32::from(precision.min(38))),
            precision,
            scale,
            raise,
        }
    }

    fn data_type(self) -> DataType {
        DataType::Decimal128(self.precision, self.scale)
    }

    fn fits(self, low: i128, wraps: i64) -> bool {
        wraps == 0 && low.unsigned_abs() < self.limit
    }

    fn outcome(self, error: DataFusionError) -> Result<Option<i128>> {
        if self.raise { Err(error) } else { Ok(None) }
    }

    fn total(
        self,
        low: i128,
        wraps: i64,
        error: impl FnOnce() -> DataFusionError,
    ) -> Result<Option<i128>> {
        if self.fits(low, wraps) {
            Ok(Some(low))
        } else {
            self.outcome(error())
        }
    }

    fn final_error(self, place: Place, merged: bool) -> DataFusionError {
        if place == Place::Window || (place == Place::Ungrouped && merged) {
            numeric_out_of_range_error(self.precision, self.scale)
        } else {
            sum_overflow_error()
        }
    }
}

pub(crate) fn sum_overflow_error() -> DataFusionError {
    DataFusionError::Execution(
        "[ARITHMETIC_OVERFLOW] Overflow in sum of decimals. Use 'try_sum' to tolerate overflow \
         and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to \
         bypass this error. SQLSTATE: 22003"
            .to_string(),
    )
}

fn unwrapped(wraps: &[i64]) -> Result<()> {
    if wraps.iter().any(|wraps| *wraps != 0) {
        return exec_err!(
            "sum state: a partial of a decimal of precision {NARROW_PRECISION} or under passed \
             128 bits, so its input holds values past its declared precision"
        );
    }
    Ok(())
}

fn bounded_rows(values: &PrimitiveArray<Decimal128Type>, rows: usize, type_rows: usize) -> usize {
    if rows <= type_rows {
        return type_rows;
    }
    let (Some(low), Some(high)) = (min(values), max(values)) else {
        return type_rows;
    };
    let magnitude = low.unsigned_abs().max(high.unsigned_abs()).max(1);
    usize::try_from(i128::MAX.cast_unsigned() / magnitude).unwrap_or(usize::MAX)
}

fn decimals<'a>(values: &'a [ArrayRef], what: &str) -> Result<&'a PrimitiveArray<Decimal128Type>> {
    match values.first() {
        Some(array) if matches!(array.data_type(), DataType::Decimal128(..)) => {
            Ok(array.as_primitive::<Decimal128Type>())
        }
        _ => exec_err!("sum {what}: one Decimal128 argument expected"),
    }
}

fn partial_wraps(states: &[ArrayRef], rows: usize) -> Result<&PrimitiveArray<Int64Type>> {
    match states.get(1) {
        Some(array) if array.data_type() == &DataType::Int64 && array.len() == rows => {
            Ok(array.as_primitive::<Int64Type>())
        }
        _ => exec_err!("sum merge: an Int64 wrap count state expected"),
    }
}

fn wraps_array(wraps: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::new(wraps.into(), None))
}

#[derive(Debug)]
pub(crate) struct DecimalSum {
    low: i128,
    wraps: i64,
    rows: u64,
    type_rows: usize,
    check: SumCheck,
    place: Place,
    carrier: Carrier,
}

impl DecimalSum {
    pub(crate) fn new(input_precision: u8, check: SumCheck, place: Place) -> Self {
        Self {
            low: 0,
            wraps: 0,
            rows: 0,
            type_rows: safe_rows::<i128>(input_precision),
            check,
            place,
            carrier: Carrier::of(input_precision),
        }
    }
}

impl Accumulator for DecimalSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = decimals(values, "update")?;
        let rows = values.len() - values.null_count();
        self.rows += rows as u64;
        let safe = bounded_rows(values, rows, self.type_rows);
        add_batch(values, safe, &mut self.low, &mut self.wraps);
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = decimals(values, "retract")?;
        let rows = values.len() - values.null_count();
        self.rows = self.rows.saturating_sub(rows as u64);
        let safe = bounded_rows(values, rows, self.type_rows);
        sub_batch(values, safe, &mut self.low, &mut self.wraps);
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let partials = decimals(states, "merge")?;
        for partial in partials.iter().flatten() {
            self.rows += 1;
            self.low = self.low.add_carry(partial, &mut self.wraps);
        }
        if self.carrier == Carrier::Wide {
            let wraps = partial_wraps(states, partials.len())?;
            for (row, partial) in wraps.values().iter().enumerate() {
                if *partial != 0 && partials.is_valid(row) {
                    self.wraps = self.wraps.wrapping_add(*partial);
                }
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let low = (self.rows > 0).then_some(self.low);
        let sum = ScalarValue::Decimal128(low, self.check.precision, self.check.scale);
        match self.carrier {
            Carrier::Narrow => {
                unwrapped(&[self.wraps])?;
                Ok(vec![sum])
            }
            Carrier::Wide => Ok(vec![sum, ScalarValue::Int64(low.map(|_| self.wraps))]),
        }
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = if self.rows == 0 {
            None
        } else {
            let (check, place) = (self.check, self.place);
            check.total(self.low, self.wraps, || check.final_error(place, false))?
        };
        Ok(ScalarValue::Decimal128(
            value,
            self.check.precision,
            self.check.scale,
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[derive(Debug)]
pub(crate) struct DecimalSumDistinct {
    values: HashSet<i128>,
    merged: bool,
    check: SumCheck,
    place: Place,
}

impl DecimalSumDistinct {
    pub(crate) fn new(check: SumCheck, place: Place) -> Self {
        Self {
            values: HashSet::default(),
            merged: false,
            check,
            place,
        }
    }

    fn insert(&mut self, values: &PrimitiveArray<Decimal128Type>) {
        if values.null_count() == 0 {
            self.values.extend(values.values().iter().copied());
        } else {
            self.values.extend(values.iter().flatten());
        }
    }
}

impl Accumulator for DecimalSumDistinct {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = decimals(values, "distinct update")?;
        self.insert(values);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(lists) = states.first() else {
            return exec_err!("sum distinct merge: one list state expected");
        };
        self.merged = true;
        for list in lists.as_list::<i32>().iter().flatten() {
            self.insert(decimals(&[list], "distinct merge")?);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values =
            PrimitiveArray::<Decimal128Type>::from_iter_values(self.values.iter().copied())
                .with_data_type(self.check.data_type());
        Ok(vec![
            SingleRowListArrayBuilder::new(Arc::new(values)).build_list_scalar(),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = if self.values.is_empty() {
            None
        } else {
            let (mut low, mut wraps) = (0_i128, 0_i64);
            for value in &self.values {
                low = low.add_carry(*value, &mut wraps);
            }
            let (check, place, merged) = (self.check, self.place, self.merged);
            check.total(low, wraps, || check.final_error(place, merged))?
        };
        Ok(ScalarValue::Decimal128(
            value,
            self.check.precision,
            self.check.scale,
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.capacity() * std::mem::size_of::<i128>()
    }
}

pub(crate) struct DecimalSumGroups {
    sums: Vec<i128>,
    wraps: Vec<i64>,
    null_state: GroupNullState,
    check: SumCheck,
    carrier: Carrier,
}

impl DecimalSumGroups {
    pub(crate) fn new(input_precision: u8, check: SumCheck) -> Self {
        Self {
            sums: Vec::new(),
            wraps: Vec::new(),
            null_state: GroupNullState::new(),
            check,
            carrier: Carrier::of(input_precision),
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.sums.resize(total_num_groups, 0);
        self.wraps.resize(total_num_groups, 0);
    }

    fn fold_rows(
        &mut self,
        values: &PrimitiveArray<Decimal128Type>,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize(total_num_groups);
        let sums = &mut self.sums;
        let wraps = &mut self.wraps;
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, value| {
                sums[group_index] = sums[group_index].add_carry(value, &mut wraps[group_index]);
            },
        )
    }

    fn fold_partials(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let partials = decimals(states, "groups merge")?;
        self.fold_rows(partials, group_indices, opt_filter, total_num_groups)?;
        if self.carrier == Carrier::Narrow {
            return Ok(());
        }
        let partial_wraps = partial_wraps(states, partials.len())?;
        if partial_wraps.values().iter().all(|wraps| *wraps == 0) {
            return Ok(());
        }
        let wraps = &mut self.wraps;
        for (row, (group_index, partial)) in
            group_indices.iter().zip(partial_wraps.values()).enumerate()
        {
            if *partial != 0
                && partials.is_valid(row)
                && opt_filter.is_none_or(|filter| filter.is_valid(row) && filter.value(row))
                && let Some(total) = wraps.get_mut(*group_index)
            {
                *total = total.wrapping_add(*partial);
            }
        }
        Ok(())
    }

    fn taken(&mut self, emit_to: EmitTo) -> Result<(Vec<i128>, Vec<i64>, Option<NullBuffer>)> {
        let nulls = self.null_state.build(emit_to)?;
        let sums = emit_to.take_needed(&mut self.sums);
        let wraps = emit_to.take_needed(&mut self.wraps);
        check_emitted(wraps.len(), sums.len(), nulls.as_ref())?;
        Ok((sums, wraps, nulls))
    }

    fn array(&self, sums: ScalarBuffer<i128>, nulls: Option<NullBuffer>) -> ArrayRef {
        Arc::new(
            PrimitiveArray::<Decimal128Type>::new(sums, nulls)
                .with_data_type(self.check.data_type()),
        )
    }

    fn shipped(&self, sums: ArrayRef, wraps: Vec<i64>) -> Result<Vec<ArrayRef>> {
        match self.carrier {
            Carrier::Narrow => {
                unwrapped(&wraps)?;
                Ok(vec![sums])
            }
            Carrier::Wide => Ok(vec![sums, wraps_array(wraps)]),
        }
    }
}

impl GroupsAccumulator for DecimalSumGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let values = decimals(values, "groups update")?;
        self.fold_rows(values, group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.fold_partials(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let check = self.check;
        let (sums, wraps, nulls) = self.taken(emit_to)?;
        let past = sums
            .iter()
            .zip(&wraps)
            .any(|(low, wraps)| !check.fits(*low, *wraps));
        if past && check.raise {
            return Err(sum_overflow_error());
        }
        let nulls = if past {
            let kept: Vec<bool> = sums
                .iter()
                .zip(&wraps)
                .map(|(low, wraps)| check.fits(*low, *wraps))
                .collect();
            NullBuffer::union(nulls.as_ref(), Some(&NullBuffer::from(kept)))
        } else {
            nulls
        };
        Ok(self.array(sums.into(), nulls))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (sums, wraps, nulls) = self.taken(emit_to)?;
        self.shipped(self.array(sums.into(), nulls), wraps)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = decimals(values, "groups convert")?;
        let sums = self.array(values.values().clone(), filtered_nulls(opt_filter, values));
        self.shipped(sums, vec![0; values.len()])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.sums.capacity() * std::mem::size_of::<i128>()
            + self.wraps.capacity() * std::mem::size_of::<i64>()
            + self.null_state.size()
    }
}
