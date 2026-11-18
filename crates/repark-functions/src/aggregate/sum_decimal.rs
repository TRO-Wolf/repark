use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::compute::{max, min};
use arrow::datatypes::{DataType, Decimal128Type};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{DataFusionError, HashSet, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{Accumulator, EmitTo, GroupsAccumulator};

use crate::avg_decimal::{WideDecimal, add_batch, safe_rows, sub_batch};
use crate::avg_groups::{check_emitted, filtered_nulls};
use crate::decimal_spark::numeric_out_of_range_error;
use crate::groups_null_state::GroupNullState;

pub(crate) const OVERFLOWED: i128 = i128::MIN;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Place {
    Ungrouped,
    Grouped,
    Window,
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

#[derive(Debug)]
pub(crate) struct DecimalSum {
    low: i128,
    wraps: i64,
    rows: u64,
    overflowed: bool,
    merged: bool,
    type_rows: usize,
    check: SumCheck,
    place: Place,
}

impl DecimalSum {
    pub(crate) fn new(input_precision: u8, check: SumCheck, place: Place) -> Self {
        Self {
            low: 0,
            wraps: 0,
            rows: 0,
            overflowed: false,
            merged: false,
            type_rows: safe_rows::<i128>(input_precision),
            check,
            place,
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
        self.merged = true;
        for partial in partials.iter().flatten() {
            self.rows += 1;
            if partial == OVERFLOWED {
                if self.check.raise {
                    return Err(sum_overflow_error());
                }
                self.overflowed = true;
            } else {
                self.low = self.low.add_carry(partial, &mut self.wraps);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let shipped = if self.rows == 0 {
            None
        } else if !self.overflowed && self.check.fits(self.low, self.wraps) {
            Some(self.low)
        } else if self.check.raise {
            return Err(sum_overflow_error());
        } else {
            Some(OVERFLOWED)
        };
        Ok(vec![ScalarValue::Decimal128(
            shipped,
            self.check.precision,
            self.check.scale,
        )])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = if self.rows == 0 {
            None
        } else if self.overflowed {
            self.check.outcome(sum_overflow_error())?
        } else {
            let (check, place, merged) = (self.check, self.place, self.merged);
            check.total(self.low, self.wraps, || check.final_error(place, merged))?
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
    overflowed: Vec<bool>,
    any_overflowed: bool,
    null_state: GroupNullState,
    check: SumCheck,
}

impl DecimalSumGroups {
    pub(crate) fn new(check: SumCheck) -> Self {
        Self {
            sums: Vec::new(),
            overflowed: Vec::new(),
            any_overflowed: false,
            null_state: GroupNullState::new(),
            check,
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.sums.resize(total_num_groups, 0);
        if !self.check.raise {
            self.overflowed.resize(total_num_groups, false);
        }
    }

    fn fold(
        &mut self,
        values: &PrimitiveArray<Decimal128Type>,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        partials: bool,
    ) -> Result<()> {
        self.resize(total_num_groups);
        let limit = self.check.limit;
        let sums = &mut self.sums;
        let overflowed = &mut self.overflowed;
        let mut hit = false;
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, value| {
                let mut carried = 0;
                let total = sums[group_index].add_carry(value, &mut carried);
                sums[group_index] = total;
                if carried != 0
                    || total.unsigned_abs() >= limit
                    || (partials && value == OVERFLOWED)
                {
                    hit = true;
                    if let Some(flag) = overflowed.get_mut(group_index) {
                        *flag = true;
                    }
                }
            },
        )?;
        if hit {
            if self.check.raise {
                return Err(sum_overflow_error());
            }
            self.any_overflowed = true;
        }
        Ok(())
    }

    fn taken(&mut self, emit_to: EmitTo) -> Result<(Vec<i128>, Vec<bool>, Option<NullBuffer>)> {
        let nulls = self.null_state.build(emit_to)?;
        let sums = emit_to.take_needed(&mut self.sums);
        let overflowed = if self.check.raise {
            Vec::new()
        } else {
            emit_to.take_needed(&mut self.overflowed)
        };
        check_emitted(sums.len(), sums.len(), nulls.as_ref())?;
        if !self.check.raise {
            check_emitted(overflowed.len(), sums.len(), None)?;
        }
        Ok((sums, overflowed, nulls))
    }

    fn array(&self, sums: ScalarBuffer<i128>, nulls: Option<NullBuffer>) -> ArrayRef {
        Arc::new(
            PrimitiveArray::<Decimal128Type>::new(sums, nulls)
                .with_data_type(self.check.data_type()),
        )
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
        self.fold(values, group_indices, opt_filter, total_num_groups, false)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let partials = decimals(values, "groups merge")?;
        self.fold(partials, group_indices, opt_filter, total_num_groups, true)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (sums, overflowed, nulls) = self.taken(emit_to)?;
        let nulls = if self.any_overflowed && overflowed.iter().any(|flag| *flag) {
            let kept = NullBuffer::from(overflowed.iter().map(|flag| !flag).collect::<Vec<_>>());
            NullBuffer::union(nulls.as_ref(), Some(&kept))
        } else {
            nulls
        };
        Ok(self.array(sums.into(), nulls))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (mut sums, overflowed, nulls) = self.taken(emit_to)?;
        if self.any_overflowed {
            for (sum, flag) in sums.iter_mut().zip(&overflowed) {
                if *flag {
                    *sum = OVERFLOWED;
                }
            }
        }
        Ok(vec![self.array(sums.into(), nulls)])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = decimals(values, "groups convert")?;
        let nulls = filtered_nulls(opt_filter, values);
        Ok(vec![self.array(values.values().clone(), nulls)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.sums.capacity() * std::mem::size_of::<i128>()
            + self.overflowed.capacity()
            + self.null_state.size()
    }
}
