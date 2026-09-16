use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, Int32Array, Int64Array, IntervalMonthDayNanoArray,
};
use arrow::datatypes::{DataType, Field, FieldRef, IntervalMonthDayNano, IntervalUnit, TimeUnit};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Accumulator;

const MICROS_PER_DAY: i128 = 86_400_000_000;
const MICROS_PER_DAY_I64: i64 = 86_400_000_000;

pub(crate) fn interval_overflow() -> DataFusionError {
    DataFusionError::Plan(
        "[INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION] Integer overflow while operating \
         with intervals. Use `try_add` to tolerate overflow and return NULL instead. \
         SQLSTATE: 22015"
            .to_string(),
    )
}

#[derive(Debug, Default)]
pub(crate) struct IntervalAvgAccumulator {
    months: i64,
    micros: i64,
    count: u64,
    overflowed: bool,
    null_on_overflow: bool,
}

impl IntervalAvgAccumulator {
    pub(crate) fn with_null_on_overflow(null_on_overflow: bool) -> Self {
        Self {
            null_on_overflow,
            ..Self::default()
        }
    }

    fn add_value(&mut self, months: i64, micros: i64) {
        if self.overflowed {
            return;
        }
        match self
            .months
            .checked_add(months)
            .zip(self.micros.checked_add(micros))
        {
            Some((months, micros)) => {
                self.months = months;
                self.micros = micros;
                self.count += 1;
            }
            None => {
                self.overflowed = true;
            }
        }
    }

    fn sub_value(&mut self, months: i64, micros: i64) {
        if self.overflowed {
            return;
        }
        match self
            .months
            .checked_sub(months)
            .zip(self.micros.checked_sub(micros))
        {
            Some((months, micros)) => {
                self.months = months;
                self.micros = micros;
                self.count = self.count.saturating_sub(1);
            }
            None => {
                self.overflowed = true;
            }
        }
    }

    fn failure(&self) -> Result<Option<IntervalMonthDayNano>> {
        if self.null_on_overflow {
            return Ok(None);
        }
        Err(interval_overflow())
    }

    fn average(&self) -> Result<Option<IntervalMonthDayNano>> {
        if self.overflowed {
            return self.failure();
        }
        if self.count == 0 {
            return Ok(None);
        }
        if self.micros == 0 {
            let months = round_half_away(self.months, self.count)?;
            let months = i32::try_from(months).map_err(|_| interval_overflow())?;
            return Ok(Some(IntervalMonthDayNano::new(months, 0, 0)));
        }
        if self.months == 0 {
            let micros = round_half_away(self.micros, self.count)?;
            let days = micros.div_euclid(MICROS_PER_DAY_I64);
            let nanos = micros.rem_euclid(MICROS_PER_DAY_I64) * 1_000;
            let days = i32::try_from(days).map_err(|_| interval_overflow())?;
            return Ok(Some(IntervalMonthDayNano::new(0, days, nanos)));
        }
        self.failure()
    }
}

fn round_half_away(sum: i64, count: u64) -> Result<i64> {
    let wide_sum = i128::from(sum);
    let wide_count = i128::from(count);
    let adjust = if wide_sum >= 0 {
        wide_count
    } else {
        -wide_count
    };
    i64::try_from((2 * wide_sum + adjust) / (2 * wide_count)).map_err(|_| interval_overflow())
}

fn month_day_nano_parts(value: &IntervalMonthDayNano) -> Result<(i64, i64)> {
    let micros = i128::from(value.days) * MICROS_PER_DAY + i128::from(value.nanoseconds) / 1_000;
    let micros = i64::try_from(micros).map_err(|_| interval_overflow())?;
    Ok((i64::from(value.months), micros))
}

fn duration_micros(value: i64, unit: TimeUnit) -> Result<i64> {
    match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => Some(value / 1_000),
    }
    .ok_or_else(interval_overflow)
}

fn duration_array_micros(array: &ArrayRef, index: usize, unit: TimeUnit) -> Result<i64> {
    let raw = match unit {
        TimeUnit::Second => array
            .as_any()
            .downcast_ref::<DurationSecondArray>()
            .map(|values| values.value(index)),
        TimeUnit::Millisecond => array
            .as_any()
            .downcast_ref::<DurationMillisecondArray>()
            .map(|values| values.value(index)),
        TimeUnit::Microsecond => array
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .map(|values| values.value(index)),
        TimeUnit::Nanosecond => array
            .as_any()
            .downcast_ref::<DurationNanosecondArray>()
            .map(|values| values.value(index)),
    }
    .ok_or_else(|| DataFusionError::Execution("avg interval: duration mistyped".to_string()))?;
    duration_micros(raw, unit)
}

fn read_parts(array: &ArrayRef, index: usize) -> Result<Option<(i64, i64)>> {
    if array.is_null(index) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let values = array
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("avg interval: month-day-nano mistyped".to_string())
                })?;
            month_day_nano_parts(&values.value(index)).map(Some)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let values = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("avg interval: year-month mistyped".to_string())
            })?;
            Ok(Some((i64::from(values.value(index)), 0)))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let values = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("avg interval: day-time mistyped".to_string())
            })?;
            let micros = values
                .value(index)
                .checked_mul(1_000)
                .ok_or_else(interval_overflow)?;
            Ok(Some((0, micros)))
        }
        DataType::Duration(unit) => {
            duration_array_micros(array, index, *unit).map(|micros| Some((0, micros)))
        }
        other => Err(DataFusionError::Execution(format!(
            "avg interval: unsupported input {other}"
        ))),
    }
}

fn state_values(states: &[ArrayRef]) -> Result<(Option<(i64, i64)>, u64)> {
    let months = states[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Execution("avg interval state mistyped".to_string()))?;
    let micros = states[1]
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Execution("avg interval state mistyped".to_string()))?;
    let counts = states[2]
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Execution("avg interval state mistyped".to_string()))?;
    let count = u64::try_from(counts.value(0).max(0)).map_err(|_| {
        DataFusionError::Execution("avg interval state count out of range".to_string())
    })?;
    if months.is_null(0) || micros.is_null(0) {
        return Ok((None, count));
    }
    Ok((Some((months.value(0), micros.value(0))), count))
}

fn merge_sums(accumulator: &mut IntervalAvgAccumulator, months: i64, micros: i64, count: u64) {
    if accumulator.overflowed {
        accumulator.count += count;
        return;
    }
    if let Some((months, micros)) = accumulator
        .months
        .checked_add(months)
        .zip(accumulator.micros.checked_add(micros))
    {
        accumulator.months = months;
        accumulator.micros = micros;
        accumulator.count += count;
    } else {
        accumulator.overflowed = true;
        accumulator.count += count;
    }
}

impl Accumulator for IntervalAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        for index in 0..array.len() {
            if let Some((months, micros)) = read_parts(array, index)? {
                self.add_value(months, micros);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::IntervalMonthDayNano(self.average()?))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let count = i64::try_from(self.count).map_err(|_| {
            DataFusionError::Execution("avg interval state count out of range".to_string())
        })?;
        if self.overflowed {
            return Ok(vec![
                ScalarValue::Int64(None),
                ScalarValue::Int64(None),
                ScalarValue::Int64(Some(count)),
            ]);
        }
        Ok(vec![
            ScalarValue::Int64(Some(self.months)),
            ScalarValue::Int64(Some(self.micros)),
            ScalarValue::Int64(Some(count)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let (sums, count) = state_values(states)?;
        if let Some((months, micros)) = sums {
            merge_sums(self, months, micros, count);
        } else {
            self.overflowed = true;
            self.count += count;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        for index in 0..array.len() {
            if let Some((months, micros)) = read_parts(array, index)? {
                self.sub_value(months, micros);
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

pub(crate) fn interval_state_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("months", DataType::Int64, true)),
        Arc::new(Field::new("micros", DataType::Int64, true)),
        Arc::new(Field::new("count", DataType::Int64, true)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn month_day_nanos(values: Vec<IntervalMonthDayNano>) -> ArrayRef {
        Arc::new(IntervalMonthDayNanoArray::from(values)) as ArrayRef
    }

    fn evaluate(values: &ArrayRef, null_on_overflow: bool) -> Result<ScalarValue> {
        let mut accumulator = IntervalAvgAccumulator::with_null_on_overflow(null_on_overflow);
        accumulator.update_batch(std::slice::from_ref(values))?;
        accumulator.evaluate()
    }

    fn evaluated(value: ScalarValue) -> IntervalMonthDayNano {
        match value {
            ScalarValue::IntervalMonthDayNano(Some(value)) => value,
            other => panic!("interval average mistyped: {other}"),
        }
    }

    #[test]
    fn day_time_average_is_exact() {
        let values = month_day_nanos(vec![
            IntervalMonthDayNano::new(0, 1, 0),
            IntervalMonthDayNano::new(0, 2, 0),
        ]);
        let value = evaluated(evaluate(&values, true).expect("day-time average"));
        assert_eq!(
            (value.months, value.days, value.nanoseconds),
            (0, 1, 43_200_000_000_000)
        );
    }

    #[test]
    fn year_month_average_rounds_half_away() {
        let values = month_day_nanos(vec![
            IntervalMonthDayNano::new(1, 0, 0),
            IntervalMonthDayNano::new(2, 0, 0),
        ]);
        let value = evaluated(evaluate(&values, true).expect("year-month average"));
        assert_eq!((value.months, value.days, value.nanoseconds), (2, 0, 0));
    }

    #[test]
    fn empty_average_is_null() {
        let values = month_day_nanos(Vec::new());
        let ScalarValue::IntervalMonthDayNano(value) =
            evaluate(&values, true).expect("empty average")
        else {
            panic!("empty average mistyped");
        };
        assert!(value.is_none());
    }

    #[test]
    fn all_null_input_is_null() {
        let values: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            None::<IntervalMonthDayNano>,
            None,
        ])) as ArrayRef;
        let ScalarValue::IntervalMonthDayNano(value) =
            evaluate(&values, true).expect("null average")
        else {
            panic!("null average mistyped");
        };
        assert!(value.is_none());
    }

    #[test]
    fn overflow_is_null_for_try_and_raises_for_avg() {
        let values = month_day_nanos(vec![
            IntervalMonthDayNano::new(0, 106_751_991, 0),
            IntervalMonthDayNano::new(0, 106_751_991, 0),
        ]);
        let ScalarValue::IntervalMonthDayNano(value) =
            evaluate(&values, true).expect("try overflow")
        else {
            panic!("try overflow mistyped");
        };
        assert!(value.is_none());
        let error = evaluate(&values, false).expect_err("avg overflow must raise");
        assert!(
            error.to_string().contains("[INTERVAL_ARITHMETIC_OVERFLOW"),
            "{error}"
        );
    }

    #[test]
    fn duration_input_averages_as_day_time() {
        let values: ArrayRef = Arc::new(arrow::array::DurationMicrosecondArray::from(vec![
            86_400_000_000_i64,
        ])) as ArrayRef;
        let value = evaluated(evaluate(&values, true).expect("duration average"));
        assert_eq!((value.months, value.days, value.nanoseconds), (0, 1, 0));
    }
}
