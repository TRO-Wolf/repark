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
    wide_months: i128,
    wide_micros: i128,
    count: u64,
    overflowed: bool,
    merged_overflow: bool,
    null_on_overflow: bool,
}

impl IntervalAvgAccumulator {
    pub(crate) fn with_null_on_overflow(null_on_overflow: bool) -> Self {
        Self {
            null_on_overflow,
            ..Self::default()
        }
    }

    fn narrow_sums(&self) -> Option<(i64, i64)> {
        if self.merged_overflow {
            return None;
        }
        i64::try_from(self.wide_months)
            .ok()
            .zip(i64::try_from(self.wide_micros).ok())
    }

    fn refresh_overflow(&mut self) {
        self.overflowed = self.narrow_sums().is_none();
    }

    fn add_value(&mut self, months: i64, micros: i64) {
        self.wide_months += i128::from(months);
        self.wide_micros += i128::from(micros);
        self.count += 1;
        self.refresh_overflow();
    }

    fn sub_value(&mut self, months: i64, micros: i64) {
        self.wide_months -= i128::from(months);
        self.wide_micros -= i128::from(micros);
        self.count = self.count.saturating_sub(1);
        self.refresh_overflow();
    }

    fn merge_sums(&mut self, months: i64, micros: i64, count: u64) {
        self.wide_months += i128::from(months);
        self.wide_micros += i128::from(micros);
        self.count += count;
        self.refresh_overflow();
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
        let (months, micros) = self.narrow_sums().ok_or_else(|| {
            DataFusionError::Execution("avg interval: sums escaped narrow range".to_string())
        })?;
        if micros == 0 {
            let months = round_half_away(months, self.count)?;
            let months = i32::try_from(months).map_err(|_| interval_overflow())?;
            return Ok(Some(IntervalMonthDayNano::new(months, 0, 0)));
        }
        if months == 0 {
            let micros = round_half_away(micros, self.count)?;
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

fn typed_values<'a, T: 'static>(array: &'a ArrayRef, what: &str) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| DataFusionError::Execution(format!("avg interval: {what} mistyped")))
}

fn state_count(counts: &Int64Array, row: usize) -> Result<u64> {
    u64::try_from(counts.value(row).max(0)).map_err(|_| {
        DataFusionError::Execution("avg interval state count out of range".to_string())
    })
}

impl IntervalAvgAccumulator {
    fn ingest(&mut self, parts: (i64, i64), add: bool) {
        if add {
            self.add_value(parts.0, parts.1);
        } else {
            self.sub_value(parts.0, parts.1);
        }
    }

    fn apply_month_day_nanos(
        &mut self,
        values: &IntervalMonthDayNanoArray,
        add: bool,
    ) -> Result<()> {
        for index in 0..values.len() {
            if !values.is_null(index) {
                self.ingest(month_day_nano_parts(&values.value(index))?, add);
            }
        }
        Ok(())
    }

    fn apply_year_months(&mut self, values: &Int32Array, add: bool) {
        for index in 0..values.len() {
            if !values.is_null(index) {
                self.ingest((i64::from(values.value(index)), 0), add);
            }
        }
    }

    fn apply_day_times(&mut self, values: &Int64Array, add: bool) -> Result<()> {
        for index in 0..values.len() {
            if !values.is_null(index) {
                let micros = values
                    .value(index)
                    .checked_mul(1_000)
                    .ok_or_else(interval_overflow)?;
                self.ingest((0, micros), add);
            }
        }
        Ok(())
    }

    fn apply_durations(&mut self, array: &ArrayRef, unit: TimeUnit, add: bool) -> Result<()> {
        match unit {
            TimeUnit::Second => {
                let values = typed_values::<DurationSecondArray>(array, "duration")?;
                for index in 0..values.len() {
                    if !values.is_null(index) {
                        self.ingest((0, duration_micros(values.value(index), unit)?), add);
                    }
                }
            }
            TimeUnit::Millisecond => {
                let values = typed_values::<DurationMillisecondArray>(array, "duration")?;
                for index in 0..values.len() {
                    if !values.is_null(index) {
                        self.ingest((0, duration_micros(values.value(index), unit)?), add);
                    }
                }
            }
            TimeUnit::Microsecond => {
                let values = typed_values::<DurationMicrosecondArray>(array, "duration")?;
                for index in 0..values.len() {
                    if !values.is_null(index) {
                        self.ingest((0, duration_micros(values.value(index), unit)?), add);
                    }
                }
            }
            TimeUnit::Nanosecond => {
                let values = typed_values::<DurationNanosecondArray>(array, "duration")?;
                for index in 0..values.len() {
                    if !values.is_null(index) {
                        self.ingest((0, duration_micros(values.value(index), unit)?), add);
                    }
                }
            }
        }
        Ok(())
    }

    fn apply_batch(&mut self, array: &ArrayRef, add: bool) -> Result<()> {
        match array.data_type() {
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let values = typed_values::<IntervalMonthDayNanoArray>(array, "month-day-nano")?;
                self.apply_month_day_nanos(values, add)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                let values = typed_values::<Int32Array>(array, "year-month")?;
                self.apply_year_months(values, add);
                Ok(())
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let values = typed_values::<Int64Array>(array, "day-time")?;
                self.apply_day_times(values, add)
            }
            DataType::Duration(unit) => self.apply_durations(array, *unit, add),
            other => Err(DataFusionError::Execution(format!(
                "avg interval: unsupported input {other}"
            ))),
        }
    }
}

impl Accumulator for IntervalAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.apply_batch(&values[0], true)
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
        match self.narrow_sums() {
            Some((months, micros)) => Ok(vec![
                ScalarValue::Int64(Some(months)),
                ScalarValue::Int64(Some(micros)),
                ScalarValue::Int64(Some(count)),
            ]),
            None => Ok(vec![
                ScalarValue::Int64(None),
                ScalarValue::Int64(None),
                ScalarValue::Int64(Some(count)),
            ]),
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let months = typed_values::<Int64Array>(&states[0], "state months")?;
        let micros = typed_values::<Int64Array>(&states[1], "state micros")?;
        let counts = typed_values::<Int64Array>(&states[2], "state counts")?;
        for row in 0..months.len() {
            let count = state_count(counts, row)?;
            if months.is_null(row) || micros.is_null(row) {
                self.merged_overflow = true;
                self.count += count;
                self.refresh_overflow();
            } else {
                self.merge_sums(months.value(row), micros.value(row), count);
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.apply_batch(&values[0], false)
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

    fn state_column(values: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(values)) as ArrayRef
    }

    fn merge_states(states: &[Vec<ScalarValue>], null_on_overflow: bool) -> IntervalAvgAccumulator {
        let columns: Vec<ArrayRef> = (0..3)
            .map(|column| {
                state_column(
                    states
                        .iter()
                        .map(|state| match &state[column] {
                            ScalarValue::Int64(value) => *value,
                            other => panic!("interval state mistyped: {other}"),
                        })
                        .collect(),
                )
            })
            .collect();
        let mut merged = IntervalAvgAccumulator::with_null_on_overflow(null_on_overflow);
        merged.merge_batch(&columns).expect("merge states");
        merged
    }

    #[test]
    fn merge_batch_consumes_every_state_row() {
        let first = month_day_nanos(vec![IntervalMonthDayNano::new(0, 1, 0)]);
        let second = month_day_nanos(vec![IntervalMonthDayNano::new(0, 2, 0)]);
        let mut left = IntervalAvgAccumulator::with_null_on_overflow(true);
        left.update_batch(std::slice::from_ref(&first))
            .expect("left update");
        let mut right = IntervalAvgAccumulator::with_null_on_overflow(true);
        right
            .update_batch(std::slice::from_ref(&second))
            .expect("right update");
        let states = vec![
            left.state().expect("left state"),
            right.state().expect("right state"),
        ];
        let mut merged = merge_states(&states, true);
        let value = evaluated(merged.evaluate().expect("merged average"));
        assert_eq!(
            (value.months, value.days, value.nanoseconds),
            (0, 1, 43_200_000_000_000)
        );
        let merged_state = merged.state().expect("merged state");
        match &merged_state[2] {
            ScalarValue::Int64(Some(count)) => assert_eq!(*count, 2),
            other => panic!("merged count mistyped: {other}"),
        }
    }

    #[test]
    fn merge_batch_propagates_overflow_from_any_row() {
        let finite = month_day_nanos(vec![IntervalMonthDayNano::new(0, 1, 0)]);
        let huge = month_day_nanos(vec![
            IntervalMonthDayNano::new(0, 106_751_991, 0),
            IntervalMonthDayNano::new(0, 106_751_991, 0),
        ]);
        let mut left = IntervalAvgAccumulator::with_null_on_overflow(true);
        left.update_batch(std::slice::from_ref(&finite))
            .expect("finite update");
        let mut right = IntervalAvgAccumulator::with_null_on_overflow(true);
        right
            .update_batch(std::slice::from_ref(&huge))
            .expect("huge update");
        for states in [
            vec![
                left.state().expect("left state"),
                right.state().expect("right state"),
            ],
            vec![
                right.state().expect("right state"),
                left.state().expect("left state"),
            ],
        ] {
            let mut merged = merge_states(&states, true);
            let ScalarValue::IntervalMonthDayNano(value) =
                merged.evaluate().expect("merged overflow")
            else {
                panic!("merged overflow mistyped");
            };
            assert!(value.is_none());
        }
    }

    #[test]
    fn retract_batch_restores_finite_sums_after_overflow_leaves() {
        let huge = month_day_nanos(vec![
            IntervalMonthDayNano::new(0, 106_751_991, 0),
            IntervalMonthDayNano::new(0, 106_751_991, 0),
        ]);
        let mut accumulator = IntervalAvgAccumulator::with_null_on_overflow(true);
        accumulator
            .update_batch(std::slice::from_ref(&huge))
            .expect("overflow update");
        let ScalarValue::IntervalMonthDayNano(overflowed) =
            accumulator.evaluate().expect("overflowed average")
        else {
            panic!("overflowed average mistyped");
        };
        assert!(overflowed.is_none());
        let leaving = month_day_nanos(vec![IntervalMonthDayNano::new(0, 106_751_991, 0)]);
        accumulator
            .retract_batch(std::slice::from_ref(&leaving))
            .expect("retract leaving row");
        let value = evaluated(accumulator.evaluate().expect("restored average"));
        assert_eq!(
            (value.months, value.days, value.nanoseconds),
            (0, 106_751_991, 0)
        );
    }
}
