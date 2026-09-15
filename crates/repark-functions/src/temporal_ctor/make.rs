use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, TimestampMicrosecondArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::ansi::spark_ansi_enabled_from_options;

fn year_out_of_bounds(year: i64) -> DataFusionError {
    super::exec_error(format!(
        "[DATETIME_FIELD_OUT_OF_BOUNDS.WITH_SUGGESTION] Invalid value for Year (valid values \
         -999999999 - 999999999): {year}. If necessary set \"spark.sql.ansi.enabled\" to \
         \"false\" to bypass this error. SQLSTATE: 22023"
    ))
}

fn long_overflow() -> DataFusionError {
    super::exec_error("long overflow".to_string())
}

const JAVA_YEAR_MIN: i64 = -999_999_999;
const JAVA_YEAR_MAX: i64 = 999_999_999;
const NANOS_PER_DAY: i128 = 86_400_000_000_000;

fn month_length(year: i64, month: i64) -> Option<i64> {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Some(31),
        4 | 6 | 9 | 11 => Some(30),
        2 => {
            let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
            Some(if leap { 29 } else { 28 })
        }
        _ => None,
    }
}

fn civil_days(year: i64, month: i64, day: i64) -> Option<i128> {
    let length = month_length(year, month)?;
    if !(1..=length).contains(&day) {
        return None;
    }
    let shifted = if month <= 2 { year - 1 } else { year };
    let era = shifted.div_euclid(400);
    let yoe = shifted - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(i128::from(era) * 146_097 + i128::from(doe) - 719_468)
}

fn civil_nanos(wall: &CheckedWall) -> Option<i128> {
    let total_secs = wall.secs_micros.div_euclid(super::MICROS_PER_SECOND);
    let frac_micros = wall.secs_micros.rem_euclid(super::MICROS_PER_SECOND);
    if !(0..=60).contains(&total_secs) {
        return None;
    }
    let second = if total_secs == 60 { 59 } else { total_secs };
    let days = civil_days(wall.year, wall.month, wall.day)?;
    let clock = (i128::from(wall.hour) * 3_600 + i128::from(wall.minute) * 60 + i128::from(second))
        * 1_000_000_000
        + i128::from(frac_micros) * 1_000;
    let leap = if total_secs == 60 { 1_000_000_000 } else { 0 };
    Some(days * NANOS_PER_DAY + clock + leap)
}

fn time_to_day_micros(array: &dyn Array, row: usize) -> Result<Option<i64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Time32(_) | DataType::Time64(_) => {
            let casted = cast(array, &DataType::Time64(TimeUnit::Microsecond)).map_err(|_| {
                super::plan_error(format!(
                    "temporal constructor expects a TIME argument, got {}",
                    array.data_type()
                ))
            })?;
            let values = casted
                .as_any()
                .downcast_ref::<arrow::array::Time64MicrosecondArray>()
                .ok_or_else(|| super::exec_error("time cast failed".to_string()))?;
            Ok(Some(values.value(row)))
        }
        DataType::Null => Ok(None),
        other => Err(super::plan_error(format!(
            "temporal constructor expects a TIME argument, got {other}"
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimestampKind {
    Zoned,
    Ltz,
    Ntz,
}

#[derive(Debug)]
pub(crate) struct MakeTimestamp {
    name: &'static str,
    kind: TimestampKind,
    try_mode: bool,
    signature: Signature,
}

impl MakeTimestamp {
    pub(crate) fn zoned(try_mode: bool) -> Self {
        Self {
            name: if try_mode {
                "try_make_timestamp"
            } else {
                "make_timestamp"
            },
            kind: TimestampKind::Zoned,
            try_mode,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub(crate) fn ltz(try_mode: bool) -> Self {
        Self {
            name: if try_mode {
                "try_make_timestamp_ltz"
            } else {
                "make_timestamp_ltz"
            },
            kind: TimestampKind::Ltz,
            try_mode,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub(crate) fn ntz(try_mode: bool) -> Self {
        Self {
            name: if try_mode {
                "try_make_timestamp_ntz"
            } else {
                "make_timestamp_ntz"
            },
            kind: TimestampKind::Ntz,
            try_mode,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn allowed(&self, count: usize) -> bool {
        match self.kind {
            TimestampKind::Zoned | TimestampKind::Ltz => matches!(count, 2 | 3 | 6 | 7),
            TimestampKind::Ntz => matches!(count, 2 | 6),
        }
    }

    fn output_type(&self) -> DataType {
        match self.kind {
            TimestampKind::Ntz => DataType::Timestamp(TimeUnit::Microsecond, None),
            _ => DataType::Timestamp(TimeUnit::Microsecond, Some(super::TIMESTAMP_TZ.into())),
        }
    }
}

impl PartialEq for MakeTimestamp {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for MakeTimestamp {}

impl Hash for MakeTimestamp {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for MakeTimestamp {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.output_type())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let _ = args;
        Ok(Arc::new(Field::new(self.name(), self.output_type(), true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if self.allowed(arg_types.len()) {
            Ok(arg_types.to_vec())
        } else {
            Err(super::plan_error(format!(
                "'{}' expects {} arguments, got {}",
                self.name,
                match self.kind {
                    TimestampKind::Zoned | TimestampKind::Ltz => "2, 3, 6 or 7",
                    TimestampKind::Ntz => "2 or 6",
                },
                arg_types.len()
            )))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let has_zone = args.args.len() == 7 || args.args.len() == 3;
        let zone_source = if has_zone {
            super::hoisted_zone(args.args.last().ok_or_else(|| {
                super::plan_error(format!("'{}' expects a timezone argument", self.name))
            })?)?
        } else {
            super::ZoneSource::Fixed(Some(super::ZoneOffset::Named(super::resolve_session_zone(
                args.config_options.as_ref(),
            )?)))
        };
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let arrays = super::broadcast_arrays(&arrays);
        let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
        let output = self.output_type();
        let worked = precast_make_args(&arrays, has_zone)?;
        let width = worked.first().map_or(args.number_rows, Array::len);
        let mut values: Vec<Option<i64>> = Vec::with_capacity(width);
        for row in 0..width {
            let zone = match &zone_source {
                super::ZoneSource::Rows => {
                    let Some(zone) = super::zone_argument(worked[worked.len() - 1].as_ref(), row)?
                    else {
                        values.push(None);
                        continue;
                    };
                    zone
                }
                super::ZoneSource::Fixed(zone) => {
                    let Some(zone) = zone else {
                        values.push(None);
                        continue;
                    };
                    *zone
                }
            };
            values.push(self.make_value(&worked, row, zone, ansi)?);
        }
        let array = TimestampMicrosecondArray::from(values);
        let array: ArrayRef = match &output {
            DataType::Timestamp(_, Some(tz)) => {
                Arc::new(array.with_timezone_opt(Some(tz.clone()))) as ArrayRef
            }
            _ => Arc::new(array) as ArrayRef,
        };
        Ok(ColumnarValue::Array(array))
    }
}

impl MakeTimestamp {
    fn make_value(
        &self,
        worked: &[ArrayRef],
        row: usize,
        zone: super::ZoneOffset,
        ansi: bool,
    ) -> Result<Option<i64>> {
        let wall = if worked.len() <= 3 {
            datetime_form_wall(worked, row)?
        } else {
            numeric_form_wall(worked, row)?
        };
        let Some(wall) = wall else {
            return Ok(None);
        };
        if self.kind == TimestampKind::Ntz {
            let Some((naive, leap)) = build_naive(&wall, self.try_mode, ansi)? else {
                return Ok(None);
            };
            if !leap {
                return Ok(Some(naive.and_utc().timestamp_micros()));
            }
            return match naive
                .checked_add_signed(chrono::TimeDelta::try_seconds(1).unwrap_or_default())
            {
                Some(rolled) => Ok(Some(rolled.and_utc().timestamp_micros())),
                None => or_null(super::datetime_overflow(), self.try_mode, ansi),
            };
        }
        build_instant(&wall, zone, self.try_mode, ansi)
    }
}

fn precast_zone(array: &ArrayRef) -> Result<ArrayRef> {
    super::precast_column(
        array,
        &DataType::Utf8,
        super::plan_error("make_timestamp timezone expects a STRING".to_string()),
    )
}

fn precast_make_args(arrays: &[ArrayRef], has_zone: bool) -> Result<Vec<ArrayRef>> {
    if arrays.len() <= 3 {
        let data = arrays.len() - usize::from(has_zone);
        arrays
            .iter()
            .enumerate()
            .map(|(index, array)| {
                if index < data {
                    Ok(Arc::clone(array))
                } else {
                    precast_zone(array)
                }
            })
            .collect()
    } else {
        arrays
            .iter()
            .enumerate()
            .map(|(index, array)| match index {
                0..=4 => super::precast_int(array),
                5 => super::precast_secs(array),
                _ => precast_zone(array),
            })
            .collect()
    }
}

struct CheckedWall {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    secs_micros: i64,
}

fn numeric_form_wall(arrays: &[ArrayRef], row: usize) -> Result<Option<CheckedWall>> {
    let mut parts: [Option<i64>; 6] = [None; 6];
    for (index, slot) in parts.iter_mut().enumerate().take(5) {
        *slot = super::int_field(arrays[index].as_ref(), row)?;
    }
    parts[5] = super::secs_to_micros(arrays[5].as_ref(), row)?;
    if parts.iter().any(Option::is_none) {
        return Ok(None);
    }
    Ok(Some(CheckedWall {
        year: parts[0].unwrap_or(0),
        month: parts[1].unwrap_or(0),
        day: parts[2].unwrap_or(0),
        hour: parts[3].unwrap_or(0),
        minute: parts[4].unwrap_or(0),
        secs_micros: parts[5].unwrap_or(0),
    }))
}

fn datetime_form_wall(arrays: &[ArrayRef], row: usize) -> Result<Option<CheckedWall>> {
    let date = super::date_to_ymd(arrays[0].as_ref(), row)?;
    let micros = time_to_day_micros(arrays[1].as_ref(), row)?;
    match (date, micros) {
        (Some((year, month, day)), Some(day_micros)) => Ok(Some(CheckedWall {
            year: i64::from(year),
            month: i64::from(month),
            day: i64::from(day),
            hour: day_micros.div_euclid(3_600_000_000),
            minute: day_micros.rem_euclid(3_600_000_000).div_euclid(60_000_000),
            secs_micros: day_micros.rem_euclid(60_000_000),
        })),
        _ => Ok(None),
    }
}

fn or_null(error: DataFusionError, try_mode: bool, ansi: bool) -> Result<Option<i64>> {
    if try_mode || !ansi {
        Ok(None)
    } else {
        Err(error)
    }
}

fn build_naive(
    wall: &CheckedWall,
    try_mode: bool,
    ansi: bool,
) -> Result<Option<(NaiveDateTime, bool)>> {
    if !(1..=12).contains(&wall.month) {
        return or_null(super::month_out_of_bounds(wall.month), try_mode, ansi).map(|_| None);
    }
    if !(0..=23).contains(&wall.hour) {
        return or_null(super::hour_out_of_bounds(wall.hour), try_mode, ansi).map(|_| None);
    }
    if !(0..=59).contains(&wall.minute) {
        return or_null(super::minute_out_of_bounds(wall.minute), try_mode, ansi).map(|_| None);
    }
    let total_secs = wall.secs_micros.div_euclid(super::MICROS_PER_SECOND);
    let frac_micros = wall.secs_micros.rem_euclid(super::MICROS_PER_SECOND);
    if !(0..=60).contains(&total_secs) {
        return or_null(super::second_out_of_bounds(total_secs), try_mode, ansi).map(|_| None);
    }
    if total_secs == 60 && frac_micros != 0 {
        return or_null(
            super::invalid_fraction_of_second(total_secs, frac_micros),
            try_mode,
            ansi,
        )
        .map(|_| None);
    }
    if !(JAVA_YEAR_MIN..=JAVA_YEAR_MAX).contains(&wall.year) {
        return or_null(year_out_of_bounds(wall.year), try_mode, ansi).map(|_| None);
    }
    let overflow = civil_nanos(wall)
        .is_some_and(|nanos| nanos < i128::from(i64::MIN) || nanos > i128::from(i64::MAX));
    if overflow {
        if try_mode {
            return Ok(None);
        }
        return Err(long_overflow());
    }
    let month = u32::try_from(wall.month).unwrap_or(0);
    let day = u32::try_from(wall.day).unwrap_or(0);
    let year = i32::try_from(wall.year).unwrap_or(i32::MAX);
    let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
        return or_null(super::invalid_date_text(month, day), try_mode, ansi).map(|_| None);
    };
    let hour = u32::try_from(wall.hour).unwrap_or(0);
    let minute = u32::try_from(wall.minute).unwrap_or(0);
    let leap = total_secs == 60;
    let second = u32::try_from(if leap { 59 } else { total_secs }).unwrap_or(0);
    let frac = u32::try_from(frac_micros).unwrap_or(0);
    let Some(time) = NaiveTime::from_hms_micro_opt(hour, minute, second, frac) else {
        return or_null(super::invalid_date_text(month, day), try_mode, ansi).map(|_| None);
    };
    Ok(Some((date.and_time(time), leap)))
}

fn build_instant(
    wall: &CheckedWall,
    zone: super::ZoneOffset,
    try_mode: bool,
    ansi: bool,
) -> Result<Option<i64>> {
    let Some((wall, leap)) = build_naive(wall, try_mode, ansi)? else {
        return Ok(None);
    };
    match super::wall_to_instant_micros(wall, zone) {
        Some(micros) => Ok(Some(if leap {
            micros + super::MICROS_PER_SECOND
        } else {
            micros
        })),
        None => or_null(super::datetime_overflow(), try_mode, ansi),
    }
}

#[cfg(test)]
mod tests {
    use super::super::functions;

    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx_with(ansi: bool, zone: &str) -> SessionContext {
        let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), ansi);
        let config = crate::session_time_zone::with_session_time_zone(config, zone);
        let ctx = SessionContext::new_with_config(config);
        for udf in functions() {
            ctx.register_udf(udf.as_ref().clone());
        }
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in batches {
            let column = batch.column(0);
            for row in 0..batch.num_rows() {
                out.push(
                    datafusion::arrow::util::display::array_value_to_string(column, row).unwrap(),
                );
            }
        }
        out
    }

    #[tokio::test]
    async fn make_timestamp_leap_second_rolls_over() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(&ctx, "SELECT make_timestamp(2019, 6, 30, 23, 59, 60) AS v").await,
            vec!["2019-07-01T00:00:00Z".to_string()]
        );
    }

    #[tokio::test]
    async fn make_timestamp_timezone_form_ignores_session_zone() {
        let ctx = ctx_with(true, "America/New_York");
        assert_eq!(
            one(
                &ctx,
                "SELECT make_timestamp(2014, 12, 28, 6, 30, 45.887, 'CET') AS v"
            )
            .await,
            vec!["2014-12-28T05:30:45.887Z".to_string()]
        );
    }

    #[tokio::test]
    async fn make_timestamp_ntz_date_time_ignores_session_zone() {
        let ctx = ctx_with(true, "America/New_York");
        assert_eq!(
            one(
                &ctx,
                "SELECT make_timestamp_ntz(DATE'2014-12-28', TIME'06:30:45.887') AS v"
            )
            .await,
            vec!["2014-12-28T06:30:45.887".to_string()]
        );
    }

    #[tokio::test]
    async fn make_timestamp_ltz_date_time_reads_session_zone() {
        let ctx = ctx_with(true, "America/New_York");
        assert_eq!(
            one(
                &ctx,
                "SELECT make_timestamp_ltz(DATE'2014-12-28', TIME'06:30:45.887') AS v"
            )
            .await,
            vec!["2014-12-28T11:30:45.887Z".to_string()]
        );
    }

    #[tokio::test]
    async fn make_timestamp_ntz_answers_naive_wall() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT make_timestamp_ntz(2019, 6, 30, 23, 59, 60) AS v"
            )
            .await,
            vec!["2019-07-01T00:00:00".to_string()]
        );
    }

    #[tokio::test]
    async fn make_timestamp_second_out_of_bounds_names_second() {
        let ctx = ctx_with(true, "UTC");
        let error = ctx
            .sql("SELECT make_timestamp(2024, 1, 1, 0, 0, 61) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(
                "[DATETIME_FIELD_OUT_OF_BOUNDS.WITH_SUGGESTION] Invalid value for SecondOfMinute \
                 (valid values 0 - 59): 61"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn make_timestamp_second_out_of_bounds_is_null_without_ansi() {
        let ctx = ctx_with(false, "UTC");
        assert_eq!(
            one(&ctx, "SELECT make_timestamp(2024, 1, 1, 0, 0, 61) AS v").await,
            vec![String::new()]
        );
    }

    #[tokio::test]
    async fn make_timestamp_year_out_of_bounds_names_year() {
        let ctx = ctx_with(true, "UTC");
        let error = ctx
            .sql("SELECT make_timestamp(2147483647, 1, 1, 0, 0, 0) AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(
                "[DATETIME_FIELD_OUT_OF_BOUNDS.WITH_SUGGESTION] Invalid value for Year (valid \
                 values -999999999 - 999999999): 2147483647"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn make_timestamp_year_out_of_bounds_is_null_without_ansi() {
        let ctx = ctx_with(false, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT make_timestamp(2147483647, 1, 1, 0, 0, 0) AS v"
            )
            .await,
            vec![String::new()]
        );
    }

    #[tokio::test]
    async fn make_timestamp_magnitude_overflow_raises_in_both_modes() {
        for ansi in [true, false] {
            let ctx = ctx_with(ansi, "UTC");
            let error = ctx
                .sql("SELECT make_timestamp(275760, 1, 1, 0, 0, 0) AS v")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("long overflow"), "{error}");
        }
    }

    #[tokio::test]
    async fn make_timestamp_accepts_zone_spellings() {
        let ctx = ctx_with(true, "UTC");
        for (zone, want) in [
            ("'+18:00'", "2023-12-31T06:00:00Z"),
            ("'-18:00'", "2024-01-01T18:00:00Z"),
            ("'GMT+1'", "2023-12-31T23:00:00Z"),
            ("'GMT-05:00'", "2024-01-01T05:00:00Z"),
            ("'+05:30:15'", "2023-12-31T18:29:45Z"),
            ("'UTC+2'", "2023-12-31T22:00:00Z"),
            ("'GMT'", "2024-01-01T00:00:00Z"),
            ("'Z'", "2024-01-01T00:00:00Z"),
            ("'+5'", "2023-12-31T19:00:00Z"),
            ("'-0530'", "2024-01-01T05:30:00Z"),
        ] {
            assert_eq!(
                one(
                    &ctx,
                    &format!("SELECT make_timestamp(2024, 1, 1, 0, 0, 0, {zone}) AS v")
                )
                .await,
                vec![want.to_string()],
                "{zone}"
            );
        }
    }

    #[tokio::test]
    async fn make_timestamp_rejects_offsets_past_18_hours() {
        let ctx = ctx_with(true, "UTC");
        for zone in ["'+19:00'", "'+18:01'", "'-18:01'"] {
            let error = ctx
                .sql(&format!(
                    "SELECT make_timestamp(2024, 1, 1, 0, 0, 0, {zone}) AS v"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap_err()
                .to_string();
            let bare = zone.trim_matches('\'');
            assert!(
                error.contains(&format!(
                    "[INVALID_TIMEZONE] The timezone: {bare} is invalid"
                )),
                "{error}"
            );
        }
    }

    #[tokio::test]
    async fn bad_zone_raises_even_in_try_form() {
        let ctx = ctx_with(true, "UTC");
        let error = ctx
            .sql("SELECT try_make_timestamp(2019, 12, 28, 6, 30, 45.887, 'Invalid/Zone') AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("[INVALID_TIMEZONE]"), "{error}");
    }
}
