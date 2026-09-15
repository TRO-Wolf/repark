use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray, timezone::Tz,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Datelike, NaiveDate};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::{
    TIMESTAMP_TZ, WallFields, ZoneOffset, add_months_ymd, broadcast_arrays, datetime_overflow,
    day_micros_of, int_field, nullable_wall, plan_error, resolve_session_zone,
    wall_to_instant_micros,
};
use crate::ansi::spark_ansi_enabled_from_options;
use crate::datetime::{datetime_from_micros, local_datetime_from_micros};

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::new_from_impl(TimestampAdd::new())),
        Arc::new(ScalarUDF::new_from_impl(TimestampDiff::new())),
    ]
}

#[must_use]
pub fn timestampadd_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(TimestampAdd::new()))
}

#[must_use]
pub fn timestampdiff_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(TimestampDiff::new()))
}

fn render_wall(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micros: u32,
) -> String {
    if micros == 0 {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
    } else {
        let frac = format!("{micros:06}").trim_end_matches('0').to_string();
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{frac}")
    }
}

fn datetime_overflow_add(quantity: i64, unit: &str, render: &str) -> DataFusionError {
    super::exec_error(format!(
        "[DATETIME_OVERFLOW] Datetime operation overflow: add {quantity}L {unit} to TIMESTAMP '{render}'. SQLSTATE: 22008"
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AddUnit {
    Years,
    Quarters,
    Months,
    Weeks,
    Days,
    Hours,
    Minutes,
    Seconds,
    Millis,
    Micros,
}

fn parse_add_unit(text: &str, func: &str) -> Result<AddUnit> {
    match text.to_ascii_uppercase().as_str() {
        "YEAR" => Ok(AddUnit::Years),
        "QUARTER" => Ok(AddUnit::Quarters),
        "MONTH" => Ok(AddUnit::Months),
        "WEEK" => Ok(AddUnit::Weeks),
        "DAY" | "DAYOFYEAR" => Ok(AddUnit::Days),
        "HOUR" => Ok(AddUnit::Hours),
        "MINUTE" => Ok(AddUnit::Minutes),
        "SECOND" => Ok(AddUnit::Seconds),
        "MILLISECOND" => Ok(AddUnit::Millis),
        "MICROSECOND" => Ok(AddUnit::Micros),
        _ => Err(DataFusionError::Execution(format!(
            "[INVALID_PARAMETER_VALUE.DATETIME_UNIT] The value of parameter(s) `unit` in \
             `{func}` is invalid: expects one of the units without quotes YEAR, QUARTER, MONTH, \
             WEEK, DAY, DAYOFYEAR, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, but got the \
             string literal '{text}'. SQLSTATE: 22023"
        ))),
    }
}

fn unit_canonical(unit: AddUnit) -> &'static str {
    match unit {
        AddUnit::Years => "YEAR",
        AddUnit::Quarters => "QUARTER",
        AddUnit::Months => "MONTH",
        AddUnit::Weeks => "WEEK",
        AddUnit::Days => "DAY",
        AddUnit::Hours => "HOUR",
        AddUnit::Minutes => "MINUTE",
        AddUnit::Seconds => "SECOND",
        AddUnit::Millis => "MILLISECOND",
        AddUnit::Micros => "MICROSECOND",
    }
}

fn unit_micros(unit: AddUnit) -> Option<i64> {
    match unit {
        AddUnit::Weeks => Some(7 * 86_400_000_000),
        AddUnit::Days => Some(86_400_000_000),
        AddUnit::Hours => Some(3_600_000_000),
        AddUnit::Minutes => Some(60_000_000),
        AddUnit::Seconds => Some(1_000_000),
        AddUnit::Millis => Some(1_000),
        AddUnit::Micros => Some(1),
        _ => None,
    }
}

fn unit_argument(array: &dyn Array, row: usize, func: &str) -> Result<Option<AddUnit>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("string cast did not yield Utf8".to_string())
                })?;
            parse_add_unit(values.value(row), func).map(Some)
        }
        DataType::Null => Ok(None),
        other => Err(plan_error(format!(
            "'{func}' expects a STRING unit argument, got {other}"
        ))),
    }
}

enum UnitSource {
    Rows,
    Fixed(Option<AddUnit>),
}

fn hoisted_unit(arg: &ColumnarValue, func: &str) -> Result<UnitSource> {
    match super::scalar_text(arg) {
        super::ScalarText::Rows => Ok(UnitSource::Rows),
        super::ScalarText::Null => Ok(UnitSource::Fixed(None)),
        super::ScalarText::Text(text) => {
            parse_add_unit(&text, func).map(|unit| UnitSource::Fixed(Some(unit)))
        }
        super::ScalarText::Unexpected(other) => Err(plan_error(format!(
            "'{func}' expects a STRING unit argument, got {other}"
        ))),
    }
}

enum TsFamily {
    Instant { micros: i64 },
    Naive { micros: i64 },
}

fn ts_argument(array: &dyn Array, row: usize, zone: Tz, ansi: bool) -> Result<Option<TsFamily>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Timestamp(unit, tz) => {
            let micros = if *unit == TimeUnit::Microsecond {
                array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| DataFusionError::Execution("timestamp cast failed".to_string()))?
                    .value(row)
            } else {
                let casted = cast(
                    array,
                    &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                )
                .map_err(|_| DataFusionError::Execution("timestamp cast failed".to_string()))?;
                casted
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| DataFusionError::Execution("timestamp cast failed".to_string()))?
                    .value(row)
            };
            if tz.is_some() {
                Ok(Some(TsFamily::Instant { micros }))
            } else {
                Ok(Some(TsFamily::Naive { micros }))
            }
        }
        DataType::Date32 | DataType::Date64 => {
            let wall = nullable_wall(array, row, zone, ansi)?;
            match wall {
                Some(wall) => {
                    let date = NaiveDate::from_ymd_opt(wall.year, wall.month, wall.day);
                    match date.and_then(|date| {
                        wall_to_instant_micros(date.and_hms_opt(0, 0, 0)?, ZoneOffset::Named(zone))
                    }) {
                        Some(micros) => Ok(Some(TsFamily::Instant { micros })),
                        None => {
                            if ansi {
                                Err(datetime_overflow())
                            } else {
                                Ok(None)
                            }
                        }
                    }
                }
                None => Ok(None),
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
            let wall = nullable_wall(array, row, zone, ansi)?;
            match wall {
                Some(wall) => {
                    let date = NaiveDate::from_ymd_opt(wall.year, wall.month, wall.day);
                    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                        u32::try_from(wall.day_micros.div_euclid(1_000_000)).unwrap_or(0),
                        u32::try_from(wall.day_micros.rem_euclid(1_000_000) * 1_000).unwrap_or(0),
                    );
                    match (date, time) {
                        (Some(date), Some(time)) => {
                            match wall_to_instant_micros(
                                date.and_time(time),
                                ZoneOffset::Named(zone),
                            ) {
                                Some(micros) => Ok(Some(TsFamily::Instant { micros })),
                                None => {
                                    if ansi {
                                        Err(datetime_overflow())
                                    } else {
                                        Ok(None)
                                    }
                                }
                            }
                        }
                        _ => {
                            if ansi {
                                Err(datetime_overflow())
                            } else {
                                Ok(None)
                            }
                        }
                    }
                }
                None => Ok(None),
            }
        }
        other => Err(plan_error(format!(
            "timestamp arithmetic expects a TIMESTAMP argument, got {other}"
        ))),
    }
}

#[derive(Debug)]
struct TimestampAdd {
    signature: Signature,
}

impl TimestampAdd {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for TimestampAdd {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for TimestampAdd {}

impl Hash for TimestampAdd {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for TimestampAdd {
    crate::shim_udf_boilerplate!("timestampadd");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(add_result_type(arg_types.get(2)))
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs<'_>,
    ) -> Result<arrow::datatypes::FieldRef> {
        let _ = args;
        Ok(Arc::new(arrow::datatypes::Field::new(
            self.name(),
            add_result_type(args.arg_fields.get(2).map(|field| field.data_type())),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() == 3 {
            Ok(arg_types.to_vec())
        } else {
            Err(plan_error(format!(
                "'timestampadd' expects 3 arguments, got {}",
                arg_types.len()
            )))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let unit_source = hoisted_unit(
            args.args.first().ok_or_else(|| {
                plan_error("'timestampadd' expects 3 arguments, got 0".to_string())
            })?,
            "TIMESTAMPADD",
        )?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let arrays = broadcast_arrays(&arrays);
        let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
        let zone = resolve_session_zone(args.config_options.as_ref())?;
        let output = add_result_type(Some(args.return_field.data_type()));
        let units = match &unit_source {
            UnitSource::Rows => super::precast_str(&arrays[0])?,
            UnitSource::Fixed(_) => Arc::clone(&arrays[0]),
        };
        let quantities = super::precast_int(&arrays[1])?;
        let stamps = super::precast_ts(&arrays[2])?;
        let width = arrays.first().map_or(args.number_rows, Array::len);
        let mut values: Vec<Option<i64>> = Vec::with_capacity(width);
        for row in 0..width {
            let unit = match &unit_source {
                UnitSource::Rows => unit_argument(units.as_ref(), row, "TIMESTAMPADD")?,
                UnitSource::Fixed(unit) => *unit,
            };
            let quantity = int_field(quantities.as_ref(), row)?;
            match (unit, quantity) {
                (Some(unit), Some(quantity)) => {
                    match add_to_ts(stamps.as_ref(), row, zone, ansi, unit, quantity, &output)? {
                        Some(micros) => values.push(Some(micros)),
                        None => values.push(None),
                    }
                }
                _ => values.push(None),
            }
        }
        Ok(ColumnarValue::Array(finish_ts_array(values, &output)))
    }
}

fn add_result_type(ts_type: Option<&DataType>) -> DataType {
    match ts_type {
        Some(DataType::Timestamp(_, tz)) => DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
        _ => DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into())),
    }
}

fn finish_ts_array(values: Vec<Option<i64>>, output: &DataType) -> ArrayRef {
    let array = TimestampMicrosecondArray::from(values);
    match output {
        DataType::Timestamp(_, Some(tz)) => {
            Arc::new(array.with_timezone_opt(Some(tz.clone()))) as ArrayRef
        }
        _ => Arc::new(array) as ArrayRef,
    }
}

fn wall_time(wall: &WallFields) -> Option<chrono::NaiveDateTime> {
    NaiveDate::from_ymd_opt(wall.year, wall.month, wall.day).and_then(|date| {
        chrono::NaiveTime::from_num_seconds_from_midnight_opt(
            u32::try_from(wall.day_micros.div_euclid(1_000_000)).unwrap_or(0),
            u32::try_from(wall.day_micros.rem_euclid(1_000_000) * 1_000).unwrap_or(0),
        )
        .map(|time| date.and_time(time))
    })
}

fn family_wall(family: &TsFamily, zone: Tz, ansi: bool) -> Result<Option<WallFields>> {
    let wall = match family {
        TsFamily::Instant { micros } => {
            local_datetime_from_micros(*micros, zone).map(|local| WallFields {
                year: local.year(),
                month: local.month(),
                day: local.day(),
                day_micros: day_micros_of(local),
            })
        }
        TsFamily::Naive { micros } => datetime_from_micros(*micros).map(|naive| WallFields {
            year: naive.year(),
            month: naive.month(),
            day: naive.day(),
            day_micros: day_micros_of(naive),
        }),
    };
    match wall {
        Some(wall) => Ok(Some(wall)),
        None => {
            if ansi {
                Err(datetime_overflow())
            } else {
                Ok(None)
            }
        }
    }
}

fn wall_and_family(
    family: &TsFamily,
    output: &DataType,
    zone: Tz,
    ansi: bool,
) -> Result<Option<(WallFields, bool)>> {
    let naive_out = matches!(family, TsFamily::Naive { .. })
        && !matches!(output, DataType::Timestamp(_, Some(_)));
    family_wall(family, zone, ansi).map(|wall| wall.map(|wall| (wall, naive_out)))
}

fn render_wall_of(wall: &WallFields) -> String {
    render_wall(
        wall.year,
        wall.month,
        wall.day,
        u32::try_from(wall.day_micros.div_euclid(3_600_000_000)).unwrap_or(0),
        u32::try_from(
            wall.day_micros
                .rem_euclid(3_600_000_000)
                .div_euclid(60_000_000),
        )
        .unwrap_or(0),
        u32::try_from(wall.day_micros.rem_euclid(60_000_000).div_euclid(1_000_000)).unwrap_or(0),
        u32::try_from(wall.day_micros.rem_euclid(1_000_000)).unwrap_or(0),
    )
}

fn add_calendar_to_ts(
    wall: &WallFields,
    naive_out: bool,
    zone: Tz,
    delta: i64,
    quantity: i64,
    unit: AddUnit,
    render: &dyn Fn() -> String,
) -> Result<Option<i64>> {
    let overflow = || datetime_overflow_add(quantity, unit_canonical(unit), &render());
    let Some(shifted) = add_months_ymd(wall.year, wall.month, wall.day, delta) else {
        return Err(overflow());
    };
    let target = WallFields {
        year: shifted.0,
        month: shifted.1,
        day: shifted.2,
        day_micros: wall.day_micros,
    };
    let Some(target) = wall_time(&target) else {
        return Err(overflow());
    };
    if naive_out {
        Ok(Some(target.and_utc().timestamp_micros()))
    } else {
        wall_to_instant_micros(target, ZoneOffset::Named(zone))
            .map(Some)
            .ok_or_else(overflow)
    }
}

fn add_days_to_ts(
    wall: &WallFields,
    naive_out: bool,
    zone: Tz,
    days: i64,
    quantity: i64,
    unit: AddUnit,
    render: &dyn Fn() -> String,
) -> Result<Option<i64>> {
    let overflow = || datetime_overflow_add(quantity, unit_canonical(unit), &render());
    let Some(date) = NaiveDate::from_ymd_opt(wall.year, wall.month, wall.day) else {
        return Err(overflow());
    };
    let span = chrono::Days::new(days.unsigned_abs());
    let target = if days >= 0 {
        date.checked_add_days(span)
    } else {
        date.checked_sub_days(span)
    };
    let Some(target) = target else {
        return Err(overflow());
    };
    let target = WallFields {
        year: target.year(),
        month: target.month(),
        day: target.day(),
        day_micros: wall.day_micros,
    };
    let Some(target) = wall_time(&target) else {
        return Err(overflow());
    };
    if naive_out {
        Ok(Some(target.and_utc().timestamp_micros()))
    } else {
        wall_to_instant_micros(target, ZoneOffset::Named(zone))
            .map(Some)
            .ok_or_else(overflow)
    }
}

fn add_duration_to_ts(
    micros: i64,
    quantity: i64,
    unit: AddUnit,
    render: &dyn Fn() -> String,
) -> Result<Option<i64>> {
    let overflow = || datetime_overflow_add(quantity, unit_canonical(unit), &render());
    unit_micros(unit)
        .and_then(|mult| quantity.checked_mul(mult))
        .and_then(|shift| micros.checked_add(shift))
        .map(Some)
        .ok_or_else(overflow)
}

fn add_to_ts(
    array: &dyn Array,
    row: usize,
    zone: Tz,
    ansi: bool,
    unit: AddUnit,
    quantity: i64,
    output: &DataType,
) -> Result<Option<i64>> {
    let family = ts_argument(array, row, zone, ansi)?;
    let Some(family) = family else {
        return Ok(None);
    };
    match unit {
        AddUnit::Years | AddUnit::Quarters | AddUnit::Months => {
            let Some((wall, naive_out)) = wall_and_family(&family, output, zone, ansi)? else {
                return Ok(None);
            };
            let render = || render_wall_of(&wall);
            let delta = match unit {
                AddUnit::Years => quantity.checked_mul(12),
                AddUnit::Quarters => quantity.checked_mul(3),
                _ => Some(quantity),
            };
            match delta {
                Some(delta) => {
                    add_calendar_to_ts(&wall, naive_out, zone, delta, quantity, unit, &render)
                }
                None => Err(datetime_overflow_add(
                    quantity,
                    unit_canonical(unit),
                    &render(),
                )),
            }
        }
        AddUnit::Weeks | AddUnit::Days => {
            let Some((wall, naive_out)) = wall_and_family(&family, output, zone, ansi)? else {
                return Ok(None);
            };
            let render = || render_wall_of(&wall);
            let factor = match unit {
                AddUnit::Weeks => 7,
                _ => 1,
            };
            match quantity.checked_mul(factor) {
                Some(days) => add_days_to_ts(&wall, naive_out, zone, days, quantity, unit, &render),
                None => Err(datetime_overflow_add(
                    quantity,
                    unit_canonical(unit),
                    &render(),
                )),
            }
        }
        _ => {
            let Some(wall) = family_wall(&family, zone, ansi)? else {
                return Ok(None);
            };
            let render = || render_wall_of(&wall);
            let micros = match &family {
                TsFamily::Instant { micros } | TsFamily::Naive { micros } => *micros,
            };
            add_duration_to_ts(micros, quantity, unit, &render)
        }
    }
}

#[derive(Debug)]
struct TimestampDiff {
    signature: Signature,
}

impl TimestampDiff {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for TimestampDiff {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for TimestampDiff {}

impl Hash for TimestampDiff {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for TimestampDiff {
    crate::shim_udf_boilerplate!("timestampdiff");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() == 3 {
            Ok(arg_types.to_vec())
        } else {
            Err(plan_error(format!(
                "'timestampdiff' expects 3 arguments, got {}",
                arg_types.len()
            )))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let unit_source = hoisted_unit(
            args.args.first().ok_or_else(|| {
                plan_error("'timestampdiff' expects 3 arguments, got 0".to_string())
            })?,
            "TIMESTAMPDIFF",
        )?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let arrays = broadcast_arrays(&arrays);
        let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
        let zone = resolve_session_zone(args.config_options.as_ref())?;
        let units = match &unit_source {
            UnitSource::Rows => super::precast_str(&arrays[0])?,
            UnitSource::Fixed(_) => Arc::clone(&arrays[0]),
        };
        let starts = super::precast_ts(&arrays[1])?;
        let ends = super::precast_ts(&arrays[2])?;
        let width = arrays.first().map_or(args.number_rows, Array::len);
        let mut values: Vec<Option<i64>> = Vec::with_capacity(width);
        for row in 0..width {
            let unit = match &unit_source {
                UnitSource::Rows => unit_argument(units.as_ref(), row, "TIMESTAMPDIFF")?,
                UnitSource::Fixed(unit) => *unit,
            };
            let start = ts_argument(starts.as_ref(), row, zone, ansi)?;
            let end = ts_argument(ends.as_ref(), row, zone, ansi)?;
            match (unit, start, end) {
                (Some(unit), Some(start), Some(end)) => {
                    values.push(diff_ts(unit, &start, &end, zone, ansi)?);
                }
                _ => values.push(None),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(Int64Array::from(values))))
    }
}

fn diff_ts(
    unit: AddUnit,
    start: &TsFamily,
    end: &TsFamily,
    zone: Tz,
    ansi: bool,
) -> Result<Option<i64>> {
    match unit {
        AddUnit::Years | AddUnit::Quarters | AddUnit::Months => {
            let (start_wall, end_wall) = diff_walls(start, end, zone);
            let (Some(start_wall), Some(end_wall)) = (start_wall, end_wall) else {
                return Ok(None);
            };
            let mut months = i64::from(end_wall.year - start_wall.year) * 12
                + i64::from(end_wall.month)
                - i64::from(start_wall.month);
            if months > 0
                && (end_wall.day, end_wall.day_micros) < (start_wall.day, start_wall.day_micros)
            {
                months -= 1;
            } else if months < 0
                && (end_wall.day, end_wall.day_micros) > (start_wall.day, start_wall.day_micros)
            {
                months += 1;
            }
            Ok(Some(match unit {
                AddUnit::Years => months / 12,
                AddUnit::Quarters => months / 3,
                _ => months,
            }))
        }
        AddUnit::Weeks | AddUnit::Days => {
            let (start_wall, end_wall) = diff_walls(start, end, zone);
            let (Some(start_wall), Some(end_wall)) = (start_wall, end_wall) else {
                return Ok(None);
            };
            let days = wall_epoch_day(&end_wall).checked_sub(wall_epoch_day(&start_wall));
            let Some(days) = days else {
                return overflow_or_null(ansi);
            };
            let partial = i64::from(days > 0 && end_wall.day_micros < start_wall.day_micros)
                - i64::from(days < 0 && end_wall.day_micros > start_wall.day_micros);
            let Some(days) = days.checked_sub(partial) else {
                return overflow_or_null(ansi);
            };
            Ok(Some(match unit {
                AddUnit::Weeks => days / 7,
                _ => days,
            }))
        }
        _ => {
            let (start_wall, end_wall) = diff_walls(start, end, zone);
            let (Some(start_wall), Some(end_wall)) = (start_wall, end_wall) else {
                return Ok(None);
            };
            let span = wall_epoch_day(&end_wall)
                .checked_sub(wall_epoch_day(&start_wall))
                .and_then(|days| days.checked_mul(86_400_000_000))
                .and_then(|days| {
                    days.checked_add(end_wall.day_micros.checked_sub(start_wall.day_micros)?)
                });
            match span.map(|span| span / unit_micros(unit).unwrap_or(1)) {
                Some(value) => Ok(Some(value)),
                None => overflow_or_null(ansi),
            }
        }
    }
}

fn diff_walls(
    start: &TsFamily,
    end: &TsFamily,
    zone: Tz,
) -> (Option<WallFields>, Option<WallFields>) {
    let wall_of = |family: &TsFamily| -> Option<WallFields> {
        match family {
            TsFamily::Instant { micros, .. } => {
                local_datetime_from_micros(*micros, zone).map(|local| WallFields {
                    year: local.year(),
                    month: local.month(),
                    day: local.day(),
                    day_micros: day_micros_of(local),
                })
            }
            TsFamily::Naive { micros } => datetime_from_micros(*micros).map(|naive| WallFields {
                year: naive.year(),
                month: naive.month(),
                day: naive.day(),
                day_micros: day_micros_of(naive),
            }),
        }
    };
    (wall_of(start), wall_of(end))
}

fn wall_epoch_day(wall: &WallFields) -> i64 {
    NaiveDate::from_ymd_opt(wall.year, wall.month, wall.day)
        .map_or(0, |date| i64::from(date.num_days_from_ce()))
}

fn overflow_or_null(ansi: bool) -> Result<Option<i64>> {
    if ansi {
        Err(datetime_overflow())
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx_with(ansi: bool, zone: &str) -> SessionContext {
        let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), ansi);
        let config = crate::session_time_zone::with_session_time_zone(config, zone);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
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
    async fn add_day_keeps_wall_across_dst() {
        let ctx = ctx_with(true, "America/New_York");
        assert_eq!(
            one(
                &ctx,
                "SELECT timestampadd('DAY', 5, TIMESTAMP'2024-03-10 01:30:00') AS v"
            )
            .await,
            vec!["2024-03-15T05:30:00Z".to_string()]
        );
    }

    #[tokio::test]
    async fn add_year_keeps_wall() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT timestampadd('YEAR', 5, TIMESTAMP'2024-03-10 01:30:00') AS v"
            )
            .await,
            vec!["2029-03-10T01:30:00Z".to_string()]
        );
    }

    #[tokio::test]
    async fn add_month_clamps_day() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT timestampadd('MONTH', 1, DATE'2024-01-31') AS v"
            )
            .await,
            vec!["2024-02-29T00:00:00Z".to_string()]
        );
    }

    #[tokio::test]
    async fn diff_day_counts_wall_dates_across_spring_forward() {
        let ctx = ctx_with(true, "America/New_York");
        assert_eq!(
            one(
                &ctx,
                "SELECT timestampdiff('DAY', TIMESTAMP'2024-03-10 01:30:00', TIMESTAMP'2024-03-11 01:30:00') AS v"
            )
            .await,
            vec!["1".to_string()]
        );
    }

    #[tokio::test]
    async fn diff_hour_counts_wall_hours_across_spring_forward() {
        let ctx = ctx_with(true, "America/New_York");
        assert_eq!(
            one(
                &ctx,
                "SELECT timestampdiff('HOUR', TIMESTAMP'2024-03-10 01:30:00', TIMESTAMP'2024-03-11 01:30:00') AS v"
            )
            .await,
            vec!["24".to_string()]
        );
    }

    #[tokio::test]
    async fn diff_week_counts_wall_weeks_across_spring_forward() {
        let ctx = ctx_with(true, "America/New_York");
        assert_eq!(
            one(
                &ctx,
                "SELECT timestampdiff('WEEK', TIMESTAMP'2024-03-04 01:30:00', TIMESTAMP'2024-03-11 01:30:00') AS v"
            )
            .await,
            vec!["1".to_string()]
        );
    }

    #[tokio::test]
    async fn diff_day_counts_wall_dates_across_fall_back() {
        let ctx = ctx_with(true, "America/New_York");
        assert_eq!(
            one(
                &ctx,
                "SELECT timestampdiff('DAY', TIMESTAMP'2024-11-02 12:00:00', TIMESTAMP'2024-11-03 12:00:00') AS v"
            )
            .await,
            vec!["1".to_string()]
        );
    }

    #[tokio::test]
    async fn diff_month_keeps_time_of_day_tiebreak() {
        let ctx = ctx_with(true, "UTC");
        for (end, want) in [
            ("TIMESTAMP'2024-02-29 11:00:00'", "0"),
            ("TIMESTAMP'2024-02-29 13:00:00'", "0"),
        ] {
            assert_eq!(
                one(
                    &ctx,
                    &format!(
                        "SELECT timestampdiff('MONTH', TIMESTAMP'2024-01-31 12:00:00', {end}) AS m"
                    )
                )
                .await,
                vec![want.to_string()]
            );
        }
    }

    #[tokio::test]
    async fn diff_year_keeps_time_of_day_tiebreak() {
        let ctx = ctx_with(true, "UTC");
        for (end, want) in [
            ("TIMESTAMP'2024-03-01 11:59:59'", "0"),
            ("TIMESTAMP'2024-03-01 12:00:00'", "1"),
        ] {
            assert_eq!(
                one(
                    &ctx,
                    &format!(
                        "SELECT timestampdiff('YEAR', TIMESTAMP'2023-03-01 12:00:00', {end}) AS y"
                    )
                )
                .await,
                vec![want.to_string()]
            );
        }
    }

    #[tokio::test]
    async fn diff_quarter_keeps_time_of_day_tiebreak() {
        let ctx = ctx_with(true, "UTC");
        for (unit, end) in [
            ("MONTH", "TIMESTAMP'2024-02-15 09:59:59'"),
            ("QUARTER", "TIMESTAMP'2024-04-15 09:59:59'"),
        ] {
            assert_eq!(
                one(
                    &ctx,
                    &format!(
                        "SELECT timestampdiff('{unit}', TIMESTAMP'2024-01-15 10:00:00', {end}) AS v"
                    )
                )
                .await,
                vec!["0".to_string()]
            );
        }
    }

    #[tokio::test]
    async fn diff_truncates_months() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT timestampdiff('MONTH', TIMESTAMP'2024-01-31 00:00:00', TIMESTAMP'2024-02-29 00:00:00') AS v"
            )
            .await,
            vec!["0".to_string()]
        );
    }

    #[tokio::test]
    async fn bad_unit_names_condition() {
        let ctx = ctx_with(true, "UTC");
        let error = ctx
            .sql("SELECT timestampadd('FORTNIGHT', 3, TIMESTAMP'2024-01-31 10:00:00') AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("[INVALID_PARAMETER_VALUE.DATETIME_UNIT]"),
            "{error}"
        );
    }
}
