use std::sync::Arc;

use arrow::array::temporal_conversions::{
    timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_s_to_datetime,
    timestamp_us_to_datetime,
};
use arrow::array::timezone::Tz;
use arrow::array::{ArrayRef, AsArray, Int32Array};
use arrow::compute::DatePart;
use arrow::datatypes::{
    ArrowTimestampType, DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use chrono::{
    DateTime, Datelike, FixedOffset, MappedLocalTime, NaiveDateTime, TimeDelta, Timelike,
};
use datafusion::error::{DataFusionError, Result};
use repark_common::zone_horizon::{self, offsets_at_wall, wall_at_instant, zoned_at_instant};

pub(crate) fn datetime_from_micros(micros: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_micros(micros).map(|instant| instant.naive_utc())
}

pub(crate) fn localize_wall_micros_in_zone(wall_micros: i64, zone: Tz) -> Option<i64> {
    datetime_from_micros(wall_micros)
        .and_then(|naive| micros_from_local_datetime(naive, zone, None))
}

pub(crate) fn local_datetime_from_micros(micros: i64, zone: Tz) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_micros(micros)
        .and_then(|instant| wall_at_instant(&zone, &instant.naive_utc()))
}

pub(crate) fn offset_at_instant(micros: i64, zone: Tz) -> Option<FixedOffset> {
    DateTime::from_timestamp_micros(micros)
        .map(|instant| zone_horizon::offset_at_instant(&zone, &instant.naive_utc()))
}

const GAP_LOOKBACK_HOURS: i64 = 26;

fn offset_before_gap(local: NaiveDateTime, zone: Tz) -> Option<FixedOffset> {
    let probe = local.checked_sub_signed(TimeDelta::try_hours(GAP_LOOKBACK_HOURS)?)?;
    Some(zone_horizon::offset_at_instant(&zone, &probe))
}

pub(crate) fn micros_from_local_datetime(
    local: NaiveDateTime,
    zone: Tz,
    preferred: Option<FixedOffset>,
) -> Option<i64> {
    let offset = match offsets_at_wall(&zone, &local) {
        MappedLocalTime::Single(single) => single,
        MappedLocalTime::Ambiguous(earliest, latest) => match preferred {
            Some(source) if source == earliest || source == latest => source,
            _ => earliest,
        },
        MappedLocalTime::None => offset_before_gap(local, zone)?,
    };
    let utc =
        local.checked_sub_signed(TimeDelta::try_seconds(i64::from(offset.local_minus_utc()))?)?;
    Some(utc.and_utc().timestamp_micros())
}

fn wall_parts<T: ArrowTimestampType>(
    instants: &ArrayRef,
    zone: Tz,
    decode: fn(i64) -> Option<NaiveDateTime>,
    read: impl Fn(&DateTime<FixedOffset>) -> i32,
) -> ArrayRef {
    let parts: Int32Array = instants
        .as_primitive::<T>()
        .unary_opt(|ticks| decode(ticks).map(|utc| read(&zoned_at_instant(&zone, &utc))));
    Arc::new(parts)
}

fn wall_parts_of(
    instants: &ArrayRef,
    zone: Tz,
    read: impl Fn(&DateTime<FixedOffset>) -> i32,
) -> Result<ArrayRef> {
    match instants.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => Ok(wall_parts::<TimestampSecondType>(
            instants,
            zone,
            timestamp_s_to_datetime,
            read,
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(
            wall_parts::<TimestampMillisecondType>(instants, zone, timestamp_ms_to_datetime, read),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(
            wall_parts::<TimestampMicrosecondType>(instants, zone, timestamp_us_to_datetime, read),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(wall_parts::<TimestampNanosecondType>(
            instants,
            zone,
            timestamp_ns_to_datetime,
            read,
        )),
        other => Err(DataFusionError::Internal(format!(
            "a session-zone calendar field needs a timestamp, got {other}"
        ))),
    }
}

fn every_tick_is_tabulated<T: ArrowTimestampType>(instants: &ArrayRef, per_second: i64) -> bool {
    let tabulated = zone_horizon::tabulated_utc_seconds();
    let first = tabulated.start.saturating_mul(per_second);
    let width = tabulated
        .end
        .saturating_mul(per_second)
        .wrapping_sub(first)
        .cast_unsigned();
    !instants
        .as_primitive::<T>()
        .values()
        .iter()
        .fold(false, |outside, ticks| {
            outside | (ticks.wrapping_sub(first).cast_unsigned() >= width)
        })
}

pub(crate) fn within_the_tables(instants: &ArrayRef) -> bool {
    match instants.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            every_tick_is_tabulated::<TimestampSecondType>(instants, 1)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            every_tick_is_tabulated::<TimestampMillisecondType>(instants, 1_000)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            every_tick_is_tabulated::<TimestampMicrosecondType>(instants, 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            every_tick_is_tabulated::<TimestampNanosecondType>(instants, 1_000_000_000)
        }
        _ => true,
    }
}

pub(crate) fn instant_part(
    instants: &ArrayRef,
    zone: Tz,
    part: DatePart,
    shift: i32,
) -> Result<ArrayRef> {
    let signed = |field: u32| field.cast_signed() + shift;
    match part {
        DatePart::Year => wall_parts_of(instants, zone, |wall| wall.year() + shift),
        DatePart::YearISO => wall_parts_of(instants, zone, |wall| wall.iso_week().year() + shift),
        DatePart::Quarter => wall_parts_of(instants, zone, |wall| signed(wall.month0() / 3 + 1)),
        DatePart::Month => wall_parts_of(instants, zone, |wall| signed(wall.month())),
        DatePart::WeekISO => wall_parts_of(instants, zone, |wall| signed(wall.iso_week().week())),
        DatePart::Day => wall_parts_of(instants, zone, |wall| signed(wall.day())),
        DatePart::DayOfYear => wall_parts_of(instants, zone, |wall| signed(wall.ordinal())),
        DatePart::DayOfWeekSunday0 => wall_parts_of(instants, zone, |wall| {
            signed(wall.weekday().num_days_from_sunday())
        }),
        DatePart::DayOfWeekMonday0 => wall_parts_of(instants, zone, |wall| {
            signed(wall.weekday().num_days_from_monday())
        }),
        DatePart::Hour => wall_parts_of(instants, zone, |wall| signed(wall.hour())),
        DatePart::Minute => wall_parts_of(instants, zone, |wall| signed(wall.minute())),
        DatePart::Second => wall_parts_of(instants, zone, |wall| signed(wall.second())),
        other => Err(DataFusionError::Internal(format!(
            "the calendar field {other} has no session-zone reader"
        ))),
    }
}
