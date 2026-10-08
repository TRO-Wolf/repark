use chrono::{DateTime, Datelike, NaiveDate, Utc};

use super::grammar::ParsedTimestamp;
use super::zone::SparkZone;
use crate::datetime::micros_from_local_datetime;
use repark_common::zone_horizon::{days_from_civil, is_leap_year, offset_at_instant, proxy_year};

const MICROS_PER_SECOND: i128 = 1_000_000;
const MICROS_PER_DAY: i128 = 86_400 * MICROS_PER_SECOND;
const MAX_HOUR: i64 = 23;
const MAX_MINUTE_OR_SECOND: i64 = 59;
const MONTHS_PER_YEAR: i64 = 12;

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn wall_micros(date: (i64, i64, i64), parsed: &ParsedTimestamp<'_>) -> i128 {
    let (year, month, day) = date;
    let seconds = parsed.hour * 3_600 + parsed.minute * 60 + parsed.second;
    i128::from(days_from_civil(year, month, day)) * MICROS_PER_DAY
        + i128::from(seconds) * MICROS_PER_SECOND
        + i128::from(parsed.micros)
}

fn offset_micros_at(
    date: (i64, i64, i64),
    parsed: &ParsedTimestamp<'_>,
    zone: SparkZone,
) -> Option<i128> {
    match zone {
        SparkZone::Offset(offset) => Some(i128::from(offset.local_minus_utc()) * MICROS_PER_SECOND),
        SparkZone::Named(named) => {
            let (year, month, day) = date;
            let proxy = NaiveDate::from_ymd_opt(
                i32::try_from(proxy_year(year)).ok()?,
                u32::try_from(month).ok()?,
                u32::try_from(day).ok()?,
            )?
            .and_hms_micro_opt(
                u32::try_from(parsed.hour).ok()?,
                u32::try_from(parsed.minute).ok()?,
                u32::try_from(parsed.second).ok()?,
                u32::try_from(parsed.micros).ok()?,
            )?;
            let instant = micros_from_local_datetime(proxy, named, None)?;
            Some(i128::from(proxy.and_utc().timestamp_micros()) - i128::from(instant))
        }
    }
}

fn today_in(zone: SparkZone, now: DateTime<Utc>) -> (i64, i64, i64) {
    let date = match zone {
        SparkZone::Named(named) => now
            .with_timezone(&offset_at_instant(&named, &now.naive_utc()))
            .date_naive(),
        SparkZone::Offset(offset) => now.with_timezone(&offset).date_naive(),
    };
    (
        i64::from(date.year()),
        i64::from(date.month()),
        i64::from(date.day()),
    )
}

fn is_valid_time(parsed: &ParsedTimestamp<'_>) -> bool {
    (0..=MAX_HOUR).contains(&parsed.hour)
        && (0..=MAX_MINUTE_OR_SECOND).contains(&parsed.minute)
        && (0..=MAX_MINUTE_OR_SECOND).contains(&parsed.second)
}

fn is_valid_date(date: (i64, i64, i64)) -> bool {
    let (year, month, day) = date;
    (1..=MONTHS_PER_YEAR).contains(&month) && (1..=days_in_month(year, month)).contains(&day)
}

#[must_use]
pub(crate) fn instant_micros(
    parsed: &ParsedTimestamp<'_>,
    zone: SparkZone,
    now: DateTime<Utc>,
) -> Option<i64> {
    if !is_valid_time(parsed) {
        return None;
    }
    let date = if parsed.just_time {
        today_in(zone, now)
    } else {
        (parsed.year, parsed.month, parsed.day)
    };
    if !is_valid_date(date) {
        return None;
    }
    let offset = offset_micros_at(date, parsed, zone)?;
    i64::try_from(wall_micros(date, parsed) - offset).ok()
}
