use chrono::{DateTime, Datelike, NaiveDate, Utc};

use super::grammar::ParsedTimestamp;
use super::zone::SparkZone;
use crate::datetime::micros_from_local_datetime;

pub(crate) const LAST_TABULATED_YEAR: i64 = 2099;
const CALENDAR_CYCLE_YEARS: i64 = 28;
const FAR_PAST_PROXY_BASE: i64 = 1200;
const GREGORIAN_CYCLE_YEARS: i64 = 400;
const DAYS_PER_GREGORIAN_CYCLE: i64 = 146_097;
const DAYS_FROM_YEAR_ZERO_TO_EPOCH: i64 = 719_468;
const DAYS_PER_WEEK: i64 = 7;
const MICROS_PER_SECOND: i128 = 1_000_000;
const MICROS_PER_DAY: i128 = 86_400 * MICROS_PER_SECOND;
const MAX_HOUR: i64 = 23;
const MAX_MINUTE_OR_SECOND: i64 = 59;
const MONTHS_PER_YEAR: i64 = 12;

fn is_leap_year(year: i64) -> bool {
    year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let shifted_year = if month <= 2 { year - 1 } else { year };
    let era = shifted_year.div_euclid(GREGORIAN_CYCLE_YEARS);
    let year_of_era = shifted_year.rem_euclid(GREGORIAN_CYCLE_YEARS);
    let month_from_march = (month + 9) % MONTHS_PER_YEAR;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * DAYS_PER_GREGORIAN_CYCLE + day_of_era - DAYS_FROM_YEAR_ZERO_TO_EPOCH
}

fn calendar_kind(year: i64) -> (bool, i64) {
    (
        is_leap_year(year),
        days_from_civil(year, 1, 1).rem_euclid(DAYS_PER_WEEK),
    )
}

#[must_use]
pub(crate) fn proxy_year(year: i64) -> i64 {
    if year > LAST_TABULATED_YEAR {
        let wanted = calendar_kind(year);
        return (LAST_TABULATED_YEAR - CALENDAR_CYCLE_YEARS + 1..=LAST_TABULATED_YEAR)
            .rev()
            .find(|candidate| calendar_kind(*candidate) == wanted)
            .unwrap_or(LAST_TABULATED_YEAR);
    }
    if year < FAR_PAST_PROXY_BASE {
        return FAR_PAST_PROXY_BASE + year.rem_euclid(GREGORIAN_CYCLE_YEARS);
    }
    year
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
        SparkZone::Named(named) => now.with_timezone(&named).date_naive(),
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
