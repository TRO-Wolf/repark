use std::ops::Range;

use chrono::{DateTime, Datelike, FixedOffset, MappedLocalTime, NaiveDateTime, Offset, TimeZone};

pub const LAST_TABULATED_YEAR: i64 = 2099;
const CALENDAR_CYCLE_YEARS: i64 = 28;
const FAR_PAST_PROXY_BASE: i64 = 1200;
const GREGORIAN_CYCLE_YEARS: i64 = 400;
const DAYS_PER_GREGORIAN_CYCLE: i64 = 146_097;
const DAYS_FROM_YEAR_ZERO_TO_EPOCH: i64 = 719_468;
const DAYS_PER_WEEK: i64 = 7;
const MONTHS_PER_YEAR: i64 = 12;
const SECONDS_PER_DAY: i64 = 86_400;

#[must_use]
pub fn is_leap_year(year: i64) -> bool {
    year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0)
}

#[must_use]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
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
pub fn proxy_year(year: i64) -> i64 {
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

#[must_use]
pub fn tabulated_utc_seconds() -> Range<i64> {
    days_from_civil(FAR_PAST_PROXY_BASE, 1, 1) * SECONDS_PER_DAY
        ..days_from_civil(LAST_TABULATED_YEAR + 1, 1, 1) * SECONDS_PER_DAY
}

#[inline]
fn is_tabulated(moment: &NaiveDateTime) -> bool {
    (FAR_PAST_PROXY_BASE..=LAST_TABULATED_YEAR).contains(&i64::from(moment.year()))
}

fn in_proxy_year(moment: &NaiveDateTime) -> NaiveDateTime {
    i32::try_from(proxy_year(i64::from(moment.year())))
        .ok()
        .and_then(|proxy| moment.with_year(proxy))
        .unwrap_or(*moment)
}

#[cold]
#[inline(never)]
fn offset_outside_the_tables<Z: TimeZone>(zone: &Z, utc: &NaiveDateTime) -> FixedOffset {
    zone.offset_from_utc_datetime(&in_proxy_year(utc)).fix()
}

#[cold]
#[inline(never)]
fn offsets_outside_the_tables<Z: TimeZone>(
    zone: &Z,
    wall: &NaiveDateTime,
) -> MappedLocalTime<FixedOffset> {
    zone.offset_from_local_datetime(&in_proxy_year(wall))
        .map(|offset| offset.fix())
}

#[inline]
#[must_use]
pub fn offset_at_instant<Z: TimeZone>(zone: &Z, utc: &NaiveDateTime) -> FixedOffset {
    if is_tabulated(utc) {
        zone.offset_from_utc_datetime(utc).fix()
    } else {
        offset_outside_the_tables(zone, utc)
    }
}

#[inline]
#[must_use]
pub fn zoned_at_instant<Z: TimeZone>(zone: &Z, utc: &NaiveDateTime) -> DateTime<FixedOffset> {
    DateTime::from_naive_utc_and_offset(*utc, offset_at_instant(zone, utc))
}

#[inline]
#[must_use]
pub fn wall_at_instant<Z: TimeZone>(zone: &Z, utc: &NaiveDateTime) -> Option<NaiveDateTime> {
    if is_tabulated(utc) {
        Some(zone.from_utc_datetime(utc).naive_local())
    } else {
        utc.checked_add_offset(offset_outside_the_tables(zone, utc))
    }
}

#[inline]
#[must_use]
pub fn offsets_at_wall<Z: TimeZone>(
    zone: &Z,
    wall: &NaiveDateTime,
) -> MappedLocalTime<FixedOffset> {
    if is_tabulated(wall) {
        zone.offset_from_local_datetime(wall)
            .map(|offset| offset.fix())
    } else {
        offsets_outside_the_tables(zone, wall)
    }
}

#[cfg(test)]
mod tests;
