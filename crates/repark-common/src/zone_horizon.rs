use std::fmt::Display;
use std::ops::Range;
use std::sync::Mutex;

use chrono::{
    DateTime, Datelike, FixedOffset, MappedLocalTime, NaiveDate, NaiveDateTime, Offset, TimeDelta,
    TimeZone,
};

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

#[inline]
fn is_far_past(moment: &NaiveDateTime) -> bool {
    i64::from(moment.year()) < FAR_PAST_PROXY_BASE
}

static HAS_FINAL_RULE_CACHE: Mutex<Vec<(String, bool)>> = Mutex::new(Vec::new());

struct NameBuf {
    bytes: [u8; 64],
    len: usize,
}

impl std::fmt::Write for NameBuf {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        if text.len() > self.bytes.len() - self.len {
            return Err(std::fmt::Error);
        }
        self.bytes[self.len..self.len + text.len()].copy_from_slice(text.as_bytes());
        self.len += text.len();
        Ok(())
    }
}

fn zone_name<Z: Display>(zone: &Z) -> Option<([u8; 64], usize)> {
    let mut buf = NameBuf {
        bytes: [0; 64],
        len: 0,
    };
    std::fmt::write(&mut buf, format_args!("{zone}")).ok()?;
    Some((buf.bytes, buf.len))
}

fn transitions_in_2099<Z: TimeZone>(zone: &Z) -> bool {
    let Some(start) =
        NaiveDate::from_ymd_opt(2099, 1, 1).and_then(|date| date.and_hms_opt(0, 0, 0))
    else {
        return true;
    };
    let first = zone.offset_from_utc_datetime(&start).fix();
    let Some(before) =
        NaiveDate::from_ymd_opt(2098, 12, 31).and_then(|date| date.and_hms_opt(23, 0, 0))
    else {
        return true;
    };
    if zone.offset_from_utc_datetime(&before).fix() != first {
        return true;
    }
    let Some(step) = TimeDelta::try_hours(1) else {
        return true;
    };
    let mut cursor = start;
    while let Some(next) = cursor.checked_add_signed(step) {
        if next.year() != 2099 {
            break;
        }
        cursor = next;
        if zone.offset_from_utc_datetime(&cursor).fix() != first {
            return true;
        }
    }
    let Some(after) =
        NaiveDate::from_ymd_opt(2100, 1, 1).and_then(|date| date.and_hms_opt(0, 0, 0))
    else {
        return true;
    };
    zone.offset_from_utc_datetime(&after).fix() != first
}

fn has_final_rule<Z: TimeZone + Display>(zone: &Z) -> bool {
    let Some((bytes, len)) = zone_name(zone) else {
        return transitions_in_2099(zone);
    };
    let Ok(name) = std::str::from_utf8(&bytes[..len]) else {
        return transitions_in_2099(zone);
    };
    let mut cache = HAS_FINAL_RULE_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(found) = cache.iter().find(|(cached, _)| cached == name) {
        return found.1;
    }
    let rule = transitions_in_2099(zone);
    cache.push((name.to_owned(), rule));
    rule
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
pub fn offset_at_instant<Z: TimeZone + Display>(zone: &Z, utc: &NaiveDateTime) -> FixedOffset {
    if is_tabulated(utc) || (!is_far_past(utc) && !has_final_rule(zone)) {
        zone.offset_from_utc_datetime(utc).fix()
    } else {
        offset_outside_the_tables(zone, utc)
    }
}

#[inline]
#[must_use]
pub fn zoned_at_instant<Z: TimeZone + Display>(
    zone: &Z,
    utc: &NaiveDateTime,
) -> DateTime<FixedOffset> {
    DateTime::from_naive_utc_and_offset(*utc, offset_at_instant(zone, utc))
}

#[inline]
#[must_use]
pub fn wall_at_instant<Z: TimeZone + Display>(
    zone: &Z,
    utc: &NaiveDateTime,
) -> Option<NaiveDateTime> {
    if is_tabulated(utc) || (!is_far_past(utc) && !has_final_rule(zone)) {
        Some(zone.from_utc_datetime(utc).naive_local())
    } else {
        utc.checked_add_offset(offset_outside_the_tables(zone, utc))
    }
}

#[inline]
#[must_use]
pub fn offsets_at_wall<Z: TimeZone + Display>(
    zone: &Z,
    wall: &NaiveDateTime,
) -> MappedLocalTime<FixedOffset> {
    if is_tabulated(wall) || (!is_far_past(wall) && !has_final_rule(zone)) {
        zone.offset_from_local_datetime(wall)
            .map(|offset| offset.fix())
    } else {
        offsets_outside_the_tables(zone, wall)
    }
}

#[inline]
#[must_use]
pub fn wall_to_unix_seconds<Z: TimeZone + Display>(zone: &Z, wall: &NaiveDateTime) -> Option<i64> {
    if i64::from(wall.year()) <= LAST_TABULATED_YEAR || !has_final_rule(zone) {
        match zone.from_local_datetime(wall) {
            MappedLocalTime::Single(instant) | MappedLocalTime::Ambiguous(instant, _) => {
                Some(instant.timestamp())
            }
            MappedLocalTime::None => None,
        }
    } else {
        match offsets_outside_the_tables(zone, wall) {
            MappedLocalTime::Single(offset) | MappedLocalTime::Ambiguous(offset, _) => wall
                .checked_sub_offset(offset)
                .map(|instant| instant.and_utc().timestamp()),
            MappedLocalTime::None => None,
        }
    }
}

#[inline]
#[must_use]
pub fn wall_to_micros_earlier<Z: TimeZone + Display>(
    zone: &Z,
    wall: &NaiveDateTime,
) -> Option<i64> {
    if i64::from(wall.year()) <= LAST_TABULATED_YEAR || !has_final_rule(zone) {
        match zone.from_local_datetime(wall) {
            MappedLocalTime::Single(instant) | MappedLocalTime::Ambiguous(instant, _) => {
                Some(instant.timestamp_micros())
            }
            MappedLocalTime::None => None,
        }
    } else {
        match offsets_outside_the_tables(zone, wall) {
            MappedLocalTime::Single(offset) | MappedLocalTime::Ambiguous(offset, _) => wall
                .checked_sub_offset(offset)
                .map(|instant| instant.and_utc().timestamp_micros()),
            MappedLocalTime::None => None,
        }
    }
}

#[inline]
#[must_use]
pub fn wall_to_millis_earlier<Z: TimeZone + Display>(
    zone: &Z,
    wall: &NaiveDateTime,
) -> Option<i64> {
    if i64::from(wall.year()) <= LAST_TABULATED_YEAR || !has_final_rule(zone) {
        match zone.from_local_datetime(wall) {
            MappedLocalTime::Single(current) => Some(current.timestamp_millis()),
            MappedLocalTime::Ambiguous(first, second) => Some(first.min(second).timestamp_millis()),
            MappedLocalTime::None => None,
        }
    } else {
        let offset = match offsets_outside_the_tables(zone, wall) {
            MappedLocalTime::Single(current) => current,
            MappedLocalTime::Ambiguous(first, second) => {
                if first.local_minus_utc() >= second.local_minus_utc() {
                    first
                } else {
                    second
                }
            }
            MappedLocalTime::None => return None,
        };
        wall.checked_sub_offset(offset)
            .map(|instant| instant.and_utc().timestamp_millis())
    }
}

#[inline]
#[must_use]
pub fn micros_to_wall_and_offset<Z: TimeZone + Display>(
    zone: &Z,
    micros: i64,
) -> Option<(NaiveDateTime, FixedOffset)> {
    let instant = DateTime::from_timestamp_micros(micros)?;
    let utc = instant.naive_utc();
    if i64::from(utc.year()) <= LAST_TABULATED_YEAR || !has_final_rule(zone) {
        let zoned = instant.with_timezone(zone);
        Some((zoned.naive_local(), zoned.offset().fix()))
    } else {
        let offset = offset_outside_the_tables(zone, &utc);
        utc.checked_add_offset(offset).map(|wall| (wall, offset))
    }
}

#[cfg(test)]
mod tests;
