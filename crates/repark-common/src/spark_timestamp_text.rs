use std::fmt::Display;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike};

use crate::zone_horizon::wall_at_instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[must_use]
pub fn ticks_to_micros(ticks: i64, unit: TickUnit) -> Option<i64> {
    match unit {
        TickUnit::Second => ticks.checked_mul(1_000_000),
        TickUnit::Millisecond => ticks.checked_mul(1_000),
        TickUnit::Microsecond => Some(ticks),
        TickUnit::Nanosecond => Some(ticks.div_euclid(1_000)),
    }
}

#[must_use]
pub fn wall_clock_from_ticks<Z: TimeZone + Display>(
    ticks: i64,
    unit: TickUnit,
    zoned: bool,
    session_zone: &Z,
) -> Option<NaiveDateTime> {
    let utc = match unit {
        TickUnit::Nanosecond => DateTime::from_timestamp_nanos(ticks),
        _ => DateTime::from_timestamp_micros(ticks_to_micros(ticks, unit)?)?,
    };
    if zoned {
        wall_at_instant(session_zone, &utc.naive_utc())
    } else {
        Some(utc.naive_utc())
    }
}

#[must_use]
pub fn format_spark_timestamp_string(wall: NaiveDateTime) -> String {
    let date = format_iso_local_date(wall.date());
    let time = format!(
        "{:02}:{:02}:{:02}",
        wall.hour(),
        wall.minute(),
        wall.second()
    );
    let nanos = wall.nanosecond();
    if nanos == 0 {
        format!("{date} {time}")
    } else {
        let mut fraction = format!("{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        format!("{date} {time}.{fraction}")
    }
}

#[must_use]
pub fn format_iso_local_date(date: NaiveDate) -> String {
    let year = date.year();
    let month = date.month();
    let day = date.day();
    if (0..=9999).contains(&year) {
        format!("{year:04}-{month:02}-{day:02}")
    } else if year > 9999 {
        format!("+{year}-{month:02}-{day:02}")
    } else {
        format!("-{abs:04}-{month:02}-{day:02}", abs = year.unsigned_abs())
    }
}
