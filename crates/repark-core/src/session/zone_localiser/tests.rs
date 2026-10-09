use std::str::FromStr;
use std::sync::{Arc, RwLock};

use arrow::array::timezone::Tz;
use arrow::array::{Array, TimestampMicrosecondArray};
use repark_common::zone_horizon::days_from_civil;
use repark_connect::{ConnectError, ValueRefusal, WallClockLocaliser};

use super::SessionZoneLocaliser;
use super::placement::{localise_at, place, unlocalise, unplace};
use crate::session_time_zone::parse_runtime_session_zone_value;

const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

fn wall(text: &str) -> i64 {
    let (date, time) = text.split_once(' ').expect("a date and a time");
    let mut fields = date.rsplitn(3, '-');
    let day: i64 = fields.next().and_then(|day| day.parse().ok()).expect("day");
    let month: i64 = fields
        .next()
        .and_then(|month| month.parse().ok())
        .expect("month");
    let year: i64 = fields
        .next()
        .and_then(|year| year.parse().ok())
        .expect("year");
    let mut clock = time
        .split(':')
        .map(|part| part.parse::<i64>().expect("clock field"));
    let (hour, minute) = (clock.next().unwrap_or(0), clock.next().unwrap_or(0));
    days_from_civil(year, month, day) * MICROS_PER_DAY
        + hour * MICROS_PER_HOUR
        + minute * 60_000_000
}

fn zone(id: &str) -> Tz {
    Tz::from_str(id).expect("zone")
}

fn offset_hours(text: &str, id: &str) -> Option<i64> {
    let at = wall(text);
    place(at, zone(id), 0)
        .ok()
        .map(|instant| (at - instant) / MICROS_PER_HOUR)
}

fn refusal(result: &repark_connect::Result<i64>) -> Option<ValueRefusal> {
    match result {
        Err(ConnectError::UnrepresentableValue { reason, .. }) => Some(*reason),
        _ => None,
    }
}

fn localiser(raw: &str) -> SessionZoneLocaliser {
    let zone = parse_runtime_session_zone_value(raw).expect("a runtime zone");
    SessionZoneLocaliser {
        zone: Arc::new(RwLock::new(Arc::new(zone))),
    }
}

#[test]
fn a_wall_clock_is_placed_at_the_zone_offset_of_its_date() {
    assert_eq!(
        offset_hours("2024-01-15 12:00", "America/New_York"),
        Some(-5)
    );
    assert_eq!(
        offset_hours("2024-07-15 12:00", "America/New_York"),
        Some(-4)
    );
    let winter = wall("2024-01-15 12:00");
    assert_eq!(
        place(winter, zone("+05:30"), 0).ok(),
        Some(winter - 11 * MICROS_PER_HOUR / 2)
    );
}

#[test]
fn gap_and_overlap_wall_clocks_refuse_naming_the_zone_row() {
    let new_york = zone("America/New_York");
    let gap = refusal(&place(wall("2024-03-10 02:30"), new_york, 3));
    let overlap = refusal(&place(wall("2024-11-03 01:30"), new_york, 4));
    assert_eq!(gap, Some(ValueRefusal::WallClockGap));
    assert_eq!(overlap, Some(ValueRefusal::WallClockOverlap));
    assert_eq!(
        ValueRefusal::WallClockGap.registry_row(),
        "CONNECT-DIV-pg-timestamp-zone"
    );
    let edge = wall("2024-03-10 02:30");
    assert_eq!(place(edge, zone("UTC"), 0).ok(), Some(edge));
}

#[test]
fn a_wall_clock_past_2099_is_placed_by_the_final_rule() {
    assert_eq!(
        place(wall("2100-07-01 12:00"), zone("America/New_York"), 0).ok(),
        Some(4_118_140_800_000_000)
    );
    assert_eq!(
        offset_hours("2100-01-15 12:00", "America/New_York"),
        Some(-5)
    );
    assert_eq!(
        offset_hours("9999-07-01 12:00", "America/New_York"),
        Some(-4)
    );
    let new_york = zone("America/New_York");
    let gap = refusal(&place(wall("2150-03-08 02:30"), new_york, 0));
    let overlap = refusal(&place(wall("2150-11-01 01:30"), new_york, 0));
    assert_eq!(gap, Some(ValueRefusal::WallClockGap));
    assert_eq!(overlap, Some(ValueRefusal::WallClockOverlap));
}

#[test]
fn the_2099_and_2100_sides_of_the_horizon_agree() {
    for (before, after, id, hours) in [
        (
            "2099-07-01 12:00",
            "2100-07-01 12:00",
            "America/New_York",
            -4,
        ),
        (
            "2099-12-31 23:30",
            "2100-01-01 00:30",
            "Australia/Sydney",
            11,
        ),
        (
            "2099-12-31 23:30",
            "2100-01-01 00:30",
            "Pacific/Auckland",
            13,
        ),
    ] {
        assert_eq!(offset_hours(before, id), Some(hours), "{id} {before}");
        assert_eq!(offset_hours(after, id), Some(hours), "{id} {after}");
    }
}

#[test]
fn southern_and_far_future_wall_clocks_keep_their_season() {
    for (text, id, hours) in [
        ("2150-01-15 12:00", "Australia/Sydney", 11),
        ("2150-07-15 12:00", "Australia/Sydney", 10),
        ("2200-01-15 12:00", "Pacific/Auckland", 13),
        ("2200-07-15 12:00", "Pacific/Auckland", 12),
        ("100000-01-15 12:00", "Australia/Sydney", 11),
        ("262142-07-15 12:00", "America/New_York", -4),
    ] {
        assert_eq!(offset_hours(text, id), Some(hours), "{id} {text}");
    }
}

#[test]
fn the_end_of_the_calendar_refuses_as_out_of_range_never_as_a_gap() {
    let late = wall("262142-12-31 23:00");
    for id in ["-12:00", "America/New_York"] {
        let refused = refusal(&place(late, zone(id), 0));
        assert_eq!(refused, Some(ValueRefusal::TimestampPastCalendar), "{id}");
    }
    assert_eq!(
        place(late, zone("+12:00"), 0).ok(),
        Some(late - 12 * MICROS_PER_HOUR)
    );
    let beyond = wall("262143-01-01 00:00");
    let refused = refusal(&place(beyond, zone("UTC"), 0));
    assert_eq!(refused, Some(ValueRefusal::TimestampPastCalendar));
    assert_eq!(
        ValueRefusal::TimestampPastCalendar.registry_row(),
        "CONNECT-DECL-pg-out-of-range"
    );
    let message = ValueRefusal::TimestampPastCalendar.to_string();
    assert!(message.contains("+262142-12-31"), "{message}");
    assert!(!message.contains("daylight"), "{message}");
}

#[test]
fn an_instant_unplaces_to_the_wall_clock_of_its_zone() {
    let new_york = zone("America/New_York");
    for (text, id) in [
        ("2024-01-15 12:00", "America/New_York"),
        ("2024-07-15 12:00", "America/New_York"),
        ("2024-07-15 12:00", "+05:30"),
        ("2100-07-01 12:00", "America/New_York"),
        ("2150-01-15 12:00", "Australia/Sydney"),
    ] {
        let placed = place(wall(text), zone(id), 0).expect("a placeable wall");
        assert_eq!(
            unplace(placed, zone(id), 0).ok(),
            Some(wall(text)),
            "{id} {text}"
        );
    }
    let before = place(wall("2024-11-03 00:30"), new_york, 0).expect("before the fold");
    let fold_first = before + MICROS_PER_HOUR;
    let fold_second = before + 2 * MICROS_PER_HOUR;
    assert_eq!(
        unplace(fold_first, new_york, 0).ok(),
        Some(wall("2024-11-03 01:30"))
    );
    assert_eq!(
        unplace(fold_first, new_york, 0).ok(),
        unplace(fold_second, new_york, 0).ok()
    );
}

#[test]
fn unlocalise_keeps_nulls_and_returns_bare_walls() {
    let placed = TimestampMicrosecondArray::from(vec![
        Some(place(wall("2024-07-15 12:00"), zone("America/New_York"), 0).expect("placed")),
        None,
    ]);
    let walls = unlocalise("America/New_York", &placed).expect("walls");
    assert_eq!(walls.value(0), wall("2024-07-15 12:00"));
    assert!(walls.is_null(1));
    assert_eq!(
        walls.data_type(),
        &arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
    );
}

#[test]
fn an_instant_past_the_calendar_refuses_as_out_of_range() {
    let refused = refusal(&unplace(i64::MAX, zone("UTC"), 0));
    assert_eq!(refused, Some(ValueRefusal::TimestampPastCalendar));
    let late = place(wall("262142-07-15 12:00"), zone("America/New_York"), 0).expect("placed");
    assert_eq!(
        unplace(late, zone("America/New_York"), 0).ok(),
        Some(wall("262142-07-15 12:00"))
    );
}

#[test]
fn localise_at_matches_the_shared_localiser_and_keeps_gap_refusals() {
    let at = wall("2024-07-15 12:00");
    let walls = TimestampMicrosecondArray::from(vec![Some(at), None]);
    let shared = localiser("America/New_York")
        .localise(&walls)
        .expect("shared");
    let direct = localise_at("America/New_York", &walls).expect("direct");
    assert_eq!(direct.value(0), shared.value(0));
    assert!(direct.is_null(1));
    let gap = TimestampMicrosecondArray::from(vec![wall("2024-03-10 02:30")]);
    let refused = localise_at("America/New_York", &gap).expect_err("a gap refuses");
    assert!(matches!(
        refused,
        ConnectError::UnrepresentableValue {
            reason: ValueRefusal::WallClockGap,
            ..
        }
    ));
}

#[test]
fn java_form_session_zones_place_at_their_canonical_offset() {
    let at = wall("2024-07-15 12:00");
    let walls = TimestampMicrosecondArray::from(vec![Some(at), None]);
    for (raw, label, minutes) in [
        ("Z", "UTC", 0),
        ("UT", "UTC", 0),
        ("GMT+8", "+08:00", 480),
        ("UTC+05:30", "+05:30", 330),
        ("-8", "-08:00", -480),
        ("+3", "+03:00", 180),
        ("America/New_York", "America/New_York", -240),
    ] {
        let localiser = localiser(raw);
        assert_eq!(&*localiser.zone_label(), label, "{raw}");
        let placed = localiser.localise(&walls).expect(raw);
        assert_eq!(placed.value(0), at - minutes * 60_000_000, "{raw}");
        assert!(placed.is_null(1), "{raw}");
    }
}
