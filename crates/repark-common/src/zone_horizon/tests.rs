use chrono::{
    DateTime, Datelike, FixedOffset, MappedLocalTime, NaiveDate, NaiveDateTime, NaiveTime,
    TimeDelta, TimeZone,
};

use super::{
    LAST_TABULATED_YEAR, offset_at_instant, offsets_at_wall, proxy_year, tabulated_utc_seconds,
    wall_at_instant,
};

const STANDARD_SECONDS: i32 = -5 * 3_600;
const SUMMER_SECONDS: i32 = -4 * 3_600;
const UNTABULATED_PAST_SECONDS: i32 = -17_762;
const FIRST_TABULATED_YEAR: i32 = 1_200;

#[derive(Debug, Clone, Copy)]
struct TabulatedToHorizon;

fn fixed(seconds: i32) -> FixedOffset {
    FixedOffset::east_opt(seconds).expect("an offset inside a day")
}

fn second_sunday_of_march(year: i32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, 3, 1).expect("the first of March");
    let to_sunday = (7 - first.weekday().num_days_from_sunday()) % 7;
    first + TimeDelta::days(i64::from(to_sunday) + 7)
}

fn first_sunday_of_november(year: i32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, 11, 1).expect("the first of November");
    let to_sunday = (7 - first.weekday().num_days_from_sunday()) % 7;
    first + TimeDelta::days(i64::from(to_sunday))
}

impl TabulatedToHorizon {
    fn seconds_at(utc: &NaiveDateTime) -> i32 {
        let year = utc.year();
        if year < FIRST_TABULATED_YEAR {
            return UNTABULATED_PAST_SECONDS;
        }
        if i64::from(year) > LAST_TABULATED_YEAR {
            return STANDARD_SECONDS;
        }
        let start = second_sunday_of_march(year)
            .and_hms_opt(7, 0, 0)
            .expect("the spring change");
        let end = first_sunday_of_november(year)
            .and_hms_opt(6, 0, 0)
            .expect("the autumn change");
        if (start..end).contains(utc) {
            SUMMER_SECONDS
        } else {
            STANDARD_SECONDS
        }
    }
}

impl TimeZone for TabulatedToHorizon {
    type Offset = FixedOffset;

    fn from_offset(_offset: &FixedOffset) -> Self {
        Self
    }

    fn offset_from_local_date(&self, local: &NaiveDate) -> MappedLocalTime<FixedOffset> {
        self.offset_from_local_datetime(&local.and_time(NaiveTime::MIN))
    }

    fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> MappedLocalTime<FixedOffset> {
        let holds = |seconds: i32| {
            local
                .checked_sub_offset(fixed(seconds))
                .is_some_and(|utc| Self::seconds_at(&utc) == seconds)
        };
        let candidates = [SUMMER_SECONDS, STANDARD_SECONDS, UNTABULATED_PAST_SECONDS];
        let mut found = candidates.into_iter().filter(|seconds| holds(*seconds));
        match (found.next(), found.next()) {
            (Some(earliest), Some(latest)) => {
                MappedLocalTime::Ambiguous(fixed(earliest), fixed(latest))
            }
            (Some(only), None) => MappedLocalTime::Single(fixed(only)),
            _ => MappedLocalTime::None,
        }
    }

    fn offset_from_utc_date(&self, utc: &NaiveDate) -> FixedOffset {
        self.offset_from_utc_datetime(&utc.and_time(NaiveTime::MIN))
    }

    fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> FixedOffset {
        fixed(Self::seconds_at(utc))
    }
}

fn utc(year: i32, month: u32, day: u32, hour: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|date| date.and_hms_opt(hour, 0, 0))
        .expect("a calendar moment")
}

fn hours(offset: FixedOffset) -> i32 {
    offset.local_minus_utc() / 3_600
}

#[test]
fn an_instant_inside_the_tables_reads_the_zone_itself() {
    for moment in [
        utc(1_200, 7, 1, 12),
        utc(1_970, 1, 1, 0),
        utc(2_024, 7, 1, 12),
        utc(2_024, 12, 1, 12),
        utc(2_099, 7, 1, 16),
        utc(2_099, 12, 31, 23),
    ] {
        assert_eq!(
            offset_at_instant(&TabulatedToHorizon, &moment),
            TabulatedToHorizon.offset_from_utc_datetime(&moment),
            "{moment}"
        );
    }
}

#[test]
fn the_tabulated_seconds_are_the_years_the_helpers_leave_alone() {
    let seconds = |moment: NaiveDateTime| moment.and_utc().timestamp();
    let tabulated = tabulated_utc_seconds();
    assert_eq!(tabulated.start, seconds(utc(1_200, 1, 1, 0)));
    assert_eq!(tabulated.end, seconds(utc(2_100, 1, 1, 0)));
    assert!(tabulated.contains(&seconds(utc(2_024, 7, 1, 12))));
    assert!(tabulated.contains(&(seconds(utc(2_100, 1, 1, 0)) - 1)));
    assert!(!tabulated.contains(&seconds(utc(2_100, 7, 1, 0))));
    assert!(!tabulated.contains(&(seconds(utc(1_200, 1, 1, 0)) - 1)));
}

#[test]
fn an_instant_past_the_horizon_reads_its_proxy_year() {
    assert_eq!(
        hours(TabulatedToHorizon.offset_from_utc_datetime(&utc(2_100, 7, 1, 16))),
        -5
    );
    for (moment, expected) in [
        (utc(2_100, 1, 1, 0), -5),
        (utc(2_100, 7, 1, 16), -4),
        (utc(2_100, 12, 1, 16), -5),
        (utc(2_104, 2, 29, 16), -5),
        (utc(2_104, 7, 1, 16), -4),
        (utc(2_500, 7, 1, 16), -4),
        (utc(9_999, 7, 1, 16), -4),
        (utc(262_000, 7, 1, 16), -4),
    ] {
        assert_eq!(
            hours(offset_at_instant(&TabulatedToHorizon, &moment)),
            expected,
            "{moment}"
        );
    }
}

#[test]
fn the_change_past_the_horizon_falls_on_the_rule_day_of_the_real_year() {
    for year in [2_100, 2_101, 2_104, 2_200, 2_500, 9_999] {
        let change = second_sunday_of_march(year)
            .and_hms_opt(7, 0, 0)
            .expect("the spring change");
        let before = change - TimeDelta::seconds(1);
        assert_eq!(
            hours(offset_at_instant(&TabulatedToHorizon, &before)),
            -5,
            "{year}"
        );
        assert_eq!(
            hours(offset_at_instant(&TabulatedToHorizon, &change)),
            -4,
            "{year}"
        );
        assert_eq!(
            wall_at_instant(&TabulatedToHorizon, &change),
            second_sunday_of_march(year).and_hms_opt(3, 0, 0),
            "{year}"
        );
    }
}

#[test]
fn an_instant_before_the_tables_reads_its_proxy_year() {
    let moment = utc(800, 7, 1, 16);
    assert_eq!(proxy_year(800), 1_600 - 400);
    assert_eq!(
        TabulatedToHorizon
            .offset_from_utc_datetime(&moment)
            .local_minus_utc(),
        UNTABULATED_PAST_SECONDS
    );
    assert_eq!(hours(offset_at_instant(&TabulatedToHorizon, &moment)), -4);
    assert_eq!(
        offsets_at_wall(&TabulatedToHorizon, &utc(800, 7, 1, 12)).single(),
        Some(fixed(SUMMER_SECONDS))
    );
}

#[test]
fn a_wall_clock_past_the_horizon_keeps_its_gap_and_its_overlap() {
    let spring = second_sunday_of_march(2_100);
    let autumn = first_sunday_of_november(2_100);
    assert_eq!(
        offsets_at_wall(
            &TabulatedToHorizon,
            &spring.and_hms_opt(2, 30, 0).expect("gap")
        ),
        MappedLocalTime::None
    );
    assert_eq!(
        offsets_at_wall(
            &TabulatedToHorizon,
            &autumn.and_hms_opt(1, 30, 0).expect("overlap")
        ),
        MappedLocalTime::Ambiguous(fixed(SUMMER_SECONDS), fixed(STANDARD_SECONDS))
    );
    assert_eq!(
        offsets_at_wall(&TabulatedToHorizon, &utc(2_100, 7, 1, 12)),
        MappedLocalTime::Single(fixed(SUMMER_SECONDS))
    );
    assert_eq!(
        offsets_at_wall(&TabulatedToHorizon, &utc(2_099, 7, 1, 12)),
        TabulatedToHorizon.offset_from_local_datetime(&utc(2_099, 7, 1, 12))
    );
}

#[test]
fn the_wall_of_an_instant_reads_back_as_the_instant() {
    let first = utc(2_100, 1, 1, 0).and_utc().timestamp();
    let last = utc(2_501, 1, 1, 0).and_utc().timestamp();
    let step = 86_400 * 3 + 3_600 * 5 + 1_861;
    let mut checked = 0_u32;
    let mut summer = 0_u32;
    let mut seconds = first;
    while seconds < last {
        let instant = DateTime::from_timestamp(seconds, 0)
            .expect("an instant")
            .naive_utc();
        let wall = wall_at_instant(&TabulatedToHorizon, &instant).expect("a wall clock");
        let offset = offset_at_instant(&TabulatedToHorizon, &instant);
        let back = match offsets_at_wall(&TabulatedToHorizon, &wall) {
            MappedLocalTime::Single(single) => {
                assert_eq!(single, offset, "{instant}");
                wall.checked_sub_offset(single)
            }
            MappedLocalTime::Ambiguous(earliest, latest) => {
                assert!(offset == earliest || offset == latest, "{instant}");
                wall.checked_sub_offset(offset)
            }
            MappedLocalTime::None => None,
        };
        assert_eq!(back, Some(instant), "{instant}");
        summer += u32::from(offset == fixed(SUMMER_SECONDS));
        checked += 1;
        seconds += step;
    }
    assert!(checked > 45_000, "{checked}");
    assert!(summer > checked / 2, "{summer} of {checked}");
}
