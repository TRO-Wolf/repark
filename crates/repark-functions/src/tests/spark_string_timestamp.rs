use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, AsArray, StringArray};
use arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use chrono::{DateTime, Datelike, NaiveDate, Utc};

use crate::datetime::micros_from_local_datetime;
use crate::spark_string_timestamp::grammar::parse_timestamp_string;
use crate::spark_string_timestamp::instant::{LAST_TABULATED_YEAR, proxy_year};
use crate::spark_string_timestamp::zone::{SparkZone, spark_zone_id};
use crate::spark_string_timestamp::{
    StringCastFailure, cast_strings_to_ltz, string_to_timestamp_micros,
};

const NEW_YORK: &str = "America/New_York";

fn zone(name: &str) -> Tz {
    name.parse::<Tz>().expect("test zone parses")
}

fn fixed_now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-09-19T12:00:00Z")
        .expect("fixed clock parses")
        .with_timezone(&Utc)
}

fn micros_in(text: &str, session: &str) -> Option<i64> {
    string_to_timestamp_micros(text, zone(session), fixed_now())
}

fn offset_seconds(raw: &str) -> Option<i32> {
    match spark_zone_id(raw)? {
        SparkZone::Offset(offset) => Some(offset.local_minus_utc()),
        SparkZone::Named(_) => None,
    }
}

fn is_named(raw: &str) -> bool {
    matches!(spark_zone_id(raw), Some(SparkZone::Named(_)))
}

#[test]
fn year_and_year_month_default_the_missing_fields_to_one() {
    let year = parse_timestamp_string("2020").expect("year parses");
    assert_eq!((year.year, year.month, year.day), (2020, 1, 1));
    let month = parse_timestamp_string("2020-6").expect("year-month parses");
    assert_eq!((month.year, month.month, month.day), (2020, 6, 1));
    assert_eq!(micros_in("2020", "UTC"), Some(1_577_836_800_000_000));
}

#[test]
fn single_digit_fields_and_an_hour_without_minutes_parse() {
    let parsed = parse_timestamp_string("2020-6-1 1:2:3").expect("single digits parse");
    assert_eq!(
        (
            parsed.month,
            parsed.day,
            parsed.hour,
            parsed.minute,
            parsed.second
        ),
        (6, 1, 1, 2, 3)
    );
    assert_eq!(
        micros_in("2020-06-01 10", "UTC"),
        Some(1_591_005_600_000_000)
    );
    assert_eq!(
        micros_in("2020-06-01T10", "UTC"),
        Some(1_591_005_600_000_000)
    );
}

#[test]
fn fraction_truncates_past_six_digits_and_pads_short_ones() {
    let micros = |text: &str| parse_timestamp_string(text).map(|parsed| parsed.micros);
    assert_eq!(micros("2020-06-01 10:00:00.1"), Some(100_000));
    assert_eq!(micros("2020-06-01 10:00:00.000001"), Some(1));
    assert_eq!(micros("2020-06-01 10:00:00.1234567891"), Some(123_456));
    assert_eq!(micros("2020-06-01 10:00:00."), Some(0));
}

#[test]
fn time_only_forms_resolve_against_today_in_the_zone() {
    for text in ["T10:00", "10:00:00", "10:00", "T10"] {
        assert!(
            parse_timestamp_string(text).is_some_and(|parsed| parsed.just_time),
            "{text}"
        );
    }
    assert_eq!(micros_in("10:00:00", "UTC"), Some(1_789_812_000_000_000));
    assert_eq!(micros_in("10:00:00", NEW_YORK), Some(1_789_826_400_000_000));
    assert_eq!(
        micros_in("T10:00:00+01:00", NEW_YORK),
        Some(1_789_808_400_000_000)
    );
}

#[test]
fn time_only_refuses_a_sign_or_a_leading_blank_before_t() {
    for text in [" T10:00", "+T10:00", "-10:00", "10", "24:00"] {
        assert_eq!(micros_in(text, "UTC"), None, "{text}");
    }
}

#[test]
fn a_zone_is_read_only_after_the_seconds_or_the_fraction() {
    let seconds = parse_timestamp_string("2020-06-01 10:00:00 UTC").expect("zone parses");
    assert_eq!(seconds.zone, Some(" UTC"));
    let fraction = parse_timestamp_string("2020-06-01 10:00:00.5Z").expect("zone parses");
    assert_eq!((fraction.micros, fraction.zone), (500_000, Some("Z")));
    for text in ["2020-06-01T00:00Z", "2020-06-01 10:00 UTC", "2020-06-01Z"] {
        assert_eq!(parse_timestamp_string(text), None, "{text}");
    }
    assert_eq!(micros_in("2020-06-01 10:00:00.1.2", "UTC"), None);
}

#[test]
fn trim_drops_control_bytes_and_del_but_keeps_non_ascii_blanks() {
    for text in [
        "\t2020-06-01\n",
        "\u{0b}2020-06-01",
        "\u{1f}2020-06-01\u{7f}",
        " 2020-06-01 ",
    ] {
        assert_eq!(
            micros_in(text, "UTC"),
            Some(1_590_969_600_000_000),
            "{text:?}"
        );
    }
    for text in ["\u{a0}2020-06-01", "2020-06-01\u{3000}", "", "   "] {
        assert_eq!(micros_in(text, "UTC"), None, "{text:?}");
    }
}

#[test]
fn year_digit_count_is_four_to_six_and_other_fields_one_or_two() {
    assert_eq!(micros_in("02020-06-01", "UTC"), Some(1_590_969_600_000_000));
    assert_eq!(
        micros_in("123456-01-01", "UTC"),
        Some(3_833_727_840_000_000_000)
    );
    for text in [
        "20-06-01",
        "1234567-01-01",
        "2020-06-001",
        "20200601",
        "2020--01",
    ] {
        assert_eq!(micros_in(text, "UTC"), None, "{text}");
    }
    assert_eq!(
        micros_in("-0044-03-15", "UTC"),
        Some(-63_549_360_000_000_000)
    );
    assert_eq!(
        micros_in("-0000-06-01", "UTC"),
        Some(-62_154_086_400_000_000)
    );
}

#[test]
fn calendar_and_clock_fields_are_range_checked() {
    assert_eq!(micros_in("2000-02-29", "UTC"), Some(951_782_400_000_000));
    for text in [
        "2021-02-29",
        "1900-02-29",
        "2020-06-31",
        "2020-13-01",
        "2020-00-01",
        "2020-06-01 24:00:00",
        "2020-06-01 23:59:60",
        "2020-06-01 10:60",
    ] {
        assert_eq!(micros_in(text, "UTC"), None, "{text}");
    }
}

#[test]
fn java_offset_forms_and_their_limits() {
    assert_eq!(offset_seconds("+2"), Some(7_200));
    assert_eq!(offset_seconds("+02"), Some(7_200));
    assert_eq!(offset_seconds("+0230"), Some(9_000));
    assert_eq!(offset_seconds("+02:30:15"), Some(9_015));
    assert_eq!(offset_seconds("+023015"), Some(9_015));
    assert_eq!(offset_seconds("-00:00"), Some(0));
    assert_eq!(offset_seconds("+18:00"), Some(64_800));
    assert_eq!(offset_seconds("Z"), Some(0));
    for raw in ["+19:00", "+18:01", "z", "+", "+02:3x"] {
        assert!(spark_zone_id(raw).is_none(), "{raw}");
    }
}

#[test]
fn legacy_single_digit_hour_and_minute_offsets_are_padded() {
    assert_eq!(offset_seconds("+2:00"), Some(7_200));
    assert_eq!(offset_seconds("+02:0"), Some(7_200));
    assert_eq!(offset_seconds("+2:3"), Some(7_380));
    assert_eq!(offset_seconds("UTC+1:30"), Some(5_400));
}

#[test]
fn utc_gmt_ut_prefixes_short_ids_and_regions_resolve_like_java() {
    assert_eq!(offset_seconds(" UTC"), Some(0));
    assert_eq!(offset_seconds("UTC+1"), Some(3_600));
    assert_eq!(offset_seconds("GMT-5"), Some(-18_000));
    assert_eq!(offset_seconds("UT+3"), Some(10_800));
    assert_eq!(offset_seconds("EST"), Some(-18_000));
    assert!(is_named("GMT0"));
    assert!(is_named("PST"));
    assert!(is_named("Etc/GMT+5"));
    assert_eq!(
        micros_in("2020-06-01 10:00:00 IST", "UTC"),
        Some(1_590_985_800_000_000)
    );
    for raw in ["utc", "Mars/Phobos", "1UTC", "America/New York"] {
        assert!(spark_zone_id(raw).is_none(), "{raw}");
    }
}

#[test]
fn a_named_zone_in_the_string_overrides_the_session_zone() {
    assert_eq!(
        micros_in("2020-06-01 10:00:00 UTC", NEW_YORK),
        Some(1_591_005_600_000_000)
    );
    assert_eq!(
        micros_in("2020-06-01 10:00:00 America/New_York", "UTC"),
        Some(1_591_020_000_000_000)
    );
}

#[test]
fn a_dst_gap_shifts_forward_and_an_overlap_takes_the_earlier_offset() {
    assert_eq!(
        micros_in("2026-03-08 02:30:00", NEW_YORK),
        Some(1_772_955_000_000_000)
    );
    assert_eq!(
        micros_in("2026-03-08 03:00:00", NEW_YORK),
        Some(1_772_953_200_000_000)
    );
    assert_eq!(
        micros_in("2026-11-01 01:30:00", NEW_YORK),
        Some(1_793_511_000_000_000)
    );
    assert_eq!(
        micros_in("2026-11-01 02:00:00", NEW_YORK),
        Some(1_793_516_400_000_000)
    );
}

#[test]
fn chrono_tz_tables_stop_after_the_last_tabulated_year() {
    let local = |year: i32| {
        NaiveDate::from_ymd_opt(year, 7, 1)
            .and_then(|date| date.and_hms_opt(12, 0, 0))
            .expect("wall builds")
    };
    let offset_hours = |year: i32| {
        let wall = local(year);
        let instant =
            micros_from_local_datetime(wall, zone(NEW_YORK), None).expect("wall localizes");
        (wall.and_utc().timestamp_micros() - instant) / 3_600_000_000
    };
    let last = i32::try_from(LAST_TABULATED_YEAR).expect("year fits");
    assert_eq!(offset_hours(last), -4);
    assert_eq!(offset_hours(last + 1), -5);
}

#[test]
fn far_future_walls_follow_the_final_dst_rule() {
    assert_eq!(
        micros_in("2100-07-01 12:00:00", NEW_YORK),
        Some(4_118_140_800_000_000)
    );
    assert_eq!(
        micros_in("2999-07-01 12:00:00", NEW_YORK),
        Some(32_487_840_000_000_000)
    );
    assert_eq!(
        micros_in("2999-01-01", NEW_YORK),
        Some(32_472_162_000_000_000)
    );
}

#[test]
fn proxy_years_keep_the_leap_flag_and_the_january_first_weekday() {
    for year in [
        2100_i64, 2400, 2999, 9999, 12_345, 294_247, -290_308, -44, 0, 1,
    ] {
        let proxy = proxy_year(year);
        let first = |candidate: i64| {
            NaiveDate::from_ymd_opt(i32::try_from(candidate).expect("proxy fits"), 1, 1)
        };
        let leap = |candidate: i64| {
            NaiveDate::from_ymd_opt(i32::try_from(candidate).expect("proxy fits"), 2, 29).is_some()
        };
        if let (Some(original), Some(proxied)) = (first(year), first(proxy)) {
            assert_eq!(original.weekday(), proxied.weekday(), "{year}");
            assert_eq!(leap(year), leap(proxy), "{year}");
        }
        assert!(
            (1_200..=LAST_TABULATED_YEAR).contains(&proxy),
            "{year} -> {proxy}"
        );
    }
}

#[test]
fn local_mean_time_applies_before_standard_time() {
    assert_eq!(
        micros_in("0001-01-01", NEW_YORK),
        Some(-62_135_579_038_000_000)
    );
    assert_eq!(
        micros_in("1883-11-18 12:00:00", NEW_YORK),
        Some(-2_717_651_038_000_000)
    );
}

#[test]
fn the_microsecond_range_edges_answer_or_overflow_to_null() {
    assert_eq!(
        micros_in("-290308-12-21 19:59:05.224192", "UTC"),
        Some(i64::MIN)
    );
    assert_eq!(micros_in("-290308-12-21 19:59:05.224191", "UTC"), None);
    assert_eq!(
        micros_in("+294247-01-10 04:00:54.775807", "UTC"),
        Some(i64::MAX)
    );
    assert_eq!(micros_in("+294247-01-10 04:00:54.775808", "UTC"), None);
    assert_eq!(
        micros_in("-290308-12-21 19:59:05.224191", NEW_YORK),
        Some(-9_223_372_019_092_775_809)
    );
    assert_eq!(micros_in("+294247-01-10 04:00:54.775807", NEW_YORK), None);
}

fn strings(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

#[test]
fn the_null_failure_mode_yields_null_and_keeps_input_nulls() {
    let produced = cast_strings_to_ltz(
        &strings(&[Some("2020"), Some("garbage"), None]),
        zone("UTC"),
        StringCastFailure::Null,
    )
    .expect("null mode never raises");
    assert_eq!(
        produced.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC")))
    );
    let micros = produced.as_primitive::<TimestampMicrosecondType>();
    assert_eq!(micros.value(0), 1_577_836_800_000_000);
    assert!(micros.is_null(1));
    assert!(micros.is_null(2));
}

#[test]
fn the_raise_failure_mode_names_the_first_bad_value_in_spark_quoting() {
    let error = cast_strings_to_ltz(
        &strings(&[Some("2020"), None, Some("it's a \\ path"), Some("later")]),
        zone("UTC"),
        StringCastFailure::Raise,
    )
    .expect_err("raise mode refuses a malformed value");
    assert_eq!(
        error.strip_backtrace(),
        "Execution error: [CAST_INVALID_INPUT] The value 'it\\'s a \\\\ path' of the type \
         \"STRING\" cannot be cast to \"TIMESTAMP\" because it is malformed. Correct the value \
         as per the syntax, or change its target type. Use `try_cast` to tolerate malformed \
         input and return NULL instead. SQLSTATE: 22018"
    );
}
