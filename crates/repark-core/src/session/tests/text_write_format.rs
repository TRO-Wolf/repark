use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::datatypes::{DataType, Field, Schema};
use chrono::{FixedOffset, NaiveDate, TimeZone};

use crate::session::text_write_format::fast::{
    DateDefaultState, TimestampDefaultState, wall_micros_bounds,
};
use crate::session::text_write_format::render::{
    RenderValue, render_compiled_into, render_date_default_into, render_ntz_default_into,
    render_timestamp_default_into,
};
use crate::session::text_write_format::select::build_text_write_copy_parts;
use crate::session::text_write_format::spec::TextWriteSpec;
use crate::session::text_write_format::udf::OffsetCache;
use crate::session::text_write_format::{
    CompiledPattern, PatternKind, compile_write_pattern, micros_to_naive_wall, micros_to_wall_zone,
};

const GUIDE_URL: &str = "https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html";

fn suggestion_message(pattern: &str) -> String {
    format!(
        "[INVALID_DATETIME_PATTERN.WITH_SUGGESTION] Unrecognized datetime pattern: '{pattern}'. \
         You can form a valid datetime pattern with the guide from '{GUIDE_URL}'. SQLSTATE: 22007"
    )
}

fn recognition_message(pattern: &str) -> String {
    format!(
        "[INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION] You may get a \
         different result due to the upgrading to Spark >= 3.0:\nFail to recognize '{pattern}' \
         pattern in the DateTimeFormatter.\nYou can form a valid datetime pattern with the \
         guide from '{GUIDE_URL}'. SQLSTATE: 42K0B"
    )
}

fn failure_message(pattern: &str, kind: PatternKind) -> String {
    compile_write_pattern(pattern, kind)
        .expect_err("pattern must fail")
        .message()
        .to_string()
}

fn render_compiled(compiled: &CompiledPattern, value: &RenderValue) -> Result<String, String> {
    let mut output = String::new();
    render_compiled_into(compiled, value, &mut output)?;
    Ok(output)
}

fn render_timestamp_default(
    wall: &chrono::NaiveDateTime,
    nanos: u32,
    offset: FixedOffset,
) -> String {
    let mut output = String::new();
    render_timestamp_default_into(&mut output, wall, nanos, offset);
    output
}

fn render_ntz_default(wall: &chrono::NaiveDateTime, nanos: u32) -> String {
    let mut output = String::new();
    render_ntz_default_into(&mut output, wall, nanos);
    output
}

fn render_date_default(date: NaiveDate) -> String {
    let mut output = String::new();
    render_date_default_into(&mut output, date);
    output
}

fn render_instant(pattern: &str, micros: i64, zone_name: &str) -> Result<String, String> {
    let zone = Tz::from_str(zone_name).expect("zone parses");
    let (wall, offset) = micros_to_wall_zone(micros, zone).expect("instant in range");
    let nanos = u32::try_from(micros.rem_euclid(1_000_000)).expect("micros fit") * 1_000;
    let compiled = compile_write_pattern(pattern, PatternKind::Timestamp).expect("pattern valid");
    let value = RenderValue::Instant {
        wall,
        nanos,
        offset,
        zone_id: zone_name,
    };
    render_compiled(&compiled, &value)
}

fn render_wall(pattern: &str, micros: i64) -> Result<String, String> {
    let wall = micros_to_naive_wall(micros).expect("instant in range");
    let nanos = u32::try_from(micros.rem_euclid(1_000_000)).expect("micros fit") * 1_000;
    let compiled =
        compile_write_pattern(pattern, PatternKind::TimestampNtz).expect("pattern valid");
    render_compiled(&compiled, &RenderValue::Wall { wall, nanos })
}

fn render_date(pattern: &str, year: i32, month: u32, day: u32) -> Result<String, String> {
    let date = NaiveDate::from_ymd_opt(year, month, day).expect("date valid");
    let compiled = compile_write_pattern(pattern, PatternKind::Date).expect("pattern valid");
    render_compiled(&compiled, &RenderValue::Date { date })
}

fn utc_micros(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> i64 {
    chrono::Utc
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .unwrap()
        .timestamp_micros()
}

#[test]
fn timestamp_brief_user_format_compiles() {
    compile_write_pattern("yyyy/MM/dd HH:mm", PatternKind::Timestamp).expect("brief format valid");
}

#[test]
fn timestamp_zone_name_refused_with_suggestion() {
    for pattern in ["z", "zz", "zzzz", "v"] {
        assert_eq!(
            failure_message(pattern, PatternKind::Timestamp),
            suggestion_message(pattern),
            "unsupported zone-name pattern refuses loudly"
        );
    }
}

#[test]
fn timestamp_double_v_refused_with_suggestion() {
    assert_eq!(
        failure_message("vv", PatternKind::Timestamp),
        suggestion_message("vv")
    );
}

#[test]
fn timestamp_zone_id_count_validated() {
    compile_write_pattern("VV", PatternKind::Timestamp).expect("VV valid");
    assert_eq!(
        failure_message("V", PatternKind::Timestamp),
        suggestion_message("V")
    );
    assert_eq!(
        failure_message("VVV", PatternKind::Timestamp),
        suggestion_message("VVV")
    );
}

#[test]
fn timestamp_localized_offset_count_validated() {
    compile_write_pattern("O", PatternKind::Timestamp).expect("O valid");
    compile_write_pattern("OOOO", PatternKind::Timestamp).expect("OOOO valid");
    assert_eq!(
        failure_message("OOO", PatternKind::Timestamp),
        suggestion_message("OOO")
    );
    assert_eq!(
        failure_message("OOOOO", PatternKind::Timestamp),
        suggestion_message("OOOOO")
    );
}

#[test]
fn timestamp_quarter_overflow_reports_level() {
    let failure = compile_write_pattern("QQQQQ", PatternKind::Timestamp).expect_err("level fails");
    assert!(failure.illegal_argument());
    assert_eq!(
        failure.message(),
        "[INVALID_DATETIME_PATTERN.LENGTH] Unrecognized datetime pattern: QQQQQ. Too many letters \
         in datetime pattern: QQQQQ. Please reduce pattern length. SQLSTATE: 22007"
    );
}

#[test]
fn timestamp_day_overflow_reports_recognition() {
    assert_eq!(
        failure_message("dddd", PatternKind::Timestamp),
        recognition_message("dddd")
    );
}

#[test]
fn timestamp_bogus_reports_illegal_character() {
    let failure = compile_write_pattern("BOGUS", PatternKind::Timestamp).expect_err("bogus fails");
    assert!(failure.illegal_argument());
    assert_eq!(
        failure.message(),
        "[INVALID_DATETIME_PATTERN.ILLEGAL_CHARACTER] Unrecognized datetime pattern: BOGUS. \
         Illegal pattern character found in datetime pattern: B. Please provide legal character. \
         SQLSTATE: 22007"
    );
}

#[test]
fn timestamp_bare_close_reports_recognition() {
    assert_eq!(
        failure_message("]", PatternKind::Timestamp),
        recognition_message("]")
    );
}

#[test]
fn ntz_legacy_failures_downgrade_to_suggestion() {
    for pattern in ["qqqqq", "BOGUS", "w", "SSSSSSSSSS"] {
        assert_eq!(
            failure_message(pattern, PatternKind::TimestampNtz),
            suggestion_message(pattern),
            "ntz downgrades legacy failure"
        );
    }
}

#[test]
fn ntz_zone_id_count_refused_with_suggestion() {
    assert_eq!(
        failure_message("yyyy V", PatternKind::TimestampNtz),
        suggestion_message("yyyy V")
    );
}

#[test]
fn date_quarter_overflow_reports_level() {
    let failure = compile_write_pattern("qqqqq", PatternKind::Date).expect_err("level fails");
    assert!(failure.illegal_argument());
}

#[test]
fn date_month_overflow_reports_recognition() {
    assert_eq!(
        failure_message("MMMMM", PatternKind::Date),
        recognition_message("MMMMM")
    );
}

#[test]
fn date_bogus_reports_illegal_character() {
    let failure = compile_write_pattern("BOGUS", PatternKind::Date).expect_err("bogus fails");
    assert!(failure.illegal_argument());
    assert!(failure.message().contains("pattern character"));
}

#[test]
fn date_unmatched_close_reports_lazy_message() {
    let failure = compile_write_pattern("xxx]", PatternKind::Date).expect_err("close fails");
    assert!(!failure.illegal_argument());
    assert_eq!(
        failure.message(),
        "Pattern invalid as it contains ] without previous ["
    );
}

#[test]
fn date_unclosed_quote_reports_lazy_message() {
    let failure = compile_write_pattern("yyyy-MM-dd'", PatternKind::Date).expect_err("quote fails");
    assert_eq!(
        failure.message(),
        "Pattern ends with an incomplete string literal: uuuu-MM-dd'"
    );
}

#[test]
fn date_zone_id_count_reports_lazy_message() {
    let failure = compile_write_pattern("yyyy V", PatternKind::Date).expect_err("V fails");
    assert_eq!(failure.message(), "Pattern letter count must be 2: V");
}

#[test]
fn default_render_spans_dst_gap_in_session_zone() {
    let micros = utc_micros(2024, 3, 10, 7, 30, 0);
    let zone = Tz::from_str("America/New_York").expect("zone parses");
    let (wall, offset) = micros_to_wall_zone(micros, zone).expect("instant in range");
    assert_eq!(
        render_timestamp_default(&wall, 0, offset),
        "2024-03-10T03:30:00.000-04:00"
    );
}

#[test]
fn default_render_keeps_lmt_offset_seconds() {
    let micros = utc_micros(1899, 12, 31, 18, 38, 49);
    let zone = Tz::from_str("Asia/Kolkata").expect("zone parses");
    let (wall, offset) = micros_to_wall_zone(micros, zone).expect("instant in range");
    assert_eq!(
        render_timestamp_default(&wall, 0, offset),
        "1899-12-31T23:59:59.000+05:21:10"
    );
}

#[test]
fn default_render_uses_z_for_zero_offset() {
    let micros = utc_micros(2024, 6, 15, 12, 34, 56) + 789_456;
    let zone = Tz::from_str("UTC").expect("zone parses");
    let (wall, offset) = micros_to_wall_zone(micros, zone).expect("instant in range");
    assert_eq!(
        render_timestamp_default(&wall, 789_456_000, offset),
        "2024-06-15T12:34:56.789Z"
    );
}

#[test]
fn default_render_truncates_fraction_to_millis() {
    let micros = utc_micros(2024, 6, 15, 12, 34, 56) + 999_999;
    let zone = Tz::from_str("UTC").expect("zone parses");
    let (wall, offset) = micros_to_wall_zone(micros, zone).expect("instant in range");
    assert_eq!(
        render_timestamp_default(&wall, 999_999_000, offset),
        "2024-06-15T12:34:56.999Z"
    );
}

#[test]
fn default_ntz_render_has_no_zone() {
    let micros = utc_micros(2024, 6, 15, 12, 34, 56) + 789_456;
    let wall = micros_to_naive_wall(micros).expect("instant in range");
    assert_eq!(
        render_ntz_default(&wall, 789_456_000),
        "2024-06-15T12:34:56.789"
    );
}

#[test]
fn default_date_render() {
    let date = NaiveDate::from_ymd_opt(2024, 6, 15).expect("date valid");
    assert_eq!(render_date_default(date), "2024-06-15");
}

#[test]
fn user_format_renders_brief_pattern() {
    let micros = utc_micros(2024, 6, 15, 16, 34, 56);
    assert_eq!(
        render_instant("yyyy/MM/dd HH:mm", micros, "America/New_York").expect("renders"),
        "2024/06/15 12:34"
    );
}

#[test]
fn offset_letters_match_spark_for_lmt_seconds() {
    let micros = utc_micros(1899, 12, 31, 18, 38, 49);
    let zone = "Asia/Kolkata";
    assert_eq!(render_instant("x", micros, zone).expect("renders"), "+0521");
    assert_eq!(
        render_instant("xx", micros, zone).expect("renders"),
        "+0521"
    );
    assert_eq!(
        render_instant("xxx", micros, zone).expect("renders"),
        "+05:21"
    );
    assert_eq!(
        render_instant("xxxx", micros, zone).expect("renders"),
        "+052110"
    );
    assert_eq!(
        render_instant("xxxxx", micros, zone).expect("renders"),
        "+05:21:10"
    );
    assert_eq!(
        render_instant("OOOO", micros, zone).expect("renders"),
        "GMT+05:21:10"
    );
}

#[test]
fn offset_letters_match_spark_for_whole_hour() {
    let micros = utc_micros(2024, 6, 15, 16, 34, 56);
    let zone = "America/New_York";
    assert_eq!(
        render_instant("XXXX", micros, zone).expect("renders"),
        "-0400"
    );
    assert_eq!(
        render_instant("ZZZZ", micros, zone).expect("renders"),
        "GMT-04:00"
    );
    assert_eq!(render_instant("O", micros, zone).expect("renders"), "GMT-4");
    assert_eq!(
        render_instant("VV", micros, zone).expect("renders"),
        "America/New_York"
    );
}

#[test]
fn localized_offset_zero_renders_gmt() {
    let micros = utc_micros(2024, 6, 15, 12, 34, 56);
    assert_eq!(
        render_instant("OOOO", micros, "UTC").expect("renders"),
        "GMT"
    );
}

#[test]
fn year_and_era_and_mjd_match_spark() {
    let micros = utc_micros(2024, 6, 15, 16, 34, 56);
    let zone = "America/New_York";
    assert_eq!(
        render_instant("yyyyy", micros, zone).expect("renders"),
        "02024"
    );
    assert_eq!(
        render_instant("gg", micros, zone).expect("renders"),
        "60476"
    );
    assert_eq!(render_instant("G", micros, zone).expect("renders"), "AD");
}

#[test]
fn padded_fields_match_spark_pad_cell() {
    let micros = utc_micros(2024, 1, 5, 3, 4, 5) + 6_007;
    assert_eq!(
        render_instant("DDD dd m s H h K k a S SS", micros, "UTC").expect("renders"),
        "005 05 4 5 3 3 3 3 AM 0 00"
    );
}

#[test]
fn date_fields_match_spark_weekday_cell() {
    assert_eq!(
        render_date("E Q G F D", 2024, 6, 15).expect("renders"),
        "Sat 2 AD 1 167"
    );
}

#[test]
fn ntz_fields_match_spark_cell() {
    let micros = utc_micros(2024, 6, 15, 12, 34, 56) + 789_456;
    assert_eq!(
        render_wall("E Q G h a F D", micros).expect("renders"),
        "Sat 2 AD 12 PM 1 167"
    );
}

#[test]
fn quoted_literal_keeps_escaped_quote() {
    let micros = utc_micros(2024, 6, 15, 16, 34, 56);
    assert_eq!(
        render_instant("'o''clock' yyyy", micros, "America/New_York").expect("renders"),
        "o'clock 2024"
    );
}

#[test]
fn empty_section_renders_empty() {
    let micros = utc_micros(2024, 6, 15, 12, 34, 56);
    assert_eq!(
        render_instant("[]", micros, "UTC").expect("renders"),
        String::new()
    );
}

#[test]
fn unclosed_section_skips_failing_field() {
    assert_eq!(
        render_date("[xxx", 2024, 6, 15).expect("renders"),
        String::new()
    );
}

#[test]
fn date_time_field_fails_lazy() {
    assert_eq!(
        render_date("HH", 2024, 6, 15).expect_err("hour fails"),
        "Unsupported field: HourOfDay"
    );
}

#[test]
fn ntz_offset_field_fails_lazy() {
    let micros = utc_micros(2024, 6, 15, 12, 34, 56);
    assert_eq!(
        render_wall("XXX", micros).expect_err("offset fails"),
        "Unsupported field: OffsetSeconds"
    );
}

#[test]
fn ntz_zone_id_fails_lazy() {
    let micros = utc_micros(2024, 6, 15, 12, 34, 56);
    assert_eq!(
        render_wall("VV", micros).expect_err("zone fails"),
        "Unable to extract ZoneId from temporal 2024-06-15T12:34:56"
    );
}

#[test]
fn date_zone_name_fails_lazy() {
    assert_eq!(
        render_date("z", 2024, 6, 15).expect_err("zone fails"),
        "Unable to extract ZoneId from temporal 2024-06-15"
    );
}

fn schema_of(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

fn timestamp_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
        true,
    )
}

#[test]
fn parts_without_temporal_keep_plain_format() {
    let schema = schema_of(vec![Field::new("id", DataType::Int64, true)]);
    let options = HashMap::new();
    let parts = build_text_write_copy_parts(&schema, "v", "UTC", &options, &[], "CSV")
        .expect("parts build");
    assert_eq!(parts.select_sql, "SELECT * FROM v");
    assert_eq!(parts.stored_as, "CSV");
    assert!(parts.spec_options_sql.is_empty());
}

#[test]
fn parts_with_temporal_resolve_sink_format_and_zone() {
    let schema = schema_of(vec![
        Field::new("id", DataType::Int64, true),
        timestamp_field("t"),
    ]);
    let options = HashMap::new();
    let parts = build_text_write_copy_parts(&schema, "v", "America/New_York", &options, &[], "CSV")
        .expect("parts build");
    assert_eq!(parts.select_sql, "SELECT * FROM v");
    assert_eq!(parts.stored_as, "repark_text_csv");
    assert!(
        parts
            .spec_options_sql
            .contains("'repark.text.zone' 'America/New_York'"),
        "zone rides the spec options: {}",
        parts.spec_options_sql
    );
    let parts =
        build_text_write_copy_parts(&schema, "v", "America/New_York", &options, &[], "JSON")
            .expect("parts build");
    assert_eq!(parts.stored_as, "repark_text_json");
}

#[test]
fn parts_skip_partition_columns() {
    let schema = schema_of(vec![timestamp_field("day")]);
    let options = HashMap::new();
    let parts =
        build_text_write_copy_parts(&schema, "v", "UTC", &options, &["day".to_string()], "CSV")
            .expect("parts build");
    assert_eq!(parts.select_sql, "SELECT * FROM v");
    assert_eq!(parts.stored_as, "CSV");
    assert!(parts.spec_options_sql.is_empty());
}

#[test]
fn parts_reject_bad_user_pattern() {
    let schema = schema_of(vec![timestamp_field("t")]);
    let mut options = HashMap::new();
    options.insert("timestampFormat".to_string(), "vv".to_string());
    let error = build_text_write_copy_parts(&schema, "v", "UTC", &options, &[], "CSV")
        .expect_err("rejects");
    assert!(
        error
            .to_string()
            .contains("INVALID_DATETIME_PATTERN.WITH_SUGGESTION"),
        "spark class surfaces: {error}"
    );
}

#[test]
fn fixed_offset_renders_default_shape() {
    let wall = NaiveDate::from_ymd_opt(2024, 1, 5)
        .expect("date valid")
        .and_hms_opt(3, 4, 5)
        .expect("time valid");
    let offset = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("offset valid");
    assert_eq!(
        render_timestamp_default(&wall, 6_007_000, offset),
        "2024-01-05T03:04:05.006+05:30"
    );
}

#[test]
fn quote_run_four_renders_single_quote() {
    let micros = utc_micros(2024, 3, 5, 7, 8, 9);
    assert_eq!(
        render_instant("yyyy''''MM", micros, "America/New_York").expect("renders"),
        "2024'03"
    );
    assert_eq!(
        render_instant("''''yyyy", micros, "America/New_York").expect("renders"),
        "'2024"
    );
}

#[test]
fn quote_run_eight_renders_three_quotes() {
    let micros = utc_micros(2024, 3, 5, 7, 8, 9);
    assert_eq!(
        render_instant("''''''''", micros, "America/New_York").expect("renders"),
        "'''"
    );
    assert_eq!(
        render_instant("''''''", micros, "America/New_York").expect("renders"),
        "''"
    );
    assert_eq!(
        render_instant("'''a'''", micros, "America/New_York").expect("renders"),
        "'a'"
    );
    assert_eq!(
        render_instant("yyyy'''' ''", micros, "America/New_York").expect("renders"),
        "2024' '"
    );
}

#[test]
fn year_run_seven_refuses_per_kind() {
    assert_eq!(
        failure_message("yyyyyyy", PatternKind::Timestamp),
        recognition_message("yyyyyyy")
    );
    assert_eq!(
        failure_message("yyyyyyy", PatternKind::TimestampNtz),
        suggestion_message("yyyyyyy")
    );
    assert_eq!(
        failure_message("yyyyyyy", PatternKind::Date),
        recognition_message("yyyyyyy")
    );
    for kind in [
        PatternKind::Timestamp,
        PatternKind::TimestampNtz,
        PatternKind::Date,
    ] {
        compile_write_pattern("yyyyyy", kind).expect("six years stay valid");
    }
}

#[test]
fn proleptic_year_matches_spark_without_era() {
    assert_eq!(render_date("yyyy", 0, 1, 1).expect("renders"), "0000");
    assert_eq!(render_date("y", 0, 1, 1).expect("renders"), "0");
    assert_eq!(render_date("yy", 0, 1, 1).expect("renders"), "00");
    assert_eq!(render_date("yyyy", -1, 6, 15).expect("renders"), "-0001");
    assert_eq!(render_date("y", -1, 6, 15).expect("renders"), "-1");
    assert_eq!(render_date("yy", -1, 6, 15).expect("renders"), "01");
    assert_eq!(render_date("yyyy", -1000, 6, 15).expect("renders"), "-1000");
    assert_eq!(
        render_date("yyyy", 10_000, 6, 15).expect("renders"),
        "+10000"
    );
    assert_eq!(
        render_wall("yyyy", utc_micros(0, 6, 15, 12, 0, 0)).expect("renders"),
        "0000"
    );
}

#[test]
fn year_of_era_applies_only_with_era_letter() {
    assert_eq!(render_date("G yyyy", 0, 1, 1).expect("renders"), "BC 0001");
    assert_eq!(
        render_date("G yyyy", 10_000, 6, 15).expect("renders"),
        "AD +10000"
    );
}

#[test]
fn timestamp_trailing_close_class_depends_on_legacy_letters() {
    assert_eq!(
        failure_message("yyyy]", PatternKind::Timestamp),
        recognition_message("yyyy]")
    );
    assert_eq!(
        failure_message("xxx]", PatternKind::Timestamp),
        suggestion_message("xxx]")
    );
}

#[test]
fn date_trailing_close_recognized_for_legacy_letters() {
    assert_eq!(
        failure_message("yyyy]", PatternKind::Date),
        recognition_message("yyyy]")
    );
}

#[test]
fn parts_hex_encode_pattern_options() {
    let schema = schema_of(vec![timestamp_field("t")]);
    let mut options = HashMap::new();
    options.insert("timestampFormat".to_string(), "'\\'yyyy".to_string());
    let parts = build_text_write_copy_parts(&schema, "v", "UTC", &options, &[], "CSV")
        .expect("parts build");
    assert!(
        parts
            .spec_options_sql
            .contains("'repark.text.timestamp_format_hex' '275c2779797979'"),
        "pattern rides as hex: {}",
        parts.spec_options_sql
    );
}

#[test]
fn spec_format_options_decode_hex_and_strip_keys() {
    let mut format_options = HashMap::new();
    format_options.insert("repark.text.zone".to_string(), "UTC".to_string());
    format_options.insert(
        "repark.text.timestamp_format_hex".to_string(),
        "275c2779797979".to_string(),
    );
    format_options.insert("format.has_header".to_string(), "false".to_string());
    let (spec, rest) = TextWriteSpec::from_format_options(&format_options).expect("spec builds");
    assert_eq!(spec.zone_raw, "UTC");
    assert_eq!(
        rest,
        HashMap::from([("format.has_header".to_string(), "false".to_string())])
    );
    let mut bad = HashMap::new();
    bad.insert("repark.text.zone".to_string(), "UTC".to_string());
    bad.insert(
        "repark.text.timestamp_format_hex".to_string(),
        "zz".to_string(),
    );
    let error = TextWriteSpec::from_format_options(&bad).expect_err("bad hex refuses");
    assert!(
        error.to_string().contains("not valid hex"),
        "loud hex refusal: {error}"
    );
}

#[test]
fn parts_format_case_duplicate_temporal_columns() {
    let schema = schema_of(vec![timestamp_field("T"), timestamp_field("t")]);
    let options = HashMap::new();
    let parts = build_text_write_copy_parts(&schema, "v", "America/New_York", &options, &[], "CSV")
        .expect("parts build");
    assert_eq!(parts.stored_as, "repark_text_csv");
    assert_eq!(parts.select_sql, "SELECT * FROM v");
}

#[test]
fn modified_julian_day_pads_to_letter_count() {
    let table = [
        (
            (1858, 11, 20),
            ["3", "03", "003", "0003", "00003", "000003"],
        ),
        (
            (1858, 11, 10),
            ["-7", "-07", "-007", "-0007", "-00007", "-000007"],
        ),
        (
            (1880, 1, 1),
            ["7715", "7715", "7715", "7715", "07715", "007715"],
        ),
        (
            (2024, 3, 5),
            ["60374", "60374", "60374", "60374", "60374", "060374"],
        ),
    ];
    for ((year, month, day), expected) in table {
        for (position, want) in expected.iter().enumerate() {
            let pattern = "g".repeat(position + 1);
            assert_eq!(
                render_date(&pattern, year, month, day).expect("g renders"),
                (*want).to_string(),
                "pattern {pattern} on {year}-{month}-{day}"
            );
        }
    }
}

#[test]
fn recognition_failure_recommends_no_legacy_policy() {
    for pattern in ["dddd", "yyyyyyy", "yyyy]"] {
        let message = failure_message(pattern, PatternKind::Timestamp);
        assert!(
            !message.contains("LEGACY"),
            "no LEGACY clause survives: {message}"
        );
    }
}

fn fast_timestamp_text(
    state: &mut TimestampDefaultState,
    staged: &mut [u8; 48],
    wall: i64,
    offset: Option<FixedOffset>,
) -> String {
    let len = state.render(wall, offset, staged);
    std::str::from_utf8(&staged[..len])
        .expect("ascii")
        .to_string()
}

fn fast_differential_zones() -> [&'static str; 7] {
    [
        "America/New_York",
        "Australia/Lord_Howe",
        "Pacific/Apia",
        "Asia/Kathmandu",
        "UTC",
        "+05:30",
        "-08:00",
    ]
}

fn fast_differential_instants() -> Vec<i64> {
    let mut instants = Vec::new();
    for hour in 0..1440 {
        instants.push(utc_micros(2024, 2, 20, 0, 0, 0) + i64::from(hour) * 3_600_000_000);
    }
    for day in 0..5 {
        instants.push(utc_micros(1883, 11, 16, 12, 0, 0) + i64::from(day) * 86_400_000_000);
    }
    for hour in 0..72 {
        instants.push(utc_micros(1850, 6, 1, 0, 0, 0) + i64::from(hour) * 3_600_000_000);
    }
    for (year, month, day) in [
        (2024, 2, 28),
        (2023, 12, 31),
        (1999, 12, 31),
        (2000, 2, 28),
        (9999, 12, 31),
        (1, 1, 1),
        (1, 12, 31),
    ] {
        let noon = utc_micros(year, month, day, 12, 0, 0);
        for delta in [-86_400_000_001, -86_400_000_000, -1, 0, 1, 86_400_000_000] {
            instants.push(noon + delta);
        }
        for second in 0..130 {
            instants.push(noon + second * 1_000_000);
        }
    }
    instants.push(utc_micros(2200, 6, 15, 12, 0, 0));
    instants.push(utc_micros(1800, 1, 1, 0, 0, 0));
    instants.push(utc_micros(200_000, 6, 15, 12, 0, 0));
    instants.push(i64::MAX - 1);
    instants.push(i64::MIN + 1);
    instants
}

#[test]
fn fast_default_renders_match_scalar_renders() {
    let zones = fast_differential_zones();
    let instants = fast_differential_instants();
    for zone_name in zones {
        let zone = Tz::from_str(zone_name).expect("zone parses");
        let mut cache = OffsetCache::new(zone);
        let mut state = TimestampDefaultState::new();
        let mut staged = [0u8; 48];
        for micros in &instants {
            let old = micros_to_wall_zone(*micros, zone).map(|(wall, offset)| {
                let nanos =
                    u32::try_from(micros.rem_euclid(1_000_000)).expect("micros fit") * 1_000;
                render_timestamp_default(&wall, nanos, offset)
            });
            let fast = cache.resolve_micros(*micros).map(|(wall, offset)| {
                fast_timestamp_text(&mut state, &mut staged, wall, Some(offset))
            });
            assert_eq!(fast, old, "zone {zone_name} micros {micros}");
        }
        let mut state = TimestampDefaultState::new();
        for micros in instants.iter().rev() {
            let old = micros_to_wall_zone(*micros, zone).map(|(wall, offset)| {
                let nanos =
                    u32::try_from(micros.rem_euclid(1_000_000)).expect("micros fit") * 1_000;
                render_timestamp_default(&wall, nanos, offset)
            });
            let fast = cache.resolve_micros(*micros).map(|(wall, offset)| {
                fast_timestamp_text(&mut state, &mut staged, wall, Some(offset))
            });
            assert_eq!(fast, old, "zone {zone_name} reversed micros {micros}");
        }
    }
    let mut state = TimestampDefaultState::new();
    let mut staged = [0u8; 48];
    let (lowest, highest) = wall_micros_bounds();
    for micros in &instants {
        let old = micros_to_naive_wall(*micros).map(|wall| {
            let nanos = u32::try_from(micros.rem_euclid(1_000_000)).expect("micros fit") * 1_000;
            render_ntz_default(&wall, nanos)
        });
        let fast = if *micros < lowest || *micros > highest {
            None
        } else {
            Some(fast_timestamp_text(&mut state, &mut staged, *micros, None))
        };
        assert_eq!(fast, old, "ntz micros {micros}");
    }
    let mut boundaries = Vec::new();
    for days in [
        -2_500_000i64,
        -1_000_000,
        -100_000,
        -10_000,
        -1_000,
        -100,
        0,
        100,
    ] {
        for extra in 0..400 {
            boundaries.push(days + extra);
        }
    }
    for days in [
        i64::from(i32::MIN),
        i64::from(i32::MAX),
        2_958_465,
        2_958_466,
        -719_163,
        719_162,
    ] {
        boundaries.push(days);
    }
    let mut date_state = DateDefaultState::new();
    for days in &boundaries {
        let old = i32::try_from(*days).ok().and_then(|narrow| {
            narrow.checked_add(719_163).and_then(|absolute| {
                NaiveDate::from_num_days_from_ce_opt(absolute).map(render_date_default)
            })
        });
        let fast = date_state.render(*days, &mut staged).map(|len| {
            std::str::from_utf8(&staged[..len])
                .expect("ascii")
                .to_string()
        });
        assert_eq!(fast, old, "date days {days}");
    }
    let mut date_state = DateDefaultState::new();
    for days in boundaries.iter().rev() {
        let old = i32::try_from(*days).ok().and_then(|narrow| {
            narrow.checked_add(719_163).and_then(|absolute| {
                NaiveDate::from_num_days_from_ce_opt(absolute).map(render_date_default)
            })
        });
        let fast = date_state.render(*days, &mut staged).map(|len| {
            std::str::from_utf8(&staged[..len])
                .expect("ascii")
                .to_string()
        });
        assert_eq!(fast, old, "date reversed days {days}");
    }
}

#[test]
fn offset_cache_matches_direct_zone_resolution() {
    let zones = [
        "America/New_York",
        "Australia/Lord_Howe",
        "Pacific/Apia",
        "Asia/Kathmandu",
        "UTC",
        "+05:30",
    ];
    let mut instants = Vec::new();
    for hour in 0..1440 {
        instants.push(utc_micros(2024, 2, 20, 0, 0, 0) + i64::from(hour) * 3_600_000_000);
    }
    for day in 0..5 {
        instants.push(utc_micros(1883, 11, 16, 12, 0, 0) + i64::from(day) * 86_400_000_000);
    }
    for hour in 0..72 {
        instants.push(utc_micros(1850, 6, 1, 0, 0, 0) + i64::from(hour) * 3_600_000_000);
    }
    let edge = utc_micros(2024, 3, 10, 7, 0, 0);
    for delta in -3..=3 {
        instants.push(edge + delta);
        instants.push(edge + delta * 1_000_000);
    }
    instants.push(utc_micros(2200, 6, 15, 12, 0, 0));
    instants.push(utc_micros(1800, 1, 1, 0, 0, 0));
    for zone_name in zones {
        let zone = Tz::from_str(zone_name).expect("zone parses");
        let mut cache = OffsetCache::new(zone);
        for micros in &instants {
            assert_eq!(
                cache.resolve(*micros),
                micros_to_wall_zone(*micros, zone),
                "zone {zone_name} micros {micros}"
            );
        }
        let mut cache = OffsetCache::new(zone);
        for micros in instants.iter().rev() {
            assert_eq!(
                cache.resolve(*micros),
                micros_to_wall_zone(*micros, zone),
                "zone {zone_name} reversed micros {micros}"
            );
        }
    }
}
