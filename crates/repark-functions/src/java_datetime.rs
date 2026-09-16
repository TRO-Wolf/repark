use std::sync::Arc;

use arrow::array::timezone::Tz;
use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};

use crate::ansi::spark_ansi_enabled_from_options;
use crate::datetime::{invoke_local_dates, localize_wall_micros_in_zone};
use crate::session_time_zone::session_time_zone_from_options;
use crate::timestamp_cast::parse_session_zone;

#[derive(Debug)]
pub(crate) enum JavaToken {
    Year { width: usize },
    MonthNumber { width: usize },
    MonthName { full: bool },
    Day { width: usize },
    Hour24 { width: usize },
    Hour12 { width: usize },
    Minute { width: usize },
    Second { width: usize },
    Fraction { width: usize },
    AmPm,
    Literal(char),
    Unsupported,
}

#[derive(Debug)]
pub(crate) struct ParsedPattern {
    tokens: Vec<JavaToken>,
}

impl ParsedPattern {
    pub(crate) fn tokens(&self) -> &[JavaToken] {
        &self.tokens
    }
}

#[derive(Debug)]
pub(crate) struct Wall {
    pub(crate) year: i32,
    pub(crate) month: u32,
    pub(crate) day: u32,
    pub(crate) hour: u32,
    pub(crate) minute: u32,
    pub(crate) second: u32,
    pub(crate) nano: u32,
}

impl Wall {
    pub(crate) fn to_naive(&self) -> Option<NaiveDateTime> {
        NaiveDate::from_ymd_opt(self.year, self.month, self.day)?.and_hms_nano_opt(
            self.hour,
            self.minute,
            self.second,
            self.nano,
        )
    }
}

#[derive(Debug)]
pub(crate) enum WallFailure {
    AtIndex { position: usize },
    InvalidValue { detail: String },
}

pub(crate) enum FormatPlan {
    AllNull,
    Shared(ParsedPattern),
    PerRow,
}

pub(crate) fn week_year_refusal_text() -> String {
    "[INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION] You may get a \
     different result due to the upgrading to Spark >= 3.0:"
        .to_string()
}

pub(crate) fn cannot_parse_text(value: &str, failure: &WallFailure, try_name: &str) -> String {
    match failure {
        WallFailure::AtIndex { position } => format!(
            "[CANNOT_PARSE_TIMESTAMP] Text '{value}' could not be parsed at index \
             {position}. Use `{try_name}` to tolerate invalid input string and return NULL \
             instead. SQLSTATE: 22007"
        ),
        WallFailure::InvalidValue { detail } => format!(
            "[CANNOT_PARSE_TIMESTAMP] Text '{value}' could not be parsed: {detail}. Use \
             `{try_name}` to tolerate invalid input string and return NULL instead. \
             SQLSTATE: 22007"
        ),
    }
}

pub(crate) fn compile_java_pattern(pattern: &str) -> Result<ParsedPattern> {
    refuse_week_year(pattern)?;
    refuse_illegal_pattern(pattern)?;
    let chars: Vec<char> = pattern.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0;
    let mut in_quote = false;
    while index < chars.len() {
        let current = chars[index];
        if current == '\'' {
            if chars.get(index + 1) == Some(&'\'') {
                tokens.push(JavaToken::Literal('\''));
                index += 2;
                continue;
            }
            in_quote = !in_quote;
            index += 1;
            continue;
        }
        if in_quote {
            tokens.push(JavaToken::Literal(current));
            index += 1;
            continue;
        }
        if matches!(
            current,
            'y' | 'u' | 'M' | 'd' | 'H' | 'h' | 'm' | 's' | 'S' | 'a'
        ) {
            let start = index;
            while index < chars.len() && chars[index] == current {
                index += 1;
            }
            tokens.push(pattern_letter_token(current, index - start));
            continue;
        }
        if current.is_ascii_alphabetic() {
            tokens.push(JavaToken::Unsupported);
        } else {
            tokens.push(JavaToken::Literal(current));
        }
        index += 1;
    }
    if in_quote {
        return Err(DataFusionError::Execution(format!(
            "[INVALID_DATETIME_PATTERN] Unterminated quoted literal in datetime pattern: \
             {pattern}. SQLSTATE: 22007"
        )));
    }
    Ok(ParsedPattern { tokens })
}

pub(crate) fn plan_format_column(formats: &StringArray) -> Result<FormatPlan> {
    let mut first: Option<&str> = None;
    let mut constant = true;
    for row in 0..formats.len() {
        if formats.is_null(row) {
            continue;
        }
        match first {
            None => first = Some(formats.value(row)),
            Some(prior) => {
                if prior != formats.value(row) {
                    constant = false;
                    break;
                }
            }
        }
    }
    match first {
        None => Ok(FormatPlan::AllNull),
        Some(pattern) => {
            if constant {
                Ok(FormatPlan::Shared(compile_java_pattern(pattern)?))
            } else {
                Ok(FormatPlan::PerRow)
            }
        }
    }
}

pub(crate) fn parse_wall_text(text: &str, pattern: &ParsedPattern) -> Result<Wall, WallFailure> {
    let mut rest = text;
    let mut consumed = 0usize;
    let mut wall = Wall {
        year: 1970,
        month: 1,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
        nano: 0,
    };
    let mut afternoon: Option<bool> = None;
    for token in pattern.tokens() {
        match token {
            JavaToken::Year { width } => {
                let (value, taken) = take_year(rest, *width).ok_or(at(consumed))?;
                wall.year = value;
                rest = step(rest, taken, &mut consumed);
            }
            JavaToken::MonthNumber { width } => {
                let (value, taken) = take_two_digit_field(rest, *width)
                    .filter(|(value, _)| (1..=12).contains(value))
                    .ok_or_else(|| month_failure(rest, *width, consumed))?;
                wall.month = value;
                rest = step(rest, taken, &mut consumed);
            }
            JavaToken::MonthName { full } => {
                let (value, taken) = take_month_name(rest, *full).ok_or(at(consumed))?;
                wall.month = value;
                rest = step(rest, taken, &mut consumed);
            }
            JavaToken::Day { width } => {
                let (value, taken) = take_two_digit_field(rest, *width)
                    .filter(|(value, _)| (1..=31).contains(value))
                    .ok_or_else(|| day_failure(rest, *width, consumed))?;
                wall.day = value;
                rest = step(rest, taken, &mut consumed);
            }
            JavaToken::Hour24 { width } => {
                let (value, taken) = take_two_digit_field(rest, *width)
                    .filter(|(value, _)| *value <= 23)
                    .ok_or_else(|| hour_failure(rest, *width, consumed, false))?;
                wall.hour = value;
                rest = step(rest, taken, &mut consumed);
            }
            JavaToken::Hour12 { width } => {
                let (value, taken) = take_two_digit_field(rest, *width)
                    .filter(|(value, _)| (1..=12).contains(value))
                    .ok_or_else(|| hour_failure(rest, *width, consumed, true))?;
                wall.hour = value;
                rest = step(rest, taken, &mut consumed);
            }
            JavaToken::Minute { width } => {
                let (value, taken) = take_two_digit_field(rest, *width)
                    .filter(|(value, _)| *value <= 59)
                    .ok_or_else(|| minute_failure(rest, *width, consumed))?;
                wall.minute = value;
                rest = step(rest, taken, &mut consumed);
            }
            JavaToken::Second { width } => {
                let (value, taken) = take_two_digit_field(rest, *width)
                    .filter(|(value, _)| *value <= 59)
                    .ok_or_else(|| second_failure(rest, *width, consumed))?;
                wall.second = value;
                rest = step(rest, taken, &mut consumed);
            }
            JavaToken::Fraction { width } => {
                let (value, taken) = take_digits(rest, *width, *width).ok_or(at(consumed))?;
                wall.nano = fraction_nanos(value, *width).ok_or(at(consumed))?;
                rest = step(rest, taken, &mut consumed);
            }
            JavaToken::AmPm => {
                afternoon = Some(take_meridiem(rest).ok_or(at(consumed))?);
                rest = step(rest, 2, &mut consumed);
            }
            JavaToken::Literal(expected) => {
                if !rest.starts_with(*expected) {
                    return Err(at(consumed));
                }
                rest = step(rest, expected.len_utf8(), &mut consumed);
            }
            JavaToken::Unsupported => {
                return Err(at(consumed));
            }
        }
    }
    if !rest.is_empty() {
        return Err(at(consumed));
    }
    if let Some(post_meridiem) = afternoon {
        wall.hour = match (wall.hour, post_meridiem) {
            (12, false) => 0,
            (hour, true) if hour < 12 => hour + 12,
            (hour, _) => hour,
        };
    }
    if wall.to_naive().is_none() {
        return Err(day_out_of_month(wall.day));
    }
    Ok(wall)
}

pub(crate) fn parse_wall_or_null(
    text: &str,
    pattern: &ParsedPattern,
    ansi: bool,
    try_name: &str,
) -> Result<Option<NaiveDateTime>> {
    match parse_wall_text(text, pattern) {
        Ok(wall) => match wall.to_naive() {
            Some(naive) => Ok(Some(naive)),
            None => {
                if ansi {
                    Err(DataFusionError::Execution(cannot_parse_text(
                        text,
                        &day_out_of_month(wall.day),
                        try_name,
                    )))
                } else {
                    Ok(None)
                }
            }
        },
        Err(failure) => {
            if ansi {
                Err(DataFusionError::Execution(cannot_parse_text(
                    text, &failure, try_name,
                )))
            } else {
                Ok(None)
            }
        }
    }
}

fn at(position: usize) -> WallFailure {
    WallFailure::AtIndex { position }
}

fn step<'text>(rest: &'text str, taken: usize, consumed: &mut usize) -> &'text str {
    *consumed += taken;
    &rest[taken..]
}

fn pattern_letter_token(letter: char, width: usize) -> JavaToken {
    match letter {
        'y' | 'u' => {
            if width == 2 {
                JavaToken::Year { width: 2 }
            } else {
                JavaToken::Year {
                    width: width.max(4),
                }
            }
        }
        'M' => match width {
            1 | 2 => JavaToken::MonthNumber { width },
            3 => JavaToken::MonthName { full: false },
            4 => JavaToken::MonthName { full: true },
            _ => JavaToken::Unsupported,
        },
        'd' => match width {
            1 | 2 => JavaToken::Day { width },
            _ => JavaToken::Unsupported,
        },
        'H' => match width {
            1 | 2 => JavaToken::Hour24 { width },
            _ => JavaToken::Unsupported,
        },
        'h' => match width {
            1 | 2 => JavaToken::Hour12 { width },
            _ => JavaToken::Unsupported,
        },
        'm' => match width {
            1 | 2 => JavaToken::Minute { width },
            _ => JavaToken::Unsupported,
        },
        's' => match width {
            1 | 2 => JavaToken::Second { width },
            _ => JavaToken::Unsupported,
        },
        'S' => {
            if (1..=9).contains(&width) {
                JavaToken::Fraction { width }
            } else {
                JavaToken::Unsupported
            }
        }
        _ => JavaToken::AmPm,
    }
}

fn refuse_week_year(pattern: &str) -> Result<()> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut index = 0;
    let mut in_quote = false;
    while index < chars.len() {
        let current = chars[index];
        if current == '\'' {
            if chars.get(index + 1) == Some(&'\'') {
                index += 2;
                continue;
            }
            in_quote = !in_quote;
            index += 1;
            continue;
        }
        if !in_quote && current == 'Y' {
            return Err(DataFusionError::Execution(week_year_refusal_text()));
        }
        index += 1;
    }
    Ok(())
}

fn refuse_illegal_pattern(pattern: &str) -> Result<()> {
    let mut in_quote = false;
    for byte in pattern.bytes() {
        if byte == b'\'' {
            in_quote = !in_quote;
            continue;
        }
        if in_quote {
            continue;
        }
        if byte.is_ascii_alphabetic() && !LEGAL_DATETIME_LETTERS.contains(&byte) {
            return Err(DataFusionError::Execution(format!(
                "[INVALID_DATETIME_PATTERN.ILLEGAL_CHARACTER] Unrecognized datetime \
                 pattern: {pattern}. Illegal pattern character found in datetime pattern: \
                 {}. Please provide legal character. SQLSTATE: 22007",
                byte as char
            )));
        }
    }
    Ok(())
}

const LEGAL_DATETIME_LETTERS: &[u8] = b"GyYuQqMLwdDFeEcahHkKmsSAVnNzZOXx";

fn take_digits(text: &str, min_width: usize, max_width: usize) -> Option<(u32, usize)> {
    let mut count = 0usize;
    for byte in text.bytes() {
        if !byte.is_ascii_digit() || count == max_width {
            break;
        }
        count += 1;
    }
    if count < min_width {
        return None;
    }
    Some((text[..count].parse().ok()?, count))
}

fn take_year(text: &str, width: usize) -> Option<(i32, usize)> {
    if width == 2 {
        let (value, taken) = take_digits(text, 2, 2)?;
        Some((2000 + i32::try_from(value).ok()?, taken))
    } else {
        let (value, taken) = take_digits(text, width, width)?;
        Some((i32::try_from(value).ok()?, taken))
    }
}

fn take_two_digit_field(text: &str, width: usize) -> Option<(u32, usize)> {
    if width == 1 {
        take_digits(text, 1, 2)
    } else {
        take_digits(text, 2, 2)
    }
}

fn take_meridiem(text: &str) -> Option<bool> {
    let marker = text.get(..2)?;
    if marker.eq_ignore_ascii_case("am") {
        Some(false)
    } else if marker.eq_ignore_ascii_case("pm") {
        Some(true)
    } else {
        None
    }
}

fn fraction_nanos(value: u32, width: usize) -> Option<u32> {
    value.checked_mul(10u32.pow(u32::try_from(9 - width).ok()?))
}

fn month_failure(text: &str, width: usize, consumed: usize) -> WallFailure {
    ranged_failure(text, width, consumed, "MonthOfYear", 1, 12)
}

fn day_failure(text: &str, width: usize, consumed: usize) -> WallFailure {
    ranged_failure(text, width, consumed, "DayOfMonth", 1, 31)
}

fn day_out_of_month(day: u32) -> WallFailure {
    WallFailure::InvalidValue {
        detail: format!("Invalid value for DayOfMonth (valid values 1 - 28/31): {day}"),
    }
}

fn hour_failure(text: &str, width: usize, consumed: usize, twelve_hour: bool) -> WallFailure {
    if twelve_hour {
        ranged_failure(text, width, consumed, "ClockHourOfAmPm", 1, 12)
    } else {
        ranged_failure(text, width, consumed, "HourOfDay", 0, 23)
    }
}

fn minute_failure(text: &str, width: usize, consumed: usize) -> WallFailure {
    ranged_failure(text, width, consumed, "MinuteOfHour", 0, 59)
}

fn second_failure(text: &str, width: usize, consumed: usize) -> WallFailure {
    ranged_failure(text, width, consumed, "SecondOfMinute", 0, 59)
}

fn ranged_failure(
    text: &str,
    width: usize,
    consumed: usize,
    field: &str,
    low: u32,
    high: u32,
) -> WallFailure {
    match take_two_digit_field(text, width) {
        Some((value, _)) => WallFailure::InvalidValue {
            detail: format!("Invalid value for {field} (valid values {low} - {high}): {value}"),
        },
        None => at(consumed),
    }
}

const MONTH_ABBREV: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
const MONTH_FULL: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

fn take_month_name(text: &str, full: bool) -> Option<(u32, usize)> {
    let names: &[&str] = if full { &MONTH_FULL } else { &MONTH_ABBREV };
    let mut best: Option<(u32, usize)> = None;
    for (index, name) in names.iter().enumerate() {
        if text.len() >= name.len() && text[..name.len()].eq_ignore_ascii_case(name) {
            let month = u32::try_from(index + 1).ok()?;
            let taken = name.len();
            if best.is_none_or(|(_, prior)| taken > prior) {
                best = Some((month, taken));
            }
        }
    }
    best
}

fn utf8_column(array: &ArrayRef) -> Result<StringArray> {
    let casted = cast(array.as_ref(), &DataType::Utf8)?;
    casted
        .as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("utf8 cast did not yield Utf8".to_string()))
}

fn session_zone(options: &ConfigOptions) -> Result<Tz> {
    parse_session_zone(session_time_zone_from_options(options))
}

fn wall_micros(wall: NaiveDateTime) -> i64 {
    wall.and_utc().timestamp_micros()
}

fn localize_or_gap(
    naive: NaiveDateTime,
    zone: Tz,
    text: &str,
    ansi: bool,
    try_name: &str,
) -> Result<Option<i64>> {
    match localize_wall_micros_in_zone(wall_micros(naive), zone) {
        Some(instant) => Ok(Some(instant)),
        None => {
            if ansi {
                Err(DataFusionError::Execution(cannot_parse_text(
                    text,
                    &at(text.len()),
                    try_name,
                )))
            } else {
                Ok(None)
            }
        }
    }
}

pub(crate) fn to_date_with_format(
    input: &ArrayRef,
    formats: &ArrayRef,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    if !matches!(
        input.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        let dates = invoke_local_dates(input, args.config_options.as_ref())?;
        return Ok(ColumnarValue::Array(dates));
    }
    let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
    let texts = utf8_column(input)?;
    let format_texts = utf8_column(formats)?;
    let mut builder = Date32Array::builder(texts.len());
    match plan_format_column(&format_texts)? {
        FormatPlan::AllNull => {
            for _ in 0..texts.len() {
                builder.append_null();
            }
        }
        FormatPlan::Shared(pattern) => {
            for row in 0..texts.len() {
                if texts.is_null(row) || format_texts.is_null(row) {
                    builder.append_null();
                    continue;
                }
                match parse_wall_or_null(texts.value(row), &pattern, ansi, "try_to_date")? {
                    Some(naive) => {
                        builder.append_value(Date32Type::from_naive_date(naive.date()));
                    }
                    None => builder.append_null(),
                }
            }
        }
        FormatPlan::PerRow => {
            for row in 0..texts.len() {
                if texts.is_null(row) || format_texts.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let pattern = compile_java_pattern(format_texts.value(row))?;
                match parse_wall_or_null(texts.value(row), &pattern, ansi, "try_to_date")? {
                    Some(naive) => {
                        builder.append_value(Date32Type::from_naive_date(naive.date()));
                    }
                    None => builder.append_null(),
                }
            }
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

pub(crate) fn stamps_with_format_column(
    input: &ArrayRef,
    formats: &ArrayRef,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
    let zone = session_zone(args.config_options.as_ref())?;
    let texts = utf8_column(input)?;
    let format_texts = utf8_column(formats)?;
    let mut builder = TimestampMicrosecondArray::builder(texts.len());
    match plan_format_column(&format_texts)? {
        FormatPlan::AllNull => {
            for _ in 0..texts.len() {
                builder.append_null();
            }
        }
        FormatPlan::Shared(pattern) => {
            for row in 0..texts.len() {
                if texts.is_null(row) || format_texts.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let text = texts.value(row);
                match parse_wall_or_null(text, &pattern, ansi, "try_to_timestamp")? {
                    Some(naive) => {
                        match localize_or_gap(naive, zone, text, ansi, "try_to_timestamp")? {
                            Some(instant) => builder.append_value(instant),
                            None => builder.append_null(),
                        }
                    }
                    None => builder.append_null(),
                }
            }
        }
        FormatPlan::PerRow => {
            for row in 0..texts.len() {
                if texts.is_null(row) || format_texts.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let text = texts.value(row);
                let pattern = compile_java_pattern(format_texts.value(row))?;
                match parse_wall_or_null(text, &pattern, ansi, "try_to_timestamp")? {
                    Some(naive) => {
                        match localize_or_gap(naive, zone, text, ansi, "try_to_timestamp")? {
                            Some(instant) => builder.append_value(instant),
                            None => builder.append_null(),
                        }
                    }
                    None => builder.append_null(),
                }
            }
        }
    }
    Ok(ColumnarValue::Array(Arc::new(
        builder.finish().with_timezone("UTC"),
    )))
}

pub(crate) fn unix_seconds_with_format(
    input: &ArrayRef,
    formats: &ArrayRef,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
    let zone = session_zone(args.config_options.as_ref())?;
    if matches!(input.data_type(), DataType::Timestamp(_, _)) {
        let seconds = crate::timestamp_cast::unix_seconds_from_timestamp(input.as_ref())?;
        return Ok(ColumnarValue::Array(Arc::new(seconds)));
    }
    if matches!(input.data_type(), DataType::Null) {
        return Ok(ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            None;
            input
                .len(
                )
        ]))));
    }
    if matches!(input.data_type(), DataType::Date32 | DataType::Date64) {
        return Ok(ColumnarValue::Array(Arc::new(unix_seconds_from_dates(
            input, zone, ansi,
        )?)));
    }
    let texts = utf8_column(input)?;
    let format_texts = utf8_column(formats)?;
    let mut builder = Int64Array::builder(texts.len());
    match plan_format_column(&format_texts)? {
        FormatPlan::AllNull => {
            for _ in 0..texts.len() {
                builder.append_null();
            }
        }
        FormatPlan::Shared(pattern) => {
            for row in 0..texts.len() {
                if texts.is_null(row) || format_texts.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let text = texts.value(row);
                match parse_wall_or_null(text, &pattern, ansi, "try_to_timestamp")? {
                    Some(naive) => {
                        match localize_or_gap(naive, zone, text, ansi, "try_to_timestamp")? {
                            Some(instant) => {
                                builder.append_value(instant.div_euclid(1_000_000));
                            }
                            None => builder.append_null(),
                        }
                    }
                    None => builder.append_null(),
                }
            }
        }
        FormatPlan::PerRow => {
            for row in 0..texts.len() {
                if texts.is_null(row) || format_texts.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let text = texts.value(row);
                let pattern = compile_java_pattern(format_texts.value(row))?;
                match parse_wall_or_null(text, &pattern, ansi, "try_to_timestamp")? {
                    Some(naive) => {
                        match localize_or_gap(naive, zone, text, ansi, "try_to_timestamp")? {
                            Some(instant) => {
                                builder.append_value(instant.div_euclid(1_000_000));
                            }
                            None => builder.append_null(),
                        }
                    }
                    None => builder.append_null(),
                }
            }
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn unix_seconds_from_dates(input: &ArrayRef, zone: Tz, ansi: bool) -> Result<Int64Array> {
    let days = cast(input.as_ref(), &DataType::Date32)?;
    let days = days
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| DataFusionError::Internal("date cast did not yield Date32".to_string()))?;
    let mut builder = Int64Array::builder(days.len());
    for row in 0..days.len() {
        if days.is_null(row) {
            builder.append_null();
            continue;
        }
        match Date32Type::to_naive_date_opt(days.value(row)) {
            Some(date) => {
                let wall = date
                    .and_hms_opt(0, 0, 0)
                    .and_then(|midnight| localize_wall_micros_in_zone(wall_micros(midnight), zone));
                if let Some(instant) = wall {
                    builder.append_value(instant.div_euclid(1_000_000));
                } else if ansi {
                    return Err(DataFusionError::Execution(cannot_parse_text(
                        &date.to_string(),
                        &at(date.to_string().len()),
                        "try_to_timestamp",
                    )));
                } else {
                    builder.append_null();
                }
            }
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(text: &str) -> ParsedPattern {
        compile_java_pattern(text).expect("fixture pattern compiles")
    }

    fn wall(text: &str, format: &str) -> NaiveDateTime {
        parse_wall_text(text, &pattern(format))
            .expect("fixture row parses")
            .to_naive()
            .expect("fixture wall is a real datetime")
    }

    #[test]
    fn fixture_patterns_parse_to_the_recorded_walls() {
        assert_eq!(
            wall("31/12/2016 10:30", "dd/MM/yyyy HH:mm").to_string(),
            "2016-12-31 10:30:00"
        );
        assert_eq!(
            wall("2016-12-31 00:12:00", "yyyy-MM-dd HH:mm:ss").to_string(),
            "2016-12-31 00:12:00"
        );
        assert_eq!(
            wall("2016-12-31 10:30:15.123", "yyyy-MM-dd HH:mm:ss.SSS").to_string(),
            "2016-12-31 10:30:15.123"
        );
        assert_eq!(
            wall("12/31/2016 10:30 PM", "MM/dd/yyyy hh:mm a").to_string(),
            "2016-12-31 22:30:00"
        );
        assert_eq!(
            wall("12/31/2016 12:00 AM", "MM/dd/yyyy hh:mm a").to_string(),
            "2016-12-31 00:00:00"
        );
        assert_eq!(
            wall("12/31/2016 12:00 PM", "MM/dd/yyyy hh:mm a").to_string(),
            "2016-12-31 12:00:00"
        );
        assert_eq!(
            wall("2016-12-31", "yyyy-MM-dd").to_string(),
            "2016-12-31 00:00:00"
        );
        assert_eq!(
            wall("2016-12-31T10:30:00", "yyyy-MM-dd'T'HH:mm:ss").to_string(),
            "2016-12-31 10:30:00"
        );
        assert_eq!(
            wall("Dec 31 2016", "MMM dd yyyy").to_string(),
            "2016-12-31 00:00:00"
        );
        assert_eq!(
            wall("dec 31 2016", "MMM dd yyyy").to_string(),
            "2016-12-31 00:00:00"
        );
        assert_eq!(
            wall("31.12.2016", "dd.MM.yyyy").to_string(),
            "2016-12-31 00:00:00"
        );
        assert_eq!(
            wall("20161231", "yyyyMMdd").to_string(),
            "2016-12-31 00:00:00"
        );
    }

    #[test]
    fn bare_year_defaults_to_january_first_midnight() {
        assert_eq!(wall("2024", "yyyy").to_string(), "2024-01-01 00:00:00");
    }

    #[test]
    fn two_digit_year_pivots_on_two_thousand() {
        assert_eq!(
            wall("16-12-31", "yy-MM-dd").to_string(),
            "2016-12-31 00:00:00"
        );
    }

    #[test]
    fn single_letter_fields_accept_short_values() {
        assert_eq!(
            wall("2016-1-2 3:04:05", "yyyy-M-d H:mm:ss").to_string(),
            "2016-01-02 03:04:05"
        );
    }

    #[test]
    fn partial_pattern_reports_the_consumed_index() {
        let failure = parse_wall_text("2016-12-31", &pattern("yyyy-MM-dd HH"))
            .expect_err("a short row cannot satisfy the pattern");
        match failure {
            WallFailure::AtIndex { position } => assert_eq!(position, 10),
            WallFailure::InvalidValue { detail } => panic!("wrong failure: {detail}"),
        }
        let text = cannot_parse_text("2016-12-31", &failure, "try_to_timestamp");
        assert_eq!(
            text,
            "[CANNOT_PARSE_TIMESTAMP] Text '2016-12-31' could not be parsed at index 10. Use \
             `try_to_timestamp` to tolerate invalid input string and return NULL instead. \
             SQLSTATE: 22007"
        );
    }

    #[test]
    fn garbage_reports_index_zero_with_the_try_hint() {
        let failure =
            parse_wall_text("garbage", &pattern("yyyy-MM-dd")).expect_err("garbage cannot parse");
        match failure {
            WallFailure::AtIndex { position } => assert_eq!(position, 0),
            WallFailure::InvalidValue { detail } => panic!("wrong failure: {detail}"),
        }
        let text = cannot_parse_text("garbage", &failure, "try_to_date");
        assert!(text.contains("could not be parsed at index 0"));
        assert!(text.contains("`try_to_date`"));
    }

    #[test]
    fn month_thirteen_names_month_of_year() {
        let failure = parse_wall_text("2016-13-31", &pattern("yyyy-MM-dd"))
            .expect_err("month 13 cannot parse");
        match &failure {
            WallFailure::InvalidValue { detail } => assert_eq!(
                detail,
                "Invalid value for MonthOfYear (valid values 1 - 12): 13"
            ),
            WallFailure::AtIndex { position } => panic!("wrong failure at {position}"),
        }
        assert_eq!(
            cannot_parse_text("2016-13-31", &failure, "try_to_date"),
            "[CANNOT_PARSE_TIMESTAMP] Text '2016-13-31' could not be parsed: Invalid value \
             for MonthOfYear (valid values 1 - 12): 13. Use `try_to_date` to tolerate \
             invalid input string and return NULL instead. SQLSTATE: 22007"
        );
    }

    #[test]
    fn impossible_calendar_day_fails_after_the_fields() {
        parse_wall_text("2016-02-30", &pattern("yyyy-MM-dd"))
            .expect_err("February 30 is not a date");
    }

    #[test]
    fn trailing_input_fails_at_the_pattern_end() {
        let failure = parse_wall_text("2016-12-31!", &pattern("yyyy-MM-dd"))
            .expect_err("trailing input cannot parse");
        match failure {
            WallFailure::AtIndex { position } => assert_eq!(position, 10),
            WallFailure::InvalidValue { detail } => panic!("wrong failure: {detail}"),
        }
    }

    #[test]
    fn week_year_is_an_upgrade_refusal_not_a_parse_result() {
        let error = compile_java_pattern("YYYY-MM-dd").expect_err("week year refuses");
        assert!(error.to_string().contains(&week_year_refusal_text()));
        assert!(
            week_year_refusal_text()
                .contains("[INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION]")
        );
        compile_java_pattern("yyyy-MM-dd'T'HH:mm:ss").expect("quoted T is a literal");
    }

    #[test]
    fn constant_format_column_compiles_once() {
        let formats = StringArray::from(vec![Some("yyyy-MM-dd"), Some("yyyy-MM-dd"), None]);
        match plan_format_column(&formats).expect("constant plan") {
            FormatPlan::Shared(compiled) => assert_eq!(compiled.tokens().len(), 5),
            FormatPlan::AllNull | FormatPlan::PerRow => panic!("constant formats share one plan"),
        }
        let nulls = StringArray::from(vec![None::<String>, None]);
        match plan_format_column(&nulls).expect("null plan") {
            FormatPlan::AllNull => (),
            FormatPlan::Shared(_) | FormatPlan::PerRow => panic!("all-null formats stay null"),
        }
        let mixed = StringArray::from(vec![Some("yyyy-MM-dd"), Some("dd.MM.yyyy")]);
        match plan_format_column(&mixed).expect("mixed plan") {
            FormatPlan::PerRow => (),
            FormatPlan::Shared(_) | FormatPlan::AllNull => panic!("mixed formats parse per row"),
        }
    }
}
