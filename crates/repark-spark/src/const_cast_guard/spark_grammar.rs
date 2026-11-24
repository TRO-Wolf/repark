use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, TimeUnit, i256};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};

const MAX_DECIMAL_PRECISION: i128 = 38;
const MAX_OFFSET_HOURS: i32 = 18;
const MAX_MINUTE_OR_SECOND: i32 = 59;
const MIN_YEAR_DIGITS: usize = 4;
const MAX_YEAR_DIGITS: usize = 6;
const MAX_FIELD_DIGITS: usize = 2;
const FRACTION_DIGITS: usize = 6;
const MICROS_PER_SECOND: i64 = 1_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

const SHORT_ZONE_IDS: [(&str, &str); 28] = [
    ("ACT", "Australia/Darwin"),
    ("AET", "Australia/Sydney"),
    ("AGT", "America/Argentina/Buenos_Aires"),
    ("ART", "Africa/Cairo"),
    ("AST", "America/Anchorage"),
    ("BET", "America/Sao_Paulo"),
    ("BST", "Asia/Dhaka"),
    ("CAT", "Africa/Harare"),
    ("CNT", "America/St_Johns"),
    ("CST", "America/Chicago"),
    ("CTT", "Asia/Shanghai"),
    ("EAT", "Africa/Addis_Ababa"),
    ("ECT", "Europe/Paris"),
    ("EST", "-05:00"),
    ("HST", "-10:00"),
    ("IET", "America/Indiana/Indianapolis"),
    ("IST", "Asia/Kolkata"),
    ("JST", "Asia/Tokyo"),
    ("MIT", "Pacific/Apia"),
    ("MST", "-07:00"),
    ("NET", "Asia/Yerevan"),
    ("NST", "Pacific/Auckland"),
    ("PLT", "Asia/Karachi"),
    ("PNT", "America/Phoenix"),
    ("PRT", "America/Puerto_Rico"),
    ("PST", "America/Los_Angeles"),
    ("SST", "Pacific/Guadalcanal"),
    ("VST", "Asia/Ho_Chi_Minh"),
];

pub(super) enum SparkDecimalOutcome {
    Ok(i128),
    InvalidInput,
    OutOfRange,
    OutOfSupportedRange,
}

pub(super) enum StringCastOutcome {
    Unhandled,
    Valid(Option<ScalarValue>),
    InvalidInput,
    OutOfRange(u8, i8),
    OutOfSupportedRange,
}

pub(super) fn eval_string_cast_with_grammar(
    text: &str,
    target: &DataType,
    ansi_enabled: bool,
) -> Option<Result<Option<ScalarValue>>> {
    match parse_string_cast(text, target) {
        StringCastOutcome::Unhandled => None,
        StringCastOutcome::Valid(scalar) => Some(Ok(scalar)),
        StringCastOutcome::InvalidInput => Some(if ansi_enabled {
            Err(invalid_input_error(
                text,
                "STRING",
                &spark_type_name(target),
            ))
        } else {
            Ok(None)
        }),
        StringCastOutcome::OutOfRange(precision, scale) => Some(if ansi_enabled {
            Err(DataFusionError::Execution(format!(
                "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  {} cannot be represented as \
                 Decimal({precision}, {scale}). If necessary set \"spark.sql.ansi.enabled\" to \
                 \"false\" to bypass this error, and return NULL instead. SQLSTATE: 22003",
                text.trim()
            )))
        } else {
            Ok(None)
        }),
        StringCastOutcome::OutOfSupportedRange => Some(if ansi_enabled {
            Err(DataFusionError::Execution(format!(
                "[NUMERIC_OUT_OF_SUPPORTED_RANGE] The value {} cannot be interpreted as a numeric \
                 since it has more than 38 digits. SQLSTATE: 22003",
                text.trim()
            )))
        } else {
            Ok(None)
        }),
    }
}

pub(super) fn invalid_input_error(text: &str, from: &str, to: &str) -> DataFusionError {
    let escaped = text.replace('\\', "\\\\").replace('\'', "\\'");
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{escaped}' of the type \"{from}\" cannot be cast to \
         \"{to}\" because it is malformed. Correct the value as per the syntax, or change its \
         target type. Use `try_cast` to tolerate malformed input and return NULL instead. \
         SQLSTATE: 22018"
    ))
}

pub(super) fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP_NTZ".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_type_name(field.data_type()))
        }
        DataType::Struct(_) => "STRUCT".to_string(),
        DataType::Map(_, _) => "MAP".to_string(),
        other => format!("{other}"),
    }
}

pub(super) fn parse_string_cast(text: &str, target: &DataType) -> StringCastOutcome {
    match target {
        DataType::Date32 | DataType::Date64 => match parse_spark_date(text) {
            Some(scalar) => StringCastOutcome::Valid(Some(scalar)),
            None => StringCastOutcome::InvalidInput,
        },
        DataType::Timestamp(TimeUnit::Microsecond, None) => match parse_spark_timestamp_ntz(text) {
            Some(scalar) => StringCastOutcome::Valid(Some(scalar)),
            None => StringCastOutcome::InvalidInput,
        },
        DataType::Float64 => match parse_spark_double(text) {
            Some(value) => StringCastOutcome::Valid(Some(ScalarValue::Float64(Some(value)))),
            None => StringCastOutcome::InvalidInput,
        },
        DataType::Float32 => match parse_spark_float(text) {
            Some(value) => StringCastOutcome::Valid(Some(ScalarValue::Float32(Some(value)))),
            None => StringCastOutcome::InvalidInput,
        },
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale)
            if *precision <= 38 =>
        {
            match parse_spark_decimal(text, *precision, *scale) {
                SparkDecimalOutcome::Ok(value) => {
                    StringCastOutcome::Valid(decimal_scalar(value, target))
                }
                SparkDecimalOutcome::InvalidInput => StringCastOutcome::InvalidInput,
                SparkDecimalOutcome::OutOfRange => {
                    StringCastOutcome::OutOfRange(*precision, *scale)
                }
                SparkDecimalOutcome::OutOfSupportedRange => StringCastOutcome::OutOfSupportedRange,
            }
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            match parse_spark_integral(text, target) {
                Some(scalar) => StringCastOutcome::Valid(Some(scalar)),
                None => StringCastOutcome::InvalidInput,
            }
        }
        _ => StringCastOutcome::Unhandled,
    }
}

fn trim_numeric(text: &str) -> &str {
    text.trim_matches(|ch: char| ch <= ' ')
}

fn is_datetime_trimmable(byte: u8) -> bool {
    byte <= b' ' || byte == 0x7f
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn valid_ymd(year: i64, month: i64, day: i64) -> Option<(u32, u32)> {
    let m = u32::try_from(month).ok()?;
    let d = u32::try_from(day).ok()?;
    if !(1..=12).contains(&m) {
        return None;
    }
    let max_day = days_in_month(year, m);
    if d < 1 || d > max_day {
        return None;
    }
    Some((m, d))
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let adjusted_year = if month <= 2 { year - 1 } else { year };
    let era = adjusted_year.div_euclid(400);
    let yoe = adjusted_year.rem_euclid(400);
    let m = i64::from(month);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub(super) fn parse_spark_date(text: &str) -> Option<ScalarValue> {
    let bytes = text.as_bytes();
    let mut pos = bytes.iter().position(|b| !is_datetime_trimmable(*b))?;
    let end = bytes.iter().rposition(|b| !is_datetime_trimmable(*b))? + 1;

    let mut negative_year = false;
    if bytes[pos] == b'-' || bytes[pos] == b'+' {
        negative_year = bytes[pos] == b'-';
        pos += 1;
    }

    let mut values = [0_i64, 1, 1];
    let mut segment = 0_usize;
    let mut current = 0_i64;
    let mut digits = 0_usize;

    while pos < end {
        let byte = bytes[pos];
        if byte.is_ascii_digit() {
            current = current
                .saturating_mul(10)
                .saturating_add(i64::from(byte - b'0'));
            digits += 1;
        } else if byte == b'-' && segment < 2 {
            if !valid_date_segment_digits(segment, digits) {
                return None;
            }
            values[segment] = current;
            segment += 1;
            current = 0;
            digits = 0;
        } else if (byte == b' ' || byte == b'T') && segment == 2 {
            break;
        } else {
            return None;
        }
        pos += 1;
    }

    if !valid_date_segment_digits(segment, digits) {
        return None;
    }
    values[segment] = current;

    let year = if negative_year { -values[0] } else { values[0] };
    let (month, day) = valid_ymd(year, values[1], values[2])?;
    let days = i32::try_from(days_from_civil(year, month, day)).ok()?;
    Some(ScalarValue::Date32(Some(days)))
}

fn valid_date_segment_digits(segment: usize, digits: usize) -> bool {
    if segment == 0 {
        (MIN_YEAR_DIGITS..=MAX_YEAR_DIGITS).contains(&digits)
    } else {
        (1..=MAX_FIELD_DIGITS).contains(&digits)
    }
}

struct ParsedTimestamp<'a> {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    micros: i64,
    zone: Option<&'a str>,
    just_time: bool,
}

const TS_YEAR: usize = 0;
const TS_DAY: usize = 2;
const TS_HOUR: usize = 3;
const TS_MINUTE: usize = 4;
const TS_SECOND: usize = 5;
const TS_FRACTION: usize = 6;
const TS_ZONE: usize = 7;

struct TimestampParser<'a> {
    values: [i64; 9],
    seg: usize,
    cur: i64,
    digits: usize,
    frac_digits: usize,
    just_time: bool,
    zone: Option<&'a str>,
    year_is_negative: Option<bool>,
}

impl<'a> TimestampParser<'a> {
    fn commit_segment(&mut self) -> Option<()> {
        if !valid_ts_segment_digits(self.seg, self.digits) {
            return None;
        }
        self.values[self.seg] = self.cur;
        self.seg += 1;
        self.cur = 0;
        self.digits = 0;
        Some(())
    }

    fn feed_separator(
        &mut self,
        bytes: &'a [u8],
        pos: &mut usize,
        start: usize,
        end: usize,
    ) -> Option<()> {
        let byte = bytes[*pos];
        if *pos == start && byte == b'T' {
            self.just_time = true;
            self.seg = TS_HOUR;
        } else if self.seg < TS_DAY {
            if byte == b'-' {
                self.commit_segment()?;
            } else if self.seg == TS_YEAR && byte == b':' && self.year_is_negative.is_none() {
                if !(1..=MAX_FIELD_DIGITS).contains(&self.digits) {
                    return None;
                }
                self.just_time = true;
                self.values[TS_HOUR] = self.cur;
                self.seg = TS_MINUTE;
                self.cur = 0;
                self.digits = 0;
            } else {
                return None;
            }
        } else if self.seg == TS_DAY {
            if byte != b' ' && byte != b'T' {
                return None;
            }
            self.commit_segment()?;
        } else if self.seg == TS_HOUR || self.seg == TS_MINUTE {
            if byte != b':' {
                return None;
            }
            self.commit_segment()?;
        } else if self.seg == TS_SECOND || self.seg == TS_FRACTION {
            let opens_fraction = byte == b'.' && self.seg == TS_SECOND;
            self.commit_segment()?;
            if !opens_fraction {
                self.zone = Some(std::str::from_utf8(&bytes[*pos..end]).ok()?);
                *pos = end - 1;
            }
            if self.seg == TS_FRACTION && byte != b'.' {
                self.seg += 1;
            }
        } else {
            return None;
        }
        Some(())
    }
}

fn parse_timestamp_grammar(text: &str) -> Option<ParsedTimestamp<'_>> {
    let bytes = text.as_bytes();
    let mut pos = bytes.iter().position(|b| !is_datetime_trimmable(*b))?;
    let end = bytes.iter().rposition(|b| !is_datetime_trimmable(*b))? + 1;
    let start = pos;

    let mut state = TimestampParser {
        values: [1_i64, 1, 1, 0, 0, 0, 0, 0, 0],
        seg: TS_YEAR,
        cur: 0,
        digits: 0,
        frac_digits: 0,
        just_time: false,
        zone: None,
        year_is_negative: None,
    };

    if bytes[pos] == b'-' || bytes[pos] == b'+' {
        state.year_is_negative = Some(bytes[pos] == b'-');
        pos += 1;
    }

    while pos < end {
        let byte = bytes[pos];
        if byte.is_ascii_digit() {
            if state.seg == TS_FRACTION {
                state.frac_digits += 1;
            }
            if state.seg != TS_FRACTION || state.digits < FRACTION_DIGITS {
                state.cur = state
                    .cur
                    .saturating_mul(10)
                    .saturating_add(i64::from(byte - b'0'));
            }
            state.digits += 1;
        } else {
            state.feed_separator(bytes, &mut pos, start, end)?;
        }
        pos += 1;
    }

    if !valid_ts_segment_digits(state.seg, state.digits) {
        return None;
    }
    *state.values.get_mut(state.seg)? = state.cur;
    if state.seg > TS_ZONE {
        return None;
    }

    let mut micros = state.values[TS_FRACTION];
    while state.frac_digits < FRACTION_DIGITS {
        micros *= 10;
        state.frac_digits += 1;
    }

    Some(ParsedTimestamp {
        year: if state.year_is_negative == Some(true) {
            -state.values[0]
        } else {
            state.values[0]
        },
        month: state.values[1],
        day: state.values[2],
        hour: state.values[3],
        minute: state.values[4],
        second: state.values[5],
        micros,
        zone: state.zone,
        just_time: state.just_time,
    })
}

fn valid_ts_segment_digits(segment: usize, digits: usize) -> bool {
    match segment {
        0 => (MIN_YEAR_DIGITS..=MAX_YEAR_DIGITS).contains(&digits),
        6 => true,
        7 => digits <= MAX_FIELD_DIGITS,
        1..=5 => (1..=MAX_FIELD_DIGITS).contains(&digits),
        _ => false,
    }
}

fn validate_hms(hour: i64, minute: i64, second: i64) -> bool {
    (0..=23).contains(&hour) && (0..=59).contains(&minute) && (0..=59).contains(&second)
}

fn is_valid_spark_zone(raw: &str) -> bool {
    let trimmed = trim_numeric(raw);
    let formatted = pad_single_minute(&pad_single_hour(trimmed));
    let aliased = SHORT_ZONE_IDS
        .iter()
        .find(|(short, _)| *short == formatted)
        .map_or(formatted.as_str(), |(_, full)| full);
    is_valid_java_zone(aliased)
}

fn is_sign(byte: u8) -> bool {
    byte == b'+' || byte == b'-'
}

fn pad_single_hour(zone: &str) -> String {
    let bytes = zone.as_bytes();
    let found = bytes
        .windows(3)
        .position(|w| is_sign(w[0]) && w[1].is_ascii_digit() && w[2] == b':');
    match found {
        Some(sign) => format!("{}0{}", &zone[..=sign], &zone[sign + 1..]),
        None => zone.to_string(),
    }
}

fn pad_single_minute(zone: &str) -> String {
    let bytes = zone.as_bytes();
    let Some(start) = bytes.len().checked_sub(5) else {
        return zone.to_string();
    };
    let tail = &bytes[start..];
    if is_sign(tail[0])
        && tail[1].is_ascii_digit()
        && tail[2].is_ascii_digit()
        && tail[3] == b':'
        && tail[4].is_ascii_digit()
    {
        format!("{}0{}", &zone[..start + 4], &zone[start + 4..])
    } else {
        zone.to_string()
    }
}

fn is_valid_java_zone(zone: &str) -> bool {
    if zone.len() <= 1 || zone.starts_with('+') || zone.starts_with('-') {
        return is_valid_java_offset(zone);
    }
    if zone.starts_with("UTC") || zone.starts_with("GMT") {
        return is_valid_prefixed_zone(zone, 3);
    }
    if zone.starts_with("UT") {
        return is_valid_prefixed_zone(zone, 2);
    }
    is_valid_region_zone(zone)
}

fn is_valid_prefixed_zone(zone: &str, prefix_len: usize) -> bool {
    let rest = &zone[prefix_len..];
    match rest.as_bytes().first() {
        None => true,
        Some(byte) if is_sign(*byte) => is_valid_java_offset(rest),
        Some(_) => is_valid_region_zone(zone),
    }
}

fn is_valid_region_zone(zone: &str) -> bool {
    let bytes = zone.as_bytes();
    let well_formed = bytes.len() >= 2
        && bytes[0].is_ascii_alphabetic()
        && bytes[1..].iter().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'~' | b'/' | b'.' | b'_' | b'+' | b'-')
        });
    well_formed && zone.parse::<Tz>().is_ok()
}

fn two_digits(bytes: &[u8], pos: usize) -> Option<i32> {
    let tens = *bytes.get(pos)?;
    let units = *bytes.get(pos + 1)?;
    if !tens.is_ascii_digit() || !units.is_ascii_digit() {
        return None;
    }
    Some(i32::from(tens - b'0') * 10 + i32::from(units - b'0'))
}

fn two_digits_colon(bytes: &[u8], pos: usize) -> Option<i32> {
    if bytes.get(pos - 1) != Some(&b':') {
        return None;
    }
    two_digits(bytes, pos)
}

fn is_valid_java_offset(zone: &str) -> bool {
    if zone == "Z" {
        return true;
    }
    let bytes = zone.as_bytes();
    let parsed = match bytes.len() {
        2 => bytes
            .get(1)
            .filter(|b| b.is_ascii_digit())
            .map(|b| (i32::from(b - b'0'), 0, 0)),
        3 => two_digits(bytes, 1).map(|h| (h, 0, 0)),
        5 => two_digits(bytes, 1)
            .zip(two_digits(bytes, 3))
            .map(|(h, m)| (h, m, 0)),
        6 => two_digits(bytes, 1)
            .zip(two_digits_colon(bytes, 4))
            .map(|(h, m)| (h, m, 0)),
        7 => two_digits(bytes, 1)
            .zip(two_digits(bytes, 3))
            .zip(two_digits(bytes, 5))
            .map(|((h, m), s)| (h, m, s)),
        9 => two_digits(bytes, 1)
            .zip(two_digits_colon(bytes, 4))
            .zip(two_digits_colon(bytes, 7))
            .map(|((h, m), s)| (h, m, s)),
        _ => None,
    };
    let Some((hours, minutes, seconds)) = parsed else {
        return false;
    };
    is_sign(bytes[0])
        && hours <= MAX_OFFSET_HOURS
        && minutes <= MAX_MINUTE_OR_SECOND
        && seconds <= MAX_MINUTE_OR_SECOND
        && !(hours == MAX_OFFSET_HOURS && (minutes != 0 || seconds != 0))
}

pub(super) fn parse_spark_timestamp_ntz(text: &str) -> Option<ScalarValue> {
    let parsed = parse_timestamp_grammar(text)?;
    if parsed.just_time {
        return None;
    }
    if let Some(zone) = parsed.zone
        && !is_valid_spark_zone(zone)
    {
        return None;
    }
    let (month, day) = valid_ymd(parsed.year, parsed.month, parsed.day)?;
    if !validate_hms(parsed.hour, parsed.minute, parsed.second) {
        return None;
    }
    let days = days_from_civil(parsed.year, month, day);
    let secs = days
        .checked_mul(SECONDS_PER_DAY)?
        .checked_add(parsed.hour * 3_600 + parsed.minute * 60 + parsed.second)?;
    let micros = secs
        .checked_mul(MICROS_PER_SECOND)?
        .checked_add(parsed.micros)?;
    Some(ScalarValue::TimestampMicrosecond(Some(micros), None))
}

pub(super) fn is_valid_spark_timestamp_ltz(text: &str) -> bool {
    let Some(parsed) = parse_timestamp_grammar(text) else {
        return false;
    };
    if let Some(zone) = parsed.zone
        && !is_valid_spark_zone(zone)
    {
        return false;
    }
    if !parsed.just_time && valid_ymd(parsed.year, parsed.month, parsed.day).is_none() {
        return false;
    }
    validate_hms(parsed.hour, parsed.minute, parsed.second)
}

pub(super) fn parse_spark_double(text: &str) -> Option<f64> {
    let trimmed = trim_numeric(text);
    if trimmed.is_empty() {
        return None;
    }
    let (negative, unsigned) = strip_sign(trimmed);
    if unsigned.eq_ignore_ascii_case("nan") {
        return Some(f64::NAN);
    }
    if unsigned.eq_ignore_ascii_case("inf") || unsigned.eq_ignore_ascii_case("infinity") {
        return Some(if negative {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }
    let body = unsigned
        .strip_suffix(['d', 'D', 'f', 'F'])
        .unwrap_or(unsigned);
    if body.starts_with("0x") || body.starts_with("0X") {
        return parse_java_hex_float(negative, &body[2..]);
    }
    if !is_java_decimal_float_syntax(body) {
        return None;
    }
    let value = body.parse::<f64>().ok()?;
    Some(if negative { -value } else { value })
}

pub(super) fn parse_spark_float(text: &str) -> Option<f32> {
    let trimmed = trim_numeric(text);
    if trimmed.is_empty() {
        return None;
    }
    let (negative, unsigned) = strip_sign(trimmed);
    if unsigned.eq_ignore_ascii_case("nan") {
        return Some(f32::NAN);
    }
    if unsigned.eq_ignore_ascii_case("inf") || unsigned.eq_ignore_ascii_case("infinity") {
        return Some(if negative {
            f32::NEG_INFINITY
        } else {
            f32::INFINITY
        });
    }
    let body = unsigned
        .strip_suffix(['d', 'D', 'f', 'F'])
        .unwrap_or(unsigned);
    if body.starts_with("0x") || body.starts_with("0X") {
        let value = parse_java_hex_float(negative, &body[2..])?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "float narrowing matches Java Float.parseFloat"
        )]
        return Some(value as f32);
    }
    if !is_java_decimal_float_syntax(body) {
        return None;
    }
    let value = body.parse::<f32>().ok()?;
    Some(if negative { -value } else { value })
}

fn strip_sign(text: &str) -> (bool, &str) {
    if let Some(rest) = text.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = text.strip_prefix('+') {
        (false, rest)
    } else {
        (false, text)
    }
}

fn is_java_decimal_float_syntax(body: &str) -> bool {
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(pos) => (&body[..pos], Some(&body[pos + 1..])),
        None => (body, None),
    };
    let mut saw_digit = false;
    let mut saw_dot = false;
    for byte in mantissa.bytes() {
        if byte.is_ascii_digit() {
            saw_digit = true;
        } else if byte == b'.' && !saw_dot {
            saw_dot = true;
        } else {
            return false;
        }
    }
    if !saw_digit {
        return false;
    }
    if let Some(exp) = exponent {
        let (_, digits) = strip_sign(exp);
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
    }
    true
}

fn parse_java_hex_float(negative: bool, body: &str) -> Option<f64> {
    let p_pos = body.find(['p', 'P'])?;
    let sig = &body[..p_pos];
    let exp_str = &body[p_pos + 1..];
    let (exp_neg, exp_digits) = strip_sign(exp_str);
    if exp_digits.is_empty() || !exp_digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut raw_exp: i64 = 0;
    for byte in exp_digits.bytes() {
        raw_exp = raw_exp
            .saturating_mul(10)
            .saturating_add(i64::from(byte - b'0'))
            .min(100_000);
    }
    let p_exp = if exp_neg { -raw_exp } else { raw_exp };

    let mut saw_hex = false;
    let mut saw_dot = false;
    let mut mantissa: u64 = 0;
    let mut kept_sig = 0_usize;
    let mut exp2 = p_exp;

    for byte in sig.bytes() {
        if byte == b'.' {
            if saw_dot {
                return None;
            }
            saw_dot = true;
            continue;
        }
        let nibble = hex_nibble(byte)?;
        saw_hex = true;
        if mantissa == 0 && nibble == 0 {
            if saw_dot {
                exp2 = exp2.saturating_sub(4);
            }
            continue;
        }
        if kept_sig < 15 {
            mantissa = (mantissa << 4) | u64::from(nibble);
            kept_sig += 1;
            if saw_dot {
                exp2 = exp2.saturating_sub(4);
            }
        } else if !saw_dot {
            exp2 = exp2.saturating_add(4);
        }
    }
    if !saw_hex {
        return None;
    }
    if mantissa == 0 {
        return Some(if negative { -0.0 } else { 0.0 });
    }
    if exp2 > 2_048 {
        return Some(if negative {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }
    if exp2 < -2_048 {
        return Some(if negative { -0.0 } else { 0.0 });
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "mantissa holds at most 60 bits for hex float scaling"
    )]
    let mut value = mantissa as f64;
    let mut remaining = exp2;
    while remaining != 0 {
        let step = remaining.clamp(-512, 512);
        let step_i32 = i32::try_from(step).ok()?;
        value *= 2.0_f64.powi(step_i32);
        remaining -= step;
    }
    Some(if negative { -value } else { value })
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(super) fn parse_spark_decimal(text: &str, precision: u8, scale: i8) -> SparkDecimalOutcome {
    let trimmed = trim_numeric(text);
    if trimmed.is_empty() {
        return SparkDecimalOutcome::InvalidInput;
    }
    let (negative, unsigned) = strip_sign(trimmed);
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(pos) => (&unsigned[..pos], Some(&unsigned[pos + 1..])),
        None => (unsigned, None),
    };

    let mut digits = Vec::with_capacity(mantissa.len());
    let mut saw_digit = false;
    let mut saw_dot = false;
    let mut frac_len: i128 = 0;

    for byte in mantissa.bytes() {
        if byte.is_ascii_digit() {
            saw_digit = true;
            if saw_dot {
                frac_len += 1;
            }
            if !digits.is_empty() || byte != b'0' {
                digits.push(byte - b'0');
            }
        } else if byte == b'.' && !saw_dot {
            saw_dot = true;
        } else {
            return SparkDecimalOutcome::InvalidInput;
        }
    }
    if !saw_digit {
        return SparkDecimalOutcome::InvalidInput;
    }

    let exp: i128 = if let Some(exp_str) = exponent {
        let (exp_neg, exp_digits) = strip_sign(exp_str);
        if exp_digits.is_empty() || !exp_digits.bytes().all(|b| b.is_ascii_digit()) {
            return SparkDecimalOutcome::InvalidInput;
        }
        let mut parsed: i128 = 0;
        for byte in exp_digits.bytes() {
            parsed = parsed
                .saturating_mul(10)
                .saturating_add(i128::from(byte - b'0'))
                .min(1_000_000_000);
        }
        if exp_neg { -parsed } else { parsed }
    } else {
        0
    };

    let eff_exp = exp - frac_len;
    let bd_precision = i128::try_from(digits.len().max(1)).unwrap_or(MAX_DECIMAL_PRECISION + 1);
    if bd_precision + eff_exp > MAX_DECIMAL_PRECISION {
        return SparkDecimalOutcome::OutOfSupportedRange;
    }

    if digits.is_empty() {
        return SparkDecimalOutcome::Ok(0);
    }

    let shift = eff_exp + i128::from(scale);
    let max_mag = decimal_max_magnitude(precision);
    let magnitude = if shift >= 0 {
        let Ok(shift_usize) = usize::try_from(shift) else {
            return SparkDecimalOutcome::OutOfRange;
        };
        if digits.len().saturating_add(shift_usize) > usize::from(precision) {
            return SparkDecimalOutcome::OutOfRange;
        }
        let mut mag: i128 = 0;
        for &digit in &digits {
            mag = mag * 10 + i128::from(digit);
        }
        for _ in 0..shift_usize {
            mag *= 10;
        }
        mag
    } else {
        let drop = usize::try_from(-shift).unwrap_or(usize::MAX);
        if drop > digits.len() {
            0
        } else {
            let keep_len = digits.len() - drop;
            if keep_len > usize::from(precision) {
                return SparkDecimalOutcome::OutOfRange;
            }
            let mut mag: i128 = 0;
            for &digit in &digits[..keep_len] {
                mag = mag * 10 + i128::from(digit);
            }
            let round_up = digits.get(keep_len).is_some_and(|&d| d >= 5);
            mag + i128::from(round_up)
        }
    };

    if magnitude > max_mag {
        return SparkDecimalOutcome::OutOfRange;
    }
    let signed = if negative { -magnitude } else { magnitude };
    SparkDecimalOutcome::Ok(signed)
}

fn decimal_max_magnitude(precision: u8) -> i128 {
    let mut limit: i128 = 0;
    for _ in 0..precision.min(38) {
        limit = limit * 10 + 9;
    }
    limit
}

pub(super) fn decimal_scalar(value: i128, target: &DataType) -> Option<ScalarValue> {
    match *target {
        DataType::Decimal128(precision, scale) => {
            Some(ScalarValue::Decimal128(Some(value), precision, scale))
        }
        DataType::Decimal256(precision, scale) => Some(ScalarValue::Decimal256(
            Some(i256::from_i128(value)),
            precision,
            scale,
        )),
        _ => None,
    }
}

pub(super) fn parse_spark_integral(text: &str, target: &DataType) -> Option<ScalarValue> {
    let trimmed = trim_numeric(text);
    if trimmed.is_empty() {
        return None;
    }
    let (_, digits) = strip_sign(trimmed);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    match target {
        DataType::Int8 => trimmed
            .parse::<i8>()
            .ok()
            .map(|v| ScalarValue::Int8(Some(v))),
        DataType::Int16 => trimmed
            .parse::<i16>()
            .ok()
            .map(|v| ScalarValue::Int16(Some(v))),
        DataType::Int32 => trimmed
            .parse::<i32>()
            .ok()
            .map(|v| ScalarValue::Int32(Some(v))),
        DataType::Int64 => trimmed
            .parse::<i64>()
            .ok()
            .map(|v| ScalarValue::Int64(Some(v))),
        _ => None,
    }
}
