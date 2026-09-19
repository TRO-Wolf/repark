use arrow::array::timezone::Tz;
use chrono::FixedOffset;

const MAX_OFFSET_HOURS: i32 = 18;
const MAX_MINUTE_OR_SECOND: i32 = 59;
const SECONDS_PER_HOUR: i32 = 3_600;
const SECONDS_PER_MINUTE: i32 = 60;

const SHORT_IDS: [(&str, &str); 28] = [
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

#[derive(Debug, Clone, Copy)]
pub(crate) enum SparkZone {
    Named(Tz),
    Offset(FixedOffset),
}

#[must_use]
pub(crate) fn spark_zone_id(raw: &str) -> Option<SparkZone> {
    let trimmed = raw.trim_matches(|character: char| character <= ' ');
    let formatted = pad_single_minute(&pad_single_hour(trimmed));
    let aliased = SHORT_IDS
        .iter()
        .find(|(short, _)| *short == formatted)
        .map_or(formatted.as_str(), |(_, full)| full);
    java_zone_of(aliased)
}

fn is_sign(byte: u8) -> bool {
    byte == b'+' || byte == b'-'
}

fn pad_single_hour(zone: &str) -> String {
    let bytes = zone.as_bytes();
    let found = bytes
        .windows(3)
        .position(|window| is_sign(window[0]) && window[1].is_ascii_digit() && window[2] == b':');
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
    let matches = is_sign(tail[0])
        && tail[1].is_ascii_digit()
        && tail[2].is_ascii_digit()
        && tail[3] == b':'
        && tail[4].is_ascii_digit();
    if matches {
        format!("{}0{}", &zone[..start + 4], &zone[start + 4..])
    } else {
        zone.to_string()
    }
}

fn java_zone_of(zone: &str) -> Option<SparkZone> {
    if zone.len() <= 1 || zone.starts_with('+') || zone.starts_with('-') {
        return java_offset_of(zone).map(SparkZone::Offset);
    }
    if zone.starts_with("UTC") || zone.starts_with("GMT") {
        return java_zone_with_prefix(zone, 3);
    }
    if zone.starts_with("UT") {
        return java_zone_with_prefix(zone, 2);
    }
    java_region_of(zone)
}

fn java_zone_with_prefix(zone: &str, prefix_length: usize) -> Option<SparkZone> {
    let rest = &zone[prefix_length..];
    match rest.as_bytes().first() {
        None => Some(SparkZone::Offset(FixedOffset::east_opt(0)?)),
        Some(byte) if is_sign(*byte) => java_offset_of(rest).map(SparkZone::Offset),
        Some(_) => java_region_of(zone),
    }
}

fn is_region_character(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'~' | b'/' | b'.' | b'_' | b'+' | b'-')
}

fn java_region_of(zone: &str) -> Option<SparkZone> {
    let bytes = zone.as_bytes();
    let well_formed = bytes.len() >= 2
        && bytes[0].is_ascii_alphabetic()
        && bytes[1..].iter().all(|byte| is_region_character(*byte));
    if !well_formed {
        return None;
    }
    zone.parse::<Tz>().ok().map(SparkZone::Named)
}

fn two_digits(bytes: &[u8], position: usize) -> Option<i32> {
    let tens = *bytes.get(position)?;
    let units = *bytes.get(position + 1)?;
    if !tens.is_ascii_digit() || !units.is_ascii_digit() {
        return None;
    }
    Some(i32::from(tens - b'0') * 10 + i32::from(units - b'0'))
}

fn two_digits_after_colon(bytes: &[u8], position: usize) -> Option<i32> {
    if bytes.get(position - 1) != Some(&b':') {
        return None;
    }
    two_digits(bytes, position)
}

fn java_offset_of(zone: &str) -> Option<FixedOffset> {
    if zone == "Z" {
        return FixedOffset::east_opt(0);
    }
    let bytes = zone.as_bytes();
    let (hours, minutes, seconds) = match bytes.len() {
        2 => {
            let hour = *bytes.get(1)?;
            if !hour.is_ascii_digit() {
                return None;
            }
            (i32::from(hour - b'0'), 0, 0)
        }
        3 => (two_digits(bytes, 1)?, 0, 0),
        5 => (two_digits(bytes, 1)?, two_digits(bytes, 3)?, 0),
        6 => (two_digits(bytes, 1)?, two_digits_after_colon(bytes, 4)?, 0),
        7 => (
            two_digits(bytes, 1)?,
            two_digits(bytes, 3)?,
            two_digits(bytes, 5)?,
        ),
        9 => (
            two_digits(bytes, 1)?,
            two_digits_after_colon(bytes, 4)?,
            two_digits_after_colon(bytes, 7)?,
        ),
        _ => return None,
    };
    if !is_sign(bytes[0])
        || hours > MAX_OFFSET_HOURS
        || minutes > MAX_MINUTE_OR_SECOND
        || seconds > MAX_MINUTE_OR_SECOND
        || (hours == MAX_OFFSET_HOURS && (minutes != 0 || seconds != 0))
    {
        return None;
    }
    let magnitude = hours * SECONDS_PER_HOUR + minutes * SECONDS_PER_MINUTE + seconds;
    let signed = if bytes[0] == b'-' {
        -magnitude
    } else {
        magnitude
    };
    FixedOffset::east_opt(signed)
}
