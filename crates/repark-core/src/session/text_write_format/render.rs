use chrono::{Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Weekday};

use super::{CompiledPattern, PatternToken};

const UNIX_TO_MJD_EPOCH_DAYS: i64 = 40_587;

#[derive(Clone, Copy, Debug)]
pub enum RenderValue<'a> {
    Instant {
        wall: NaiveDateTime,
        nanos: u32,
        offset: FixedOffset,
        zone_id: &'a str,
    },
    Wall {
        wall: NaiveDateTime,
        nanos: u32,
    },
    Date {
        date: NaiveDate,
    },
}

fn unsupported_field(name: &str) -> String {
    format!("Unsupported field: {name}")
}

fn zone_id_missing(value: &RenderValue) -> String {
    match value {
        RenderValue::Date { date } => format!(
            "Unable to extract ZoneId from temporal {}",
            date.format("%Y-%m-%d")
        ),
        RenderValue::Wall { wall, nanos } => format!(
            "Unable to extract ZoneId from temporal {}",
            java_wall_text(wall, *nanos)
        ),
        RenderValue::Instant { .. } => "Unable to extract ZoneId from temporal".to_string(),
    }
}

fn java_wall_text(wall: &NaiveDateTime, nanos: u32) -> String {
    let mut text = wall.format("%Y-%m-%dT%H:%M:%S").to_string();
    if nanos != 0 {
        let mut digits = format!("{nanos:09}");
        while digits.ends_with('0') {
            digits.pop();
        }
        text.push('.');
        text.push_str(&digits);
    }
    text
}

fn offset_parts(offset: FixedOffset) -> (char, u32, u32, u32) {
    let total = offset.local_minus_utc();
    let sign = if total < 0 { '-' } else { '+' };
    let total = total.unsigned_abs();
    let hours = total / 3600;
    let minutes = (total % 3600) / 60;
    let seconds = total % 60;
    (sign, hours, minutes, seconds)
}

fn format_iso_offset(sign: char, hours: u32, minutes: u32, seconds: u32, colon: bool) -> String {
    let mut text = format!("{sign}{hours:02}");
    if colon {
        text.push(':');
        text.push_str(&format!("{minutes:02}"));
    } else {
        text.push_str(&format!("{minutes:02}"));
    }
    if seconds != 0 {
        if colon {
            text.push(':');
        }
        text.push_str(&format!("{seconds:02}"));
    }
    text
}

fn render_offset_field(letter: char, count: usize, offset: FixedOffset) -> String {
    let (sign, hours, minutes, seconds) = offset_parts(offset);
    let zero = hours == 0 && minutes == 0 && seconds == 0;
    match letter {
        'X' if zero => "Z".to_string(),
        'x' if zero => match count {
            1 => "+00".to_string(),
            2 | 4 => "+0000".to_string(),
            _ => "+00:00".to_string(),
        },
        'X' | 'x' => match count {
            1 => {
                let mut text = format!("{sign}{hours:02}");
                if minutes != 0 {
                    text.push_str(&format!("{minutes:02}"));
                }
                text
            }
            2 => format!("{sign}{hours:02}{minutes:02}"),
            3 => format!("{sign}{hours:02}:{minutes:02}"),
            4 => {
                let mut text = format!("{sign}{hours:02}{minutes:02}");
                if seconds != 0 {
                    text.push_str(&format!("{seconds:02}"));
                }
                text
            }
            _ => format_iso_offset(sign, hours, minutes, seconds, true),
        },
        'Z' if count <= 3 => format!("{sign}{hours:02}{minutes:02}"),
        'Z' if count == 4 => render_localized_offset(count, offset),
        'Z' => {
            if zero {
                "Z".to_string()
            } else {
                format_iso_offset(sign, hours, minutes, seconds, true)
            }
        }
        'O' => render_localized_offset(count, offset),
        _ => String::new(),
    }
}

fn render_localized_offset(count: usize, offset: FixedOffset) -> String {
    let (sign, hours, minutes, seconds) = offset_parts(offset);
    if hours == 0 && minutes == 0 && seconds == 0 {
        return "GMT".to_string();
    }
    if count == 4 {
        return format!(
            "GMT{}",
            format_iso_offset(sign, hours, minutes, seconds, true)
        );
    }
    let mut text = format!("GMT{sign}{hours}");
    if minutes != 0 || seconds != 0 {
        text.push_str(&format!(":{minutes:02}"));
        if seconds != 0 {
            text.push_str(&format!(":{seconds:02}"));
        }
    }
    text
}

fn quarter_name(quarter: u32) -> &'static str {
    match quarter {
        1 => "1st quarter",
        2 => "2nd quarter",
        3 => "3rd quarter",
        _ => "4th quarter",
    }
}

fn weekday_short(weekday: Weekday) -> &'static str {
    match weekday {
        Weekday::Mon => "Mon",
        Weekday::Tue => "Tue",
        Weekday::Wed => "Wed",
        Weekday::Thu => "Thu",
        Weekday::Fri => "Fri",
        Weekday::Sat => "Sat",
        Weekday::Sun => "Sun",
    }
}

fn weekday_full(weekday: Weekday) -> &'static str {
    match weekday {
        Weekday::Mon => "Monday",
        Weekday::Tue => "Tuesday",
        Weekday::Wed => "Wednesday",
        Weekday::Thu => "Thursday",
        Weekday::Fri => "Friday",
        Weekday::Sat => "Saturday",
        Weekday::Sun => "Sunday",
    }
}

fn month_short(month: u32) -> &'static str {
    match month {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        _ => "Dec",
    }
}

fn month_full(month: u32) -> &'static str {
    match month {
        1 => "January",
        2 => "February",
        3 => "March",
        4 => "April",
        5 => "May",
        6 => "June",
        7 => "July",
        8 => "August",
        9 => "September",
        10 => "October",
        11 => "November",
        _ => "December",
    }
}

fn render_era(year: i32, count: usize) -> &'static str {
    if year >= 1 {
        if count <= 3 { "AD" } else { "Anno Domini" }
    } else if count <= 3 {
        "BC"
    } else {
        "Before Christ"
    }
}

fn render_year(year: i32, count: usize) -> String {
    let era_year = if year <= 0 {
        1i64 - i64::from(year)
    } else {
        i64::from(year)
    };
    if count == 2 {
        return format!("{:02}", era_year.rem_euclid(100));
    }
    if count == 1 {
        return era_year.to_string();
    }
    let width = count.max(1);
    format!("{:0width$}", era_year, width = width)
}

fn render_fraction(nanos: u32, count: usize) -> String {
    let width = count.clamp(1, 9);
    let divisor = 10u32.pow(9 - width as u32);
    format!("{:0width$}", nanos / divisor, width = width)
}

fn render_date_field(
    letter: char,
    count: usize,
    date: NaiveDate,
    epoch_day: i64,
) -> Option<String> {
    match letter {
        'y' => Some(render_year(date.year(), count)),
        'M' | 'L' => Some(match count {
            1 => date.month().to_string(),
            2 => format!("{:02}", date.month()),
            3 => month_short(date.month()).to_string(),
            _ => month_full(date.month()).to_string(),
        }),
        'd' => Some(format!("{:0width$}", date.day(), width = count.max(1))),
        'D' => Some(format!("{:0width$}", date.ordinal(), width = count.max(1))),
        'E' => Some(if count <= 3 {
            weekday_short(date.weekday()).to_string()
        } else {
            weekday_full(date.weekday()).to_string()
        }),
        'Q' | 'q' => {
            let quarter = date.month0() / 3 + 1;
            Some(if count <= 2 {
                format!("{quarter:0width$}", width = count.max(1))
            } else if count == 3 {
                format!("Q{quarter}")
            } else {
                quarter_name(quarter).to_string()
            })
        }
        'F' => Some((((date.day() - 1) % 7) + 1).to_string()),
        'G' => Some(render_era(date.year(), count).to_string()),
        'g' => Some((epoch_day + UNIX_TO_MJD_EPOCH_DAYS).to_string()),
        _ => None,
    }
}

fn render_time_field(
    letter: char,
    count: usize,
    time: &NaiveDateTime,
    nanos: u32,
) -> Option<String> {
    let width = count.max(1);
    match letter {
        'H' => Some(format!("{:0width$}", time.hour(), width = width)),
        'K' => Some(format!("{:0width$}", time.hour() % 12, width = width)),
        'k' => Some(format!(
            "{:0width$}",
            if time.hour() == 0 { 24 } else { time.hour() },
            width = width
        )),
        'h' => Some(format!(
            "{:0width$}",
            if time.hour() % 12 == 0 {
                12
            } else {
                time.hour() % 12
            },
            width = width
        )),
        'm' => Some(format!("{:0width$}", time.minute(), width = width)),
        's' => Some(format!("{:0width$}", time.second(), width = width)),
        'S' => Some(render_fraction(nanos, count)),
        'a' => Some(if time.hour() < 12 { "AM" } else { "PM" }.to_string()),
        _ => None,
    }
}

fn time_field_name(letter: char) -> &'static str {
    match letter {
        'H' => "HourOfDay",
        'h' => "ClockHourOfAmPm",
        'K' => "HourOfAmPm",
        'k' => "ClockHourOfDay",
        'm' => "MinuteOfHour",
        's' => "SecondOfMinute",
        'S' => "NanoOfSecond",
        _ => "AmPmOfDay",
    }
}

fn render_field(
    letter: char,
    count: usize,
    value: &RenderValue,
) -> std::result::Result<String, String> {
    let (date, wall, nanos) = match value {
        RenderValue::Instant { wall, nanos, .. } => (wall.date(), Some(*wall), *nanos),
        RenderValue::Wall { wall, nanos } => (wall.date(), Some(*wall), *nanos),
        RenderValue::Date { date } => (*date, None, 0),
    };
    let epoch_day = NaiveDateTime::new(date, NaiveTime::MIN)
        .and_utc()
        .timestamp()
        .div_euclid(86_400);
    if let Some(text) = render_date_field(letter, count, date, epoch_day) {
        return Ok(text);
    }
    if let Some(wall_value) = wall {
        if let Some(text) = render_time_field(letter, count, &wall_value, nanos) {
            return Ok(text);
        }
    } else if matches!(letter, 'H' | 'h' | 'K' | 'k' | 'm' | 's' | 'S' | 'a') {
        return Err(unsupported_field(time_field_name(letter)));
    }
    match value {
        RenderValue::Instant {
            offset, zone_id, ..
        } => match letter {
            'X' | 'x' | 'Z' | 'O' => Ok(render_offset_field(letter, count, *offset)),
            'V' => Ok((*zone_id).to_string()),
            _ => Err(unsupported_field("OffsetSeconds")),
        },
        RenderValue::Wall { .. } | RenderValue::Date { .. } => match letter {
            'X' | 'x' | 'Z' | 'O' => Err(unsupported_field("OffsetSeconds")),
            'V' | 'v' | 'z' => Err(zone_id_missing(value)),
            _ => Err(unsupported_field("OffsetSeconds")),
        },
    }
}

pub fn render_compiled(
    compiled: &CompiledPattern,
    value: &RenderValue,
) -> std::result::Result<String, String> {
    let mut output = String::new();
    let mut section_starts: Vec<(usize, usize)> = Vec::new();
    let mut index = 0usize;
    while index < compiled.tokens.len() {
        match &compiled.tokens[index] {
            PatternToken::Literal(text) => {
                output.push_str(text);
                index += 1;
            }
            PatternToken::OpenSection => {
                section_starts.push((index, output.len()));
                index += 1;
            }
            PatternToken::CloseSection => {
                section_starts.pop();
                index += 1;
            }
            PatternToken::Field { letter, count } => match render_field(*letter, *count, value) {
                Ok(text) => {
                    output.push_str(&text);
                    index += 1;
                }
                Err(failure) => {
                    if let Some((open, start_len)) = section_starts.pop() {
                        output.truncate(start_len);
                        index =
                            compiled.matching[open].map_or(compiled.tokens.len(), |end| end + 1);
                        section_starts.retain(|(position, _)| *position < open);
                    } else {
                        return Err(failure);
                    }
                }
            },
        }
    }
    Ok(output)
}

pub fn render_timestamp_default(wall: &NaiveDateTime, nanos: u32, offset: FixedOffset) -> String {
    let millis = nanos / 1_000_000;
    let mut text = format!(
        "{}T{:02}:{:02}:{:02}.{:03}",
        wall.date().format("%Y-%m-%d"),
        wall.hour(),
        wall.minute(),
        wall.second(),
        millis
    );
    let (sign, hours, minutes, seconds) = offset_parts(offset);
    if hours == 0 && minutes == 0 && seconds == 0 {
        text.push('Z');
    } else {
        text.push_str(&format_iso_offset(sign, hours, minutes, seconds, true));
    }
    text
}

pub fn render_ntz_default(wall: &NaiveDateTime, nanos: u32) -> String {
    let millis = nanos / 1_000_000;
    format!(
        "{}T{:02}:{:02}:{:02}.{:03}",
        wall.date().format("%Y-%m-%d"),
        wall.hour(),
        wall.minute(),
        wall.second(),
        millis
    )
}

pub fn render_date_default(date: &NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}
