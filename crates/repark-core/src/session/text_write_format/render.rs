use std::fmt::Write as _;

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

fn format_iso_offset_into(
    output: &mut String,
    sign: char,
    hours: u32,
    minutes: u32,
    seconds: u32,
    colon: bool,
) {
    let _ = write!(output, "{sign}{hours:02}");
    if colon {
        output.push(':');
    }
    let _ = write!(output, "{minutes:02}");
    if seconds != 0 {
        if colon {
            output.push(':');
        }
        let _ = write!(output, "{seconds:02}");
    }
}

fn render_localized_offset_into(output: &mut String, count: usize, offset: FixedOffset) {
    let (sign, hours, minutes, seconds) = offset_parts(offset);
    if hours == 0 && minutes == 0 && seconds == 0 {
        output.push_str("GMT");
        return;
    }
    if count == 4 {
        output.push_str("GMT");
        format_iso_offset_into(output, sign, hours, minutes, seconds, true);
        return;
    }
    let _ = write!(output, "GMT{sign}{hours}");
    if minutes != 0 || seconds != 0 {
        let _ = write!(output, ":{minutes:02}");
        if seconds != 0 {
            let _ = write!(output, ":{seconds:02}");
        }
    }
}

fn render_offset_field_into(output: &mut String, letter: char, count: usize, offset: FixedOffset) {
    let (sign, hours, minutes, seconds) = offset_parts(offset);
    let zero = hours == 0 && minutes == 0 && seconds == 0;
    match letter {
        'X' if zero => output.push('Z'),
        'x' if zero => match count {
            1 => output.push_str("+00"),
            2 | 4 => output.push_str("+0000"),
            _ => output.push_str("+00:00"),
        },
        'X' | 'x' => match count {
            1 => {
                let _ = write!(output, "{sign}{hours:02}");
                if minutes != 0 {
                    let _ = write!(output, "{minutes:02}");
                }
            }
            2 => {
                let _ = write!(output, "{sign}{hours:02}{minutes:02}");
            }
            3 => {
                let _ = write!(output, "{sign}{hours:02}:{minutes:02}");
            }
            4 => {
                let _ = write!(output, "{sign}{hours:02}{minutes:02}");
                if seconds != 0 {
                    let _ = write!(output, "{seconds:02}");
                }
            }
            _ => format_iso_offset_into(output, sign, hours, minutes, seconds, true),
        },
        'Z' if count <= 3 => {
            let _ = write!(output, "{sign}{hours:02}{minutes:02}");
        }
        'Z' if count == 4 => render_localized_offset_into(output, count, offset),
        'Z' => {
            if zero {
                output.push('Z');
            } else {
                format_iso_offset_into(output, sign, hours, minutes, seconds, true);
            }
        }
        'O' => render_localized_offset_into(output, count, offset),
        _ => {}
    }
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

fn render_year_into(output: &mut String, year: i32, count: usize, has_era: bool) {
    let proleptic = i64::from(year);
    let value = if has_era && proleptic <= 0 {
        1 - proleptic
    } else {
        proleptic
    };
    if count == 2 {
        let _ = write!(output, "{:02}", value.abs() % 100);
        return;
    }
    if count == 1 {
        let _ = write!(output, "{value}");
        return;
    }
    let digits = value.unsigned_abs();
    let width = count.max(1);
    if value < 0 {
        output.push('-');
    } else if count >= 4 {
        let mut threshold = 1u64;
        for _ in 0..count {
            threshold = threshold.saturating_mul(10);
        }
        if digits >= threshold {
            output.push('+');
        }
    }
    let _ = write!(output, "{digits:0width$}");
}

fn render_fraction_into(output: &mut String, nanos: u32, count: usize) {
    let width = count.clamp(1, 9);
    let narrow = u32::try_from(width).unwrap_or(9);
    let divisor = 10u32.pow(9 - narrow);
    let _ = write!(output, "{:0width$}", nanos / divisor);
}

fn render_date_field_into(
    output: &mut String,
    letter: char,
    count: usize,
    date: NaiveDate,
    epoch_day: i64,
    has_era: bool,
) -> bool {
    match letter {
        'y' => {
            render_year_into(output, date.year(), count, has_era);
            true
        }
        'M' | 'L' => {
            match count {
                1 => {
                    let _ = write!(output, "{}", date.month());
                }
                2 => {
                    let _ = write!(output, "{:02}", date.month());
                }
                3 => output.push_str(month_short(date.month())),
                _ => output.push_str(month_full(date.month())),
            }
            true
        }
        'd' => {
            let _ = write!(output, "{:0width$}", date.day(), width = count.max(1));
            true
        }
        'D' => {
            let _ = write!(output, "{:0width$}", date.ordinal(), width = count.max(1));
            true
        }
        'E' => {
            if count <= 3 {
                output.push_str(weekday_short(date.weekday()));
            } else {
                output.push_str(weekday_full(date.weekday()));
            }
            true
        }
        'Q' | 'q' => {
            let quarter = date.month0() / 3 + 1;
            if count <= 2 {
                let _ = write!(output, "{quarter:0width$}", width = count.max(1));
            } else if count == 3 {
                let _ = write!(output, "Q{quarter}");
            } else {
                output.push_str(quarter_name(quarter));
            }
            true
        }
        'F' => {
            let _ = write!(output, "{}", ((date.day() - 1) % 7) + 1);
            true
        }
        'G' => {
            output.push_str(render_era(date.year(), count));
            true
        }
        'g' => {
            let days = epoch_day + UNIX_TO_MJD_EPOCH_DAYS;
            let width = count.max(1);
            if days < 0 {
                output.push('-');
                let _ = write!(output, "{:0width$}", days.unsigned_abs());
            } else {
                let _ = write!(output, "{days:0width$}");
            }
            true
        }
        _ => false,
    }
}

fn render_time_field_into(
    output: &mut String,
    letter: char,
    count: usize,
    time: &NaiveDateTime,
    nanos: u32,
) -> bool {
    let width = count.max(1);
    match letter {
        'H' => {
            let _ = write!(output, "{:0width$}", time.hour(), width = width);
            true
        }
        'K' => {
            let _ = write!(output, "{:0width$}", time.hour() % 12, width = width);
            true
        }
        'k' => {
            let _ = write!(
                output,
                "{:0width$}",
                if time.hour() == 0 { 24 } else { time.hour() },
                width = width
            );
            true
        }
        'h' => {
            let _ = write!(
                output,
                "{:0width$}",
                if time.hour().is_multiple_of(12) {
                    12
                } else {
                    time.hour() % 12
                },
            );
            true
        }
        'm' => {
            let _ = write!(output, "{:0width$}", time.minute(), width = width);
            true
        }
        's' => {
            let _ = write!(output, "{:0width$}", time.second(), width = width);
            true
        }
        'S' => {
            render_fraction_into(output, nanos, count);
            true
        }
        'a' => {
            if time.hour() < 12 {
                output.push_str("AM");
            } else {
                output.push_str("PM");
            }
            true
        }
        _ => false,
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

fn render_field_into(
    output: &mut String,
    letter: char,
    count: usize,
    value: &RenderValue,
    epoch_day: i64,
    has_era: bool,
) -> std::result::Result<(), String> {
    let (date, wall, nanos) = match value {
        RenderValue::Instant { wall, nanos, .. } | RenderValue::Wall { wall, nanos } => {
            (wall.date(), Some(*wall), *nanos)
        }
        RenderValue::Date { date } => (*date, None, 0),
    };
    if render_date_field_into(output, letter, count, date, epoch_day, has_era) {
        return Ok(());
    }
    if let Some(wall_value) = wall {
        if render_time_field_into(output, letter, count, &wall_value, nanos) {
            return Ok(());
        }
    } else if matches!(letter, 'H' | 'h' | 'K' | 'k' | 'm' | 's' | 'S' | 'a') {
        return Err(unsupported_field(time_field_name(letter)));
    }
    match value {
        RenderValue::Instant {
            offset, zone_id, ..
        } => match letter {
            'X' | 'x' | 'Z' | 'O' => {
                render_offset_field_into(output, letter, count, *offset);
                Ok(())
            }
            'V' => {
                output.push_str(zone_id);
                Ok(())
            }
            _ => Err(unsupported_field("OffsetSeconds")),
        },
        RenderValue::Wall { .. } | RenderValue::Date { .. } => match letter {
            'V' | 'v' | 'z' => Err(zone_id_missing(value)),
            _ => Err(unsupported_field("OffsetSeconds")),
        },
    }
}

pub fn render_compiled_into(
    compiled: &CompiledPattern,
    value: &RenderValue,
    output: &mut String,
) -> std::result::Result<(), String> {
    output.clear();
    let mut section_starts: Vec<(usize, usize)> = Vec::new();
    let date = match value {
        RenderValue::Instant { wall, .. } | RenderValue::Wall { wall, .. } => wall.date(),
        RenderValue::Date { date } => *date,
    };
    let epoch_day = NaiveDateTime::new(date, NaiveTime::MIN)
        .and_utc()
        .timestamp()
        .div_euclid(86_400);
    let has_era = compiled.has_era;
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
            PatternToken::Field { letter, count } => {
                match render_field_into(output, *letter, *count, value, epoch_day, has_era) {
                    Ok(()) => {
                        index += 1;
                    }
                    Err(failure) => {
                        if let Some((open, start_len)) = section_starts.pop() {
                            output.truncate(start_len);
                            index = compiled.matching[open]
                                .map_or(compiled.tokens.len(), |end| end + 1);
                            section_starts.retain(|(position, _)| *position < open);
                        } else {
                            return Err(failure);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn push_year(output: &mut String, year: i32) {
    if (0..10_000).contains(&year) {
        let _ = write!(output, "{year:04}");
    } else {
        let _ = write!(output, "{year:+05}");
    }
}

fn push_date(output: &mut String, date: NaiveDate) {
    push_year(output, date.year());
    let _ = write!(output, "-{:02}-{:02}", date.month(), date.day());
}

pub fn render_timestamp_default_into(
    output: &mut String,
    wall: &NaiveDateTime,
    nanos: u32,
    offset: FixedOffset,
) {
    output.clear();
    push_date(output, wall.date());
    let _ = write!(
        output,
        "T{:02}:{:02}:{:02}.{:03}",
        wall.hour(),
        wall.minute(),
        wall.second(),
        nanos / 1_000_000
    );
    let (sign, hours, minutes, seconds) = offset_parts(offset);
    if hours == 0 && minutes == 0 && seconds == 0 {
        output.push('Z');
    } else {
        format_iso_offset_into(output, sign, hours, minutes, seconds, true);
    }
}

pub fn render_ntz_default_into(output: &mut String, wall: &NaiveDateTime, nanos: u32) {
    output.clear();
    push_date(output, wall.date());
    let _ = write!(
        output,
        "T{:02}:{:02}:{:02}.{:03}",
        wall.hour(),
        wall.minute(),
        wall.second(),
        nanos / 1_000_000
    );
}

pub fn render_date_default_into(output: &mut String, date: NaiveDate) {
    output.clear();
    push_date(output, date);
}
