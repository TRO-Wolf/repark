use std::str::FromStr;

use arrow::array::timezone::Tz;
use chrono::{DateTime, FixedOffset, LocalResult, NaiveDateTime, TimeZone};
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::tokenizer::Token;

use crate::SessionTimeZone;

use super::TimeTravelSpec;

#[allow(clippy::missing_errors_doc)]
pub fn parse_version_value(raw: &str) -> Result<TimeTravelSpec> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(DataFusionError::Plan(
            "VERSION AS OF requires a non-empty snapshot id or branch/tag name".to_string(),
        ));
    }
    if let Ok(snapshot_id) = trimmed.parse::<i64>() {
        return Ok(TimeTravelSpec::SnapshotId(snapshot_id));
    }
    Ok(TimeTravelSpec::VersionRef(trimmed.to_string()))
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_timestamp_to_ms(raw: &str) -> Result<i64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(DataFusionError::Plan(
            "TIMESTAMP AS OF requires a non-empty timestamp".to_string(),
        ));
    }
    if let Ok(ms) = trimmed.parse::<i64>() {
        return Ok(ms);
    }
    if let Ok(offset_dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return Ok(offset_dt.timestamp_millis());
    }
    let without_z = trimmed
        .strip_suffix('Z')
        .or_else(|| trimmed.strip_suffix('z'))
        .unwrap_or(trimmed);
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ];
    for format in formats {
        if let Ok(naive) = NaiveDateTime::parse_from_str(without_z, format) {
            return Ok(naive.and_utc().timestamp_millis());
        }
        if format == "%Y-%m-%d"
            && let Ok(date) = chrono::NaiveDate::parse_from_str(without_z, format)
            && let Some(naive) = date.and_hms_opt(0, 0, 0)
        {
            return Ok(naive.and_utc().timestamp_millis());
        }
    }
    Err(DataFusionError::Plan(format!(
        "cannot parse TIMESTAMP AS OF value {trimmed:?} \
         (expected epoch ms, RFC3339, or YYYY-MM-dd[ HH:MM:SS][Z])"
    )))
}

#[must_use]
pub fn parse_timestamp_string_to_ms(text: &str, zone: &SessionTimeZone) -> Option<i64> {
    const WALL: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ];
    const OFFSET_WALL: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%:z",
    ];
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(offset_dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(offset_dt.timestamp_millis());
    }
    let without_z = trimmed
        .strip_suffix('Z')
        .or_else(|| trimmed.strip_suffix('z'))
        .unwrap_or(trimmed);
    for format in WALL {
        if let Ok(naive) = NaiveDateTime::parse_from_str(without_z, format) {
            return zoned_wall_to_ms(naive, zone);
        }
    }
    for format in OFFSET_WALL {
        if let Ok(offset_dt) = DateTime::parse_from_str(without_z, format) {
            return Some(offset_dt.timestamp_millis());
        }
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(without_z, "%Y-%m-%d")
        && let Some(midnight) = date.and_hms_opt(0, 0, 0)
    {
        return zoned_wall_to_ms(midnight, zone);
    }
    None
}

#[must_use]
pub fn parse_timestamp_asof_to_ms(raw: &str, zone: &SessionTimeZone) -> Option<i64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(seconds) = trimmed.parse::<i64>() {
        return seconds.checked_mul(1000);
    }
    if let Some(millis) = fixed_decimal_seconds_to_ms(trimmed) {
        return Some(millis);
    }
    parse_timestamp_string_to_ms(trimmed, zone)
}

#[must_use]
pub fn extract_timestamp_expr(significant: &[(usize, &Token)], value_sig: usize) -> Vec<Token> {
    const TERMINATORS: [&str; 20] = [
        "WHERE",
        "GROUP",
        "ORDER",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "WINDOW",
        "JOIN",
        "INNER",
        "LEFT",
        "RIGHT",
        "FULL",
        "CROSS",
        "NATURAL",
        "ON",
        "USING",
        "UNION",
        "EXCEPT",
        "INTERSECT",
        "FOR",
    ];
    let mut depth = 0_usize;
    let mut out = Vec::new();
    let mut index = value_sig;
    while index < significant.len() {
        let token: &Token = significant[index].1;
        match token {
            Token::LParen => depth += 1,
            Token::RParen => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            }
            Token::Comma | Token::SemiColon => {
                if depth == 0 {
                    break;
                }
            }
            Token::Word(word)
                if depth == 0
                    && TERMINATORS.contains(&word.value.to_ascii_uppercase().as_str()) =>
            {
                break;
            }
            _ => {}
        }
        out.push(token.clone());
        index += 1;
    }
    out
}

fn fixed_decimal_seconds_to_ms(trimmed: &str) -> Option<i64> {
    let (sign, digits) = match trimmed.strip_prefix('-') {
        Some(rest) => (-1_i128, rest),
        None => (1_i128, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };
    let (secs, frac) = digits.split_once('.')?;
    if !secs.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if !frac.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let seconds: i128 = if secs.is_empty() {
        0
    } else {
        secs.parse().ok()?
    };
    if frac.is_empty() {
        return i64::try_from(sign * seconds.checked_mul(1000)?).ok();
    }
    let mut whole = frac.to_string();
    whole.truncate(3);
    while whole.len() < 3 {
        whole.push('0');
    }
    let frac_ms: i128 = whole.parse().ok()?;
    i64::try_from(sign * (seconds.checked_mul(1000)?.checked_add(frac_ms)?)).ok()
}

pub(crate) fn zoned_wall_to_ms(naive: NaiveDateTime, zone: &SessionTimeZone) -> Option<i64> {
    if let Ok(named) = Tz::from_str(zone.id()) {
        return match named.from_local_datetime(&naive) {
            LocalResult::Single(current) => Some(current.timestamp_millis()),
            LocalResult::Ambiguous(first, second) => Some(first.min(second).timestamp_millis()),
            LocalResult::None => None,
        };
    }
    fixed_offset_zone(zone.id()).and_then(|offset| {
        offset
            .from_local_datetime(&naive)
            .single()
            .map(|current| current.timestamp_millis())
    })
}

fn fixed_offset_zone(id: &str) -> Option<FixedOffset> {
    let (sign, rest) = match id.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, id.strip_prefix('+').unwrap_or(id)),
    };
    let (hours, minutes) = match rest.split_once(':') {
        Some((hours, minutes)) => (hours, minutes),
        None if rest.len() == 4 => rest.split_at(2),
        None => (rest, "00"),
    };
    let parsed_hours: i32 = hours.parse().ok()?;
    let parsed_minutes: i32 = minutes.parse().ok()?;
    FixedOffset::east_opt(sign * (parsed_hours * 3600 + parsed_minutes * 60))
}

#[must_use]
pub fn format_snapshot_bound_ms(bound_ms: i64, zone: &SessionTimeZone) -> String {
    let Some(naive) = DateTime::from_timestamp_millis(bound_ms).map(|moment| moment.naive_utc())
    else {
        return format!("{bound_ms}");
    };
    if let Ok(named) = Tz::from_str(zone.id()) {
        let zoned = named.from_utc_datetime(&naive);
        if bound_ms % 1000 == 0 {
            return zoned.format("%Y-%m-%dT%H:%M:%S%:z").to_string();
        }
        return zoned.format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string();
    }
    if let Some(offset) = fixed_offset_zone(zone.id()) {
        let zoned = offset.from_utc_datetime(&naive);
        if bound_ms % 1000 == 0 {
            return zoned.format("%Y-%m-%dT%H:%M:%S%:z").to_string();
        }
        return zoned.format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string();
    }
    format!("{bound_ms}")
}
