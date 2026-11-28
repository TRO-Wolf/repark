use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};

use super::{
    JavaToken, ParsedPattern, Wall, WallFailure, at, day_failure, fraction_nanos, hour_failure,
    minute_failure, month_failure, second_failure, step, take_digits, take_meridiem,
    take_month_name, take_two_digit_field, take_year, to_date_with_format, week_year_refusal_text,
};
use crate::ansi::SparkAnsiConfig;

#[derive(Clone)]
pub(super) struct Cursor<'text> {
    pub(super) rest: &'text str,
    pub(super) consumed: usize,
    pub(super) wall: Wall,
    pub(super) afternoon: Option<bool>,
    seen: u8,
}

pub(super) fn marker(current: char) -> JavaToken {
    match current {
        '[' => JavaToken::Open(0),
        ']' => JavaToken::Close,
        _ => JavaToken::Literal(current),
    }
}

pub(super) fn link(mut tokens: Vec<JavaToken>, pattern: &str) -> Result<ParsedPattern> {
    let mut open = Vec::new();
    let mut spans = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        match token {
            JavaToken::Open(_) => open.push(index),
            JavaToken::Close => {
                let start = open.pop().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{} Fail to recognize '{pattern}' pattern in the DateTimeFormatter.",
                        week_year_refusal_text()
                    ))
                })?;
                spans.push((start, index + 1));
            }
            _ => {}
        }
    }
    spans.extend(open.into_iter().map(|start| (start, tokens.len())));
    for (start, after) in spans {
        tokens[start] = JavaToken::Open(after);
    }
    let exact = tokens
        .iter()
        .any(|token| matches!(token, JavaToken::Unsupported))
        || crosses_digit_run(&tokens);
    if exact {
        tokens = tokens.into_iter().map(literal_marker).collect();
    }
    Ok(ParsedPattern { tokens, exact })
}

fn literal_marker(token: JavaToken) -> JavaToken {
    match token {
        JavaToken::Open(_) => JavaToken::Literal('['),
        JavaToken::Close => JavaToken::Literal(']'),
        other => other,
    }
}

fn crosses_digit_run(tokens: &[JavaToken]) -> bool {
    let mut greedy = false;
    let mut crossed = false;
    for token in tokens {
        match token {
            JavaToken::Open(_) | JavaToken::Close => crossed |= greedy,
            JavaToken::MonthName { .. }
            | JavaToken::AmPm
            | JavaToken::Literal(_)
            | JavaToken::Unsupported => {
                greedy = false;
                crossed = false;
            }
            JavaToken::Year { .. }
            | JavaToken::MonthNumber { .. }
            | JavaToken::Day { .. }
            | JavaToken::Hour24 { .. }
            | JavaToken::Hour12 { .. }
            | JavaToken::Minute { .. }
            | JavaToken::Second { .. }
            | JavaToken::Fraction { .. } => {
                if crossed {
                    return true;
                }
                greedy |= unbounded(token);
            }
        }
    }
    false
}

fn unbounded(token: &JavaToken) -> bool {
    match token {
        JavaToken::Year { width } => *width != 2,
        JavaToken::MonthNumber { width }
        | JavaToken::Day { width }
        | JavaToken::Hour24 { width }
        | JavaToken::Hour12 { width }
        | JavaToken::Minute { width }
        | JavaToken::Second { width } => *width == 1,
        _ => false,
    }
}

pub(super) fn walk<'text>(
    text: &'text str,
    pattern: &ParsedPattern,
) -> Result<Cursor<'text>, WallFailure> {
    let mut cursor = Cursor {
        rest: text,
        consumed: 0,
        wall: Wall {
            year: 1970,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            nano: 0,
        },
        afternoon: None,
        seen: 0,
    };
    let mut saved: Vec<(Cursor<'text>, usize)> = Vec::new();
    let mut index = 0;
    while let Some(token) = pattern.tokens().get(index) {
        index += 1;
        let failure = match token {
            JavaToken::Open(after) => {
                saved.push((cursor.clone(), *after));
                continue;
            }
            JavaToken::Close => {
                saved.pop();
                continue;
            }
            _ => {
                let before = cursor.clone();
                let outcome = take_token(token, &mut cursor, pattern.exact)
                    .and_then(|()| settle(token, &before, &mut cursor, !saved.is_empty()));
                match outcome {
                    Ok(()) => continue,
                    Err(failure) => failure,
                }
            }
        };
        match (failure, saved.pop()) {
            (WallFailure::AtIndex { .. }, Some((prior, after))) => {
                cursor = prior;
                index = after;
            }
            (failure, _) => return Err(failure),
        }
    }
    Ok(cursor)
}

fn settle(
    token: &JavaToken,
    before: &Cursor<'_>,
    cursor: &mut Cursor<'_>,
    sectioned: bool,
) -> Result<(), WallFailure> {
    let Some((bit, value)) = field(token, &cursor.wall) else {
        return Ok(());
    };
    let clash = sectioned
        && before.seen & bit != 0
        && field(token, &before.wall).map(|(_, prior)| prior) != Some(value);
    cursor.seen |= bit;
    if clash {
        Err(at(before.consumed))
    } else {
        Ok(())
    }
}

fn field(token: &JavaToken, wall: &Wall) -> Option<(u8, i64)> {
    match token {
        JavaToken::Year { .. } => Some((1, i64::from(wall.year))),
        JavaToken::MonthNumber { .. } | JavaToken::MonthName { .. } => {
            Some((2, i64::from(wall.month)))
        }
        JavaToken::Day { .. } => Some((4, i64::from(wall.day))),
        JavaToken::Hour24 { .. } | JavaToken::Hour12 { .. } => Some((8, i64::from(wall.hour))),
        JavaToken::Minute { .. } => Some((16, i64::from(wall.minute))),
        JavaToken::Second { .. } => Some((32, i64::from(wall.second))),
        JavaToken::Fraction { .. } => Some((64, i64::from(wall.nano))),
        JavaToken::AmPm
        | JavaToken::Literal(_)
        | JavaToken::Unsupported
        | JavaToken::Open(_)
        | JavaToken::Close => None,
    }
}

fn take_token(item: &JavaToken, cursor: &mut Cursor<'_>, exact: bool) -> Result<(), WallFailure> {
    let rest = cursor.rest;
    let consumed = cursor.consumed;
    let taken = match item {
        JavaToken::Year { width } => {
            let (value, taken) = take_year(rest, *width).ok_or(at(consumed))?;
            cursor.wall.year = value;
            taken
        }
        JavaToken::MonthNumber { width } => {
            let (value, taken) = take_two_digit_field(rest, *width)
                .filter(|(value, _)| (1..=12).contains(value))
                .ok_or_else(|| month_failure(rest, *width, consumed))?;
            cursor.wall.month = value;
            taken
        }
        JavaToken::MonthName { full } => {
            let (value, taken) = take_month_name(rest, *full).ok_or(at(consumed))?;
            cursor.wall.month = value;
            taken
        }
        JavaToken::Day { width } => {
            let (value, taken) = take_two_digit_field(rest, *width)
                .filter(|(value, _)| (1..=31).contains(value))
                .ok_or_else(|| day_failure(rest, *width, consumed))?;
            cursor.wall.day = value;
            taken
        }
        JavaToken::Hour24 { width } => {
            let (value, taken) = take_two_digit_field(rest, *width)
                .filter(|(value, _)| *value <= 23)
                .ok_or_else(|| hour_failure(rest, *width, consumed, false))?;
            cursor.wall.hour = value;
            taken
        }
        JavaToken::Hour12 { width } => {
            let (value, taken) = take_two_digit_field(rest, *width)
                .filter(|(value, _)| (1..=12).contains(value))
                .ok_or_else(|| hour_failure(rest, *width, consumed, true))?;
            cursor.wall.hour = value;
            taken
        }
        JavaToken::Minute { width } => {
            let (value, taken) = take_two_digit_field(rest, *width)
                .filter(|(value, _)| *value <= 59)
                .ok_or_else(|| minute_failure(rest, *width, consumed))?;
            cursor.wall.minute = value;
            taken
        }
        JavaToken::Second { width } => {
            let (value, taken) = take_two_digit_field(rest, *width)
                .filter(|(value, _)| *value <= 59)
                .ok_or_else(|| second_failure(rest, *width, consumed))?;
            cursor.wall.second = value;
            taken
        }
        JavaToken::Fraction { width } => {
            let least = if exact { *width } else { 1 };
            let (value, taken) = take_digits(rest, least, *width).ok_or(at(consumed))?;
            cursor.wall.nano = fraction_nanos(value, taken).ok_or(at(consumed))?;
            taken
        }
        JavaToken::AmPm => {
            cursor.afternoon = Some(take_meridiem(rest).ok_or(at(consumed))?);
            2
        }
        JavaToken::Literal(expected) => {
            if !rest.starts_with(*expected) {
                return Err(at(consumed));
            }
            expected.len_utf8()
        }
        JavaToken::Unsupported | JavaToken::Open(_) | JavaToken::Close => {
            return Err(at(consumed));
        }
    };
    cursor.rest = step(rest, taken, &mut cursor.consumed);
    Ok(())
}

pub(crate) fn try_dates_with_format(
    input: &ArrayRef,
    formats: &ArrayRef,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    let mut options = (*args.config_options).clone();
    options
        .extensions
        .insert(SparkAnsiConfig { enabled: false });
    let mut quiet = args.clone();
    quiet.config_options = Arc::new(options);
    to_date_with_format(input, formats, &quiet)
}
