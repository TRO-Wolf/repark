use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::exceptions::{ArithmeticException, IllegalArgumentException, mask_user_visible};
use crate::streaming::attached;

const MONTHS_CONDITION: &str = "_LEGACY_ERROR_TEMP_3262";
const NEGATIVE_TEXT: &str = "requirement failed: the interval of trigger should not be negative";
const OVERFLOW_TEXT: &str = "long overflow";
const MICROS_PER_MILLI: i128 = 1_000;
const MICROS_PER_SECOND: i128 = 1_000_000;
const MICROS_PER_MINUTE: i128 = 60_000_000;
const MICROS_PER_HOUR: i128 = 3_600_000_000;
const MICROS_PER_DAY: i128 = 86_400_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TriggerRefusal {
    InvalidFormat {
        condition: &'static str,
        text: String,
        params: Vec<(String, String)>,
    },
    Months {
        text: String,
        interval: String,
    },
    Negative,
    Overflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    Millis,
    Micros,
    Seconds,
    Minutes,
    Hours,
    Days,
    Weeks,
    Months,
    Years,
}

fn unit_of(word: &str) -> Option<Unit> {
    match word {
        "millisecond" | "milliseconds" => Some(Unit::Millis),
        "microsecond" | "microseconds" => Some(Unit::Micros),
        "second" | "seconds" => Some(Unit::Seconds),
        "minute" | "minutes" => Some(Unit::Minutes),
        "hour" | "hours" => Some(Unit::Hours),
        "day" | "days" => Some(Unit::Days),
        "week" | "weeks" => Some(Unit::Weeks),
        "month" | "months" => Some(Unit::Months),
        "year" | "years" => Some(Unit::Years),
        _ => None,
    }
}

fn subday_factor(unit: Unit) -> Option<i128> {
    match unit {
        Unit::Millis => Some(MICROS_PER_MILLI),
        Unit::Micros => Some(1),
        Unit::Seconds => Some(MICROS_PER_SECOND),
        Unit::Minutes => Some(MICROS_PER_MINUTE),
        Unit::Hours => Some(MICROS_PER_HOUR),
        Unit::Days | Unit::Weeks | Unit::Months | Unit::Years => None,
    }
}

struct ParsedNumber {
    negative: bool,
    echo: String,
    int_value: Option<i128>,
    frac_digits: Option<String>,
}

fn is_java_space(value: char) -> bool {
    matches!(value, ' ' | '\t' | '\n' | '\u{B}' | '\u{C}' | '\r')
}

fn split_java_words(body: &str) -> Vec<&str> {
    body.split(is_java_space)
        .filter(|word| !word.is_empty())
        .collect()
}

fn parse_i128_checked(digits: &str) -> Option<i128> {
    let mut value: i128 = 0;
    for byte in digits.bytes() {
        value = value
            .checked_mul(10)?
            .checked_add(i128::from(byte - b'0'))?;
    }
    Some(value)
}

fn split_unsigned(word: &str) -> Option<(String, Option<String>)> {
    if let Some((head, tail)) = word.split_once('.') {
        if tail.contains('.') {
            return None;
        }
        if head.is_empty() && tail.is_empty() {
            return None;
        }
        if !head.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        if !tail.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        return Some((head.to_owned(), Some(tail.to_owned())));
    }
    if word.is_empty() || !word.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((word.to_owned(), None))
}

fn invalid_format(
    trimmed: &str,
    condition: &'static str,
    detail: &str,
    params: Vec<(String, String)>,
) -> TriggerRefusal {
    TriggerRefusal::InvalidFormat {
        condition,
        text: format!(
            "[{condition}] Error parsing '{trimmed}' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. {detail} SQLSTATE: 22006"
        ),
        params,
    }
}

fn with_input(trimmed: &str, extra: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut params = vec![("input".to_owned(), trimmed.to_owned())];
    params.extend(extra);
    params
}

fn empty_refusal(trimmed: &str) -> TriggerRefusal {
    invalid_format(
        trimmed,
        "INVALID_INTERVAL_FORMAT.INPUT_IS_EMPTY",
        "Interval string cannot be empty.",
        with_input(trimmed, Vec::new()),
    )
}

fn unrecognized_number(trimmed: &str, word: &str) -> TriggerRefusal {
    invalid_format(
        trimmed,
        "INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER",
        &format!("Unrecognized number {word}."),
        with_input(trimmed, vec![("number".to_owned(), word.to_owned())]),
    )
}

fn missing_unit(trimmed: &str, echo: &str) -> TriggerRefusal {
    invalid_format(
        trimmed,
        "INVALID_INTERVAL_FORMAT.MISSING_UNIT",
        &format!("Expect a unit name after {echo} but hit EOL."),
        with_input(trimmed, vec![("word".to_owned(), echo.to_owned())]),
    )
}

fn invalid_unit(trimmed: &str, word: &str) -> TriggerRefusal {
    invalid_format(
        trimmed,
        "INVALID_INTERVAL_FORMAT.INVALID_UNIT",
        &format!("Invalid unit {word}."),
        with_input(trimmed, vec![("unit".to_owned(), word.to_owned())]),
    )
}

fn invalid_value(trimmed: &str, word: &str) -> TriggerRefusal {
    invalid_format(
        trimmed,
        "INVALID_INTERVAL_FORMAT.INVALID_VALUE",
        &format!("Invalid value {word}."),
        with_input(trimmed, vec![("value".to_owned(), word.to_owned())]),
    )
}

fn invalid_fraction(trimmed: &str, word: &str) -> TriggerRefusal {
    invalid_format(
        trimmed,
        "INVALID_INTERVAL_FORMAT.INVALID_FRACTION",
        &format!("{word} cannot have fractional part."),
        with_input(trimmed, vec![("unit".to_owned(), word.to_owned())]),
    )
}

fn arithmetic_wrapped(trimmed: &str) -> TriggerRefusal {
    invalid_format(
        trimmed,
        "INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION",
        &format!("Uncaught arithmetic exception while parsing '{trimmed}'."),
        with_input(trimmed, Vec::new()),
    )
}

fn invalid_prefix(trimmed: &str, word: &str) -> TriggerRefusal {
    invalid_format(
        trimmed,
        "INVALID_INTERVAL_FORMAT.INVALID_PREFIX",
        &format!("Invalid interval prefix {word}."),
        with_input(trimmed, vec![("prefix".to_owned(), word.to_owned())]),
    )
}

fn missing_number(trimmed: &str, sign: &str) -> TriggerRefusal {
    invalid_format(
        trimmed,
        "INVALID_INTERVAL_FORMAT.MISSING_NUMBER",
        &format!("Expect a number after {sign} but hit EOL."),
        with_input(trimmed, vec![("word".to_owned(), sign.to_owned())]),
    )
}

fn strip_prefix<'a>(trimmed: &str, folded: &'a str) -> Result<&'a str, TriggerRefusal> {
    if folded == "interval" {
        return Err(empty_refusal(trimmed));
    }
    if let Some(rest) = folded.strip_prefix("interval") {
        if rest.starts_with(is_java_space) {
            return Ok(rest);
        }
        let Some(prefix) = folded.split(is_java_space).find(|word| !word.is_empty()) else {
            return Err(empty_refusal(trimmed));
        };
        return Err(invalid_prefix(trimmed, prefix));
    }
    Ok(folded)
}

fn make_number(negative: bool, echo: &str, head: &str, tail: Option<&str>) -> ParsedNumber {
    ParsedNumber {
        negative,
        echo: echo.to_owned(),
        int_value: parse_i128_checked(head),
        frac_digits: tail.map(str::to_owned),
    }
}

fn parse_number(
    trimmed: &str,
    word: &str,
    next: Option<&str>,
) -> Result<(ParsedNumber, bool), TriggerRefusal> {
    if word == "+" || word == "-" {
        let negative = word == "-";
        let Some(following) = next else {
            return Err(missing_number(trimmed, word));
        };
        let Some((head, tail)) = split_unsigned(following) else {
            return Err(invalid_value(trimmed, following));
        };
        return Ok((
            make_number(negative, following, &head, tail.as_deref()),
            true,
        ));
    }
    if let Some(rest) = word.strip_prefix('-') {
        let Some((head, tail)) = split_unsigned(rest) else {
            return Err(invalid_value(trimmed, word));
        };
        return Ok((make_number(true, word, &head, tail.as_deref()), false));
    }
    if let Some(rest) = word.strip_prefix('+') {
        let Some((head, tail)) = split_unsigned(rest) else {
            return Err(invalid_value(trimmed, word));
        };
        return Ok((make_number(false, word, &head, tail.as_deref()), false));
    }
    let digits_start = word
        .bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_digit() || byte == b'.');
    if !digits_start {
        return Err(unrecognized_number(trimmed, word));
    }
    let Some((head, tail)) = split_unsigned(word) else {
        return Err(invalid_value(trimmed, word));
    };
    Ok((make_number(false, word, &head, tail.as_deref()), false))
}

fn int_count(trimmed: &str, number: &ParsedNumber) -> Result<i32, TriggerRefusal> {
    let Some(value) = number.int_value else {
        return Err(arithmetic_wrapped(trimmed));
    };
    let signed = if number.negative { -value } else { value };
    i32::try_from(signed).map_err(|_| arithmetic_wrapped(trimmed))
}

fn scale_frac(frac: &str) -> i128 {
    let mut scaled: i128 = 0;
    for byte in frac.bytes().take(6) {
        scaled = scaled * 10 + i128::from(byte - b'0');
    }
    for _ in frac.len()..6 {
        scaled *= 10;
    }
    scaled
}

fn subday_micros(
    trimmed: &str,
    number: &ParsedNumber,
    factor: i128,
) -> Result<i64, TriggerRefusal> {
    let micros = match (&number.int_value, &number.frac_digits) {
        (Some(int), None) => int
            .checked_mul(factor)
            .ok_or_else(|| arithmetic_wrapped(trimmed))?,
        (Some(int), Some(frac)) => int
            .checked_mul(MICROS_PER_SECOND)
            .and_then(|base| base.checked_add(scale_frac(frac)))
            .ok_or_else(|| arithmetic_wrapped(trimmed))?,
        (None, _) => return Err(arithmetic_wrapped(trimmed)),
    };
    let signed = if number.negative { -micros } else { micros };
    i64::try_from(signed).map_err(|_| arithmetic_wrapped(trimmed))
}

fn convert_group(
    trimmed: &str,
    number: &ParsedNumber,
    unit: Unit,
) -> Result<(i64, i64), TriggerRefusal> {
    match unit {
        Unit::Months | Unit::Years => {
            let count = int_count(trimmed, number)?;
            let months = if unit == Unit::Years {
                count
                    .checked_mul(12)
                    .ok_or_else(|| arithmetic_wrapped(trimmed))?
            } else {
                count
            };
            Ok((i64::from(months), 0))
        }
        Unit::Days | Unit::Weeks => {
            let count = int_count(trimmed, number)?;
            let days = if unit == Unit::Weeks {
                count
                    .checked_mul(7)
                    .ok_or_else(|| arithmetic_wrapped(trimmed))?
            } else {
                count
            };
            let micros = i128::from(days) * MICROS_PER_DAY;
            let micros = i64::try_from(micros).map_err(|_| TriggerRefusal::Overflow)?;
            Ok((0, micros))
        }
        _ => {
            let Some(factor) = subday_factor(unit) else {
                return Err(arithmetic_wrapped(trimmed));
            };
            let micros = subday_micros(trimmed, number, factor)?;
            Ok((0, micros))
        }
    }
}

pub(crate) fn parse_trigger_millis(input: &str) -> Result<u64, TriggerRefusal> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(empty_refusal(trimmed));
    }
    let folded = trimmed.to_lowercase();
    let body = strip_prefix(trimmed, &folded)?;
    let words = split_java_words(body);
    if words.is_empty() {
        return Err(empty_refusal(trimmed));
    }
    let mut months: i64 = 0;
    let mut total_micros: i64 = 0;
    let mut index = 0;
    while let Some(word) = words.get(index).copied() {
        let next = words.get(index + 1).copied();
        let (number, consumed_next) = parse_number(trimmed, word, next)?;
        index += if consumed_next { 2 } else { 1 };
        let Some(unit_word) = words.get(index).copied() else {
            return Err(missing_unit(trimmed, &number.echo));
        };
        index += 1;
        let Some(unit) = unit_of(unit_word) else {
            return Err(invalid_unit(trimmed, unit_word));
        };
        if number.frac_digits.is_some() && unit != Unit::Seconds {
            return Err(invalid_fraction(trimmed, unit_word));
        }
        let (months_delta, micros_delta) = convert_group(trimmed, &number, unit)?;
        months = months
            .checked_add(months_delta)
            .filter(|value| i32::try_from(*value).is_ok())
            .ok_or_else(|| arithmetic_wrapped(trimmed))?;
        total_micros = total_micros
            .checked_add(micros_delta)
            .ok_or(TriggerRefusal::Overflow)?;
    }
    if months != 0 {
        return Err(TriggerRefusal::Months {
            text: format!("Doesn't support month or year interval: {trimmed}"),
            interval: trimmed.to_owned(),
        });
    }
    if total_micros < 0 {
        return Err(TriggerRefusal::Negative);
    }
    u64::try_from(total_micros / 1000).map_err(|_| TriggerRefusal::Negative)
}

impl TriggerRefusal {
    fn into_pyerr(self, py: Python<'_>) -> PyErr {
        match self {
            TriggerRefusal::InvalidFormat {
                condition,
                text,
                params,
            } => {
                let raised = IllegalArgumentException::new_err(mask_user_visible(text));
                let pairs: Vec<(&str, &str)> = params
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .collect();
                attached(py, raised, condition, &pairs)
            }
            TriggerRefusal::Months { text, interval } => {
                let raised = IllegalArgumentException::new_err(mask_user_visible(text));
                attached(
                    py,
                    raised,
                    MONTHS_CONDITION,
                    &[("interval", interval.as_str())],
                )
            }
            TriggerRefusal::Negative => IllegalArgumentException::new_err(NEGATIVE_TEXT),
            TriggerRefusal::Overflow => ArithmeticException::new_err(OVERFLOW_TEXT),
        }
    }
}

#[pyfunction]
#[allow(clippy::missing_errors_doc)]
pub(crate) fn check_trigger_interval(text: &str) -> PyResult<u64> {
    parse_trigger_millis(text).map_err(|refusal| Python::attach(|py| refusal.into_pyerr(py)))
}

#[allow(clippy::missing_errors_doc)]
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(check_trigger_interval, module)?)?;
    Ok(())
}

#[cfg(test)]
#[path = "trigger_interval_edge_tests.rs"]
mod edge_tests;
#[cfg(test)]
#[path = "trigger_interval_tests.rs"]
mod tests;
