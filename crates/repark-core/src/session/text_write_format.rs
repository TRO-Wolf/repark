use std::collections::HashMap;

use arrow::array::timezone::Tz;
use chrono::{FixedOffset, NaiveDateTime, Offset as _};
use datafusion::common::DataFusionError;
use repark_common::Error;

pub mod fast;
pub mod file_format;
pub mod render;
pub mod select;
pub mod serializer;
pub mod sink;
pub mod spec;
pub mod udf;

const GUIDE_URL: &str = "https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PatternKind {
    Timestamp,
    TimestampNtz,
    Date,
}

#[derive(Clone, Debug)]
pub enum PatternToken {
    Literal(String),
    Field { letter: char, count: usize },
    OpenSection,
    CloseSection,
}

#[derive(Clone, Debug)]
pub struct CompiledPattern {
    tokens: Vec<PatternToken>,
    matching: Vec<Option<usize>>,
    has_era: bool,
}

#[derive(Clone, Debug)]
pub struct PatternFailure {
    message: String,
    illegal_argument: bool,
}

impl PatternFailure {
    fn invalid_suggestion(pattern: &str) -> Self {
        Self {
            message: format!(
                "[INVALID_DATETIME_PATTERN.WITH_SUGGESTION] Unrecognized datetime pattern: \
                 '{pattern}'. You can form a valid datetime pattern with the guide from \
                 '{GUIDE_URL}'. SQLSTATE: 22007"
            ),
            illegal_argument: false,
        }
    }

    fn illegal_character(pattern: &str, letter: char) -> Self {
        Self {
            message: format!(
                "[INVALID_DATETIME_PATTERN.ILLEGAL_CHARACTER] Unrecognized datetime pattern: \
                 {pattern}. Illegal pattern character found in datetime pattern: {letter}. \
                 Please provide legal character. SQLSTATE: 22007"
            ),
            illegal_argument: true,
        }
    }

    fn level(run: &str) -> Self {
        Self {
            message: format!(
                "[INVALID_DATETIME_PATTERN.LENGTH] Unrecognized datetime pattern: {run}. Too \
                 many letters in datetime pattern: {run}. Please reduce pattern length. \
                 SQLSTATE: 22007"
            ),
            illegal_argument: true,
        }
    }

    fn recognition(pattern: &str) -> Self {
        Self {
            message: format!(
                "[INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION] You may \
                 get a different result due to the upgrading to Spark >= 3.0:\nFail to \
                 recognize '{pattern}' pattern in the DateTimeFormatter.\nYou can form a \
                 valid datetime pattern with the guide from '{GUIDE_URL}'. SQLSTATE: 42K0B"
            ),
            illegal_argument: false,
        }
    }

    fn week_based(letter: char) -> Self {
        Self {
            message: format!(
                "[INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_WEEK_BASED_PATTERN] You may \
                 get a different result due to the upgrading to Spark >= 3.0:\nAll \
                 week-based patterns are unsupported since Spark 3.0, detected week-based \
                 character: {letter}.\nPlease use the SQL function EXTRACT instead. \
                 SQLSTATE: 42K0B"
            ),
            illegal_argument: true,
        }
    }

    fn lazy(message: String) -> Self {
        Self {
            message,
            illegal_argument: false,
        }
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub fn illegal_argument(&self) -> bool {
        self.illegal_argument
    }
}

pub fn pattern_failure_error(failure: &PatternFailure) -> Error {
    if failure.illegal_argument() {
        Error::IllegalArgument(failure.message().to_string())
    } else {
        Error::DataFusion(failure.message().to_string())
    }
}

pub fn pattern_failure_datafusion(failure: &PatternFailure) -> DataFusionError {
    DataFusionError::Execution(failure.message().to_string())
}

struct PatternScan {
    runs: Vec<(char, usize)>,
    letters: Vec<char>,
    unclosed_quote: bool,
    unmatched_close: bool,
    section_char: bool,
}

fn closing_quote(characters: &[char], open: usize) -> Option<usize> {
    let mut index = open + 1;
    while index < characters.len() {
        if characters[index] == '\'' {
            if characters.get(index + 1) == Some(&'\'') {
                index += 2;
                continue;
            }
            return Some(index + 1);
        }
        index += 1;
    }
    None
}

fn scan_pattern(pattern: &str) -> PatternScan {
    let characters: Vec<char> = pattern.chars().collect();
    let mut runs = Vec::new();
    let mut letters = Vec::new();
    let mut unclosed_quote = false;
    let mut unmatched_close = false;
    let mut section_char = false;
    let mut depth = 0usize;
    let mut index = 0usize;
    while index < characters.len() {
        let current = characters[index];
        if current == '\'' {
            if let Some(end) = closing_quote(&characters, index) {
                index = end;
                continue;
            }
            unclosed_quote = true;
            break;
        }
        if current == '[' {
            depth += 1;
            index += 1;
            continue;
        }
        if current == ']' {
            if depth == 0 {
                unmatched_close = true;
            } else {
                depth -= 1;
            }
            index += 1;
            continue;
        }
        if matches!(current, '{' | '}' | '#') {
            section_char = true;
            index += 1;
            continue;
        }
        if current.is_ascii_alphabetic() {
            let start = index;
            while index < characters.len() && characters[index] == current {
                index += 1;
            }
            runs.push((current, index - start));
            letters.push(current);
            continue;
        }
        index += 1;
    }
    PatternScan {
        runs,
        letters,
        unclosed_quote,
        unmatched_close,
        section_char,
    }
}

fn is_simple_date_format_letter(letter: char) -> bool {
    matches!(
        letter,
        'G' | 'y'
            | 'M'
            | 'd'
            | 'h'
            | 'H'
            | 'm'
            | 's'
            | 'S'
            | 'E'
            | 'D'
            | 'F'
            | 'w'
            | 'W'
            | 'a'
            | 'K'
            | 'k'
            | 'z'
            | 'Z'
    )
}

fn is_conditional_week_letter(letter: char) -> bool {
    matches!(letter, 'w' | 'W' | 'u' | 'Y')
}

fn week_trigger_present(scan: &PatternScan) -> bool {
    if scan.unclosed_quote {
        return true;
    }
    scan.letters.iter().any(|letter| {
        !is_simple_date_format_letter(*letter) && !is_conditional_week_letter(*letter)
    })
}

fn legacy_recognition_triggered(scan: &PatternScan, bare_close: bool) -> bool {
    if scan.section_char || bare_close {
        return true;
    }
    scan.runs.iter().any(|(letter, count)| match letter {
        'w' | 'W' | 'u' | 'Y' => true,
        'F' | 'a' => *count > 1,
        'M' | 'L' | 'E' | 'G' | 'z' => *count > 4,
        'S' => *count > 9,
        'D' => *count > 3,
        'd' | 'h' | 'H' | 'm' | 's' | 'K' | 'k' => *count > 2,
        'Z' => *count > 5,
        'y' => *count > 6,
        _ => false,
    })
}

fn is_unknown_letter(letter: char) -> bool {
    matches!(
        letter,
        'C' | 'R' | 'T' | 'I' | 'J' | 'P' | 'U' | 'i' | 'j' | 'l' | 'o' | 'r' | 't' | 'f' | 'b'
    )
}

fn legacy_close_recognized(scan: &PatternScan) -> bool {
    scan.runs.iter().all(|(letter, _)| {
        is_simple_date_format_letter(*letter) || matches!(letter, 'Y' | 'L' | 'u' | 'X')
    })
}

fn structural_timestamp_failure(scan: &PatternScan) -> bool {
    if scan.unclosed_quote {
        return true;
    }
    for (letter, count) in &scan.runs {
        match letter {
            'V' if *count != 2 => return true,
            'O' if *count != 1 && *count != 4 => return true,
            'X' | 'x' if *count > 5 => return true,
            'v' | 'z' => return true,
            other if is_unknown_letter(*other) => return true,
            _ => {}
        }
    }
    false
}

fn shared_legacy_failure(pattern: &str, scan: &PatternScan) -> Option<PatternFailure> {
    if let Some(found) = scan
        .letters
        .iter()
        .find(|letter| matches!(letter, 'c' | 'e'))
    {
        return Some(PatternFailure::week_based(*found));
    }
    if scan
        .letters
        .iter()
        .any(|letter| is_conditional_week_letter(*letter))
        && week_trigger_present(scan)
    {
        let found = scan
            .letters
            .iter()
            .find(|letter| is_conditional_week_letter(**letter))
            .unwrap_or(&'w');
        return Some(PatternFailure::week_based(*found));
    }
    if let Some(found) = scan
        .letters
        .iter()
        .find(|letter| matches!(letter, 'A' | 'B' | 'n' | 'N' | 'p'))
    {
        return Some(PatternFailure::illegal_character(pattern, *found));
    }
    for (letter, count) in &scan.runs {
        if matches!(letter, 'q' | 'Q') && *count > 4 {
            let run: String = std::iter::repeat_n(*letter, *count).collect();
            return Some(PatternFailure::level(&run));
        }
    }
    if legacy_recognition_triggered(scan, pattern == "]") {
        return Some(PatternFailure::recognition(pattern));
    }
    None
}

fn validate_timestamp_pattern(pattern: &str, scan: &PatternScan) -> Option<PatternFailure> {
    if let Some(failure) = shared_legacy_failure(pattern, scan) {
        return Some(failure);
    }
    if scan.unmatched_close {
        if legacy_close_recognized(scan) {
            return Some(PatternFailure::recognition(pattern));
        }
        return Some(PatternFailure::invalid_suggestion(pattern));
    }
    if structural_timestamp_failure(scan) {
        return Some(PatternFailure::invalid_suggestion(pattern));
    }
    None
}

fn validate_ntz_pattern(pattern: &str, scan: &PatternScan) -> Option<PatternFailure> {
    if shared_legacy_failure(pattern, scan).is_some() || scan.unclosed_quote || scan.unmatched_close
    {
        return Some(PatternFailure::invalid_suggestion(pattern));
    }
    for (letter, count) in &scan.runs {
        let bad = match letter {
            'V' => *count != 2,
            'O' => *count != 1 && *count != 4,
            'X' | 'x' => *count > 5,
            'v' => *count != 1,
            'z' => *count > 4,
            other => is_unknown_letter(*other),
        };
        if bad {
            return Some(PatternFailure::invalid_suggestion(pattern));
        }
    }
    None
}

fn validate_date_pattern(pattern: &str, scan: &PatternScan) -> Option<PatternFailure> {
    if let Some(failure) = shared_legacy_failure(pattern, scan) {
        return Some(failure);
    }
    if scan.unmatched_close && legacy_close_recognized(scan) {
        return Some(PatternFailure::recognition(pattern));
    }
    let characters: Vec<char> = pattern.chars().collect();
    let mut index = 0usize;
    let mut depth = 0usize;
    while index < characters.len() {
        let current = characters[index];
        if current == '\'' {
            if let Some(end) = closing_quote(&characters, index) {
                index = end;
                continue;
            }
            break;
        }
        if current == '[' {
            depth += 1;
            index += 1;
            continue;
        }
        if current == ']' {
            if depth == 0 {
                return Some(PatternFailure::lazy(
                    "Pattern invalid as it contains ] without previous [".to_string(),
                ));
            }
            depth -= 1;
            index += 1;
            continue;
        }
        if current.is_ascii_alphabetic() {
            let start = index;
            while index < characters.len() && characters[index] == current {
                index += 1;
            }
            let count = index - start;
            if is_unknown_letter(current) {
                return Some(PatternFailure::lazy(format!(
                    "Unknown pattern letter: {current}"
                )));
            }
            if current == 'V' && count != 2 {
                return Some(PatternFailure::lazy(
                    "Pattern letter count must be 2: V".to_string(),
                ));
            }
            if current == 'O' && count != 1 && count != 4 {
                return Some(PatternFailure::lazy(
                    "Pattern letter count must be 1 or 4: O".to_string(),
                ));
            }
            if matches!(current, 'X' | 'x') && count > 5 {
                return Some(PatternFailure::lazy(format!(
                    "Pattern letter count must be from 1 to 5: {current}"
                )));
            }
            continue;
        }
        index += 1;
    }
    if scan.unclosed_quote {
        let normalized: String = pattern
            .chars()
            .map(|current| if current == 'y' { 'u' } else { current })
            .collect();
        return Some(PatternFailure::lazy(format!(
            "Pattern ends with an incomplete string literal: {normalized}"
        )));
    }
    None
}

fn compile_tokens(pattern: &str) -> CompiledPattern {
    let characters: Vec<char> = pattern.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0usize;
    while index < characters.len() {
        let current = characters[index];
        if current == '\'' {
            if let Some(end) = closing_quote(&characters, index) {
                let inner: String = characters[index + 1..end - 1].iter().collect();
                if inner.is_empty() {
                    tokens.push(PatternToken::Literal("'".to_string()));
                } else {
                    tokens.push(PatternToken::Literal(inner.replace("''", "'")));
                }
                index = end;
                continue;
            }
            let rest: String = characters[index + 1..].iter().collect();
            tokens.push(PatternToken::Literal(rest.replace("''", "'")));
            index = characters.len();
            continue;
        }
        if current == '[' {
            tokens.push(PatternToken::OpenSection);
            index += 1;
            continue;
        }
        if current == ']' {
            tokens.push(PatternToken::CloseSection);
            index += 1;
            continue;
        }
        if current.is_ascii_alphabetic() {
            let start = index;
            while index < characters.len() && characters[index] == current {
                index += 1;
            }
            tokens.push(PatternToken::Field {
                letter: current,
                count: index - start,
            });
            continue;
        }
        let mut literal = String::new();
        while index < characters.len() {
            let next = characters[index];
            if next == '\'' || next == '[' || next == ']' || next.is_ascii_alphabetic() {
                break;
            }
            literal.push(next);
            index += 1;
        }
        tokens.push(PatternToken::Literal(literal));
    }
    let mut matching = vec![None; tokens.len()];
    let mut stack: Vec<usize> = Vec::new();
    for (position, token) in tokens.iter().enumerate() {
        match token {
            PatternToken::OpenSection => stack.push(position),
            PatternToken::CloseSection => {
                if let Some(open) = stack.pop() {
                    matching[open] = Some(position);
                }
            }
            _ => {}
        }
    }
    let has_era = tokens
        .iter()
        .any(|token| matches!(token, PatternToken::Field { letter: 'G', .. }));
    CompiledPattern {
        tokens,
        matching,
        has_era,
    }
}

pub fn compile_write_pattern(
    pattern: &str,
    kind: PatternKind,
) -> std::result::Result<CompiledPattern, PatternFailure> {
    let scan = scan_pattern(pattern);
    let failure = match kind {
        PatternKind::Timestamp => validate_timestamp_pattern(pattern, &scan),
        PatternKind::TimestampNtz => validate_ntz_pattern(pattern, &scan),
        PatternKind::Date => validate_date_pattern(pattern, &scan),
    };
    if let Some(failure) = failure {
        return Err(failure);
    }
    Ok(compile_tokens(pattern))
}

pub fn micros_to_wall_zone(micros: i64, zone: Tz) -> Option<(NaiveDateTime, FixedOffset)> {
    let instant = chrono::DateTime::from_timestamp_micros(micros)?;
    let zoned = instant.with_timezone(&zone);
    Some((zoned.naive_local(), zoned.offset().fix()))
}

pub fn micros_to_naive_wall(micros: i64) -> Option<NaiveDateTime> {
    chrono::DateTime::from_timestamp_micros(micros).map(|instant| instant.naive_utc())
}

pub fn is_text_write_format_option(lowered: &str) -> bool {
    matches!(
        lowered,
        "timestampformat" | "timestampntzformat" | "dateformat"
    )
}

pub fn write_option_patterns(
    options: &HashMap<String, String>,
) -> (Option<String>, Option<String>, Option<String>) {
    let mut timestamp = None;
    let mut ntz = None;
    let mut date = None;
    let mut ordered: Vec<(&String, &String)> = options.iter().collect();
    ordered.sort();
    for (key, value) in ordered {
        match key.to_ascii_lowercase().as_str() {
            "timestampformat" if timestamp.is_none() => timestamp = Some(value.clone()),
            "timestampntzformat" if ntz.is_none() => ntz = Some(value.clone()),
            "dateformat" if date.is_none() => date = Some(value.clone()),
            _ => {}
        }
    }
    (timestamp, ntz, date)
}
