use datafusion::common::{DataFusionError, Result};
use regex::Regex;

pub(crate) const FANCY_BACKTRACK_LIMIT: usize = 10_000_000;
pub(crate) const FANCY_LOOP_HAYSTACK_MAX: usize = 10_000;

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Engine {
    Plain,
    Fancy { loops: bool },
}

#[derive(Clone)]
pub(crate) enum SparkRegex {
    Plain(Regex),
    Fancy(FancyCompiled),
}

impl std::fmt::Debug for SparkRegex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SparkRegex::Plain(regex) => regex.fmt(formatter),
            SparkRegex::Fancy(compiled) => formatter
                .debug_struct("Fancy")
                .field("pattern", &compiled.java_pattern)
                .finish(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct FancyCompiled {
    regex: fancy_regex::Regex,
    java_pattern: String,
    loops: bool,
}

pub(crate) fn compile_spark_regex(pattern: &str, fn_name: &str) -> Result<SparkRegex> {
    let groups = count_groups(pattern);
    let rewritten = rewrite_out_of_range_octal(pattern, groups);
    let normalized = crate::spark_regex_lookbehind::normalize_lookbehind(&rewritten);
    match scan_features(&normalized) {
        ScanVerdict::InvalidJava => Err(invalid_pattern_error(
            fn_name,
            pattern,
            &translate_pattern(&normalized),
            None,
        )),
        ScanVerdict::Plain => {
            let translated = translate_pattern(&normalized);
            Regex::new(&translated)
                .map(SparkRegex::Plain)
                .map_err(|error| {
                    invalid_pattern_error(fn_name, pattern, &translated, Some(error.to_string()))
                })
        }
        ScanVerdict::Fancy { loops } => {
            let translated = translate_pattern(&normalized);
            fancy_regex::RegexBuilder::new(&translated)
                .backtrack_limit(FANCY_BACKTRACK_LIMIT)
                .build()
                .map(|regex| {
                    SparkRegex::Fancy(FancyCompiled {
                        regex,
                        java_pattern: pattern.to_owned(),
                        loops,
                    })
                })
                .map_err(|error| {
                    invalid_pattern_error(fn_name, pattern, &translated, Some(error.to_string()))
                })
        }
    }
}

fn invalid_pattern_error(
    fn_name: &str,
    pattern: &str,
    translated: &str,
    detail: Option<String>,
) -> DataFusionError {
    if fn_name == "split" {
        match detail {
            Some(detail) => DataFusionError::Execution(format!(
                "invalid regular expression '{translated}': {detail}"
            )),
            None => {
                DataFusionError::Execution(format!("invalid regular expression '{translated}'"))
            }
        }
    } else {
        DataFusionError::Execution(format!(
            "[INVALID_PARAMETER_VALUE.PATTERN] The value of parameter(s) `regexp` in `{fn_name}` \
             is invalid: '{pattern}'. SQLSTATE: 22023"
        ))
    }
}

fn overrun_error(java_pattern: &str, budget: &str, limit: usize) -> DataFusionError {
    DataFusionError::Execution(format!(
        "regex overrun on pattern '{java_pattern}': exceeded {budget} (limit {limit})"
    ))
}

fn runtime_error(java_pattern: &str, error: fancy_regex::Error) -> DataFusionError {
    match error {
        fancy_regex::Error::RuntimeError(fancy_regex::RuntimeError::BacktrackLimitExceeded) => {
            overrun_error(java_pattern, "backtrack budget", FANCY_BACKTRACK_LIMIT)
        }
        other => DataFusionError::Execution(format!(
            "regex runtime failure on pattern '{java_pattern}': {other}"
        )),
    }
}

pub(crate) fn strip_dollar_braces(replacement: &str) -> String {
    let mut out = String::with_capacity(replacement.len());
    let mut rest = replacement;
    while let Some(dollar) = rest.find('$') {
        out.push_str(&rest[..dollar]);
        let after = &rest[dollar + 1..];
        if let Some(braced) = after.strip_prefix('{')
            && let Some(close) = braced.find('}')
        {
            rest = &braced[close + 1..];
        } else {
            out.push('$');
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ScanVerdict {
    Plain,
    Fancy { loops: bool },
    InvalidJava,
}

fn scan_features(pattern: &str) -> ScanVerdict {
    let bytes = pattern.as_bytes();
    let mut index = 0;
    let mut fancy = false;
    let mut loops = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                index += 1;
                if index >= bytes.len() {
                    break;
                }
                match bytes[index] {
                    b'Q' => {
                        index += 1;
                        while index < bytes.len() {
                            if bytes[index] == b'\\' && bytes.get(index + 1) == Some(&b'E') {
                                index += 2;
                                break;
                            }
                            index += 1;
                        }
                    }
                    b'1'..=b'9' => {
                        fancy = true;
                        index += 1;
                    }
                    b'k' => {
                        match bytes.get(index + 1) {
                            Some(b'<') => fancy = true,
                            Some(b'\'') => return ScanVerdict::InvalidJava,
                            _ => {}
                        }
                        index += 1;
                    }
                    _ => index += 1,
                }
            }
            b'[' => {
                index = skip_class(bytes, index);
            }
            b'(' => {
                match group_kind(bytes, index) {
                    GroupKind::Lookahead | GroupKind::Lookbehind | GroupKind::Atomic => {
                        fancy = true;
                    }
                    GroupKind::Invalid => return ScanVerdict::InvalidJava,
                    GroupKind::Consuming => {
                        if let Some(close) = match_group(bytes, index)
                            && has_open_quantifier(bytes, close + 1)
                        {
                            fancy = true;
                        }
                    }
                    GroupKind::Other => {}
                }
                index += 1;
            }
            b'*' | b'+' => {
                fancy = fancy || bytes.get(index + 1) == Some(&b'+');
                loops = true;
                index += 1;
            }
            b'?' => {
                fancy = fancy || bytes.get(index + 1) == Some(&b'+');
                index += 1;
            }
            b'{' => {
                if is_open_quantifier(bytes, index) {
                    loops = true;
                }
                index += 1;
            }
            b'}' => {
                if bytes.get(index + 1) == Some(&b'+') {
                    let mut back = index;
                    while back > 0 && (bytes[back - 1].is_ascii_digit() || bytes[back - 1] == b',')
                    {
                        back -= 1;
                    }
                    if back > 0
                        && bytes[back - 1] == b'{'
                        && bytes.get(back).is_some_and(u8::is_ascii_digit)
                    {
                        fancy = true;
                    }
                }
                index += 1;
            }
            _ => index += 1,
        }
    }
    if fancy {
        ScanVerdict::Fancy { loops }
    } else {
        ScanVerdict::Plain
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupKind {
    Lookahead,
    Lookbehind,
    Atomic,
    Consuming,
    Other,
    Invalid,
}

pub(crate) fn group_kind(bytes: &[u8], open: usize) -> GroupKind {
    if bytes.get(open + 1) != Some(&b'?') {
        return GroupKind::Consuming;
    }
    match bytes.get(open + 2) {
        Some(b'=' | b'!') => GroupKind::Lookahead,
        Some(b'>') => GroupKind::Atomic,
        Some(b'P') => GroupKind::Invalid,
        Some(b'<') => match bytes.get(open + 3) {
            Some(b'=' | b'!') => GroupKind::Lookbehind,
            _ => GroupKind::Consuming,
        },
        Some(b'\'') => GroupKind::Consuming,
        _ => group_kind_flags(bytes, open),
    }
}

fn group_kind_flags(bytes: &[u8], open: usize) -> GroupKind {
    let mut index = open + 2;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_lowercase() || *byte == b'-')
    {
        index += 1;
    }
    if index > open + 2 && bytes.get(index) == Some(&b':') {
        GroupKind::Consuming
    } else {
        GroupKind::Other
    }
}

pub(crate) fn match_group(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut index = open;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                if bytes.get(index + 1) == Some(&b'Q') {
                    index = find_quote_end(bytes, index + 2).saturating_sub(1);
                } else {
                    index += 1;
                }
            }
            b'[' => index = skip_class(bytes, index).saturating_sub(1),
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
        index += 1;
    }
    None
}

fn has_open_quantifier(bytes: &[u8], index: usize) -> bool {
    match bytes.get(index) {
        Some(b'*' | b'+') => true,
        Some(b'{') => is_open_quantifier(bytes, index),
        _ => false,
    }
}

fn is_open_quantifier(bytes: &[u8], open: usize) -> bool {
    let mut index = open + 1;
    if bytes.get(index).is_none_or(|byte| !byte.is_ascii_digit()) {
        return false;
    }
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
    }
    if bytes.get(index) == Some(&b',') {
        index += 1;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        return bytes.get(index) == Some(&b'}');
    }
    false
}

pub(crate) fn skip_class(bytes: &[u8], open: usize) -> usize {
    let mut index = open + 1;
    if bytes.get(index) == Some(&b'^') {
        index += 1;
    }
    if bytes.get(index) == Some(&b']') {
        index += 1;
    }
    while index < bytes.len() && bytes[index] != b']' {
        if bytes[index] == b'\\' {
            index += 1;
        }
        index += 1;
    }
    index + 1
}

fn count_groups(pattern: &str) -> usize {
    let bytes = pattern.as_bytes();
    let mut index = 0;
    let mut groups = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                if bytes.get(index + 1) == Some(&b'Q') {
                    index = find_quote_end(bytes, index + 2).saturating_sub(1);
                } else {
                    index += 1;
                }
            }
            b'[' => index = skip_class(bytes, index).saturating_sub(1),
            b'(' if group_kind(bytes, index) == GroupKind::Consuming => groups += 1,
            _ => {}
        }
        index += 1;
    }
    groups
}

fn rewrite_out_of_range_octal(pattern: &str, groups: usize) -> String {
    if !pattern.contains('\\') {
        return pattern.to_owned();
    }
    let bytes = pattern.as_bytes();
    let mut out = String::with_capacity(pattern.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            if let Some((rewritten, width)) = rewrite_escape(bytes, index, groups) {
                out.push_str(&rewritten);
                index += width;
                continue;
            }
            if index + 1 < bytes.len() && bytes[index + 1] == b'Q' {
                let end = find_quote_end(bytes, index + 2);
                out.push_str(&pattern[index..end]);
                index = end;
                continue;
            }
            if index + 1 < bytes.len() && bytes[index + 1] == b'\\' {
                out.push_str(&pattern[index..=index + 1]);
                index += 2;
                continue;
            }
        }
        if bytes[index] == b'[' {
            let end = skip_class(bytes, index);
            out.push_str(&pattern[index..end.min(pattern.len())]);
            index = end;
            continue;
        }
        let width = pattern[index..].chars().next().map_or(1, char::len_utf8);
        out.push_str(&pattern[index..index + width]);
        index += width;
    }
    out
}

fn rewrite_escape(bytes: &[u8], index: usize, groups: usize) -> Option<(String, usize)> {
    let first = *bytes.get(index + 1)?;
    if !first.is_ascii_digit() || first == b'0' {
        return None;
    }
    let digit = (first - b'0') as usize;
    let mut value = digit;
    let mut width = 1;
    if bytes.get(index + 2).is_some_and(u8::is_ascii_digit) {
        value = digit * 10 + (bytes[index + 2] - b'0') as usize;
        width = 2;
    }
    if value >= 1 && value <= groups {
        return Some((
            String::from_utf8_lossy(&bytes[index..=index + width]).into_owned(),
            1 + width,
        ));
    }
    if width == 2 && digit <= groups {
        return Some((
            String::from_utf8_lossy(&bytes[index..=index + 1]).into_owned(),
            2,
        ));
    }
    if width == 2 && value <= 0o77 {
        return Some((format!("\\x{value:02X}"), 1 + width));
    }
    if (1..=7).contains(&digit) {
        return Some((format!("\\x0{digit}"), 2));
    }
    None
}

pub(crate) fn find_quote_end(bytes: &[u8], start: usize) -> usize {
    let mut index = start;
    while index < bytes.len() {
        if bytes[index] == b'\\' && bytes.get(index + 1) == Some(&b'E') {
            return index + 2;
        }
        index += 1;
    }
    bytes.len()
}

pub(crate) fn translate_pattern(pattern: &str) -> String {
    let quoted = translate_java_quotations(pattern);
    let classes = crate::java_regex::translate_java_char_classes(&quoted);
    crate::collection::bind_ascii_perl_classes(&classes)
}

fn translate_java_quotations(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut rest = pattern;
    while let Some(start) = rest.find("\\Q") {
        out.push_str(&rest[..start]);
        let quoted = &rest[start + 2..];
        if let Some(end) = quoted.find("\\E") {
            out.push_str(&regex::escape(&quoted[..end]));
            rest = &quoted[end + 2..];
        } else {
            out.push_str(&regex::escape(quoted));
            rest = "";
        }
    }
    out.push_str(rest);
    out
}

fn count_overflow() -> DataFusionError {
    DataFusionError::Execution("regexp_count exceeds Spark INT".to_owned())
}

fn bump_count(count: i32) -> Result<i32> {
    count.checked_add(1).ok_or_else(count_overflow)
}

const MID_SURROGATE_PROBE: &str = "\u{FFFD}\u{FFFD}";
const MID_SURROGATE_PROBE_OFFSET: usize = 3;

impl SparkRegex {
    #[cfg(test)]
    pub(crate) fn engine(&self) -> Engine {
        match self {
            SparkRegex::Plain(_) => Engine::Plain,
            SparkRegex::Fancy(compiled) => Engine::Fancy {
                loops: compiled.loops,
            },
        }
    }

    fn check_haystack(&self, text: &str) -> Result<()> {
        if let SparkRegex::Fancy(compiled) = self
            && compiled.loops
            && text.len() > FANCY_LOOP_HAYSTACK_MAX
        {
            return Err(overrun_error(
                &compiled.java_pattern,
                "looping-pattern haystack",
                FANCY_LOOP_HAYSTACK_MAX,
            ));
        }
        Ok(())
    }

    fn overrun(&self, error: fancy_regex::Error) -> DataFusionError {
        match self {
            SparkRegex::Fancy(compiled) => runtime_error(&compiled.java_pattern, error),
            SparkRegex::Plain(_) => {
                DataFusionError::Internal("plain regex has no runtime failure".to_owned())
            }
        }
    }

    pub(crate) fn is_empty_pattern(&self) -> bool {
        match self {
            SparkRegex::Plain(regex) => regex.as_str().is_empty(),
            SparkRegex::Fancy(compiled) => compiled.regex.as_str().is_empty(),
        }
    }

    pub(crate) fn captures_len(&self) -> usize {
        match self {
            SparkRegex::Plain(regex) => regex.captures_len(),
            SparkRegex::Fancy(compiled) => compiled.regex.captures_len(),
        }
    }

    pub(crate) fn is_match(&self, text: &str) -> Result<bool> {
        self.check_haystack(text)?;
        match self {
            SparkRegex::Plain(regex) => Ok(regex.is_match(text)),
            SparkRegex::Fancy(compiled) => compiled
                .regex
                .is_match(text)
                .map_err(|error| self.overrun(error)),
        }
    }

    pub(crate) fn find_first(&self, text: &str) -> Result<Option<(usize, usize)>> {
        self.check_haystack(text)?;
        match self {
            SparkRegex::Plain(regex) => {
                Ok(regex.find(text).map(|found| (found.start(), found.end())))
            }
            SparkRegex::Fancy(compiled) => compiled
                .regex
                .find(text)
                .map(|matched| matched.map(|found| (found.start(), found.end())))
                .map_err(|error| self.overrun(error)),
        }
    }

    pub(crate) fn capture_at(
        &self,
        text: &str,
        start: usize,
        group: usize,
    ) -> Result<Option<String>> {
        match self {
            SparkRegex::Plain(regex) => Ok(regex
                .captures_at(text, start)
                .and_then(|caps| caps.get(group).map(|matched| matched.as_str().to_owned()))),
            SparkRegex::Fancy(compiled) => compiled
                .regex
                .captures_from_pos(text, start)
                .map(|caps| {
                    caps.and_then(|captures| {
                        captures
                            .get(group)
                            .map(|matched| matched.as_str().to_owned())
                    })
                })
                .map_err(|error| self.overrun(error)),
        }
    }

    pub(crate) fn find_from(&self, text: &str, start: usize) -> Result<Option<(usize, usize)>> {
        match self {
            SparkRegex::Plain(regex) => Ok(regex
                .find_at(text, start)
                .map(|found| (found.start(), found.end()))),
            SparkRegex::Fancy(compiled) => compiled
                .regex
                .find_from_pos(text, start)
                .map(|matched| matched.map(|found| (found.start(), found.end())))
                .map_err(|error| self.overrun(error)),
        }
    }

    pub(crate) fn matches_at_mid_surrogate_index(&self) -> Result<bool> {
        match self {
            SparkRegex::Plain(regex) => Ok(regex
                .find_at(MID_SURROGATE_PROBE, MID_SURROGATE_PROBE_OFFSET)
                .is_some_and(|found| found.start() == MID_SURROGATE_PROBE_OFFSET)),
            SparkRegex::Fancy(compiled) => compiled
                .regex
                .find_from_pos(MID_SURROGATE_PROBE, MID_SURROGATE_PROBE_OFFSET)
                .map(|matched| {
                    matched.is_some_and(|found| found.start() == MID_SURROGATE_PROBE_OFFSET)
                })
                .map_err(|error| self.overrun(error)),
        }
    }

    pub(crate) fn collect_matches(
        &self,
        text: &str,
        max_matches: usize,
    ) -> Result<Vec<(usize, usize)>> {
        self.check_haystack(text)?;
        let mut found_all = Vec::new();
        if max_matches == 0 {
            return Ok(found_all);
        }
        if self.is_empty_pattern() {
            for offset in text
                .char_indices()
                .map(|(offset, _)| offset)
                .chain([text.len()])
            {
                if let Some(found) = self.find_from(text, offset)? {
                    found_all.push(found);
                }
            }
            return Ok(found_all);
        }
        let mut byte = 0usize;
        loop {
            if byte > text.len() {
                break;
            }
            let Some(found) = self.find_from(text, byte)? else {
                break;
            };
            found_all.push(found);
            if found_all.len() >= max_matches {
                break;
            }
            if found_all.len() > usize::try_from(i32::MAX).unwrap_or(usize::MAX) {
                return Err(count_overflow());
            }
            if found.0 == found.1 {
                if found.0 == text.len() {
                    break;
                }
                let Some(character) = text[found.0..].chars().next() else {
                    break;
                };
                byte = found.0 + character.len_utf8();
            } else {
                byte = found.1;
            }
        }
        Ok(found_all)
    }

    pub(crate) fn count_non_overlapping(&self, text: &str) -> Result<i32> {
        self.check_haystack(text)?;
        if self.is_empty_pattern() {
            let count = text.encode_utf16().count().saturating_add(1);
            return i32::try_from(count).map_err(|_| count_overflow());
        }
        let mut count: i32 = 0;
        let mut byte = 0usize;
        let mut mid_surrogate = false;
        loop {
            if mid_surrogate {
                if self.matches_at_mid_surrogate_index()? {
                    count = bump_count(count)?;
                }
                let Some(character) = text.get(byte..).and_then(|rest| rest.chars().next()) else {
                    break;
                };
                byte += character.len_utf8();
                mid_surrogate = false;
                continue;
            }
            if byte > text.len() {
                break;
            }
            let Some(found) = self.find_from(text, byte)? else {
                break;
            };
            count = bump_count(count)?;
            if found.0 == found.1 {
                if found.0 == text.len() {
                    break;
                }
                let Some(character) = text[found.0..].chars().next() else {
                    break;
                };
                if character.len_utf16() == 2 {
                    mid_surrogate = true;
                    byte = found.0;
                } else {
                    byte = found.0 + character.len_utf8();
                }
            } else {
                byte = found.1;
            }
        }
        Ok(count)
    }

    pub(crate) fn instr_start(&self, text: &str) -> Result<i32> {
        let Some(found) = self.find_first(text)? else {
            return Ok(0);
        };
        let units_before = text[..found.0].encode_utf16().count();
        let start = units_before.saturating_add(1);
        i32::try_from(start)
            .map_err(|_| DataFusionError::Execution("regexp_instr exceeds Spark INT".to_owned()))
    }

    pub(crate) fn replace_all(&self, text: &str, replacement: &str) -> Result<String> {
        self.check_haystack(text)?;
        let stripped = strip_dollar_braces(replacement);
        match self {
            SparkRegex::Plain(regex) => Ok(regex.replace_all(text, stripped.as_str()).into_owned()),
            SparkRegex::Fancy(compiled) => {
                let mut out = String::with_capacity(text.len());
                let mut last = 0usize;
                for (start, end) in self.collect_matches(text, usize::MAX)? {
                    out.push_str(&text[last..start]);
                    match compiled.regex.captures_from_pos(text, start) {
                        Ok(Some(caps)) => caps.expand(&stripped, &mut out),
                        Ok(None) => out.push_str(&stripped),
                        Err(error) => return Err(self.overrun(error)),
                    }
                    last = end;
                }
                out.push_str(&text[last..]);
                Ok(out)
            }
        }
    }
}

#[cfg(test)]
mod tests;
