use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasher;
use std::sync::{Mutex, PoisonError};

pub const REDACTED: &str = "***";

const REGISTERED_VALUE_BOUND: usize = 256;

static REGISTERED_CONFIG_VALUES: Mutex<VecDeque<(String, String)>> = Mutex::new(VecDeque::new());

pub fn register_config_value(value: &str) {
    let masked = mask_value_credentials(value);
    if masked == value {
        return;
    }
    let mut stored = REGISTERED_CONFIG_VALUES
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if let Some(index) = stored.iter().position(|(known, _)| known == value) {
        stored.remove(index);
    } else if stored.len() >= REGISTERED_VALUE_BOUND {
        stored.pop_front();
    }
    stored.push_back((value.to_string(), masked));
}

pub fn register_config_map<S: BuildHasher>(config: &HashMap<String, String, S>) {
    for value in config.values() {
        register_config_value(value);
    }
}

#[must_use]
pub fn mask_registered_values(text: &str) -> String {
    let snapshot: Vec<(String, String)> = {
        let stored = REGISTERED_CONFIG_VALUES
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        stored.iter().cloned().collect()
    };
    let mut ordered = snapshot;
    ordered.sort_by(|left, right| right.0.len().cmp(&left.0.len()));
    let mut masked = text.to_string();
    for (value, replacement) in &ordered {
        let quoted = format!("{value:?}");
        let inner = quoted
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
            .unwrap_or(quoted.as_str());
        if inner != value {
            masked = replace_whole_tokens(&masked, inner, replacement);
        }
        masked = replace_whole_tokens(&masked, value, replacement);
    }
    masked
}

fn replace_whole_tokens(text: &str, needle: &str, replacement: &str) -> String {
    if needle.is_empty() {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    while let Some(offset) = text[cursor..].find(needle) {
        let start = cursor + offset;
        let end = start + needle.len();
        let before_ok = text[..start]
            .chars()
            .next_back()
            .is_none_or(|edge| !is_token_char(edge));
        let after_ok = text[end..]
            .chars()
            .next()
            .is_none_or(|edge| !is_token_char(edge));
        out.push_str(&text[cursor..start]);
        if before_ok && after_ok {
            out.push_str(replacement);
            cursor = end;
        } else {
            let next = text[start..]
                .chars()
                .next()
                .map_or(end, |edge| start + edge.len_utf8());
            out.push_str(&text[start..next]);
            cursor = next;
        }
    }
    out.push_str(&text[cursor..]);
    out
}

fn is_token_char(edge: char) -> bool {
    edge.is_ascii_alphanumeric() || matches!(edge, '_' | '=' | '.' | '-')
}

#[must_use]
pub fn prop_key_is_secret(key: &str) -> bool {
    let lower = folded_key(key);
    secret_shaped(&lower)
        || lower.contains("account_key")
        || lower.contains("authorization")
        || lower == "pat"
        || lower.ends_with("_pat")
}

#[must_use]
pub fn column_name_is_secret_shaped(name: &str) -> bool {
    secret_shaped(&folded_key(name))
}

fn folded_key(key: &str) -> String {
    key.to_ascii_lowercase().replace(['-', '.'], "_")
}

fn secret_shaped(lower: &str) -> bool {
    let compact = lower.replace('_', "");
    lower.contains("aws_secret")
        || lower.contains("secret")
        || lower.contains("password")
        || lower.contains("token")
        || lower.contains("credential")
        || lower.contains("connection_string")
        || lower.ends_with("access_key_id")
        || lower.ends_with("access_key")
        || compact.contains("accesskey")
        || compact.contains("apikey")
        || compact.contains("privatekey")
        || compact == "bearer"
        || compact.ends_with("bearer")
        || lower.contains("user_info")
        || compact.contains("userinfo")
        || lower == "key"
        || lower.ends_with("_key") && !lower.contains("bucket") && !lower.contains("arn")
}

#[must_use]
pub fn redact_value(key: &str, value: &str) -> String {
    if prop_key_is_secret(key) {
        REDACTED.to_string()
    } else {
        mask_value_credentials(value)
    }
}

const STORAGE_SCHEMES: &[&str] = &[
    "s3", "s3a", "s3n", "gs", "gcs", "abfs", "abfss", "wasb", "wasbs", "hdfs", "file", "viewfs",
    "oss", "cos", "r2",
];

#[must_use]
pub fn mask_value_credentials(value: &str) -> String {
    let mut spans = Spans::default();
    url_userinfo_spans(value, &mut spans);
    login_spans(value, &mut spans);
    parameter_spans(value, &mut spans);
    colon_pair_spans(value, &mut spans);
    spans.render(value)
}

#[must_use]
pub fn mask_url_userinfo(text: &str) -> String {
    let mut spans = Spans::default();
    url_userinfo_spans(text, &mut spans);
    spans.render(text)
}

#[derive(Default)]
struct Spans {
    masked: Vec<(usize, usize)>,
    url_regions: Vec<(usize, usize)>,
}

impl Spans {
    fn add(&mut self, start: usize, end: usize) {
        if end > start {
            self.masked.push((start, end));
        }
    }

    fn covers(&self, position: usize) -> bool {
        self.masked
            .iter()
            .any(|(start, end)| *start <= position && position < *end)
    }

    fn in_url_region(&self, position: usize) -> bool {
        self.url_regions
            .iter()
            .any(|(start, end)| *start <= position && position < *end)
    }

    fn render(mut self, value: &str) -> String {
        self.masked.sort_unstable();
        let mut out = String::with_capacity(value.len());
        let mut copied = 0;
        for (start, end) in self.masked {
            if end <= copied {
                continue;
            }
            let start = start.max(copied);
            out.push_str(&value[copied..start]);
            out.push_str(REDACTED);
            copied = end;
        }
        out.push_str(&value[copied..]);
        out
    }
}

fn url_userinfo_spans(value: &str, spans: &mut Spans) {
    let mut search = 0;
    while let Some(found) = value[search..].find("://") {
        let scheme_end = search + found;
        let authority_start = scheme_end + "://".len();
        let storage = is_storage_scheme(&value[..scheme_end]);
        let tail = &value[authority_start..];
        let region = &tail[..next_url_start(tail)];
        let region_end = region
            .find(['/', '?', '#', ';'])
            .map_or(region.len(), |end| {
                end + region[end..]
                    .find(char::is_whitespace)
                    .unwrap_or(region.len() - end)
            });
        spans
            .url_regions
            .push((authority_start, authority_start + region_end));
        let consumed = authority_span(region, storage).map_or(0, |(masked_start, masked_end)| {
            spans.add(authority_start + masked_start, authority_start + masked_end);
            masked_end
        });
        search = authority_start + consumed;
    }
}

fn is_storage_scheme(before: &str) -> bool {
    let scheme_start = before
        .char_indices()
        .rev()
        .find(|(_, character)| !is_scheme_char(*character))
        .map_or(0, |(position, character)| position + character.len_utf8());
    let scheme = before[scheme_start..].rsplit(':').next().unwrap_or("");
    STORAGE_SCHEMES
        .iter()
        .any(|storage| scheme.eq_ignore_ascii_case(storage))
}

fn next_url_start(tail: &str) -> usize {
    let Some(next) = tail.find("://") else {
        return tail.len();
    };
    tail[..next]
        .char_indices()
        .rev()
        .find(|(_, character)| !is_scheme_char(*character))
        .map_or(0, |(position, character)| position + character.len_utf8())
}

fn is_scheme_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.' | ':')
}

fn authority_span(region: &str, storage: bool) -> Option<(usize, usize)> {
    let authority_end = region.find(['/', '?', '#', ';']).unwrap_or(region.len());
    let mut authority = &region[..authority_end];
    if let Some(space) = authority.find(char::is_whitespace)
        && is_clean_host(&authority[..space])
    {
        authority = &authority[..space];
    }
    let userinfo_like = authority.contains(':');
    let end = if let Some(at) = authority.rfind('@') {
        let host = &authority[at + 1..];
        match unclean_userinfo_end(region, authority.len(), userinfo_like) {
            Some(later)
                if !is_host_like(host)
                    || !is_clean_host(host) && is_clean_host(follower_host(region, later)) =>
            {
                later
            }
            _ => at,
        }
    } else if is_clean_host(authority) {
        return None;
    } else {
        unclean_userinfo_end(region, authority.len(), userinfo_like)?
    };
    if storage && !region[..end].contains(':') {
        return None;
    }
    userinfo_password_span(&region[..end]).map(|(start, _)| (start, end))
}

fn userinfo_password_span(userinfo: &str) -> Option<(usize, usize)> {
    if userinfo.is_empty() {
        return None;
    }
    match userinfo.split_once(':') {
        Some((user, _))
            if !user.contains(|character: char| {
                matches!(character, '@' | '/' | '?' | '#' | ';') || character.is_whitespace()
            }) =>
        {
            Some((user.len() + 1, userinfo.len()))
        }
        _ => Some((0, userinfo.len())),
    }
}

fn unclean_userinfo_end(region: &str, from: usize, userinfo_like: bool) -> Option<usize> {
    let limit = if userinfo_like {
        region.len()
    } else {
        parameter_boundary(region, from).unwrap_or(region.len())
    };
    let mut search = from;
    while let Some(offset) = region[search..limit].find('@') {
        let first = search + offset;
        search = first + 1;
        if !host_follows(region, first) {
            continue;
        }
        let mut at = first;
        while let Some(next) = region[at + 1..limit].find(['@', '/', '?', '#']) {
            let candidate = at + 1 + next;
            if region.as_bytes()[candidate] != b'@' || !host_follows(region, candidate) {
                break;
            }
            at = candidate;
        }
        return Some(at);
    }
    None
}

fn parameter_boundary(region: &str, from: usize) -> Option<usize> {
    region[from..]
        .char_indices()
        .find(|(position, character)| {
            (*character == ';' || character.is_whitespace())
                && starts_named_parameter(&region[from + position + character.len_utf8()..])
        })
        .map(|(position, _)| from + position)
}

fn starts_named_parameter(after: &str) -> bool {
    let after = after.trim_start();
    let name_end = after
        .find(|character: char| !is_parameter_name_char(character))
        .unwrap_or(after.len());
    name_end > 0 && after[name_end..].trim_start().starts_with('=')
}

fn host_follows(region: &str, at: usize) -> bool {
    is_host_like(follower_host(region, at))
}

fn follower_host(region: &str, at: usize) -> &str {
    host_text(&region[at + 1..])
}

fn host_text(follower: &str) -> &str {
    let bracket_end = if follower.starts_with('[') {
        follower.find(']').map_or(0, |close| close + 1)
    } else {
        0
    };
    let host_end = follower[bracket_end..]
        .find(|character: char| {
            matches!(
                character,
                '/' | '?' | '#' | ';' | '"' | '\'' | '(' | ')' | ']' | '{' | '}' | ',' | '<' | '>'
            ) || character.is_whitespace()
        })
        .map_or(follower.len(), |end| bracket_end + end);
    &follower[..host_end]
}

fn is_clean_host(authority: &str) -> bool {
    authority.is_empty()
        || authority.split(',').all(|part| match host_and_port(part) {
            Some((host, port)) => {
                port.is_some_and(|digits| !digits.is_empty())
                    || host.starts_with('[')
                    || is_dotted_name(host)
                    || host.eq_ignore_ascii_case("localhost")
            }
            None => false,
        })
}

fn is_dotted_name(host: &str) -> bool {
    let labels: Vec<&str> = host.split('.').collect();
    labels.len() > 1
        && labels.iter().all(|label| !label.is_empty())
        && (labels
            .iter()
            .all(|label| label.bytes().all(|byte| byte.is_ascii_digit()))
            || labels.last().is_some_and(|top| {
                top.chars().count() >= 2 && top.chars().all(char::is_alphabetic)
            }))
}

fn is_host_like(text: &str) -> bool {
    !text.is_empty() && text.split(',').all(|part| host_and_port(part).is_some())
}

fn host_and_port(segment: &str) -> Option<(&str, Option<&str>)> {
    let (host, port) = if segment.starts_with('[') {
        let close = segment.find(']')? + 1;
        let remainder = &segment[close..];
        if remainder.is_empty() {
            (&segment[..close], None)
        } else {
            (&segment[..close], Some(remainder.strip_prefix(':')?))
        }
    } else {
        match segment.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (segment, None),
        }
    };
    let host_ok = host.starts_with('[')
        || !host.is_empty()
            && host.chars().all(|character| {
                character.is_alphanumeric() || matches!(character, '-' | '.' | '_' | '%')
            });
    let port_ok = port.is_none_or(|digits| digits.bytes().all(|byte| byte.is_ascii_digit()));
    (host_ok && port_ok).then_some((host, port))
}

fn login_spans(value: &str, spans: &mut Spans) {
    let mut index = 0;
    while let Some(offset) = value[index..].find(['/', ':']) {
        let separator = index + offset;
        index = separator + 1;
        if spans.covers(separator) || spans.in_url_region(separator) {
            continue;
        }
        let slash = value.as_bytes()[separator] == b'/';
        if !slash && value[index..].starts_with("//") {
            continue;
        }
        let Some(user_start) = login_user_start(value, separator, slash) else {
            continue;
        };
        if !login_bounded(&value[..user_start], slash)
            || !slash && value[user_start..separator].eq_ignore_ascii_case("jdbc")
        {
            continue;
        }
        if let Some(at) = login_password_end(value, index, slash) {
            spans.add(index, at);
            index = at;
        }
    }
}

fn login_user_start(value: &str, separator: usize, slash: bool) -> Option<usize> {
    let before = &value[..separator];
    if slash && let Some(quoted) = before.strip_suffix('"') {
        return quoted.rfind('"');
    }
    let start = before
        .char_indices()
        .rev()
        .find(|(_, character)| !is_identifier_char(*character))
        .map_or(0, |(position, character)| position + character.len_utf8());
    before[start..]
        .starts_with(|character: char| character.is_ascii_alphabetic())
        .then_some(start)
}

fn login_bounded(before: &str, slash: bool) -> bool {
    before
        .chars()
        .next_back()
        .is_none_or(|character| slash && character == ':' || is_word_delimiter(character))
}

fn login_password_end(value: &str, start: usize, slash: bool) -> Option<usize> {
    let rest = &value[start..];
    if rest.is_empty() || rest.starts_with(char::is_whitespace) || !slash && rest.starts_with('/') {
        return None;
    }
    if slash && let Some(quoted) = rest.strip_prefix('"') {
        let close = quoted.find('"')? + 2;
        return rest[close..].starts_with('@').then_some(start + close);
    }
    let reach = if slash {
        rest.find(char::is_whitespace).unwrap_or(rest.len())
    } else {
        rest.len()
    };
    let mut search = 0;
    while let Some(offset) = rest[search..reach].find('@') {
        let first = search + offset;
        search = first + 1;
        if first == 0 || !login_follower_ok(&rest[first + 1..], slash) {
            continue;
        }
        let mut at = first;
        while let Some(next) = rest[at + 1..reach].find(|character: char| {
            matches!(character, '@' | '/')
                || slash && (character == '(' || character.is_whitespace())
        }) {
            let candidate = at + 1 + next;
            if rest.as_bytes()[candidate] != b'@'
                || !login_follower_ok(&rest[candidate + 1..], slash)
            {
                break;
            }
            at = candidate;
        }
        let password = &rest[..at];
        let follower = rest[at + 1..].trim_start_matches([' ', '\t']);
        if password.contains("://")
            || !slash && password.ends_with(':')
            || slash && password.contains('/') && !follower.starts_with(['(', '/'])
        {
            return None;
        }
        return Some(start + at);
    }
    None
}

fn login_follower_ok(follower: &str, slash: bool) -> bool {
    if slash {
        let follower = follower.trim_start_matches([' ', '\t']);
        follower.starts_with(['(', '[']) && !follower.starts_with("[]")
            || follower.starts_with("//")
            || follower.starts_with(char::is_alphanumeric)
    } else {
        is_host_like(host_text(follower))
    }
}

fn is_identifier_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '$' | '#' | '.' | '-')
}

fn is_word_delimiter(character: char) -> bool {
    character.is_whitespace()
        || matches!(
            character,
            ';' | ',' | '(' | ')' | '"' | '\'' | '=' | '<' | '>'
        )
}

fn parameter_spans(value: &str, spans: &mut Spans) {
    let mut index = 0;
    let mut name_floor = 0;
    while let Some(offset) = value[index..].find('=') {
        let equals = index + offset;
        index = equals + 1;
        if spans.covers(equals) {
            continue;
        }
        let name = parameter_name_before(&value[name_floor..equals]);
        if name.is_empty() || !parameter_name_is_secret(name) {
            continue;
        }
        let value_start = index + (value[index..].len() - value[index..].trim_start().len());
        let value_end = if value[..equals].trim_end().ends_with(&format!("({name}")) {
            value[value_start..]
                .find(')')
                .map_or(value.len(), |end| value_start + end)
        } else {
            parameter_value_end(value, value_start)
        };
        spans.add(value_start, value_end);
        if value_end > value_start {
            name_floor = value_end;
        }
        index = value_end.max(index);
    }
}

fn parameter_name_before(prefix: &str) -> &str {
    let trimmed = prefix.trim_end();
    let start = trimmed
        .char_indices()
        .rev()
        .find(|(_, character)| !is_parameter_name_char(*character))
        .map_or(0, |(position, character)| position + character.len_utf8());
    &trimmed[start..]
}

fn is_parameter_name_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
}

fn parameter_name_is_secret(name: &str) -> bool {
    let compact: String = name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|character| character.to_ascii_lowercase())
        .collect();
    if compact.ends_with("name") {
        return false;
    }
    prop_key_is_secret(name)
        || matches!(compact.as_str(), "sig" | "pw" | "pass" | "sas")
        || ["pwd", "passwd", "passcode", "signature", "accountkey"]
            .iter()
            .any(|suffix| compact.ends_with(suffix))
}

fn parameter_value_end(value: &str, start: usize) -> usize {
    let rest = &value[start..];
    match rest.as_bytes().first() {
        Some(quote @ (b'\'' | b'"')) => {
            quoted_value_end(rest, *quote).map_or(value.len(), |end| start + end)
        }
        Some(b'{') => braced_value_end(rest).map_or(value.len(), |end| start + end),
        _ => rest
            .char_indices()
            .find(|(position, character)| {
                (matches!(character, '&' | ';') || character.is_whitespace())
                    && starts_next_parameter(&rest[position + character.len_utf8()..])
            })
            .map_or(value.len(), |(position, _)| start + position),
    }
}

fn quoted_value_end(rest: &str, quote: u8) -> Option<usize> {
    let bytes = rest.as_bytes();
    let mut position = 1;
    while position < bytes.len() {
        match bytes[position] {
            b'\\' => position += 2,
            byte if byte == quote => return Some(position + 1),
            _ => position += 1,
        }
    }
    None
}

fn braced_value_end(rest: &str) -> Option<usize> {
    let bytes = rest.as_bytes();
    let mut position = 1;
    while position < bytes.len() {
        if bytes[position] == b'}' {
            if bytes.get(position + 1) == Some(&b'}') {
                position += 2;
                continue;
            }
            return Some(position + 1);
        }
        position += 1;
    }
    None
}

fn starts_next_parameter(after: &str) -> bool {
    let after = after.trim_start_matches(|character: char| {
        character.is_whitespace() || matches!(character, '&' | ';')
    });
    if after.is_empty() {
        return true;
    }
    let name_end = after
        .find(|character: char| !is_parameter_name_char(character))
        .unwrap_or(after.len());
    name_end > 0 && after[name_end..].trim_start().starts_with('=')
}

fn colon_pair_spans(value: &str, spans: &mut Spans) {
    let mut index = 0;
    let mut name_floor = 0;
    while let Some(offset) = value[index..].find(':') {
        let colon = index + offset;
        index = colon + 1;
        if spans.covers(colon) {
            continue;
        }
        let Some((name, name_start, quoted)) = colon_pair_name(value, name_floor, colon) else {
            continue;
        };
        let after = &value[index..];
        if !(quoted || after.is_empty() || after.starts_with([' ', '\t', '\n', '\r'])) {
            continue;
        }
        if !parameter_name_is_secret(name) {
            continue;
        }
        let (value_start, value_end) = colon_value_span(value, index, name_start, quoted);
        spans.add(value_start, value_end);
        if value_end > value_start {
            name_floor = value_end;
        }
        index = value_end.max(index);
    }
}

fn colon_pair_name(value: &str, floor: usize, colon: usize) -> Option<(&str, usize, bool)> {
    let prefix = &value[floor..colon];
    let trimmed = prefix.trim_end_matches([' ', '\t']);
    if let Some(quote) = trimmed
        .chars()
        .next_back()
        .filter(|character| matches!(character, '"' | '\''))
    {
        let inner = &trimmed[..trimmed.len() - 1];
        let open = inner.rfind(quote)?;
        return Some((&inner[open + 1..], floor + open, true));
    }
    if prefix.trim_end().len() != prefix.len() {
        return None;
    }
    let name = parameter_name_before(prefix);
    (!name.is_empty()).then(|| (name, colon - name.len(), false))
}

fn colon_value_span(
    value: &str,
    after_colon: usize,
    name_start: usize,
    quoted_name: bool,
) -> (usize, usize) {
    let same_line = &value[after_colon..];
    let inline_start =
        after_colon + (same_line.len() - same_line.trim_start_matches([' ', '\t']).len());
    let inline = &value[inline_start..];
    let line_ends = inline.is_empty() || inline.starts_with(['\n', '\r']);
    if quoted_name {
        let start = after_colon + (same_line.len() - same_line.trim_start().len());
        return (start, inline_value_end(value, start, true));
    }
    if line_ends {
        let block_start =
            inline_start + (inline.len() - inline.trim_start_matches(['\n', '\r']).len());
        let block_end = indented_block_end(value, block_start, line_indent(value, name_start));
        let shown_start = block_start
            + (value[block_start..block_end].len()
                - value[block_start..block_end]
                    .trim_start_matches([' ', '\t'])
                    .len());
        return (shown_start, block_end);
    }
    if is_block_scalar_indicator(inline) {
        let line_end = inline
            .find(['\n', '\r'])
            .map_or(value.len(), |end| inline_start + end);
        let block_start = line_end
            + (value[line_end..].len() - value[line_end..].trim_start_matches(['\n', '\r']).len());
        let block_end = indented_block_end(value, block_start, line_indent(value, name_start));
        return (inline_start, block_end.max(line_end));
    }
    (inline_start, inline_value_end(value, inline_start, false))
}

fn inline_value_end(value: &str, start: usize, quoted_name: bool) -> usize {
    let rest = &value[start..];
    if let Some(quote @ (b'\'' | b'"')) = rest.as_bytes().first() {
        return quoted_value_end(rest, *quote).map_or(value.len(), |end| start + end);
    }
    let stop = |character: char| {
        matches!(character, '\n' | '\r') || quoted_name && matches!(character, ',' | '}' | ']')
    };
    rest.find(stop).map_or(value.len(), |end| start + end)
}

fn is_block_scalar_indicator(inline: &str) -> bool {
    let line = inline.split(['\n', '\r']).next().unwrap_or("");
    let line = line.split(" #").next().unwrap_or("").trim_end();
    line.starts_with(['|', '>'])
        && line[1..]
            .chars()
            .all(|character| matches!(character, '-' | '+') || character.is_ascii_digit())
}

fn line_indent(value: &str, position: usize) -> usize {
    let line_start = value[..position]
        .rfind(['\n', '\r'])
        .map_or(0, |end| end + 1);
    value[line_start..position]
        .chars()
        .take_while(|character| matches!(character, ' ' | '\t' | '-'))
        .count()
}

fn indented_block_end(value: &str, block_start: usize, key_indent: usize) -> usize {
    let mut end = block_start;
    let mut cursor = block_start;
    while cursor < value.len() {
        let line_end = value[cursor..]
            .find('\n')
            .map_or(value.len(), |offset| cursor + offset);
        let line = &value[cursor..line_end];
        let indent = line.len() - line.trim_start_matches([' ', '\t']).len();
        if !line.trim().is_empty() {
            if indent <= key_indent {
                break;
            }
            end = cursor + line.trim_end_matches(['\r', ' ', '\t']).len();
        }
        cursor = line_end + 1;
    }
    end
}

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod tests;
