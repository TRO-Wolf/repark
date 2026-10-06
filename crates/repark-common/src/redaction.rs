pub const REDACTED: &str = "***";

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

#[must_use]
pub fn mask_value_credentials(value: &str) -> String {
    let masked = mask_secret_parameters(value);
    let masked = mask_colon_pairs(&masked);
    let masked = mask_url_userinfo(&masked);
    mask_slash_credentials(&masked)
}

fn mask_url_userinfo(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(found) = rest.find("://") {
        let authority_start = found + "://".len();
        out.push_str(&rest[..authority_start]);
        let tail = &rest[authority_start..];
        let region = &tail[..next_url_start(tail)];
        let consumed = mask_authority(&mut out, region);
        rest = &tail[consumed..];
    }
    out.push_str(rest);
    out
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

fn mask_authority(out: &mut String, region: &str) -> usize {
    let authority_end = region.find(['/', '?', '#', ';']).unwrap_or(region.len());
    let mut authority = &region[..authority_end];
    if let Some(space) = authority.find(char::is_whitespace)
        && is_clean_host(&authority[..space])
    {
        authority = &authority[..space];
    }
    if let Some(at) = authority.rfind('@') {
        let host = &authority[at + 1..];
        let end = match unclean_userinfo_end(region, authority.len()) {
            Some(later)
                if !is_host_like(host)
                    || !is_clean_host(host) && is_clean_host(follower_host(region, later)) =>
            {
                later
            }
            _ => at,
        };
        push_masked_userinfo(out, &region[..end]);
        return end;
    }
    if is_clean_host(authority) {
        return 0;
    }
    match unclean_userinfo_end(region, authority.len()) {
        Some(at) => {
            push_masked_userinfo(out, &region[..at]);
            at
        }
        None => 0,
    }
}

fn unclean_userinfo_end(region: &str, from: usize) -> Option<usize> {
    let mut search = from;
    while let Some(offset) = region[search..].find('@') {
        let first = search + offset;
        search = first + 1;
        if !host_follows(region, first) {
            continue;
        }
        let mut at = first;
        while let Some(next) = region[at + 1..].find(['@', '/', '?', '#']) {
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

fn host_follows(region: &str, at: usize) -> bool {
    is_host_like(follower_host(region, at))
}

fn follower_host(region: &str, at: usize) -> &str {
    let follower = &region[at + 1..];
    let host_end = follower
        .find(|character: char| {
            matches!(character, '/' | '?' | '#' | ';') || character.is_whitespace()
        })
        .unwrap_or(follower.len());
    &follower[..host_end]
}

fn push_masked_userinfo(out: &mut String, userinfo: &str) {
    if userinfo.is_empty() {
        return;
    }
    if let Some((user, _)) = userinfo.split_once(':')
        && !user.contains(|character: char| {
            matches!(character, '@' | '/' | '?' | '#' | ';') || character.is_whitespace()
        })
    {
        out.push_str(user);
        out.push(':');
    }
    out.push_str(REDACTED);
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

fn mask_secret_parameters(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut copied = 0;
    let mut index = 0;
    while let Some(offset) = value[index..].find('=') {
        let equals = index + offset;
        let name = parameter_name_before(&value[copied..equals]);
        index = equals + 1;
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
        if value_end > value_start {
            out.push_str(&value[copied..value_start]);
            out.push_str(REDACTED);
            copied = value_end;
        }
        index = value_end.max(index);
    }
    out.push_str(&value[copied..]);
    out
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

fn mask_colon_pairs(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut copied = 0;
    let mut index = 0;
    while let Some(offset) = value[index..].find(':') {
        let colon = index + offset;
        index = colon + 1;
        let Some((name, quoted)) = colon_pair_name(&value[copied..colon]) else {
            continue;
        };
        let after = &value[index..];
        if !quoted && !after.starts_with([' ', '\t']) {
            continue;
        }
        if !parameter_name_is_secret(name) {
            continue;
        }
        let value_start = index + (after.len() - after.trim_start_matches([' ', '\t']).len());
        let value_end = colon_value_end(value, value_start, quoted);
        if value_end > value_start {
            out.push_str(&value[copied..value_start]);
            out.push_str(REDACTED);
            copied = value_end;
        }
        index = value_end.max(index);
    }
    out.push_str(&value[copied..]);
    out
}

fn colon_pair_name(prefix: &str) -> Option<(&str, bool)> {
    let trimmed = prefix.trim_end_matches([' ', '\t']);
    if let Some(quote) = trimmed
        .chars()
        .next_back()
        .filter(|character| matches!(character, '"' | '\''))
    {
        let inner = &trimmed[..trimmed.len() - 1];
        let open = inner.rfind(quote)?;
        return Some((&inner[open + 1..], true));
    }
    if trimmed.len() != prefix.len() {
        return None;
    }
    let name = parameter_name_before(prefix);
    (!name.is_empty()).then_some((name, false))
}

fn colon_value_end(value: &str, start: usize, quoted_name: bool) -> usize {
    let rest = &value[start..];
    if let Some(quote @ (b'\'' | b'"')) = rest.as_bytes().first() {
        return quoted_value_end(rest, *quote).map_or(value.len(), |end| start + end);
    }
    let stop = |character: char| {
        matches!(character, '\n' | '\r') || quoted_name && matches!(character, ',' | '}' | ']')
    };
    rest.find(stop).map_or(value.len(), |end| start + end)
}

fn mask_slash_credentials(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut copied = 0;
    let mut index = 0;
    while let Some(offset) = value[index..].find(['/', ':']) {
        let separator = index + offset;
        index = separator + 1;
        let colon = value.as_bytes()[separator] == b':';
        if colon && value[index..].starts_with("//") {
            continue;
        }
        let user_start = value[copied..separator]
            .char_indices()
            .rev()
            .find(|(_, character)| !is_identifier_char(*character))
            .map_or(copied, |(position, character)| {
                copied + position + character.len_utf8()
            });
        if !value[user_start..separator]
            .starts_with(|character: char| character.is_ascii_alphabetic())
        {
            continue;
        }
        let bounded = value[..user_start]
            .chars()
            .next_back()
            .is_none_or(|character| !colon && character == ':' || is_word_delimiter(character));
        if !bounded {
            continue;
        }
        let Some(at) = credential_password_end(value, index, colon) else {
            continue;
        };
        out.push_str(&value[copied..index]);
        out.push_str(REDACTED);
        copied = at;
        index = at;
    }
    out.push_str(&value[copied..]);
    out
}

fn credential_password_end(value: &str, start: usize, colon: bool) -> Option<usize> {
    let rest = &value[start..];
    if !colon && let Some(quoted) = rest.strip_prefix('"') {
        let close = quoted.find('"')? + 2;
        return rest[close..].starts_with('@').then_some(start + close);
    }
    let word_end = rest.find(is_word_delimiter).unwrap_or(rest.len());
    let at = rest[..word_end].rfind('@')?;
    if at == 0 || at + 1 >= word_end {
        return None;
    }
    if colon && (rest[..at].contains([':', '/']) || !host_follows(&rest[..word_end], at)) {
        return None;
    }
    Some(start + at)
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

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod tests;
