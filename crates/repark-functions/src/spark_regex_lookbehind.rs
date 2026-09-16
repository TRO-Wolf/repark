use crate::spark_regex_engine::{GroupKind, find_quote_end, group_kind, match_group, skip_class};

pub(crate) fn normalize_lookbehind(pattern: &str) -> String {
    let bytes = pattern.as_bytes();
    let mut out = String::with_capacity(pattern.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            if bytes.get(index + 1) == Some(&b'Q') {
                let end = find_quote_end(bytes, index + 2).min(pattern.len());
                out.push_str(&pattern[index..end]);
                index = end;
                continue;
            }
            let width = 1 + pattern[index + 1..]
                .chars()
                .next()
                .map_or(0, char::len_utf8);
            out.push_str(&pattern[index..index + width]);
            index += width;
            continue;
        }
        if bytes[index] == b'[' {
            let end = skip_class(bytes, index).min(pattern.len());
            out.push_str(&pattern[index..end]);
            index = end;
            continue;
        }
        if bytes[index] == b'('
            && bytes.get(index + 1) == Some(&b'?')
            && bytes.get(index + 2) == Some(&b'<')
            && matches!(bytes.get(index + 3), Some(b'=' | b'!'))
            && let Some(close) = match_group(bytes, index)
        {
            let negative = bytes[index + 3] == b'!';
            let body = normalize_lookbehind(&pattern[index + 4..close]);
            out.push_str(&normalize_lookbehind_body(&body, negative));
            index = close + 1;
            continue;
        }
        let width = pattern[index..].chars().next().map_or(1, char::len_utf8);
        out.push_str(&pattern[index..index + width]);
        index += width;
    }
    out
}

fn normalize_lookbehind_body(body: &str, negative: bool) -> String {
    let open = if negative { "(?<!" } else { "(?<=" };
    let mut branches = Vec::new();
    for branch in split_top_level(body) {
        match normalize_lookbehind_branch(branch) {
            NormalizedBranch::Body(rewritten) => branches.push(rewritten),
            NormalizedBranch::AlwaysTrue => {
                if negative {
                    return "(?!)".to_owned();
                }
                return "(?=)".to_owned();
            }
        }
    }
    if branches.is_empty() {
        return if negative {
            "(?!)".to_owned()
        } else {
            "(?=)".to_owned()
        };
    }
    format!("{open}{})", branches.join("|"))
}

enum NormalizedBranch {
    Body(String),
    AlwaysTrue,
}

fn normalize_lookbehind_branch(branch: &str) -> NormalizedBranch {
    let Some((atom, quant)) = strip_one_quantifier(branch) else {
        return NormalizedBranch::Body(branch.to_owned());
    };
    if atom.is_empty() {
        return NormalizedBranch::Body(branch.to_owned());
    }
    match quant {
        TrailingQuant::Star | TrailingQuant::Optional | TrailingQuant::BoundedZero => {
            NormalizedBranch::AlwaysTrue
        }
        TrailingQuant::Plus | TrailingQuant::Open(_) => {
            if branch_nullable(branch) {
                NormalizedBranch::AlwaysTrue
            } else {
                NormalizedBranch::Body(atom)
            }
        }
        TrailingQuant::Bounded {
            min,
            max,
            lazy,
            possessive,
        } => NormalizedBranch::Body(expand_bounded(&atom, min, max, lazy, possessive)),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TrailingQuant {
    Star,
    Optional,
    Plus,
    Open(usize),
    BoundedZero,
    Bounded {
        min: usize,
        max: usize,
        lazy: bool,
        possessive: bool,
    },
}

fn strip_one_quantifier(branch: &str) -> Option<(String, TrailingQuant)> {
    let bytes = branch.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let last = bytes.len() - 1;
    match bytes[last] {
        b'*' => {
            if last > 0 && bytes[last - 1] == b'+' {
                Some((branch[..last - 1].to_owned(), TrailingQuant::Star))
            } else {
                Some((branch[..last].to_owned(), TrailingQuant::Star))
            }
        }
        b'?' => {
            if last > 0 && (bytes[last - 1] == b'+' || bytes[last - 1] == b'?') {
                Some((branch[..last - 1].to_owned(), TrailingQuant::Star))
            } else {
                Some((branch[..last].to_owned(), TrailingQuant::Optional))
            }
        }
        b'+' => {
            if last > 0 && bytes[last - 1] == b'?' {
                Some((branch[..last - 1].to_owned(), TrailingQuant::Plus))
            } else {
                Some((branch[..last].to_owned(), TrailingQuant::Plus))
            }
        }
        b'}' => parse_brace_quantifier(branch),
        _ => None,
    }
}

fn parse_brace_quantifier(branch: &str) -> Option<(String, TrailingQuant)> {
    let bytes = branch.as_bytes();
    let mut open = bytes.len() - 1;
    while open > 0 && bytes[open] != b'{' {
        open -= 1;
    }
    if bytes[open] != b'{' {
        return None;
    }
    let mut inner = branch[open + 1..bytes.len() - 1].to_owned();
    let mut lazy = false;
    let mut possessive = false;
    if inner.ends_with('?') {
        lazy = true;
        inner.pop();
    } else if inner.ends_with('+') {
        possessive = true;
        inner.pop();
    }
    let atom = branch[..open].to_owned();
    if let Some((min_text, max_text)) = inner.split_once(',') {
        let min: usize = min_text.parse().ok()?;
        if max_text.is_empty() {
            return Some((atom, TrailingQuant::Open(min)));
        }
        let max: usize = max_text.parse().ok()?;
        if min == 0 {
            return Some((atom, TrailingQuant::BoundedZero));
        }
        return Some((
            atom,
            TrailingQuant::Bounded {
                min,
                max,
                lazy,
                possessive,
            },
        ));
    }
    let exact: usize = inner.parse().ok()?;
    let _ = exact;
    None
}

fn expand_bounded(atom: &str, min: usize, max: usize, lazy: bool, possessive: bool) -> String {
    if max.saturating_sub(min) > 32 || max > 64 {
        return format!("{atom}{{{min},{max}}}");
    }
    let mut branches: Vec<String> = Vec::new();
    for count in min..=max {
        branches.push(atom.repeat(count));
    }
    if lazy {
        branches.join("|")
    } else if possessive {
        branches.reverse();
        format!("(?>{})", branches.join("|"))
    } else {
        branches.reverse();
        branches.join("|")
    }
}

fn split_top_level(body: &str) -> Vec<&str> {
    let bytes = body.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 1,
            b'[' => index = skip_class(bytes, index).saturating_sub(1),
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b'|' if depth == 0 => {
                parts.push(&body[start..index]);
                start = index + 1;
            }
            _ => (),
        }
        index += 1;
    }
    parts.push(&body[start..]);
    parts
}

fn branch_nullable(branch: &str) -> bool {
    split_top_level(branch)
        .iter()
        .any(|part| concat_nullable(part))
}

fn concat_nullable(chain: &str) -> bool {
    let bytes = chain.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let (base_nullable, after_atom) = atom_nullable(bytes, index);
        let (atom_counts, after_quant) = quantifier_allows_empty(bytes, after_atom);
        if !base_nullable && !atom_counts {
            return false;
        }
        if after_quant == after_atom && after_atom == index {
            index += 1;
        } else {
            index = after_quant;
        }
    }
    true
}

fn atom_nullable(bytes: &[u8], index: usize) -> (bool, usize) {
    if index >= bytes.len() {
        return (true, index);
    }
    match bytes[index] {
        b'^' | b'$' => (true, index + 1),
        b'\\' => (false, (index + 2).min(bytes.len())),
        b'[' => (false, skip_class(bytes, index)),
        b'(' => {
            let Some(close) = match_group(bytes, index) else {
                return (false, bytes.len());
            };
            let nullable = match group_kind(bytes, index) {
                GroupKind::Lookahead | GroupKind::Lookbehind => true,
                GroupKind::Atomic | GroupKind::Consuming => {
                    let (start, end) = group_content(bytes, index, close);
                    branch_nullable(&String::from_utf8_lossy(&bytes[start..end]))
                }
                GroupKind::Other | GroupKind::Invalid => false,
            };
            (nullable, close + 1)
        }
        _ => (false, index + 1),
    }
}

fn group_content(bytes: &[u8], open: usize, close: usize) -> (usize, usize) {
    if bytes.get(open + 1) != Some(&b'?') {
        return (open + 1, close);
    }
    match bytes.get(open + 2) {
        Some(b':' | b'=' | b'!' | b'>') => (open + 3, close),
        Some(b'<') => {
            let mut index = open + 3;
            while index < close && bytes[index] != b'>' {
                index += 1;
            }
            (index + 1, close)
        }
        Some(b'\'') => {
            let mut index = open + 3;
            while index < close && bytes[index] != b'\'' {
                index += 1;
            }
            (index + 1, close)
        }
        _ => {
            let mut index = open + 2;
            while index < close && bytes[index] != b':' && bytes[index] != b')' {
                index += 1;
            }
            if bytes.get(index) == Some(&b':') {
                (index + 1, close)
            } else {
                (close, close)
            }
        }
    }
}

fn quantifier_allows_empty(bytes: &[u8], index: usize) -> (bool, usize) {
    if index >= bytes.len() {
        return (false, index);
    }
    match bytes[index] {
        b'*' | b'?' => {
            let mut end = index + 1;
            if matches!(bytes.get(end), Some(b'?' | b'+')) {
                end += 1;
            }
            (true, end)
        }
        b'+' => {
            let mut end = index + 1;
            if matches!(bytes.get(end), Some(b'?' | b'+')) {
                end += 1;
            }
            (false, end)
        }
        b'{' => {
            let mut end = index + 1;
            while end < bytes.len() && bytes[end] != b'}' {
                end += 1;
            }
            if end >= bytes.len() {
                return (false, index);
            }
            let inner = String::from_utf8_lossy(&bytes[index + 1..end]);
            let allows = inner.starts_with("0,") || inner.starts_with("0}") || inner == "0";
            let mut after = end + 1;
            if bytes.get(after) == Some(&b'?') || bytes.get(after) == Some(&b'+') {
                after += 1;
            }
            (allows, after)
        }
        _ => (false, index),
    }
}
