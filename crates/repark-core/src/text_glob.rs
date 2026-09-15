use std::path::{Path, PathBuf};

use crate::{Error, Result};

pub(crate) fn is_hidden_name(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_some_and(|text| text.starts_with('.') || text.starts_with('_'))
}

pub(crate) fn has_glob_meta(path: &str) -> bool {
    let mut escaped = false;
    for byte in path.bytes() {
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if matches!(byte, b'*' | b'?' | b'[' | b'{') {
            return true;
        }
    }
    false
}

#[derive(Debug, Clone, PartialEq)]
enum GlobToken {
    Literal(char),
    AnyChar,
    AnySeq,
    Class {
        negated: bool,
        spans: Vec<(char, char)>,
    },
}

fn parse_glob_class(pattern: &[char]) -> Option<(GlobToken, usize)> {
    let mut index = 1usize;
    let mut negated = false;
    if pattern
        .get(index)
        .is_some_and(|cell| *cell == '^' || *cell == '!')
    {
        negated = true;
        index += 1;
    }
    let mut spans: Vec<(char, char)> = Vec::new();
    let mut first = true;
    loop {
        let current = *pattern.get(index)?;
        if current == ']' && !first {
            index += 1;
            break;
        }
        first = false;
        if current == '\\' {
            index += 1;
            spans.push((*pattern.get(index)?, *pattern.get(index)?));
            index += 1;
            continue;
        }
        if pattern.get(index + 1) == Some(&'-')
            && let Some(&end) = pattern.get(index + 2)
            && end != ']'
        {
            spans.push((current, end));
            index += 3;
            continue;
        }
        spans.push((current, current));
        index += 1;
    }
    Some((GlobToken::Class { negated, spans }, index))
}

fn parse_glob_segment(pattern: &str) -> Vec<GlobToken> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut tokens: Vec<GlobToken> = Vec::new();
    let mut index = 0usize;
    while index < chars.len() {
        match chars[index] {
            '\\' => {
                index += 1;
                if let Some(cell) = chars.get(index) {
                    tokens.push(GlobToken::Literal(*cell));
                    index += 1;
                } else {
                    tokens.push(GlobToken::Literal('\\'));
                }
            }
            '?' => {
                tokens.push(GlobToken::AnyChar);
                index += 1;
            }
            '*' => {
                tokens.push(GlobToken::AnySeq);
                index += 1;
            }
            '[' => {
                if let Some((token, used)) = parse_glob_class(&chars[index..]) {
                    tokens.push(token);
                    index += used;
                } else {
                    tokens.push(GlobToken::Literal('['));
                    index += 1;
                }
            }
            cell => {
                tokens.push(GlobToken::Literal(cell));
                index += 1;
            }
        }
    }
    tokens
}

fn token_matches(token: &GlobToken, glyph: char) -> bool {
    match token {
        GlobToken::AnyChar | GlobToken::AnySeq => true,
        GlobToken::Literal(want) => glyph == *want,
        GlobToken::Class { negated, spans } => {
            spans.iter().any(|span| span.0 <= glyph && glyph <= span.1) != *negated
        }
    }
}

fn match_tokens(tokens: &[GlobToken], text: &[char]) -> bool {
    let mut text_at = 0usize;
    let mut token_at = 0usize;
    let mut star_at: Option<usize> = None;
    let mut star_text = 0usize;
    while text_at < text.len() {
        let single = token_at < tokens.len()
            && text[text_at] != '/'
            && !matches!(tokens[token_at], GlobToken::AnySeq)
            && token_matches(&tokens[token_at], text[text_at]);
        if single {
            text_at += 1;
            token_at += 1;
        } else if token_at < tokens.len() && matches!(tokens[token_at], GlobToken::AnySeq) {
            star_at = Some(token_at);
            token_at += 1;
            star_text = text_at;
        } else if let Some(star) = star_at {
            if star_text < text.len() && text[star_text] == '/' {
                return false;
            }
            star_text += 1;
            text_at = star_text;
            token_at = star + 1;
        } else {
            return false;
        }
    }
    while token_at < tokens.len() && matches!(tokens[token_at], GlobToken::AnySeq) {
        token_at += 1;
    }
    token_at == tokens.len()
}

fn match_segment(pattern: &str, text: &str) -> bool {
    let tokens = parse_glob_segment(pattern);
    let chars: Vec<char> = text.chars().collect();
    match_tokens(&tokens, &chars)
}

fn expand_braces(pattern: &str) -> Vec<String> {
    expand_braces_inner(pattern, 0)
}

fn expand_braces_inner(pattern: &str, depth: usize) -> Vec<String> {
    if depth > 16 {
        return vec![pattern.to_string()];
    }
    let chars: Vec<char> = pattern.chars().collect();
    let mut open: Option<usize> = None;
    let mut index = 0usize;
    while index < chars.len() {
        if chars[index] == '\\' {
            index += 2;
            continue;
        }
        if chars[index] == '{' {
            open = Some(index);
            break;
        }
        index += 1;
    }
    let Some(start) = open else {
        return vec![pattern.to_string()];
    };
    let mut depth = 0usize;
    let mut close: Option<usize> = None;
    let mut escaped = false;
    for (at, cell) in chars.iter().enumerate().skip(start) {
        if escaped {
            escaped = false;
            continue;
        }
        if *cell == '\\' {
            escaped = true;
            continue;
        }
        if *cell == '{' {
            depth += 1;
        } else if *cell == '}' {
            depth -= 1;
            if depth == 0 {
                close = Some(at);
                break;
            }
        }
    }
    let Some(end) = close else {
        return vec![pattern.to_string()];
    };
    let head: String = chars[..start].iter().collect();
    let tail: String = chars[end + 1..].iter().collect();
    let mut parts: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut nested = 0usize;
    let mut inner = chars[start + 1..end].iter().peekable();
    while let Some(cell) = inner.next() {
        if *cell == '\\' {
            current.push(*cell);
            if let Some(next) = inner.next() {
                current.push(*next);
            }
            continue;
        }
        if *cell == ',' && nested == 0 {
            parts.push(std::mem::take(&mut current));
        } else {
            if *cell == '{' {
                nested += 1;
            } else if *cell == '}' {
                nested = nested.saturating_sub(1);
            }
            current.push(*cell);
        }
    }
    parts.push(current);
    let mut out: Vec<String> = Vec::new();
    for part in &parts {
        for expanded in expand_braces_inner(&format!("{head}{part}{tail}"), depth + 1) {
            out.push(expanded);
        }
    }
    out
}

fn match_glob(pattern: &str, candidate: &str) -> bool {
    expand_braces(pattern).iter().any(|expanded| {
        let wants: Vec<&str> = expanded.split('/').collect();
        let gots: Vec<&str> = candidate.split('/').collect();
        wants.len() == gots.len()
            && wants
                .iter()
                .zip(gots.iter())
                .all(|pair| match_segment(pair.0, pair.1))
    })
}

fn split_glob_base(pattern: &str) -> (PathBuf, String) {
    let mut escaped = false;
    let mut meta_at: Option<usize> = None;
    for (at, byte) in pattern.bytes().enumerate() {
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if matches!(byte, b'*' | b'?' | b'[' | b'{') {
            meta_at = Some(at);
            break;
        }
    }
    let Some(meta) = meta_at else {
        return (PathBuf::from(pattern), String::new());
    };
    match pattern[..meta].rfind('/') {
        Some(slash) => (
            PathBuf::from(&pattern[..slash]),
            pattern[slash + 1..].to_string(),
        ),
        None => (PathBuf::from("."), pattern.to_string()),
    }
}

fn collect_glob_files(dir: &Path, display: &str, out: &mut Vec<PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir).map_err(|error| {
        Error::Analysis(format!(
            "text read cannot list directory {display:?}: {error}"
        ))
    })?;
    let mut ordered: Vec<std::fs::DirEntry> = Vec::new();
    for entry in entries {
        ordered.push(entry.map_err(|error| {
            Error::Analysis(format!(
                "text read cannot list directory {display:?}: {error}"
            ))
        })?);
    }
    ordered.sort_by_key(std::fs::DirEntry::path);
    for entry in &ordered {
        if is_hidden_name(&entry.file_name()) {
            continue;
        }
        let candidate = entry.path();
        let kind = entry.file_type().map_err(|error| {
            Error::Analysis(format!(
                "text read cannot list directory {display:?}: {error}"
            ))
        })?;
        if kind.is_dir() {
            collect_glob_files(&candidate, display, out)?;
        } else if kind.is_file() || (kind.is_symlink() && candidate.is_file()) {
            out.push(candidate);
        }
    }
    Ok(())
}

pub(crate) fn expand_text_glob(pattern: &str) -> Result<Vec<PathBuf>> {
    let (base, wanted) = split_glob_base(pattern);
    if !base.is_dir() {
        return Ok(Vec::new());
    }
    let mut candidates: Vec<PathBuf> = Vec::new();
    collect_glob_files(&base, pattern, &mut candidates)?;
    let base_prefix = base.to_string_lossy().replace('\\', "/");
    let mut matched: Vec<PathBuf> = Vec::new();
    for candidate in &candidates {
        let text = candidate.to_string_lossy().replace('\\', "/");
        let relative = text
            .strip_prefix(base_prefix.as_str())
            .unwrap_or(text.as_str())
            .trim_start_matches('/');
        if match_glob(&wanted, relative) {
            matched.push(candidate.clone());
        }
    }
    matched.sort();
    Ok(matched)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_segment_star_question() {
        assert!(match_segment("*.txt", "a.txt"));
        assert!(match_segment("*.txt", ".txt"));
        assert!(!match_segment("*.txt", "a.log"));
        assert!(!match_segment("*.txt", "a/b.txt"));
        assert!(match_segment("list_?.txt", "list_1.txt"));
        assert!(!match_segment("list_?.txt", "list_12.txt"));
    }

    #[test]
    fn glob_segment_class() {
        assert!(match_segment("foo[bar].txt", "foob.txt"));
        assert!(match_segment("foo[bar].txt", "fooa.txt"));
        assert!(!match_segment("foo[bar].txt", "foo[.txt"));
        assert!(!match_segment("foo[bar].txt", "foo[bar].txt"));
        assert!(match_segment("v[0-9].txt", "v7.txt"));
        assert!(!match_segment("v[0-9].txt", "vx.txt"));
        assert!(match_segment("v[^0-9].txt", "vx.txt"));
        assert!(!match_segment("v[^0-9].txt", "v7.txt"));
    }

    #[test]
    fn glob_braces_expand() {
        assert_eq!(
            expand_braces("{a,b}.txt"),
            vec!["a.txt".to_string(), "b.txt".to_string()]
        );
        assert!(match_glob("{a,b}/*.txt", "b/f.txt"));
        assert!(!match_glob("{a,b}/*.txt", "c/f.txt"));
    }

    #[test]
    fn glob_star_star_matches_within_segment() {
        assert!(match_glob("**/*.txt", "a/b.txt"));
        assert!(!match_glob("**/*.txt", "a/b/c.txt"));
    }
}
