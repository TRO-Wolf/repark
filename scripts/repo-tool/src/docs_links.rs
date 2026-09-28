use crate::validation::{Inputs, lines};
use crate::validation::{pattern, trim, whitespace};
use anyhow::{Context, Result, bail};
use regex::Regex;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

const ALLOWLIST: &str = "scripts/docs_links_allowlist.txt";

#[derive(Serialize)]
struct Output {
    ok: bool,
    diagnostics: Vec<String>,
    files: usize,
    links: usize,
}

struct Patterns {
    alphanumeric: Regex,
    link: Regex,
    code_span: Regex,
    fence: Regex,
    heading: Regex,
    heading_link: Regex,
    docs_cell: Regex,
    allowlist_entry: Regex,
}

impl Patterns {
    fn new() -> Result<Self> {
        Ok(Self {
            alphanumeric: pattern(r"[\p{L}\p{N}]")?,
            link: pattern(
                r#"\[[^\]]*\]\(\s*(<[^<>]*>|(?:[^()\s]|\([^()]*\))*)(?:\s+"[^"]*")?\s*\)"#,
            )?,
            code_span: pattern(r"`[^`]*`")?,
            fence: pattern(r"^ {0,3}(?:```|~~~)")?,
            heading: pattern(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$")?,
            heading_link: pattern(r"!?\[([^\]]*)\]\([^()]*\)")?,
            docs_cell: pattern(r"docs:\s*([^\s|`]+)")?,
            allowlist_entry: pattern(r"[^\s:]+:\S+")?,
        })
    }
}

#[expect(
    clippy::missing_errors_doc,
    reason = "CLI error contract is documented in INTERFACE.md"
)]
pub fn run(inputs: &mut Inputs) -> Result<Value> {
    let patterns = Patterns::new()?;
    let allowlist = load_allowlist(inputs, &patterns)?;
    let documents: Vec<String> = inputs
        .tracked
        .iter()
        .filter(|path| path.as_bytes().ends_with(b".md"))
        .cloned()
        .collect();
    let mut anchors = BTreeMap::new();
    let mut diagnostics = Vec::new();
    let mut matched = BTreeSet::new();
    let mut links = 0;
    for relative in &documents {
        scan_file(
            inputs,
            relative,
            &patterns,
            &allowlist,
            &mut anchors,
            &mut matched,
            &mut diagnostics,
            &mut links,
        )?;
    }
    for entry in allowlist.difference(&matched) {
        diagnostics.push(format!(
            "{ALLOWLIST}: stale entry {entry} — the link is no longer broken; remove this row"
        ));
    }
    Ok(serde_json::to_value(Output {
        ok: diagnostics.is_empty(),
        diagnostics,
        files: documents.len(),
        links,
    })?)
}

fn load_allowlist(inputs: &mut Inputs, patterns: &Patterns) -> Result<BTreeSet<String>> {
    if !inputs.root.join(ALLOWLIST).exists() {
        return Ok(BTreeSet::new());
    }
    let mut entries = BTreeSet::new();
    let text = inputs.read(ALLOWLIST)?.to_owned();
    for (index, line) in lines(&text).enumerate() {
        let entry = trim(line);
        if entry.is_empty() || entry.starts_with('#') {
            continue;
        }
        if !patterns.allowlist_entry.is_match(entry)
            || patterns
                .allowlist_entry
                .find(entry)
                .map(|found| found.as_str())
                != Some(entry)
        {
            bail!(
                "{ALLOWLIST}:{}: malformed allowlist entry {entry:?}",
                index + 1
            );
        }
        entries.insert(entry.to_owned());
    }
    Ok(entries)
}

#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::too_many_lines,
    reason = "Python compatibility keeps ordered line scanning in one function"
)]
fn scan_file(
    inputs: &mut Inputs,
    relative: &str,
    patterns: &Patterns,
    allowlist: &BTreeSet<String>,
    anchors: &mut BTreeMap<String, BTreeSet<String>>,
    matched: &mut BTreeSet<String>,
    diagnostics: &mut Vec<String>,
    links: &mut usize,
) -> Result<()> {
    let source = inputs.read(relative)?.to_owned();
    let mut in_fence = false;
    let mut fence_open = 0;
    let mut fence_text = String::new();
    let source_base = inputs
        .root
        .join(relative)
        .parent()
        .context("source has no parent")?
        .to_path_buf();
    let ledger_base = inputs.root.clone();
    for (index, line) in lines(&source).enumerate() {
        let number = index + 1;
        if patterns.fence.is_match(line) {
            in_fence = !in_fence;
            if in_fence {
                fence_open = number;
                trim(line).clone_into(&mut fence_text);
            }
            continue;
        }
        if in_fence {
            continue;
        }
        let stripped = patterns.code_span.replace_all(line, "");
        for capture in patterns.link.captures_iter(&stripped) {
            let raw = capture.get(1).context("link capture missing")?.as_str();
            let target = local_target(raw);
            let (local, fragment) = target.split_once('#').unwrap_or((target, ""));
            if local.is_empty() {
                if !fragment.is_empty() {
                    *links += 1;
                    if !heading_anchors(inputs, relative, patterns, anchors)?.contains(fragment) {
                        record(
                            relative,
                            number,
                            raw,
                            &format!("anchor #{fragment} does not match a heading"),
                            allowlist,
                            matched,
                            diagnostics,
                        );
                    }
                }
                continue;
            }
            if local.starts_with('/') || is_external(local) {
                continue;
            }
            *links += 1;
            check_target(
                inputs,
                relative,
                number,
                raw,
                local,
                fragment,
                &source_base,
                patterns,
                allowlist,
                anchors,
                matched,
                diagnostics,
            )?;
        }
        if relative.starts_with("task/ledgers/")
            && stripped.trim_start_matches(whitespace).starts_with('|')
        {
            for capture in patterns.docs_cell.captures_iter(&stripped) {
                let start = capture.get(0).context("docs cell match missing")?.start();
                if stripped[..start]
                    .chars()
                    .next_back()
                    .is_some_and(|character| {
                        is_python_word(character, &patterns.alphanumeric)
                            || matches!(character, '.' | '/' | '-')
                    })
                {
                    continue;
                }
                let token = capture
                    .get(1)
                    .context("docs cell capture missing")?
                    .as_str()
                    .trim_end_matches(['.', ',', ';', ':', '!', '?']);
                if token.contains(['<', '>']) {
                    continue;
                }
                *links += 1;
                let (local, fragment) = token.split_once('#').unwrap_or((token, ""));
                check_target(
                    inputs,
                    relative,
                    number,
                    token,
                    local,
                    fragment,
                    &ledger_base,
                    patterns,
                    allowlist,
                    anchors,
                    matched,
                    diagnostics,
                )?;
            }
        }
    }
    if in_fence {
        record(
            relative,
            fence_open,
            &fence_text,
            "unclosed fenced code block",
            allowlist,
            matched,
            diagnostics,
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn check_target(
    inputs: &mut Inputs,
    relative: &str,
    number: usize,
    raw: &str,
    local: &str,
    fragment: &str,
    base: &Path,
    patterns: &Patterns,
    allowlist: &BTreeSet<String>,
    anchors: &mut BTreeMap<String, BTreeSet<String>>,
    matched: &mut BTreeSet<String>,
    diagnostics: &mut Vec<String>,
) -> Result<()> {
    let Some((target, exists, is_directory)) = resolve_target(&inputs.root, base, local)? else {
        record(
            relative,
            number,
            raw,
            "resolves outside the repository",
            allowlist,
            matched,
            diagnostics,
        );
        return Ok(());
    };
    if !exists {
        record(
            relative,
            number,
            raw,
            "does not exist",
            allowlist,
            matched,
            diagnostics,
        );
    } else if !is_tracked(&target, is_directory, &inputs.tracked) {
        record(
            relative,
            number,
            raw,
            "exists but is not tracked",
            allowlist,
            matched,
            diagnostics,
        );
    } else if !fragment.is_empty()
        && target.as_bytes().ends_with(b".md")
        && !heading_anchors(inputs, &target, patterns, anchors)?.contains(fragment)
    {
        record(
            relative,
            number,
            raw,
            &format!("anchor #{fragment} does not match a heading"),
            allowlist,
            matched,
            diagnostics,
        );
    }
    Ok(())
}

fn record(
    relative: &str,
    number: usize,
    raw: &str,
    reason: &str,
    allowlist: &BTreeSet<String>,
    matched: &mut BTreeSet<String>,
    diagnostics: &mut Vec<String>,
) {
    let key = format!("{relative}:{raw}");
    if allowlist.contains(&key) {
        matched.insert(key);
    } else {
        diagnostics.push(format!("{relative}:{number}: {raw} -> {reason}"));
    }
}

fn local_target(raw: &str) -> &str {
    let trimmed = trim(raw);
    if trimmed.starts_with('<') && trimmed.ends_with('>') {
        trim(&trimmed[1..trimmed.len() - 1])
    } else {
        trimmed.split_once(' ').map_or(trimmed, |(path, _)| path)
    }
}

fn is_external(local: &str) -> bool {
    let lower = local.to_lowercase();
    ["http://", "https://", "mailto:", "ftp://"]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
}

fn is_python_word(character: char, alphanumeric: &Regex) -> bool {
    let mut bytes = [0; 4];
    character == '_' || alphanumeric.is_match(character.encode_utf8(&mut bytes))
}

fn is_tracked(relative: &str, is_directory: bool, tracked: &BTreeSet<String>) -> bool {
    if !is_directory {
        return tracked.contains(relative);
    }
    let prefix = format!("{relative}/");
    tracked
        .range(prefix.clone()..)
        .next()
        .is_some_and(|entry| entry.starts_with(&prefix))
}

fn resolve_target(root: &Path, base: &Path, local: &str) -> Result<Option<(String, bool, bool)>> {
    let resolved = resolve_components(&base.join(local))?;
    let Ok(relative) = resolved.strip_prefix(root) else {
        return Ok(None);
    };
    let name = if relative.as_os_str().is_empty() {
        ".".to_owned()
    } else {
        relative
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/")
    };
    Ok(Some((name, resolved.exists(), resolved.is_dir())))
}

enum PathStep {
    Root,
    Parent,
    Name(std::ffi::OsString),
    EndLink(PathBuf),
}

fn push_components(path: &Path, pending: &mut Vec<PathStep>) {
    for component in path.components().rev() {
        match component {
            Component::RootDir => pending.push(PathStep::Root),
            Component::ParentDir => pending.push(PathStep::Parent),
            Component::Normal(part) => pending.push(PathStep::Name(part.to_owned())),
            Component::CurDir | Component::Prefix(_) => (),
        }
    }
}

fn resolve_components(path: &Path) -> Result<PathBuf> {
    let mut resolved = PathBuf::new();
    let mut pending = Vec::new();
    let mut active = BTreeSet::new();
    push_components(path, &mut pending);
    while let Some(step) = pending.pop() {
        match step {
            PathStep::Root => resolved = PathBuf::from("/"),
            PathStep::Parent => {
                resolved.pop();
            }
            PathStep::EndLink(link) => {
                active.remove(&link);
            }
            PathStep::Name(part) => {
                resolved.push(part);
                if fs::symlink_metadata(&resolved)
                    .is_ok_and(|metadata| metadata.file_type().is_symlink())
                {
                    if !active.insert(resolved.clone()) {
                        bail!("symlink loop while resolving {}", path.display());
                    }
                    let link = fs::read_link(&resolved)
                        .with_context(|| format!("read symlink {}", resolved.display()))?;
                    pending.push(PathStep::EndLink(resolved.clone()));
                    resolved.pop();
                    push_components(&link, &mut pending);
                }
            }
        }
    }
    Ok(resolved)
}

fn heading_anchors<'a>(
    inputs: &mut Inputs,
    relative: &str,
    patterns: &Patterns,
    cache: &'a mut BTreeMap<String, BTreeSet<String>>,
) -> Result<&'a BTreeSet<String>> {
    if !cache.contains_key(relative) {
        let text = inputs.read(relative)?.to_owned();
        let mut anchors = BTreeSet::new();
        let mut counts = BTreeMap::<String, usize>::new();
        let mut in_fence = false;
        for line in lines(&text) {
            if patterns.fence.is_match(line) {
                in_fence = !in_fence;
                continue;
            }
            if in_fence {
                continue;
            }
            let Some(capture) = patterns.heading.captures(line) else {
                continue;
            };
            let heading = capture.get(1).context("heading capture missing")?.as_str();
            let visible = patterns.heading_link.replace_all(heading, "$1");
            let slug = slug_text(&visible, &patterns.alphanumeric);
            let mut seen = *counts.get(&slug).unwrap_or(&0);
            let mut final_slug = if seen == 0 {
                slug.clone()
            } else {
                format!("{slug}-{seen}")
            };
            while anchors.contains(&final_slug) {
                seen += 1;
                final_slug = format!("{slug}-{seen}");
            }
            counts.insert(slug, seen + 1);
            anchors.insert(final_slug);
        }
        cache.insert(relative.to_owned(), anchors);
    }
    cache.get(relative).context("heading cache missing")
}

fn slug_text(text: &str, alphanumeric: &Regex) -> String {
    let mut result = String::new();
    for character in text.to_lowercase().chars() {
        if whitespace(character) {
            result.push('-');
        } else if is_python_word(character, alphanumeric) || character == '-' {
            result.push(character);
        }
    }
    result
}
