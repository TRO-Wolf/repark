use crate::repository::{Repository, validate_relative};
use anyhow::{Result, bail};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use pulldown_cmark::{Event, Parser, Tag, TagEnd};
use regex::Regex;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const LINK_ESCAPE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'/')
    .remove(b'.')
    .remove(b'-')
    .remove(b'_');

const START: &str = "<!-- repo-tool:contents:start -->";
const END: &str = "<!-- repo-tool:contents:end -->";

#[derive(Serialize)]
struct Output<'a> {
    ok: bool,
    snapshot: &'a str,
    input_scope: &'a crate::repository::InputScope,
    maps: usize,
    written: Vec<String>,
    diagnostics: Vec<String>,
}

#[derive(Default)]
struct Options {
    mode: Mode,
    strict: bool,
    require_managed: bool,
    directory: Option<String>,
}

#[derive(Default, PartialEq, Eq)]
enum Mode {
    #[default]
    Check,
    Write,
    Init,
}

#[expect(
    clippy::missing_errors_doc,
    reason = "CLI error contract is documented in MAPS.md"
)]
pub fn run(repository: &Repository, arguments: &[String]) -> Result<Value> {
    let options = parse_options(repository, arguments)?;
    if options.mode != Mode::Check
        && Repository::load_scoped(&repository.root, "worktree", repository.input_scope.clone())?
            .snapshot
            != repository.snapshot
    {
        bail!("repository changed since snapshot");
    }
    let maps: Vec<String> = if let Some(path) = &options.directory {
        vec![map_path(path)]
    } else {
        repository
            .paths
            .iter()
            .filter(|path| path == &"map.md" || path.ends_with("/map.md"))
            .cloned()
            .collect()
    };
    if maps.is_empty() {
        bail!("no maps in snapshot");
    }
    let mut diagnostics = Vec::new();
    let mut written = Vec::new();
    for path in &maps {
        process_map(repository, path, &options, &mut diagnostics, &mut written)?;
    }
    Ok(serde_json::to_value(Output {
        ok: diagnostics.is_empty(),
        snapshot: &repository.snapshot,
        input_scope: &repository.input_scope,
        maps: maps.len(),
        written,
        diagnostics,
    })?)
}

fn parse_options(repository: &Repository, arguments: &[String]) -> Result<Options> {
    let mut options = Options::default();
    let mut check = false;
    let mut write = false;
    let mut init = false;
    let mut cursor = arguments.iter();
    while let Some(argument) = cursor.next() {
        match argument.as_str() {
            "--check" => check = true,
            "--write" => write = true,
            "--strict" => options.strict = true,
            "--init" => init = true,
            "--require-managed" => options.require_managed = true,
            "--path" => {
                options.directory = Some(
                    cursor
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--path needs a directory"))?
                        .clone(),
                );
            }
            other => bail!("unknown maps option: {other}"),
        }
    }
    if check && write {
        bail!("--check and --write conflict");
    }
    if options.require_managed && write {
        bail!("--require-managed conflicts with --write");
    }
    if init && (!write || options.directory.is_none()) {
        bail!("--init requires --write --path DIR");
    }
    if write && repository.source != "worktree" {
        bail!("maps --write requires worktree snapshot");
    }
    options.mode = if init {
        Mode::Init
    } else if write {
        Mode::Write
    } else {
        Mode::Check
    };
    if let Some(path) = &options.directory {
        if path != "." {
            validate_relative(path)?;
        }
        if !repository
            .directories()
            .contains(if path == "." { "" } else { path })
        {
            bail!("directory absent from snapshot: {path}");
        }
        if options.mode != Mode::Check && is_archive(path) {
            bail!("archive maps are immutable: {path}");
        }
    }
    Ok(options)
}

fn process_map(
    repository: &Repository,
    path: &str,
    options: &Options,
    diagnostics: &mut Vec<String>,
    written: &mut Vec<String>,
) -> Result<()> {
    let original = repository.files.get(path).map(String::as_str);
    if original.is_none() && options.mode != Mode::Init {
        diagnostics.push(format!("{path}: map is missing or is not UTF-8 text"));
        return Ok(());
    }
    let old = original.unwrap_or("");
    let markers = marker_span(old);
    let span = match markers {
        Ok(span) => span,
        Err(problem) => {
            diagnostics.push(format!("{path}: {problem}"));
            return Ok(());
        }
    };
    if options.mode == Mode::Init && span.is_some() {
        diagnostics.push(format!("{path}: already opted in"));
        return Ok(());
    }
    if options.require_managed && span.is_none() {
        diagnostics.push(format!("{path}: managed contents block required"));
    }
    if options.mode != Mode::Check && is_archive(parent(path)) {
        return Ok(());
    }
    let generated = inventory(repository, path);
    let candidate = if options.mode == Mode::Init {
        let prefix = if old.is_empty() {
            format!("# map — {}\n\n", parent(path))
        } else if old.ends_with('\n') {
            format!("{old}\n")
        } else {
            format!("{old}\n\n")
        };
        format!("{prefix}{START}\n{generated}{END}\n")
    } else if let Some((start, end)) = span {
        format!("{}{START}\n{generated}{END}{}", &old[..start], &old[end..])
    } else {
        old.to_owned()
    };
    if options.mode == Mode::Check {
        if candidate != old {
            diagnostics.push(format!(
                "{path}: managed contents differ from snapshot inventory"
            ));
        }
        check_links(repository, path, old, diagnostics);
        if options.strict {
            check_coverage(repository, path, old, diagnostics)?;
        }
    } else {
        if span.is_none() && options.mode != Mode::Init {
            return Ok(());
        }
        if candidate != old {
            repository.write_text(path, &candidate)?;
            written.push(path.to_owned());
        }
    }
    Ok(())
}

fn parent(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(parent, _)| parent)
}

fn map_path(directory: &str) -> String {
    if directory == "." {
        "map.md".to_owned()
    } else {
        format!("{directory}/map.md")
    }
}

fn is_archive(path: &str) -> bool {
    path == "docs/history"
        || path.starts_with("docs/history/")
        || path == "task/ledgers/archive"
        || path.starts_with("task/ledgers/archive/")
}

fn marker_span(text: &str) -> std::result::Result<Option<(usize, usize)>, &'static str> {
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for (_, offset, line) in outside_fence_lines(text) {
        let content = line.trim_end_matches(['\r', '\n']);
        if content == START {
            starts.push(offset + content.find(START).unwrap_or(0));
        } else if content == END {
            ends.push(offset + content.find(END).unwrap_or(0));
        } else if (content.contains(START) || content.contains(END)) && !content.contains('`') {
            return Err("contents marker must occupy its own line");
        }
    }
    if starts.is_empty() && ends.is_empty() {
        return Ok(None);
    }
    if starts.len() != 1 || ends.len() != 1 {
        return Err("malformed or multiple contents markers");
    }
    let start = starts[0];
    let end = ends[0] + END.len();
    if start >= ends[0] {
        return Err("contents markers are reversed");
    }
    Ok(Some((start, end)))
}

fn outside_fence_lines(text: &str) -> Vec<(usize, usize, &str)> {
    let mut lines = Vec::new();
    let mut fence: Option<(char, usize)> = None;
    let mut offset = 0;
    for (number, line) in text.split_inclusive('\n').enumerate() {
        let content = line.trim_end_matches(['\r', '\n']);
        let trimmed = content.trim_start_matches(' ');
        let indent = content.len() - trimmed.len();
        let delimiter = trimmed.chars().next();
        let run = delimiter.map_or(0, |character| {
            trimmed
                .chars()
                .take_while(|item| *item == character)
                .count()
        });
        let fence_line = indent <= 3 && matches!(delimiter, Some('`' | '~')) && run >= 3;
        if let Some((character, length)) = fence {
            if fence_line
                && delimiter == Some(character)
                && run >= length
                && trimmed.chars().skip(run).all(char::is_whitespace)
            {
                fence = None;
            }
        } else if fence_line {
            fence = delimiter.map(|character| (character, run));
        } else {
            lines.push((number + 1, offset, line));
        }
        offset += line.len();
    }
    lines
}

fn inventory(repository: &Repository, map: &str) -> String {
    let directory = parent(map);
    let prefix = if directory.is_empty() {
        String::new()
    } else {
        format!("{directory}/")
    };
    let mut entries = BTreeSet::new();
    for path in repository
        .paths
        .range(prefix.clone()..)
        .take_while(|path| path.starts_with(&prefix))
    {
        let Some(rest) = path.strip_prefix(&prefix) else {
            continue;
        };
        if rest == "map.md" {
            continue;
        }
        if let Some((child, _)) = rest.split_once('/') {
            if !child.starts_with('.')
                && repository
                    .paths
                    .contains(&format!("{prefix}{child}/map.md"))
            {
                entries.insert(format!("{child}/map.md"));
            }
        } else if mappable(rest) {
            entries.insert(rest.to_owned());
        }
    }
    entries
        .into_iter()
        .fold(String::new(), |mut output, entry| {
            use std::fmt::Write;
            let label = entry
                .replace('\\', "\\\\")
                .replace('[', "\\[")
                .replace(']', "\\]")
                .replace(['\n', '\r'], " ");
            let target = utf8_percent_encode(&entry, LINK_ESCAPE);
            let _ = writeln!(output, "- [{label}]({target})");
            output
        })
}

fn mappable(name: &str) -> bool {
    !name.starts_with('.')
        && name != "map.md"
        && name != "Cargo.lock"
        && name != "uv.lock"
        && matches!(
            Path::new(name)
                .extension()
                .and_then(|suffix| suffix.to_str()),
            Some("rs" | "py" | "sh" | "md" | "toml")
        )
}

fn local_target(target: &str) -> Option<&str> {
    let value = target.trim();
    let lower = value.to_ascii_lowercase();
    if lower.starts_with('#')
        || ["http://", "https://", "mailto:", "ftp://"]
            .iter()
            .any(|prefix| lower.starts_with(prefix))
    {
        return None;
    }
    Some(value.split('#').next().unwrap_or(""))
}

fn resolve(map: &str, target: &str) -> std::result::Result<String, &'static str> {
    let decoded = percent_decode_str(target)
        .decode_utf8()
        .map_err(|_| "invalid UTF-8 link")?;
    if decoded.starts_with('/') {
        return Err("absolute path");
    }
    let mut parts: Vec<&str> = parent(map)
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    for part in decoded.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return Err("escapes repository");
                }
            }
            other => parts.push(other),
        }
    }
    Ok(parts.join("/"))
}

fn check_links(repository: &Repository, map: &str, text: &str, diagnostics: &mut Vec<String>) {
    let mut seen = BTreeMap::new();
    let managed = marker_span(text).ok().flatten();
    let mut item_stack = Vec::new();
    let line_starts: Vec<_> = std::iter::once(0)
        .chain(text.match_indices('\n').map(|(index, _)| index + 1))
        .collect();
    for (event, range) in Parser::new(text).into_offset_iter() {
        match event {
            Event::Start(Tag::Item) => item_stack.push(false),
            Event::End(TagEnd::Item) => {
                item_stack.pop();
            }
            Event::Start(Tag::Link { dest_url, .. } | Tag::Image { dest_url, .. }) => {
                let number = line_starts.partition_point(|start| *start <= range.start);
                let target = dest_url.as_ref();
                validate_link(repository, map, number, target, diagnostics);
                if let Some(first_link) = item_stack.last_mut()
                    && !*first_link
                    && !managed.is_some_and(|(start, end)| (start..end).contains(&range.start))
                {
                    *first_link = true;
                    if let Some(first) = seen.get(target) {
                        diagnostics.push(format!(
                            "{map}:{number}: duplicate row: {target} first appears at line {first}"
                        ));
                    } else {
                        seen.insert(target.to_owned(), number);
                    }
                }
            }
            _ => {}
        }
    }
}

fn validate_link(
    repository: &Repository,
    map: &str,
    number: usize,
    target: &str,
    diagnostics: &mut Vec<String>,
) {
    if let Some(local) = local_target(target)
        && !local.is_empty()
    {
        match resolve(map, local) {
            Ok(path) if repository.contains(&path) => {}
            Ok(_) => diagnostics.push(format!("{map}:{number}: dead link: {local}")),
            Err(problem) => {
                diagnostics.push(format!("{map}:{number}: link {local}: {problem}"));
            }
        }
    }
}

fn check_coverage(
    repository: &Repository,
    map: &str,
    text: &str,
    diagnostics: &mut Vec<String>,
) -> Result<()> {
    let directory = parent(map);
    let prefix = if directory.is_empty() {
        String::new()
    } else {
        format!("{directory}/")
    };
    for path in repository
        .paths
        .range(prefix.clone()..)
        .take_while(|path| path.starts_with(&prefix))
    {
        let Some(rest) = path.strip_prefix(&prefix) else {
            continue;
        };
        if rest.contains('/') || !mappable(rest) {
            continue;
        }
        let escaped = regex::escape(rest);
        let pattern = Regex::new(&format!(r"(^|[^\w./-]){escaped}($|[^\w.-])"))?;
        if !pattern.is_match(text) {
            diagnostics.push(format!("{map}: unmentioned: {rest}"));
        }
    }
    Ok(())
}
