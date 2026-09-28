use crate::repository::Repository;
use anyhow::{Result, bail};
use regex::Regex;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Serialize)]
struct Marker {
    path: String,
    line: usize,
    kind: String,
    id: String,
    attributes: BTreeMap<String, String>,
    inline: bool,
}

#[derive(Serialize)]
struct Unit {
    id: String,
    bins: BTreeSet<String>,
}

#[derive(Serialize)]
struct StateResult<'a> {
    ok: bool,
    snapshot: &'a str,
    source: &'a str,
    units: Vec<Unit>,
    markers: Vec<Marker>,
    diagnostics: Vec<String>,
}

#[expect(
    clippy::missing_errors_doc,
    reason = "CLI errors are documented in CONTEXT.md"
)]
#[expect(
    clippy::too_many_lines,
    reason = "The marker and ledger views share one ordered pass"
)]
pub fn run(repository: &Repository, arguments: &[String]) -> Result<Value> {
    if !arguments.is_empty() {
        bail!("state accepts no arguments");
    }
    let mut units: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut diagnostics = Vec::new();
    let archive =
        Regex::new(r"^task/ledgers/archive/\d{4}-\d{2}/\d{4}-\d{2}-\d{2}-(.+)-ledger\.md$")?;
    for path in &repository.paths {
        let entry = if let Some(name) = path
            .strip_prefix("task/ledgers/staging/")
            .and_then(|name| name.strip_suffix("-ledger.md"))
        {
            Some((name, "staging"))
        } else if let Some(name) = path
            .strip_prefix("task/ledgers/completed/")
            .and_then(|name| name.strip_suffix("-ledger.md"))
        {
            Some((name, "completed"))
        } else {
            archive
                .captures(path)
                .map(|capture| (capture.get(1).map_or("", |value| value.as_str()), "archive"))
        };
        if let Some((name, state)) = entry {
            units
                .entry(name.to_owned())
                .or_default()
                .insert(state.to_owned());
        }
    }
    for (name, locations) in &units {
        if locations.len() > 1 {
            diagnostics.push(format!(
                "unit {name} appears in multiple ledger bins: {}",
                locations.iter().cloned().collect::<Vec<_>>().join(", ")
            ));
        }
    }
    let marker = Regex::new(r"<!--\s*(/?)(ws|unit)\b([^>]*?)-->")?;
    let marker_start = Regex::new(r"<!--\s*/?(?:ws|unit)\b")?;
    let attribute = Regex::new(r"([a-z]+)=([^\s]+)")?;
    let date = Regex::new(r"^\d{4}-\d{2}-\d{2}$")?;
    let history = Regex::new(r"^docs/history/[a-z0-9][a-z0-9-]*$")?;
    let pr = Regex::new(r"^#\d+$")?;
    let mut markers = Vec::new();
    for path in ["STATUS.md", "briefs/next-sequence.md"] {
        let Some(contents) = repository.files.get(path) else {
            diagnostics.push(format!("{path}: missing lifecycle document"));
            continue;
        };
        let mut open: Option<String> = None;
        let mut fence = None;
        for (index, line) in contents.lines().enumerate() {
            if advance_fence(line, &mut fence) {
                continue;
            }
            if fence.is_some() {
                continue;
            }
            for start in marker_start.find_iter(line) {
                if line[..start.start()].matches('`').count() % 2 == 1 {
                    continue;
                }
                if !marker
                    .find_iter(line)
                    .any(|found| found.start() == start.start())
                {
                    diagnostics.push(format!("{path}:{}: malformed lifecycle marker", index + 1));
                }
            }
            for capture in marker.captures_iter(line) {
                let full = capture.get(0).map_or("", |value| value.as_str());
                if line.contains('`')
                    && line.split('`').any(|part| {
                        part.contains(full)
                            && line
                                .find(part)
                                .is_some_and(|start| line[..start].matches('`').count() % 2 == 1)
                    })
                {
                    continue;
                }
                let kind = &capture[2];
                let closing = &capture[1] == "/";
                let where_ = format!("{path}:{}", index + 1);
                if closing {
                    if open.as_deref() == Some(kind) {
                        open = None;
                    } else {
                        diagnostics.push(format!("{where_}: unmatched /{kind}"));
                    }
                    continue;
                }
                let mut attrs = BTreeMap::new();
                for item in attribute.captures_iter(&capture[3]) {
                    if attrs
                        .insert(item[1].to_owned(), item[2].to_owned())
                        .is_some()
                    {
                        diagnostics
                            .push(format!("{where_}: duplicate marker attribute {}", &item[1]));
                    }
                }
                let id = attrs.get("id").cloned().unwrap_or_default();
                if id.is_empty() {
                    diagnostics.push(format!("{where_}: {kind} marker has no id"));
                }
                if kind == "ws" {
                    let state = attrs.get("state").map(String::as_str);
                    if !matches!(state, Some("open" | "held" | "closed")) {
                        diagnostics.push(format!("{where_}: invalid ws state"));
                    }
                    if state == Some("closed") {
                        if !attrs
                            .get("closed")
                            .is_some_and(|value| date.is_match(value))
                        {
                            diagnostics
                                .push(format!("{where_}: closed ws needs valid closed date"));
                        }
                        if !attrs.get("by").is_some_and(|value| pr.is_match(value)) {
                            diagnostics.push(format!("{where_}: closed ws needs valid by PR"));
                        }
                        if !attrs
                            .get("history")
                            .is_some_and(|value| history.is_match(value))
                        {
                            diagnostics
                                .push(format!("{where_}: closed ws needs valid history path"));
                        }
                    }
                }
                let inline = kind == "unit" && line.starts_with('|');
                if open.is_some() && !inline {
                    diagnostics.push(format!("{where_}: nested lifecycle marker"));
                }
                if !inline {
                    open = Some(kind.to_owned());
                }
                markers.push(Marker {
                    path: path.to_owned(),
                    line: index + 1,
                    kind: kind.to_owned(),
                    id,
                    attributes: attrs,
                    inline,
                });
            }
        }
        if let Some(kind) = open {
            diagnostics.push(format!("{path}: {kind} marker never closes"));
        }
    }
    let units: Vec<_> = units
        .into_iter()
        .map(|(id, bins)| Unit { id, bins })
        .collect();
    diagnostics.sort();
    Ok(serde_json::to_value(StateResult {
        ok: diagnostics.is_empty(),
        snapshot: &repository.snapshot,
        source: &repository.source,
        units,
        markers,
        diagnostics,
    })?)
}

fn advance_fence(line: &str, fence: &mut Option<(u8, usize)>) -> bool {
    let content = line.trim_start();
    let Some(marker) = content.as_bytes().first().copied() else {
        return false;
    };
    if marker != b'`' && marker != b'~' {
        return false;
    }
    let width = content.bytes().take_while(|byte| *byte == marker).count();
    if width < 3 {
        return false;
    }
    match fence {
        Some((opened, length))
            if *opened == marker && width >= *length && content[width..].trim().is_empty() =>
        {
            *fence = None;
        }
        None => *fence = Some((marker, width)),
        _ => {}
    }
    true
}
