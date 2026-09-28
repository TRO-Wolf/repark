use crate::repository::Repository;
use anyhow::{Result, bail};
use regex::Regex;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Serialize)]
struct ClauseRecord {
    unit: String,
    clause: String,
    status: String,
    source: String,
    pin_markers: Vec<String>,
}

#[derive(Serialize)]
struct TraceResult<'a> {
    ok: bool,
    snapshot: &'a str,
    source: &'a str,
    clauses: Vec<ClauseRecord>,
    diagnostics: Vec<String>,
    proof: &'static str,
}

#[expect(
    clippy::missing_errors_doc,
    reason = "CLI errors are documented in CONTEXT.md"
)]
#[expect(
    clippy::too_many_lines,
    reason = "Trace scans ledger rows and source citations in fixed passes"
)]
pub fn run(repository: &Repository, arguments: &[String]) -> Result<Value> {
    let selected = match arguments {
        [] => None,
        [flag, value] if flag == "--unit" && !value.is_empty() && !value.contains('/') => {
            Some(value.as_str())
        }
        _ => bail!("trace accepts only [--unit ID]"),
    };
    let row = Regex::new(r"^\|\s*(C-\d{3})\s*\|(.*)$")?;
    let verdict = Regex::new(r"^\**(PROVEN|OPEN|REJECTED)\**(?:\s*\(.*\))?$")?;
    let marker = Regex::new(r"pins:\s*([a-z0-9][a-z0-9.-]*)/(C-\d{3}(?:\s*,\s*C-\d{3})*)")?;
    let clause_id = Regex::new(r"C-\d{3}")?;
    let archive_prefix = Regex::new(r"^\d{4}-\d{2}-\d{2}-")?;
    let mut ledger_paths: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut records = BTreeMap::new();
    let mut diagnostics = Vec::new();
    for (path, contents) in &repository.files {
        if !path.starts_with("task/ledgers/") || !path.ends_with("-ledger.md") {
            continue;
        }
        let basename = path
            .rsplit('/')
            .next()
            .unwrap_or("")
            .trim_end_matches("-ledger.md");
        let unit = if path.starts_with("task/ledgers/archive/") {
            archive_prefix.replace(basename, "").into_owned()
        } else {
            basename.to_owned()
        };
        if selected.is_some_and(|requested| requested != unit) {
            continue;
        }
        ledger_paths
            .entry(unit.clone())
            .or_default()
            .push(path.clone());
        let mut fence = None;
        for (index, line) in contents.lines().enumerate() {
            if advance_fence(line, &mut fence) {
                continue;
            }
            if fence.is_some() {
                continue;
            }
            let Some(capture) = row.captures(line) else {
                continue;
            };
            let clause = capture[1].to_owned();
            let cells: Vec<_> = capture[2].split('|').map(str::trim).collect();
            let statuses: Vec<_> = cells
                .iter()
                .filter_map(|cell| verdict.captures(cell).map(|found| found[1].to_owned()))
                .collect();
            let where_ = format!("{path}:{}", index + 1);
            if statuses.len() != 1 {
                diagnostics.push(format!("{where_}: {clause} needs exactly one verdict cell"));
                continue;
            }
            if records
                .insert(
                    (unit.clone(), clause.clone()),
                    (statuses[0].clone(), where_),
                )
                .is_some()
            {
                diagnostics.push(format!(
                    "{path}:{}: duplicate clause {unit}/{clause}",
                    index + 1
                ));
            }
        }
    }
    for (unit, paths) in &ledger_paths {
        if paths.len() > 1 {
            diagnostics.push(format!("duplicate unit {unit}: {}", paths.join(", ")));
        }
        if !records.keys().any(|(record_unit, _)| record_unit == unit) {
            diagnostics.push(format!("unit {unit} has no clause rows"));
        }
    }
    if ledger_paths.is_empty() {
        diagnostics.push(selected.map_or("no relevant ledgers".to_owned(), |unit| {
            format!("unit {unit} has no ledger")
        }));
    }
    let mut citations: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for (path, contents) in &repository.files {
        if !["crates/", "python/", "scripts/"]
            .iter()
            .any(|root| path.starts_with(root))
        {
            continue;
        }
        let mut fence = None;
        for (index, line) in contents.lines().enumerate() {
            if advance_fence(line, &mut fence) || fence.is_some() {
                continue;
            }
            for found in marker.captures_iter(line) {
                let unit = found[1].to_owned();
                if selected.is_some_and(|requested| requested != unit) {
                    continue;
                }
                for clause in clause_id.find_iter(&found[2]) {
                    let key = (unit.clone(), clause.as_str().to_owned());
                    if !records.contains_key(&key) {
                        diagnostics.push(format!(
                            "{path}:{}: unknown pin {}/{}",
                            index + 1,
                            unit,
                            clause.as_str()
                        ));
                    }
                    citations
                        .entry(key)
                        .or_default()
                        .insert(format!("{path}:{}", index + 1));
                }
            }
        }
    }
    let mut clauses = Vec::new();
    for ((unit, clause), (status, source)) in records {
        let pin_markers: Vec<_> = citations
            .remove(&(unit.clone(), clause.clone()))
            .unwrap_or_default()
            .into_iter()
            .collect();
        if status == "PROVEN" && pin_markers.is_empty() {
            diagnostics.push(format!("{source}: unpinned PROVEN clause {unit}/{clause}"));
        }
        clauses.push(ClauseRecord {
            unit,
            clause,
            status,
            source,
            pin_markers,
        });
    }
    diagnostics.sort();
    Ok(serde_json::to_value(TraceResult {
        ok: diagnostics.is_empty(),
        snapshot: &repository.snapshot,
        source: &repository.source,
        clauses,
        diagnostics,
        proof: "recorded citations only; no semantic proof or test identity inferred",
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
