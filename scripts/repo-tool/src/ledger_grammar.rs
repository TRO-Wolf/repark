use crate::{
    ledger_records::{Grammar, Row},
    validation::Inputs,
};
use anyhow::{Result, bail};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const STAGING: &str = "task/ledgers/staging/";
const COMPLETED: &str = "task/ledgers/completed/";
const ARCHIVE: &str = "task/ledgers/archive/";
const SUFFIX: &str = "-ledger.md";
const EXCEPTIONS: &str = include_str!("../../ledger_grammar_exceptions.json");

#[derive(Serialize)]
struct Output {
    ok: bool,
    diagnostics: Vec<String>,
    live_ledgers: usize,
    clauses: usize,
    pinned_clauses: usize,
    exception_rows: usize,
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Grammar check errors are documented in CHECKS.md"
)]
pub fn run(inputs: &mut Inputs) -> Result<Value> {
    let grammar = Grammar::new()?;
    let exceptions: BTreeMap<String, (usize, bool)> = serde_json::from_str(EXCEPTIONS)?;
    let live = ledger_paths(inputs, &[STAGING, COMPLETED]);
    let archived = ledger_paths(inputs, &[ARCHIVE]);
    if live.is_empty() && archived.is_empty() {
        bail!("no ledgers found under task/ledgers — refuse to pass closed");
    }
    let mut findings = Vec::new();
    let mut known = BTreeSet::new();
    let mut proven = BTreeMap::new();
    let mut clauses = 0;
    for path in live.iter().chain(&archived) {
        let text = inputs.read(path)?;
        let rows = grammar.rows(text);
        let unit = unit_of(path, &grammar);
        let verdicts = if path.starts_with(ARCHIVE) {
            rows.iter()
                .map(|row| (row.clause.clone(), String::new()))
                .collect()
        } else {
            let mut order = Vec::new();
            let verdicts = check_rows(path, &rows, &grammar, &mut findings, &mut order);
            clauses += rows.len();
            if !grammar.reading(text) {
                proven.insert(
                    path.clone(),
                    order
                        .into_iter()
                        .filter(|id| verdicts.get(id).is_some_and(|value| value == "PROVEN"))
                        .collect::<Vec<_>>(),
                );
            }
            let governed = exceptions
                .get(basename(path))
                .copied()
                .unwrap_or((0, true))
                .1;
            if governed && rows.is_empty() {
                findings.push(format!(
                    "{path}: no clause table (a ledger is propositions first)"
                ));
            }
            let required =
                governed && !rows.is_empty() && !verdicts.values().any(|value| value == "OPEN");
            findings.extend(grammar.attestation(path, text, required));
            findings.extend(grammar.findings(path, text));
            verdicts
        };
        known.extend(verdicts.keys().map(|id| (unit.clone(), id.clone())));
    }
    let cited = citations(inputs, &grammar);
    for ((unit, clause), file) in &cited {
        if !known.contains(&(unit.clone(), clause.clone())) {
            findings.push(format!(
                "{file}: `pins: {unit}/{clause}` names no clause in any ledger"
            ));
        }
    }
    for (path, ids) in proven {
        let unit = unit_of(&path, &grammar);
        let unpinned: Vec<_> = ids
            .into_iter()
            .filter(|id| !cited.contains_key(&(unit.clone(), id.clone())))
            .collect();
        let ceiling = exceptions
            .get(basename(&path))
            .copied()
            .unwrap_or((0, true))
            .0;
        if unpinned.len() > ceiling {
            findings.push(format!("{path}: {} PROVEN clause(s) with no `pins: {unit}/C-NNN` citation (ceiling {ceiling}): {}", unpinned.len(), unpinned[..(unpinned.len()-ceiling).max(1)].join(", ")));
        } else if ceiling > 0 && unpinned.len() < ceiling {
            findings.push(format!("{path}: ceiling {ceiling} is above the measured {} — ratchet it down in EXCEPTIONS", unpinned.len()));
        }
    }
    for name in exceptions.keys() {
        if !live.contains(&format!("{STAGING}{name}"))
            && !live.contains(&format!("{COMPLETED}{name}"))
        {
            findings.push(format!(
                "EXCEPTIONS names {name}, which is in no live bin — delete the row"
            ));
        }
    }
    Ok(serde_json::to_value(Output {
        ok: findings.is_empty(),
        diagnostics: findings,
        live_ledgers: live.len(),
        clauses,
        pinned_clauses: cited.len(),
        exception_rows: exceptions.len(),
    })?)
}

fn ledger_paths(inputs: &Inputs, prefixes: &[&str]) -> Vec<String> {
    inputs
        .tracked
        .iter()
        .filter(|path| {
            prefixes.iter().any(|prefix| path.starts_with(prefix)) && path.ends_with(SUFFIX)
        })
        .cloned()
        .collect()
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or("")
}

fn unit_of(path: &str, grammar: &Grammar) -> String {
    let name = basename(path).strip_suffix(SUFFIX).unwrap_or("");
    if path.starts_with(ARCHIVE) {
        grammar.archive.replace(name, "").into_owned()
    } else {
        name.to_owned()
    }
}

fn check_rows(
    path: &str,
    rows: &[Row],
    grammar: &Grammar,
    findings: &mut Vec<String>,
    order: &mut Vec<String>,
) -> BTreeMap<String, String> {
    let mut verdicts = BTreeMap::new();
    for row in rows {
        let location = format!("{path}:{}: {}", row.number, row.clause);
        if verdicts.contains_key(&row.clause) {
            findings.push(format!("{location} duplicate clause id"));
        }
        let matches: Vec<_> = row
            .cells
            .iter()
            .filter_map(|cell| grammar.verdict.captures(cell))
            .map(|found| found[1].to_owned())
            .collect();
        if matches.len() != 1 {
            findings.push(format!(
                "{location} needs exactly one verdict cell (OPEN/PROVEN/REJECTED), found {}",
                matches.len()
            ));
            continue;
        }
        if !verdicts.contains_key(&row.clause) {
            order.push(row.clause.clone());
        }
        verdicts.insert(row.clause.clone(), matches[0].clone());
        if row
            .cells
            .iter()
            .filter(|cell| !cell.is_empty() && !grammar.verdict.is_match(cell))
            .count()
            < 2
        {
            findings.push(format!(
                "{location} needs the clause text and at least one evidence cell"
            ));
        }
    }
    verdicts
}

fn citations(inputs: &mut Inputs, grammar: &Grammar) -> BTreeMap<(String, String), String> {
    let paths: Vec<_> = inputs
        .tracked
        .iter()
        .filter(|path| {
            ["crates/", "python/", "scripts/"]
                .iter()
                .any(|root| path.starts_with(root))
        })
        .cloned()
        .collect();
    let mut found = BTreeMap::new();
    for path in paths {
        let Ok(text) = inputs.read(&path) else {
            continue;
        };
        for matched in grammar.citation.captures_iter(text) {
            for clause in grammar.clause.find_iter(&matched[2]) {
                found
                    .entry((matched[1].to_owned(), clause.as_str().to_owned()))
                    .or_insert_with(|| path.clone());
            }
        }
    }
    found
}
