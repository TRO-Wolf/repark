use crate::validation::{pattern, trim};
use anyhow::Result;
use regex::Regex;
use std::collections::BTreeMap;

pub(crate) struct Grammar {
    pub row: Regex,
    pub verdict: Regex,
    pub citation: Regex,
    pub clause: Regex,
    pub archive: Regex,
    fence: Regex,
    reading: Regex,
    code: Regex,
    nonprintable: Regex,
}

pub(crate) struct Row {
    pub number: usize,
    pub clause: String,
    pub cells: Vec<String>,
}

impl Grammar {
    pub fn new() -> Result<Self> {
        Ok(Self {
            row: pattern(r"^\|\s*(C-\d{3})\s*\|(.*)$")?,
            verdict: pattern(r"^\**(PROVEN|OPEN|REJECTED)\**(?:\s*\(.*\))?$")?,
            citation: pattern(r"pins:\s*([a-z0-9][a-z0-9.-]*)/(C-\d{3}(?:\s*,\s*C-\d{3})*)")?,
            clause: pattern(r"C-\d{3}")?,
            archive: pattern(r"^\d{4}-\d{2}-\d{2}-")?,
            fence: pattern(r"^\s*(```|~~~)")?,
            reading: pattern(r"\*\*Path:\*\*\s*([A-Za-z0-9_-]+)")?,
            code: pattern(r"`[^`]*`")?,
            nonprintable: pattern(r"[\p{C}\p{Z}]")?,
        })
    }

    pub fn rows(&self, text: &str) -> Vec<Row> {
        let mut fenced = false;
        let mut rows = Vec::new();
        for (index, line) in crate::validation::lines(text).enumerate() {
            if self.fence.is_match(line) {
                fenced = !fenced;
                continue;
            }
            if fenced {
                continue;
            }
            if let Some(found) = self.row.captures(line) {
                let mut cells: Vec<_> = found[2]
                    .split('|')
                    .map(|cell| trim(cell).to_owned())
                    .collect();
                if cells.last().is_some_and(String::is_empty) {
                    cells.pop();
                }
                rows.push(Row {
                    number: index + 1,
                    clause: found[1].to_owned(),
                    cells,
                });
            }
        }
        rows
    }

    pub fn reading(&self, text: &str) -> bool {
        crate::validation::lines(text).take(40).any(|line| {
            self.reading
                .captures(&self.code.replace_all(line, ""))
                .is_some_and(|found| &found[1] == "READING")
        })
    }

    fn blocks<'a>(&self, text: &'a str, marker: &str) -> Vec<Vec<&'a str>> {
        let mut blocks = Vec::new();
        let mut current: Option<Vec<&str>> = None;
        for line in crate::validation::lines(text) {
            if self.fence.is_match(line) {
                if let Some(block) = current.take() {
                    if block
                        .first()
                        .is_some_and(|line| trim(line).starts_with(marker))
                    {
                        blocks.push(block);
                    }
                } else {
                    current = Some(Vec::new());
                }
            } else if let Some(block) = &mut current {
                block.push(line);
            }
        }
        blocks
    }

    pub fn attestation(&self, path: &str, text: &str, required: bool) -> Vec<String> {
        let blocks = self.blocks(text, "COVERAGE_ATTESTATION:");
        let mut findings = Vec::new();
        let Some(block) = blocks.first() else {
            if required {
                findings.push(format!(
                    "{path}: no COVERAGE_ATTESTATION block (ref 05 shape, in a fenced block)"
                ));
            }
            return findings;
        };
        if blocks.len() > 1 {
            findings.push(format!(
                "{path}: {} COVERAGE_ATTESTATION blocks; file one",
                blocks.len()
            ));
        }
        let mut entries: BTreeMap<&str, BTreeMap<&str, &str>> = BTreeMap::new();
        let mut current = None;
        let mut complete = None;
        for line in block.iter().map(|line| trim(line)) {
            if let Some(id) = line.strip_prefix("- id:") {
                let id = trim(id);
                if entries.contains_key(id) {
                    current = None;
                } else {
                    entries.insert(id, BTreeMap::new());
                    current = Some(id);
                }
            } else if let Some(value) = line.strip_prefix("complete:") {
                complete = Some(uncomment(value));
                current = None;
            } else if let Some(id) = current
                && !line.starts_with('-')
                && let Some((key, value)) = line.split_once(':')
                && let Some(entry) = entries.get_mut(id)
            {
                entry.insert(trim(key), uncomment(value));
            }
        }
        let mut satisfied = true;
        for number in 1..=10 {
            let category = format!("AT-{number}");
            let Some(entry) = entries.get(category.as_str()) else {
                findings.push(format!("{path}: attestation lacks {category}"));
                satisfied = false;
                continue;
            };
            let status = entry.get("status").copied().unwrap_or("");
            let problem = match status {
                "ATTACKED"
                    if matches!(entry.get("artifacts").copied().unwrap_or(""), "" | "[]") =>
                {
                    Some("ATTACKED without artifacts".to_owned())
                }
                "N/A" if entry.get("justification").copied().unwrap_or("").is_empty() => {
                    Some("N/A without justification".to_owned())
                }
                "ATTACKED" | "N/A" => None,
                _ => Some(format!(
                    "status must be ATTACKED or N/A, found {}",
                    python_repr(status, &self.nonprintable)
                )),
            };
            if let Some(problem) = problem {
                findings.push(format!("{path}: {category} {problem}"));
                satisfied = false;
            }
        }
        for category in entries.keys() {
            if !(1..=10).any(|number| **category == format!("AT-{number}")) {
                findings.push(format!(
                    "{path}: attestation names unknown category {category}"
                ));
            }
        }
        match complete {
            None => findings.push(format!("{path}: attestation has no `complete:` line")),
            Some(value) if (value == "true") != satisfied => findings.push(format!(
                "{path}: attestation says complete: {value} but the categories say {satisfied}"
            )),
            Some(_) => {}
        }
        findings
    }

    pub fn findings(&self, path: &str, text: &str) -> Vec<String> {
        let mut findings = Vec::new();
        for block in self.blocks(text, "FINDING:") {
            let mut fields = BTreeMap::new();
            for line in block.iter().skip(1).map(|line| trim(line)) {
                if !line.starts_with(['-', '|'])
                    && let Some((key, value)) = line.split_once(':')
                {
                    fields.insert(trim(key), uncomment(value));
                }
            }
            let ident = fields.get("id").copied().unwrap_or("<no id>");
            let location = format!("{path}: FINDING {ident}");
            if !fields.contains_key("id") {
                findings.push(format!("{location} has no id"));
            }
            if !matches!(
                fields.get("severity").copied(),
                Some("S0" | "S1" | "S2" | "S3")
            ) {
                findings.push(format!("{location} severity must be S0..S3"));
            }
            if !(1..=10).any(|number| {
                fields.get("category").copied() == Some(format!("AT-{number}").as_str())
            }) {
                findings.push(format!("{location} category must be AT-1..AT-10"));
            }
            if !self
                .clause
                .is_match(fields.get("clause").copied().unwrap_or(""))
            {
                findings.push(format!(
                    "{location} names no charter clause (orphan work under D5)"
                ));
            }
            if !["OPEN", "REMEDIATED", "ACCEPTED_FLAGGED", "DISPUTED"]
                .iter()
                .any(|prefix| {
                    fields
                        .get("disposition")
                        .copied()
                        .unwrap_or("")
                        .starts_with(prefix)
                })
            {
                findings.push(format!("{location} disposition must start with one of OPEN, REMEDIATED, ACCEPTED_FLAGGED, DISPUTED"));
            }
        }
        findings
    }
}

fn uncomment(value: &str) -> &str {
    trim(value.split('#').next().unwrap_or(""))
}

fn python_repr(text: &str, nonprintable: &Regex) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut output = String::from(quote);
    let mut bytes = [0; 4];
    for character in text.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            value if value == quote => {
                output.push('\\');
                output.push(value);
            }
            value if value != ' ' && nonprintable.is_match(value.encode_utf8(&mut bytes)) => {
                output.push_str(&escape_code(value));
            }
            value => output.push(value),
        }
    }
    output.push(quote);
    output
}

fn escape_code(character: char) -> String {
    let value = u32::from(character);
    match value {
        0..=0xff => format!("\\x{value:02x}"),
        0x100..=0xffff => format!("\\u{value:04x}"),
        _ => format!("\\U{value:08x}"),
    }
}
