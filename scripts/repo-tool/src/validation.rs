use crate::{docs_links, ledger_grammar, repository};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Serialize)]
struct Output {
    ok: bool,
    source: &'static str,
    checks: BTreeMap<String, Value>,
}

pub struct Inputs {
    pub root: PathBuf,
    pub tracked: BTreeSet<String>,
    texts: BTreeMap<String, String>,
}

impl Inputs {
    #[expect(
        clippy::missing_errors_doc,
        reason = "Worktree check errors are documented in CHECKS.md"
    )]
    pub fn load(root: &Path) -> Result<Self> {
        let root = root
            .canonicalize()
            .context("repository root does not exist")?;
        let top = repository::git(&root, &["rev-parse", "--show-toplevel"])?;
        if PathBuf::from(String::from_utf8(top)?.trim()).canonicalize()? != root {
            bail!("--repo must name the repository root");
        }
        let entries = repository::git(&root, &["ls-files", "-z"])?;
        let mut tracked = BTreeSet::new();
        for path in entries
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
        {
            let path = std::str::from_utf8(path).context("Git path is not UTF-8")?;
            repository::validate_relative(path)?;
            tracked.insert(path.to_owned());
        }
        Ok(Self {
            root,
            tracked,
            texts: BTreeMap::new(),
        })
    }

    #[expect(
        clippy::missing_errors_doc,
        reason = "Worktree check errors are documented in CHECKS.md"
    )]
    pub fn read(&mut self, path: &str) -> Result<&str> {
        repository::validate_relative(path)?;
        if !self.texts.contains_key(path) {
            let text = std::fs::read_to_string(self.root.join(path))
                .with_context(|| format!("read {path}"))?;
            let text = if text.contains('\r') {
                text.replace("\r\n", "\n").replace('\r', "\n")
            } else {
                text
            };
            self.texts.insert(path.to_owned(), text);
        }
        self.texts
            .get(path)
            .map(String::as_str)
            .context("input text was not retained")
    }
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Worktree check errors are documented in CHECKS.md"
)]
pub fn run(root: &Path, arguments: &[String]) -> Result<Value> {
    let names: Vec<&str> = match arguments {
        [] => vec!["docs-links", "ledger-grammar"],
        [name] if matches!(name.as_str(), "docs-links" | "ledger-grammar") => vec![name],
        _ => bail!("checks accepts only [docs-links|ledger-grammar]"),
    };
    let mut inputs = Inputs::load(root)?;
    let mut checks = BTreeMap::new();
    for name in names {
        let result = match name {
            "docs-links" => docs_links::run(&mut inputs)?,
            _ => ledger_grammar::run(&mut inputs)?,
        };
        checks.insert(name.to_owned(), result);
    }
    let ok = checks.values().all(|value| value["ok"] == true);
    Ok(serde_json::to_value(Output {
        ok,
        source: "worktree",
        checks,
    })?)
}

pub fn lines(text: &str) -> impl Iterator<Item = &str> {
    text.split_terminator([
        '\n', '\u{b}', '\u{c}', '\u{1c}', '\u{1d}', '\u{1e}', '\u{85}', '\u{2028}', '\u{2029}',
    ])
}

#[must_use]
pub fn whitespace(character: char) -> bool {
    character.is_whitespace() || matches!(character, '\u{1c}'..='\u{1f}')
}

#[must_use]
pub fn trim(text: &str) -> &str {
    text.trim_matches(whitespace)
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Compatibility regex errors are documented in CHECKS.md"
)]
pub fn pattern(expression: &str) -> Result<regex::Regex> {
    Ok(regex::Regex::new(
        &expression
            .replace(r"\s", r"[\s\x1c-\x1f]")
            .replace(r"\S", r"[^\s\x1c-\x1f]"),
    )?)
}
