use crate::repository::{Repository, validate_relative};
use anyhow::{Result, bail};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Pin {
    name: String,
    kind: String,
    path: String,
    key: String,
    expected: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    version: u32,
    targets: Vec<String>,
    pins: Vec<Pin>,
}

#[derive(Serialize)]
struct GatesResult<'a> {
    ok: bool,
    snapshot: &'a str,
    source: &'a str,
    targets: &'a [String],
    pins: &'a [Pin],
    diagnostics: Vec<String>,
}

#[expect(
    clippy::missing_errors_doc,
    reason = "CLI errors are documented in CONTEXT.md"
)]
pub fn run(repository: &Repository, arguments: &[String]) -> Result<Value> {
    let [flag, path] = arguments else {
        bail!("gates requires --config PATH");
    };
    if flag != "--config" {
        bail!("gates requires --config PATH");
    }
    validate_relative(path)?;
    let config: Config = serde_json::from_str(repository.read(path)?)?;
    if config.version != 1 {
        bail!("unsupported gates config version");
    }
    if config.targets.is_empty() || config.pins.is_empty() {
        bail!("gates config needs targets and pins");
    }
    let mut names = BTreeSet::new();
    for target in &config.targets {
        if !names.insert(target.as_str()) {
            bail!("duplicate target: {target}");
        }
    }
    for required in ["ci", "verify", "preflight"] {
        if !names.contains(required) {
            bail!("required target omitted: {required}");
        }
    }
    let mut pin_names = BTreeSet::new();
    for pin in &config.pins {
        if pin.name.is_empty()
            || pin.key.is_empty()
            || !["make", "toml"].contains(&pin.kind.as_str())
        {
            bail!("pin needs a name, supported kind, and key");
        }
        validate_relative(&pin.path)?;
        if !pin_names.insert(&pin.name) {
            bail!("duplicate pin: {}", pin.name);
        }
    }
    let makefile = repository.read("Makefile")?;
    let rule = Regex::new(r"(?m)^([A-Za-z0-9_.-]+):(?:\s|$)")?;
    let actual: BTreeSet<_> = rule
        .captures_iter(makefile)
        .map(|capture| capture[1].to_owned())
        .collect();
    let mut diagnostics = Vec::new();
    for target in &config.targets {
        if !actual.contains(target) {
            diagnostics.push(format!("Makefile target absent: {target}"));
        }
    }
    for pin in &config.pins {
        match repository.read(&pin.path) {
            Ok(content) => match actual_pin(content, pin)? {
                Some(actual) if actual == pin.expected => (),
                Some(actual) => diagnostics.push(format!(
                    "pin {} mismatch in {}: expected {}, actual {}",
                    pin.name, pin.path, pin.expected, actual
                )),
                None => diagnostics.push(format!(
                    "pin {} key {} absent from {}",
                    pin.name, pin.key, pin.path
                )),
            },
            Err(_) => diagnostics.push(format!(
                "pin {} file absent from snapshot: {}",
                pin.name, pin.path
            )),
        }
    }
    diagnostics.sort();
    Ok(serde_json::to_value(GatesResult {
        ok: diagnostics.is_empty(),
        snapshot: &repository.snapshot,
        source: &repository.source,
        targets: &config.targets,
        pins: &config.pins,
        diagnostics,
    })?)
}

fn actual_pin(content: &str, pin: &Pin) -> Result<Option<Value>> {
    if pin.kind == "toml" {
        let parsed: toml::Value = toml::from_str(content)?;
        let mut current = &parsed;
        for component in pin.key.split('.') {
            if component.is_empty() {
                bail!("empty TOML key component in {}", pin.name);
            }
            let Some(next) = current.get(component) else {
                return Ok(None);
            };
            current = next;
        }
        return Ok(Some(serde_json::to_value(current)?));
    }
    if pin.path != "Makefile" {
        bail!("make pin {} must read Makefile", pin.name);
    }
    let assignment = Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\s*(?::=|\?=|\+=|=)\s*(.*?)\s*$")?;
    let mut found = None;
    for line in content.lines() {
        if line.trim_start().starts_with('#') {
            continue;
        }
        if let Some(parts) = assignment.captures(line)
            && parts.get(1).is_some_and(|name| name.as_str() == pin.key)
            && found.replace(Value::String(parts[2].to_owned())).is_some()
        {
            bail!("ambiguous Makefile assignment for {}", pin.key);
        }
    }
    Ok(found)
}
