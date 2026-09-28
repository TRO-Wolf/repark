use crate::repository::{InputScope, Repository, validate_relative};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    path: String,
    #[serde(default)]
    section: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    version: u32,
    global: Vec<Source>,
    roles: BTreeMap<String, Vec<Source>>,
    units: BTreeMap<String, Vec<Source>>,
}

#[derive(Serialize)]
struct SelectedSource<'a> {
    path: String,
    section: Option<String>,
    text: &'a str,
}

#[derive(Serialize)]
struct ContextResult<'a> {
    ok: bool,
    snapshot: &'a str,
    source: &'a str,
    input_scope: &'a InputScope,
    role: &'a str,
    unit: &'a str,
    bytes: usize,
    sources: Vec<SelectedSource<'a>>,
    diagnostics: Vec<String>,
}

struct Options<'a> {
    path: &'a str,
    role: &'a str,
    unit: &'a str,
    max_bytes: Option<usize>,
}

fn parse_options(arguments: &[String]) -> Result<Options<'_>> {
    let mut config_path = None;
    let mut role = None;
    let mut unit = None;
    let mut max_bytes = None;
    let mut iter = arguments.iter();
    while let Some(argument) = iter.next() {
        match argument.as_str() {
            "--config" => {
                config_path = Some(iter.next().context("--config needs a path")?.as_str());
            }
            "--role" => role = Some(iter.next().context("--role needs a value")?.as_str()),
            "--unit" => unit = Some(iter.next().context("--unit needs an ID")?.as_str()),
            "--max-bytes" => {
                max_bytes = Some(
                    iter.next()
                        .context("--max-bytes needs a number")?
                        .parse::<usize>()?,
                );
            }
            other => bail!("unknown context argument: {other}"),
        }
    }
    let path = config_path.context("context requires --config PATH")?;
    validate_relative(path)?;
    let role = role.context("context requires --role")?;
    if !["executor", "critic", "orchestrator"].contains(&role) {
        bail!("invalid role: {role}");
    }
    let unit = unit.context("context requires --unit ID")?;
    if unit.is_empty() || unit.contains('/') || unit.contains('\\') {
        bail!("invalid unit ID");
    }
    Ok(Options {
        path,
        role,
        unit,
        max_bytes,
    })
}

fn approved_sources(contents: &str, options: &Options<'_>) -> Result<Vec<Source>> {
    let config: Config = serde_json::from_str(contents)?;
    if config.version != 1 {
        bail!("unsupported context config version");
    }
    let role_sources = config
        .roles
        .get(options.role)
        .context("role missing from context config")?;
    let unit_sources = config
        .units
        .get(options.unit)
        .context("unit missing from context config")?;
    if config.global.is_empty() || role_sources.is_empty() || unit_sources.is_empty() {
        bail!("context config requires global, role, and unit sources");
    }
    let mut sources = Vec::new();
    sources.push(Source {
        path: "AGENTS.md".to_owned(),
        section: None,
    });
    sources.extend(config.global);
    sources.extend(role_sources.iter().map(|s| Source {
        path: s.path.clone(),
        section: s.section.clone(),
    }));
    sources.extend(unit_sources.iter().map(|s| Source {
        path: s.path.clone(),
        section: s.section.clone(),
    }));
    Ok(sources)
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Scoped input contracts live in CONTEXT.md"
)]
pub fn load_repository(root: &Path, source: &str, arguments: &[String]) -> Result<Repository> {
    let options = parse_options(arguments)?;
    let mut paths = BTreeSet::from([options.path.to_owned()]);
    let config = Repository::load_scoped(root, source, InputScope::Files(paths.clone()))?;
    let contents = config.read(options.path)?;
    paths.extend(
        approved_sources(contents, &options)?
            .into_iter()
            .map(|item| item.path),
    );
    let repository = Repository::load_scoped(root, source, InputScope::Files(paths))?;
    if repository.read(options.path)? != contents {
        bail!("context config changed while selecting inputs");
    }
    Ok(repository)
}

#[expect(
    clippy::missing_errors_doc,
    reason = "CLI errors are documented in CONTEXT.md"
)]
pub fn run(repository: &Repository, arguments: &[String]) -> Result<Value> {
    let options = parse_options(arguments)?;
    let sources = approved_sources(repository.read(options.path)?, &options)?;
    let mut seen = BTreeSet::new();
    let mut selected = Vec::new();
    let mut diagnostics = Vec::new();
    let mut total_bytes = 0usize;
    for source in sources {
        validate_relative(&source.path)?;
        if source.path == "AGENTS.md" && source.section.is_some() {
            bail!("AGENTS.md must be included in full");
        }
        let key = (source.path.clone(), source.section.clone());
        if !seen.insert(key) {
            continue;
        }
        match repository.read(&source.path) {
            Ok(contents) => match select_section(contents, source.section.as_deref()) {
                Ok(text) => {
                    total_bytes += text.len();
                    selected.push(SelectedSource {
                        path: source.path,
                        section: source.section,
                        text,
                    });
                }
                Err(message) => diagnostics.push(format!("{}: {message}", source.path)),
            },
            Err(_) => diagnostics.push(format!("{}: source missing from snapshot", source.path)),
        }
    }
    if let Some(limit) = options.max_bytes
        && total_bytes > limit
    {
        diagnostics.push(format!(
            "context exceeds byte budget: {total_bytes} > {limit}"
        ));
    }
    diagnostics.sort();
    Ok(serde_json::to_value(ContextResult {
        ok: diagnostics.is_empty(),
        snapshot: &repository.snapshot,
        source: &repository.source,
        input_scope: &repository.input_scope,
        role: options.role,
        unit: options.unit,
        bytes: total_bytes,
        sources: selected,
        diagnostics,
    })?)
}

fn select_section<'a>(
    contents: &'a str,
    section: Option<&str>,
) -> std::result::Result<&'a str, String> {
    let Some(section) = section else {
        return Ok(contents);
    };
    if section.is_empty() {
        return Err("empty heading".to_owned());
    }
    let lines: Vec<&str> = contents.split_inclusive('\n').collect();
    let mut offset = 0usize;
    let mut found = None;
    let mut count = 0usize;
    let mut fence = None;
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if advance_fence(trimmed, &mut fence) {
            offset += line.len();
            continue;
        }
        let depth = trimmed.bytes().take_while(|byte| *byte == b'#').count();
        if fence.is_none()
            && (1..=6).contains(&depth)
            && trimmed.as_bytes().get(depth) == Some(&b' ')
            && trimmed[depth + 1..].trim() == section
        {
            found = Some((index, offset, depth));
            count += 1;
        }
        offset += line.len();
    }
    if count != 1 {
        return Err(format!("heading {section:?} has {count} matches"));
    }
    let (index, start, depth) = found.ok_or_else(|| "heading absent".to_owned())?;
    let mut end = contents.len();
    let mut offset = start + lines[index].len();
    let mut fence = None;
    for line in lines.iter().skip(index + 1) {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if advance_fence(trimmed, &mut fence) {
            offset += line.len();
            continue;
        }
        let next_depth = trimmed.bytes().take_while(|byte| *byte == b'#').count();
        if fence.is_none()
            && (1..=depth).contains(&next_depth)
            && trimmed.as_bytes().get(next_depth) == Some(&b' ')
        {
            end = offset;
            break;
        }
        offset += line.len();
    }
    Ok(&contents[start..end])
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
