use anyhow::{Context, Result, bail};
use repark_repo::{
    cache, context, evidence, gates, maps,
    repository::{InputScope, Repository},
    state, trace, validation, workflow,
};
use serde::Serialize;
use serde_json::Value;
use std::io::Write;
use std::path::PathBuf;

#[derive(Serialize)]
struct Help {
    usage: &'static str,
    commands: &'static [&'static str],
}

#[derive(Serialize)]
struct Index<'a> {
    ok: bool,
    snapshot: &'a str,
    source: &'a str,
    files: &'a std::collections::BTreeSet<String>,
    directories: std::collections::BTreeSet<String>,
}

fn main() {
    match execute().and_then(|value| write_result(&value)) {
        Ok(true) => (),
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(2);
        }
    }
}

fn write_result(value: &Value) -> Result<bool> {
    let mut output = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut output, value)?;
    writeln!(output)?;
    Ok(value.get("ok").and_then(Value::as_bool) != Some(false))
}

fn execute() -> Result<Value> {
    let mut arguments = std::env::args().skip(1);
    let mut root = PathBuf::from(".");
    let mut snapshot = "worktree".to_owned();
    let mut cache_directory = None;
    let mut command = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--repo" => root = PathBuf::from(arguments.next().context("--repo needs a path")?),
            "--snapshot" => snapshot = arguments.next().context("--snapshot needs a value")?,
            "--cache-dir" => {
                cache_directory = Some(arguments.next().context("--cache-dir needs a path")?);
            }
            "--help" | "-h" => {
                return Ok(serde_json::to_value(Help {
                    usage: "repark-repo [--repo PATH] [--snapshot worktree|index|REF] [--cache-dir PATH] COMMAND [ARGS]",
                    commands: &[
                        "index", "maps", "context", "trace", "state", "gates", "evidence",
                        "workflow", "checks",
                    ],
                })?);
            }
            value if value.starts_with('-') => bail!("unknown global option: {value}"),
            value => {
                command = Some(value.to_owned());
                break;
            }
        }
    }
    let command = command.as_deref().unwrap_or("index");
    let arguments: Vec<String> = arguments.collect();
    if command == "checks" {
        if snapshot != "worktree" || cache_directory.is_some() {
            bail!("checks requires worktree inputs and cannot use a persistent cache");
        }
        return validation::run(&root, &arguments);
    }
    let repository = match command {
        "maps" => Repository::load_scoped(&root, &snapshot, InputScope::Maps)?,
        "context" => context::load_repository(&root, &snapshot, &arguments)?,
        _ => Repository::load(&root, &snapshot)?,
    };
    if let Some(directory) = cache_directory {
        let all_arguments: Vec<_> = std::iter::once(command.to_owned())
            .chain(arguments.iter().cloned())
            .collect();
        cache::execute(&repository, &directory, &all_arguments, || {
            dispatch(&repository, command, &arguments)
        })
    } else {
        dispatch(&repository, command, &arguments)
    }
}

fn dispatch(repository: &Repository, command: &str, arguments: &[String]) -> Result<Value> {
    match command {
        "index" if arguments.is_empty() => Ok(serde_json::to_value(Index {
            ok: true,
            snapshot: &repository.snapshot,
            source: &repository.source,
            files: &repository.paths,
            directories: repository.directories(),
        })?),
        "index" => bail!("index accepts no arguments"),
        "maps" => maps::run(repository, arguments),
        "context" => context::run(repository, arguments),
        "trace" => trace::run(repository, arguments),
        "state" => state::run(repository, arguments),
        "gates" => gates::run(repository, arguments),
        "evidence" => evidence::run(repository, arguments),
        "workflow" => workflow::run(repository, arguments),
        other => bail!("unknown command: {other}"),
    }
}
