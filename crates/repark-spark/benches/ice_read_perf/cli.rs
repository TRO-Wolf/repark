use std::path::PathBuf;
use std::process::ExitCode;

use repark_iceberg::catalog::{IcebergFileClass, IcebergIoStats};

use crate::BoxError;
use crate::bed::{self, SetupOptions};
use crate::r3::{R3_EXIT_CODE, StepSummary};
use crate::remote::{self, Phase, RemoteSetupOptions};
use crate::run::{self, CatalogChoice, Mode, RunOptions};

pub const EXIT_FAILURE: u8 = 1;

pub const EXIT_USAGE: u8 = 2;

pub const USAGE: &str = "usage:
  ice_read_perf setup --warehouse <dir> [--files 200] [--rows-per-file 50000]
  ice_read_perf setup --catalog glue|s3tables --prop k=v... --table <ns.table> --phase create|write
                      [--files 200] [--rows-per-file 50000]
  ice_read_perf run --mode cold|warm|concurrent|concurrent-cold [--repeat 1] [--out <file.json>]
                    [--query Q1..Q7] then either
                    --warehouse <dir> [--manifest <bed.json>]
                    or --catalog glue|s3tables --prop k=v... --table <ns.table>
                    [--files 200] [--rows-per-file 50000]";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Setup(SetupOptions),
    SetupRemote(RemoteSetupOptions),
    Run(RunOptions),
}

#[derive(Debug)]
pub enum Outcome {
    Done,
    SizeFlagged(Box<IcebergIoStats>),
}

pub fn main_with(args: &[String], summary: &StepSummary) -> ExitCode {
    let command = match parse(args) {
        Ok(command) => command,
        Err(message) => {
            eprintln!("error: {message}\n{USAGE}");
            return ExitCode::from(EXIT_USAGE);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: cannot start the tokio runtime: {error}");
            return ExitCode::from(EXIT_FAILURE);
        }
    };
    let result = runtime.block_on(async {
        match &command {
            Command::Setup(options) => bed::setup(options, summary).await,
            Command::SetupRemote(options) => remote::setup_remote(options, summary).await,
            Command::Run(options) => run::run(options, summary, None).await,
        }
    });
    match result {
        Ok(Outcome::Done) => ExitCode::SUCCESS,
        Ok(Outcome::SizeFlagged(io)) => {
            eprintln!(
                "R3 stopped the bench before any scan: data-file requests={} delete-file requests={}",
                io.by_class(IcebergFileClass::DataFile).requests,
                io.by_class(IcebergFileClass::DeleteFile).requests
            );
            ExitCode::from(R3_EXIT_CODE)
        }
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(EXIT_FAILURE)
        }
    }
}

pub fn parse(args: &[String]) -> Result<Command, String> {
    let args: Vec<&str> = args
        .iter()
        .map(String::as_str)
        .filter(|arg| *arg != "--bench")
        .collect();
    let Some((subcommand, rest)) = args.split_first() else {
        return Err("missing subcommand".to_string());
    };
    let pairs = flag_pairs(rest)?;
    match *subcommand {
        "setup" => parse_setup(&pairs),
        "run" => parse_run(&pairs).map(Command::Run),
        other => Err(format!("unknown subcommand `{other}`")),
    }
}

fn flag_pairs<'a>(rest: &[&'a str]) -> Result<Vec<(&'a str, &'a str)>, String> {
    let mut pairs = Vec::new();
    let mut index = 0;
    while index < rest.len() {
        let flag = rest[index];
        if !flag.starts_with("--") {
            return Err(format!("unexpected argument `{flag}`"));
        }
        let value = rest
            .get(index + 1)
            .ok_or_else(|| format!("`{flag}` needs a value"))?;
        pairs.push((flag, *value));
        index += 2;
    }
    Ok(pairs)
}

fn prop_pair(value: &str) -> Result<(String, String), String> {
    value
        .split_once('=')
        .map(|(key, prop_value)| (key.to_string(), prop_value.to_string()))
        .ok_or_else(|| format!("--prop needs k=v, got `{value}`"))
}

fn parse_setup(pairs: &[(&str, &str)]) -> Result<Command, String> {
    let mut warehouse = None;
    let mut files = bed::DEFAULT_FILES;
    let mut rows_per_file = bed::DEFAULT_ROWS_PER_FILE;
    let mut catalog = CatalogChoice::Local;
    let mut props = Vec::new();
    let mut table = None;
    let mut phase = None;
    for (flag, value) in pairs {
        match *flag {
            "--warehouse" => warehouse = Some(PathBuf::from(value)),
            "--files" => files = positive(flag, value)?,
            "--rows-per-file" => rows_per_file = positive(flag, value)?,
            "--catalog" => catalog = CatalogChoice::parse(value)?,
            "--prop" => props.push(prop_pair(value)?),
            "--table" => table = Some((*value).to_string()),
            "--phase" => phase = Some(Phase::parse(value)?),
            other => return Err(format!("unknown setup flag `{other}`")),
        }
    }
    if catalog == CatalogChoice::Local {
        if phase.is_some() || table.is_some() || !props.is_empty() {
            return Err(
                "setup --phase / --table / --prop are for --catalog glue|s3tables".to_string(),
            );
        }
        return Ok(Command::Setup(SetupOptions {
            warehouse: warehouse.ok_or("setup needs --warehouse")?,
            files,
            rows_per_file,
        }));
    }
    if warehouse.is_some() {
        return Err(format!(
            "setup --catalog {} writes into the catalog; --warehouse is for a local bed",
            catalog.name()
        ));
    }
    Ok(Command::SetupRemote(RemoteSetupOptions {
        catalog,
        props,
        table: table.ok_or_else(|| {
            format!(
                "setup --catalog {} needs --table <ns.table>",
                catalog.name()
            )
        })?,
        phase: phase.ok_or_else(|| {
            format!(
                "setup --catalog {} needs --phase create|write",
                catalog.name()
            )
        })?,
        files,
        rows_per_file,
    }))
}

fn parse_run(pairs: &[(&str, &str)]) -> Result<RunOptions, String> {
    let mut options = RunOptions {
        warehouse: None,
        ..RunOptions::local(Mode::Warm, std::path::Path::new(""))
    };
    let mut mode = None;
    for (flag, value) in pairs {
        match *flag {
            "--mode" => mode = Some(Mode::parse(value)?),
            "--warehouse" => options.warehouse = Some(PathBuf::from(value)),
            "--out" => options.out = Some(PathBuf::from(value)),
            "--catalog" => options.catalog = CatalogChoice::parse(value)?,
            "--prop" => options.props.push(prop_pair(value)?),
            "--table" => options.table = Some((*value).to_string()),
            "--manifest" => options.manifest = Some(PathBuf::from(value)),
            "--query" => options.query = Some((*value).to_string()),
            "--repeat" => options.repeat = positive(flag, value)?,
            "--files" => options.files = Some(positive(flag, value)?),
            "--rows-per-file" => options.rows_per_file = Some(positive(flag, value)?),
            other => return Err(format!("unknown run flag `{other}`")),
        }
    }
    options.mode = mode.ok_or("run needs --mode cold|warm|concurrent|concurrent-cold")?;
    if options.catalog == CatalogChoice::Local && options.warehouse.is_none() {
        return Err("run --catalog local needs --warehouse".to_string());
    }
    Ok(options)
}

fn positive<T: std::str::FromStr + PartialOrd + Default>(
    flag: &str,
    value: &str,
) -> Result<T, String> {
    match value.parse::<T>() {
        Ok(parsed) if parsed > T::default() => Ok(parsed),
        _ => Err(format!("`{flag}` needs a positive integer, got `{value}`")),
    }
}

pub fn boxed(message: impl Into<String>) -> BoxError {
    message.into().into()
}
