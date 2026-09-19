mod bed;
mod cli;
mod r3;
mod report;
mod run;

use std::process::ExitCode;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    cli::main_with(&args, &r3::StepSummary::from_env())
}
