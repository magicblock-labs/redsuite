#[path = "../../shared/components.rs"]
mod components;
mod fixtures;
#[path = "../../shared/fixtures.rs"]
mod fixtures_catalog;
#[path = "../../shared/logging.rs"]
mod logging;
mod package;
mod runtime;
use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use std::{
    ffi::OsString,
    path::Path,
    process::{Command, ExitCode},
};

#[derive(Parser)]
#[command(about = "RedSuite build tools")]
struct Cli {
    #[command(subcommand)]
    task: Task,
}

#[derive(Subcommand)]
enum Task {
    /// Build and stage the SBF fixtures.
    Programs,
    /// Format the workspace with its pinned toolchain.
    Fmt {
        #[arg(long)]
        check: bool,
    },
    #[command(subcommand)]
    Package(package::Task),
    #[command(subcommand)]
    Runtime(Runtime),
}

#[derive(Subcommand)]
enum Runtime {
    /// Run a command with the pinned dependencies on PATH.
    Exec {
        /// Skip RedSuite fixtures when building MagicBlock programs or using SBF tools.
        #[arg(long)]
        runtime_only: bool,
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<OsString>,
    },
}
fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}
fn run(command: &mut Command) -> Result<()> {
    let status = command.status().with_context(|| format!("starting {command:?}"))?;
    ensure!(status.success(), "{command:?} failed: {status}");
    Ok(())
}
fn dispatch(task: Task) -> Result<()> {
    match task {
        Task::Programs => {
            fixtures::build(root(), &root().join("target/deploy"), None)?;
        }
        Task::Fmt { check } => {
            let mut command = Command::new("cargo");
            command.args(["fmt", "--all"]).current_dir(root());
            if check {
                command.arg("--check");
            }
            run(&mut command)?;
        }
        Task::Package(task) => package::dispatch(task)?,
        Task::Runtime(Runtime::Exec { runtime_only, command }) => {
            runtime::exec(command, runtime_only)?
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let result = logging::init().and_then(|()| dispatch(Cli::parse().task));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("{e:#}");
            ExitCode::FAILURE
        }
    }
}
