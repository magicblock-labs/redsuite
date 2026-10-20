//! Family-specific arguments own their environment bindings, so unrelated
//! commands neither read nor validate benchmark-only settings.

use std::num::NonZeroUsize;

use clap::{Args, Parser, Subcommand};
use redsuite_core::{
    frontend,
    profile::{LoopMode, Profile},
};

#[derive(Parser)]
#[command(
    name = "redsuite",
    version,
    about = "Validator correctness, security, and performance scenarios",
    after_long_help = frontend::env_help()
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Curated benchmarks.
    #[command(subcommand)]
    Redline(Benchmarks),
    /// Correctness checks.
    #[command(subcommand)]
    Redshift(Checks),
    /// Security checks.
    #[command(subcommand)]
    Redhat(Checks),
    #[command(flatten)]
    Shared(frontend::Command),
}

#[derive(Subcommand)]
pub enum Checks {
    /// List matching scenarios, or the whole family if omitted.
    List {
        /// Full/short scenario names or prefixes within this family.
        #[arg(value_name = "SELECTION")]
        selections: Vec<String>,
    },
    /// Run matching scenarios, or the whole family if omitted.
    Run(CheckRun),
}

#[derive(Subcommand)]
pub enum Benchmarks {
    /// List matching benchmarks, or all benchmarks if omitted.
    List {
        /// Full/short benchmark names or prefixes.
        #[arg(value_name = "SELECTION")]
        selections: Vec<String>,
    },
    /// Run curated benchmarks sequentially.
    Run(BenchmarkRun),
}

#[derive(Args)]
pub struct Run {
    /// Full/short scenario name or prefix within this family.
    #[arg(value_name = "SELECTION")]
    pub selections: Vec<String>,
    /// Stop the stack and delete its storage after the run, retaining logs.
    #[arg(long)]
    pub cleanup: bool,
}

#[derive(Args)]
pub struct CheckRun {
    #[command(flatten)]
    pub run: Run,
    /// Maximum concurrent fresh scenarios.
    #[arg(long, default_value = "2", value_name = "N")]
    pub fresh_concurrency: NonZeroUsize,
    /// Run one scenario at a time; does not change cleanup behavior.
    #[arg(long)]
    pub serial: bool,
}

#[derive(Args)]
pub struct BenchmarkRun {
    #[command(flatten)]
    pub run: Run,
    /// Workload preset.
    #[arg(long, env = "REDSUITE_PROFILE", value_enum, default_value = "lite")]
    pub profile: Profile,
    /// Loop mode for rpc_warm_ingress.
    #[arg(
        long = "loop",
        env = "REDSUITE_LOOP",
        value_enum,
        default_value = "open"
    )]
    pub loop_mode: LoopMode,
}
