//! Family-specific arguments own their environment bindings, so unrelated
//! commands neither read nor validate benchmark-only settings.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::{Duration, Instant},
};

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
    /// Curated benchmarks and continuous load.
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
    /// Generate continuous mixed transaction load, then drain and verify it.
    Load(Load),
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
    /// Seed for reproducible case generation; generated randomly if omitted.
    #[arg(long, value_name = "U64")]
    pub seed: Option<u64>,
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

#[derive(Args)]
pub struct Load {
    /// Workload preset.
    #[arg(long, env = "REDSUITE_PROFILE", value_enum, default_value = "lite")]
    pub profile: Profile,
    /// Load-generation time, excluding setup and drain (e.g. 30s, 10m, 1h).
    #[arg(long, value_parser = duration)]
    pub duration: Duration,
    /// Total offered transactions/second; defaults to the preset rate.
    #[arg(long)]
    pub rate: Option<NonZeroU32>,
    /// Total outstanding requests across workers; defaults to the preset budget.
    #[arg(long)]
    pub concurrency: Option<NonZeroUsize>,
    /// Stop the stack and delete its storage after the run, retaining logs.
    #[arg(long)]
    pub cleanup: bool,
}

fn duration(value: &str) -> Result<Duration, String> {
    let duration =
        humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
        return Err(
            "duration must be positive and fit a monotonic deadline".into()
        );
    }
    Ok(duration)
}
