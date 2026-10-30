//! Family-specific arguments own their environment bindings, so unrelated
//! commands neither read nor validate benchmark-only settings.

use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use clap::{Args, Parser, Subcommand};
use redsuite_core::{
    config::{LoopMode, Profile},
    frontend,
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
    #[command(about = "Run mixed scenarios on a shared ER until a failure or Ctrl+C.")]
    Continuous(ContinuousRun),
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
    #[arg(long, value_name = "U64")]
    pub seed: Option<u64>,
    /// Maximum concurrent fresh scenarios.
    #[arg(long, default_value = "2", value_name = "N")]
    pub fresh_concurrency: NonZeroUsize,
    /// Maximum concurrent shared scenarios in each compatible ER group.
    #[arg(long, default_value = "4", value_name = "N")]
    pub shared_concurrency: NonZeroUsize,
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
    /// Measured load duration per workload, excluding setup and drain.
    #[arg(long, value_parser = duration)]
    pub duration: Duration,
    /// Existing base-chain HTTP RPC endpoint (normally devnet).
    #[arg(long, requires_all = ["base_ws", "payer"])]
    pub base_rpc: Option<String>,
    #[arg(long, requires = "base_rpc")]
    pub base_ws: Option<String>,
    /// Existing ER; requires its matching base chain and observation endpoints.
    #[arg(long, requires_all = ["base_rpc", "er_ws", "metrics"])]
    pub er_rpc: Option<String>,
    #[arg(long, requires = "er_rpc")]
    pub er_ws: Option<String>,
    #[arg(long, requires = "er_rpc")]
    pub metrics: Option<String>,
    /// Funded setup keypair; fixture programs must already be deployed.
    #[arg(long, requires = "base_rpc")]
    pub payer: Option<std::path::PathBuf>,
    /// Transaction observation loop mode.
    #[arg(long = "loop", env = "REDSUITE_LOOP", value_enum, default_value = "open")]
    pub loop_mode: LoopMode,
}

#[derive(Args)]
pub struct ContinuousRun {
    #[arg(long, value_name = "U64")]
    pub seed: Option<u64>,
    #[arg(long, help = "Measured load duration per Redline workload.", value_parser = duration)]
    pub duration: Duration,
    #[arg(
        long,
        help = "Maximum concurrent shared scenarios.",
        default_value = "4",
        value_name = "N"
    )]
    pub shared_concurrency: NonZeroUsize,
    #[arg(
        long,
        help = "Redline workload preset.",
        env = "REDSUITE_PROFILE",
        value_enum,
        default_value = "lite"
    )]
    pub profile: Profile,
}

fn duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
        return Err("duration must be positive and fit a monotonic deadline".into());
    }
    Ok(duration)
}
