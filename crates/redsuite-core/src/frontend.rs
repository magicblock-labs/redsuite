use clap::Subcommand;

use crate::{report, topology, Result};

pub const ENV_VARS: &[(&str, &str)] = &[
    (
        topology::ER_BIN_ENV,
        "the ER binary under test (else `magicblock-validator` on PATH)",
    ),
    (
        topology::VERIFIER_BIN_ENV,
        "the verifier binary for replicated topologies (else beside the ER \
         binary, else on PATH)",
    ),
    (
        topology::YELLOWSTONE_PLUGIN_ENV,
        "a prebuilt libyellowstone_grpc_geyser.so to load instead of the \
         cached upstream release",
    ),
    (
        topology::YELLOWSTONE_CACHE_ENV,
        "where cached Yellowstone releases live (default target/yellowstone \
         under the workspace root)",
    ),
    (
        topology::ROOT_ENV,
        "workspace root, when the binary runs outside the checkout",
    ),
    (
        topology::STACK_DIR_ENV,
        "where the stack lives: ledgers, state, logs (default \
         target/redsuite-stack under the workspace root)",
    ),
    (
        topology::ACCOUNTSDB_ROOT_ENV,
        "root for every validator's accountsdb, when it should live on \
         another disk than the ledger (default: inside each storage dir)",
    ),
    (
        topology::CLONE_URL_ENV,
        "where a cold boot clones base programs from (default mainnet-beta)",
    ),
    (
        "RUST_LOG",
        "log filter for RedSuite and launched validators (default info; debug includes phase and workload progress)",
    ),
];

pub fn env_help() -> String {
    let mut text = String::from("Environment:\n");
    for (name, description) in ENV_VARS {
        text.push_str(&format!("  {name:<26} {description}\n"));
    }
    text
}

#[derive(Subcommand)]
pub enum Command {
    /// Inspect the shared stack and leftover rollup processes.
    #[command(subcommand)]
    Stack(Stack),
    /// Export benchmark reports.
    #[command(subcommand)]
    Report(Report),
}

#[derive(Subcommand)]
pub enum Stack {
    /// Show processes, endpoints, and log locations.
    Status,
    /// Stop the stack and remove its storage, retaining logs.
    Down,
}

#[derive(Subcommand)]
pub enum Report {
    /// Export the latest campaign in Bencher Metric Format.
    Bmf {
        /// Write JSON to a file instead of stdout.
        #[arg(long, value_name = "PATH")]
        out: Option<String>,
    },
}

impl Command {
    pub fn run(self) -> Result<()> {
        match self {
            Command::Stack(Stack::Status) => topology::status(),
            Command::Stack(Stack::Down) => topology::down(),
            Command::Report(Report::Bmf { out }) => report::bmf(out.as_deref()),
        }
    }
}
