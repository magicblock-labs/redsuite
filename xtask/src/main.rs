use std::{
    path::{Path, PathBuf},
    process::{exit, Command},
};

use clap::{Parser, Subcommand};
use redsuite_core::{catalog::Fixture, frontend, logging};
use tracing::error;

type Result<T> = redsuite_core::Result<T>;

const FAMILY_PROGRAMS: &[&str] = &["redline", "redshift", "redhat"];

#[derive(Parser)]
#[command(
    name = "xtask",
    bin_name = "cargo xtask",
    version,
    about = "RedSuite development tools",
    after_long_help = frontend::env_help()
)]
struct Cli {
    #[command(subcommand)]
    command: Task,
}

#[derive(Subcommand)]
enum Task {
    /// Build and stage the SBF programs.
    Programs,
    /// Format the workspace using nightly rustfmt.
    Fmt {
        /// Check formatting without rewriting files.
        #[arg(long)]
        check: bool,
    },
    #[command(flatten)]
    Shared(frontend::Command),
}

fn main() {
    let cli = Cli::parse();
    if let Err(error) = logging::init() {
        eprintln!("cannot initialize logging: {error}");
        exit(1);
    }
    let outcome = match cli.command {
        Task::Programs => programs(),
        Task::Fmt { check } => fmt(check),
        Task::Shared(command) => command.run(),
    };
    if let Err(error) = outcome {
        error!(%error, "command failed");
        exit(1);
    }
}

fn fmt(check: bool) -> Result<()> {
    let mut cmd = Command::new("cargo");
    cmd.args(["+nightly", "fmt"]);
    if check {
        cmd.arg("--check");
    }
    cmd.args(["--", "--config-path"])
        .arg(root().join("rustfmt-nightly.toml"))
        .current_dir(root());
    run_cmd("cargo +nightly fmt", &mut cmd)
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn run_cmd(desc: &str, cmd: &mut Command) -> Result<()> {
    let status = cmd
        .status()
        .map_err(|e| format!("{desc}: failed to spawn: {e}"))?;
    if !status.success() {
        return Err(format!("{desc} failed ({status})").into());
    }
    Ok(())
}

const SBF_LTO: &str = "profile.release.lto=\"fat\"";

fn programs() -> Result<()> {
    for program in FAMILY_PROGRAMS {
        run_cmd(
            &format!("cargo build-sbf ({program})"),
            Command::new("cargo")
                .args(["build-sbf", "--manifest-path"])
                .arg(
                    root()
                        .join(format!("programs/{program}/program/Cargo.toml")),
                )
                .arg("--sbf-out-dir")
                .arg(root().join("target/deploy"))
                .args(["--", "--config", SBF_LTO]),
        )?;
    }
    build_redshift_variant(
        "slim",
        &[],
        Fixture::RedshiftProgramSlim.so_name(),
    )?;
    build_redshift_variant(
        "slim-upgraded",
        &["upgraded"],
        Fixture::RedshiftProgramSlimUpgraded.so_name(),
    )?;
    Ok(())
}

fn build_redshift_variant(
    label: &str,
    features: &[&str],
    staged_name: &str,
) -> Result<()> {
    let out_dir = root().join(format!("target/deploy/redshift-{label}"));
    let mut command = Command::new("cargo");
    command
        .args(["build-sbf", "--arch", "v3", "--manifest-path"])
        .arg(root().join("programs/redshift/program/Cargo.toml"))
        .arg("--no-default-features");
    if !features.is_empty() {
        command.args(["--features", &features.join(",")]);
    }
    command.arg("--sbf-out-dir").arg(&out_dir);
    command.args(["--", "--config", SBF_LTO]);
    run_cmd(&format!("cargo build-sbf (redshift {label})"), &mut command)?;
    let built = out_dir.join("redshift_program.so");
    let staged = root().join("target/deploy").join(staged_name);
    // Not fs::copy: it fstats the freshly created destination, which fails
    // with ENOENT on virtiofs bind mounts (Docker Desktop devcontainers).
    std::fs::read(&built)
        .and_then(|bytes| std::fs::write(&staged, bytes))
        .map_err(|err| format!("staging {staged_name}: {err}"))?;
    Ok(())
}
