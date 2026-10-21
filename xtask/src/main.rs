use std::{
    fs,
    path::Path,
    process::{Command, ExitCode},
};

use clap::Parser;
use redsuite_core::{catalog::Fixture, frontend, logging, Result};
use tracing::error;

#[derive(Parser)]
#[command(
    name = "xtask",
    bin_name = "cargo xtask",
    version,
    about = "RedSuite development tools",
    after_long_help = frontend::env_help()
)]
enum Task {
    /// Build and stage the SBF programs.
    Programs,
    /// Format the workspace using the pinned Rust toolchain.
    Fmt {
        /// Check formatting without rewriting files.
        #[arg(long)]
        check: bool,
    },
    #[command(flatten)]
    Shared(frontend::Command),
}

fn main() -> ExitCode {
    let task = Task::parse();
    if let Err(error) = logging::init() {
        eprintln!("cannot initialize logging: {error}");
        return ExitCode::FAILURE;
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let outcome = match task {
        Task::Programs => programs(root),
        Task::Fmt { check } => {
            let mut command = Command::new("cargo");
            command.args(["fmt", "--all"]).current_dir(root);
            if check {
                command.arg("--check");
            }
            run(&mut command)
        }
        Task::Shared(command) => command.run(),
    };
    if let Err(error) = outcome {
        error!(%error, "command failed");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run(command: &mut Command) -> Result<()> {
    let status = command
        .status()
        .map_err(|error| format!("{command:?}: failed to spawn: {error}"))?;
    if !status.success() {
        return Err(format!("{command:?} failed ({status})").into());
    }
    Ok(())
}

fn programs(root: &Path) -> Result<()> {
    let deploy = root.join("target/deploy");
    for fixture in Fixture::ALL {
        let (family, variant) = match fixture {
            Fixture::RedlineProgram => ("redline", None),
            Fixture::RedshiftProgram => ("redshift", None),
            Fixture::RedhatProgram => ("redhat", None),
            Fixture::UpgradeProbe => ("upgrade", Some("v1")),
            Fixture::UpgradeProbeUpgraded => ("upgrade", Some("v2")),
        };
        let out = match variant {
            Some(label) => deploy.join(format!("{family}-{label}")),
            None => deploy.clone(),
        };
        let mut command = Command::new("cargo");
        command
            .args(["build-sbf", "--manifest-path"])
            .arg(root.join(format!("programs/{family}/program/Cargo.toml")))
            .arg("--sbf-out-dir")
            .arg(&out);
        if variant.is_some() {
            command.args(["--arch", "v3"]);
            if fixture == Fixture::UpgradeProbeUpgraded {
                command.args(["--features", "upgraded"]);
            }
        }
        command.args(["--", "--config", "profile.release.lto=\"fat\""]);
        run(&mut command)?;
        if variant.is_some() {
            let staged = deploy.join(fixture.so_name());
            // fs::copy can fail with ENOENT on virtiofs bind mounts.
            fs::read(out.join("upgrade_probe.so"))
                .and_then(|bytes| fs::write(&staged, bytes))
                .map_err(|error| format!("staging {}: {error}", staged.display()))?;
        }
    }
    Ok(())
}
