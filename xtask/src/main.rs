use std::{
    env,
    path::{Path, PathBuf},
    process::{exit, Command},
};

use redsuite_core::{catalog::Fixture, frontend, manifest};

type Result<T> = redsuite_core::Result<T>;

const FAMILY_PROGRAMS: &[&str] = &["redline", "redshift", "redhat"];

const USAGE_HEAD: &str = "\
usage:
  cargo xtask programs                                       build the family SBF programs into target/deploy/
  cargo xtask fmt [--check]                                  format the workspace (nightly rustfmt, rustfmt-nightly.toml)
";

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        exit(1);
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let arg = |index: usize| args.get(index).map(String::as_str);
    match arg(0) {
        Some("programs") => programs(),
        Some("fmt") => match arg(1) {
            None => fmt(false),
            Some("--check") => fmt(true),
            _ => usage(),
        },
        _ => match frontend::dispatch(&args) {
            Some(outcome) => outcome,
            None => usage(),
        },
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

fn usage() -> Result<()> {
    eprint!("{USAGE_HEAD}{}", frontend::usage("cargo xtask"));
    exit(2);
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
    let manifest_path = manifest::emit()?;
    eprintln!("staged fixture manifest: {}", manifest_path.display());
    Ok(())
}

// The slim variants do not include the schedulecommit module or its sdk
// dependencies. The loader_matrix v4 cell deploys these small binaries,
// because a large redeploy wedges the clone of the program in the ER.
fn build_redshift_variant(
    label: &str,
    features: &[&str],
    staged_name: &str,
) -> Result<()> {
    let out_dir = root().join(format!("target/deploy/redshift-{label}"));
    let mut command = Command::new("cargo");
    command
        .args(["build-sbf", "--manifest-path"])
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
    std::fs::copy(&built, &staged)
        .map_err(|err| format!("staging {staged_name}: {err}"))?;
    Ok(())
}
