use std::{
    env,
    path::{Path, PathBuf},
    process::{exit, Command},
};

use redsuite_core::{report, topology};

type Result<T> = redsuite_core::Result<T>;

const FAMILY_PROGRAMS: &[&str] = &["redline", "redshift", "redhat"];

const USAGE: &str = "\
usage:
  cargo xtask programs                                         build the family SBF programs into target/deploy/
  cargo xtask stack status                                     show the shared base+ER stack (booted on demand by tests)
  cargo xtask stack down                                       stop the shared stack and clear its state
  cargo xtask report list                                      list persisted scenario reports (target/redsuite-reports/)
  cargo xtask report compare [scenario] [--strict] [--brief]   diff the latest two runs per scenario; --strict fails on regressions, --brief shows changed metrics only
  cargo xtask report bmf [--out <path>]                        export the latest reports as Bencher Metric Format JSON
  cargo xtask fmt [--check]                                    format the workspace (nightly rustfmt, rustfmt-nightly.toml)
";

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        exit(1);
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let arg = |i: usize| args.get(i).map(String::as_str);
    match arg(0) {
        Some("programs") => programs(),
        Some("stack") => match arg(1) {
            Some("status") => topology::status(),
            Some("down") => topology::down(),
            _ => usage(),
        },
        Some("report") => match arg(1) {
            Some("list") => report::list(),
            Some("compare") => {
                let rest = &args[2..];
                let strict = rest.iter().any(|a| a == "--strict");
                let brief = rest.iter().any(|a| a == "--brief");
                let filter =
                    rest.iter().find(|a| !a.starts_with("--")).cloned();
                report::compare(filter.as_deref(), strict, brief)
            }
            Some("bmf") => match (arg(2), arg(3)) {
                (Some("--out"), Some(path)) => report::bmf(Some(path)),
                (None, _) => report::bmf(None),
                _ => usage(),
            },
            _ => usage(),
        },
        Some("fmt") => match arg(1) {
            None => fmt(false),
            Some("--check") => fmt(true),
            _ => usage(),
        },
        _ => usage(),
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
    eprint!("{USAGE}");
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

fn programs() -> Result<()> {
    for program in FAMILY_PROGRAMS {
        run_cmd(
            &format!("cargo build-sbf ({program})"),
            Command::new("cargo")
                .args(["build-sbf", "--manifest-path"])
                .arg(root().join(format!("programs/{program}/Cargo.toml"))),
        )?;
    }
    build_redshift_variant("slim", &[], "redshift_program_slim.so")?;
    build_redshift_variant(
        "slim-upgraded",
        &["upgraded"],
        "redshift_program_slim_upgraded.so",
    )?;
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
        .arg(root().join("programs/redshift/Cargo.toml"))
        .arg("--no-default-features");
    if !features.is_empty() {
        command.args(["--features", &features.join(",")]);
    }
    command.arg("--sbf-out-dir").arg(&out_dir);
    run_cmd(&format!("cargo build-sbf (redshift {label})"), &mut command)?;
    let built = out_dir.join("redshift_program.so");
    let staged = root().join("target/deploy").join(staged_name);
    std::fs::copy(&built, &staged)
        .map_err(|err| format!("staging {staged_name}: {err}"))?;
    Ok(())
}
