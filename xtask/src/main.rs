use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{exit, Command},
};

use sha2::{Digest, Sha256};
use toml_edit::{value, DocumentMut};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const FAMILY_PROGRAMS: &[&str] = &["redline", "redshift", "redhat"];

const USAGE: &str = "\
usage:
  cargo xtask programs                                         build the family SBF programs into target/deploy/
  cargo xtask refresh-base-programs <name>                     build a base program from source at the rev pinned in MANIFEST.toml
  cargo xtask refresh-base-programs <name> --from-chain <url>  dump the deployed program from a cluster
  cargo xtask refresh-base-programs all                        rebuild every base program from source
  cargo xtask check-base-programs                              verify base programs match the manifest sha256s
  cargo xtask stack status                                     show the shared base+ER stack (booted on demand by tests)
  cargo xtask stack down                                       stop the shared stack and clear its state
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
        Some("check-base-programs") => check_base_programs(),
        Some("refresh-base-programs") => match (arg(1), arg(2), arg(3)) {
            (Some(name), Some("--from-chain"), Some(url)) => {
                dump_from_chain(name, url)
            }
            (Some("all"), None, None) => {
                for name in base_program_names()? {
                    build_from_source(&name)?;
                }
                Ok(())
            }
            (Some(name), None, None) => build_from_source(name),
            _ => usage(),
        },
        Some("stack") => match arg(1) {
            Some("status") => stack_status(),
            Some("down") => stack_down(),
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

fn manifest_path() -> PathBuf {
    root().join("base-programs/MANIFEST.toml")
}

fn load_manifest() -> Result<DocumentMut> {
    Ok(fs::read_to_string(manifest_path())?.parse()?)
}

fn base_program_names() -> Result<Vec<String>> {
    Ok(load_manifest()?
        .iter()
        .filter(|(_, item)| item.is_table())
        .map(|(name, _)| name.to_string())
        .collect())
}

fn field(doc: &DocumentMut, name: &str, key: &str) -> Result<String> {
    doc.get(name)
        .and_then(|item| item.as_table())
        .and_then(|table| table.get(key))
        .and_then(|item| item.as_str())
        .map(str::to_owned)
        .ok_or_else(|| {
            format!(
                "missing `{key}` under [{name}] in base-programs/MANIFEST.toml"
            )
            .into()
        })
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
    Ok(())
}

fn build_from_source(name: &str) -> Result<()> {
    let doc = load_manifest()?;
    let repo = field(&doc, name, "repo")?;
    let rev = field(&doc, name, "rev")?;
    let artifact = field(&doc, name, "artifact")?;

    let src = root().join("target/base-programs-src").join(name);
    let _ = fs::remove_dir_all(&src);
    fs::create_dir_all(src.parent().unwrap())?;

    println!("==> {name}: cloning {repo} @ {rev}");
    run_cmd(
        "git clone",
        Command::new("git")
            .args(["clone", "--quiet", &repo])
            .arg(&src),
    )?;
    run_cmd(
        "git checkout",
        Command::new("git")
            .arg("-C")
            .arg(&src)
            .args(["checkout", "--quiet", &rev]),
    )?;

    println!("==> {name}: cargo build-sbf");
    run_cmd(
        "cargo build-sbf",
        Command::new("cargo").arg("build-sbf").current_dir(&src),
    )?;

    let so = src.join("target/deploy").join(&artifact);
    if !so.exists() {
        return Err(format!(
            "expected artifact `{artifact}` not found in {}",
            so.parent().unwrap().display()
        )
        .into());
    }

    let toolchain = Command::new("cargo")
        .args(["build-sbf", "--version"])
        .output()
        .map(|out| {
            String::from_utf8_lossy(&out.stdout)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default();
    install(
        name,
        &so,
        &format!("built from {repo} @ {rev} ({toolchain})"),
    )
}

fn dump_from_chain(name: &str, url: &str) -> Result<()> {
    let doc = load_manifest()?;
    let id = field(&doc, name, "program-id")?;

    let out = root()
        .join("target/base-programs-src")
        .join(format!("{name}-dump.so"));
    fs::create_dir_all(out.parent().unwrap())?;

    println!("==> {name}: dumping {id} from {url}");
    run_cmd(
        "solana program dump",
        Command::new("solana")
            .args(["program", "dump", &id])
            .arg(&out)
            .args(["--url", url]),
    )?;
    install(name, &out, &format!("dumped from chain {url}"))
}

fn install(name: &str, so: &Path, provenance: &str) -> Result<()> {
    let sha = sha256(so)?;
    fs::copy(so, root().join("base-programs").join(format!("{name}.so")))?;

    let mut doc = load_manifest()?;
    doc[name]["sha256"] = value(sha.as_str());
    doc[name]["obtained"] = value(provenance);
    doc[name]["date"] = value(today_utc());
    fs::write(manifest_path(), doc.to_string())?;

    println!("==> {name}: installed base-programs/{name}.so (sha256 {sha})");
    Ok(())
}

fn check_base_programs() -> Result<()> {
    let doc = load_manifest()?;
    let mut failed = false;
    for name in base_program_names()? {
        let want = field(&doc, &name, "sha256")?;
        let got =
            sha256(&root().join("base-programs").join(format!("{name}.so")))?;
        if want == got {
            println!("ok:   {name}.so");
        } else {
            eprintln!("FAIL: {name}.so (manifest {want}, actual {got})");
            failed = true;
        }
    }
    if failed {
        Err("base-programs check failed".into())
    } else {
        Ok(())
    }
}

fn sha256(path: &Path) -> Result<String> {
    let bytes =
        fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

/// Subset of redsuite-core's `StackState` (topology/stack.rs); serde
/// ignores the rest.
#[derive(json::Deserialize)]
struct StackState {
    base_rpc_port: u16,
    base_pid: u32,
    base_bin: String,
    er_rpc_port: u16,
    er_metrics_port: u16,
    er_pid: u32,
    er_bin: String,
    er_identity: String,
}

fn stack_state_path() -> PathBuf {
    root().join("target/redsuite-stack/state.json")
}

fn read_stack_state() -> Result<Option<StackState>> {
    let path = stack_state_path();
    if !path.exists() {
        return Ok(None);
    }
    let state = json::from_str(&fs::read_to_string(&path)?)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(Some(state))
}

/// PID alive and still the recorded binary (guards against PID reuse).
fn proc_matches(pid: u32, bin: &str) -> bool {
    fs::read(format!("/proc/{pid}/cmdline"))
        .map(|cmdline| String::from_utf8_lossy(&cmdline).contains(bin))
        .unwrap_or(false)
}

fn rpc_listening(port: u16) -> bool {
    use std::net::{SocketAddr, TcpStream};
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(500))
        .is_ok()
}

fn stack_status() -> Result<()> {
    let Some(state) = read_stack_state()? else {
        println!(
            "no shared stack ({} absent) — the first scenario test boots it",
            stack_state_path().display()
        );
        return Ok(());
    };
    let describe = |name: &str, pid: u32, bin: &str, port: u16| {
        let proc_state = if proc_matches(pid, bin) {
            "running"
        } else {
            "DEAD"
        };
        let rpc_state = if rpc_listening(port) {
            "rpc up"
        } else {
            "rpc DOWN"
        };
        println!("{name:5} pid {pid:<8} {proc_state:8} 127.0.0.1:{port:<6} {rpc_state}   ({bin})");
    };
    describe("base", state.base_pid, &state.base_bin, state.base_rpc_port);
    describe("er", state.er_pid, &state.er_bin, state.er_rpc_port);
    println!("er identity   {}", state.er_identity);
    println!(
        "er metrics    http://127.0.0.1:{}/metrics",
        state.er_metrics_port
    );
    println!(
        "logs          {}",
        root().join("target/redsuite-stack").display()
    );
    Ok(())
}

fn stack_down() -> Result<()> {
    let Some(state) = read_stack_state()? else {
        println!("no shared stack to stop");
        return Ok(());
    };
    let procs = [
        ("er", state.er_pid, state.er_bin.as_str()),
        ("base", state.base_pid, state.base_bin.as_str()),
    ];
    for (name, pid, bin) in procs {
        if proc_matches(pid, bin) {
            println!("stopping {name} (pid {pid})");
            let _ = Command::new("kill").arg(pid.to_string()).status();
        }
    }
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline
        && procs.iter().any(|(_, pid, bin)| proc_matches(*pid, bin))
    {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    for (name, pid, bin) in procs {
        if proc_matches(pid, bin) {
            println!("killing {name} (pid {pid}) — did not exit in time");
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status();
        }
    }
    fs::remove_file(stack_state_path())?;
    println!("stack down");
    Ok(())
}

// Howard Hinnant's civil_from_days; avoids pulling in a date crate.
fn today_utc() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let z = (secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_update_preserves_comments_and_other_sections() {
        let src = "# top comment\n[dlp]\nsha256 = \"aaa\"\n\n[mdp]\nsha256 = \"bbb\"\n";
        let mut doc: DocumentMut = src.parse().unwrap();
        doc["dlp"]["sha256"] = value("changed");
        let out = doc.to_string();
        assert!(out.contains("# top comment"));
        assert!(out.contains("sha256 = \"changed\""));
        assert!(out.contains("sha256 = \"bbb\""));
    }

    #[test]
    fn today_utc_is_plausible() {
        let date = today_utc();
        assert_eq!(date.len(), 10);
        assert!(date.starts_with("20"));
    }
}
