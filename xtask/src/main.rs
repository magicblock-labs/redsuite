mod report;

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{exit, Command},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const FAMILY_PROGRAMS: &[&str] = &["redline", "redshift", "redhat"];

const USAGE: &str = "\
usage:
  cargo xtask programs                                         build the family SBF programs into target/deploy/
  cargo xtask stack status                                     show the shared base+ER stack (booted on demand by tests)
  cargo xtask stack down                                       stop the shared stack and clear its state
  cargo xtask report list                                      list persisted scenario reports (target/redsuite-reports/)
  cargo xtask report compare [scenario] [--strict]             diff the latest two runs per scenario; --strict fails on regressions
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
            Some("status") => stack_status(),
            Some("down") => stack_down(),
            _ => usage(),
        },
        Some("report") => match arg(1) {
            Some("list") => report::list(),
            Some("compare") => {
                let rest = &args[2..];
                let strict = rest.iter().any(|a| a == "--strict");
                let filter =
                    rest.iter().find(|a| !a.starts_with("--")).cloned();
                report::compare(filter.as_deref(), strict)
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
    Ok(())
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
