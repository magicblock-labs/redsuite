use std::process::Command;

#[test]
fn require_running_base_refuses_to_start_a_stopped_base() {
    let root = tempfile::tempdir().unwrap();
    let stack = root.path().join("stack");
    let output = Command::new(env!("CARGO_BIN_EXE_redsuite"))
        .args(["redshift", "run", "--require-running-base", "settlement_lifecycle"])
        .env("REDSUITE_ROOT", root.path())
        .env("REDSUITE_STACK_DIR", &stack)
        .env("RUST_LOG", "error")
        .env("PATH", root.path())
        .output()
        .unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(
        stderr.contains(&format!(
            "no healthy base at {} (this run requires a running base)",
            stack.display()
        )),
        "{stderr}"
    );
    assert!(!stack.join("base.log").exists());
    assert!(!stack.join("state.json").exists());
}
