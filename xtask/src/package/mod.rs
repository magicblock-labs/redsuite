use crate::{root, run, runtime};
use anyhow::{ensure, Context, Result};
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
};
#[derive(Subcommand)]
pub enum Task {
    Metadata,
    Runtime {
        out: PathBuf,
    },
    Fixtures {
        source: PathBuf,
        out: PathBuf,
    },
    Bundle {
        runtime: PathBuf,
        fixtures: PathBuf,
        binary: PathBuf,
        out: PathBuf,
    },
    Verify {
        bundle: PathBuf,
        #[arg(long)]
        live: bool,
    },
}

#[derive(Default, Deserialize, Serialize)]
pub struct Manifest {
    pub kind: String,
    pub revision: String,
    pub platform: String,
    pub inputs: BTreeMap<String, String>,
}
pub fn output(c: &mut Command) -> Result<String> {
    let o = c.output().with_context(|| format!("starting {c:?}"))?;
    ensure!(
        o.status.success(),
        "{c:?}: {}",
        String::from_utf8_lossy(&o.stderr)
    );
    Ok(String::from_utf8(o.stdout)?.trim().into())
}
pub fn revision(source: &Path) -> Result<String> {
    output(Command::new("git").arg("-C").arg(source).args(["rev-parse", "HEAD"]))
}
pub fn sha(path: &Path) -> Result<String> {
    let mut f = fs::File::open(path).with_context(|| format!("reading {}", path.display()))?;
    let mut h = Sha256::new();
    let mut b = [0; 65536];
    loop {
        let n = f.read(&mut b)?;
        if n == 0 {
            break;
        }
        h.update(&b[..n]);
    }
    Ok(format!("{:x}", h.finalize()))
}
pub fn copy(from: &Path, to: &Path) -> Result<()> {
    fs::create_dir_all(to.parent().context("destination parent")?)?;
    fs::copy(from, to)
        .with_context(|| format!("copying {} to {}", from.display(), to.display()))?;
    Ok(())
}
pub fn download(url: &str, path: &Path) -> Result<()> {
    if path.is_file() {
        return Ok(());
    }
    fs::create_dir_all(path.parent().context("download parent")?)?;
    let tmp = path.with_extension("partial");
    run(Command::new("curl")
        .args(["--fail", "--location", "--retry", "3", "--output"])
        .arg(&tmp)
        .arg(url))?;
    fs::rename(tmp, path)?;
    Ok(())
}
fn checksum_path(file: &Path) -> PathBuf {
    let mut path = file.as_os_str().to_os_string();
    path.push(".sha256");
    path.into()
}
pub fn extract_verified(file: &Path, to: &Path) -> Result<()> {
    let sidecar = checksum_path(file);
    let expected =
        fs::read_to_string(&sidecar).with_context(|| format!("reading {}", sidecar.display()))?;
    ensure!(
        expected.split_whitespace().next() == Some(sha(file)?.as_str()),
        "archive checksum mismatch: {}",
        file.display()
    );
    extract(file, to)
}
pub fn extract(file: &Path, to: &Path) -> Result<()> {
    run(Command::new("tar").arg("-xf").arg(file).arg("-C").arg(to))
}
pub fn seal(dir: &Path, m: &Manifest) -> Result<()> {
    fs::write(dir.join("manifest.json"), serde_json::to_vec_pretty(m)?)?;
    Ok(())
}
pub fn archive(dir: &Path, out: &Path) -> Result<()> {
    fs::create_dir_all(out)?;
    let dest = out.join(format!(
        "{}.tar.gz",
        dir.file_name().context("archive name")?.to_string_lossy()
    ));
    let tmp = dest.with_extension("partial");
    run(Command::new("tar")
        .arg("-czf")
        .arg(&tmp)
        .arg("-C")
        .arg(dir.parent().context("archive parent")?)
        .arg(dir.file_name().unwrap()))?;
    fs::rename(tmp, &dest)?;
    fs::write(
        checksum_path(&dest),
        format!(
            "{}  {}\n",
            sha(&dest)?,
            dest.file_name().unwrap().to_string_lossy()
        ),
    )?;
    println!("{}", dest.display());
    Ok(())
}
pub fn dispatch(task: Task) -> Result<()> {
    let config = runtime::Config::load()?;
    match task {
        Task::Metadata => {
            println!(
                "platform={}\nruntime_id={}\nfixture_id={}",
                runtime::platform()?.0,
                config.runtime_id(),
                config.fixture_id()
            );
        }
        Task::Runtime { out } => runtime::assemble(&config, &out)?,
        Task::Fixtures { source, out } => assemble_fixtures(config, source, out)?,
        Task::Bundle { runtime, fixtures, binary, out } => {
            assemble_bundle(config, runtime, fixtures, binary, out)?
        }
        Task::Verify { bundle, live } => verify(&fs::canonicalize(bundle)?, live)?,
    }
    Ok(())
}
fn verify(bundle: &Path, live: bool) -> Result<()> {
    let work = tempfile::Builder::new().prefix("rs qualification ").tempdir_in("/tmp")?;
    let bin = bundle.join("bin/redsuite");
    for args in [&["--help"][..], &["redshift", "list"], &["stack", "status"]] {
        run(Command::new(&bin)
            .args(args)
            .current_dir(work.path())
            .env_remove("REDSUITE_ROOT")
            .env_remove("REDSUITE_FIXTURES_DIR")
            .env_remove("REDSUITE_YELLOWSTONE_PLUGIN"))?;
    }
    let mut deps = Command::new(if cfg!(target_os = "macos") { "otool" } else { "ldd" });
    if cfg!(target_os = "macos") {
        deps.arg("-L");
    }
    run(deps
        .arg(bundle.join("bin/redsuite-bin"))
        .arg(bundle.join("runtime/agave/bin/solana-test-validator"))
        .arg(bundle.join("runtime/yellowstone").join(runtime::platform()?.1)))?;
    if live {
        let result = run(Command::new(&bin)
            .args(["redshift", "run", "cache_transport_recovery", "--cleanup"])
            .env("REDSUITE_ROOT", work.path()));
        let cleanup =
            run(Command::new(&bin).args(["stack", "down"]).env("REDSUITE_ROOT", work.path()));
        result?;
        cleanup?;
    }
    Ok(())
}

fn assemble_fixtures(config: runtime::Config, source: PathBuf, out: PathBuf) -> Result<()> {
    let source = fs::canonicalize(source)?;
    ensure!(
        output(Command::new("git").arg("-C").arg(&source).args([
            "status",
            "--porcelain",
            "--untracked-files=all"
        ]))?
        .is_empty(),
        "fixture source must be clean"
    );
    let rev = revision(&source)?;
    let tmp = tempfile::tempdir()?;
    let snapshot = tmp.path().join("source.tar.gz");
    run(Command::new("git")
        .arg("-C")
        .arg(&source)
        .args(["archive", "--format=tar.gz"])
        .arg(format!("--output={}", snapshot.display()))
        .arg(&rev))?;
    let build = tmp.path().join("build");
    fs::create_dir(&build)?;
    extract(&snapshot, &build)?;
    let runtime = runtime::acquire_runtime(&config)?;
    let built = crate::fixtures::build(&build, &build.join("target/deploy"), Some(&runtime))?;
    let dir = tmp.path().join(config.fixture_name());
    fs::create_dir(&dir)?;
    let mut m = Manifest {
        kind: "fixtures".into(),
        revision: rev,
        ..Default::default()
    };
    for file in built {
        copy(&file, &dir.join(file.file_name().unwrap()))?;
    }
    m.inputs.insert(
        "sbf".into(),
        output(Command::new(runtime.join("agave/bin/cargo-build-sbf")).arg("--version"))?,
    );
    copy(&snapshot, &dir.join("source.tar.gz"))?;
    copy(&root().join("LICENSE"), &dir.join("LICENSE"))?;
    seal(&dir, &m)?;
    archive(&dir, &out)?;
    Ok(())
}

fn assemble_bundle(
    config: runtime::Config,
    rt: PathBuf,
    fixtures: PathBuf,
    binary: PathBuf,
    out: PathBuf,
) -> Result<()> {
    let tmp = tempfile::tempdir()?;
    extract_verified(&rt, tmp.path())?;
    extract_verified(&fixtures, tmp.path())?;
    let rev = revision(root())?;
    let r = tmp.path().join(config.runtime_name()?);
    let f = tmp.path().join(config.fixture_name());
    let manifest: Manifest = serde_json::from_slice(&fs::read(f.join("manifest.json"))?)?;
    ensure!(
        manifest.revision == rev,
        "fixtures must match the runner's source commit"
    );
    let dir = tmp.path().join(format!(
        "redsuite-{}-{}",
        env!("CARGO_PKG_VERSION"),
        runtime::platform()?.0
    ));
    fs::create_dir(&dir)?;
    fs::rename(r, dir.join("runtime"))?;
    fs::rename(f, dir.join("fixtures"))?;
    copy(&binary, &dir.join("bin/redsuite-bin"))?;
    for (a, b) in [("launcher", "redsuite"), ("runtime-env", "runtime-env")] {
        copy(
            &root().join("xtask/assets").join(a),
            &dir.join("bin").join(b),
        )?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir.join("bin/redsuite"), fs::Permissions::from_mode(0o755))?;
    }
    copy(&root().join("LICENSE"), &dir.join("LICENSE"))?;
    let m = Manifest {
        kind: "bundle".into(),
        revision: rev,
        platform: runtime::platform()?.0.into(),
        ..Default::default()
    };
    seal(&dir, &m)?;
    archive(&dir, &out)?;
    Ok(())
}
