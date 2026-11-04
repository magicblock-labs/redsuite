use crate::{root, run, runtime};
use anyhow::{ensure, Context, Result};
use clap::Subcommand;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    env, fs,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
};
#[derive(Subcommand)]
pub enum Task {
    Metadata,
    Runtime { out: PathBuf },
    Fixtures { source: PathBuf, out: PathBuf },
    Runner { binary: PathBuf, out: PathBuf },
}

#[derive(Default, Serialize)]
struct Manifest {
    kind: String,
    revision: String,
    platform: String,
    inputs: BTreeMap<String, String>,
}
fn output(c: &mut Command) -> Result<String> {
    let o = c.output().with_context(|| format!("starting {c:?}"))?;
    ensure!(
        o.status.success(),
        "{c:?}: {}",
        String::from_utf8_lossy(&o.stderr)
    );
    Ok(String::from_utf8(o.stdout)?.trim().into())
}
fn revision(source: &Path) -> Result<String> {
    output(Command::new("git").arg("-C").arg(source).args(["rev-parse", "HEAD"]))
}
fn sha(path: &Path) -> Result<String> {
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
pub fn extract(file: &Path, to: &Path) -> Result<()> {
    run(Command::new("tar").arg("-xf").arg(file).arg("-C").arg(to))
}
fn archive(dir: &Path, out: &Path, manifest: &Manifest) -> Result<()> {
    fs::write(
        dir.join("manifest.json"),
        serde_json::to_vec_pretty(manifest)?,
    )?;
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
        dest.with_extension("gz.sha256"),
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
                "platform={}\nruntime_tag={}\nfixture_tag={}",
                runtime::platform()?.0,
                config.runtime_tag(),
                config.fixture_tag()
            );
        }
        Task::Runtime { out } => assemble_runtime(&config, &out)?,
        Task::Fixtures { source, out } => assemble_fixtures(&config, &source, &out)?,
        Task::Runner { binary, out } => assemble_runner(&binary, &out)?,
    }
    Ok(())
}
fn assemble_runtime(config: &runtime::Config, out: &Path) -> Result<()> {
    let (target, lib) = runtime::platform()?;
    let cache = env::var_os("REDSUITE_BUILD_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root().join("target/package-cache"));
    fs::create_dir_all(&cache)?;
    let temp = tempfile::tempdir()?;
    let dir = temp.path().join(config.runtime_name()?);
    fs::create_dir(&dir)?;
    let url = format!(
        "https://github.com/anza-xyz/agave/releases/download/v{}/solana-release-{target}.tar.bz2",
        config.pins.agave.version
    );
    let agave = cache.join(format!(
        "agave-{}-{target}.tar.bz2",
        config.pins.agave.revision
    ));
    download(&url, &agave)?;
    extract(&agave, &dir)?;
    fs::rename(dir.join("solana-release"), dir.join("agave"))?;
    let rev = &config.pins.yellowstone.revision;
    let srcurl = format!("https://github.com/rpcpool/yellowstone-grpc/archive/{rev}.tar.gz");
    let src = cache.join(format!("{rev}.tar.gz"));
    download(&srcurl, &src)?;
    extract(&src, temp.path())?;
    let source = temp.path().join(format!("yellowstone-grpc-{rev}"));
    let plugin = if cfg!(target_os = "macos") {
        // Upstream's build script uses git describe; an archive can otherwise
        // inherit this checkout's Git identity or fail outside a repository.
        let build_source = temp.path().join("yellowstone-build");
        run(Command::new("git")
            .args(["clone", "--depth", "1", "--branch"])
            .arg(config.pins.yellowstone_tag())
            .arg("https://github.com/rpcpool/yellowstone-grpc.git")
            .arg(&build_source))?;
        ensure!(
            revision(&build_source)? == *rev,
            "Yellowstone release tag no longer matches the pinned revision"
        );
        let build = cache.join(format!("yellowstone-{rev}"));
        run(Command::new("cargo")
            .args([
                "build",
                "--release",
                "--locked",
                "--target",
                target,
                "-p",
                "yellowstone-grpc-geyser",
            ])
            .current_dir(&build_source)
            .env_remove("RUSTUP_TOOLCHAIN")
            .env("RUSTFLAGS", "")
            .env("CARGO_TARGET_DIR", &build))?;
        build.join(target).join("release").join(lib)
    } else {
        let file = cache.join(format!("yellowstone-{rev}.so"));
        download(
            &format!(
                "https://github.com/rpcpool/yellowstone-grpc/releases/download/{}/{lib}",
                config.pins.yellowstone_tag()
            ),
            &file,
        )?;
        file
    };
    copy(&plugin, &dir.join("yellowstone").join(lib))?;
    copy(&src, &dir.join("sources/yellowstone.tar.gz"))?;
    copy(
        &source.join("LICENSE"),
        &dir.join("licenses/Yellowstone-LICENSE"),
    )?;
    download(
        &format!(
            "https://raw.githubusercontent.com/anza-xyz/agave/{}/LICENSE",
            config.pins.agave.revision
        ),
        &dir.join("licenses/Agave-LICENSE"),
    )?;
    copy(
        &root().join("LICENSE"),
        &dir.join("licenses/RedSuite-LICENSE"),
    )?;
    let manifest = Manifest {
        kind: "runtime".into(),
        platform: target.into(),
        inputs: BTreeMap::from([
            ("components".into(), toml::to_string(&config.pins)?),
            ("agave_url".into(), url),
            ("yellowstone_source".into(), srcurl),
        ]),
        ..Default::default()
    };
    archive(&dir, out, &manifest)
}
fn assemble_fixtures(config: &runtime::Config, source: &Path, out: &Path) -> Result<()> {
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
    let snapshot = tmp.path().join("source.tar");
    run(Command::new("git")
        .arg("-C")
        .arg(&source)
        .args(["archive", "--format=tar"])
        .arg(format!("--output={}", snapshot.display()))
        .arg(&rev))?;
    let build = tmp.path().join("build");
    fs::create_dir(&build)?;
    extract(&snapshot, &build)?;
    let runtime = runtime::acquire_runtime(config)?;
    let built = crate::fixtures::build(&build, &build.join("target/deploy"), Some(&runtime))?;
    let dir = tmp.path().join(config.fixture_name());
    fs::create_dir(&dir)?;
    for file in built {
        copy(&file, &dir.join(file.file_name().unwrap()))?;
    }
    let manifest = Manifest {
        kind: "fixtures".into(),
        revision: rev,
        inputs: BTreeMap::from([(
            "sbf".into(),
            output(Command::new(runtime.join("agave/bin/cargo-build-sbf")).arg("--version"))?,
        )]),
        ..Default::default()
    };
    copy(&root().join("LICENSE"), &dir.join("LICENSE"))?;
    archive(&dir, out, &manifest)
}

fn assemble_runner(binary: &Path, out: &Path) -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path().join(runtime::runner_name()?);
    copy(binary, &dir.join("bin/redsuite"))?;
    copy(
        &root().join("shared/components.toml"),
        &dir.join("components.toml"),
    )?;
    copy(&root().join("LICENSE"), &dir.join("LICENSE"))?;
    archive(
        &dir,
        out,
        &Manifest {
            kind: "runner".into(),
            revision: revision(root())?,
            platform: runtime::platform()?.0.into(),
            ..Default::default()
        },
    )
}
