use crate::{
    package::{self, Manifest},
    root, run,
};
use anyhow::{ensure, Context, Result};
use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::Command,
};
// Bump when the archive layout or build recipe changes.
const RECIPE: &str = "1";

pub struct Config {
    pins: crate::components::Runtime,
}
impl Config {
    pub fn load() -> Result<Self> {
        Ok(Self {
            pins: toml::from_str(&fs::read_to_string(root().join("shared/components.toml"))?)?,
        })
    }
    pub fn runtime_id(&self) -> String {
        format!(
            "{}-{}-{RECIPE}",
            self.pins.agave.revision, self.pins.yellowstone.revision
        )
    }
    pub fn fixture_id(&self) -> String {
        format!("{}-{RECIPE}", self.pins.agave.revision)
    }
    pub fn runtime_name(&self) -> Result<String> {
        Ok(format!(
            "redsuite-runtime-{}-{}",
            platform()?.0,
            self.runtime_id()
        ))
    }
    pub fn fixture_name(&self) -> String {
        format!(
            "redsuite-fixtures-{}-{}",
            env!("CARGO_PKG_VERSION"),
            self.fixture_id()
        )
    }
}
pub fn platform() -> Result<(&'static str, &'static str)> {
    match (env::consts::OS, env::consts::ARCH) {
        ("macos", "aarch64") => Ok(("aarch64-apple-darwin", "libyellowstone_grpc_geyser.dylib")),
        ("linux", "x86_64") => Ok(("x86_64-unknown-linux-gnu", "libyellowstone_grpc_geyser.so")),
        _ => anyhow::bail!("supported hosts: macOS Apple Silicon and Linux x86-64"),
    }
}
fn cache() -> PathBuf {
    env::var_os("REDSUITE_BUILD_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root().join("target/package-cache"))
}
pub fn assemble(config: &Config, out: &Path) -> Result<()> {
    let (target, lib) = platform()?;
    let cache = cache();
    fs::create_dir_all(&cache)?;
    let temp = tempfile::tempdir()?;
    let dir = temp.path().join(config.runtime_name()?);
    fs::create_dir(&dir)?;
    let url = format!(
        "https://github.com/anza-xyz/agave/releases/download/v{}/solana-release-{target}.tar.bz2",
        config.pins.agave.version
    );
    let archive = cache.join(format!(
        "agave-{}-{target}.tar.bz2",
        config.pins.agave.revision
    ));
    package::download(&url, &archive)?;
    package::extract(&archive, &dir)?;
    fs::rename(dir.join("solana-release"), dir.join("agave"))?;
    let rev = &config.pins.yellowstone.revision;
    let srcurl = format!("https://github.com/rpcpool/yellowstone-grpc/archive/{rev}.tar.gz");
    let src = cache.join(format!("{rev}.tar.gz"));
    package::download(&srcurl, &src)?;
    package::extract(&src, temp.path())?;
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
            package::revision(&build_source)? == *rev,
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
        package::download(
            &format!(
                "https://github.com/rpcpool/yellowstone-grpc/releases/download/{}/{lib}",
                config.pins.yellowstone_tag()
            ),
            &file,
        )?;
        file
    };
    package::copy(&plugin, &dir.join("yellowstone").join(lib))?;
    package::copy(&src, &dir.join("sources/yellowstone.tar.gz"))?;
    package::copy(
        &source.join("LICENSE"),
        &dir.join("licenses/Yellowstone-LICENSE"),
    )?;
    package::download(
        &format!(
            "https://raw.githubusercontent.com/anza-xyz/agave/{}/LICENSE",
            config.pins.agave.revision
        ),
        &dir.join("licenses/Agave-LICENSE"),
    )?;
    package::copy(
        &root().join("LICENSE"),
        &dir.join("licenses/RedSuite-LICENSE"),
    )?;
    let mut m = Manifest {
        kind: "runtime".into(),
        platform: target.into(),
        inputs: BTreeMap::from([("components".into(), toml::to_string(&config.pins)?)]),
        ..Default::default()
    };
    m.inputs.insert("agave_url".into(), url);
    m.inputs.insert("yellowstone_source".into(), srcurl);
    package::seal(&dir, &m)?;
    package::archive(&dir, out)
}
fn dependency_cache() -> PathBuf {
    if let Some(p) = env::var_os("REDSUITE_CACHE_DIR") {
        return p.into();
    }
    let base = env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env::var_os("HOME").unwrap_or_default()).join(".cache"));
    base.join("redsuite")
}
fn acquire(name: &str, kind: &str) -> Result<PathBuf> {
    let base = dependency_cache();
    fs::create_dir_all(&base)?;
    let dest = base.join(kind).join(name);
    if dest.is_dir() {
        return Ok(fs::canonicalize(dest)?);
    }
    fs::create_dir_all(dest.parent().unwrap())?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(base.join("install.lock"))?;
    lock.lock()?;
    if dest.is_dir() {
        return Ok(fs::canonicalize(dest)?);
    }
    let temp = tempfile::tempdir_in(&base)?;
    let archive = temp.path().join(format!("{name}.tar.gz"));
    for suffix in [".tar.gz.sha256", ".tar.gz"] {
        let file = temp.path().join(format!("{name}{suffix}"));
        if let Some(local) = env::var_os("REDSUITE_ASSETS_DIR") {
            package::copy(&PathBuf::from(local).join(format!("{name}{suffix}")), &file)
                .with_context(|| {
                    format!("missing local asset {name}{suffix}; assemble the pinned recipe first")
                })?;
        } else {
            let release = env!("CARGO_PKG_VERSION");
            let url = format!("https://github.com/magicblock-labs/redsuite/releases/download/{release}/{name}{suffix}");
            package::download(&url, &file)
                .context("pinned release asset unavailable; no fallback release is selected")?;
        }
    }
    package::extract_verified(&archive, temp.path())?;
    let staged = temp.path().join(name);
    fs::rename(staged, &dest)?;
    Ok(fs::canonicalize(dest)?)
}
pub fn acquire_runtime(config: &Config) -> Result<PathBuf> {
    acquire(&config.runtime_name()?, "runtime")
}
pub fn exec(command: Vec<OsString>, runtime_only: bool) -> Result<()> {
    let config = Config::load()?;
    let runtime = acquire_runtime(&config)?;
    let fixtures = if runtime_only {
        None
    } else {
        Some(match env::var_os("REDSUITE_FIXTURES_DIR") {
            Some(path) => PathBuf::from(path),
            None => acquire(&config.fixture_name(), "fixtures")?,
        })
    };
    tracing::info!(runtime = %runtime.display(), fixtures = ?fixtures, "using RedSuite dependencies");
    let mut cmd = Command::new("bash");
    cmd.arg(root().join("xtask/assets/runtime-env"))
        .arg(runtime)
        .arg(fixtures.as_deref().unwrap_or_else(|| Path::new("")))
        .arg(platform()?.1)
        .args(command);
    use std::os::unix::process::CommandExt;
    Err(cmd.exec().into())
}
