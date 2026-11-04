use crate::{package, root};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::{
    env,
    ffi::OsString,
    fs, iter,
    path::{Path, PathBuf},
    process::Command,
};
#[derive(Deserialize)]
pub struct Config {
    #[serde(flatten)]
    pub(super) pins: crate::components::Runtime,
    runtime: Distribution,
    fixtures: Distribution,
}

#[derive(Deserialize)]
struct Distribution {
    release: String,
    version: String,
}
impl Config {
    pub fn load() -> Result<Self> {
        Ok(toml::from_str(&fs::read_to_string(
            root().join("shared/components.toml"),
        )?)?)
    }
    pub fn fixture_tag(&self) -> String {
        format!("fixtures-{}", self.fixtures.version)
    }
    pub fn runtime_name(&self) -> Result<String> {
        Ok(format!("redsuite-{}-{}", self.runtime_tag(), platform()?.0))
    }
    pub fn runtime_tag(&self) -> String {
        format!("runtime-{}", self.runtime.version)
    }
    pub fn fixture_name(&self) -> String {
        format!("redsuite-fixtures-{}", self.fixtures.version)
    }
}
pub fn platform() -> Result<(&'static str, &'static str)> {
    match (env::consts::OS, env::consts::ARCH) {
        ("macos", "aarch64") => Ok(("aarch64-apple-darwin", "libyellowstone_grpc_geyser.dylib")),
        ("linux", "x86_64") => Ok(("x86_64-unknown-linux-gnu", "libyellowstone_grpc_geyser.so")),
        _ => anyhow::bail!("supported hosts: macOS Apple Silicon and Linux x86-64"),
    }
}
pub fn runner_name() -> Result<String> {
    Ok(format!(
        "redsuite-runner-{}-{}",
        env!("CARGO_PKG_VERSION"),
        platform()?.0
    ))
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
fn acquire(name: &str, kind: &str, release: &str) -> Result<PathBuf> {
    let base = dependency_cache();
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
    let file = format!("{name}.tar.gz");
    let archive = temp.path().join(&file);
    if let Some(local) = env::var_os("REDSUITE_ASSETS_DIR") {
        package::copy(&PathBuf::from(local).join(&file), &archive)
            .with_context(|| format!("missing local asset {file}; assemble it first"))?;
    } else {
        let url = format!(
            "https://github.com/magicblock-labs/redsuite/releases/download/{release}/{file}"
        );
        package::download(&url, &archive)
            .context("pinned release asset unavailable; no fallback release is selected")?;
    }
    package::extract(&archive, temp.path())?;
    let staged = temp.path().join(name);
    fs::rename(staged, &dest)?;
    Ok(fs::canonicalize(dest)?)
}
pub fn acquire_runtime(config: &Config) -> Result<PathBuf> {
    acquire(&config.runtime_name()?, "runtime", &config.runtime.release)
}
pub fn runner(args: Vec<OsString>) -> Result<()> {
    let runner = acquire(&runner_name()?, "runner", env!("CARGO_PKG_VERSION"))?;
    let command = iter::once(runner.join("bin/redsuite").into_os_string()).chain(args);
    exec(command, false)
}

pub fn exec(command: impl IntoIterator<Item = OsString>, runtime_only: bool) -> Result<()> {
    let config = Config::load()?;
    let runtime = acquire_runtime(&config)?;
    let fixtures = if runtime_only {
        None
    } else {
        Some(match env::var_os("REDSUITE_FIXTURES_DIR") {
            Some(path) => PathBuf::from(path),
            None => acquire(&config.fixture_name(), "fixtures", &config.fixtures.release)?,
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
