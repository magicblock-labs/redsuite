use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};

use super::state;
use crate::Result;

pub const PLUGIN_ENV: &str = "REDSUITE_YELLOWSTONE_PLUGIN";
pub const CACHE_ENV: &str = "REDSUITE_YELLOWSTONE_DIR";

const LIB: &str = "libyellowstone_grpc_geyser.so";
const DOWNLOADS: &str =
    "https://github.com/rpcpool/yellowstone-grpc/releases/download";

const RELEASES: &[(&str, &str)] = &[
    ("4.1", "v14.2.4+solana.4.1.0"),
    ("4.2", "v15.2.1+solana.4.2.2"),
    ("4.3", "v16.0.0+solana.4.3.0"),
];

pub struct Plugin {
    pub tag: String,
    pub libpath: PathBuf,
}

impl Plugin {
    pub fn config_json(&self, address: &str) -> String {
        json::to_string(&json::json!({
            "libpath": self.libpath.display().to_string(),
            "log": { "level": "warn" },
            "grpc": { "listen": [{ "address": address }] },
        }))
        .unwrap_or_default()
    }

    pub fn write_config(&self, path: &Path, address: &str) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, self.config_json(address))?;
        Ok(())
    }
}

pub async fn ensure(base_bin: &Path) -> Result<Plugin> {
    if let Some(path) = std::env::var_os(PLUGIN_ENV) {
        let libpath = PathBuf::from(path);
        if !libpath.is_file() {
            return Err(format!(
                "{PLUGIN_ENV} points at {}, which is not a file",
                libpath.display()
            )
            .into());
        }
        return Ok(Plugin {
            tag: format!("{} (from {PLUGIN_ENV})", libpath.display()),
            libpath,
        });
    }
    let tag = release_for(&base_version(base_bin)?)?;

    if !cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        return Err(format!(
            "upstream publishes the Yellowstone geyser plugin only for \
             x86_64 linux; build one for this host and point {PLUGIN_ENV} at it"
        )
        .into());
    }

    Ok(Plugin {
        libpath: cached(tag).await?,
        tag: tag.to_owned(),
    })
}

fn release_for(agave: &str) -> Result<&'static str> {
    let series = series(agave);
    RELEASES
        .iter()
        .find_map(|(mapped, tag)| (*mapped == series).then_some(tag))
        .copied()
        .ok_or_else(|| {
            let known: Vec<&str> =
                RELEASES.iter().map(|(mapped, _)| *mapped).collect();
            format!(
                "no Yellowstone geyser release is mapped to solana-test-validator \
                 {agave}; mapped series are {} — add one to \
                 crates/redsuite-core/src/topology/yellowstone.rs or point \
                 {PLUGIN_ENV} at a compatible plugin",
                known.join(", ")
            )
            .into()
        })
}

fn series(version: &str) -> String {
    let mut parts = version.split('.');
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => format!("{major}.{minor}"),
        _ => version.to_owned(),
    }
}

fn base_version(base_bin: &Path) -> Result<String> {
    let output =
        Command::new(base_bin)
            .arg("--version")
            .output()
            .map_err(|err| {
                format!("running {} --version: {err}", base_bin.display())
            })?;
    let text = String::from_utf8_lossy(&output.stdout);
    text.split_whitespace()
        .nth(1)
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            format!(
                "could not read a version out of `{} --version`: {}",
                base_bin.display(),
                text.trim()
            )
            .into()
        })
}

fn cache_dir(tag: &str) -> PathBuf {
    match std::env::var_os(CACHE_ENV) {
        Some(dir) => PathBuf::from(dir).join(tag),
        None => state::workspace_root().join("target/yellowstone").join(tag),
    }
}

async fn cached(tag: &str) -> Result<PathBuf> {
    let dir = cache_dir(tag);
    let libpath = dir.join(LIB);
    if libpath.is_file() {
        return Ok(libpath);
    }

    fs::create_dir_all(&dir)?;
    let expected = expected_digest(tag).await?;
    crate::console::line(format_args!(
        "downloading the Yellowstone geyser plugin {tag} to {}",
        dir.display()
    ));
    let bytes = fetch(&asset_url(tag, LIB)).await?;
    let actual = sha256(&bytes);
    if actual != expected {
        return Err(format!(
            "{LIB} from {tag} hashes to {actual}, but the release checksums \
             list {expected}"
        )
        .into());
    }

    let staged = dir.join(format!("{LIB}.partial"));
    fs::write(&staged, &bytes)?;
    fs::rename(&staged, &libpath)?;
    Ok(libpath)
}

async fn expected_digest(tag: &str) -> Result<String> {
    let version = tag.strip_prefix('v').unwrap_or(tag);
    let name = format!("yellowstone-grpc_{version}_checksums.txt");
    let text = String::from_utf8(fetch(&asset_url(tag, &name)).await?)
        .map_err(|err| format!("{name} is not utf-8: {err}"))?;
    text.lines()
        .find_map(|line| {
            let (digest, file) = line.split_once("  ")?;
            (file.trim() == LIB).then(|| digest.trim().to_owned())
        })
        .ok_or_else(|| format!("{name} has no entry for {LIB}").into())
}

fn asset_url(tag: &str, asset: &str) -> String {
    format!("{DOWNLOADS}/{}/{}", encode(tag), encode(asset))
}

fn encode(value: &str) -> String {
    value.replace('+', "%2B")
}

async fn fetch(url: &str) -> Result<Vec<u8>> {
    let response = reqwest::get(url)
        .await
        .map_err(|err| format!("fetching {url}: {err}"))?;
    if !response.status().is_success() {
        return Err(
            format!("fetching {url}: HTTP {}", response.status()).into()
        );
    }
    Ok(response
        .bytes()
        .await
        .map_err(|err| format!("reading {url}: {err}"))?
        .to_vec())
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
