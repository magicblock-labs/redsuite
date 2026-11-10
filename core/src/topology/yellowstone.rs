use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::ensure;
use tracing::debug;

use super::state;
use crate::Result;

pub const PLUGIN_ENV: &str = "REDSUITE_YELLOWSTONE_PLUGIN";
pub const CACHE_ENV: &str = "REDSUITE_YELLOWSTONE_DIR";

const LIB: &str = "libyellowstone_grpc_geyser.so";
const DOWNLOADS: &str = "https://github.com/rpcpool/yellowstone-grpc/releases/download";

#[path = "../../../shared/components.rs"]
mod components;

pub struct Plugin {
    pub tag: String,
    pub libpath: PathBuf,
}

impl Plugin {
    pub fn config_json(&self, address: SocketAddr) -> String {
        json::to_string(&json::json!({
            "libpath": self.libpath.display().to_string(),
            "log": { "level": "warn" },
            "grpc": { "listen": [{ "address": address }] },
        }))
        .unwrap_or_default()
    }

    pub fn write_config(&self, path: &Path, address: SocketAddr) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, self.config_json(address))?;
        Ok(())
    }
}

pub async fn ensure() -> Result<Plugin> {
    if let Some(path) = env::var_os(PLUGIN_ENV) {
        let libpath = PathBuf::from(path);
        ensure!(
            libpath.is_file(),
            "{PLUGIN_ENV} points at {}, which is not a file",
            libpath.display()
        );
        return Ok(Plugin {
            tag: format!("{} (from {PLUGIN_ENV})", libpath.display()),
            libpath,
        });
    }
    let components: components::Runtime =
        toml::from_str(include_str!("../../../shared/components.toml"))?;
    let tag = components.yellowstone_tag();

    ensure!(
        cfg!(all(target_os = "linux", target_arch = "x86_64")),
        "upstream publishes the Yellowstone geyser plugin only for \
             x86_64 linux; build one for this host and point {PLUGIN_ENV} at it"
    );

    Ok(Plugin {
        libpath: cached(&tag).await?,
        tag,
    })
}

fn cache_dir(tag: &str) -> PathBuf {
    match env::var_os(CACHE_ENV) {
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
    debug!(tag, ?dir, "downloading Yellowstone geyser plugin");
    let bytes = reqwest::get(format!("{DOWNLOADS}/{tag}/{LIB}"))
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let staged = dir.join(format!("{LIB}.partial"));
    fs::write(&staged, &bytes)?;
    fs::rename(&staged, &libpath)?;
    Ok(libpath)
}
