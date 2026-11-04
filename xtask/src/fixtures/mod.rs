use crate::{fixtures_catalog::Fixture, run};
use anyhow::{Context, Result};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};
pub fn build(root: &Path, deploy: &Path, runtime: Option<&Path>) -> Result<Vec<PathBuf>> {
    fs::create_dir_all(deploy)?;
    let path = runtime
        .map(|runtime| {
            env::join_paths(
                std::iter::once(runtime.join("agave/bin"))
                    .chain(env::split_paths(&env::var_os("PATH").unwrap_or_default())),
            )
        })
        .transpose()?;
    for fixture in Fixture::ALL {
        let (family, variant) = fixture.build();
        let out = match variant {
            Some(label) => deploy.join(format!("{family}-{label}")),
            None => deploy.to_path_buf(),
        };
        let mut command = Command::new("cargo");
        command
            .args(["build-sbf", "--manifest-path"])
            .arg(root.join(format!("programs/{family}/program/Cargo.toml")))
            .arg("--sbf-out-dir")
            .arg(&out);
        if variant.is_some() {
            command.args(["--arch", "v3"]);
            if fixture == Fixture::UpgradeProbeUpgraded {
                command.args(["--features", "upgraded"]);
            }
        }
        if let Some(path) = &path {
            command.env("PATH", path);
        }
        command.args(["--", "--locked", "--config", "profile.release.lto=\"fat\""]);
        run(&mut command)?;
        if variant.is_some() {
            let staged = deploy.join(fixture.so_name());
            // fs::copy can fail with ENOENT on virtiofs bind mounts.
            fs::read(out.join("upgrade_probe.so"))
                .and_then(|bytes| fs::write(&staged, bytes))
                .with_context(|| format!("staging {}", staged.display()))?;
        }
    }
    Ok(Fixture::ALL.into_iter().map(|f| deploy.join(f.so_name())).collect())
}
