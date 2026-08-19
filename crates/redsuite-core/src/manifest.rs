use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use hash::Hash;
use json::{Deserialize, Serialize};
use pubkey::Pubkey;

use crate::{catalog::Fixture, topology, Result};

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureManifest {
    pub schema_version: u32,
    pub source_revision: String,
    pub artifacts: Vec<FixtureArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureArtifact {
    pub program_id: Pubkey,
    pub package: String,
    pub features: Vec<String>,
    pub path: PathBuf,
    pub content_hash: Hash,
}

pub fn manifest_path() -> PathBuf {
    manifest_path_at(&topology::workspace_root())
}

fn manifest_path_at(root: &Path) -> PathBuf {
    root.join("target/deploy/fixtures.json")
}

use sha2::{Digest, Sha256};

pub fn content_hash(bytes: &[u8]) -> Hash {
    Hash::new_from_array(Sha256::digest(bytes).into())
}

pub fn emit() -> Result<PathBuf> {
    let root = topology::workspace_root();
    let revision = source_revision(&root);
    emit_at(&root, revision)
}

fn emit_at(root: &Path, source_revision: String) -> Result<PathBuf> {
    let mut artifacts = Vec::new();
    for fixture in Fixture::ALL {
        let path = PathBuf::from("target/deploy").join(fixture.so_name());
        let bytes = fs::read(root.join(&path)).map_err(|err| {
            format!("reading staged fixture {}: {err}", path.display())
        })?;
        artifacts.push(FixtureArtifact {
            program_id: fixture.program_id(),
            package: fixture.package().to_owned(),
            features: fixture
                .features()
                .iter()
                .map(|feature| (*feature).to_owned())
                .collect(),
            path,
            content_hash: content_hash(&bytes),
        });
    }
    let manifest = FixtureManifest {
        schema_version: SCHEMA_VERSION,
        source_revision,
        artifacts,
    };
    let path = manifest_path_at(root);
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, json::to_string(&manifest)?)?;
    fs::rename(&tmp, &path)?;
    Ok(path)
}

pub fn load() -> Result<FixtureManifest> {
    load_at(&topology::workspace_root())
}

fn load_at(root: &Path) -> Result<FixtureManifest> {
    let path = manifest_path_at(root);
    let raw = fs::read_to_string(&path).map_err(|_| {
        format!(
            "{} is absent — `cargo xtask programs` builds the fixtures and \
             records it",
            path.display()
        )
    })?;
    let manifest: FixtureManifest = json::from_str(&raw).map_err(|err| {
        format!(
            "{} does not parse ({err}) — `cargo xtask programs` rewrites it",
            path.display()
        )
    })?;
    if manifest.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "{} has schema version {}, this build expects {SCHEMA_VERSION} — \
             `cargo xtask programs` rewrites it",
            path.display(),
            manifest.schema_version
        )
        .into());
    }
    Ok(manifest)
}

pub fn resolve(fixture: Fixture) -> Result<PathBuf> {
    resolve_at(&topology::workspace_root(), fixture)
}

fn resolve_at(root: &Path, fixture: Fixture) -> Result<PathBuf> {
    let manifest = load_at(root)?;
    let name = fixture.so_name();
    let artifact = manifest
        .artifacts
        .iter()
        .find(|artifact| {
            artifact.path.file_name().is_some_and(|file| file == name)
        })
        .ok_or_else(|| {
            format!(
                "{name} is not in the fixture manifest — \
                 `cargo xtask programs` stages it"
            )
        })?;
    let declared_id = fixture.program_id();
    if artifact.program_id != declared_id {
        return Err(format!(
            "{name}: the manifest records program id {}, the interface crate \
             declares {declared_id} — `cargo xtask programs` restages it",
            artifact.program_id
        )
        .into());
    }
    if artifact
        .features
        .iter()
        .map(String::as_str)
        .ne(fixture.features().iter().copied())
    {
        return Err(format!(
            "{name}: the manifest records features {:?}, this build expects \
             {:?} — `cargo xtask programs` restages it",
            artifact.features,
            fixture.features()
        )
        .into());
    }
    let path = root.join(&artifact.path);
    let bytes = fs::read(&path).map_err(|_| {
        format!(
            "fixture {} is not built — `cargo xtask programs` builds it",
            path.display()
        )
    })?;
    if content_hash(&bytes) != artifact.content_hash {
        return Err(format!(
            "{name} changed after staging (content hash mismatch) — \
             `cargo xtask programs` restages it"
        )
        .into());
    }
    Ok(path)
}

pub fn revision_drift(manifest: &FixtureManifest) -> Option<String> {
    let current = source_revision(&topology::workspace_root());
    if current == "unknown"
        || manifest.source_revision == "unknown"
        || current == manifest.source_revision
    {
        return None;
    }
    Some(format!(
        "the staged fixtures were built at revision {}, the tree is at \
         {current} — `cargo xtask programs` restages them",
        manifest.source_revision
    ))
}

fn source_revision(root: &Path) -> String {
    let Some(head) = git_output(root, &["rev-parse", "HEAD"]) else {
        return "unknown".to_owned();
    };
    let dirty =
        git_output(root, &["status", "--porcelain", "--untracked-files=no"])
            .is_none_or(|status| !status.is_empty());
    if dirty {
        format!("{head}-dirty")
    } else {
        head
    }
}

fn git_output(root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_root(test_name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "redsuite-manifest-{test_name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("target/deploy")).unwrap();
        for fixture in Fixture::ALL {
            fs::write(
                root.join("target/deploy").join(fixture.so_name()),
                fixture.so_name().as_bytes(),
            )
            .unwrap();
        }
        root
    }

    #[test]
    fn staged_fixtures_roundtrip_through_the_manifest() {
        let root = scratch_root("roundtrip");
        emit_at(&root, "test-revision".to_owned()).unwrap();
        let manifest = load_at(&root).unwrap();
        assert_eq!(manifest.schema_version, SCHEMA_VERSION);
        assert_eq!(manifest.source_revision, "test-revision");
        assert_eq!(manifest.artifacts.len(), Fixture::ALL.len());
        for fixture in Fixture::ALL {
            let resolved = resolve_at(&root, fixture).unwrap();
            assert_eq!(
                resolved,
                root.join("target/deploy").join(fixture.so_name())
            );
        }
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn changed_bytes_fail_resolution() {
        let root = scratch_root("changed-bytes");
        emit_at(&root, "test-revision".to_owned()).unwrap();
        let staged = root
            .join("target/deploy")
            .join(Fixture::RedhatProgram.so_name());
        fs::write(&staged, b"rebuilt without restaging").unwrap();
        let error = resolve_at(&root, Fixture::RedhatProgram).unwrap_err();
        assert!(error.to_string().contains("content hash mismatch"));
        assert!(resolve_at(&root, Fixture::RedlineProgram).is_ok());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_missing_manifest_names_the_remedy() {
        let root = scratch_root("missing-manifest");
        let error = resolve_at(&root, Fixture::RedlineProgram).unwrap_err();
        assert!(error.to_string().contains("cargo xtask programs"));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_foreign_schema_version_is_rejected() {
        let root = scratch_root("schema-version");
        emit_at(&root, "test-revision".to_owned()).unwrap();
        let mut manifest = load_at(&root).unwrap();
        manifest.schema_version = SCHEMA_VERSION + 1;
        fs::write(manifest_path_at(&root), json::to_string(&manifest).unwrap())
            .unwrap();
        let error = load_at(&root).unwrap_err();
        assert!(error.to_string().contains("schema version"));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn recorded_features_must_match_the_catalog() {
        let root = scratch_root("features");
        emit_at(&root, "test-revision".to_owned()).unwrap();
        let mut manifest = load_at(&root).unwrap();
        for artifact in &mut manifest.artifacts {
            if artifact
                .path
                .ends_with(Fixture::RedshiftProgramSlim.so_name())
            {
                artifact.features = vec!["upgraded".to_owned()];
            }
        }
        fs::write(manifest_path_at(&root), json::to_string(&manifest).unwrap())
            .unwrap();
        let error =
            resolve_at(&root, Fixture::RedshiftProgramSlim).unwrap_err();
        assert!(error.to_string().contains("features"));
        fs::remove_dir_all(&root).unwrap();
    }
}
