use std::{
    fs::File, future::Future, panic::AssertUnwindSafe, path::Path,
    sync::OnceLock,
};

use futures_util::FutureExt;
pub use rand::Rng;
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tracing::info;

use crate::{
    catalog::Fixture, check, runner::panic_message, topology, CheckError,
    Result,
};

const GENERATOR_VERSION: u32 = 2;

pub struct CaseStreams {
    pub payload: StdRng,
    pub grammar: StdRng,
    pub schedule: StdRng,
}

fn stream_rng(
    seed: u64,
    domain: &str,
    group: &str,
    scenario: &str,
    name: &str,
    index: u64,
) -> StdRng {
    let mut digest = Sha256::new();
    digest.update(b"redsuite.attack");
    digest.update(GENERATOR_VERSION.to_le_bytes());
    digest.update(seed.to_le_bytes());
    for key in [domain, group, scenario, name] {
        digest.update((key.len() as u64).to_le_bytes());
        digest.update(key.as_bytes());
    }
    digest.update(index.to_le_bytes());
    StdRng::from_seed(digest.finalize().into())
}

pub fn fixtures_rng(
    seed: u64,
    group: &str,
    scenario: &str,
    fixture: &str,
    index: u64,
) -> StdRng {
    stream_rng(seed, "fixtures", group, scenario, fixture, index)
}

pub struct Case<T> {
    pub name: String,
    pub index: u64,
    pub input: T,
    seed: u64,
    group: String,
    scenario: String,
    description: String,
}

pub fn plan<'a, T: Serialize>(
    seed: u64,
    group: &str,
    scenario: &str,
    cases: impl IntoIterator<Item = (&'a str, u64)>,
    generate: impl Fn(&str, u64, &mut CaseStreams) -> T,
) -> Result<Vec<Case<T>>> {
    let cases = cases
        .into_iter()
        .map(|(name, index)| {
            let rng =
                |domain| stream_rng(seed, domain, group, scenario, name, index);
            let mut streams = CaseStreams {
                payload: rng("payload"),
                grammar: rng("grammar"),
                schedule: rng("schedule"),
            };
            let input = generate(name, index, &mut streams);
            Ok(Case {
                description: json::to_string(&input)?,
                name: name.to_owned(),
                index,
                input,
                seed,
                group: group.to_owned(),
                scenario: scenario.to_owned(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    info!(seed, group, scenario, revisions = revisions(), "case plan");
    for case in &cases {
        info!(seed, group, scenario, case = %case.name, index = case.index,
            input = %case.description, "case planned");
    }
    Ok(cases)
}

impl<T> Case<T> {
    pub async fn run<R>(
        &self,
        work: impl Future<Output = Result<R>>,
    ) -> Result<R> {
        check::phase(
            &format!(
                "{}/{}/{}[{}]",
                self.group, self.scenario, self.name, self.index
            ),
            async {
                AssertUnwindSafe(work).catch_unwind().await.unwrap_or_else(
                    |payload| Err(panic_message(payload).into()),
                )
            },
        )
        .await
        .map_err(|error| {
            CheckError::new("case failed")
                .context("seed", self.seed.to_string())
                .context("group", &self.group)
                .context("scenario", &self.scenario)
                .context("case", format!("{}[{}]", self.name, self.index))
                .context("input", &self.description)
                .context("revisions", revisions())
                .caused_by(error)
                .into()
        })
    }
}

fn artifact_hash(path: &Path) -> std::io::Result<String> {
    let mut digest = Sha256::new();
    std::io::copy(&mut File::open(path)?, &mut digest)?;
    Ok(hash::Hash::new_from_array(digest.finalize().into()).to_string())
}

fn revisions() -> &'static str {
    static REVISIONS: OnceLock<String> = OnceLock::new();
    REVISIONS.get_or_init(|| {
        let artifacts = [
            ("redsuite", std::env::current_exe().ok()),
            ("validator", topology::er_bin_path().ok()),
        ]
        .into_iter()
        .chain(
            Fixture::ALL
                .map(|fixture| (fixture.so_name(), fixture.resolve().ok())),
        )
        .map(|(name, path)| {
            let revision = path
                .and_then(|path| artifact_hash(&path).ok())
                .unwrap_or_else(|| "unavailable".into());
            format!("{name}={revision}")
        })
        .collect::<Vec<_>>()
        .join(", ");
        format!(
            "generator={GENERATOR_VERSION}, redsuite={}, {artifacts}",
            env!("CARGO_PKG_VERSION")
        )
    })
}
