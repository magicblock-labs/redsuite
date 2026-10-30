use std::ptr;

use anyhow::ensure;
use futures_util::{stream::FuturesUnordered, StreamExt};
use rand::seq::SliceRandom;
use redsuite_core::{
    catalog::{Er, Family, ScenarioEntry, ScenarioFuture, DEFAULT},
    config::ExecutionConfig,
    random::{self, Rng},
    topology, ErCtx, Result,
};
use tracing::info;

use super::{group::GroupEr, prepare};
use crate::catalog;

const FAMILIES: [Family; 3] = [Family::Redshift, Family::Redhat, Family::Redline];
const OWNER: &str = "continuous";

fn cases() -> Result<Vec<(Family, ScenarioEntry)>> {
    let mut cases = Vec::new();
    for family in FAMILIES {
        for entry in catalog::select(family, &[])? {
            if entry.er.group().is_some_and(|group| ptr::eq(group, &DEFAULT)) {
                cases.push((family, entry));
            }
        }
    }
    Ok(cases)
}

async fn settle(
    running: &mut FuturesUnordered<ScenarioFuture>,
    keep: usize,
    failed: &mut Vec<String>,
) {
    while running.len() > keep {
        let Some(record) = running.next().await else { break };
        if !record.passed() {
            failed.push(record.name);
        }
    }
}

async fn run_round(
    cases: Vec<(Family, ScenarioEntry)>,
    config: &ExecutionConfig,
    er: &ErCtx,
    limit: usize,
) -> Vec<String> {
    let mut running = FuturesUnordered::new();
    let mut failed = Vec::new();
    for (family, entry) in cases {
        let exclusive = matches!(entry.er, Er::Exclusive(_));
        let keep = if exclusive { 0 } else { limit - 1 };
        settle(&mut running, keep, &mut failed).await;
        if !failed.is_empty() || config.stopped() {
            break;
        }
        running.push(entry.run(family, config.clone(), Some(Ok(er.clone()))));
        if exclusive {
            settle(&mut running, 0, &mut failed).await;
        }
    }
    settle(&mut running, 0, &mut failed).await;
    failed
}

async fn run_rounds(config: &ExecutionConfig, er: &ErCtx, limit: usize) -> Result<()> {
    let mut round = 0;
    while !config.stopped() {
        let mut cases = cases()?;
        cases.shuffle(&mut random::rng(config.seed, &[OWNER, "order"], round));
        let round_seed = random::rng(config.seed, &[OWNER, "seed"], round).gen();
        let order: Vec<_> = cases.iter().map(|(family, entry)| entry.name(*family)).collect();
        info!(
            seed = config.seed,
            round,
            round_seed,
            ?order,
            "continuous round"
        );
        let round_config = ExecutionConfig {
            seed: round_seed,
            ..config.clone()
        };
        let failed = run_round(cases, &round_config, er, limit).await;
        ensure!(
            failed.is_empty(),
            "round {round} of seed {} (round seed {round_seed}) failed: {}",
            config.seed,
            failed.join(", ")
        );
        round += 1;
    }
    Ok(())
}

pub async fn run(config: ExecutionConfig, limit: usize) -> Result<()> {
    let base = topology::base_only(config.clone()).await?;
    let selected = cases()?;
    prepare(selected.iter().map(|(_, entry)| entry), &base).await?;
    let base = Ok(base);
    let group = GroupEr::boot(&DEFAULT, &base).await?;
    let outcome = run_rounds(&config, group.ctx(), limit).await;
    let finished = group.finish().await;
    if let Ok(base) = &base {
        base.reclaim(OWNER);
    }
    outcome.and(finished)
}
