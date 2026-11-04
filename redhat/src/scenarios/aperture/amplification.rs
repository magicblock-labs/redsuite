use super::{fixture::Fixture, BODY_LIMIT, QUERY_LIMIT, TIMEOUT};
use anyhow::{ensure, Context};
use futures_util::future::join_all;
use hash::Hash;
use pubkey::Pubkey;
use redsuite_core::{
    api::RpcError,
    netfault::{Faults, RuleHandle, Selector},
    programs::REDSHIFT,
    random::{self, Rng},
    rpc_types::request::RpcRequest,
    transport::wsraw::RawWs,
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use serde_json::{json, Value};
use std::{rc::Rc, result};
use tokio::time;
use tracing::debug;

const COLD_INPUTS: usize = 24;
const REFETCH: usize = 2;
const SUBSCRIPTIONS: usize = 512;
const STALLED: usize = 16;
const CANARIES: u64 = 4;
const BUSY_EXECUTORS: &str = "engine_processor_busy_executors";

pub struct UnpaidWork {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

fn reads(key: &Pubkey) -> Selector {
    Selector::methods(&["getAccountInfo", "getMultipleAccounts"])
        .http()
        .request()
        .account(key)
}

fn stale(rng: &mut impl Rng) -> Hash {
    Hash::new_from_array(random::pubkey(rng).to_bytes())
}

fn cold(rng: &mut impl Rng, count: usize) -> Vec<Pubkey> {
    (0..count).map(|_| random::pubkey(rng)).collect()
}

fn tally(observed: Vec<RuleHandle>) -> (u64, usize) {
    let fetched = observed.iter().filter(|rule| rule.hits() > 0).count() as u64;
    let peak = observed.iter().map(RuleHandle::hits).max().unwrap_or_default();
    observed.into_iter().for_each(RuleHandle::remove);
    (fetched, peak)
}

fn charged(label: &str, claims: u64, observed: Vec<RuleHandle>, distinct: usize) -> Result<()> {
    let (fetched, peak) = tally(observed);
    debug!(
        label,
        claims, fetched, peak, distinct, "remote work against claims"
    );
    ensure!(
        fetched <= claims,
        "{label}: fetched {fetched} keys, claimed {claims}"
    );
    ensure!(
        claims <= distinct as u64,
        "{label}: claimed {claims} for {distinct} keys"
    );
    ensure!(peak <= REFETCH, "{label}: one key fetched {peak} times");
    Ok(())
}

async fn query(er: &ErCtx, keys: &[Pubkey]) -> Result<(u64, result::Result<Value, RpcError>)> {
    let keys: Vec<_> = keys.iter().map(Pubkey::to_string).collect();
    er.api()
        .try_call_claimed(
            RpcRequest::GetMultipleAccounts,
            &(keys, json!({"encoding": "base64"})),
        )
        .await
}

async fn busy(er: &ErCtx) -> Result<f64> {
    er.scrape_metrics()
        .await?
        .value_sum(BUSY_EXECUTORS)
        .context("busy executors metric missing")
}

async fn idle(er: &ErCtx) -> Result<()> {
    wait::retry("executors drain to idle", TIMEOUT, async || {
        let busy = busy(er).await?;
        ensure!(busy == 0.0, "{busy} executors busy");
        Ok(())
    })
    .await
}

impl UnpaidWork {
    async fn cost(fixture: &mut Fixture, nonce: u64) -> Result<(u64, u64)> {
        let (claims, read) = query(&fixture.er, &fixture.keys[1..3]).await?;
        read?;
        let payer = |fixture: &Fixture| {
            fixture.expected[0]
                .as_ref()
                .map(|payer| payer.lamports)
                .context("payer missing")
        };
        let before = payer(fixture)?;
        fixture.write(nonce, 1).await?;
        Ok((claims, before - payer(fixture)?))
    }

    async fn prepared(fixture: &Fixture, faults: &Faults, rng: &mut impl Rng) -> Result<()> {
        let inputs = cold(rng, COLD_INPUTS);
        let observed = inputs.iter().map(|key| faults.observe(reads(key))).collect();
        let tx = fixture.signed_with(fixture.message(8_000, 1, &inputs).await?, stale(rng))?;
        let start = fixture.er.api().get_slot().await?;
        let (claims, sent) = tx.send_claimed(&fixture.er, false).await?;
        let error = sent.err().context("stale transaction admitted")?;
        ensure!(error.error.code.code() == -32003, "{error}");
        charged("stale transaction", claims, observed, COLD_INPUTS)?;
        fixture.not_admitted(&tx.signature, start, None).await?;

        let distinct = cold(rng, QUERY_LIMIT);
        let over = cold(rng, QUERY_LIMIT + 1);
        let repeated = random::pubkey(rng);
        for (label, keys, unique, admitted) in [
            ("cold query", distinct.clone(), distinct, true),
            (
                "repeated key",
                vec![repeated; QUERY_LIMIT],
                vec![repeated],
                true,
            ),
            ("over-limit query", over.clone(), over, false),
        ] {
            let observed = unique.iter().map(|key| faults.observe(reads(key))).collect();
            let (claims, result) = query(&fixture.er, &keys).await?;
            match result {
                Ok(reply) => ensure!(
                    admitted && reply["value"].as_array().map(Vec::len) == Some(keys.len()),
                    "{label} served: {reply}"
                ),
                Err(error) => ensure!(!admitted && error.error.code.code() == -32602, "{error}"),
            }
            charged(
                label,
                claims,
                observed,
                unique.len() * usize::from(admitted),
            )?;
        }

        let filter = json!({"memcmp": {"offset": 0, "bytes": "1"}});
        let filters = vec![filter.clone(); (BODY_LIMIT - 512) / (filter.to_string().len() + 1)];
        let count = filters.len();
        let scans = faults.observe(Selector::method("getProgramAccounts"));
        let params = (
            REDSHIFT.id().to_string(),
            json!({"encoding": "base64", "filters": filters}),
        );
        let api = fixture.er.api();
        let (scan, canary) = tokio::join!(
            api.try_call_claimed::<Value>(RpcRequest::GetProgramAccounts, &params),
            time::timeout(TIMEOUT, api.get_slot()),
        );
        let (claims, scan) = scan?;
        canary.context("rpc starved by an oversized filter set")??;
        debug!(
            filters = count,
            refused = scan.is_err(),
            "oversized filter set"
        );
        ensure!(
            claims == 0 && scans.hits() == 0,
            "filter scan claimed {claims} remote reads"
        );
        scans.remove();
        Ok(())
    }

    async fn flood(fixture: &mut Fixture, faults: &Faults, rng: &mut impl Rng) -> Result<()> {
        let mut control = RawWs::connect(fixture.er.ws_url()).await?;
        let subscription = control.account_subscribe(&fixture.keys[1]).await?;
        let keys = cold(rng, SUBSCRIPTIONS);
        let observed = keys
            .iter()
            .map(|key| faults.observe(Selector::default().request().account(key)))
            .collect();
        let mut flooded = RawWs::connect(fixture.er.ws_url()).await?;
        let mut accepted = 0;
        for key in &keys {
            if let Err(error) = flooded.try_account_subscribe(key).await? {
                debug!(accepted, %error, "subscription limit refused");
                break;
            }
            accepted += 1;
        }
        fixture.confirmed_write(&mut control, subscription, 8_100).await?;
        let (fetched, _) = tally(observed);
        debug!(accepted, fetched, "per-connection subscription flood");
        ensure!(fetched == 0, "{fetched} local subscriptions reached base");
        let _ = flooded.close().await;
        fixture.confirmed_write(&mut control, subscription, 8_101).await?;
        control.close().await
    }

    async fn occupancy(fixture: &mut Fixture, faults: &Faults, rng: &mut impl Rng) -> Result<()> {
        let mut hostile = Vec::with_capacity(STALLED);
        let mut stalls = Vec::with_capacity(STALLED);
        for (nonce, key) in (8_200..).zip(cold(rng, STALLED)) {
            stalls.push(faults.stall(reads(&key)));
            hostile
                .push(fixture.signed_with(fixture.message(nonce, 1, &[key]).await?, stale(rng))?);
        }
        let er = fixture.er.clone();
        let start = er.api().get_slot().await?;
        let (sent, pressure) = tokio::join!(
            join_all(hostile.iter().map(|tx| tx.send_config(&er, true))),
            async {
                for stall in &stalls {
                    stall.wait_hit(TIMEOUT).await?;
                }
                let mut peak = 0.0f64;
                for nonce in 8_300..8_300 + CANARIES {
                    time::timeout(TIMEOUT, fixture.write(nonce, 1))
                        .await
                        .context("canary starved by stalled base reads")??;
                    peak = peak.max(busy(&er).await?);
                }
                debug!(
                    stalled = STALLED,
                    peak, "canaries landed while base reads stalled"
                );
                stalls.into_iter().for_each(RuleHandle::remove);
                Result::Ok(())
            }
        );
        pressure?;
        for result in sent {
            result??;
        }
        idle(&er).await?;
        for tx in &hostile {
            fixture.not_admitted(&tx.signature, start, None).await?;
        }
        Ok(())
    }
}

impl Scenario for UnpaidWork {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        let mut rng = random::rng(self.base.config().seed, &["redhat", "unpaid_work"], 0);
        let mut fixture = Fixture::new(self.base, self.er, &mut rng).await?;
        let faults = fixture.er.faults()?;
        idle(&fixture.er).await?;
        let baseline = Self::cost(&mut fixture, 8_001).await?;
        ensure!(
            baseline.0 == 0,
            "warm read claimed {} remote reads",
            baseline.0
        );

        Self::prepared(&fixture, &faults, &mut rng).await?;
        Self::flood(&mut fixture, &faults, &mut rng).await?;
        Self::occupancy(&mut fixture, &faults, &mut rng).await?;

        faults.reset().await?;
        idle(&fixture.er).await?;
        let recovered = Self::cost(&mut fixture, 8_900).await?;
        ensure!(
            recovered == baseline,
            "per-request cost {recovered:?} did not return to {baseline:?}"
        );
        Ok(())
    }
}
