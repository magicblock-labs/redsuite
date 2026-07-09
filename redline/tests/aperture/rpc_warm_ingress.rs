use std::{rc::Rc, time::Duration};

use async_trait::async_trait;
use redline::program::{instruction::build, layout};
use redsuite_core::{
    assert::poll_until,
    prep, run_scenario,
    runner::{drive, drive_closed, RunConfig},
    transport::ws::{AccountUpdates, SignatureConfirmations},
    BaseCtx, ChainCtx, ErCtx, MetricsDelta, Result, Scenario, ScenarioReport,
    TxSender,
};

const PAYER_LAMPORTS: u64 = 2_000_000_000;

const CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);

struct Profile {
    name: &'static str,
    payers: usize,
    accounts: u8,
    warmup: u64,
    iterations: u64,
    rate: u32,
    concurrency: usize,
}

const LITE: Profile = Profile {
    name: "lite",
    payers: 8,
    accounts: 16,
    warmup: 100,
    iterations: 400,
    rate: 200,
    concurrency: 64,
};

const FULL: Profile = Profile {
    name: "full",
    payers: 32,
    accounts: 64,
    warmup: 3_000,
    iterations: 30_000,
    rate: 1_000,
    concurrency: 256,
};

// scheduled/nightly sustain window (~60 s warmup + ~300 s measured) —
// catches degradation over time that a 30 s window can't
const SOAK: Profile = Profile {
    name: "soak",
    payers: 64,
    accounts: 64,
    warmup: 60_000,
    iterations: 300_000,
    rate: 1_000,
    concurrency: 256,
};

fn profile() -> &'static Profile {
    match std::env::var("REDSUITE_PROFILE") {
        Ok(name) if name == "full" => &FULL,
        Ok(name) if name == "soak" => &SOAK,
        Ok(name) if name == "lite" => &LITE,
        Ok(name) => {
            panic!("unknown REDSUITE_PROFILE `{name}` (lite|full|soak)")
        }
        Err(_) => &LITE,
    }
}

// Open loop (default): the rate permit is released on delivery — sustained
// pressure.
// Closed loop: held until every confirmation for the id arrives —
// true round-trip under backpressure.
fn loop_mode() -> &'static str {
    match std::env::var("REDSUITE_LOOP") {
        Ok(mode) if mode == "closed" => "closed",
        Ok(mode) if mode == "open" => "open",
        Ok(mode) => panic!("unknown REDSUITE_LOOP `{mode}` (open|closed)"),
        Err(_) => "open",
    }
}

struct WarmIngress;

#[async_trait(?Send)]
impl Scenario for WarmIngress {
    fn name(&self) -> &str {
        "redline/rpc_warm_ingress"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let profile = profile();
        let payers =
            prep::funded_payers(base, profile.payers, PAYER_LAMPORTS).await?;
        let pdas = redline::init_delegated_accounts(
            base,
            &payers[0],
            profile.accounts,
            redline::ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;
        for pda in &pdas {
            poll_until(Duration::from_secs(15), || async {
                matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == redline::ACCOUNT_SPACE as usize)
            })
            .await;
        }

        let senders: Vec<TxSender> = payers
            .into_iter()
            .map(|payer| er.sender(Rc::new(payer)))
            .collect();

        let shape = |id: u64| {
            let len = pdas.len() as u64;
            let base_index = ((id - 1) * 3) % len;
            let source = pdas[base_index as usize];
            let first_dest = pdas[((base_index + 1) % len) as usize];
            let second_dest = pdas[((base_index + 2) % len) as usize];
            (
                build::account_data_copy(
                    id,
                    &[source],
                    &[first_dest, second_dest],
                ),
                first_dest,
            )
        };

        let warmup = drive(
            RunConfig {
                iterations: profile.warmup,
                rate: profile.rate,
                concurrency: profile.concurrency,
            },
            |id| {
                let sender = senders[(id as usize) % senders.len()].clone();
                let (ix, _) = shape(id);
                async move { sender.send(&[ix]).await.map(|_| ()) }
            },
        )
        .await;
        assert_eq!(
            warmup.failed, 0,
            "warmup deliveries failed: {:?}",
            warmup.first_error
        );

        let updates = Rc::new(
            AccountUpdates::connect(er.ws_url(), redline::account_update_id)
                .await?,
        );
        for pda in &pdas {
            updates.account_subscribe(pda).await?;
        }
        updates
            .await_subscribed(pdas.len(), Duration::from_secs(5))
            .await?;
        let sigs = Rc::new(SignatureConfirmations::connect(er.ws_url()).await?);

        // warmup discarded and setup complete — open the measured window
        let before = er.scrape_metrics().await?;

        let offset = profile.warmup;
        let request = |iteration: u64| {
            let id = offset + iteration;
            let sender = senders[(id as usize) % senders.len()].clone();
            let (ix, tracked_dest) = shape(id);
            updates.track(id, tracked_dest);
            let sigs = sigs.clone();
            async move {
                // sign → subscribe → deliver: the signature subscription
                // must exist before the tx can confirm
                let tx = sender.prepare(&[ix]).await?;
                sigs.subscribe(id, &tx.signatures[0]).await?;
                sender.deliver(&tx).await?;
                Ok(())
            }
        };
        let sync = |iteration: u64| {
            let id = offset + iteration;
            let sigs = sigs.clone();
            let updates = updates.clone();
            async move {
                tokio::time::timeout(CONFIRM_TIMEOUT, async move {
                    sigs.await_id(id).await?;
                    updates.await_id(id).await
                })
                .await
                .map_err(|_| {
                    format!(
                        "confirmations for id {id} not within {CONFIRM_TIMEOUT:?}"
                    )
                })?
            }
        };
        let cfg = RunConfig {
            iterations: profile.iterations,
            rate: profile.rate,
            concurrency: profile.concurrency,
        };
        let mode = loop_mode();
        let outcome = if mode == "closed" {
            drive_closed(cfg, request, sync).await
        } else {
            drive(cfg, request).await
        };
        assert_eq!(
            outcome.failed, 0,
            "measured iterations failed: {:?}",
            outcome.first_error
        );

        // open loop drains here; a closed loop has already settled per id
        sigs.await_all(CONFIRM_TIMEOUT).await?;
        updates.await_settled(CONFIRM_TIMEOUT).await?;
        // window closes only once the fan-out settled — the after scrape
        // must cover everything the measured load caused
        let after = er.scrape_metrics().await?;
        let delta = MetricsDelta::new(before, after);

        // validator-side cross-checks, gated on what this build exposes:
        // the no-op gate (our load provably hit the validator) and the
        // nothing-failed-on-chain invariant
        if let Some(processed) = delta.counter("mbv_transaction_count") {
            assert!(
                processed >= profile.iterations as f64,
                "validator processed {processed} txs in the measured window, \
                 expected at least {}",
                profile.iterations
            );
        }
        if let Some(failed) = delta.counter("mbv_failed_transactions_count") {
            assert_eq!(failed, 0.0, "transactions failed on the validator");
        }

        let update_outcome = updates.finalize();
        assert_eq!(
            update_outcome.observed + update_outcome.superseded,
            profile.iterations as usize,
            "every tracked write must be observed or superseded"
        );
        let sig_outcome = sigs.finalize();
        assert_eq!(
            sig_outcome.failed, 0,
            "transactions failed on-chain: {:?}",
            sig_outcome.first_failure
        );
        assert_eq!(
            sig_outcome.unconfirmed, 0,
            "signatures unconfirmed after {CONFIRM_TIMEOUT:?}"
        );
        assert_eq!(
            sig_outcome.confirmed, profile.iterations as usize,
            "every measured tx must confirm"
        );

        for (idx, pda) in pdas.iter().enumerate() {
            let len = pdas.len() as u64;
            let idx = idx as u64;
            let mut last_id = 0;
            for id in offset + 1..=offset + profile.iterations {
                let base_index = ((id - 1) * 3) % len;
                if (base_index + 1) % len == idx
                    || (base_index + 2) % len == idx
                {
                    last_id = id;
                }
            }
            if last_id == 0 {
                continue;
            }
            let on_er = er.account(pda).await?.ok_or("pda not on er")?;
            let id_bytes = &on_er.data
                [layout::ID_OFFSET..layout::ID_OFFSET + layout::ID_SIZE];
            assert_eq!(
                id_bytes,
                last_id.to_le_bytes(),
                "er copy must hold the last id written to pda {idx}"
            );
        }

        let mut report = ScenarioReport::ok(self.name())
            .setting("profile", profile.name)
            .setting("shape", "read-write 3/tx (1 src + 2 dst data-copy)")
            .setting("loop", mode)
            .setting("confirm timeout s", CONFIRM_TIMEOUT.as_secs())
            .setting("payers", profile.payers)
            .setting("accounts", profile.accounts)
            .setting("warmup iters", profile.warmup)
            .setting("measured iters", profile.iterations)
            .setting("offered tps", profile.rate)
            .setting("concurrency", profile.concurrency)
            .observe("delivery us", outcome.delivery)
            .observe("signature latency us", sig_outcome.latency)
            .observe("achieved rps", outcome.rps)
            .observe("account-update lag us", update_outcome.lag)
            .metric("achieved tps", outcome.achieved_rps())
            .metric("measured wall s", outcome.wall.as_secs_f64())
            .metric("superseded", update_outcome.superseded as f64)
            // validator-side numbers (histogram window averages, converted
            // to us) — never comparable 1:1 with the client-side stats
            // above, but a divergence points at the harness, not the
            // validator (R1)
            .metric_if(
                "validator tx processing avg us",
                delta
                    .histogram_avg("mbv_transaction_processing_time")
                    .map(|seconds| seconds * 1e6),
            )
            .metric_if(
                "validator ensure accounts avg us",
                delta
                    .histogram_avg(
                        r#"mbv_ensure_accounts_time{kind="transaction"}"#,
                    )
                    .map(|seconds| seconds * 1e6),
            )
            .metric_if(
                "validator txs in window",
                delta.counter("mbv_transaction_count"),
            )
            .metric_if(
                "monitored accounts (gauge)",
                delta.gauge("mbv_monitored_accounts_gauge"),
            );
        if let Some(sync) = outcome.sync {
            report = report.observe("sync round-trip us", sync);
        }
        Ok(report)
    }
}

#[tokio::test]
async fn rpc_warm_ingress() {
    run_scenario(WarmIngress).await;
}
