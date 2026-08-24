use std::time::{Duration, Instant};

use async_trait::async_trait;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, prep,
    stats::StreamingStats,
    transport::{rate::RateManager, ws::AccountUpdates},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
};

use crate::program::{layout, DELEGATION_PROGRAM_ID};

const ACCOUNTS: u8 = 4;
const ITERATIONS: u64 = 40;
const RATE: u32 = 20;
const CONCURRENCY: usize = 8;
const PAYER_LAMPORTS: u64 = 2_000_000_000;

pub struct SimpleLoad;

#[async_trait(?Send)]
impl Scenario for SimpleLoad {
    fn name(&self) -> &str {
        "redline/simple_load"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let payer = prep::funded_payer(base, PAYER_LAMPORTS).await?;
        let pdas = crate::init_delegated_accounts(
            base,
            &payer,
            ACCOUNTS,
            crate::ACCOUNT_SPACE,
            er.identity(),
        )
        .await?;

        for pda in &pdas {
            let on_base = base.account(pda).await?.ok_or("pda not on base")?;
            check_eq!(
                on_base.owner,
                DELEGATION_PROGRAM_ID,
                "delegated pda must be dlp-owned on base"
            )?;
            check::poll(
                &format!("the ER clones the delegated pda {pda}"),
                Duration::from_secs(15),
                || async {
                    matches!(er.account(pda).await, Ok(Some(acc)) if acc.data.len() == crate::ACCOUNT_SPACE as usize)
                },
            )
            .await?;
        }

        let updates =
            AccountUpdates::connect(er.ws_url(), crate::account_update_id)
                .await?;
        for pda in &pdas {
            updates.account_subscribe(pda).await?;
        }
        updates
            .await_subscribed(pdas.len(), Duration::from_secs(5))
            .await?;

        let mut rate = RateManager::new(CONCURRENCY, RATE);
        let mut latency = StreamingStats::new();
        for id in 1..=ITERATIONS {
            let _permit = rate.tick().await;
            let target = pdas[((id - 1) % pdas.len() as u64) as usize];
            let ix = crate::program::instruction::build::simple_byte_set(
                id,
                &[target],
            );
            updates.track(id, target);
            let sent = Instant::now();
            er.send(&payer, &[ix]).await?;
            latency.push(sent.elapsed().as_micros() as u32);
        }

        updates
            .await_observed(ITERATIONS as usize, Duration::from_secs(15))
            .await?;
        let update_lag = updates.finalize().lag;

        for (i, pda) in pdas.iter().enumerate() {
            let last_id = ITERATIONS - ACCOUNTS as u64 + 1 + i as u64;
            let on_er = er.account(pda).await?.ok_or("pda not on er")?;
            let id_bytes = &on_er.data
                [layout::ID_OFFSET..layout::ID_OFFSET + layout::ID_SIZE];
            check_eq!(
                id_bytes,
                last_id.to_le_bytes(),
                "er copy must hold the last id written to pda {i}"
            )?;

            let on_base = base.account(pda).await?.ok_or("pda gone on base")?;
            check!(
                on_base.data[layout::DATA_OFFSET..].iter().all(|&b| b == 0),
                "base copy must stay untouched until an explicit commit"
            )?;
        }

        Ok(ScenarioReport::ok(self.name())
            .observe("send+confirm us", Unit::Micros, latency.finalize(false))
            .observe("account-update lag us", Unit::Micros, update_lag))
    }
}
