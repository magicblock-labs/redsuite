use std::time::{Duration, Instant};

use async_trait::async_trait;
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, prep, stats::StreamingStats, BaseCtx, ChainCtx, CheckError, ErCtx,
    Result, Scenario, ScenarioReport,
};
use signer::Signer;

const TRANSFERS: u64 = 10;
const LAMPORTS_PER_TRANSFER: u64 = 1_000_000;
const PAYER_LAMPORTS: u64 = 1_000_000_000;

pub struct Example;

#[async_trait(?Send)]
impl Scenario for Example {
    fn name(&self) -> &str {
        "redshift/example"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let payer = prep::funded_payer(base, PAYER_LAMPORTS).await?;
        let recipient = Keypair::new().pubkey();

        let mut latency = StreamingStats::new();
        let mut expected = 0;
        // identical transactions share a signature and get deduplicated —
        // every transaction must differ somewhere (here: the amount)
        for unique in 1..=TRANSFERS {
            let lamports = LAMPORTS_PER_TRANSFER + unique;
            let transfer = transfer_ix(payer.pubkey(), recipient, lamports);
            let sent = Instant::now();
            base.send(&payer, &[transfer]).await?;
            latency.push(sent.elapsed().as_micros() as u32);
            expected += lamports;
        }
        let on_base = base
            .account(&recipient)
            .await?
            .ok_or("recipient never appeared on base")?;
        if on_base.lamports != expected {
            return Err(CheckError::new(
                "the base recipient holds the delivered lamports",
            )
            .expected(expected.to_string())
            .actual(on_base.lamports.to_string())
            .into());
        }

        check::poll(
            "the er clones the recipient at the delivered balance",
            Duration::from_secs(15),
            || async {
                matches!(
                    er.account(&recipient).await,
                    Ok(Some(cloned)) if cloned.lamports == expected
                )
            },
        )
        .await?;

        let metrics = er.scrape_metrics().await?;

        Ok(ScenarioReport::ok(self.name())
            .setting("transfers", TRANSFERS)
            .observe("send+confirm us", Unit::Micros, latency.finalize(false))
            .metric("lamports delivered", Unit::Lamports, expected as f64)
            .metric_if(
                "er monitored accounts",
                Unit::Count,
                metrics.get("mbv_monitored_accounts_gauge"),
            ))
    }
}

fn transfer_ix(from: Pubkey, to: Pubkey, lamports: u64) -> Instruction {
    let system_program = Pubkey::default();
    let mut data = 2u32.to_le_bytes().to_vec();
    data.extend_from_slice(&lamports.to_le_bytes());
    Instruction::new_with_bytes(
        system_program,
        &data,
        vec![AccountMeta::new(from, true), AccountMeta::new(to, false)],
    )
}
