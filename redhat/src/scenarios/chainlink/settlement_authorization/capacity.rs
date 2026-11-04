use super::{
    callbacks::{callback_args, scheduled, CallbackCategory},
    commits::buffers::outbox_intent_pda,
    *,
};
use futures_util::future::join_all;
use magic_api::MAGIC_CONTEXT_PUBKEY;
use redsuite_core::wait;
use std::time::Duration;

const BURST: u64 = 8;
const PREFUNDED: u64 = 4;
const RENT_EXEMPT: u64 = 890_880;
const CALLBACK_FEE: u64 = 10_000;
const SETTLE: Duration = Duration::from_secs(30);

impl SettlementAuthorization {
    async fn staged(&self) -> Result<(u64, u64)> {
        let context = self.er.local_images(&[MAGIC_CONTEXT_PUBKEY]).await?.pop().flatten();
        let data = context.context("magic context missing")?.data;
        let word = |at: usize| Some(u64::from_le_bytes(data.get(at..at + 8)?.try_into().ok()?));
        word(0).zip(word(8)).context("magic context truncated")
    }

    async fn drained(&self) -> Result<u64> {
        wait::retry("staged intents drain", SETTLE, async || {
            let (next, staged) = self.staged().await?;
            ensure!(staged == 0, "{staged} intents still staged");
            Ok(next)
        })
        .await
    }

    async fn lamports(&self, keys: &[Pubkey]) -> Result<Vec<u64>> {
        self.er
            .accounts(keys)
            .await?
            .into_iter()
            .map(|account| account.map(|account| account.lamports))
            .collect::<Option<_>>()
            .context("callback payer or vault missing")
    }

    pub(super) async fn check_capacity(&self) -> Result<()> {
        let plan = &self.callback_plan;
        let (funder, payer, cell) = (&plan.funder, &plan.payer, plan.cells[1]);
        let vault = dlp::magic_fee_vault_pda(&self.er.identity());
        let funds = [payer.pubkey(), vault];
        let nonce = random::rng(
            self.base.config().seed,
            &["redhat", "settlement_capacity"],
            0,
        )
        .gen_range(1..u64::MAX / 2);
        let next = self.drained().await?;
        let transfers: Vec<_> = (next..next + PREFUNDED)
            .map(|id| system::transfer(&funder.pubkey(), &outbox_intent_pda(id), RENT_EXEMPT))
            .collect();
        self.base.submit_and_confirm(funder, &transfers).await?;

        let before = self.lamports(&funds).await?;
        let mut expected = self
            .er
            .local_images(&[cell])
            .await?
            .pop()
            .flatten()
            .context("burst cell missing")?;
        let signatures = join_all((nonce..).step_by(2).take(BURST as usize).map(|nonce| {
            let instructions = callback_args(cell, 0).map(|args| {
                [
                    scheduled(
                        payer.pubkey(),
                        self.er.identity(),
                        &[cell],
                        nonce,
                        false,
                        None,
                        CallbackCategory::Standalone,
                    ),
                    REDSHIFT
                        .batch()
                        .nonce(nonce + 1)
                        .invoke(MAGIC.add_callback(payer.pubkey(), vault, args))
                        .build(),
                ]
            });
            async move { self.er.submit_and_confirm(payer, &instructions?).await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        let (_, peak) = self.staged().await?;
        for signature in &signatures {
            self.er
                .check_commit(&self.base, signature, &[], false)
                .await?
                .check_success("callback burst past prefunded outbox rows")?;
        }
        let mut header = CellHeader::try_decode(&expected.data)?;
        header.revision += BURST;
        header.write(&mut expected.data)?;
        wait::retry("burst callbacks delivered", SETTLE, async || {
            let actual = self.er.local_images(&[cell]).await?.pop().flatten();
            ensure!(actual.as_ref() == Some(&expected), "burst cell {actual:?}");
            Ok(())
        })
        .await?;
        let after = self.drained().await?;
        tracing::debug!(
            next,
            after,
            peak,
            prefunded = PREFUNDED,
            "callback burst settled"
        );
        ensure!(
            after >= next + BURST,
            "intent ids {next}..{after} skipped the burst"
        );
        ensure!(
            self.lamports(&funds).await?
                == [before[0] - BURST * CALLBACK_FEE, before[1] + BURST * CALLBACK_FEE],
            "burst callbacks were not charged exactly once"
        );
        Ok(())
    }
}
