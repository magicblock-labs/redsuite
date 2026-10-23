use std::{future::Future, rc::Rc, sync::Arc, time::Duration};

use futures_util::future::join_all;
use keypair::Keypair;
use pubkey::Pubkey;
use signer::Signer;
use url::Url;

use crate::{
    check,
    context::{BaseCtx, ChainCtx, ErCtx},
    dlp, system, CheckError, DynError, ErClient, Result, TxSender,
};

const ZERO_DATA_RENT_EXEMPT_LAMPORTS: u64 = 890_880;
pub const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;

const PREP_CONCURRENCY: usize = 32;

pub(crate) async fn bounded<T, F, Fut>(count: usize, what: &str, make: F) -> Result<Vec<T>>
where
    F: Fn(usize) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    use futures_util::{StreamExt, TryFutureExt, TryStreamExt};
    futures_util::stream::iter(0..count)
        .map(|index| {
            make(index).map_err(move |error| {
                DynError::from(
                    CheckError::new(format!("preparing {what} {index} of {count}"))
                        .caused_by(error),
                )
            })
        })
        .buffered(PREP_CONCURRENCY)
        .try_collect()
        .await
}

pub fn payer_from_bytes(bytes: &[u8]) -> Keypair {
    Keypair::try_from(bytes).expect("payer bytes round-trip")
}

// Each worker owns its client/cache and the same round-robin payer partition.
pub fn worker_senders(
    rpc_url: &Url,
    payers: &[[u8; 64]],
    worker: usize,
    threads: usize,
) -> Vec<TxSender> {
    let client = ErClient::new(rpc_url.clone());
    payers
        .iter()
        .enumerate()
        .filter(|(index, _)| index % threads == worker)
        .map(|(_, bytes)| client.sender(Rc::new(payer_from_bytes(bytes))))
        .collect()
}

pub struct EscrowedPayer {
    pub payer: Keypair,
    pub escrow: Pubkey,
    pub delegation_record: Pubkey,
    pub escrow_lamports: u64,
}

pub fn payer_bytes(payers: &[Keypair]) -> Arc<Vec<[u8; 64]>> {
    Arc::new(payers.iter().map(Keypair::to_bytes).collect())
}

impl BaseCtx {
    pub async fn funded_payer(&self, lamports: u64) -> Result<Keypair> {
        let payer = Keypair::new();
        self.airdrop(&payer.pubkey(), lamports).await.map_err(|error| {
            CheckError::new(format!("funding payer {}", payer.pubkey())).caused_by(error)
        })?;
        Ok(payer)
    }

    pub async fn funded_payers(&self, count: usize, lamports: u64) -> Result<Vec<Keypair>> {
        bounded(count, "payer", |_| self.funded_payer(lamports)).await
    }

    pub async fn escrowed_payer(&self, validator: Pubkey, lamports: u64) -> Result<EscrowedPayer> {
        let payer = self.funded_payer(lamports).await?;
        let payer_pubkey = payer.pubkey();
        let top_up_lamports = lamports / 2;
        let escrow_setup = [
            dlp::top_up_ephemeral_balance(&payer_pubkey, top_up_lamports, 0),
            dlp::delegate_ephemeral_balance(&payer_pubkey, &validator, 0),
        ];
        self.submit_and_confirm(&payer, &escrow_setup).await?;

        let escrow = dlp::ephemeral_balance_pda(&payer_pubkey, 0);
        let escrow_lamports = top_up_lamports + ZERO_DATA_RENT_EXEMPT_LAMPORTS;
        let on_chain = self
            .account(&escrow)
            .await?
            .ok_or("escrow account missing after top-up + delegate")?;
        if on_chain.owner != dlp::dlp_id() {
            return Err(format!(
                "escrow {escrow} owned by {}, expected the delegation program",
                on_chain.owner
            )
            .into());
        }
        if on_chain.lamports != escrow_lamports {
            return Err(format!(
                "escrow {escrow} holds {} lamports, expected {escrow_lamports}",
                on_chain.lamports
            )
            .into());
        }
        Ok(EscrowedPayer {
            payer,
            escrow,
            delegation_record: dlp::delegation_record_pda(&escrow),
            escrow_lamports,
        })
    }

    pub async fn delegated_payer(
        &self,
        funder: &Keypair,
        validator: Pubkey,
        lamports: u64,
    ) -> Result<Keypair> {
        let delegatee = self.funded_payer(lamports).await?;
        self.delegate_payer(funder, &delegatee, validator).await?;
        Ok(delegatee)
    }

    pub async fn delegate_payer(
        &self,
        funder: &Keypair,
        delegatee: &Keypair,
        validator: Pubkey,
    ) -> Result<()> {
        let delegatee_pubkey = delegatee.pubkey();
        let delegate_setup = [
            system::assign(&delegatee_pubkey, &dlp::dlp_id()),
            dlp::delegate_account(&funder.pubkey(), &delegatee_pubkey, &validator),
        ];
        self.submit_and_confirm_with(funder, &[delegatee], &delegate_setup).await?;
        let on_chain = self
            .account(&delegatee_pubkey)
            .await?
            .ok_or("delegated payer account missing after delegation")?;
        if on_chain.owner != dlp::dlp_id() {
            return Err(format!(
                "delegated payer {delegatee_pubkey} owned by {}, expected the \
             delegation program",
                on_chain.owner
            )
            .into());
        }
        Ok(())
    }
}

impl ErCtx {
    pub async fn await_delegated(
        &self,
        account: &Pubkey,
        timeout: Duration,
        what: &str,
    ) -> Result<()> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Status {
            is_delegated: bool,
        }
        check::poll_for(what, timeout, || async {
            let status: Status = self
                .api()
                .call("getDelegationStatus", &(account.to_string(),))
                .await
                .map_err(|error| format!("read failed: {error}"))?;
            if status.is_delegated {
                Ok(())
            } else {
                Err("not delegated".to_owned())
            }
        })
        .await
        .map_err(|error| error.expected("delegated"))?;
        Ok(())
    }

    pub async fn await_clones(
        &self,
        pdas: &[Pubkey],
        space: usize,
        timeout: Duration,
    ) -> Result<()> {
        for pda in pdas {
            check::poll(
                &format!("the ER clones the delegated pda {pda}"),
                timeout,
                || async {
                    matches!(self.account(pda).await, Ok(Some(acc)) if acc.data.len() == space)
                },
            )
            .await?;
        }
        Ok(())
    }

    pub async fn touch(&self, accounts: &[Pubkey]) {
        for window in accounts.chunks(16) {
            join_all(window.iter().map(|account| self.account(account))).await;
        }
    }

    pub async fn prewarm(
        &self,
        accounts: &[Pubkey],
        space: usize,
        timeout: Duration,
    ) -> Result<()> {
        self.touch(accounts).await;
        self.await_clones(accounts, space, timeout).await
    }

    pub async fn await_cloned_payers(&self, payers: &[Pubkey], timeout: Duration) -> Result<()> {
        bounded(payers.len(), "cloned payer", |index| {
            let payer = payers[index];
            async move {
                check::poll(&format!("the ER clones payer {payer}"), timeout, || async {
                    matches!(self.account(&payer).await, Ok(Some(acc)) if acc.lamports > 0)
                })
                .await?;
                Ok(())
            }
        })
        .await?;
        Ok(())
    }

    pub async fn await_program_clone(&self, program: &Pubkey, timeout: Duration) -> Result<()> {
        check::poll(
            &format!("the er clones the program {program} as executable"),
            timeout,
            || async { matches!(self.account(program).await, Ok(Some(clone)) if clone.executable) },
        )
        .await?;
        Ok(())
    }
}
