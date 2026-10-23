use std::{rc::Rc, slice, time::Duration};

use anyhow::{ensure, Context};
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::{transaction_logs, transaction_meta, TxError},
    expect_error, prep,
    programs::{CellHeader, CellId, SettlementMode, UndelegationOutcome, MAGIC, REDSHIFT},
    receipt::{self, CommitReceipt},
    rpc_types::response::TransactionError,
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;
use solana_instruction_error::InstructionError;
use tokio::time;
use tracing::instrument;

use crate::{program::DELEGATION_PROGRAM_ID, LOCKOUT_REJECTIONS, PAYER_LAMPORTS};

const LARGE_ACCOUNT_SPACE: u32 = 2 * 1024;
const ER_WRITE_TIMEOUT: Duration = Duration::from_secs(20);
const REDELEGATE_SETTLE: Duration = Duration::from_secs(2);
const ROLLBACK_SETTLE: Duration = Duration::from_millis(1500);
const MOD_AFTER_REFUSAL: &str = "instruction modified data of an account it does not own";
const TWICE_REFUSAL: &str = "is required to be writable and delegated in order to be undelegated";

/// Checks settlement snapshots, ownership transitions, lockout, and transaction rollback.
pub struct SettlementLifecycle {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

fn decoded_count(data: &[u8]) -> Result<u64> {
    Ok(CellHeader::try_decode(data)?.value)
}

impl Scenario for SettlementLifecycle {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        crate::scenarios::chainlink::settlement_clone_controls::run(
            self.base.clone(),
            self.er.clone(),
        )
        .await?;
        tokio::try_join!(
            self.base_requested_undelegation(),
            self.test_lifecycle_cell(2),
            self.test_foreign_ownership(),
            self.large_account_cell(SettlementMode::Commit),
            self.large_account_cell(SettlementMode::Undelegate),
            self.test_mod_after_rejection(2),
            self.test_twice_rejection(),
            self.test_failed_undelegation_lockout(),
        )?;
        Ok(())
    }
}

impl SettlementLifecycle {
    #[instrument(skip_all, err)]
    async fn base_requested_undelegation(&self) -> Result<()> {
        let payer = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let cells = self.base.cells(REDSHIFT, &payer).create_owned(self.er.identity(), 1).await?;
        self.er.await_cells(&cells).await?;
        let cell = &cells[0];
        let request = sdk::pda::undelegation_request_pda_from_delegated_account(&cell.address);
        ensure!(self.base.account(&request).await?.is_none());
        self.er
            .submit_and_confirm(&payer, &[REDSHIFT.batch().add(&[cell.address], 1).build()])
            .await?;
        let snapshot = self.er.account(&cell.address).await?.context("missing ER cell")?;
        ensure!(decoded_count(&snapshot.data)? == 1);
        ensure!(
            decoded_count(
                &self.base.account(&cell.address).await?.context("missing base cell")?.data
            )? == 0
        );
        self.base
            .submit_and_confirm(
                &payer,
                &[REDSHIFT
                    .request_undelegation(payer.pubkey(), CellId::new(cell.authority.pubkey(), 0))],
            )
            .await?;
        self.base
            .api()
            .await_accounts(
                &[cell.address, request],
                "base undelegation and request cleanup",
                |accounts| {
                    let account = accounts[0].as_ref().context("missing cell")?;
                    ensure!(account.owner == redshift_interface::ID);
                    ensure!(account.data == snapshot.data);
                    ensure!(account.lamports == snapshot.lamports);
                    ensure!(accounts[1].is_none());
                    Ok(())
                },
            )
            .await?;
        Ok(())
    }

    async fn settled(
        &self,
        pdas: &[Pubkey],
        signature: &Signature,
        mode: SettlementMode,
        count: u64,
    ) -> Result<CommitReceipt> {
        let undelegates = matches!(mode, SettlementMode::Undelegate);
        let owner = if undelegates { redshift_interface::id() } else { DELEGATION_PROGRAM_ID };
        let commit_receipt =
            self.er.check_commit(self.base.as_ref(), signature, pdas, undelegates).await?;
        if !commit_receipt.failure_is_duplicate_rejection() {
            ensure!(commit_receipt.base_signatures.len() == 1);
        }
        for pda in pdas {
            self.base
                .api()
                .await_accounts(slice::from_ref(pda), "base settlement", |accounts| {
                    let account = accounts[0].as_ref().context("settled cell absent from base")?;
                    ensure!(account.owner == owner);
                    ensure!(decoded_count(&account.data)? == count);
                    Ok(())
                })
                .await?;
            let on_er = self
                .er
                .account(pda)
                .await?
                .context("the er clone is not present after the commit")?;
            ensure!(decoded_count(&on_er.data)? == count);
        }
        Ok(commit_receipt)
    }

    async fn commit_undelegate_lifecycle(
        &self,
        payer: &Keypair,
        er_fee_payer: &Keypair,
        base_negative_payer: &Keypair,
        count: usize,
    ) -> Result<()> {
        let cells =
            self.base.cells(REDSHIFT, payer).create_owned(self.er.identity(), count).await?;
        self.er.await_cells(&cells).await?;
        let pdas: Vec<_> = cells.iter().map(|c| c.address).collect();
        let plain = self.commit(payer, &pdas, SettlementMode::Commit).await?;
        self.settled(&pdas, &plain, SettlementMode::Commit, 1).await?;

        let signature = self.commit(payer, &pdas, SettlementMode::Undelegate).await?;

        for &pda in &pdas {
            let attempt = self
                .er
                .submit_and_confirm(er_fee_payer, &[REDSHIFT.batch().add(&[pda], 1).build()])
                .await;
            let error = expect_error(attempt, "ER write rejected after undelegation request")?;
            crate::rejection_code("the write", &LOCKOUT_REJECTIONS, &format!("{error:#}"))?;
        }

        self.settled(&pdas, &signature, SettlementMode::Undelegate, 2).await?;

        for &pda in &pdas {
            self.base
                .submit_and_confirm(payer, &[REDSHIFT.batch().add(&[pda], 1).build()])
                .await?;
        }
        for pda in &pdas {
            let on_base = self
                .base
                .account(pda)
                .await?
                .context("the pda is not on base after the chain write")?;
            ensure!(decoded_count(&on_base.data)? == 3);
        }

        time::sleep(REDELEGATE_SETTLE).await;
        let redelegate: Vec<_> = cells
            .iter()
            .map(|cell| {
                REDSHIFT.delegate(
                    payer.pubkey(),
                    CellId::new(cell.authority.pubkey(), 0),
                    Some(self.er.identity()),
                    prep::COMMIT_FREQUENCY_MS,
                )
            })
            .collect();
        let signers: Vec<_> = cells.iter().map(|cell| &cell.authority).collect();
        self.base.submit_and_confirm_with(payer, &signers, &redelegate).await?;
        for pda in &pdas {
            let on_base = self
                .base
                .account(pda)
                .await?
                .context("the pda is not on base after the redelegation")?;
            ensure!(on_base.owner == DELEGATION_PROGRAM_ID);
        }

        for &pda in &pdas {
            let attempt = self
                .base
                .submit_and_confirm(
                    base_negative_payer,
                    &[REDSHIFT.batch().add(&[pda], 1).build()],
                )
                .await;
            let error = expect_error(attempt, "base write rejected after redelegation")?;
            crate::rejection_code("the write", &LOCKOUT_REJECTIONS, &format!("{error:#}"))?;
        }

        for cell in &cells {
            wait::retry(
                "ER writes after redelegation",
                ER_WRITE_TIMEOUT,
                async || {
                    self.er
                        .submit_and_confirm(
                            er_fee_payer,
                            &[REDSHIFT.batch().add(&[cell.address], 1).build()],
                        )
                        .await
                },
            )
            .await
            .with_context(|| format!("writing redelegated cell {}", cell.address))?;
            let on_er = self
                .er
                .account(&cell.address)
                .await?
                .context("the er clone is not present after the redelegation")?;
            ensure!(decoded_count(&on_er.data)? == 4);
        }

        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn large_account_cell(&self, commit_type: SettlementMode) -> Result<()> {
        let undelegates = matches!(commit_type, SettlementMode::Undelegate);
        let payer = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let cells = self
            .base
            .cells(REDSHIFT, &payer)
            .create_owned_sized(self.er.identity(), 1, LARGE_ACCOUNT_SPACE)
            .await?;
        let cell = &cells[0];
        let pda = cell.address;
        let on_base = self
            .base
            .account(&pda)
            .await?
            .context("large account missing after delegation")?;
        ensure!(on_base.data.len() == (LARGE_ACCOUNT_SPACE as usize));
        self.er.await_clones(&[pda], LARGE_ACCOUNT_SPACE as usize).await?;

        let seed = u64::from(undelegates) + 1;
        let mut expected = on_base.data;
        CellHeader::fill_payload(&mut expected, seed)?;
        let mut header = CellHeader::try_decode(&expected)?;
        header.revision += 1;
        header.write(&mut expected)?;
        let signature = self
            .er
            .submit_and_confirm(
                &payer,
                &[
                    REDSHIFT.batch().fill(&[pda], seed, 1).require_signer(payer.pubkey()).build(),
                    REDSHIFT.batch().settle(payer.pubkey(), &[pda], commit_type).build(),
                ],
            )
            .await?;
        self.er
            .check_commit(self.base.as_ref(), &signature, &[pda], undelegates)
            .await?;
        let on_er = self.er.account(&pda).await?.context("large account missing after commit")?;
        ensure!(on_er.data == expected);
        let expected_owner =
            if undelegates { redshift_interface::id() } else { DELEGATION_PROGRAM_ID };
        self.base
            .api()
            .await_accounts(
                &[pda],
                "large-account data and ownership settle on base",
                |accounts| {
                    let account = accounts[0].as_ref().context("base account missing")?;
                    ensure!(account.owner == expected_owner);
                    ensure!(account.data == expected);
                    Ok(())
                },
            )
            .await
    }

    async fn test_lifecycle_cell(&self, count: usize) -> Result<()> {
        let payer = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let er_fee_payer =
            self.base.delegated_payer(&payer, self.er.identity(), PAYER_LAMPORTS).await?;
        let base_negative_payer = self.base.funded_payer(PAYER_LAMPORTS).await?;

        self.commit_undelegate_lifecycle(&payer, &er_fee_payer, &base_negative_payer, count)
            .await
    }

    async fn test_foreign_ownership(&self) -> Result<()> {
        let payer = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let outsider =
            self.base.delegated_payer(&payer, self.er.identity(), PAYER_LAMPORTS).await?;
        let sender = self.er.sender(Rc::new(outsider));
        let outsider = sender.payer().pubkey();
        let validator = Keypair::new().pubkey();
        for (commit_type, what) in
            [(SettlementMode::Commit, "commit"), (SettlementMode::Undelegate, "undelegation")]
        {
            let cells = self.base.cells(REDSHIFT, &payer).create_owned(validator, 1).await?;
            self.er.await_cells(&cells).await?;
            let tx = sender
                .prepare(&[REDSHIFT
                    .batch()
                    .settle(outsider, &[cells[0].address, outsider], commit_type)
                    .build()])
                .await?;
            let before = self.er.account(&cells[0].address).await?;
            let signature = self.er.api().send_transaction(&tx).await?;
            let outcome = self.er.api().try_confirm(&signature).await?;
            let expected = match commit_type {
                SettlementMode::Commit => InstructionError::IllegalOwner,
                SettlementMode::Undelegate => InstructionError::ReadonlyDataModified,
            };
            ensure!(
                outcome
                    == Err(TxError {
                        err: TransactionError::InstructionError(0, expected)
                    }),
                "foreign {what}: {outcome:?}"
            );
            ensure!(self.er.account(&cells[0].address).await? == before);
        }
        Ok(())
    }

    async fn test_mod_after_rejection(&self, count: usize) -> Result<()> {
        let payer = Rc::new(self.base.funded_payer(PAYER_LAMPORTS).await?);
        let cells = self
            .base
            .cells(REDSHIFT, &payer)
            .create_owned(self.er.identity(), count)
            .await?;
        self.er.await_cells(&cells).await?;
        let addresses: Vec<_> = cells.iter().map(|cell| cell.address).collect();
        self.rejected_intent_cell(
            &payer,
            REDSHIFT
                .batch()
                .settle(payer.pubkey(), &addresses, SettlementMode::Undelegate)
                .add(&addresses, 1)
                .build(),
            MOD_AFTER_REFUSAL,
        )
        .await?;
        for cell in &cells {
            let on_er = self
                .er
                .account(&cell.address)
                .await?
                .context("the er clone is gone after the failed tx")?;
            ensure!(decoded_count(&on_er.data)? == 0);
        }
        Ok(())
    }

    async fn test_twice_rejection(&self) -> Result<()> {
        let payer = Rc::new(self.base.funded_payer(PAYER_LAMPORTS).await?);
        let cells = self.base.cells(REDSHIFT, &payer).create_owned(self.er.identity(), 2).await?;
        self.er.await_cells(&cells).await?;
        let addresses: Vec<_> = cells.iter().map(|cell| cell.address).collect();
        let signers: Vec<_> =
            cells.iter().map(|cell| CellId::new(cell.authority.pubkey(), 0)).collect();
        let mut repeated =
            MAGIC.commit(payer.pubkey(), None, &addresses, SettlementMode::Undelegate);
        for meta in repeated.accounts.iter_mut().skip(2) {
            meta.is_signer = true;
        }
        let instruction = REDSHIFT
            .batch()
            .settle(payer.pubkey(), &addresses, SettlementMode::Undelegate)
            .invoke_signed(repeated, &signers)
            .build();
        self.rejected_intent_cell(&payer, instruction, TWICE_REFUSAL).await
    }

    async fn test_failed_undelegation_lockout(&self) -> Result<()> {
        let payer = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let er_fee_payer =
            self.base.delegated_payer(&payer, self.er.identity(), PAYER_LAMPORTS).await?;

        let cells = self.base.cells(REDSHIFT, &payer).create_owned(self.er.identity(), 1).await?;
        self.er.await_cells(&cells).await?;
        let pda = cells[0].address;

        self.er
            .submit_and_confirm(
                &payer,
                &[REDSHIFT.batch().undelegation(&[pda], UndelegationOutcome::Reject).build()],
            )
            .await?;
        let on_er = self
            .er
            .account(&pda)
            .await?
            .context("ER cell missing after setting rejection policy")?;
        ensure!(CellHeader::try_decode(&on_er.data)?.undelegation == UndelegationOutcome::Reject);

        let signature = self
            .er
            .submit_and_confirm(
                &payer,
                &[REDSHIFT
                    .batch()
                    .settle(payer.pubkey(), &[pda], SettlementMode::Undelegate)
                    .build()],
            )
            .await?;
        self.er.check_commit(self.base.as_ref(), &signature, &[pda], true).await?;

        self.base
            .api()
            .await_accounts(&[pda], "the rejection policy settles on base", |accounts| {
                ensure!(matches!(
                    accounts[0].as_ref(),
                    Some(acc)
                        if CellHeader::try_decode(&acc.data).ok().map(|cell| cell.undelegation)
                            == Some(UndelegationOutcome::Reject)
                ));
                Ok(())
            })
            .await?;
        let on_base = self
            .base
            .account(&pda)
            .await?
            .context("the pda is not on base after the patched commit")?;
        ensure!(on_base.owner == DELEGATION_PROGRAM_ID);

        let attempt = self
            .er
            .submit_and_confirm(&er_fee_payer, &[REDSHIFT.batch().set(&[pda], 2222).build()])
            .await;
        let error = expect_error(attempt, "ER lockout survives failed base undelegation")?;
        crate::rejection_code(
            "the write after the failed undelegation",
            &LOCKOUT_REJECTIONS,
            &format!("{error:#}"),
        )?;
        Ok(())
    }

    async fn commit(
        &self,
        payer: &Keypair,
        cells: &[Pubkey],
        mode: SettlementMode,
    ) -> Result<Signature> {
        self.er
            .submit_and_confirm(
                payer,
                &[REDSHIFT.batch().add(cells, 1).settle(payer.pubkey(), cells, mode).build()],
            )
            .await
    }

    async fn rejected_intent_cell(
        &self,
        payer: &Rc<Keypair>,
        instruction: Instruction,
        refusal: &str,
    ) -> Result<()> {
        let sender = self.er.sender(payer.clone());
        let signature = sender.submit(&[instruction]).await?;
        let tx = self.er.api().await_transaction(&signature).await?;
        let meta = transaction_meta(&tx)?;
        let logs = transaction_logs(meta);
        let error = meta.err.as_ref().context("expected ER rejection")?;
        let observed = format!("{}\n{error:?}", logs.join("\n"));
        ensure!(
            observed.contains(refusal),
            "expected {refusal:?}, observed: {observed}"
        );

        let sent_signature = receipt::receipt_signature_in_logs(logs)
            .context("the failed tx logs carry no ScheduledCommitSent signature")?;
        time::sleep(ROLLBACK_SETTLE).await;
        ensure!(self.er.api().get_transaction(&sent_signature).await?.is_none());
        Ok(())
    }
}
