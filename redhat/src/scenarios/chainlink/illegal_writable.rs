use std::rc::Rc;

use anyhow::{ensure, Context};
use instruction::Instruction;
use pubkey::Pubkey;

use redsuite_core::{
    api::{transaction_logs, transaction_meta},
    dlp,
    programs::{SettlementMode, MAGIC, REDHAT, REDSHIFT},
    system, BaseCtx, ChainCtx, ErCtx, Result, Scenario, TxSender,
};
use signer::Signer;

use crate::PAYER_LAMPORTS;

const PROGRAM_ID_NOT_FOUND: &str = "failed to find parent program id";
const IMMUTABLE: &str = "Account is immutable";
const INVALID_ACCOUNT_OWNER: &str = "Invalid account owner";
const NEEDS_TO_BE_OWNED: &str = "needs to be owned by the invoking program";

pub struct IllegalWritable {
    base: Rc<BaseCtx>,
    er: ErCtx,
    sender: TxSender,
    pdas: Vec<Pubkey>,
}

struct Case<'a> {
    name: &'a str,
    instructions: &'a [Instruction],
    refusals: &'a [&'a [&'a str]],
}

impl Scenario for IllegalWritable {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let funder = base.funded_payer(PAYER_LAMPORTS).await?;
        let cells = base.cells(REDSHIFT, &funder).create_owned(er.identity(), 2).await?;
        er.await_cells(&cells).await?;
        let pdas: Vec<_> = cells.iter().map(|c| c.address).collect();

        // A plain (non-delegated) payer for the direct-invocation cells: the
        // committed accounts are the delegated PDAs, so the tx pays no ER fee
        // through the payer itself.
        let sender = er.sender(Rc::new(funder));
        Ok(Self { base, er, sender, pdas })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er, sender, pdas } = self;
        let payer = sender.payer().pubkey();
        let cases = [
            Case {
                name: "direct invocation",
                instructions: &[MAGIC.commit(payer, None, &pdas, SettlementMode::Commit)],
                refusals: &[&[PROGRAM_ID_NOT_FOUND]],
            },
            Case {
                name: "surrounding transfers",
                instructions: &[
                    system::transfer(&payer, &pdas[0], 1_000_000),
                    MAGIC.commit(payer, None, &pdas, SettlementMode::Commit),
                    system::transfer(&payer, &pdas[0], 2_000_000),
                ],
                refusals: &[&[PROGRAM_ID_NOT_FOUND, IMMUTABLE]],
            },
            Case {
                name: "sibling CPIs",
                instructions: &[REDHAT
                    .batch()
                    .invoke(REDSHIFT.settle(payer, &pdas, SettlementMode::Commit))
                    .invoke(MAGIC.commit(payer, None, &pdas, SettlementMode::Commit))
                    .build()],
                refusals: &[&[INVALID_ACCOUNT_OWNER], &[NEEDS_TO_BE_OWNED]],
            },
            Case {
                name: "malicious tail",
                instructions: &[
                    REDHAT.batch().build(),
                    REDSHIFT
                        .batch()
                        .add(&pdas, 1)
                        .settle(payer, &pdas, SettlementMode::Commit)
                        .build(),
                    REDHAT
                        .batch()
                        .invoke(MAGIC.commit(payer, None, &pdas, SettlementMode::Commit))
                        .build(),
                ],
                refusals: &[&[INVALID_ACCOUNT_OWNER], &[NEEDS_TO_BE_OWNED]],
            },
        ];
        for Case {
            name: case,
            instructions,
            refusals,
        } in cases
        {
            let signature = sender.submit(instructions).await?;
            let tx = er.api().await_transaction(&signature).await?;
            let error = transaction_meta(&tx)?
                .err
                .as_ref()
                .with_context(|| format!("{case}: attack unexpectedly succeeded"))?;
            let observed = format!(
                "{}\n{error:?}",
                transaction_logs(transaction_meta(&tx)?).join("\n")
            );
            // Every group must match; alternatives within a group are ORed.
            for alternatives in refusals {
                ensure!(alternatives.iter().any(|needle| observed.contains(needle)));
            }
        }

        // The refused attacks must not have scheduled anything: no base commit
        // moved the PDAs off dlp.
        for pda in &pdas {
            let on_base = base.account(pda).await?.context("the cell pda vanished from base")?;
            ensure!(on_base.owner == dlp::dlp_id());
        }

        Ok(())
    }
}
