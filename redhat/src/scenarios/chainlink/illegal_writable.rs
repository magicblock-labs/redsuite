use std::{rc::Rc, time::Duration};

use async_trait::async_trait;
use instruction::Instruction;
use keypair::Keypair;
use redshift_program::schedulecommit::{build, ScheduleCommitType};
use redsuite_core::{
    check, check_eq, prep, system, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
    ScenarioReport,
};
use signer::Signer;

const RECEIPT_TIMEOUT: Duration = Duration::from_secs(20);
const PROGRAM_ID_NOT_FOUND: &str = "failed to find parent program id";
const INVALID_ACCOUNT_OWNER: &str = "Invalid account owner";
const NEEDS_TO_BE_OWNED: &str = "needs to be owned by the invoking program";

pub struct IllegalWritable;

// The attack tx fails on-chain; send without confirming and read the recorded
// error and logs back from the ledger.
async fn refused(
    er: &ErCtx,
    payer: &Rc<Keypair>,
    instructions: &[Instruction],
    needles: &[&str],
) -> Result<()> {
    let sender = er.sender(payer.clone());
    let signature = sender.send(instructions).await?;
    let tx = er
        .api()
        .await_transaction(&signature, RECEIPT_TIMEOUT)
        .await?;
    check!(
        tx.err.is_some(),
        "the attack must fail on-chain, got {tx:?}"
    )?;
    let observed =
        format!("{}\n{:?}", tx.logs.join("\n"), tx.err.as_ref().unwrap());
    for needle in needles {
        check!(
            observed.contains(needle),
            "the refusal must name '{needle}', got: {observed}"
        )?;
    }
    Ok(())
}

#[async_trait(?Send)]
impl Scenario for IllegalWritable {
    fn name(&self) -> &str {
        "redhat/illegal_writable"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let committees =
            crate::init_delegated_committees(base, &funder, er.identity(), 2)
                .await?;
        crate::await_committee_clones(er, &committees).await?;
        let players: Vec<_> =
            committees.iter().map(|c| c.player.pubkey()).collect();
        let pdas: Vec<_> = committees.iter().map(|c| c.pda).collect();

        // A plain (non-delegated) payer for the direct-invocation cells: the
        // committed accounts are the delegated PDAs, so the tx pays no ER fee
        // through the payer itself.
        let payer = Rc::new(funder.insecure_clone());

        // Test 1 — commit the PDAs directly via the magic program. No CPI
        // parent owns them, so the pipeline cannot find the invoking program.
        refused(
            er,
            &payer,
            &[build::direct_schedule_commit(payer.pubkey(), None, &pdas)],
            &[PROGRAM_ID_NOT_FOUND],
        )
        .await?;

        // Test 3 — the same direct commit sandwiched between two transfers to
        // one PDA, an attempt to confuse the parent-program detection.
        refused(
            er,
            &payer,
            &[
                system::transfer(&payer.pubkey(), &pdas[0], 1_000_000),
                build::direct_schedule_commit(payer.pubkey(), None, &pdas),
                system::transfer(&payer.pubkey(), &pdas[0], 2_000_000),
            ],
            &[PROGRAM_ID_NOT_FOUND],
        )
        .await?;

        // Test 4 — a program that does not own the PDAs commits them twice:
        // once via the owning program (legitimate) and once directly. The
        // direct half fails the ownership check.
        refused(
            er,
            &payer,
            &[redhat_program::build::sibling_schedule_commit_cpis(
                payer.pubkey(),
                &players,
                &pdas,
            )],
            &[INVALID_ACCOUNT_OWNER, NEEDS_TO_BE_OWNED],
        )
        .await?;

        // Test 5 — a no-op, then a legitimate commit via the owning program,
        // then the malicious direct commit. The malicious tail still fails the
        // ownership check and fails the whole transaction.
        refused(
            er,
            &payer,
            &[
                redhat_program::build::non_cpi(payer.pubkey()),
                build::schedule_commit_cpi(
                    payer.pubkey(),
                    players.clone(),
                    true,
                    false,
                    ScheduleCommitType::CommitFinalize,
                    true,
                ),
                redhat_program::build::nested_schedule_commit_cpi(
                    payer.pubkey(),
                    &players,
                    &pdas,
                ),
            ],
            &[INVALID_ACCOUNT_OWNER, NEEDS_TO_BE_OWNED],
        )
        .await?;

        // The refused attacks must not have scheduled anything: no base commit
        // moved the PDAs off dlp.
        for pda in &pdas {
            let on_base = base
                .account(pda)
                .await?
                .ok_or("the committee pda vanished from base")?;
            check_eq!(
                on_base.owner,
                crate::DELEGATION_PROGRAM_ID,
                "a refused attack must leave the committee delegated"
            )?;
        }

        Ok(ScenarioReport::ok(self.name())
            .setting("committees", pdas.len())
            .setting("direct refusal", PROGRAM_ID_NOT_FOUND)
            .setting("malicious cpi refusal", NEEDS_TO_BE_OWNED))
    }
}
