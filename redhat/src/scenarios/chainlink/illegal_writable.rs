use std::{rc::Rc, time::Duration};

use instruction::Instruction;
use pubkey::Pubkey;
use redshift_interface::schedulecommit::{build, ScheduleCommitType};
use redsuite_core::{
    check, check_eq, dlp, system, BaseCtx, ChainCtx, ErCtx, Result, Scenario, TxSender,
};
use signer::Signer;

use crate::PAYER_LAMPORTS;

const RECEIPT_TIMEOUT: Duration = Duration::from_secs(20);
const PROGRAM_ID_NOT_FOUND: &str = "failed to find parent program id";
const IMMUTABLE: &str = "Account is immutable";
const INVALID_ACCOUNT_OWNER: &str = "Invalid account owner";
const NEEDS_TO_BE_OWNED: &str = "needs to be owned by the invoking program";

pub struct IllegalWritable {
    base: Rc<BaseCtx>,
    er: ErCtx,
    sender: TxSender,
    players: Vec<Pubkey>,
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
        let committees = base.schedulecommit(&funder).init(er.identity(), 2).await?;
        er.await_committees(&committees).await?;
        let players: Vec<_> = committees.iter().map(|c| c.player.pubkey()).collect();
        let pdas: Vec<_> = committees.iter().map(|c| c.pda).collect();

        // A plain (non-delegated) payer for the direct-invocation cells: the
        // committed accounts are the delegated PDAs, so the tx pays no ER fee
        // through the payer itself.
        let sender = er.sender(Rc::new(funder));
        Ok(Self { base, er, sender, players, pdas })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er, sender, players, pdas } = self;
        let payer = sender.payer().pubkey();
        let cases = [
            Case {
                name: "direct invocation",
                instructions: &[build::direct_schedule_commit(payer, None, &pdas)],
                refusals: &[&[PROGRAM_ID_NOT_FOUND]],
            },
            Case {
                name: "surrounding transfers",
                instructions: &[
                    system::transfer(&payer, &pdas[0], 1_000_000),
                    build::direct_schedule_commit(payer, None, &pdas),
                    system::transfer(&payer, &pdas[0], 2_000_000),
                ],
                refusals: &[&[PROGRAM_ID_NOT_FOUND, IMMUTABLE]],
            },
            Case {
                name: "sibling CPIs",
                instructions: &[redhat_interface::build::sibling_schedule_commit_cpis(
                    payer, &players, &pdas,
                )],
                refusals: &[&[INVALID_ACCOUNT_OWNER], &[NEEDS_TO_BE_OWNED]],
            },
            Case {
                name: "malicious tail",
                instructions: &[
                    redhat_interface::build::non_cpi(payer),
                    build::schedule_commit_cpi(
                        payer,
                        players,
                        true,
                        false,
                        ScheduleCommitType::CommitFinalize,
                        true,
                    ),
                    redhat_interface::build::nested_schedule_commit_cpi(payer, &pdas),
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
            let tx = er.api().await_transaction(&signature, RECEIPT_TIMEOUT).await?;
            check!(
                tx.err.is_some(),
                "{case} ({signature}): the attack must fail on-chain, got {tx:?}"
            )?;
            let observed = format!("{}\n{:?}", tx.logs.join("\n"), tx.err.as_ref().unwrap());
            // Every group must match; alternatives within a group are ORed.
            for alternatives in refusals {
                check!(
                    alternatives.iter().any(|needle| observed.contains(needle)),
                    "{case} ({signature}): the refusal must name one of {alternatives:?}, got: {observed}"
                )?;
            }
        }

        // The refused attacks must not have scheduled anything: no base commit
        // moved the PDAs off dlp.
        for pda in &pdas {
            let on_base = base.account(pda).await?.ok_or("the committee pda vanished from base")?;
            check_eq!(
                on_base.owner,
                dlp::dlp_id(),
                "a refused attack must leave the committee delegated"
            )?;
        }

        Ok(())
    }
}
