use std::{
    rc::Rc,
    slice,
    time::{Duration, Instant},
};

use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, programs::Redshift, topology, BaseCtx, ChainCtx, CheckError, ErCtx, Result,
    Scenario,
};
use signature::Signature;
use tokio::time;

use crate::PAYER_LAMPORTS;

const MEMO_V1_ID: Pubkey = Pubkey::from_str_const("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");
const MEMO_V2_ID: Pubkey = Pubkey::from_str_const("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

const PAYER_DELEGATION: u64 = 1_000_000_000;
const INVOKE_TIMEOUT: Duration = Duration::from_secs(30);
// The engine reports a confirmed status one blocktime before the ledger
// serves the transaction, so a lookup right after confirm must re-poll.
const LEDGER_VISIBILITY_TIMEOUT: Duration = Duration::from_millis(250);
const LEDGER_VISIBILITY_POLL: Duration = Duration::from_millis(10);
const LOADER_V3_PROGRAMDATA_METADATA: usize = 45;

pub struct LoaderMatrix {
    base: Rc<BaseCtx>,
    er: ErCtx,
    payer: Keypair,
}

impl Scenario for LoaderMatrix {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let funder = base.funded_payer(PAYER_LAMPORTS).await?;
        let payer = base.delegated_payer(&funder, er.identity(), PAYER_DELEGATION).await?;

        Ok(Self { base, er, payer })
    }

    async fn run(self) -> Result<()> {
        let Self { base, er, payer } = self;
        let payer = &payer;
        let memo_v1 = MEMO_V1_ID;
        let memo_v2 = MEMO_V2_ID;

        let base_memo_v1 =
            base.account(&memo_v1).await?.ok_or("memo v1 is not on the base chain")?;
        invoke_until(
            &er,
            payer,
            |attempt| memo_instruction(memo_v1, attempt),
            |logs| program_succeeded(logs, &memo_v1),
        )
        .await?;
        let (v1_owner, v1_data) = cloned_program(&er, &memo_v1).await?;
        check_eq!(
            v1_owner,
            sdk_ids::bpf_loader_deprecated::ID,
            "the cloned memo v1 must keep LoaderV1 ownership"
        )?;
        check!(
            v1_data == base_memo_v1.data,
            "the memo v1 clone must byte-equal the base program account \
             (er {} bytes, base {} bytes)",
            v1_data.len(),
            base_memo_v1.data.len()
        )?;

        let base_memo_v2 =
            base.account(&memo_v2).await?.ok_or("memo v2 is not on the base chain")?;
        invoke_until(
            &er,
            payer,
            |attempt| memo_instruction(memo_v2, attempt),
            has_memo,
        )
        .await?;
        let (v2_owner, v2_data) = cloned_program(&er, &memo_v2).await?;
        check_eq!(
            v2_owner,
            sdk_ids::loader_v4::ID,
            "the cloned memo v2 must land under LoaderV4 ownership"
        )?;
        check!(
            v2_data == base_memo_v2.data,
            "the memo v2 clone must byte-equal the base program account \
             (er {} bytes, base {} bytes)",
            v2_data.len(),
            base_memo_v2.data.len()
        )?;

        let (v3_id, _) = topology::redshift_loader_v3_target();
        let v3_logs = invoke_until(
            &er,
            payer,
            |attempt| Redshift::at(v3_id).log(&format!("probe {attempt}")),
            has_log_msg,
        )
        .await?;
        let v3_programdata =
            Pubkey::find_program_address(&[v3_id.as_ref()], &sdk_ids::bpf_loader_upgradeable::ID).0;
        let mut base_v3_programdata = base
            .account(&v3_programdata)
            .await?
            .ok_or("the v3 programdata account is not on the base chain")?
            .data;
        let base_v3_elf = base_v3_programdata.split_off(LOADER_V3_PROGRAMDATA_METADATA);
        let (v3_owner, v3_data) = cloned_program(&er, &v3_id).await?;
        check_eq!(
            v3_owner,
            sdk_ids::loader_v4::ID,
            "the cloned v3 program must land under LoaderV4 ownership"
        )?;
        check!(
            v3_data == base_v3_elf,
            "the v3 clone must byte-equal the base programdata ELF \
             (er {} bytes, base {} bytes)",
            v3_data.len(),
            base_v3_elf.len()
        )?;
        check!(
            v3_logs.iter().any(|line| line.contains("LogMsg:")),
            "the v3 program invocation must emit its LogMsg line"
        )?;

        Ok(())
    }
}

fn memo_instruction(program_id: Pubkey, attempt: u64) -> Instruction {
    Instruction::new_with_bytes(
        program_id,
        format!("redsuite memo {attempt}").as_bytes(),
        vec![],
    )
}

fn has_log_msg(logs: &[String]) -> bool {
    logs.iter().any(|line| line.contains("LogMsg: probe"))
}

fn has_memo(logs: &[String]) -> bool {
    logs.iter().any(|line| line.contains("redsuite memo"))
}

fn program_succeeded(logs: &[String], program: &Pubkey) -> bool {
    let success_line = format!("Program {program} success");
    logs.iter().any(|line| line == &success_line)
}

async fn cloned_program(er: &ErCtx, program: &Pubkey) -> Result<(Pubkey, Vec<u8>)> {
    let cloned = er
        .account(program)
        .await?
        .ok_or_else(|| CheckError::new("program not present in the ER"))?;
    Ok((cloned.owner, cloned.data))
}

async fn confirmed_logs(er: &ErCtx, signature: &Signature) -> Result<Vec<String>> {
    Ok(redsuite_core::check::poll_until(
        LEDGER_VISIBILITY_TIMEOUT,
        LEDGER_VISIBILITY_POLL,
        async || er.api().get_transaction(signature).await,
    )
    .await?
    .map(|info| info.logs)
    .unwrap_or_default())
}

async fn invoke_until<Build, Accept>(
    er: &ErCtx,
    payer: &Keypair,
    build: Build,
    accept: Accept,
) -> Result<Vec<String>>
where
    Build: Fn(u64) -> Instruction,
    Accept: Fn(&[String]) -> bool,
{
    let deadline = Instant::now() + INVOKE_TIMEOUT;
    let mut attempt = 0u64;
    let mut last_logs = Vec::new();
    loop {
        attempt += 1;
        let ix = build(attempt);
        let program = ix.program_id;
        match er.submit_and_confirm(payer, slice::from_ref(&ix)).await {
            Ok(signature) => {
                let logs = confirmed_logs(er, &signature).await?;
                if accept(&logs) {
                    return Ok(logs);
                }
                last_logs = logs;
            }
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(
                        CheckError::new(format!("invoke of {program} never succeeded"))
                            .caused_by(err)
                            .into(),
                    );
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(CheckError::new(format!(
                "invoke of {program} never met its log condition"
            ))
            .actual(format!("{last_logs:?}"))
            .into());
        }
        time::sleep(Duration::from_millis(500)).await;
    }
}
