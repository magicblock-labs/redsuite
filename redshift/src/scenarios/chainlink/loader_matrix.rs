use std::time::{Duration, Instant};

use async_trait::async_trait;
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, loader_v4, prep, topology, BaseCtx, ChainCtx, CheckError,
    ErCtx, Result, Scenario, ScenarioReport,
};
use signature::Signature;

const MEMO_V1_ID: &str = "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo";
const MEMO_V2_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

const PAYER_DELEGATION: u64 = 1_000_000_000;
const INVOKE_TIMEOUT: Duration = Duration::from_secs(30);
// The engine reports a confirmed status one blocktime before the ledger
// serves the transaction, so a lookup right after confirm must re-poll.
const LEDGER_VISIBILITY_TIMEOUT: Duration = Duration::from_millis(250);
const LEDGER_VISIBILITY_POLL: Duration = Duration::from_millis(10);
const LOADER_V3_PROGRAMDATA_METADATA: usize = 45;

pub struct LoaderMatrix;

#[async_trait(?Send)]
impl Scenario for LoaderMatrix {
    fn name(&self) -> &str {
        "redshift/loader_matrix"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let payer = &prep::delegated_payer(
            base,
            &funder,
            er.identity(),
            PAYER_DELEGATION,
        )
        .await?;

        let memo_v1: Pubkey = MEMO_V1_ID.parse()?;
        let memo_v2: Pubkey = MEMO_V2_ID.parse()?;

        let base_memo_v1 = base
            .account(&memo_v1)
            .await?
            .ok_or("memo v1 is not on the base chain")?;
        invoke_until(er, payer, memo_v1, &memo_data, vec![], |logs| {
            program_succeeded(logs, &memo_v1)
        })
        .await?;
        let (v1_owner, v1_data) = cloned_program(er, &memo_v1).await?;
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

        let base_memo_v2 = base
            .account(&memo_v2)
            .await?
            .ok_or("memo v2 is not on the base chain")?;
        invoke_until(er, payer, memo_v2, &memo_data, vec![], has_memo).await?;
        let (v2_owner, v2_data) = cloned_program(er, &memo_v2).await?;
        check_eq!(
            v2_owner,
            loader_v4::loader_v4_id(),
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
        let v3_logs =
            invoke_until(er, payer, v3_id, &log_data, vec![], has_log_msg)
                .await?;
        let v3_programdata = Pubkey::find_program_address(
            &[v3_id.as_ref()],
            &sdk_ids::bpf_loader_upgradeable::ID,
        )
        .0;
        let mut base_v3_programdata = base
            .account(&v3_programdata)
            .await?
            .ok_or("the v3 programdata account is not on the base chain")?
            .data;
        let base_v3_elf =
            base_v3_programdata.split_off(LOADER_V3_PROGRAMDATA_METADATA);
        let (v3_owner, v3_data) = cloned_program(er, &v3_id).await?;
        check_eq!(
            v3_owner,
            loader_v4::loader_v4_id(),
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

        Ok(ScenarioReport::ok(self.name())
            .setting("loaders", "v1,v2,v3")
            .setting(
                "clone representation",
                "bare ELF, no LoaderV4State header",
            )
            .setting("v1 owner", v1_owner.to_string()))
    }
}

fn memo_data(attempt: u64) -> Vec<u8> {
    format!("redsuite memo {attempt}").into_bytes()
}

fn log_data(attempt: u64) -> Vec<u8> {
    redshift_interface::log_msg_data(&format!("probe {attempt}"))
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

async fn cloned_program(
    er: &ErCtx,
    program: &Pubkey,
) -> Result<(Pubkey, Vec<u8>)> {
    let cloned = er
        .account(program)
        .await?
        .ok_or_else(|| CheckError::new("program not present in the ER"))?;
    Ok((cloned.owner, cloned.data))
}

async fn confirmed_logs(
    er: &ErCtx,
    signature: &Signature,
) -> Result<Vec<String>> {
    let deadline = Instant::now() + LEDGER_VISIBILITY_TIMEOUT;
    loop {
        if let Some(info) = er.api().get_transaction(signature).await? {
            return Ok(info.logs);
        }
        if Instant::now() >= deadline {
            return Ok(Vec::new());
        }
        tokio::time::sleep(LEDGER_VISIBILITY_POLL).await;
    }
}

async fn invoke_until(
    er: &ErCtx,
    payer: &Keypair,
    program: Pubkey,
    data_for: &dyn Fn(u64) -> Vec<u8>,
    accounts: Vec<AccountMeta>,
    accept: impl Fn(&[String]) -> bool,
) -> Result<Vec<String>> {
    let deadline = Instant::now() + INVOKE_TIMEOUT;
    let mut attempt = 0u64;
    let mut last_logs = Vec::new();
    loop {
        attempt += 1;
        let ix = Instruction {
            program_id: program,
            accounts: accounts.clone(),
            data: data_for(attempt),
        };
        match er
            .submit_and_confirm(payer, std::slice::from_ref(&ix))
            .await
        {
            Ok(signature) => {
                let logs = confirmed_logs(er, &signature).await?;
                if accept(&logs) {
                    return Ok(logs);
                }
                last_logs = logs;
            }
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(CheckError::new(format!(
                        "invoke of {program} never succeeded"
                    ))
                    .caused_by(err)
                    .into());
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
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
