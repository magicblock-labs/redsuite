use std::time::{Duration, Instant};

use async_trait::async_trait;
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::report::Unit;
use redsuite_core::{
    catalog::Fixture, check, check_eq, loader_v4, manifest, prep, topology,
    BaseCtx, ChainCtx, CheckError, ErCtx, Result, Scenario, ScenarioReport,
};
use signer::Signer;

const MEMO_V1_ID: &str = "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo";
const MEMO_V2_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

const PAYER_DELEGATION: u64 = 1_000_000_000;
const V4_AUTHORITY_FUNDING: u64 = 15_000_000_000;
const INVOKE_TIMEOUT: Duration = Duration::from_secs(30);
const UPGRADE_TIMEOUT: Duration = Duration::from_secs(90);
const LOADER_V3_PROGRAMDATA_METADATA: usize = 45;
const LOADER_V4_HEADER: usize = 48;

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

        let base_bytes =
            std::fs::read(manifest::resolve(Fixture::RedshiftProgramSlim)?)
                .map_err(|err| {
                    format!("reading the slim redshift .so: {err}")
                })?;
        let upgraded_bytes = std::fs::read(manifest::resolve(
            Fixture::RedshiftProgramSlimUpgraded,
        )?)
        .map_err(|err| {
            format!("reading the slim upgraded redshift .so: {err}")
        })?;

        let authority = prep::funded_payer(base, V4_AUTHORITY_FUNDING).await?;
        let program = Keypair::new();
        let deploy_started = Instant::now();
        loader_v4::deploy_program(base, &authority, &program, &base_bytes)
            .await?;
        let deploy_s = deploy_started.elapsed().as_secs_f64();

        let v4_logs = invoke_until(
            er,
            payer,
            program.pubkey(),
            &log_data,
            vec![],
            has_log_msg,
        )
        .await?;
        check!(
            v4_logs
                .iter()
                .any(|line| line.contains("LogMsg:") && !line.contains("upgraded")),
            "the freshly deployed v4 program must log without the upgrade suffix"
        )?;
        let (v4_owner, v4_data) = cloned_program(er, &program.pubkey()).await?;
        check_eq!(
            v4_owner,
            loader_v4::loader_v4_id(),
            "the deployed v4 program must clone under LoaderV4 ownership"
        )?;
        let base_v4_elf =
            base_v4_program_bytes(base, &program.pubkey()).await?;
        check!(
            base_v4_elf.starts_with(&base_bytes),
            "the base v4 program bytes must start with the deployed .so \
             (base {} bytes, .so {} bytes)",
            base_v4_elf.len(),
            base_bytes.len()
        )?;
        check!(
            v4_data == base_v4_elf,
            "the v4 clone must byte-equal the base program bytes \
             (er {} bytes, base {} bytes)",
            v4_data.len(),
            base_v4_elf.len()
        )?;

        loader_v4::deploy_program(base, &authority, &program, &upgraded_bytes)
            .await?;
        let upgrade_started = Instant::now();
        invoke_until_upgraded(er, payer, program.pubkey()).await?;
        let upgrade_pickup_s = upgrade_started.elapsed().as_secs_f64();
        let (upgraded_owner, upgraded_data) =
            cloned_program(er, &program.pubkey()).await?;
        check_eq!(
            upgraded_owner,
            loader_v4::loader_v4_id(),
            "the upgraded v4 program must stay under LoaderV4 ownership"
        )?;
        let base_upgraded_elf =
            base_v4_program_bytes(base, &program.pubkey()).await?;
        check!(
            base_upgraded_elf.starts_with(&upgraded_bytes),
            "the base v4 program bytes must start with the upgraded .so \
             (base {} bytes, .so {} bytes)",
            base_upgraded_elf.len(),
            upgraded_bytes.len()
        )?;
        check!(
            upgraded_data == base_upgraded_elf,
            "the upgraded v4 clone must byte-equal the base program bytes \
             (er {} bytes, base {} bytes)",
            upgraded_data.len(),
            base_upgraded_elf.len()
        )?;

        Ok(ScenarioReport::ok(self.name())
            .setting("loaders", "v1,v2,v3,v4")
            .setting(
                "clone representation",
                "bare ELF, no LoaderV4State header",
            )
            .setting("v1 owner", v1_owner.to_string())
            .metric("v4 deploy s", Unit::Seconds, deploy_s)
            .metric("v4 upgrade pickup s", Unit::Seconds, upgrade_pickup_s))
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

async fn base_v4_program_bytes(
    base: &BaseCtx,
    program: &Pubkey,
) -> Result<Vec<u8>> {
    let account = base
        .account(program)
        .await?
        .ok_or_else(|| CheckError::new("program not present on base"))?;
    if account.data.len() < LOADER_V4_HEADER {
        return Err(CheckError::new(
            "base program account too small for a LoaderV4 header",
        )
        .actual(format!("{} bytes", account.data.len()))
        .into());
    }
    Ok(account.data[LOADER_V4_HEADER..].to_vec())
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
                let logs = er
                    .api()
                    .get_transaction(&signature)
                    .await?
                    .map(|info| info.logs)
                    .unwrap_or_default();
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

async fn invoke_until_upgraded(
    er: &ErCtx,
    payer: &Keypair,
    program: Pubkey,
) -> Result<()> {
    let deadline = Instant::now() + UPGRADE_TIMEOUT;
    let mut attempt = 0u64;
    loop {
        attempt += 1;
        let ix = Instruction {
            program_id: program,
            accounts: vec![],
            data: log_data(attempt),
        };
        if let Ok(signature) = er
            .submit_and_confirm(payer, std::slice::from_ref(&ix))
            .await
        {
            let logs = er
                .api()
                .get_transaction(&signature)
                .await?
                .map(|info| info.logs)
                .unwrap_or_default();
            if logs.iter().any(|line| {
                line.contains("LogMsg: probe") && line.contains("upgraded")
            }) {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(CheckError::new(
                "the ER never picked up the v4 upgrade",
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
