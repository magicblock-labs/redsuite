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
const LOADER_V4_STATUS_DEPLOYED: u64 = 1;

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

        invoke_until(er, payer, memo_v1, &memo_data, vec![], |_| true).await?;
        check!(
            er.account(&memo_v1).await?.is_some(),
            "memo v1 (LoaderV1) must be cloned into the ER after invocation"
        )?;

        invoke_until(er, payer, memo_v2, &memo_data, vec![], has_memo).await?;
        let (v2_authority, v2_status) = loader_v4_state(er, &memo_v2).await?;
        check_eq!(
            v2_status,
            LOADER_V4_STATUS_DEPLOYED,
            "the cloned memo v2 must normalize to a Deployed LoaderV4 program"
        )?;
        check_eq!(
            v2_authority,
            memo_v2,
            "a v1/v2 clone normalizes its authority to the program id itself"
        )?;

        let (v3_id, v3_authority) = topology::redshift_loader_v3_target();
        let v3_logs =
            invoke_until(er, payer, v3_id, &log_data, vec![], has_log_msg)
                .await?;
        let (cloned_v3_authority, v3_status) =
            loader_v4_state(er, &v3_id).await?;
        check_eq!(
            v3_status,
            LOADER_V4_STATUS_DEPLOYED,
            "the cloned v3 program must be a Deployed LoaderV4 program"
        )?;
        check_eq!(
            cloned_v3_authority,
            v3_authority,
            "the v3 upgrade authority must survive the clone into the ER"
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
        let (v4_authority, v4_status) =
            loader_v4_state(er, &program.pubkey()).await?;
        check_eq!(
            v4_status,
            LOADER_V4_STATUS_DEPLOYED,
            "the deployed v4 program must clone as a Deployed LoaderV4 program"
        )?;
        check_eq!(
            v4_authority,
            authority.pubkey(),
            "the v4 deploy authority must survive the clone into the ER"
        )?;

        loader_v4::deploy_program(base, &authority, &program, &upgraded_bytes)
            .await?;
        let upgrade_started = Instant::now();
        invoke_until_upgraded(er, payer, program.pubkey()).await?;
        let upgrade_pickup_s = upgrade_started.elapsed().as_secs_f64();
        let (upgraded_authority, upgraded_status) =
            loader_v4_state(er, &program.pubkey()).await?;
        check_eq!(
            upgraded_status,
            LOADER_V4_STATUS_DEPLOYED,
            "the upgraded v4 program must stay a Deployed LoaderV4 program"
        )?;
        check_eq!(
            upgraded_authority,
            authority.pubkey(),
            "the v4 authority must survive the upgrade"
        )?;

        Ok(ScenarioReport::ok(self.name())
            .setting("loaders", "v1,v2,v3,v4")
            .setting("v2 authority normalized", v2_authority == memo_v2)
            .setting(
                "v3 authority preserved",
                cloned_v3_authority == v3_authority,
            )
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

async fn loader_v4_state(
    er: &ErCtx,
    program: &Pubkey,
) -> Result<(Pubkey, u64)> {
    let account = er
        .account(program)
        .await?
        .ok_or_else(|| CheckError::new("program not present in the ER"))?;
    if account.data.len() < 48 {
        return Err(CheckError::new(
            "program account too small for a LoaderV4 header",
        )
        .actual(format!("{} bytes", account.data.len()))
        .into());
    }
    let authority = Pubkey::try_from(&account.data[8..40])
        .map_err(|_| "bad authority bytes in LoaderV4 header")?;
    let status = u64::from_le_bytes(
        account.data[40..48]
            .try_into()
            .map_err(|_| "bad status bytes in LoaderV4 header")?,
    );
    Ok((authority, status))
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
