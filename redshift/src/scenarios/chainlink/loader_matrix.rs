use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context};
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{programs::Redshift, topology, BaseCtx, ChainCtx, ErCtx, Result, Scenario};
use signer::Signer;
use tokio::time;

use crate::PAYER_LAMPORTS;

const MEMO_V1_ID: Pubkey = Pubkey::from_str_const("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");
const MEMO_V2_ID: Pubkey = Pubkey::from_str_const("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

const PAYER_DELEGATION: u64 = 1_000_000_000;
const INVOKE_TIMEOUT: Duration = Duration::from_secs(30);
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
        let memo_v1 = MEMO_V1_ID;
        let memo_v2 = MEMO_V2_ID;

        let base_memo_v1 =
            self.base.account(&memo_v1).await?.context("memo v1 is not on the base chain")?;
        invoke_until(
            &self.er,
            &self.payer,
            |attempt| memo_instruction(memo_v1, attempt),
            |logs| program_succeeded(logs, &memo_v1),
        )
        .await?;
        let (v1_owner, v1_data) = cloned_program(&self.er, &memo_v1).await?;
        ensure!(v1_owner == sdk_ids::bpf_loader_deprecated::ID);
        ensure!(v1_data == base_memo_v1.data);

        let base_memo_v2 =
            self.base.account(&memo_v2).await?.context("memo v2 is not on the base chain")?;
        invoke_until(
            &self.er,
            &self.payer,
            |attempt| memo_instruction(memo_v2, attempt),
            has_memo,
        )
        .await?;
        let (v2_owner, v2_data) = cloned_program(&self.er, &memo_v2).await?;
        ensure!(v2_owner == sdk_ids::loader_v4::ID);
        ensure!(v2_data == base_memo_v2.data);

        let (v3_id, _) = topology::redshift_loader_v3_target();
        invoke_until(
            &self.er,
            &self.payer,
            |attempt| Redshift::at(v3_id).log(&format!("probe {attempt}")),
            has_probe,
        )
        .await?;
        let v3_programdata =
            Pubkey::find_program_address(&[v3_id.as_ref()], &sdk_ids::bpf_loader_upgradeable::ID).0;
        let mut base_v3_programdata = self
            .base
            .account(&v3_programdata)
            .await?
            .context("the v3 programdata account is not on the base chain")?
            .data;
        let base_v3_elf = base_v3_programdata.split_off(LOADER_V3_PROGRAMDATA_METADATA);
        let (v3_owner, v3_data) = cloned_program(&self.er, &v3_id).await?;
        ensure!(v3_owner == sdk_ids::loader_v4::ID);
        ensure!(v3_data == base_v3_elf);

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

fn has_probe(logs: &[String]) -> bool {
    logs.iter().any(|line| line.starts_with("Program log: probe "))
}

fn has_memo(logs: &[String]) -> bool {
    logs.iter().any(|line| line.contains("redsuite memo"))
}

fn program_succeeded(logs: &[String], program: &Pubkey) -> bool {
    let success_line = format!("Program {program} success");
    logs.iter().any(|line| line == &success_line)
}

async fn cloned_program(er: &ErCtx, program: &Pubkey) -> Result<(Pubkey, Vec<u8>)> {
    let cloned = er.account(program).await?.context("program not present in the ER")?;
    Ok((cloned.owner, cloned.data))
}

async fn invoke_until(
    er: &ErCtx,
    payer: &Keypair,
    build: impl Fn(u64) -> Instruction,
    accept: impl Fn(&[String]) -> bool,
) -> Result<()> {
    let deadline = Instant::now() + INVOKE_TIMEOUT;
    let events = er.api().events().await?;
    let mut notifications = events.logs(&payer.pubkey()).await?;
    let mut attempt = 0u64;
    loop {
        attempt += 1;
        match er.submit_and_confirm(payer, &[build(attempt)]).await {
            Ok(signature) => {
                let update =
                    time::timeout_at(deadline.into(), notifications.for_signature(&signature))
                        .await
                        .context("program logs not delivered")??;
                ensure!(
                    update.err.is_none(),
                    "program invocation failed: {:?}",
                    update.err
                );
                if accept(&update.logs) {
                    return Ok(());
                }
            }
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(err.context("program invocation never succeeded"));
                }
            }
        }
        ensure!(Instant::now() < deadline);
        time::sleep(Duration::from_millis(500)).await;
    }
}
