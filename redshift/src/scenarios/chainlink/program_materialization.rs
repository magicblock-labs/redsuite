use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, ensure, Context};
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    api::{transaction_logs, transaction_meta},
    programs::{CellHeader, Redline, Redshift, REDLINE},
    topology::{self, ErOptions, PrivateEr},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use signature::Signature;
use signer::Signer;
use solana_address_lookup_table_interface::instruction::create_lookup_table;
use tokio::time;

use crate::{ACCOUNT_SPACE, PAYER_LAMPORTS};

const LABEL: &str = "clone-without-alt-writes";
const ALT_SETTLE: Duration = Duration::from_secs(1);
const SIGNATURE_WINDOW: usize = 50;

const MEMO_V1_ID: Pubkey = Pubkey::from_str_const("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");
const MEMO_V2_ID: Pubkey = Pubkey::from_str_const("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

const PAYER_DELEGATION: u64 = 1_000_000_000;
const INVOKE_TIMEOUT: Duration = Duration::from_secs(30);
const LOADER_V3_PROGRAMDATA_METADATA: usize = 45;

const WRITES: [u64; 2] = [61, 62];

/// Materializes supported loaders and multiple programs without L1 lookup-table writes.
pub struct ProgramMaterialization {
    base: Rc<BaseCtx>,
    identity: Keypair,
    baseline: Vec<Signature>,
    private: PrivateEr,
}

impl PrivateErScenario for ProgramMaterialization {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let identity = topology::identity_for_label(LABEL)?;
        let baseline = base
            .api()
            .get_signatures_for_address(&identity.pubkey(), SIGNATURE_WINDOW)
            .await?;
        let private = topology::private_er(
            &base,
            ErOptions {
                label: LABEL.to_owned(),
                ..Default::default()
            },
        )
        .await?;
        Ok(Self {
            base,
            identity,
            baseline,
            private,
        })
    }

    async fn run(self) -> Result<()> {
        let alt_program = sdk_ids::address_lookup_table::ID.to_string();
        let alt_transactions = async || -> Result<usize> {
            let signatures = self
                .base
                .api()
                .get_signatures_for_address(&self.identity.pubkey(), SIGNATURE_WINDOW)
                .await?;
            let mut count = 0;
            for signature in signatures {
                if self.baseline.contains(&signature) {
                    continue;
                }
                let Some(tx) = self.base.api().get_transaction(&signature).await? else {
                    continue;
                };
                if transaction_logs(transaction_meta(&tx)?)
                    .iter()
                    .any(|line| line.contains(&alt_program))
                {
                    count += 1;
                }
            }
            Ok(count)
        };

        ensure!(alt_transactions().await? == 0);
        let er = self.private.ctx();
        er.await_program_clone(&redshift_interface::ID).await?;
        er.await_program_clone(&topology::COMMITTOR_ID).await?;

        verify_loaders(&self.base, er).await?;
        verify_multi_program(&self.base, er).await?;

        time::sleep(ALT_SETTLE).await;
        ensure!(alt_transactions().await? == 0);
        // Keep the detector's positive control out of the ER observation window.
        self.private.finish().await?;

        let recent_slot = self.base.api().get_slot().await?;
        let (create, _) =
            create_lookup_table(self.identity.pubkey(), self.identity.pubkey(), recent_slot);
        self.base.submit_and_confirm(&self.identity, &[create]).await?;
        ensure!(alt_transactions().await? == 1);
        Ok(())
    }
}

async fn verify_loaders(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let funder = base.funded_payer(PAYER_LAMPORTS).await?;
    let payer = base.delegated_payer(&funder, er.identity(), PAYER_DELEGATION).await?;
    let memo_v1 = MEMO_V1_ID;
    let memo_v2 = MEMO_V2_ID;

    let base_memo_v1 = base.account(&memo_v1).await?.context("memo v1 is not on the base chain")?;
    invoke_until(
        er,
        &payer,
        |attempt| memo_instruction(memo_v1, attempt),
        |logs| program_succeeded(logs, &memo_v1),
    )
    .await?;
    let (v1_owner, v1_data) = cloned_program(er, &memo_v1).await?;
    ensure!(v1_owner == sdk_ids::bpf_loader_deprecated::ID);
    ensure!(v1_data == base_memo_v1.data);

    let base_memo_v2 = base.account(&memo_v2).await?.context("memo v2 is not on the base chain")?;
    invoke_until(
        er,
        &payer,
        |attempt| memo_instruction(memo_v2, attempt),
        has_memo,
    )
    .await?;
    let (v2_owner, v2_data) = cloned_program(er, &memo_v2).await?;
    ensure!(v2_owner == sdk_ids::loader_v4::ID);
    ensure!(v2_data == base_memo_v2.data);

    let (v3_id, _) = topology::redshift_loader_v3_target();
    invoke_until(
        er,
        &payer,
        |attempt| Redshift::at(v3_id).log(&format!("probe {attempt}")),
        has_probe,
    )
    .await?;
    let v3_programdata =
        Pubkey::find_program_address(&[v3_id.as_ref()], &sdk_ids::bpf_loader_upgradeable::ID).0;
    let mut base_v3_programdata = base
        .account(&v3_programdata)
        .await?
        .context("the v3 programdata account is not on the base chain")?
        .data;
    let base_v3_elf = base_v3_programdata.split_off(LOADER_V3_PROGRAMDATA_METADATA);
    let (v3_owner, v3_data) = cloned_program(er, &v3_id).await?;
    ensure!(v3_owner == sdk_ids::loader_v4::ID);
    ensure!(v3_data == base_v3_elf);

    Ok(())
}

async fn verify_multi_program(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let [(first_program, _), (second_program, _)] = topology::redline_loader_v3_pair();

    let payer = base.funded_payer(PAYER_LAMPORTS).await?;
    let first_pda = base
        .cells(REDLINE, &payer)
        .at(first_program)
        .create_delegated(ACCOUNT_SPACE, er.identity(), 0)
        .await?;
    let second_pda = base
        .cells(REDLINE, &payer)
        .at(second_program)
        .create_delegated(ACCOUNT_SPACE, er.identity(), 1)
        .await?;

    let accounts = [(first_program, first_pda), (second_program, second_pda)];
    let writes: [Instruction; 2] = std::array::from_fn(|index| {
        let (program, account) = accounts[index];
        Redline::at(program).write(WRITES[index], &[account])
    });
    er.submit_and_confirm(&payer, &writes).await?;

    for ((program, _), label) in accounts.iter().zip(["first", "second"]) {
        let clone = er
            .account(program)
            .await?
            .ok_or_else(|| anyhow!("{label} program not cloned after the transaction"))?;
        ensure!(clone.executable);
    }
    for ((_, key), expected) in accounts.iter().zip(WRITES) {
        let clone = er.account(key).await?.context("cloned account missing")?;
        ensure!(CellHeader::written_id(&clone.data) == Some(expected));
    }

    Ok(())
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
