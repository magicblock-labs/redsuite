use std::{cell::Cell, fs, rc::Rc, slice, time::Duration};
use tokio::sync::Notify;

use account::Account;
use anyhow::{anyhow, ensure, Context};
use futures_util::future::{try_join, try_join_all};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::{custom_error_code, transaction_logs, transaction_meta},
    catalog::Fixture,
    netfault::{BaseProxies, Selector},
    programs::{CellHeader, UpgradeProbe},
    redline::Accounts,
    topology::{self, ErOptions, PrivateEr},
    transaction_types::EncodedConfirmedTransactionWithStatusMeta as TransactionInfo,
    wait, BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, TxSender,
};
use signer::Signer;
use solana_loader_v3_interface::{
    get_program_data_address, instruction as loader, state::UpgradeableLoaderState,
};
use tokio::time;
use tracing::{instrument, Span};

use crate::{program::layout, PAYER_LAMPORTS};

const TIMEOUT: Duration = Duration::from_secs(90);
const FETCH_FAILURE: &str = "upgrade probe cold fetch unavailable";
const PHASES: [&str; 4] = ["before upgrade", "held", "released", "converged"];

async fn buffer(base: &BaseCtx, payer: &Keypair, bytes: &[u8]) -> Result<Keypair> {
    let buffer = Keypair::new();
    base.submit_and_confirm_with(
        payer,
        &[&buffer],
        &loader::create_buffer(
            &payer.pubkey(),
            &buffer.pubkey(),
            &payer.pubkey(),
            1_000_000_000,
            bytes.len(),
        )?,
    )
    .await?;
    try_join_all(bytes.chunks(800).enumerate().map(|(index, chunk)| {
        let ix = loader::write(
            &buffer.pubkey(),
            &payer.pubkey(),
            (index * 800) as u32,
            chunk.to_vec(),
        );
        async move { base.submit_and_confirm(payer, &[ix]).await }
    }))
    .await?;
    Ok(buffer)
}

async fn local_program(er: &ErCtx, program: Pubkey) -> Result<Option<Account>> {
    Ok(er
        .api()
        .get_program_accounts(&sdk_ids::loader_v4::ID)
        .await?
        .into_iter()
        .find_map(|(key, account)| (key == program).then_some(account)))
}

/// Checks warm/cold execution across a real upgrade and rollback of failed transactions.
pub struct ProgramUpgradeConsistency {
    base: Rc<BaseCtx>,
    a: Vec<u8>,
    b: Vec<u8>,
    funder: Keypair,
    buffer: Pubkey,
    program_id: Pubkey,
    programdata: Pubkey,
    proxies: BaseProxies,
    hot: PrivateEr,
    cold: PrivateEr,
}

impl PrivateErScenario for ProgramUpgradeConsistency {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let mut a = fs::read(Fixture::UpgradeProbe.resolve()?)?;
        let mut b = fs::read(Fixture::UpgradeProbeUpgraded.resolve()?)?;
        let size = a.len().max(b.len());
        a.resize(size, 0);
        b.resize(size, 0);
        ensure!(a != b);
        let funder = base.funded_payer(10_000_000_000).await?;
        let authority = funder.pubkey();
        let (a_buffer, b_buffer) =
            try_join(buffer(&base, &funder, &a), buffer(&base, &funder, &b)).await?;
        let program = Keypair::new();
        let program_id = program.pubkey();
        let programdata = get_program_data_address(&program_id);
        base.submit_and_confirm_with(
            &funder,
            &[&program],
            &loader::deploy_with_max_program_len(
                &authority,
                &program_id,
                &a_buffer.pubkey(),
                &authority,
                2_000_000,
                size,
            )?,
        )
        .await?;
        let proxies = BaseProxies::spawn(&base).await?;
        let options = |label: &str| ErOptions {
            label: label.into(),
            base_endpoints: Some(proxies.endpoints()),

            ..ErOptions::default()
        };
        let (hot, cold) = try_join(
            topology::private_er(&base, options("upgrade-hot")),
            topology::private_er(&base, options("upgrade-cold")),
        )
        .await?;
        Ok(Self {
            base,
            a,
            b,
            funder,
            buffer: b_buffer.pubkey(),
            program_id,
            programdata,
            proxies,
            hot,
            cold,
        })
    }

    async fn run(self) -> Result<()> {
        let er = self.hot.ctx();
        let payer = self.base.delegated_payer(&self.funder, er.identity(), PAYER_LAMPORTS).await?;
        let keys = Accounts::new(
            (layout::DATA_OFFSET + layout::ID_SIZE) as u32,
            er.identity(),
        )
        .init_batched(self.base.as_ref(), slice::from_ref(&self.funder), 2, true)
        .await?;
        let keys = [keys[0], keys[1]];
        let base_state = self.base.accounts(&keys).await?;
        let expected = er
            .accounts(&keys)
            .await?
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .context("state account missing")?;
        ensure!(local_program(self.cold.ctx(), self.program_id).await?.is_none());
        let progress = Progress::default();
        let traffic = Invocations {
            er,
            sender: er.sender(Rc::new(payer)),
            keys: &keys,
            expected,
            program_id: self.program_id,
            proxies: &self.proxies,
            progress: &progress,
        };
        time::timeout(
            Duration::from_secs(180),
            try_join(traffic.run(), self.upgrade(&progress)),
        )
        .await??;
        ensure!(self.base.accounts(&keys).await? == base_state);
        try_join(self.hot.finish(), self.cold.finish()).await?;
        self.proxies.finish()?;
        Ok(())
    }
}

#[derive(Default)]
struct Progress {
    phase: Cell<usize>,
    counts: Cell<[usize; 4]>,
    changed: Notify,
    done: Cell<bool>,
}

impl Progress {
    async fn await_count(&self, phase: usize, count: usize) -> Result<()> {
        wait::on_change(&self.changed, TIMEOUT, "invocations progress", || {
            Ok((self.counts.get()[phase] >= count).then_some(()))
        })
        .await
    }
}

struct Invocations<'a> {
    er: &'a ErCtx,
    sender: TxSender,
    keys: &'a [Pubkey; 2],
    expected: Vec<Account>,
    program_id: Pubkey,
    proxies: &'a BaseProxies,
    progress: &'a Progress,
}

impl Invocations<'_> {
    async fn run(mut self) -> Result<()> {
        let mut loading_failure = Some(
            self.proxies.reject(
                Selector::methods(&["getAccountInfo", "getMultipleAccounts"])
                    .http()
                    .response()
                    .account(&self.program_id),
                FETCH_FAILURE,
            ),
        );
        let mut id = 0;
        while !self.progress.done.get() {
            id += 1;
            self.invoke(id, loading_failure.is_some()).await?;
            if let Some(fault) = loading_failure.take() {
                fault.remove();
            }
        }
        Ok(())
    }

    #[instrument(skip_all, level = "error", fields(phase))]
    async fn invoke(&mut self, id: u64, injecting: bool) -> Result<()> {
        let instruction =
            UpgradeProbe::at(self.program_id).write(id, id.is_multiple_of(4), self.keys);
        let tx = self.sender.prepare(slice::from_ref(&instruction)).await?;
        let signature = tx.signatures[0];
        let current = self.progress.phase.get();
        Span::current().record("phase", PHASES[current]);
        match self.er.api().try_send_transaction(&tx).await? {
            Err(rpc) => {
                ensure!(
                    rpc.error.code.code() == -32003
                        && if injecting {
                            rpc.error.message.contains(FETCH_FAILURE)
                        } else {
                            matches!(current, 1 | 2)
                        }
                );
            }
            Ok(_) => {
                ensure!(!injecting);
                let outcome = self.er.api().await_transaction(&signature).await?;
                self.verify(&outcome, id, current)?;
            }
        };
        let state = self
            .er
            .accounts(self.keys)
            .await?
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .context("state account missing")?;
        ensure!(
            state == self.expected,
            "state {state:?}, expected {:?}",
            self.expected
        );
        let mut counts = self.progress.counts.get();
        counts[current] += 1;
        self.progress.counts.set(counts);
        self.progress.changed.notify_waiters();
        Ok(())
    }

    fn verify(&mut self, outcome: &TransactionInfo, id: u64, current: usize) -> Result<()> {
        let outcome = transaction_meta(outcome)?;
        let versions: Vec<_> = transaction_logs(outcome)
            .iter()
            .map(String::as_str)
            .filter(|log| log.starts_with("Program log: Upgrade: "))
            .collect();
        let version = match versions.as_slice() {
            ["Program log: Upgrade: 1"] => 1,
            ["Program log: Upgrade: 2"] => 2,
            [] => {
                ensure!(outcome.err.is_some() && matches!(current, 1 | 2));
                return Ok(());
            }
            _ => return Err(anyhow!("upgrade version markers: {versions:?}")),
        };
        ensure!(matches!(
            (current, version),
            (0, 1) | (1 | 2, 1 | 2) | (3, 2)
        ));
        if let Some(error) = &outcome.err {
            ensure!(id.is_multiple_of(4));
            ensure!(custom_error_code(error.clone()) == Some(UpgradeProbe::FAILURE_CODE));
        } else {
            ensure!(!id.is_multiple_of(4));
            let value = id * 10 + version;
            for (account, value) in self.expected.iter_mut().zip([value, !value]) {
                let mut header = CellHeader::try_decode(&account.data)?;
                header.value = value;
                // Set and Fill each advance the cell revision.
                header.revision += 2;
                header.write(&mut account.data)?;
                account.data[layout::DATA_OFFSET..].copy_from_slice(&value.to_le_bytes());
            }
        }
        Ok(())
    }
}

impl ProgramUpgradeConsistency {
    async fn materialized(&self, er: &ErCtx, bytes: &[u8]) -> Result<()> {
        ensure!(local_program(er, self.program_id)
            .await?
            .is_some_and(|account| account.executable && account.data == bytes));
        Ok(())
    }

    async fn upgrade(&self, progress: &Progress) -> Result<()> {
        let er = self.hot.ctx();
        let authority = self.funder.pubkey();
        progress.await_count(0, 4).await?;
        self.materialized(er, &self.a).await?;
        let holds = [self.program_id, self.programdata].map(|key| {
            self.proxies.stall(
                Selector::methods(&[
                    "getAccountInfo",
                    "getMultipleAccounts",
                    "accountNotification",
                    "programNotification",
                ])
                .response()
                .notification()
                .account(&key),
            )
        });
        progress.phase.set(1);
        self.base
            .submit_and_confirm(
                &self.funder,
                &[loader::upgrade(&self.program_id, &self.buffer, &authority, &authority)],
            )
            .await?;
        let data = self
            .base
            .account(&self.programdata)
            .await?
            .context("missing upgraded programdata")?
            .data;
        let offset = UpgradeableLoaderState::size_of_programdata_metadata();
        ensure!(data.get(offset..) == Some(self.b.as_slice()));
        self.proxies
            .await_held(
                Selector::default().notification().account(&self.programdata),
                TIMEOUT,
            )
            .await?;
        let after_upgrade = progress.counts.get()[1];
        let (clones, ()) = try_join(
            try_join_all((0..3).map(|_| self.cold.ctx().account(&self.program_id))),
            async {
                self.proxies
                    .await_held(
                        Selector::default().response().account(&self.program_id),
                        TIMEOUT,
                    )
                    .await?;
                progress.await_count(1, after_upgrade + 8).await?;
                for hold in holds {
                    hold.remove();
                }
                progress.phase.set(2);
                Result::Ok(())
            },
        )
        .await?;
        ensure!(clones.iter().all(|a| {
            a.as_ref().is_some_and(|a| {
                a.executable && a.owner == sdk_ids::loader_v4::ID && a.data == self.b
            })
        }));
        er.api()
            .await_accounts(
                &[self.program_id, self.programdata],
                "fresh B materialization",
                |accounts| {
                    ensure!(accounts[0]
                        .as_ref()
                        .is_some_and(|account| account.executable && account.data == self.b));
                    Ok(())
                },
            )
            .await?;
        progress.phase.set(3);
        progress.await_count(3, 12).await?;
        progress.done.set(true);
        Ok(())
    }
}
