use std::rc::Rc;

use anyhow::{ensure, Context};
use engine_account::AccountMode;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::{decode_account, encode},
    catalog::BasePreparation,
    programs::{CellId, REDLINE},
    random,
    rpc_types::{
        request::RpcRequest,
        response::{Response, RpcSimulateTransactionResult},
    },
    topology::{self, ReplicatedOptions, ReplicatedTopology},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signature::Signature;
use signer::Signer;

use super::{
    replication_recovery::await_leader_advance,
    runtime::{self, Runtime, TIMEOUT},
};

pub(crate) const LABEL: &str = "replicated-runtime";

pub static PREPARE: BasePreparation = BasePreparation {
    run: |base| Box::pin(prepare(base)),
};

fn signers(base: &BaseCtx) -> [Keypair; 2] {
    let mut rng = random::rng(base.config().seed, &[LABEL], 0);
    std::array::from_fn(|_| random::keypair(&mut rng))
}

async fn prepare(base: &BaseCtx) -> Result<()> {
    let [funder, payer] = signers(base);
    for payer in [&funder, &payer] {
        ensure!(
            base.account(&payer.pubkey()).await?.is_none(),
            "runtime fixture already exists; use a fresh stack"
        );
        base.airdrop(&payer.pubkey(), crate::PAYER_LAMPORTS).await?;
    }
    let identity = topology::identity_for_label(LABEL)?.pubkey();
    base.delegate_payer(&funder, &payer, identity).await?;
    let cells = base.cells(REDLINE, &funder);
    let plain = cells.create(crate::ACCOUNT_SPACE, identity, 0).await?;
    cells.write(11, &[plain]).await?;
    cells.create_delegated(crate::ACCOUNT_SPACE, identity, 1).await?;
    Ok(())
}

pub struct ReplicatedRuntime {
    fixture: Runtime,
    topology: ReplicatedTopology,
}

impl Scenario for ReplicatedRuntime {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let fixture = Runtime::new(base.clone(), er.clone()).await?;
        let topology = topology::replicated(
            &base,
            er,
            ReplicatedOptions {
                label: LABEL.into(),
                verifiers: 2,
            },
        )
        .await?;
        topology.wait_verifiers_connected(TIMEOUT).await?;
        Ok(Self { fixture, topology })
    }

    async fn run(mut self) -> Result<()> {
        let outcome = self.verify().await;
        outcome.and(self.topology.finish().await)
    }
}

impl ReplicatedRuntime {
    async fn verify(&mut self) -> Result<()> {
        let fixture = &self.fixture;
        let [funder, payer] = signers(&fixture.base);
        let cells = fixture.base.cells(REDLINE, &funder);
        let [plain, delegated] =
            [0, 1].map(|index| REDLINE.pda(CellId::new(funder.pubkey(), index)).0);
        ensure!(
            fixture.er.local_images(&[plain, delegated]).await? == vec![None, None],
            "cold fixtures were already materialized"
        );
        for key in [plain, delegated] {
            let image = fixture.materialize(key).await?;
            ensure!(
                image.lamports == fixture.rent.minimum_balance(image.data.len()),
                "cold {key}: balance {}, expected {}",
                image.lamports,
                fixture.rent.minimum_balance(image.data.len())
            );
        }
        fixture.er.cells(REDLINE, &payer).write(21, &[delegated]).await?;

        // Real base updates must refresh mirrors without replacing delegated ER state.
        let delegated_image = fixture.er.account(&delegated).await?;
        cells.write(31, &[plain]).await?;
        fixture
            .base
            .submit_and_confirm(
                &funder,
                &[redsuite_core::system::transfer(&funder.pubkey(), &delegated, 1_000_000)],
            )
            .await?;
        let plain_image =
            fixture.base.account(&plain).await?.context("plain base account missing")?;
        fixture
            .er
            .await_account(&plain, |account| {
                ensure!(account == &plain_image);
                Ok(())
            })
            .await?;
        ensure!(fixture.er.account(&delegated).await? == delegated_image);
        let accounts = [(plain, AccountMode::Mirrored), (delegated, AccountMode::Delegated)];
        let signatures = fixture.signatures(&[plain, delegated]).await?;
        let images = fixture.images(&accounts).await?;
        // First-segment resumes prefer snapshots; test warm recovery after a seal.
        await_leader_advance(
            &self.topology,
            "engine_ledger_superblocks",
            1.0,
            "leaving genesis before retained-cursor recovery",
        )
        .await?;
        runtime::fence(&self.topology).await?;
        for verifier in 0..2 {
            runtime::inspect(self.topology.verifier_mut(verifier), &images, &signatures).await?;
            self.topology.verifier_mut(verifier).start(TIMEOUT).await?;
        }
        self.topology.wait_verifiers_connected(TIMEOUT).await?;

        // The lagging verifier must replay this rent-sensitive write before the reset.
        let stopped = self.topology.verifier_mut(1).stop(false).await?;
        ensure!(stopped.exit_code == Some(0) && !stopped.needed_sigkill);
        let old = self.resize(&payer, delegated, 256).await?;
        self.fixture.lower_rent().await?;
        self.topology.verifier(0).wait_connected(TIMEOUT).await?;
        ensure!(self.topology.verifier(0).is_running());
        ensure!(runtime::snapshots(self.topology.verifier(0)).await? == 0.0);
        let new = self.resize(&payer, delegated, 512).await?;
        self.topology.verifier_mut(1).start(TIMEOUT).await?;
        self.topology.wait_verifiers_connected(TIMEOUT).await?;
        let images = self.fixture.images(&accounts).await?;
        let signatures = self.fixture.signatures(&[plain, delegated]).await?;
        ensure!(signatures.contains(&old) && signatures.contains(&new));
        runtime::fence(&self.topology).await?;
        for verifier in 0..2 {
            ensure!(runtime::snapshots(self.topology.verifier(verifier)).await? == 0.0);
            runtime::inspect(self.topology.verifier_mut(verifier), &images, &signatures).await?;
        }
        Ok(())
    }

    async fn resize(&self, payer: &Keypair, key: Pubkey, space: u32) -> Result<(Signature, u64)> {
        let er = &self.fixture.er;
        let tx = er
            .sender(Rc::new(payer.insecure_clone()))
            .prepare(&[REDLINE.resize(payer.pubkey(), key, space)])
            .await?;
        let simulation: Response<RpcSimulateTransactionResult> = er
            .api()
            .call(
                RpcRequest::SimulateTransaction,
                &(
                    encode(&tx)?,
                    json::json!({
                        "encoding": "base64", "sigVerify": true,
                        "accounts": { "encoding": "base64", "addresses": [key.to_string()] },
                    }),
                ),
            )
            .await?;
        ensure!(
            simulation.value.err.is_none(),
            "rent-sensitive simulation failed"
        );
        let account = simulation
            .value
            .accounts
            .context("simulation omitted account images")?
            .into_iter()
            .next()
            .flatten()
            .context("simulated account missing")?;
        let simulated = decode_account(account)?;
        let minimum = self.fixture.rent.minimum_balance(space as usize);
        ensure!(simulated.data.len() == space as usize && simulated.lamports == minimum);
        let signature = er.api().try_send_and_confirm(&tx).await??;
        let executed = er.account(&key).await?.context("resized account missing")?;
        ensure!(executed == simulated, "simulation and execution differ");
        let transaction = er.api().await_transaction(&signature).await?;
        Ok((signature, transaction.slot))
    }
}
