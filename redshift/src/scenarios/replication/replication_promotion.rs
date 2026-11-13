use std::{fs, rc::Rc};

use anyhow::{ensure, Context};
use engine_account::AccountMode;
use redsuite_core::{
    catalog::BasePreparation,
    programs::{CellId, REDLINE},
    rpc_types::{request::RpcRequest, response::RpcIdentity},
    topology::{self, ReplicatedOptions, ReplicatedTopology},
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signer::Signer;

use super::runtime::{self, prepare, signers, Runtime, TIMEOUT};

pub(crate) const LABEL: &str = "replication-promotion";
pub static PREPARE: BasePreparation = BasePreparation {
    run: |base| Box::pin(prepare(base, LABEL)),
};

pub struct ReplicationPromotion {
    fixture: Runtime,
    topology: ReplicatedTopology,
}

impl Scenario for ReplicationPromotion {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let fixture = Runtime::new(base.clone(), er.clone()).await?;
        let topology = topology::replicated(
            &base,
            er,
            ReplicatedOptions {
                label: LABEL.into(),
                verifiers: 1,
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

impl ReplicationPromotion {
    async fn verify(&mut self) -> Result<()> {
        let fixture = &self.fixture;
        let [funder, payer] = signers(&fixture.base, LABEL);
        let delegated = REDLINE.pda(CellId::new(funder.pubkey(), 1)).0;
        fixture.materialize(delegated).await?;
        let cells = fixture.er.cells(REDLINE, &payer);
        for value in [21, 42] {
            cells.write(value, &[delegated]).await?;
        }
        let keys = [delegated, payer.pubkey()];
        let images = fixture.images(&keys.map(|key| (key, AccountMode::Delegated))).await?;
        let signatures = fixture.signatures(&keys).await?;
        let mut receipts = Vec::new();
        for &(signature, _) in &signatures {
            receipts.push((
                signature,
                fixture.er.api().await_transaction(&signature).await?,
            ));
        }
        let mut histories = Vec::new();
        for key in keys {
            histories.push((
                key,
                fixture.er.api().get_signatures_for_address(&key, 32).await?,
            ));
        }
        runtime::fence(&self.topology).await?;
        self.topology.stop_leader().await?;
        let verifier = self.topology.verifier(0);
        wait::retry(
            "verifier observing leader disconnect",
            TIMEOUT,
            async || {
                ensure!(
                    verifier.is_running(),
                    "verifier exited after leader shutdown"
                );
                ensure!(
                    verifier
                        .scrape_metrics()
                        .await?
                        .get("engine_replicator_client_stream_connected")
                        .context("verifier stream metric missing")?
                        == 0.0
                );
                Ok(())
            },
        )
        .await?;
        let storage = verifier.storage_dir().to_owned();
        runtime::inspect(self.topology.verifier_mut(0), &images, &signatures).await?;
        self.topology.promote(0).await?;
        ensure!(fixture.er.storage_dir().await? == storage);
        let identity: RpcIdentity =
            fixture.er.api().call(RpcRequest::GetIdentity, &[0u8; 0]).await?;
        ensure!(identity.identity == fixture.er.identity().to_string());
        ensure!(fixture.images(&keys.map(|key| (key, AccountMode::Delegated))).await? == images);
        for (signature, receipt) in receipts {
            ensure!(
                fixture.er.api().await_transaction(&signature).await? == receipt,
                "promoted transaction differs for {signature}"
            );
        }
        for (key, history) in histories {
            ensure!(fixture.er.api().get_signatures_for_address(&key, 32).await? == history);
        }
        let log = fs::read_to_string(fixture.er.log_path().await.context("leader log missing")?)?;
        ensure!(
            log.contains("accountsdb validation succeeded")
                && !log.contains("state inconsistency detected")
                && !log.contains("starting ledger replay"),
            "promotion recovered storage instead of reopening cleanly"
        );
        cells.write(43, &[delegated]).await?;
        Ok(())
    }
}
