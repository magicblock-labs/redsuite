use std::{rc::Rc, time::Duration};

use anyhow::{ensure, Context};
use redsuite_core::{
    api::transaction_meta,
    programs::{CellHeader, REDSHIFT},
    random,
    topology::{self, ReplicatedTopology},
    wait, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use signer::Signer;

use crate::PAYER_LAMPORTS;

use super::{
    observations::{
        await_catch_up, check_stream, fence, snapshots, write_cutoff, CONNECT_TIMEOUT, TRANSACTIONS,
    },
    runtime,
};

const READY_TIMEOUT: Duration = Duration::from_secs(60);

pub struct ReplicationRecovery {
    base: Rc<BaseCtx>,
    topology: ReplicatedTopology,
}

impl Scenario for ReplicationRecovery {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let topology = topology::replicated(&base, er).await?;
        Ok(Self { base, topology })
    }

    async fn run(mut self) -> Result<()> {
        let outcome = self.check_recovery().await;
        outcome.and(self.topology.finish().await)
    }
}

impl ReplicationRecovery {
    /// Checks durable state at graceful stop, replay of an offline write after
    /// restart, and isolation of an abrupt peer loss without waiting for seals.
    async fn check_recovery(&mut self) -> Result<()> {
        self.topology.wait_verifiers_connected(CONNECT_TIMEOUT).await?;
        let er = self.topology.leader().clone();
        let mut rng = random::rng(
            self.base.config().seed,
            &["redshift", "replication_recovery"],
            0,
        );
        let funder = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let payer = self.base.seeded_payer(&mut rng, PAYER_LAMPORTS).await?;
        let cell = self
            .base
            .cells(REDSHIFT, &payer)
            .create_primary(er.identity(), u32::MAX)
            .await?;
        self.base.delegate_payer(&funder, &payer, er.identity()).await?;
        er.await_delegated(&cell, "replication recovery cell").await?;
        er.await_delegated(&payer.pubkey(), "replication recovery payer").await?;

        let write = async |delta| {
            let signature = er
                .submit_and_confirm(
                    &payer,
                    &[REDSHIFT.batch().add(&[cell], delta).require_signer(payer.pubkey()).build()],
                )
                .await?;
            let tx = er.api().await_transaction(&signature).await?;
            ensure!(
                transaction_meta(&tx)?.err.is_none(),
                "replication write {signature} failed"
            );
            Ok::<_, anyhow::Error>(())
        };
        let image = async |value| {
            let account = er.account(&cell).await?.context("leader lost the recovery cell")?;
            let observed = CellHeader::try_decode(&account.data)?.value;
            ensure!(
                observed == value,
                "leader recovery cell {cell}: expected {value}, got {observed}"
            );
            Ok::<_, anyhow::Error>(account)
        };

        write(1).await?;
        fence(&self.topology).await?;
        runtime::inspect_delegated(self.topology.verifier_mut(0), cell, image(1).await?).await?;

        self.await_single_connection().await?;
        write(2).await?;
        self.await_survivor_write(1).await?;
        self.topology.verifier_mut(0).start(READY_TIMEOUT).await?;
        self.topology.wait_verifiers_connected(CONNECT_TIMEOUT).await?;
        fence(&self.topology).await?;
        ensure!(
            snapshots(self.topology.verifier(0)).await? == 0.0,
            "reopened verifier unexpectedly fetched a snapshot"
        );

        // The restarted verifier now has to keep up on its own.
        let killed = self.topology.verifier_mut(1).stop(true).await?;
        ensure!(
            killed.exit_signal == Some(9),
            "expected SIGKILL, got {killed:?}"
        );
        self.await_single_connection().await?;
        write(4).await?;
        self.await_survivor_write(0).await?;
        runtime::inspect_delegated(self.topology.verifier_mut(0), cell, image(7).await?).await
    }

    async fn await_single_connection(&self) -> Result<()> {
        wait::retry(
            "leader serving only the surviving verifier",
            CONNECT_TIMEOUT,
            async || {
                let observed = self.topology.leader_connections().await?;
                ensure!(
                    observed == 1.0,
                    "expected one replication connection, got {observed}"
                );
                Ok(())
            },
        )
        .await
    }

    async fn await_survivor_write(&self, index: usize) -> Result<()> {
        let target = write_cutoff(&self.topology).await?;
        let survivor = self.topology.verifier(index);
        await_catch_up(survivor, TRANSACTIONS, target).await?;
        check_stream(survivor).await
    }
}
