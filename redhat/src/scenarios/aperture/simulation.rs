use std::rc::Rc;

use anyhow::{ensure, Context};
use base64::{engine::general_purpose::STANDARD, Engine};
use instruction::{AccountMeta, Instruction};
use magic_api::instruction::MagicBlockInstruction;
use message::VersionedMessage;
use serde_json::Value;
use signature::Signature;
use signer::Signer;
use tracing::debug;
use transaction::{versioned::VersionedTransaction, Message};

use redsuite_core::{
    dlp,
    programs::{
        Action, CellId, Commit, IntentBundle, ResultCallback, SettlementMode, Task, MAGIC, REDSHIFT,
    },
    random::{self, Rng},
    rpc_types::{
        config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
        request::RpcRequest,
        response::Response,
    },
    signed::Signed,
    topology,
    transaction_types::UiTransactionEncoding,
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};

use super::fixture::{vacant, Fixture};

/// Checks that simulation cannot admit transactions or publish account, task, or settlement effects.
pub struct SimulationIsolation {
    base: Rc<BaseCtx>,
    er: ErCtx,
}

impl SimulationIsolation {
    async fn check_transactions(fixture: &mut Fixture, rng: &mut impl Rng) -> Result<()> {
        for sig_verify in [false, true] {
            for skip_preflight in [false, true] {
                let mut before = fixture.snapshot(&[]).await?;
                let nonce = rng.gen_range(1..u64::MAX);
                let message = fixture.message(nonce, 1, &fixture.keys[3..4]).await?;
                let tx = Signed::from_versioned(
                    &fixture.er,
                    &fixture.payer,
                    VersionedMessage::Legacy(message.clone()),
                    None,
                )
                .await?;
                let config = RpcSimulateTransactionConfig {
                    sig_verify,
                    replace_recent_blockhash: !sig_verify,
                    encoding: Some(UiTransactionEncoding::Base64),
                    ..RpcSimulateTransactionConfig::default()
                };
                let start = fixture.er.api().get_slot().await?;
                simulate(&fixture.er, &tx.encoded, config.clone()).await?;
                fixture.not_admitted(&tx.signature, start, None).await?;
                let cold = fixture.er.local_images(&fixture.keys[3..4]).await?.pop().flatten();
                ensure!(cold.is_none() || cold == fixture.base_images[3]);
                before.local[3] = cold;
                fixture.unchanged(&before).await?;
                let mut forged: VersionedTransaction =
                    wincode::deserialize_exact(&STANDARD.decode(&tx.encoded)?)?;
                forged.signatures[0] = Signature::default();
                let encoded = STANDARD.encode(wincode::serialize(&forged)?);
                let api = fixture.er.api();
                let result = api
                    .try_call::<Response<Value>>(
                        RpcRequest::SimulateTransaction,
                        &(&encoded, config),
                    )
                    .await?;
                if sig_verify {
                    let error =
                        result.expect_err("forged signature simulated with verification enabled");
                    ensure!(
                        error.error.message.to_lowercase().contains("signature"),
                        "{error}"
                    );
                } else {
                    ensure!(result?.value["err"].is_null());
                }
                let config = RpcSendTransactionConfig {
                    skip_preflight,
                    encoding: Some(UiTransactionEncoding::Base64),
                    max_retries: Some(0),
                    ..Default::default()
                };
                let result = api
                    .try_call::<String>(RpcRequest::SendTransaction, &(&encoded, config))
                    .await?;
                let error = result.expect_err("forged signature submitted");
                ensure!(
                    error.error.message.to_lowercase().contains("signature"),
                    "{error}"
                );
                fixture.unchanged(&before).await?;
                fixture
                    .check_control(VersionedMessage::Legacy(message), 1, 1, skip_preflight)
                    .await?;
                debug!(sig_verify, "simulation leaves no admission");
            }
        }
        Ok(())
    }

    async fn check_services(fixture: &Fixture) -> Result<()> {
        let payer = fixture.payer.pubkey();
        let cell = fixture.keys[1];
        let vault = dlp::magic_fee_vault_pda(&fixture.er.identity());
        fixture.er.account(&vault).await?.context("simulation fee vault missing")?;
        let protected = [magic_api::MAGIC_CONTEXT_PUBKEY, magic_api::EPHEMERAL_VAULT_PUBKEY, vault];
        fixture.er.accounts(&protected).await?;
        let id = CellId::new(payer, 777);
        let target = REDSHIFT.pda(id).0;
        let create = Instruction {
            program_id: magic_api::id(),
            accounts: vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(target, true),
                AccountMeta::new(magic_api::EPHEMERAL_VAULT_PUBKEY, false),
            ],
            data: MagicBlockInstruction::CreateEphemeralAccount { data_len: 32 }.try_to_vec()?,
        };
        let mut commit = Commit::new(SettlementMode::Commit, &[cell]);
        commit.after_commit.push(
            Action::new(
                payer,
                REDSHIFT
                    .batch()
                    .transfer_from_escrow(fixture.keys[2], 1)
                    .action(cell, dlp::dlp_id()),
                100_000,
            )
            .then(ResultCallback {
                cell,
                refund: None,
                compute_units: 20_000,
            }),
        );
        let mut bundle = IntentBundle::new(payer);
        bundle.validator = Some(fixture.er.identity());
        bundle.commits.push(commit);
        let settle = |mode| {
            let mut bundle = IntentBundle::new(payer);
            bundle.validator = Some(fixture.er.identity());
            bundle.commits.push(Commit::new(mode, &[cell]));
            REDSHIFT.batch().intents(bundle).build()
        };
        let services = [
            ("commit", settle(SettlementMode::Commit)),
            ("undelegate", settle(SettlementMode::Undelegate)),
            ("callback", REDSHIFT.batch().intents(bundle).build()),
            (
                "ephemeral",
                REDSHIFT.batch().invoke_signed(create, &[id]).build(),
            ),
            (
                "task",
                REDSHIFT
                    .batch()
                    .invoke(MAGIC.schedule(
                        payer,
                        payer,
                        Task {
                            id: 254,
                            interval_ms: 100,
                            iterations: 1,
                        },
                        vec![REDSHIFT.batch().add(&[cell], 1).build()],
                    ))
                    .build(),
            ),
        ];
        for (label, instruction) in services {
            let service = fixture.er.local_program_accounts(topology::SERVICE_OWNERS).await?;
            let mut before = fixture.snapshot(&[target]).await?;
            let local = fixture.er.local_images(&protected).await?;
            let base = fixture.base.accounts(&protected).await?;
            let message = Message::new_with_blockhash(
                &[instruction],
                Some(&payer),
                &fixture.er.api().get_latest_blockhash().await?,
            );
            let tx = Signed::from_versioned(
                &fixture.er,
                &fixture.payer,
                VersionedMessage::Legacy(message),
                None,
            )
            .await?;
            let start = fixture.er.api().get_slot().await?;
            let config = RpcSimulateTransactionConfig {
                sig_verify: true,
                encoding: Some(UiTransactionEncoding::Base64),
                ..Default::default()
            };
            simulate(&fixture.er, &tx.encoded, config)
                .await
                .with_context(|| format!("simulated {label}"))?;
            let hydrated = fixture.er.local_images(&[target]).await?.pop().flatten();
            let index = before.keys.iter().position(|key| *key == target).unwrap();
            if before.local[index].is_none() {
                ensure!(
                    hydrated.as_ref().is_none_or(vacant),
                    "{label}: simulated creation persisted: {hydrated:?}"
                );
                before.local[index] = hydrated;
            }
            fixture.not_admitted(&tx.signature, start, None).await?;
            fixture.unchanged(&before).await?;
            ensure!(
                fixture.er.local_images(&protected).await? == local,
                "{label}: simulation changed protocol state"
            );
            ensure!(fixture.base.accounts(&protected).await? == base);
            ensure!(
                fixture.er.local_program_accounts(topology::SERVICE_OWNERS).await? == service,
                "{label}: simulation published task or outbox state"
            );
            debug!(
                case = label,
                "simulated service body leaves live state unchanged"
            );
        }
        let signature = fixture
            .er
            .submit_and_confirm(&fixture.payer, &[settle(SettlementMode::Commit)])
            .await?;
        fixture
            .er
            .check_commit(&fixture.base, &signature, &[cell], false)
            .await?
            .check_success("settlement after simulation")?;
        let mut expected = fixture.base_images[1].clone().context("base cell missing")?;
        expected.data = fixture.expected[1].as_ref().context("local cell missing")?.data.clone();
        ensure!(fixture.base.account(&cell).await? == Some(expected));
        Ok(())
    }
}

impl Scenario for SimulationIsolation {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        Ok(Self { base, er })
    }

    async fn run(self) -> Result<()> {
        let mut rng = random::rng(
            self.base.config().seed,
            &["redhat", "simulation_isolation"],
            0,
        );
        let mut fixture = Fixture::new(self.base, self.er, &mut rng).await?;
        Self::check_transactions(&mut fixture, &mut rng).await?;
        Self::check_services(&fixture).await
    }
}

async fn simulate(er: &ErCtx, encoded: &str, config: RpcSimulateTransactionConfig) -> Result<()> {
    let result: Value =
        er.api().call_value(RpcRequest::SimulateTransaction, &(encoded, config)).await?;
    ensure!(
        result["err"].is_null() && result["unitsConsumed"].as_u64().is_some_and(|units| units > 0),
        "{result}"
    );
    Ok(())
}
