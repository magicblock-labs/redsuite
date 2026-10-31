use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{self, JoinHandle},
};

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    dlp, topology,
    topology::{ErOptions, PrivateEr},
    wait, BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use sdk::spl::{
    builders::{InitializeGlobalVaultBuilder, SetupAndDelegateShuttleEphemeralAtaWithMergeBuilder},
    find_rent_pda, find_shuttle_ata, find_shuttle_ephemeral_ata,
};
use signer::Signer;
use url::Url;

use super::spl;

const AIRDROP: u64 = 2_000_000_000;
const SHUTTLE_AMOUNT: u64 = 200;
const SHUTTLE_ID: u32 = 0;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
const MERGE_TIMEOUT: Duration = Duration::from_secs(30);

// The risk server owns the threshold: the validator only ever sees the
// boolean verdict, so the mock computes isRisky from the seeded score here.
const RISK_THRESHOLD: u64 = 5;

/// Loopback risk service implementing `GET /risk?pubkey=<addr>` with an `isRisky` verdict.
/// Owns score-to-verdict policy and query evidence; the validator receives no numeric threshold.
pub struct MockRiskServer {
    base_url: Url,
    worker: JoinHandle<()>,
    service: Rc<RiskService>,
}

impl MockRiskServer {
    /// Binds a free loopback port and owns the worker until the server is dropped.
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let service = Rc::new(RiskService::default());
        let worker = task::spawn_local(service.clone().run(listener));

        Ok(Self {
            base_url: format!("http://{addr}").parse()?,
            worker,
            service,
        })
    }

    /// Sets the score used for subsequent verdicts without resetting query evidence.
    pub fn set_risk(&self, address: Pubkey, risk_score: u64) {
        self.service.risks.borrow_mut().entry(address).or_default().score = risk_score;
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    fn query_count(&self, owner: &Pubkey) -> usize {
        self.service.risks.borrow().get(owner).map_or(0, |risk| risk.queries)
    }
}

/// Per-owner verdict input and evidence that the validator actually consulted the gate.
#[derive(Default)]
struct Risk {
    score: u64,
    queries: usize,
}

/// Verdict state and a query notification used to synchronize checks with actual gate consultation.
#[derive(Default)]
struct RiskService {
    risks: RefCell<HashMap<Pubkey, Risk>>,
    changed: Notify,
}

impl RiskService {
    async fn run(self: Rc<Self>, listener: TcpListener) {
        while let Ok((stream, _)) = listener.accept().await {
            self.respond(stream).await;
        }
    }

    async fn respond(&self, mut stream: TcpStream) {
        let mut buffer = [0u8; 4096];
        let read = stream.read(&mut buffer).await.unwrap_or(0);
        let request = String::from_utf8_lossy(&buffer[..read]);
        let body = self.body(&request);
        let status = if request.starts_with("GET /risk?") { "200 OK" } else { "404 Not Found" };
        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len(),
        );
        let _ = stream.write_all(response.as_bytes()).await;
    }

    fn body(&self, request: &str) -> String {
        if !request.starts_with("GET /risk?") || !request.contains("pubkey=") {
            return r#"{"error":"not found"}"#.to_owned();
        }
        let address = request
            .split("pubkey=")
            .nth(1)
            .unwrap_or("")
            .split(['&', ' '])
            .next()
            .unwrap_or("");
        let risk_score = match address.parse::<Pubkey>() {
            Ok(key) => {
                let mut risks = self.risks.borrow_mut();
                let risk = risks.entry(key).or_default();
                risk.queries += 1;
                self.changed.notify_waiters();
                risk.score
            }
            Err(_) => 0,
        };
        let is_risky = risk_score >= RISK_THRESHOLD;
        format!(r#"{{"isRisky":{is_risky}}}"#)
    }
}

impl Drop for MockRiskServer {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

/// Checks risk-gated activation, rescue, and the low-risk allowed-attempt control.
pub struct PostDelegationRiskGate {
    base: Rc<BaseCtx>,
    server: MockRiskServer,
    private: PrivateEr,
}

impl PrivateErScenario for PostDelegationRiskGate {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let server = MockRiskServer::start().await?;
        // No api key or threshold: both belong to the risk server now; loopback
        // http is the one plaintext scheme magicblock-aml accepts.
        let private = topology::private_er(
            &base,
            ErOptions {
                label: "aml-gate".to_owned(),
                settings: Box::new({
                    let url = server.base_url().clone();
                    move |params| {
                        params.chainlink.risk.enabled = true;
                        params.chainlink.risk.risk_server_url = url.to_string();
                    }
                }),
                ..Default::default()
            },
        )
        .await?;
        private.wait_ready(READY_TIMEOUT).await?;

        Ok(Self { base, server, private })
    }

    /// Compares high-risk rescue with a low-risk allowed-attempt control,
    /// not guaranteed merge success.
    async fn run(self) -> Result<()> {
        // High-risk owner (score 9): the merge is blocked, no tokens move,
        // and the shuttle ATA is undelegated on base.
        self.run_risk_case(9, false).await?;

        // Low-risk owner (score 1): the gate allows a merge attempt.
        self.run_risk_case(1, true).await?;

        self.private.finish().await?;
        Ok(())
    }
}

impl PostDelegationRiskGate {
    /// Requires a real risk query before checking either rescue or the allowed merge attempt.
    async fn run_risk_case(&self, owner_risk: u64, expect_allowed: bool) -> Result<()> {
        let er_identity = self.private.ctx().identity();
        let owner = Keypair::new();
        let owner_pk = owner.pubkey();
        self.server.set_risk(owner_pk, owner_risk);

        let fee_payer = self.base.funded_payer(AIRDROP).await?;
        self.base.airdrop(&owner_pk, AIRDROP).await?;
        let recipient = Keypair::new();
        let mint = Keypair::new();

        let source_ata = spl::derive_ata(&owner_pk, &mint.pubkey());
        let destination_ata = spl::derive_ata(&recipient.pubkey(), &mint.pubkey());

        let (shuttle_ephemeral_ata, _) =
            find_shuttle_ephemeral_ata(&owner_pk, &mint.pubkey(), SHUTTLE_ID);
        let (shuttle_ata, _) = find_shuttle_ata(&shuttle_ephemeral_ata, &mint.pubkey());

        // 1. Create mint, source ATA, destination ATA, and mint tokens
        let setup_ixs = vec![
            spl::allocate_mint(&fee_payer.pubkey(), &mint.pubkey()),
            spl::initialize_mint(&mint.pubkey(), &owner_pk),
            spl::create_ata_idempotent(&fee_payer.pubkey(), &owner_pk, &mint.pubkey()),
            spl::create_ata_idempotent(&fee_payer.pubkey(), &recipient.pubkey(), &mint.pubkey()),
            spl::mint_to(&mint.pubkey(), &source_ata, &owner_pk, SHUTTLE_AMOUNT),
        ];
        self.base
            .submit_and_confirm_with(&fee_payer, &[&mint, &owner], &setup_ixs)
            .await?;

        // 2. Initialize rent PDA if it doesn't exist yet, and top it up
        let (rent_pda, _) = find_rent_pda();
        self.base.airdrop(&rent_pda, 1_000_000_000).await?;

        // 3. Initialize Global Vault and Validator Fees Vault
        let vault_ix = InitializeGlobalVaultBuilder {
            payer: fee_payer.pubkey(),
            mint: mint.pubkey(),
        }
        .instruction();
        self.base.submit_and_confirm(&fee_payer, &[vault_ix]).await?;

        let fees_vault = dlp::validator_fees_vault_pda(&er_identity);
        ensure!(self.base.account(&fees_vault).await?.is_some());

        let _ = self.private.ctx().account(&source_ata).await;
        let _ = self.private.ctx().account(&destination_ata).await;

        // 4. Delegate shuttle ATA with post-delegation merge instruction
        let shuttle_ix = SetupAndDelegateShuttleEphemeralAtaWithMergeBuilder {
            payer: fee_payer.pubkey(),
            owner: owner_pk,
            mint: mint.pubkey(),
            source_ata,
            destination_ata,
            shuttle_id: SHUTTLE_ID,
            amount: SHUTTLE_AMOUNT,
            validator: Some(er_identity),
        }
        .instruction();
        self.base.submit_and_confirm_with(&fee_payer, &[&owner], &[shuttle_ix]).await?;

        // Verify shuttle ATA delegation record exists on base chain
        ensure!(self.delegation_record_exists(&shuttle_ata).await?);

        // 5. Wait for the risk server query
        wait::on_change(
            &self.server.service.changed,
            QUERY_TIMEOUT,
            "risk server queried shuttle owner",
            || Ok((self.server.query_count(&owner_pk) > 0).then_some(())),
        )
        .await?;

        // The gate controls attempts. An allowed action can still fail in the
        // deployed token program; only a successful action must move the tokens.
        if expect_allowed {
            let attempt = wait::retry("low-risk merge attempted", MERGE_TIMEOUT, async || {
                self.merge_attempt(&shuttle_ata, &destination_ata)
                    .await?
                    .context("merge attempt missing")
            })
            .await?;
            if attempt.is_none() {
                self.private
                    .ctx()
                    .api()
                    .await_accounts(&[destination_ata], "low-risk merge settles", |accounts| {
                        let account = accounts[0].as_ref().context("destination ATA missing")?;
                        ensure!(spl::account_balance(account) == Some(SHUTTLE_AMOUNT));
                        Ok(())
                    })
                    .await?;
            } else {
                ensure!(self.er_token_amount(&destination_ata).await? == 0);
            }
        } else {
            self.base
                .api()
                .await_query("high-risk shuttle undelegates", async || {
                    let exists = self.delegation_record_exists(&shuttle_ata).await?;
                    Ok(wait::when(!exists, exists))
                })
                .await?;
            ensure!(self.merge_attempt(&shuttle_ata, &destination_ata).await?.is_none());
            ensure!(self.er_token_amount(&destination_ata).await? == 0);
        }
        Ok(())
    }

    /// Finds an ER transaction referencing both the shuttle and destination.
    /// `None` means no attempt; `Some(None)` success; `Some(Some(error))` execution failure.
    async fn merge_attempt(
        &self,
        shuttle_ata: &Pubkey,
        destination_ata: &Pubkey,
    ) -> Result<Option<Option<String>>> {
        let shuttle_signatures =
            self.private.ctx().api().get_signatures_for_address(shuttle_ata, 10).await?;
        let destination_signatures =
            self.private.ctx().api().get_signatures_for_address(destination_ata, 10).await?;
        let Some(shared) = shuttle_signatures
            .iter()
            .find(|signature| destination_signatures.contains(signature))
        else {
            return Ok(None);
        };
        let tx = self.private.ctx().api().await_transaction(shared).await?;
        Ok(Some(
            transaction_meta(&tx)?.err.as_ref().map(|err| format!("{err:?}")),
        ))
    }

    async fn er_token_amount(&self, ata: &Pubkey) -> Result<u64> {
        Ok(spl::token_balance(self.private.ctx(), ata).await?.unwrap_or(0))
    }

    async fn delegation_record_exists(&self, delegated_account: &Pubkey) -> Result<bool> {
        let record_pda = dlp::delegation_record_pda(delegated_account);
        Ok(self.base.account(&record_pda).await?.is_some())
    }
}
