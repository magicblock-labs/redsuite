use std::{
    collections::HashMap,
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::JoinHandle,
    time::Duration,
};
use tokio::sync::Notify;

use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    dlp, topology,
    topology::{ErOptions, PrivateEr},
    wait, BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use sdk::spl::{
    builders::{
        InitializeGlobalVaultBuilder, InitializeRentPdaBuilder,
        SetupAndDelegateShuttleEphemeralAtaWithMergeBuilder,
    },
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

// Stands in for the risk server the validator queries via
// GET /risk?pubkey=<addr>, answering the camelCase {"isRisky":bool} shape
// magicblock-aml deserializes.
pub struct MockRiskServer {
    base_url: Url,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    service: RiskService,
}

impl MockRiskServer {
    pub fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let service = RiskService::default();
        let worker_service = service.clone();
        let worker = thread::spawn(move || worker_service.run(listener, worker_shutdown));

        Ok(Self {
            base_url: format!("http://{addr}").parse()?,
            shutdown,
            worker: Some(worker),
            service,
        })
    }

    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }

    pub fn set_risk(&self, address: Pubkey, risk_score: u64) {
        self.service.risks.write().unwrap().entry(address).or_default().score = risk_score;
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    fn query_count(&self, owner: &Pubkey) -> usize {
        self.service.risks.read().unwrap().get(owner).map_or(0, |risk| risk.queries)
    }
}

#[derive(Default)]
struct Risk {
    score: u64,
    queries: usize,
}

#[derive(Clone, Default)]
struct RiskService {
    risks: Arc<RwLock<HashMap<Pubkey, Risk>>>,
    changed: Arc<Notify>,
}

impl RiskService {
    fn run(self, listener: TcpListener, shutdown: Arc<AtomicBool>) {
        while !shutdown.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => self.respond(stream),
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25))
                }
                Err(_) => break,
            }
        }
    }

    fn respond(&self, mut stream: TcpStream) {
        let mut buffer = [0u8; 4096];
        let read = stream.read(&mut buffer).unwrap_or(0);
        let request = String::from_utf8_lossy(&buffer[..read]);
        let body = self.body(&request);
        let status = if request.starts_with("GET /risk?") { "200 OK" } else { "404 Not Found" };
        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len(),
        );
        let _ = stream.write_all(response.as_bytes());
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
                let mut risks = self.risks.write().unwrap();
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
        self.stop();
    }
}

pub struct AmlGate {
    base: Rc<BaseCtx>,
    server: MockRiskServer,
    private: PrivateEr,
}

impl PrivateErScenario for AmlGate {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let server = MockRiskServer::start()?;
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

    async fn run(self) -> Result<()> {
        let Self { base, server, private } = self;
        // High-risk owner (score 9): the merge is blocked, no tokens move,
        // and the shuttle ATA is undelegated on base.
        run_risk_case(&base, private.ctx(), &server, 9, false).await?;

        // Low-risk owner (score 1): the gate allows a merge attempt.
        run_risk_case(&base, private.ctx(), &server, 1, true).await?;

        private.finish().await?;
        Ok(())
    }
}

async fn run_risk_case(
    base: &BaseCtx,
    er_ctx: &ErCtx,
    server: &MockRiskServer,
    owner_risk: u64,
    expect_allowed: bool,
) -> Result<()> {
    let er_identity = er_ctx.identity();
    let owner = Keypair::new();
    let owner_pk = owner.pubkey();
    server.set_risk(owner_pk, owner_risk);

    let fee_payer = base.funded_payer(AIRDROP).await?;
    base.airdrop(&owner_pk, AIRDROP).await?;
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
    base.submit_and_confirm_with(&fee_payer, &[&mint, &owner], &setup_ixs).await?;

    // 2. Initialize rent PDA if it doesn't exist yet, and top it up
    let (rent_pda, _) = find_rent_pda();
    if base.account(&rent_pda).await?.is_none() {
        let rent_pda_ix = InitializeRentPdaBuilder { payer: fee_payer.pubkey() }.instruction();
        base.submit_and_confirm(&fee_payer, &[rent_pda_ix]).await?;
    }
    base.airdrop(&rent_pda, 1_000_000_000).await?;

    // 3. Initialize Global Vault and Validator Fees Vault
    let vault_ix = InitializeGlobalVaultBuilder {
        payer: fee_payer.pubkey(),
        mint: mint.pubkey(),
    }
    .instruction();
    base.submit_and_confirm(&fee_payer, &[vault_ix]).await?;

    let fees_vault = dlp::validator_fees_vault_pda(&er_identity);
    ensure!(base.account(&fees_vault).await?.is_some());

    let _ = er_ctx.account(&source_ata).await;
    let _ = er_ctx.account(&destination_ata).await;

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
    base.submit_and_confirm_with(&fee_payer, &[&owner], &[shuttle_ix]).await?;

    // Verify shuttle ATA delegation record exists on base chain
    ensure!(delegation_record_exists(base, &shuttle_ata).await?);

    // 5. Wait for the risk server query
    wait::on_change(
        &server.service.changed,
        QUERY_TIMEOUT,
        "risk server queried shuttle owner",
        || Ok((server.query_count(&owner_pk) > 0).then_some(())),
    )
    .await?;

    // The gate controls attempts. An allowed action can still fail in the
    // deployed token program; only a successful action must move the tokens.
    if expect_allowed {
        let attempt = wait::retry("low-risk merge attempted", MERGE_TIMEOUT, async || {
            merge_attempt(er_ctx, &shuttle_ata, &destination_ata)
                .await?
                .context("merge attempt missing")
        })
        .await?;
        if attempt.is_none() {
            er_ctx
                .api()
                .await_accounts(&[destination_ata], "low-risk merge settles", async || {
                    ensure!(er_token_amount(er_ctx, &destination_ata).await? == SHUTTLE_AMOUNT);
                    Ok(())
                })
                .await?;
            ensure!(er_token_amount(er_ctx, &destination_ata).await? == SHUTTLE_AMOUNT);
        } else {
            ensure!(er_token_amount(er_ctx, &destination_ata).await? == 0);
        }
    } else {
        base.api()
            .await_query("high-risk shuttle undelegates", async || {
                ensure!(!delegation_record_exists(base, &shuttle_ata).await?);
                Ok(())
            })
            .await?;
        ensure!(!delegation_record_exists(base, &shuttle_ata).await?);
        ensure!(merge_attempt(er_ctx, &shuttle_ata, &destination_ata).await?.is_none());
        ensure!(er_token_amount(er_ctx, &destination_ata).await? == 0);
    }
    Ok(())
}

// The merge attempt is the er transaction that references both the shuttle
// ATA and the destination. Outer None = no attempt; Some(None) = the attempt
// succeeded; Some(Some(text)) = the attempt failed with that error.
async fn merge_attempt(
    er: &ErCtx,
    shuttle_ata: &Pubkey,
    destination_ata: &Pubkey,
) -> Result<Option<Option<String>>> {
    let shuttle_signatures = er.api().get_signatures_for_address(shuttle_ata, 10).await?;
    let destination_signatures = er.api().get_signatures_for_address(destination_ata, 10).await?;
    let Some(shared) = shuttle_signatures
        .iter()
        .find(|signature| destination_signatures.contains(signature))
    else {
        return Ok(None);
    };
    let tx = er.api().await_transaction(shared).await?;
    Ok(Some(
        transaction_meta(&tx)?.err.as_ref().map(|err| format!("{err:?}")),
    ))
}

async fn er_token_amount(er: &ErCtx, ata: &Pubkey) -> Result<u64> {
    Ok(spl::token_balance(er, ata).await?.unwrap_or(0))
}

async fn delegation_record_exists(base: &BaseCtx, delegated_account: &Pubkey) -> Result<bool> {
    let record_pda = dlp::delegation_record_pda(delegated_account);
    Ok(base.account(&record_pda).await?.is_some())
}
