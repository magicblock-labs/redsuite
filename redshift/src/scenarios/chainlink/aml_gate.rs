use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    time::Duration,
};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, dlp, prep, topology, BaseCtx, ChainCtx, ErCtx,
    PrivateErScenario, Result,
};
use sdk::spl::{
    builders::{
        InitializeGlobalVaultBuilder, InitializeRentPdaBuilder,
        SetupAndDelegateShuttleEphemeralAtaWithMergeBuilder,
    },
    find_rent_pda, find_shuttle_ata, find_shuttle_ephemeral_ata,
};
use signer::Signer;

use super::spl;

const AIRDROP: u64 = 2_000_000_000;
const SHUTTLE_AMOUNT: u64 = 200;
const SHUTTLE_ID: u32 = 0;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
const UNDELEGATION_TIMEOUT: Duration = Duration::from_secs(20);
const MERGE_TIMEOUT: Duration = Duration::from_secs(30);

// The risk server owns the threshold: the validator only ever sees the
// boolean verdict, so the mock computes isRisky from the seeded score here.
const RISK_THRESHOLD: u64 = 5;

// Stands in for the risk server the validator queries via
// GET /risk?pubkey=<addr>, answering the camelCase {"isRisky":bool} shape
// magicblock-aml deserializes.
pub struct MockRiskServer {
    base_url: String,
    shutdown: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
    risks: Arc<RwLock<HashMap<String, u64>>>,
    requested_addresses: Arc<RwLock<Vec<String>>>,
}

impl MockRiskServer {
    pub fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let risks = Arc::new(RwLock::new(HashMap::new()));
        let requested_addresses = Arc::new(RwLock::new(Vec::new()));

        let worker_risks = Arc::clone(&risks);
        let worker_requested_addresses = Arc::clone(&requested_addresses);
        let worker = thread::spawn(move || {
            while !worker_shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buffer = [0u8; 4096];
                        let read = stream.read(&mut buffer).unwrap_or(0);
                        let request = String::from_utf8_lossy(&buffer[..read]);

                        let body = if request.starts_with("GET /risk?")
                            && request.contains("pubkey=")
                        {
                            let address = request
                                .split("pubkey=")
                                .nth(1)
                                .unwrap_or("")
                                .split(['&', ' '])
                                .next()
                                .unwrap_or("");
                            worker_requested_addresses
                                .write()
                                .unwrap()
                                .push(address.to_string());
                            let risk_score = worker_risks
                                .read()
                                .unwrap()
                                .get(address)
                                .copied()
                                .unwrap_or(0);
                            let is_risky = risk_score >= RISK_THRESHOLD;
                            format!(r#"{{"isRisky":{is_risky}}}"#)
                        } else {
                            r#"{"error":"not found"}"#.to_string()
                        };
                        let status = if request.starts_with("GET /risk?") {
                            "200 OK"
                        } else {
                            "404 Not Found"
                        };
                        let response = format!(
                            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(response.as_bytes());
                    }
                    Err(err)
                        if err.kind() == std::io::ErrorKind::WouldBlock =>
                    {
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Self {
            base_url: format!("http://{addr}"),
            shutdown,
            worker: Some(worker),
            risks,
            requested_addresses,
        })
    }

    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }

    pub fn set_risk(&self, address: &str, risk_score: u64) {
        self.risks
            .write()
            .unwrap()
            .insert(address.to_string(), risk_score);
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn query_count(&self, owner: &Pubkey) -> usize {
        let owner = owner.to_string();
        self.requested_addresses
            .read()
            .unwrap()
            .iter()
            .filter(|address| **address == owner)
            .count()
    }
}

impl Drop for MockRiskServer {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct AmlGate;

#[async_trait(?Send)]
impl PrivateErScenario for AmlGate {
    fn name(&self) -> &str {
        "redshift/aml_gate"
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        let server = MockRiskServer::start()?;
        // No api key or threshold: both belong to the risk server now; loopback
        // http is the one plaintext scheme magicblock-aml accepts.
        let private = topology::private_er(
            base,
            topology::ErOptions {
                label: "aml-gate".to_owned(),
                env: vec![
                    (
                        "MBV_CHAINLINK__RISK__ENABLED".to_owned(),
                        "true".to_owned(),
                    ),
                    (
                        "MBV_CHAINLINK__RISK__RISK_SERVER_URL".to_owned(),
                        server.base_url().to_owned(),
                    ),
                ],
                ..Default::default()
            },
        )
        .await?;
        private.wait_ready(READY_TIMEOUT).await?;

        // High-risk owner (score 9): the merge is blocked, no tokens move,
        // and the shuttle ATA is undelegated on base.
        run_risk_case(base, private.ctx(), &server, 9, false).await?;

        // Low-risk owner (score 1): the gate allows a merge attempt.
        run_risk_case(base, private.ctx(), &server, 1, true).await?;

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
    server.set_risk(&owner_pk.to_string(), owner_risk);

    let fee_payer = prep::funded_payer(base, AIRDROP).await?;
    base.airdrop(&owner_pk, AIRDROP).await?;
    let recipient = Keypair::new();
    let mint = Keypair::new();

    let source_ata = spl::derive_ata(&owner_pk, &mint.pubkey());
    let destination_ata = spl::derive_ata(&recipient.pubkey(), &mint.pubkey());

    let (shuttle_ephemeral_ata, _) =
        find_shuttle_ephemeral_ata(&owner_pk, &mint.pubkey(), SHUTTLE_ID);
    let (shuttle_ata, _) =
        find_shuttle_ata(&shuttle_ephemeral_ata, &mint.pubkey());

    // 1. Create mint, source ATA, destination ATA, and mint tokens
    let setup_ixs = vec![
        spl::allocate_mint(&fee_payer.pubkey(), &mint.pubkey()),
        spl::initialize_mint(&mint.pubkey(), &owner_pk),
        spl::create_ata_idempotent(
            &fee_payer.pubkey(),
            &owner_pk,
            &mint.pubkey(),
        ),
        spl::create_ata_idempotent(
            &fee_payer.pubkey(),
            &recipient.pubkey(),
            &mint.pubkey(),
        ),
        spl::mint_to(&mint.pubkey(), &source_ata, &owner_pk, SHUTTLE_AMOUNT),
    ];
    base.submit_and_confirm_with(&fee_payer, &[&mint, &owner], &setup_ixs)
        .await?;

    // 2. Initialize rent PDA if it doesn't exist yet, and top it up
    let (rent_pda, _) = find_rent_pda();
    if base.account(&rent_pda).await?.is_none() {
        let rent_pda_ix = InitializeRentPdaBuilder {
            payer: fee_payer.pubkey(),
        }
        .instruction();
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
    check!(
        base.account(&fees_vault).await?.is_some(),
        "the private ER identity {er_identity} has no validator fees vault on \
         base — genesis did not supply one for this pool slot"
    )?;

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
    base.submit_and_confirm_with(&fee_payer, &[&owner], &[shuttle_ix])
        .await?;

    // Verify shuttle ATA delegation record exists on base chain
    check!(
        delegation_record_exists(base, &shuttle_ata).await?,
        "shuttle ATA delegation record was not created on base chain"
    )?;

    // 5. Wait for the risk server query
    check::poll(
        &format!("the risk server receives the shuttle owner {owner_pk} query"),
        QUERY_TIMEOUT,
        || async { server.query_count(&owner_pk) > 0 },
    )
    .await?;

    // The gate controls attempts. An allowed action can still fail in the
    // deployed token program; only a successful action must move the tokens.
    if expect_allowed {
        check::poll(
            &format!("low-risk owner {owner_pk}: a merge attempt references {shuttle_ata} and {destination_ata}"),
            MERGE_TIMEOUT,
            || async {
                matches!(
                    merge_attempt(er_ctx, &shuttle_ata, &destination_ata).await,
                    Ok(Some(_))
                )
            },
        )
        .await?;
        let attempt = merge_attempt(er_ctx, &shuttle_ata, &destination_ata)
            .await?
            .ok_or("the merge attempt vanished after the poll")?;
        if attempt.is_none() {
            check::poll(
                &format!("low-risk owner {owner_pk}: the executed merge lands tokens in {destination_ata}"),
                MERGE_TIMEOUT,
                || async {
                    matches!(
                        er_token_amount(er_ctx, &destination_ata).await,
                        Ok(amount) if amount == SHUTTLE_AMOUNT
                    )
                },
            )
            .await?;
            check_eq!(
                er_token_amount(er_ctx, &destination_ata).await?,
                SHUTTLE_AMOUNT,
                "low-risk owner {owner_pk}: the executed merge moves tokens to {destination_ata}"
            )?;
        } else {
            check_eq!(
                er_token_amount(er_ctx, &destination_ata).await?,
                0,
                "low-risk owner {owner_pk}: a failed merge leaves {destination_ata} unchanged"
            )?;
        }
    } else {
        check::poll(
            &format!("high-risk owner {owner_pk}: shuttle {shuttle_ata} undelegates on base"),
            UNDELEGATION_TIMEOUT,
            || async {
                !delegation_record_exists(base, &shuttle_ata)
                    .await
                    .unwrap_or(true)
            },
        )
        .await?;
        check!(
            !delegation_record_exists(base, &shuttle_ata).await?,
            "high-risk owner {owner_pk}: shuttle {shuttle_ata} is undelegated on base"
        )?;
        check!(
            merge_attempt(er_ctx, &shuttle_ata, &destination_ata)
                .await?
                .is_none(),
            "high-risk owner {owner_pk}: no merge attempt for {shuttle_ata} and {destination_ata}"
        )?;
        check_eq!(
            er_token_amount(er_ctx, &destination_ata).await?,
            0,
            "high-risk owner {owner_pk}: the blocked merge leaves {destination_ata} unchanged"
        )?;
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
    let shuttle_signatures =
        er.api().get_signatures_for_address(shuttle_ata, 10).await?;
    let destination_signatures = er
        .api()
        .get_signatures_for_address(destination_ata, 10)
        .await?;
    let Some(shared) = shuttle_signatures
        .iter()
        .find(|signature| destination_signatures.contains(signature))
    else {
        return Ok(None);
    };
    let tx = er
        .api()
        .await_transaction(&shared.parse()?, Duration::from_secs(5))
        .await?;
    Ok(Some(tx.err.map(|err| format!("{err:?}"))))
}

async fn er_token_amount(er: &ErCtx, ata: &Pubkey) -> Result<u64> {
    Ok(spl::token_balance(er, ata).await?.unwrap_or(0))
}

async fn delegation_record_exists(
    base: &BaseCtx,
    delegated_account: &Pubkey,
) -> Result<bool> {
    let record_pda = dlp::delegation_record_pda(delegated_account);
    Ok(base.account(&record_pda).await?.is_some())
}
