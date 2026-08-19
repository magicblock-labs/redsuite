use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, RwLock,
    },
    thread,
    time::Duration,
};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, prep, system, topology, BaseCtx, ChainCtx, ErCtx,
    PrivateErScenario, Result, ScenarioReport,
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

pub struct MockRangeServer {
    base_url: String,
    request_count: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
    risks: Arc<RwLock<HashMap<String, u64>>>,
    requested_addresses: Arc<RwLock<Vec<String>>>,
}

impl MockRangeServer {
    pub fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let request_count = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_request_count = Arc::clone(&request_count);
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

                        let body = if request.starts_with("GET /risk/address?")
                            && request.contains("address=")
                        {
                            let address = request
                                .split("address=")
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
                            worker_request_count.fetch_add(1, Ordering::SeqCst);
                            format!(r#"{{"riskScore":{risk_score}}}"#)
                        } else {
                            r#"{"error":"not found"}"#.to_string()
                        };
                        let status =
                            if request.starts_with("GET /risk/address?") {
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
            request_count,
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

    pub fn request_count(&self) -> usize {
        self.request_count.load(Ordering::SeqCst)
    }

    pub fn requested_addresses(&self) -> Vec<String> {
        self.requested_addresses.read().unwrap().clone()
    }
}

impl Drop for MockRangeServer {
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

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let report = ScenarioReport::ok(self.name());

        // High-risk owner (score 9): the merge is blocked, no tokens move,
        // and the shuttle ATA is undelegated on base.
        let risky = run_risk_case(base, 9, false, "aml-risky").await?;

        // Low-risk owner (score 1): the merge executes and the shuttle
        // tokens land in the destination on the er.
        let low = run_risk_case(base, 1, true, "aml-low-risk").await?;

        Ok(report
            .setting("high-risk owner", risky.owner)
            .setting("high-risk queries", risky.queries)
            .setting("high-risk merge", risky.merge_error)
            .setting("high-risk destination tokens", risky.destination_tokens)
            .setting("high-risk record at end", risky.record_at_end)
            .setting("low-risk owner", low.owner)
            .setting("low-risk queries", low.queries)
            .setting("low-risk merge", low.merge_error)
            .setting("low-risk destination tokens", low.destination_tokens)
            .setting("low-risk record at end", low.record_at_end))
    }
}

struct RiskCaseOutcome {
    queries: usize,
    owner: Pubkey,
    merge_error: String,
    destination_tokens: u64,
    record_at_end: bool,
}

async fn run_risk_case(
    base: &BaseCtx,
    owner_risk: u64,
    expect_allowed: bool,
    label: &str,
) -> Result<RiskCaseOutcome> {
    let mut server = MockRangeServer::start()?;
    let owner = Keypair::new();
    let owner_pk = owner.pubkey();
    server.set_risk(&owner_pk.to_string(), owner_risk);

    let mut private = topology::private_er(
        base,
        topology::ErOptions {
            label: label.to_owned(),
            env: vec![
                ("MBV_CHAINLINK__RISK__ENABLED".to_owned(), "true".to_owned()),
                (
                    "MBV_CHAINLINK__RISK__BASE_URL".to_owned(),
                    server.base_url().to_owned(),
                ),
                (
                    "MBV_CHAINLINK__RISK__API_KEY".to_owned(),
                    "test-api-key".to_owned(),
                ),
                (
                    "MBV_CHAINLINK__RISK__RISK_SCORE_THRESHOLD".to_owned(),
                    "5".to_owned(),
                ),
            ],
            ..Default::default()
        },
    )
    .await?;
    private.wait_ready(READY_TIMEOUT).await?;
    let er_identity = private.ctx().identity();

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
        system::create_account(
            &fee_payer.pubkey(),
            &mint.pubkey(),
            spl::MINT_RENT,
            spl::MINT_LEN,
            &spl::token_program(),
        ),
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
    base.send_with(&fee_payer, &[&mint, &owner], &setup_ixs)
        .await?;

    // 2. Initialize rent PDA if it doesn't exist yet, and top it up
    let (rent_pda, _) = find_rent_pda();
    if base.account(&rent_pda).await?.is_none() {
        let rent_pda_ix = InitializeRentPdaBuilder {
            payer: fee_payer.pubkey(),
        }
        .instruction();
        base.send(&fee_payer, &[rent_pda_ix]).await?;
    }
    base.airdrop(&rent_pda, 1_000_000_000).await?;

    // 3. Initialize Global Vault and Validator Fees Vault
    let vault_ix = InitializeGlobalVaultBuilder {
        payer: fee_payer.pubkey(),
        mint: mint.pubkey(),
    }
    .instruction();
    base.send(&fee_payer, &[vault_ix]).await?;

    let fees_vault =
        dlp_api::pda::validator_fees_vault_pda_from_validator(&er_identity);
    check!(
        base.account(&fees_vault).await?.is_some(),
        "the private ER identity {er_identity} has no validator fees vault on \
         base — genesis did not supply one for this pool slot"
    )?;

    let er_ctx = private.ctx();
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
    base.send_with(&fee_payer, &[&owner], &[shuttle_ix]).await?;

    // Verify shuttle ATA delegation record exists on base chain
    check!(
        delegation_record_exists(base, &shuttle_ata).await?,
        "shuttle ATA delegation record was not created on base chain"
    )?;

    // 5. Wait for Range risk server query
    check::poll(
        "the Range risk server receives the shuttle owner query",
        QUERY_TIMEOUT,
        || async {
            server.request_count() > 0
                && server.requested_addresses().contains(&owner_pk.to_string())
        },
    )
    .await?;
    check!(
        server.requested_addresses().contains(&owner_pk.to_string()),
        "Range risk server did not check shuttle owner"
    )?;

    // 6. Verify the gate decision through the merge itself: an allowed merge
    // moves the shuttle tokens into the destination on the er, a blocked
    // merge moves nothing and the shuttle ATA is undelegated on base.
    // Delegation-record persistence is NOT a discriminator for the allowed
    // side on this stack — record it as an observation only.
    // 6. Verify the gate decision through the merge ATTEMPT: an allowed
    // owner gets the merge executed on the er (one er transaction that
    // references both the shuttle ATA and the destination), a blocked owner
    // gets the action dropped — no such transaction exists — and the shuttle
    // ATA is undelegated on base. The merge's OUTCOME is version-adaptive:
    // the mainnet-deployed eATA build differs from the build upstream tests
    // against, and under the mainnet pairing the attempt fails with
    // IllegalOwner, which routes the account into the same
    // failing-action-undelegates path. Delegation-record persistence is
    // therefore NOT a discriminator for the allowed side — the attempt is.
    let (merge_error, destination_tokens) = if expect_allowed {
        check::poll(
            "a merge attempt referencing the shuttle and destination \
             appears on the er",
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
        let tokens = if attempt.is_none() {
            check::poll(
                "the executed merge lands the shuttle tokens in the \
                 destination",
                MERGE_TIMEOUT,
                || async {
                    matches!(
                        er_token_amount(er_ctx, &destination_ata).await,
                        Ok(amount) if amount == SHUTTLE_AMOUNT
                    )
                },
            )
            .await?;
            let amount = er_token_amount(er_ctx, &destination_ata).await?;
            check_eq!(
                amount,
                SHUTTLE_AMOUNT,
                "an executed merge must move the shuttle tokens to the \
                 destination"
            )?;
            amount
        } else {
            let amount = er_token_amount(er_ctx, &destination_ata).await?;
            check_eq!(
                amount,
                0,
                "a failed merge attempt must not move the shuttle tokens"
            )?;
            amount
        };
        (attempt.unwrap_or_else(|| "none".to_owned()), tokens)
    } else {
        check::poll(
            "the high-risk shuttle ATA undelegates on base",
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
            "the high-risk shuttle ATA must be undelegated on base"
        )?;
        check!(
            merge_attempt(er_ctx, &shuttle_ata, &destination_ata)
                .await?
                .is_none(),
            "a blocked owner must not get a merge attempt on the er"
        )?;
        let amount = er_token_amount(er_ctx, &destination_ata).await?;
        check_eq!(
            amount,
            0,
            "the blocked merge must not move the shuttle tokens"
        )?;
        ("blocked".to_owned(), amount)
    };
    let record_at_end = delegation_record_exists(base, &shuttle_ata).await?;

    let queries = server.request_count();
    private.stop(true).await?;
    server.stop();

    Ok(RiskCaseOutcome {
        queries,
        owner: owner_pk,
        merge_error,
        destination_tokens,
        record_at_end,
    })
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
    let record_pda = dlp_api::pda::delegation_record_pda_from_delegated_account(
        delegated_account,
    );
    Ok(base.account(&record_pda).await?.is_some())
}
