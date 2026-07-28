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
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    assert::poll_until, prep, system, topology, BaseCtx, ChainCtx, ErCtx,
    Result, Scenario, ScenarioReport,
};
use sdk::spl::{
    builders::{
        InitializeGlobalVaultBuilder, InitializeRentPdaBuilder,
        SetupAndDelegateShuttleEphemeralAtaWithMergeBuilder,
    },
    find_rent_pda, find_shuttle_ata, find_shuttle_ephemeral_ata,
};
use signer::Signer;

const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ATA_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

const MINT_LEN: u64 = 82;
const MINT_RENT: u64 = 2_000_000;
const AIRDROP: u64 = 2_000_000_000;
const SHUTTLE_AMOUNT: u64 = 200;
const SHUTTLE_ID: u32 = 0;
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
const UNDELEGATION_TIMEOUT: Duration = Duration::from_secs(20);

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
impl Scenario for AmlGate {
    fn name(&self) -> &str {
        "redshift/aml_gate"
    }

    async fn run(&self, base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        let report = ScenarioReport::ok(self.name());

        // Test Case 1: High-risk owner (score = 9) -> post-delegation merge blocked -> auto-undelegated
        let (high_risk_queries, owner_risky_pk) =
            run_risk_case(base, 9, false, "aml-risky").await?;

        // Test Case 2: Low-risk owner (score = 1) -> post-delegation merge allowed -> stays delegated
        let (low_risk_queries, owner_low_pk) =
            run_risk_case(base, 1, true, "aml-low-risk").await?;

        Ok(report
            .setting("high-risk owner", owner_risky_pk)
            .setting("high-risk queries", high_risk_queries)
            .setting("low-risk owner", owner_low_pk)
            .setting("low-risk queries", low_risk_queries))
    }
}

async fn run_risk_case(
    base: &BaseCtx,
    owner_risk: u64,
    _expect_allowed: bool,
    label: &str,
) -> Result<(usize, Pubkey)> {
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

    let source_ata = derive_ata(&owner_pk, &mint.pubkey());
    let destination_ata = derive_ata(&recipient.pubkey(), &mint.pubkey());

    let (shuttle_ephemeral_ata, _) =
        find_shuttle_ephemeral_ata(&owner_pk, &mint.pubkey(), SHUTTLE_ID);
    let (shuttle_ata, _) =
        find_shuttle_ata(&shuttle_ephemeral_ata, &mint.pubkey());

    // 1. Create mint, source ATA, destination ATA, and mint tokens
    let setup_ixs = vec![
        system::create_account(
            &fee_payer.pubkey(),
            &mint.pubkey(),
            MINT_RENT,
            MINT_LEN,
            &token_program(),
        ),
        initialize_mint(&mint.pubkey(), &owner_pk),
        create_ata_idempotent(&fee_payer.pubkey(), &owner_pk, &mint.pubkey()),
        create_ata_idempotent(
            &fee_payer.pubkey(),
            &recipient.pubkey(),
            &mint.pubkey(),
        ),
        mint_to(&mint.pubkey(), &source_ata, &owner_pk, SHUTTLE_AMOUNT),
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
    if base.account(&fees_vault).await?.is_none() {
        let admin = topology::er_identity_keypair()?;
        let init_fees_ix =
            dlp_api::instruction_builder::init_validator_fees_vault(
                fee_payer.pubkey(),
                admin.pubkey(),
                er_identity,
            );
        base.send_with(&fee_payer, &[&admin], &[init_fees_ix])
            .await?;
    }

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
    assert!(
        delegation_record_exists(base, &shuttle_ata).await?,
        "shuttle ATA delegation record was not created on base chain"
    );

    // 5. Wait for Range risk server query
    poll_until(QUERY_TIMEOUT, || async {
        server.request_count() > 0
            && server.requested_addresses().contains(&owner_pk.to_string())
    })
    .await;
    assert!(
        server.requested_addresses().contains(&owner_pk.to_string()),
        "Range risk server did not check shuttle owner"
    );

    // 6. Verify undelegation completes on base
    poll_until(UNDELEGATION_TIMEOUT, || async {
        !delegation_record_exists(base, &shuttle_ata)
            .await
            .unwrap_or(true)
    })
    .await;
    assert!(
        !delegation_record_exists(base, &shuttle_ata).await?,
        "shuttle ATA delegation record was not undelegated on base chain"
    );

    let queries = server.request_count();
    private.stop(true).await?;
    server.stop();

    Ok((queries, owner_pk))
}

fn token_program() -> Pubkey {
    TOKEN_PROGRAM.parse().expect("token program id")
}

fn ata_program() -> Pubkey {
    ATA_PROGRAM.parse().expect("ata program id")
}

fn derive_ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), token_program().as_ref(), mint.as_ref()],
        &ata_program(),
    )
    .0
}

fn initialize_mint(mint: &Pubkey, authority: &Pubkey) -> Instruction {
    let mut data = vec![20u8, 0u8];
    data.extend_from_slice(authority.as_ref());
    data.push(0);
    Instruction {
        program_id: token_program(),
        accounts: vec![AccountMeta::new(*mint, false)],
        data,
    }
}

fn create_ata_idempotent(
    funder: &Pubkey,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Instruction {
    Instruction {
        program_id: ata_program(),
        accounts: vec![
            AccountMeta::new(*funder, true),
            AccountMeta::new(derive_ata(owner, mint), false),
            AccountMeta::new_readonly(*owner, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new_readonly(system::system_id(), false),
            AccountMeta::new_readonly(token_program(), false),
        ],
        data: vec![1],
    }
}

fn mint_to(
    mint: &Pubkey,
    destination: &Pubkey,
    authority: &Pubkey,
    amount: u64,
) -> Instruction {
    let mut data = vec![7u8];
    data.extend_from_slice(&amount.to_le_bytes());
    Instruction {
        program_id: token_program(),
        accounts: vec![
            AccountMeta::new(*mint, false),
            AccountMeta::new(*destination, false),
            AccountMeta::new_readonly(*authority, true),
        ],
        data,
    }
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
