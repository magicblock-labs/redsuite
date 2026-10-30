use std::{collections::BTreeSet, rc::Rc, time::Duration};

use anyhow::{anyhow, ensure, Context};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::future::join_all;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::account_config,
    rpc_types::{
        config::{RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcTokenAccountsFilter},
        filter::{Memcmp, RpcFilterType},
        request::RpcRequest,
        response::RpcKeyedAccount,
    },
    wait, Api, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use serde::Serialize;
use serde_json::Value;
use signer::Signer;
use solana_account_decoder_client_types::{
    token::UiTokenAmount, UiAccount, UiAccountData, UiAccountEncoding,
};
use solana_commitment_config::CommitmentConfig;
use tokio::time;
use tracing::instrument;

use crate::scenarios::chainlink::spl;

const READS: usize = CYCLE.len();
const AIRDROP: u64 = 2_000_000_000;
const OWNER_A_BALANCE: u64 = 500;
const OWNER_B_BALANCE: u64 = 300;
const OTHER_MINT_BALANCE: u64 = 70;
const UNRELATED_BALANCE: u64 = 40;
const DELEGATED_AMOUNT: u64 = 200;
const DELEGATE_OFFSET: usize = 72;
const DELEGATED_AMOUNT_OFFSET: usize = 121;
const BURST_DEADLINE: Duration = Duration::from_secs(20);
const NEGATIVE_DEADLINE: Duration = Duration::from_secs(5);

/// Two mints and independent owners/delegate, with unrelated accounts as filter-exclusion controls.
struct Fixture {
    mint: Pubkey,
    other_mint: Pubkey,
    owner_a: Pubkey,
    owner_b: Pubkey,
    delegate: Pubkey,
    unrelated_owner: Pubkey,
    ata_a: Pubkey,
    ata_b: Pubkey,
    ata_a_other: Pubkey,
    ata_unrelated: Pubkey,
}

/// Concurrent read shapes covering local projection visibility and owner/mint/delegate filtering.
#[derive(Debug, Clone, Copy)]
enum Query {
    ProgramAccountsByMint,
    ProgramAccountsByOwner,
    BalanceA,
    BalanceB,
    OwnerAByMint,
    OwnerAByProgram,
    OwnerBByMint,
    DelegateByMint,
    DelegateByProgram,
    UnrelatedOwnerByMint,
}

const CYCLE: [Query; 10] = [
    Query::ProgramAccountsByMint,
    Query::ProgramAccountsByOwner,
    Query::BalanceA,
    Query::BalanceB,
    Query::OwnerAByMint,
    Query::OwnerAByProgram,
    Query::OwnerBByMint,
    Query::DelegateByMint,
    Query::DelegateByProgram,
    Query::UnrelatedOwnerByMint,
];

fn confirmed() -> CommitmentConfig {
    CommitmentConfig::confirmed()
}

fn parsed_config() -> RpcAccountInfoConfig {
    RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::JsonParsed),
        ..account_config()
    }
}

fn keys(accounts: &[RpcKeyedAccount]) -> BTreeSet<String> {
    accounts.iter().map(|entry| entry.pubkey.clone()).collect()
}

fn expected(pubkeys: &[Pubkey]) -> BTreeSet<String> {
    pubkeys.iter().map(ToString::to_string).collect()
}

fn read_pubkey(data: &[u8], offset: usize) -> Option<Pubkey> {
    let bytes: [u8; 32] = data.get(offset..offset + 32)?.try_into().ok()?;
    Some(Pubkey::from(bytes))
}

fn read_u64(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn read_delegate(data: &[u8]) -> Option<Pubkey> {
    let tag = u32::from_le_bytes(data.get(DELEGATE_OFFSET..DELEGATE_OFFSET + 4)?.try_into().ok()?);
    (tag == 1).then(|| read_pubkey(data, DELEGATE_OFFSET + 4)).flatten()
}

fn raw_data(account: &UiAccount) -> Result<Vec<u8>> {
    match &account.data {
        UiAccountData::Binary(data, UiAccountEncoding::Base64) => Ok(STANDARD.decode(data)?),
        _ => Err(anyhow!("expected base64 account data")),
    }
}

/// Common comparison fields for independently decoded base64 and `jsonParsed` responses.
struct TokenView {
    encoding: &'static str,
    mint: Option<Pubkey>,
    owner: Option<Pubkey>,
    amount: Option<u64>,
    delegate: Option<Pubkey>,
    delegated_amount: Option<u64>,
}

fn parsed_str(info: &Value, path: &[&str]) -> Option<String> {
    let mut value = info;
    for key in path {
        value = value.get(key)?;
    }
    value.as_str().map(str::to_owned)
}

/// Normalizes either wire representation without deriving expected values from the response itself.
fn token_view(account: &UiAccount) -> Result<TokenView> {
    match &account.data {
        UiAccountData::Json(parsed) => {
            ensure!(parsed.program.as_str() == "spl-token");
            let info = &parsed.parsed["info"];
            let key = |path: &[&str]| -> Option<Pubkey> {
                parsed_str(info, path).and_then(|s| s.parse().ok())
            };
            let amount = |path: &[&str]| -> Option<u64> {
                parsed_str(info, path).and_then(|s| s.parse().ok())
            };
            Ok(TokenView {
                encoding: "jsonParsed",
                mint: key(&["mint"]),
                owner: key(&["owner"]),
                amount: amount(&["tokenAmount", "amount"]),
                delegate: key(&["delegate"]),
                delegated_amount: amount(&["delegatedAmount", "amount"]),
            })
        }
        UiAccountData::Binary(_, UiAccountEncoding::Base64) => {
            let data = raw_data(account)?;
            ensure!(data.len() == spl::TOKEN_ACCOUNT_LEN);
            let delegate = read_delegate(&data);
            Ok(TokenView {
                encoding: "base64",
                mint: read_pubkey(&data, spl::MINT_OFFSET),
                owner: read_pubkey(&data, spl::OWNER_OFFSET),
                amount: read_u64(&data, 64),
                delegate,
                delegated_amount: delegate.and_then(|_| read_u64(&data, DELEGATED_AMOUNT_OFFSET)),
            })
        }
        _ => Err(anyhow!("unsupported token account encoding")),
    }
}

/// Checks identity, amount, and optional delegate allowance through the common token view.
fn check_token_account(
    entry: &RpcKeyedAccount,
    mint: &Pubkey,
    owner: &Pubkey,
    amount: u64,
    delegate: Option<(&Pubkey, u64)>,
) -> Result<&'static str> {
    let view = token_view(&entry.account)?;
    ensure!(entry.account.owner == spl::token_program().to_string());
    ensure!(view.mint == Some(*mint));
    ensure!(view.owner == Some(*owner));
    ensure!(view.amount == Some(amount));
    match delegate {
        Some((delegate, delegated_amount)) => {
            ensure!(view.delegate == Some(*delegate));
            ensure!(view.delegated_amount == Some(delegated_amount));
        }
        None => ensure!(view.delegate.is_none()),
    }
    Ok(view.encoding)
}

fn program_accounts_config(filters: Vec<RpcFilterType>) -> RpcProgramAccountsConfig {
    RpcProgramAccountsConfig {
        filters: Some(filters),
        account_config: account_config(),
        ..RpcProgramAccountsConfig::default()
    }
}

/// Checks raw/parsed token views, ownership filters, and local projection visibility.
/// Mixed mints, owners, and an approved delegate expose both missing matches and leaked results.
pub struct TokenRpcViews {
    client: Api,
    fixture: Fixture,
}

impl Scenario for TokenRpcViews {
    /// Builds contrasting token roles, then waits for the ER-local approval to become queryable.
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let client = er.api().clone();
        let fee_payer = base.funded_payer(AIRDROP).await?;
        let owner_a = Keypair::new();
        let owner_b = Keypair::new();
        let unrelated_owner = Keypair::new();
        let delegate = Keypair::new().pubkey();
        let mint = Keypair::new();
        let other_mint = Keypair::new();
        base.airdrop(&owner_a.pubkey(), AIRDROP).await?;
        base.airdrop(&owner_b.pubkey(), AIRDROP).await?;

        let fixture = Fixture {
            mint: mint.pubkey(),
            other_mint: other_mint.pubkey(),
            owner_a: owner_a.pubkey(),
            owner_b: owner_b.pubkey(),
            delegate,
            unrelated_owner: unrelated_owner.pubkey(),
            ata_a: spl::derive_ata(&owner_a.pubkey(), &mint.pubkey()),
            ata_b: spl::derive_ata(&owner_b.pubkey(), &mint.pubkey()),
            ata_a_other: spl::derive_ata(&owner_a.pubkey(), &other_mint.pubkey()),
            ata_unrelated: spl::derive_ata(&unrelated_owner.pubkey(), &mint.pubkey()),
        };
        let payer = fee_payer.pubkey();

        base.submit_and_confirm_with(
            &fee_payer,
            &[&mint, &other_mint, &owner_a],
            &[
                spl::allocate_mint(&payer, &fixture.mint),
                spl::initialize_mint(&fixture.mint, &fixture.owner_a),
                spl::allocate_mint(&payer, &fixture.other_mint),
                spl::initialize_mint(&fixture.other_mint, &fixture.owner_a),
                spl::create_ata_idempotent(&payer, &fixture.owner_a, &fixture.mint),
                spl::create_ata_idempotent(&payer, &fixture.owner_b, &fixture.mint),
                spl::create_ata_idempotent(&payer, &fixture.owner_a, &fixture.other_mint),
                spl::create_ata_idempotent(&payer, &fixture.unrelated_owner, &fixture.mint),
                spl::mint_to(
                    &fixture.mint,
                    &fixture.ata_a,
                    &fixture.owner_a,
                    OWNER_A_BALANCE,
                ),
                spl::mint_to(
                    &fixture.mint,
                    &fixture.ata_b,
                    &fixture.owner_a,
                    OWNER_B_BALANCE,
                ),
                spl::mint_to(
                    &fixture.other_mint,
                    &fixture.ata_a_other,
                    &fixture.owner_a,
                    OTHER_MINT_BALANCE,
                ),
                spl::mint_to(
                    &fixture.mint,
                    &fixture.ata_unrelated,
                    &fixture.owner_a,
                    UNRELATED_BALANCE,
                ),
            ],
        )
        .await?;

        base.submit_and_confirm(
            &fee_payer,
            &[
                spl::initialize_global_vault(&payer, &fixture.mint),
                spl::initialize_eata(&payer, &fixture.owner_a, &fixture.mint),
                spl::initialize_eata(&payer, &fixture.owner_b, &fixture.mint),
            ],
        )
        .await?;
        base.submit_and_confirm_with(
            &fee_payer,
            &[&owner_a, &owner_b],
            &[
                spl::deposit_spl_tokens(&fixture.owner_a, &fixture.mint, OWNER_A_BALANCE),
                spl::deposit_spl_tokens(&fixture.owner_b, &fixture.mint, OWNER_B_BALANCE),
            ],
        )
        .await?;
        base.submit_and_confirm(
            &fee_payer,
            &[
                spl::delegate_eata(&payer, &fixture.owner_a, &fixture.mint, &er.identity()),
                spl::delegate_eata(&payer, &fixture.owner_b, &fixture.mint, &er.identity()),
            ],
        )
        .await?;

        er.api()
            .await_accounts(
                &[
                    fixture.mint,
                    fixture.other_mint,
                    fixture.ata_a,
                    fixture.ata_b,
                    fixture.ata_a_other,
                ],
                "the er materializes both mints, the two projected token accounts \
             and the plain other-mint account",
                |accounts| {
                    ensure!(accounts[..2].iter().all(Option::is_some));
                    for (account, amount) in accounts[2..].iter().zip([
                        OWNER_A_BALANCE,
                        OWNER_B_BALANCE,
                        OTHER_MINT_BALANCE,
                    ]) {
                        ensure!(
                            spl::account_balance(account.as_ref().context("ATA missing")?)
                                == Some(amount)
                        );
                    }
                    Ok(())
                },
            )
            .await?;

        er.submit_and_confirm(
            &owner_a,
            &[spl::approve(
                &fixture.ata_a,
                &fixture.delegate,
                &fixture.owner_a,
                DELEGATED_AMOUNT,
            )],
        )
        .await?;
        let filter = RpcTokenAccountsFilter::Mint(fixture.mint.to_string());
        let params = (fixture.delegate.to_string(), filter, parsed_config());
        client
            .await_query("the er reports the approved delegate", async || {
                let accounts = client
                    .call_value::<Vec<RpcKeyedAccount>>(
                        RpcRequest::GetTokenAccountsByDelegate,
                        &params,
                    )
                    .await?;
                Ok(wait::when(accounts.len() == 1, accounts.len()))
            })
            .await?;

        Ok(Self { client, fixture })
    }

    /// Checks a concurrent valid-read burst before malformed filter/account requests.
    async fn run(self) -> Result<()> {
        self.check_reads().await?;
        self.check_invalid_filters().await
    }
}

impl TokenRpcViews {
    /// Exercises the full query matrix concurrently against one fixed token-state oracle.
    #[instrument(skip_all, err)]
    async fn check_reads(&self) -> Result<()> {
        let reads = join_all((0..READS).map(|index| self.run_query(CYCLE[index % CYCLE.len()])));
        let outcomes = time::timeout(BURST_DEADLINE, reads)
            .await
            .with_context(|| format!("the burst of {READS} reads exceeded {BURST_DEADLINE:?}"))?;
        for outcome in outcomes {
            outcome?;
        }

        Ok(())
    }

    /// Requires specific invalid-parameter refusals, not transport errors
    /// or empty successful results.
    #[instrument(skip_all, err)]
    async fn check_invalid_filters(&self) -> Result<()> {
        let missing_mint = Keypair::new().pubkey();
        let bogus_program = Keypair::new().pubkey();
        self.expect_rejection(
            "getTokenAccountsByOwner with a missing mint",
            &["mint account not found"],
            RpcRequest::GetTokenAccountsByOwner,
            &(
                self.fixture.owner_a.to_string(),
                RpcTokenAccountsFilter::Mint(missing_mint.to_string()),
                parsed_config(),
            ),
        )
        .await?;
        self.expect_rejection(
            "getTokenAccountsByOwner with an invalid token program",
            &["unknown token program id"],
            RpcRequest::GetTokenAccountsByOwner,
            &(
                self.fixture.owner_a.to_string(),
                RpcTokenAccountsFilter::ProgramId(bogus_program.to_string()),
                parsed_config(),
            ),
        )
        .await?;
        self.expect_rejection(
            "getTokenAccountsByDelegate with a missing mint",
            &["mint account not found"],
            RpcRequest::GetTokenAccountsByDelegate,
            &(
                self.fixture.delegate.to_string(),
                RpcTokenAccountsFilter::Mint(missing_mint.to_string()),
                parsed_config(),
            ),
        )
        .await?;
        self.expect_rejection(
            "getTokenAccountBalance on a missing account",
            &["token account not found", "not a token account"],
            RpcRequest::GetTokenAccountBalance,
            &(missing_mint.to_string(), confirmed()),
        )
        .await?;

        Ok(())
    }

    /// Checks a bounded RPC refusal while leaving transport/decoding errors as scenario failures.
    async fn expect_rejection(
        &self,
        label: &str,
        needles: &[&str],
        method: RpcRequest,
        params: &impl Serialize,
    ) -> Result<()> {
        let outcome = time::timeout(
            NEGATIVE_DEADLINE,
            self.client.try_call::<Value>(method, params),
        )
        .await
        .with_context(|| format!("{label} did not answer within {NEGATIVE_DEADLINE:?}"))??;
        let error = outcome.err().with_context(|| format!("{label} must be rejected"))?;
        ensure!(
            error.error.code.code() == -32602
                && needles.iter().any(|needle| error.error.message.contains(needle)),
            "{label}: {error}"
        );
        Ok(())
    }

    /// Checks exact result membership plus token fields where the query returns account images.
    async fn run_query(&self, query: Query) -> Result<()> {
        match query {
            Query::ProgramAccountsByMint | Query::ProgramAccountsByOwner => {
                let (offset, key, wanted) = match query {
                    Query::ProgramAccountsByMint => (
                        spl::MINT_OFFSET,
                        self.fixture.mint,
                        [self.fixture.ata_a, self.fixture.ata_b],
                    ),
                    _ => (
                        spl::OWNER_OFFSET,
                        self.fixture.owner_a,
                        [self.fixture.ata_a, self.fixture.ata_a_other],
                    ),
                };
                let filters = vec![
                    RpcFilterType::DataSize(spl::TOKEN_ACCOUNT_LEN as u64),
                    RpcFilterType::Memcmp(Memcmp::new_raw_bytes(offset, key.to_bytes().to_vec())),
                ];
                let params = (
                    spl::token_program().to_string(),
                    program_accounts_config(filters),
                );
                let accounts: Vec<RpcKeyedAccount> =
                    self.client.call(RpcRequest::GetProgramAccounts, &params).await?;
                ensure!(keys(&accounts) == expected(&wanted));
                if matches!(query, Query::ProgramAccountsByOwner) {
                    return Ok(());
                }
                for entry in &accounts {
                    let (owner, amount, delegate) =
                        if entry.pubkey == self.fixture.ata_a.to_string() {
                            (
                                self.fixture.owner_a,
                                OWNER_A_BALANCE,
                                Some((&self.fixture.delegate, DELEGATED_AMOUNT)),
                            )
                        } else {
                            (self.fixture.owner_b, OWNER_B_BALANCE, None)
                        };
                    let encoding =
                        check_token_account(entry, &self.fixture.mint, &owner, amount, delegate)?;
                    ensure!(encoding == "base64");
                }
            }
            Query::BalanceA | Query::BalanceB => {
                let (ata, amount) = match query {
                    Query::BalanceA => (self.fixture.ata_a, OWNER_A_BALANCE),
                    _ => (self.fixture.ata_b, OWNER_B_BALANCE),
                };
                let balance: UiTokenAmount = self
                    .client
                    .call_value(
                        RpcRequest::GetTokenAccountBalance,
                        &(ata.to_string(), confirmed()),
                    )
                    .await?;
                ensure!(balance.amount == amount.to_string());
                ensure!(balance.decimals == 0);
            }
            Query::OwnerAByMint
            | Query::OwnerAByProgram
            | Query::OwnerBByMint
            | Query::UnrelatedOwnerByMint => {
                let (owner, filter, wanted) = match query {
                    Query::OwnerAByMint => (
                        self.fixture.owner_a,
                        RpcTokenAccountsFilter::Mint(self.fixture.mint.to_string()),
                        vec![self.fixture.ata_a],
                    ),
                    Query::OwnerAByProgram => (
                        self.fixture.owner_a,
                        RpcTokenAccountsFilter::ProgramId(spl::token_program().to_string()),
                        vec![self.fixture.ata_a, self.fixture.ata_a_other],
                    ),
                    Query::OwnerBByMint => (
                        self.fixture.owner_b,
                        RpcTokenAccountsFilter::Mint(self.fixture.mint.to_string()),
                        vec![self.fixture.ata_b],
                    ),
                    _ => (
                        self.fixture.unrelated_owner,
                        RpcTokenAccountsFilter::Mint(self.fixture.mint.to_string()),
                        vec![],
                    ),
                };
                let accounts: Vec<RpcKeyedAccount> = self
                    .client
                    .call_value(
                        RpcRequest::GetTokenAccountsByOwner,
                        &(owner.to_string(), filter, parsed_config()),
                    )
                    .await?;
                ensure!(keys(&accounts) == expected(&wanted));
                for entry in &accounts {
                    let (mint, amount, delegate) = if entry.pubkey == self.fixture.ata_a.to_string()
                    {
                        (
                            self.fixture.mint,
                            OWNER_A_BALANCE,
                            Some((&self.fixture.delegate, DELEGATED_AMOUNT)),
                        )
                    } else if entry.pubkey == self.fixture.ata_b.to_string() {
                        (self.fixture.mint, OWNER_B_BALANCE, None)
                    } else {
                        (self.fixture.other_mint, OTHER_MINT_BALANCE, None)
                    };
                    check_token_account(entry, &mint, &owner, amount, delegate)?;
                }
            }
            Query::DelegateByMint | Query::DelegateByProgram => {
                let filter = match query {
                    Query::DelegateByMint => {
                        RpcTokenAccountsFilter::Mint(self.fixture.mint.to_string())
                    }
                    _ => RpcTokenAccountsFilter::ProgramId(spl::token_program().to_string()),
                };
                let accounts: Vec<RpcKeyedAccount> = self
                    .client
                    .call_value(
                        RpcRequest::GetTokenAccountsByDelegate,
                        &(self.fixture.delegate.to_string(), filter, parsed_config()),
                    )
                    .await?;
                ensure!(keys(&accounts) == expected(&[self.fixture.ata_a]));
                check_token_account(
                    &accounts[0],
                    &self.fixture.mint,
                    &self.fixture.owner_a,
                    OWNER_A_BALANCE,
                    Some((&self.fixture.delegate, DELEGATED_AMOUNT)),
                )?;
            }
        }
        Ok(())
    }
}
