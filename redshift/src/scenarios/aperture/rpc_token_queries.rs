use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use base64::Engine;
use futures_util::future::join_all;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, prep,
    report::Unit,
    rpc_client::nonblocking::rpc_client::RpcClient,
    rpc_client_api::{
        config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
        filter::{Memcmp, RpcFilterType},
        request::TokenAccountsFilter,
        response::RpcKeyedAccount,
    },
    system, BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
};
use signer::Signer;
use solana_account_decoder_client_types::{
    UiAccount, UiAccountData, UiAccountEncoding,
};
use solana_commitment_config::CommitmentConfig;

use crate::scenarios::chainlink::spl;

const READS: usize = 256;
const AIRDROP: u64 = 2_000_000_000;
const OWNER_A_BALANCE: u64 = 500;
const OWNER_B_BALANCE: u64 = 300;
const OTHER_MINT_BALANCE: u64 = 70;
const UNRELATED_BALANCE: u64 = 40;
const DELEGATED_AMOUNT: u64 = 200;
const DELEGATE_OFFSET: usize = 72;
const DELEGATED_AMOUNT_OFFSET: usize = 121;
const MATERIALIZATION_TIMEOUT: Duration = Duration::from_secs(30);
const BURST_DEADLINE: Duration = Duration::from_secs(20);
const NEGATIVE_DEADLINE: Duration = Duration::from_secs(5);

pub struct RpcTokenQueries;

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

#[derive(Clone, Copy)]
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
    let tag = u32::from_le_bytes(
        data.get(DELEGATE_OFFSET..DELEGATE_OFFSET + 4)?
            .try_into()
            .ok()?,
    );
    (tag == 1)
        .then(|| read_pubkey(data, DELEGATE_OFFSET + 4))
        .flatten()
}

fn raw_data(account: &UiAccount) -> Result<Vec<u8>> {
    match &account.data {
        UiAccountData::Binary(encoded, UiAccountEncoding::Base64) => {
            Ok(base64::engine::general_purpose::STANDARD.decode(encoded)?)
        }
        other => {
            Err(format!("expected base64 account data, got {other:?}").into())
        }
    }
}

struct TokenView {
    encoding: &'static str,
    mint: Option<Pubkey>,
    owner: Option<Pubkey>,
    amount: Option<u64>,
    delegate: Option<Pubkey>,
    delegated_amount: Option<u64>,
}

fn parsed_str(info: &serde_json::Value, path: &[&str]) -> Option<String> {
    let mut value = info;
    for key in path {
        value = value.get(key)?;
    }
    value.as_str().map(str::to_owned)
}

fn token_view(account: &UiAccount) -> Result<TokenView> {
    match &account.data {
        UiAccountData::Json(parsed) => {
            check_eq!(
                parsed.program.as_str(),
                "spl-token",
                "jsonParsed token accounts must be attributed to spl-token"
            )?;
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
            check_eq!(
                data.len(),
                spl::TOKEN_ACCOUNT_LEN,
                "base64 token account data length"
            )?;
            let delegate = read_delegate(&data);
            Ok(TokenView {
                encoding: "base64",
                mint: read_pubkey(&data, spl::MINT_OFFSET),
                owner: read_pubkey(&data, spl::OWNER_OFFSET),
                amount: read_u64(&data, 64),
                delegate,
                delegated_amount: delegate
                    .and_then(|_| read_u64(&data, DELEGATED_AMOUNT_OFFSET)),
            })
        }
        other => {
            Err(format!("unsupported token account encoding {other:?}").into())
        }
    }
}

fn check_token_account(
    label: &str,
    entry: &RpcKeyedAccount,
    mint: &Pubkey,
    owner: &Pubkey,
    amount: u64,
    delegate: Option<(&Pubkey, u64)>,
) -> Result<&'static str> {
    let view = token_view(&entry.account)?;
    check_eq!(
        entry.account.owner,
        spl::token_program().to_string(),
        "{label}: token account owner program"
    )?;
    check_eq!(view.mint, Some(*mint), "{label}: mint")?;
    check_eq!(view.owner, Some(*owner), "{label}: owner")?;
    check_eq!(view.amount, Some(amount), "{label}: amount")?;
    match delegate {
        Some((delegate, delegated_amount)) => {
            check_eq!(view.delegate, Some(*delegate), "{label}: delegate")?;
            check_eq!(
                view.delegated_amount,
                Some(delegated_amount),
                "{label}: delegated amount"
            )?;
        }
        None => {
            check!(view.delegate.is_none(), "{label}: no delegate expected")?
        }
    }
    Ok(view.encoding)
}

fn program_accounts_config(
    filters: Vec<RpcFilterType>,
) -> RpcProgramAccountsConfig {
    RpcProgramAccountsConfig {
        filters: Some(filters),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            data_slice: None,
            commitment: Some(confirmed()),
            min_context_slot: None,
        },
        with_context: None,
        sort_results: None,
    }
}

async fn run_query(
    client: &RpcClient,
    fixture: &Fixture,
    query: Query,
) -> Result<Option<&'static str>> {
    let mut encoding = None;
    match query {
        Query::ProgramAccountsByMint => {
            let accounts = client
                .get_program_ui_accounts_with_config(
                    &spl::token_program(),
                    program_accounts_config(vec![
                        RpcFilterType::DataSize(spl::TOKEN_ACCOUNT_LEN as u64),
                        RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                            spl::MINT_OFFSET,
                            fixture.mint.to_bytes().to_vec(),
                        )),
                    ]),
                )
                .await?;
            let listed: BTreeSet<String> = accounts
                .iter()
                .map(|(pubkey, _)| pubkey.to_string())
                .collect();
            check_eq!(
                listed,
                expected(&[fixture.ata_a, fixture.ata_b]),
                "getProgramAccounts filtered by mint must list exactly the \
                 materialized token accounts of that mint"
            )?;
            for (pubkey, account) in &accounts {
                let data = raw_data(account)?;
                check_eq!(
                    data.len(),
                    spl::TOKEN_ACCOUNT_LEN,
                    "getProgramAccounts base64 data length for {pubkey}"
                )?;
                check_eq!(
                    read_pubkey(&data, spl::MINT_OFFSET),
                    Some(fixture.mint),
                    "getProgramAccounts mint bytes for {pubkey}"
                )?;
                let (owner, amount, delegate) = if *pubkey == fixture.ata_a {
                    (fixture.owner_a, OWNER_A_BALANCE, Some(fixture.delegate))
                } else {
                    (fixture.owner_b, OWNER_B_BALANCE, None)
                };
                check_eq!(
                    read_pubkey(&data, spl::OWNER_OFFSET),
                    Some(owner),
                    "getProgramAccounts owner bytes for {pubkey}"
                )?;
                check_eq!(
                    read_u64(&data, 64),
                    Some(amount),
                    "getProgramAccounts amount bytes for {pubkey}"
                )?;
                check_eq!(
                    read_delegate(&data),
                    delegate,
                    "getProgramAccounts delegate bytes for {pubkey}"
                )?;
                if delegate.is_some() {
                    check_eq!(
                        read_u64(&data, DELEGATED_AMOUNT_OFFSET),
                        Some(DELEGATED_AMOUNT),
                        "getProgramAccounts delegated amount bytes for {pubkey}"
                    )?;
                }
            }
        }
        Query::ProgramAccountsByOwner => {
            let accounts = client
                .get_program_ui_accounts_with_config(
                    &spl::token_program(),
                    program_accounts_config(vec![
                        RpcFilterType::DataSize(spl::TOKEN_ACCOUNT_LEN as u64),
                        RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                            spl::OWNER_OFFSET,
                            fixture.owner_a.to_bytes().to_vec(),
                        )),
                    ]),
                )
                .await?;
            let listed: BTreeSet<String> = accounts
                .iter()
                .map(|(pubkey, _)| pubkey.to_string())
                .collect();
            check_eq!(
                listed,
                expected(&[fixture.ata_a, fixture.ata_a_other]),
                "getProgramAccounts filtered by owner must list both of owner \
                 A's materialized token accounts"
            )?;
        }
        Query::BalanceA | Query::BalanceB => {
            let (ata, amount) = match query {
                Query::BalanceA => (fixture.ata_a, OWNER_A_BALANCE),
                _ => (fixture.ata_b, OWNER_B_BALANCE),
            };
            let balance = client
                .get_token_account_balance_with_commitment(&ata, confirmed())
                .await?
                .value;
            check_eq!(
                balance.amount,
                amount.to_string(),
                "getTokenAccountBalance amount for {ata}"
            )?;
            check_eq!(
                balance.decimals,
                0,
                "getTokenAccountBalance decimals for {ata}"
            )?;
        }
        Query::OwnerAByMint => {
            let accounts = client
                .get_token_accounts_by_owner_with_commitment(
                    &fixture.owner_a,
                    TokenAccountsFilter::Mint(fixture.mint),
                    confirmed(),
                )
                .await?
                .value;
            check_eq!(
                keys(&accounts),
                expected(&[fixture.ata_a]),
                "getTokenAccountsByOwner(A, mint) must list exactly A's account \
                 of that mint"
            )?;
            encoding = Some(check_token_account(
                "getTokenAccountsByOwner(A, mint)",
                &accounts[0],
                &fixture.mint,
                &fixture.owner_a,
                OWNER_A_BALANCE,
                Some((&fixture.delegate, DELEGATED_AMOUNT)),
            )?);
        }
        Query::OwnerAByProgram => {
            let accounts = client
                .get_token_accounts_by_owner_with_commitment(
                    &fixture.owner_a,
                    TokenAccountsFilter::ProgramId(spl::token_program()),
                    confirmed(),
                )
                .await?
                .value;
            check_eq!(
                keys(&accounts),
                expected(&[fixture.ata_a, fixture.ata_a_other]),
                "getTokenAccountsByOwner(A, program) must list both of A's \
                 token accounts"
            )?;
            for entry in &accounts {
                let (mint, amount, delegate) =
                    if entry.pubkey == fixture.ata_a.to_string() {
                        (
                            fixture.mint,
                            OWNER_A_BALANCE,
                            Some((&fixture.delegate, DELEGATED_AMOUNT)),
                        )
                    } else {
                        (fixture.other_mint, OTHER_MINT_BALANCE, None)
                    };
                encoding = Some(check_token_account(
                    "getTokenAccountsByOwner(A, program)",
                    entry,
                    &mint,
                    &fixture.owner_a,
                    amount,
                    delegate,
                )?);
            }
        }
        Query::OwnerBByMint => {
            let accounts = client
                .get_token_accounts_by_owner_with_commitment(
                    &fixture.owner_b,
                    TokenAccountsFilter::Mint(fixture.mint),
                    confirmed(),
                )
                .await?
                .value;
            check_eq!(
                keys(&accounts),
                expected(&[fixture.ata_b]),
                "getTokenAccountsByOwner(B, mint) must list exactly B's account"
            )?;
            encoding = Some(check_token_account(
                "getTokenAccountsByOwner(B, mint)",
                &accounts[0],
                &fixture.mint,
                &fixture.owner_b,
                OWNER_B_BALANCE,
                None,
            )?);
        }
        Query::DelegateByMint | Query::DelegateByProgram => {
            let filter = match query {
                Query::DelegateByMint => {
                    TokenAccountsFilter::Mint(fixture.mint)
                }
                _ => TokenAccountsFilter::ProgramId(spl::token_program()),
            };
            let accounts = client
                .get_token_accounts_by_delegate_with_commitment(
                    &fixture.delegate,
                    filter,
                    confirmed(),
                )
                .await?
                .value;
            check_eq!(
                keys(&accounts),
                expected(&[fixture.ata_a]),
                "getTokenAccountsByDelegate must list exactly the account \
                 approved to the delegate"
            )?;
            encoding = Some(check_token_account(
                "getTokenAccountsByDelegate",
                &accounts[0],
                &fixture.mint,
                &fixture.owner_a,
                OWNER_A_BALANCE,
                Some((&fixture.delegate, DELEGATED_AMOUNT)),
            )?);
        }
        Query::UnrelatedOwnerByMint => {
            let accounts = client
                .get_token_accounts_by_owner_with_commitment(
                    &fixture.unrelated_owner,
                    TokenAccountsFilter::Mint(fixture.mint),
                    confirmed(),
                )
                .await?
                .value;
            check!(
                accounts.is_empty(),
                "an owner whose account was never materialized must not appear \
                 in getTokenAccountsByOwner, got {:?}",
                keys(&accounts)
            )?;
        }
    }
    Ok(encoding)
}

async fn expect_rejection<T: std::fmt::Debug>(
    label: &str,
    needles: &[&str],
    call: impl std::future::Future<
        Output = std::result::Result<
            T,
            redsuite_core::rpc_client_api::client_error::Error,
        >,
    >,
) -> Result<()> {
    let outcome = tokio::time::timeout(NEGATIVE_DEADLINE, call)
        .await
        .map_err(|_| {
            format!("{label} did not answer within {NEGATIVE_DEADLINE:?}")
        })?;
    match outcome {
        Ok(value) => {
            Err(format!("{label} must be rejected, got {value:?}").into())
        }
        Err(error) => {
            let message = error.to_string();
            check!(
                needles.iter().any(|needle| message.contains(needle)),
                "{label} must be rejected with one of {needles:?}, got \
                 {message:?}"
            )?;
            Ok(())
        }
    }
}

#[async_trait(?Send)]
impl Scenario for RpcTokenQueries {
    fn name(&self) -> &str {
        "redshift/rpc_token_queries"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let client = RpcClient::new_with_commitment(
            er.api().url().to_owned(),
            confirmed(),
        );
        let fee_payer = prep::funded_payer(base, AIRDROP).await?;
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
            ata_a_other: spl::derive_ata(
                &owner_a.pubkey(),
                &other_mint.pubkey(),
            ),
            ata_unrelated: spl::derive_ata(
                &unrelated_owner.pubkey(),
                &mint.pubkey(),
            ),
        };
        let payer = fee_payer.pubkey();

        base.submit_and_confirm_with(
            &fee_payer,
            &[&mint, &other_mint, &owner_a],
            &[
                system::create_account(
                    &payer,
                    &fixture.mint,
                    spl::MINT_RENT,
                    spl::MINT_LEN,
                    &spl::token_program(),
                ),
                spl::initialize_mint(&fixture.mint, &fixture.owner_a),
                system::create_account(
                    &payer,
                    &fixture.other_mint,
                    spl::MINT_RENT,
                    spl::MINT_LEN,
                    &spl::token_program(),
                ),
                spl::initialize_mint(&fixture.other_mint, &fixture.owner_a),
                spl::create_ata_idempotent(
                    &payer,
                    &fixture.owner_a,
                    &fixture.mint,
                ),
                spl::create_ata_idempotent(
                    &payer,
                    &fixture.owner_b,
                    &fixture.mint,
                ),
                spl::create_ata_idempotent(
                    &payer,
                    &fixture.owner_a,
                    &fixture.other_mint,
                ),
                spl::create_ata_idempotent(
                    &payer,
                    &fixture.unrelated_owner,
                    &fixture.mint,
                ),
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
                spl::deposit_spl_tokens(
                    &fixture.owner_a,
                    &fixture.mint,
                    OWNER_A_BALANCE,
                ),
                spl::deposit_spl_tokens(
                    &fixture.owner_b,
                    &fixture.mint,
                    OWNER_B_BALANCE,
                ),
            ],
        )
        .await?;
        base.submit_and_confirm(
            &fee_payer,
            &[
                spl::delegate_eata(
                    &payer,
                    &fixture.owner_a,
                    &fixture.mint,
                    &er.identity(),
                ),
                spl::delegate_eata(
                    &payer,
                    &fixture.owner_b,
                    &fixture.mint,
                    &er.identity(),
                ),
            ],
        )
        .await?;

        check::poll(
            "the er materializes both mints, the two projected token accounts \
             and the plain other-mint account",
            MATERIALIZATION_TIMEOUT,
            || async {
                matches!(er.account(&fixture.mint).await, Ok(Some(_)))
                    && matches!(
                        er.account(&fixture.other_mint).await,
                        Ok(Some(_))
                    )
                    && spl::token_balance(er, &fixture.ata_a)
                        .await
                        .ok()
                        .flatten()
                        == Some(OWNER_A_BALANCE)
                    && spl::token_balance(er, &fixture.ata_b)
                        .await
                        .ok()
                        .flatten()
                        == Some(OWNER_B_BALANCE)
                    && spl::token_balance(er, &fixture.ata_a_other)
                        .await
                        .ok()
                        .flatten()
                        == Some(OTHER_MINT_BALANCE)
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
        check::poll(
            "the er reports the approved delegate",
            MATERIALIZATION_TIMEOUT,
            || async {
                client
                    .get_token_accounts_by_delegate_with_commitment(
                        &fixture.delegate,
                        TokenAccountsFilter::Mint(fixture.mint),
                        confirmed(),
                    )
                    .await
                    .is_ok_and(|accounts| accounts.value.len() == 1)
            },
        )
        .await?;

        let burst_started = Instant::now();
        let reads = join_all((0..READS).map(|index| {
            run_query(&client, &fixture, CYCLE[index % CYCLE.len()])
        }));
        let outcomes = tokio::time::timeout(BURST_DEADLINE, reads)
            .await
            .map_err(|_| {
                format!(
                    "the burst of {READS} reads exceeded {BURST_DEADLINE:?}"
                )
            })?;
        let burst_elapsed = burst_started.elapsed();
        let mut failures = 0u64;
        let mut encodings = BTreeSet::new();
        for (index, outcome) in outcomes.iter().enumerate() {
            match outcome {
                Ok(Some(encoding)) => {
                    encodings.insert(*encoding);
                }
                Ok(None) => {}
                Err(error) => {
                    failures += 1;
                    eprintln!(
                        "[redsuite] {}: read {index} ({}) failed: {error}",
                        self.name(),
                        index % CYCLE.len()
                    );
                }
            }
        }
        check_eq!(
            failures,
            0,
            "every read in the burst must succeed and agree"
        )?;

        let missing_mint = Keypair::new().pubkey();
        let bogus_program = Keypair::new().pubkey();
        expect_rejection(
            "getTokenAccountsByOwner with a missing mint",
            &["mint account not found"],
            client.get_token_accounts_by_owner_with_commitment(
                &fixture.owner_a,
                TokenAccountsFilter::Mint(missing_mint),
                confirmed(),
            ),
        )
        .await?;
        expect_rejection(
            "getTokenAccountsByOwner with an invalid token program",
            &["unknown token program id"],
            client.get_token_accounts_by_owner_with_commitment(
                &fixture.owner_a,
                TokenAccountsFilter::ProgramId(bogus_program),
                confirmed(),
            ),
        )
        .await?;
        expect_rejection(
            "getTokenAccountsByDelegate with a missing mint",
            &["mint account not found"],
            client.get_token_accounts_by_delegate_with_commitment(
                &fixture.delegate,
                TokenAccountsFilter::Mint(missing_mint),
                confirmed(),
            ),
        )
        .await?;
        expect_rejection(
            "getTokenAccountsByDelegate with an invalid token program",
            &["unknown token program id"],
            client.get_token_accounts_by_delegate_with_commitment(
                &fixture.delegate,
                TokenAccountsFilter::ProgramId(bogus_program),
                confirmed(),
            ),
        )
        .await?;
        expect_rejection(
            "getTokenAccountBalance on a missing account",
            &["token account not found", "not a token account"],
            client.get_token_account_balance_with_commitment(
                &missing_mint,
                confirmed(),
            ),
        )
        .await?;

        Ok(ScenarioReport::ok(self.name())
            .setting("reads", READS)
            .setting("query kinds", CYCLE.len())
            .setting("mint", fixture.mint)
            .setting("delegate", fixture.delegate)
            .setting("unrelated account", fixture.ata_unrelated)
            .setting(
                "token account encoding served",
                encodings.into_iter().collect::<Vec<_>>().join(","),
            )
            .metric(
                "burst elapsed ms",
                Unit::Millis,
                burst_elapsed.as_secs_f64() * 1e3,
            )
            .metric("burst failures", Unit::Count, failures as f64))
    }
}
