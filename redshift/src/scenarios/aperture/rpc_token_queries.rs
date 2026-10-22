use std::{collections::BTreeSet, future::Future, rc::Rc, result, time::Duration};

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
    Api, BaseCtx, ChainCtx, DynError, ErCtx, Result, Scenario,
};
use serde_json::Value;
use signer::Signer;
use solana_account_decoder_client_types::{
    token::UiTokenAmount, UiAccount, UiAccountData, UiAccountEncoding,
};
use solana_commitment_config::CommitmentConfig;
use tokio::time;

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
const BURST_DEADLINE: Duration = Duration::from_secs(20);
const NEGATIVE_DEADLINE: Duration = Duration::from_secs(5);

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

async fn run_query(client: &Api, fixture: &Fixture, query: Query) -> Result<()> {
    match query {
        Query::ProgramAccountsByMint | Query::ProgramAccountsByOwner => {
            let (offset, key, wanted) = match query {
                Query::ProgramAccountsByMint => (
                    spl::MINT_OFFSET,
                    fixture.mint,
                    [fixture.ata_a, fixture.ata_b],
                ),
                _ => (
                    spl::OWNER_OFFSET,
                    fixture.owner_a,
                    [fixture.ata_a, fixture.ata_a_other],
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
                client.call(RpcRequest::GetProgramAccounts, &params).await?;
            ensure!(keys(&accounts) == expected(&wanted));
            if matches!(query, Query::ProgramAccountsByOwner) {
                return Ok(());
            }
            for entry in &accounts {
                let (owner, amount, delegate) = if entry.pubkey == fixture.ata_a.to_string() {
                    (
                        fixture.owner_a,
                        OWNER_A_BALANCE,
                        Some((&fixture.delegate, DELEGATED_AMOUNT)),
                    )
                } else {
                    (fixture.owner_b, OWNER_B_BALANCE, None)
                };
                let encoding = check_token_account(entry, &fixture.mint, &owner, amount, delegate)?;
                ensure!(encoding == "base64");
            }
        }
        Query::BalanceA | Query::BalanceB => {
            let (ata, amount) = match query {
                Query::BalanceA => (fixture.ata_a, OWNER_A_BALANCE),
                _ => (fixture.ata_b, OWNER_B_BALANCE),
            };
            let balance: UiTokenAmount = client
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
                    fixture.owner_a,
                    RpcTokenAccountsFilter::Mint(fixture.mint.to_string()),
                    vec![fixture.ata_a],
                ),
                Query::OwnerAByProgram => (
                    fixture.owner_a,
                    RpcTokenAccountsFilter::ProgramId(spl::token_program().to_string()),
                    vec![fixture.ata_a, fixture.ata_a_other],
                ),
                Query::OwnerBByMint => (
                    fixture.owner_b,
                    RpcTokenAccountsFilter::Mint(fixture.mint.to_string()),
                    vec![fixture.ata_b],
                ),
                _ => (
                    fixture.unrelated_owner,
                    RpcTokenAccountsFilter::Mint(fixture.mint.to_string()),
                    vec![],
                ),
            };
            let accounts: Vec<RpcKeyedAccount> = client
                .call_value(
                    RpcRequest::GetTokenAccountsByOwner,
                    &(owner.to_string(), filter, parsed_config()),
                )
                .await?;
            ensure!(keys(&accounts) == expected(&wanted));
            for entry in &accounts {
                let (mint, amount, delegate) = if entry.pubkey == fixture.ata_a.to_string() {
                    (
                        fixture.mint,
                        OWNER_A_BALANCE,
                        Some((&fixture.delegate, DELEGATED_AMOUNT)),
                    )
                } else if entry.pubkey == fixture.ata_b.to_string() {
                    (fixture.mint, OWNER_B_BALANCE, None)
                } else {
                    (fixture.other_mint, OTHER_MINT_BALANCE, None)
                };
                check_token_account(entry, &mint, &owner, amount, delegate)?;
            }
        }
        Query::DelegateByMint | Query::DelegateByProgram => {
            let filter = match query {
                Query::DelegateByMint => RpcTokenAccountsFilter::Mint(fixture.mint.to_string()),
                _ => RpcTokenAccountsFilter::ProgramId(spl::token_program().to_string()),
            };
            let accounts: Vec<RpcKeyedAccount> = client
                .call_value(
                    RpcRequest::GetTokenAccountsByDelegate,
                    &(fixture.delegate.to_string(), filter, parsed_config()),
                )
                .await?;
            ensure!(keys(&accounts) == expected(&[fixture.ata_a]));
            check_token_account(
                &accounts[0],
                &fixture.mint,
                &fixture.owner_a,
                OWNER_A_BALANCE,
                Some((&fixture.delegate, DELEGATED_AMOUNT)),
            )?;
        }
    }
    Ok(())
}

async fn expect_rejection<T, Call>(label: &str, needles: &[&str], call: Call) -> Result<()>
where
    Call: Future<Output = result::Result<T, DynError>>,
{
    let outcome = time::timeout(NEGATIVE_DEADLINE, call)
        .await
        .with_context(|| format!("{label} did not answer within {NEGATIVE_DEADLINE:?}"))?;
    match outcome {
        Ok(_) => Err(anyhow!("{label} must be rejected")),
        Err(error) => {
            let message = format!("{error:#}");
            ensure!(needles.iter().any(|needle| message.contains(needle)));
            Ok(())
        }
    }
}

pub struct RpcTokenQueries {
    client: Api,
    fixture: Fixture,
}

impl Scenario for RpcTokenQueries {
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
                async || {
                    for mint in [fixture.mint, fixture.other_mint] {
                        ensure!(er.account(&mint).await?.is_some());
                    }
                    for (address, amount) in [
                        (fixture.ata_a, OWNER_A_BALANCE),
                        (fixture.ata_b, OWNER_B_BALANCE),
                        (fixture.ata_a_other, OTHER_MINT_BALANCE),
                    ] {
                        ensure!(spl::token_balance(&er, &address).await? == Some(amount));
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
        client
            .await_accounts(
                &[fixture.ata_a],
                "the er reports the approved delegate",
                async || {
                    let filter = RpcTokenAccountsFilter::Mint(fixture.mint.to_string());
                    let params = (fixture.delegate.to_string(), filter, parsed_config());
                    let accounts: Vec<RpcKeyedAccount> =
                        client.call_value(RpcRequest::GetTokenAccountsByDelegate, &params).await?;
                    ensure!(accounts.len() == 1);
                    Ok(())
                },
            )
            .await?;

        Ok(Self { client, fixture })
    }

    async fn run(self) -> Result<()> {
        let Self { client, fixture } = self;
        let reads = join_all(
            (0..READS).map(|index| run_query(&client, &fixture, CYCLE[index % CYCLE.len()])),
        );
        let outcomes = time::timeout(BURST_DEADLINE, reads)
            .await
            .with_context(|| format!("the burst of {READS} reads exceeded {BURST_DEADLINE:?}"))?;
        for outcome in outcomes {
            outcome?;
        }

        let missing_mint = Keypair::new().pubkey();
        let bogus_program = Keypair::new().pubkey();
        expect_rejection(
            "getTokenAccountsByOwner with a missing mint",
            &["mint account not found"],
            client.call_value::<Vec<RpcKeyedAccount>>(
                RpcRequest::GetTokenAccountsByOwner,
                &(
                    fixture.owner_a.to_string(),
                    RpcTokenAccountsFilter::Mint(missing_mint.to_string()),
                    parsed_config(),
                ),
            ),
        )
        .await?;
        expect_rejection(
            "getTokenAccountsByOwner with an invalid token program",
            &["unknown token program id"],
            client.call_value::<Vec<RpcKeyedAccount>>(
                RpcRequest::GetTokenAccountsByOwner,
                &(
                    fixture.owner_a.to_string(),
                    RpcTokenAccountsFilter::ProgramId(bogus_program.to_string()),
                    parsed_config(),
                ),
            ),
        )
        .await?;
        expect_rejection(
            "getTokenAccountsByDelegate with a missing mint",
            &["mint account not found"],
            client.call_value::<Vec<RpcKeyedAccount>>(
                RpcRequest::GetTokenAccountsByDelegate,
                &(
                    fixture.delegate.to_string(),
                    RpcTokenAccountsFilter::Mint(missing_mint.to_string()),
                    parsed_config(),
                ),
            ),
        )
        .await?;
        expect_rejection(
            "getTokenAccountsByDelegate with an invalid token program",
            &["unknown token program id"],
            client.call_value::<Vec<RpcKeyedAccount>>(
                RpcRequest::GetTokenAccountsByDelegate,
                &(
                    fixture.delegate.to_string(),
                    RpcTokenAccountsFilter::ProgramId(bogus_program.to_string()),
                    parsed_config(),
                ),
            ),
        )
        .await?;
        expect_rejection(
            "getTokenAccountBalance on a missing account",
            &["token account not found", "not a token account"],
            client.call_value::<UiTokenAmount>(
                RpcRequest::GetTokenAccountBalance,
                &(missing_mint.to_string(), confirmed()),
            ),
        )
        .await?;

        Ok(())
    }
}
