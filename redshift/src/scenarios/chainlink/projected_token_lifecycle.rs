use std::time::{Duration, Instant};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq,
    dlp::{self, delegate_with_actions, DelegateArgs},
    netfault::{BaseProxies, Selector},
    prep, receipt, system, topology,
    topology::ErOptions,
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use sdk::spl::{
    builders::{
        InitializeRentPdaBuilder, UndelegateEphemeralAtaBuilder,
        WithdrawThroughDelegatedShuttleWithMergeBuilder,
    },
    find_rent_pda,
};
use signature::Signature;
use signer::Signer;

use super::spl;
use crate::program::instruction::build;

const LABEL: &str = "projected-token-lifecycle";
const NAMES: [&str; 4] = ["source", "destination", "unbacked", "foreign"];
const INITIAL: [u64; 4] = [200, 100, 70, 45];
const SUPPLY: u64 = 415;
const ACTION_AMOUNT: u64 = 100;
const FAILING_AMOUNT: u64 = 1_000_000;
const TIMEOUT: Duration = Duration::from_secs(60);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);

pub struct ProjectedTokenLifecycle;

struct Wallet {
    owner: Keypair,
    ata: Pubkey,
    eata: Pubkey,
}

impl Wallet {
    fn new(mint: &Pubkey) -> Self {
        let owner = Keypair::new();
        Self {
            ata: spl::derive_ata(&owner.pubkey(), mint),
            eata: spl::derive_eata(&owner.pubkey(), mint),
            owner,
        }
    }

    async fn transfer(
        &self,
        er: &ErCtx,
        to: &Self,
        amount: u64,
    ) -> Result<Signature> {
        let ix =
            spl::transfer(&self.ata, &to.ata, &self.owner.pubkey(), amount);
        er.submit_and_confirm(&self.owner, &[ix]).await
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Balances {
    base_atas: [u64; 4],
    eatas: [Option<u64>; 4],
    projected: [u64; 4],
    vault: [u64; 2],
}

impl Balances {
    fn conserved(&self, delegated: bool) -> Result<()> {
        let active: u64 = if delegated {
            self.projected[..2].iter().sum()
        } else {
            self.eatas[..2].iter().flatten().sum()
        };
        let claims = active + self.eatas[3].unwrap_or(0) + self.vault[1];
        check_eq!(self.vault[0], claims, "the vault backs every token claim")?;
        let physical = self.base_atas.iter().sum::<u64>() + self.vault[0];
        Ok(check_eq!(physical, SUPPLY, "mint supply is conserved")?)
    }
}

async fn amount(ctx: &impl ChainCtx, address: &Pubkey) -> Result<u64> {
    spl::token_balance(ctx, address)
        .await?
        .ok_or_else(|| format!("token balance missing at {address}").into())
}

async fn balances(
    base: &BaseCtx,
    er: &ErCtx,
    wallets: &[Wallet; 4],
    vault: &[Pubkey; 2],
) -> Result<Balances> {
    let mut state = Balances::default();
    for (i, wallet) in wallets.iter().enumerate() {
        state.base_atas[i] = amount(base, &wallet.ata).await?;
        state.eatas[i] = spl::token_balance(base, &wallet.eata).await?;
        state.projected[i] = amount(er, &wallet.ata).await?;
    }
    for (i, address) in vault.iter().enumerate() {
        state.vault[i] = amount(base, address).await?;
    }
    Ok(state)
}

async fn await_projection(
    er: &ErCtx,
    wallets: &[Wallet; 4],
    expected: &[u64; 4],
) -> Result<()> {
    Ok(check::poll_for(
        "projected balances reach the current lifecycle state",
        TIMEOUT,
        || async {
            for (wallet, expected) in wallets.iter().zip(expected) {
                let actual = amount(er, &wallet.ata).await?;
                check_eq!(actual, *expected, "projection {}", wallet.ata)?;
            }
            Ok::<_, redsuite_core::DynError>(())
        },
    )
    .await?)
}

async fn check_controls(
    er: &ErCtx,
    wallets: &[Wallet; 4],
    amount: u64,
) -> Result<()> {
    let atas = wallets.each_ref().map(|wallet| wallet.ata);
    let before = er.accounts(&atas).await?;
    for control in [2, 3] {
        for (from, to) in [(control, 0), (0, control)] {
            let attempt =
                wallets[from].transfer(er, &wallets[to], amount).await;
            check!(attempt.is_err(), "unauthorized transfer {from}->{to}")?;
            let error = attempt.unwrap_err().to_string();
            check!(
                ["InvalidWritableAccount", "ExternalAccountDataModified", "ReadonlyDataModified", "Immutable"]
                    .iter().any(|code| error.contains(code)),
                "{} transfer {from}->{to} must fail for authorization, not insufficient funds or a transport error: {error}",
                NAMES[control]
            )?;
            check_eq!(er.accounts(&atas).await?, before,
                "rejected transfer {from}->{to} leaves every token account unchanged")?;
        }
    }
    Ok(())
}

async fn settled(
    base: &BaseCtx,
    er: &ErCtx,
    signature: &Signature,
    expected: &[Pubkey],
    undelegate: bool,
) -> Result<()> {
    let mut receipt =
        receipt::fetch_commit_receipt(er.api(), signature, RECEIPT_TIMEOUT)
            .await?;
    crate::scenarios::committor::require_settled(
        &receipt,
        "projected-token commit",
    )?;
    receipt.included.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    check_eq!(
        receipt.included,
        expected,
        "settlement targets eATAs, not base ATAs"
    )?;
    check!(receipt.excluded.is_empty(), "no eATA may be excluded")?;
    check_eq!(
        receipt.requested_undelegation,
        undelegate,
        "receipt lifecycle flag"
    )?;
    receipt::confirm_base_signatures(base.api(), &receipt, TIMEOUT).await
}

async fn magic_atas(
    base: &BaseCtx,
    er: &ErCtx,
    funder: &Keypair,
    mint: Pubkey,
    wallets: &[Wallet; 4],
    vault: &[Pubkey; 2],
) -> Result<()> {
    const RECEIVE: u64 = 60;
    const DEPOSIT: u64 = 45;
    let payer =
        prep::escrowed_payer(base, er.identity(), crate::PAYER_LAMPORTS)
            .await?
            .payer;
    let source = &wallets[1];
    let mut source_balance = amount(er, &source.ata).await?;
    let mut extra_base = 0;
    let mut extra_claims = 0;
    let mut supply = SUPPLY;
    let destinations: [Wallet; 5] = std::array::from_fn(|_| Wallet::new(&mint));
    let rent = find_rent_pda().0;
    if base.account(&rent).await?.is_none() {
        base.submit_and_confirm(
            funder,
            &[InitializeRentPdaBuilder {
                payer: funder.pubkey(),
            }
            .instruction()],
        )
        .await?;
    }
    base.submit_and_confirm(
        funder,
        &[
            system::transfer(&funder.pubkey(), &rent, 100_000_000),
            dlp::top_up_ephemeral_balance_for(
                &funder.pubkey(),
                &funder.pubkey(),
                10_000_000,
                255,
            ),
        ],
    )
    .await?;

    crate::phase("empty Magic ATA creation", async {
        let empty = Wallet::new(&mint);
        er.submit_and_confirm(
            &payer,
            &[spl::ensure_magic_ata(
                payer.pubkey(),
                empty.owner.pubkey(),
                mint,
            )],
        )
        .await?;
        let account = er
            .account(&empty.ata)
            .await?
            .ok_or("empty Magic ATA missing")?;
        check!(
            spl::is_magic_ata(&account.data),
            "empty creation retains the Magic ATA marker"
        )?;
        check_eq!(
            amount(er, &empty.ata).await?,
            0,
            "empty Magic ATA creation succeeds"
        )?;
        er.submit_and_confirm_with(
            &payer,
            &[&empty.owner],
            &[spl::close_magic_ata(empty.owner.pubkey(), mint)],
        )
        .await?;
        check!(
            er.account(&empty.ata).await?.is_none(),
            "explicit close removes the empty ATA"
        )?;
        Ok(())
    })
    .await?;

    for (case, destination) in destinations.iter().enumerate() {
        let label = [
            "receive/drain/close",
            "readonly overlay",
            "projection after explicit close",
            "full shuttle withdrawal",
            "transparent shuttle withdrawal",
        ][case];
        crate::phase(&format!("Magic ATA {label}: {}", destination.ata), async {
            let ensure = spl::ensure_magic_ata(payer.pubkey(), destination.owner.pubkey(), mint);
            let close = spl::close_magic_ata(destination.owner.pubkey(), mint);
            let transfer = spl::transfer(&source.ata, &destination.ata, &source.owner.pubkey(), RECEIVE);
            let restore = spl::transfer(&destination.ata, &source.ata, &destination.owner.pubkey(), RECEIVE);
            let create = spl::create_ata_idempotent(&funder.pubkey(), &destination.owner.pubkey(), &mint);
            let mint_to = spl::mint_to(&mint, &destination.ata, &funder.pubkey(), DEPOSIT);
            let readonly = if case == 1 {
                base.submit_and_confirm(funder, &[create.clone(), mint_to.clone()]).await?;
                supply += DEPOSIT;
                extra_base += DEPOSIT;
                let original = base.account(&destination.ata).await?.ok_or("base ATA missing")?;
                check_eq!(er.account(&destination.ata).await?, Some(original.clone()), "readonly ATA is initially cloned")?;
                let attempt = er.submit_and_confirm_with(&payer, &[&destination.owner, &source.owner], &[
                    spl::transfer(&destination.ata, &source.ata, &destination.owner.pubkey(), DEPOSIT),
                    ensure.clone(), transfer.clone(),
                ]).await;
                check!(attempt.is_err(), "readonly base balance cannot be spent before overlay")?;
                crate::rejection_code("readonly spend before overlay",
                    &["InvalidWritableAccount", "ExternalAccountDataModified", "ReadonlyDataModified", "Immutable"],
                    &attempt.unwrap_err().to_string())?;
                check_eq!(er.account(&destination.ata).await?, Some(original.clone()), "failed overlay transaction rolls back")?;
                check_eq!(amount(er, &source.ata).await?, source_balance, "failed overlay leaves the source unchanged")?;
                Some(original)
            } else { None };

            er.submit_and_confirm_with(&payer, &[&source.owner], &[ensure, transfer]).await?;
            check_eq!(amount(er, &destination.ata).await?, RECEIVE, "overlay contains only received tokens")?;
            check_eq!(amount(er, &source.ata).await?, source_balance - RECEIVE, "source funds the Magic ATA")?;
            check!(spl::is_magic_ata(&er.account(&destination.ata).await?.ok_or("Magic ATA missing")?.data), "received ATA is marked")?;
            check_eq!(spl::token_balance(base, &destination.ata).await?, if case == 1 { Some(DEPOSIT) } else { None }, "ER receipt leaves base unchanged")?;

            if case <= 2 {
                er.submit_and_confirm_with(&payer, &[&destination.owner], &[restore]).await?;
                check_eq!(amount(er, &source.ata).await?, source_balance, "draining restores every received token")?;
                if case == 2 {
                    base.submit_and_confirm_with(funder, &[&destination.owner], &[
                        create, mint_to,
                        spl::initialize_eata(&funder.pubkey(), &destination.owner.pubkey(), &mint),
                        spl::deposit_spl_tokens(&destination.owner.pubkey(), &mint, DEPOSIT),
                        spl::delegate_eata(&funder.pubkey(), &destination.owner.pubkey(), &mint, &er.identity()),
                    ]).await?;
                    supply += DEPOSIT;
                    extra_claims += DEPOSIT;
                    let until = Instant::now() + Duration::from_secs(5);
                    loop {
                        let account = er.account(&destination.ata).await?.ok_or("empty overlay disappeared")?;
                        check!(spl::is_magic_ata(&account.data), "projection cannot replace an existing Magic ATA")?;
                        check_eq!(amount(er, &destination.ata).await?, 0, "unclosed overlay stays empty")?;
                        if Instant::now() >= until { break; }
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
                er.submit_and_confirm_with(&payer, &[&destination.owner], &[close]).await?;
                let base_ata = base.account(&destination.ata).await?;
                check::poll_for("close restores the base ATA or permits eATA projection", TIMEOUT, || async {
                    let local = er.account(&destination.ata).await?;
                    if case == 2 {
                        let account = local.ok_or("projection missing")?;
                        check!(!spl::is_magic_ata(&account.data), "projected ATA has no Magic marker")?;
                        check_eq!(account.owner, spl::token_program(), "projected ATA owner")?;
                        check_eq!(account.data.len(), spl::TOKEN_ACCOUNT_LEN, "projected ATA layout")?;
                        check_eq!(account.lamports, base_ata.as_ref().ok_or("base ATA missing")?.lamports, "projection restores base rent")?;
                        check_eq!(amount(er, &destination.ata).await?, DEPOSIT, "close exposes delegated tokens")?;
                    } else {
                        check_eq!(local, readonly, "close restores all original bytes, lamports and ownership")?;
                    }
                    Ok::<_, redsuite_core::DynError>(())
                }).await?;
            } else {
                let shuttle = WithdrawThroughDelegatedShuttleWithMergeBuilder {
                    payer: funder.pubkey(), owner: destination.owner.pubkey(), owner_ata: destination.ata,
                    mint, shuttle_id: case as u32, amount: RECEIVE, validator: Some(er.identity()),
                }.instruction();
                let shuttle_accounts: Vec<_> = shuttle.accounts[2..5].iter().map(|meta| meta.pubkey).collect();
                let mut instructions = vec![create];
                if case == 4 {
                    instructions.extend([
                        spl::initialize_eata(&funder.pubkey(), &destination.owner.pubkey(), &mint),
                        spl::delegate_eata(&funder.pubkey(), &destination.owner.pubkey(), &mint, &er.identity()),
                    ]);
                }
                instructions.push(shuttle);
                let signature = base.submit_and_confirm_with(funder, &[&destination.owner], &instructions).await?;
                check::poll_for(&format!("shuttle {signature} settles and cleans up"), TIMEOUT, || async {
                    check_eq!(amount(base, &destination.ata).await?, RECEIVE, "shuttle delivers all tokens on base")?;
                    if let Some(local) = er.account(&destination.ata).await? {
                        check!(!spl::is_magic_ata(&local.data), "withdrawal clears the Magic ATA marker")?;
                    }
                    for address in &shuttle_accounts {
                        check!(base.account(address).await?.is_none(), "shuttle account {address} is removed")?;
                    }
                    Ok::<_, redsuite_core::DynError>(())
                }).await?;
                if case == 4 {
                    check_eq!(base.account(&destination.eata).await?.map(|a| a.owner), Some(dlp::dlp_id()), "transparent withdrawal retains delegated eATA")?;
                }
                source_balance -= RECEIVE;
                extra_base += RECEIVE;
            }
            check_eq!(amount(er, &source.ata).await?, source_balance, "final source balance")?;
            let state = balances(base, er, wallets, vault).await?;
            check_eq!(state.vault[0], state.projected[..2].iter().sum::<u64>() + state.eatas[3].unwrap_or(0) + state.vault[1] + extra_claims, "vault backs every remaining claim")?;
            check_eq!(state.base_atas.iter().sum::<u64>() + state.vault[0] + extra_base, supply, "Magic ATA lifecycle conserves token supply")?;
            Ok(())
        }).await?;
    }
    Ok(())
}

#[async_trait(?Send)]
impl PrivateErScenario for ProjectedTokenLifecycle {
    fn name(&self) -> &str {
        "redshift/projected_token_lifecycle"
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        let proxies = BaseProxies::spawn(base).await?;
        let private = crate::phase(
            "boot",
            topology::private_er(
                base,
                ErOptions {
                    label: LABEL.to_owned(),
                    env: Vec::new(),
                    request_timeout: None,
                    base_endpoints: Some(proxies.endpoints()),
                },
            ),
        )
        .await?;
        let er = private.ctx();
        let validator = topology::identity_for_label(LABEL)?;
        let identity = validator.pubkey();
        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let funder = payer.pubkey();
        let mint = Keypair::new();
        let mint_key = mint.pubkey();
        let wallets: [Wallet; 4] =
            std::array::from_fn(|_| Wallet::new(&mint_key));
        let global_vault = spl::derive_global_vault(&mint_key);
        let vault = [
            spl::derive_ata(&global_vault, &mint_key),
            spl::derive_eata(&global_vault, &mint_key),
        ];
        let atas = [wallets[0].ata, wallets[1].ata];
        let eatas = [wallets[0].eata, wallets[1].eata];
        let commit = async |id| {
            er.submit_and_confirm(
                &validator,
                &[build::commit_accounts(id, identity, &atas)],
            )
            .await
        };
        let owner_is = async |address: &Pubkey, owner| -> Result<()> {
            let actual = base.account(address).await?.map(|a| a.owner);
            Ok(check_eq!(actual, Some(owner), "owner of {address}")?)
        };
        let delegate =
            async |payer: &Keypair, wallet: &Wallet, validator| -> Result<()> {
                let ix = spl::delegate_eata(
                    &payer.pubkey(),
                    &wallet.owner.pubkey(),
                    &mint_key,
                    &validator,
                );
                base.submit_and_confirm(payer, &[ix]).await?;
                owner_is(&wallet.eata, dlp::dlp_id()).await
            };
        let delegate_with_transfer = async |amount: u64| -> Result<Pubkey> {
            let account = Keypair::new();
            base.airdrop(&account.pubkey(), crate::PAYER_LAMPORTS)
                .await?;
            let action = spl::transfer(
                &atas[0],
                &atas[1],
                &wallets[0].owner.pubkey(),
                amount,
            );
            // Publish the owner change with its record so discovery sees both.
            base.submit_and_confirm_with(
                &payer,
                &[&account, &wallets[0].owner],
                &[
                    system::assign(&account.pubkey(), &dlp::dlp_id()),
                    delegate_with_actions(
                        &funder,
                        &account.pubkey(),
                        None,
                        DelegateArgs {
                            commit_frequency_ms: u32::MAX,
                            seeds: vec![],
                            validator: Some(identity),
                        },
                        &[action],
                    ),
                ],
            )
            .await?;
            Ok(account.pubkey())
        };
        let record = async |phase: &str,
                            expected: &Balances,
                            delegated: bool|
               -> Result<()> {
            let observed = balances(base, er, &wallets, &vault).await?;
            eprintln!("[redsuite] {LABEL}: {phase}: {observed:?}");
            check_eq!(&observed, expected, "{phase}: token balances")?;
            observed.conserved(delegated)?;
            Ok(())
        };
        let mut expected = Balances {
            base_atas: [200, 100, 70, 5],
            eatas: [Some(0), Some(0), None, Some(40)],
            projected: [200, 100, 70, 5],
            vault: [40, 0],
        };
        let foreign_validator = Keypair::new().pubkey();

        crate::phase("token setup", async {
            base.submit_and_confirm_with(
                &payer,
                &[&mint],
                &[
                    spl::allocate_mint(&funder, &mint_key),
                    spl::initialize_mint(&mint_key, &funder),
                    spl::initialize_global_vault(&funder, &mint_key),
                ],
            )
            .await?;
            for (i, wallet) in wallets.iter().enumerate() {
                let owner = wallet.owner.pubkey();
                base.airdrop(&owner, crate::PAYER_LAMPORTS).await?;
                let mut instructions = vec![
                    spl::create_ata_idempotent(&funder, &owner, &mint_key),
                    spl::mint_to(&mint_key, &wallet.ata, &funder, INITIAL[i]),
                ];
                if i != 2 {
                    instructions
                        .push(spl::initialize_eata(&funder, &owner, &mint_key));
                }
                base.submit_and_confirm(&payer, &instructions).await?;
            }
            base.submit_and_confirm(
                &wallets[3].owner,
                &[spl::deposit_spl_tokens(
                    &wallets[3].owner.pubkey(),
                    &mint_key,
                    40,
                )],
            )
            .await?;
            delegate(&payer, &wallets[3], foreign_validator).await?;
            for control in [2, 3] {
                let first = check::poll_for(
                    &format!("the er clones the {} ATA", NAMES[control]),
                    TIMEOUT,
                    || amount(er, &wallets[control].ata),
                )
                .await?;
                check_eq!(
                    first,
                    expected.projected[control],
                    "the {} ATA must present its chain balance on first access",
                    NAMES[control]
                )?;
            }
            await_projection(er, &wallets, &expected.projected).await?;
            record("funded", &expected, false).await
        })
        .await?;

        crate::phase("deposit", async {
            for (wallet, initial) in wallets[..2].iter().zip(INITIAL) {
                base.submit_and_confirm(
                    &wallet.owner,
                    &[spl::deposit_spl_tokens(
                        &wallet.owner.pubkey(),
                        &mint_key,
                        initial,
                    )],
                )
                .await?;
            }
            expected.base_atas = [0, 0, 70, 5];
            expected.eatas = [Some(200), Some(100), None, Some(40)];
            expected.projected = expected.base_atas;
            expected.vault[0] = 340;
            await_projection(er, &wallets, &expected.projected).await?;
            record("deposited", &expected, false).await
        })
        .await?;

        crate::phase("delegation", async {
            for wallet in &wallets[..2] {
                delegate(&payer, wallet, identity).await?;
            }
            expected.projected = [200, 100, 70, 5];
            await_projection(er, &wallets, &expected.projected).await?;
            check_controls(er, &wallets, 1).await?;
            record("delegated", &expected, true).await
        })
        .await?;

        crate::phase("post-delegation action", async {
            let delegated = delegate_with_transfer(ACTION_AMOUNT).await?;
            expected.projected = [100, 200, 70, 5];
            await_projection(er, &wallets, &expected.projected).await?;
            owner_is(&delegated, dlp::dlp_id()).await?;
            record("post-delegation action", &expected, true).await
        })
        .await?;

        // failing post-delegation action — an action that cannot execute
        // must route the freshly delegated account into scheduled
        // undelegation (ownership returns to system on base).
        crate::phase("failing post-delegation action", async {
            let failing = delegate_with_transfer(FAILING_AMOUNT).await?;
            check::poll(
                "the failing action returns base ownership to the system \
                 program",
                TIMEOUT,
                || async {
                    matches!(
                        base.account(&failing).await,
                        Ok(Some(account)) if account.owner == system::system_id()
                    )
                },
            )
            .await?;
            record("failing post-delegation action", &expected, true).await
        })
        .await?;

        crate::phase("er transfer", async {
            wallets[0].transfer(er, &wallets[1], 37).await?;
            expected.projected = [63, 237, 70, 5];
            record("transferred", &expected, true).await
        })
        .await?;

        let (signature, held, confirmations) =
            crate::phase("commit with withheld confirmation", async {
                let confirmations = [
                    proxies.stall(
                        Selector::method("getSignatureStatuses")
                            .http()
                            .response(),
                    ),
                    proxies.stall(
                        Selector::method("signatureNotification")
                            .ws()
                            .notification(),
                    ),
                ];
                let submission = proxies.intercept(
                    Selector::method("sendTransaction")
                        .http()
                        .response()
                        .account(&eatas[0]),
                );
                let signature = commit(1).await?;
                let held = submission.wait(TIMEOUT).await?;
                let landed = held.operation.signature()?;
                let tx = base.api().await_transaction(&landed, TIMEOUT).await?;
                check!(
                    tx.err.is_none(),
                    "the intercepted commit executed on base: {:?}",
                    tx.err
                )?;
                expected.eatas[..2].copy_from_slice(&[Some(63), Some(237)]);
                record("base executed, confirmation withheld", &expected, true)
                    .await?;
                wallets[0].transfer(er, &wallets[1], 11).await?;
                expected.projected = [52, 248, 70, 5];
                record("newer ER balance", &expected, true).await?;
                Ok((signature, held, confirmations))
            })
            .await?;

        crate::phase("confirmation recovery", async {
            held.discard();
            proxies.close_connections();
            for rule in confirmations {
                rule.remove();
            }
            settled(base, er, &signature, &eatas, false).await?;
            check_controls(er, &wallets, 2).await?;
            record("recovered", &expected, true).await?;
            settled(base, er, &commit(2).await?, &eatas, false).await?;
            expected.eatas[..2].copy_from_slice(&[Some(52), Some(248)]);
            record("latest balances committed", &expected, true).await
        })
        .await?;

        crate::phase("undelegation", async {
            for wallet in &wallets[..2] {
                let signature = er
                    .submit_and_confirm(
                        &wallet.owner,
                        &[UndelegateEphemeralAtaBuilder {
                            payer: wallet.owner.pubkey(),
                            user: wallet.owner.pubkey(),
                            mint: mint_key,
                        }
                        .instruction()],
                    )
                    .await?;
                settled(base, er, &signature, &[wallet.eata], true).await?;
                owner_is(&wallet.eata, spl::eata_program()).await?;
            }
            expected.projected = expected.base_atas;
            await_projection(er, &wallets, &expected.projected).await?;
            record("undelegated", &expected, false).await
        })
        .await?;

        crate::phase("withdrawal", async {
            base.submit_and_confirm(
                &wallets[0].owner,
                &[spl::withdraw_spl_tokens(
                    &wallets[0].owner.pubkey(),
                    &mint_key,
                    20,
                )],
            )
            .await?;
            expected.base_atas[0] = 20;
            expected.eatas[0] = Some(32);
            expected.projected[0] = 20;
            expected.vault[0] = 320;
            await_projection(er, &wallets, &expected.projected).await?;
            record("withdrawn on base", &expected, false).await
        })
        .await?;

        crate::phase("redelegation", async {
            for wallet in &wallets[..2] {
                delegate(&wallet.owner, wallet, identity).await?;
            }
            expected.projected = [32, 248, 70, 5];
            await_projection(er, &wallets, &expected.projected).await?;
            check_controls(er, &wallets, 3).await?;
            record("redelegated", &expected, true).await?;
            wallets[0].transfer(er, &wallets[1], 7).await?;
            expected.projected = [25, 255, 70, 5];
            record("redelegated transfer", &expected, true).await?;
            settled(base, er, &commit(3).await?, &eatas, false).await?;
            expected.eatas[..2].copy_from_slice(&[Some(25), Some(255)]);
            record("final committed balances", &expected, true).await
        })
        .await?;

        magic_atas(base, er, &payer, mint_key, &wallets, &vault).await?;

        crate::phase("teardown", async {
            proxies.finish()?;
            private.finish().await
        })
        .await
    }
}
