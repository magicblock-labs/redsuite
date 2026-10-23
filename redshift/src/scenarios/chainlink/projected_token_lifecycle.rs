use std::{rc::Rc, time::Duration};

use anyhow::{ensure, Context};
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    check,
    dlp::{self, delegate_with_actions, DelegateArgs},
    netfault::{BaseProxies, Selector},
    programs::{SettlementMode, REDLINE},
    receipt, system,
    topology::{self, ErOptions, PrivateEr},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use sdk::spl::builders::UndelegateEphemeralAtaBuilder;
use signature::Signature;
use signer::Signer;

mod magic_atas;

use super::spl;
use crate::PAYER_LAMPORTS;
use magic_atas::MagicAtas;

const LABEL: &str = "projected-token-lifecycle";
const NAMES: [&str; 4] = ["source", "destination", "unbacked", "foreign"];
const INITIAL: [u64; 4] = [200, 100, 70, 45];
const SUPPLY: u64 = 415;
const ACTION_AMOUNT: u64 = 100;
const FAILING_AMOUNT: u64 = 1_000_000;
const TIMEOUT: Duration = Duration::from_secs(60);

struct Wallet {
    owner: Keypair,
    mint: Pubkey,
    ata: Pubkey,
    eata: Pubkey,
}

impl Wallet {
    fn new(mint: &Pubkey) -> Self {
        let owner = Keypair::new();
        Self {
            mint: *mint,
            ata: spl::derive_ata(&owner.pubkey(), mint),
            eata: spl::derive_eata(&owner.pubkey(), mint),
            owner,
        }
    }

    async fn transfer(&self, er: &ErCtx, to: &Self, amount: u64) -> Result<Signature> {
        er.submit_and_confirm(&self.owner, &[self.transfer_to(to, amount)]).await
    }

    fn transfer_to(&self, to: &Self, amount: u64) -> Instruction {
        spl::transfer(&self.ata, &to.ata, &self.owner.pubkey(), amount)
    }

    fn create(&self, payer: Pubkey) -> Instruction {
        spl::create_ata_idempotent(&payer, &self.owner.pubkey(), &self.mint)
    }

    fn mint_to(&self, payer: Pubkey, amount: u64) -> Instruction {
        spl::mint_to(&self.mint, &self.ata, &payer, amount)
    }

    fn initialize(&self, payer: Pubkey) -> Instruction {
        spl::initialize_eata(&payer, &self.owner.pubkey(), &self.mint)
    }

    fn delegate(&self, payer: Pubkey, validator: Pubkey) -> Instruction {
        spl::delegate_eata(&payer, &self.owner.pubkey(), &self.mint, &validator)
    }

    fn deposit(&self, amount: u64) -> Instruction {
        spl::deposit_spl_tokens(&self.owner.pubkey(), &self.mint, amount)
    }

    fn ensure(&self, payer: Pubkey) -> Instruction {
        spl::ensure_magic_ata(payer, self.owner.pubkey(), self.mint)
    }

    fn close(&self) -> Instruction {
        spl::close_magic_ata(self.owner.pubkey(), self.mint)
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
        self.check_supply(active, 0, SUPPLY)
    }

    fn check_supply(&self, active_claims: u64, extra_base: u64, supply: u64) -> Result<()> {
        let claims = active_claims + self.eatas[3].unwrap_or(0) + self.vault[1];
        ensure!(self.vault[0] == claims);
        let physical = self.base_atas.iter().sum::<u64>() + self.vault[0] + extra_base;
        ensure!(physical == supply);
        Ok(())
    }
}

async fn amount(ctx: &impl ChainCtx, address: &Pubkey) -> Result<u64> {
    spl::token_balance(ctx, address).await?.context("token balance missing")
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

async fn await_projection(er: &ErCtx, wallets: &[Wallet; 4], expected: &[u64; 4]) -> Result<()> {
    er.api()
        .await_accounts(
            &wallets.iter().map(|wallet| wallet.ata).collect::<Vec<_>>(),
            "projected balances converge",
            async || {
                for (wallet, expected) in wallets.iter().zip(expected) {
                    let actual = amount(er, &wallet.ata).await?;
                    ensure!(actual == *expected);
                }
                Ok(())
            },
        )
        .await
}

async fn check_controls(er: &ErCtx, wallets: &[Wallet; 4], amount: u64) -> Result<()> {
    let atas = wallets.each_ref().map(|wallet| wallet.ata);
    let before = er.accounts(&atas).await?;
    for control in [2, 3] {
        for (from, to) in [(control, 0), (0, control)] {
            let attempt = wallets[from].transfer(er, &wallets[to], amount).await;
            let error =
                check::expect_error(attempt, &format!("unauthorized transfer {from}->{to}"))?;
            let error = format!("{error:#}");
            ensure!([
                "InvalidWritableAccount",
                "ExternalAccountDataModified",
                "ReadonlyDataModified",
                "Immutable",
            ]
            .iter()
            .any(|code| error.contains(code)));
            ensure!(er.accounts(&atas).await? == before);
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
    let receipt = receipt::fetch_commit_receipt(er.api(), signature).await?;
    receipt.check_success("projected-token commit")?;
    receipt.check_accounts(expected, undelegate)?;
    receipt.confirm(base.api()).await
}

pub struct ProjectedTokenLifecycle {
    base: Rc<BaseCtx>,
    proxies: BaseProxies,
    private: PrivateEr,
}

impl PrivateErScenario for ProjectedTokenLifecycle {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let proxies = BaseProxies::spawn(&base).await?;
        let private = crate::phase(
            "boot",
            topology::private_er(
                &base,
                ErOptions {
                    label: LABEL.to_owned(),
                    base_endpoints: Some(proxies.endpoints()),
                    ..Default::default()
                },
            ),
        )
        .await?;
        Ok(Self { base, proxies, private })
    }

    async fn run(self) -> Result<()> {
        let Self { base, proxies, private } = self;
        let er = private.ctx();
        let validator = topology::identity_for_label(LABEL)?;
        let identity = validator.pubkey();
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let funder = payer.pubkey();
        let mint = Keypair::new();
        let mint_key = mint.pubkey();
        let wallets: [Wallet; 4] = std::array::from_fn(|_| Wallet::new(&mint_key));
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
                &[REDLINE
                    .batch()
                    .nonce(id)
                    .settle(identity, &atas, SettlementMode::Commit)
                    .build()],
            )
            .await
        };
        let owner_is = async |address: &Pubkey, owner| -> Result<()> {
            let actual = base.account(address).await?.map(|a| a.owner);
            ensure!(actual == Some(owner));
            Ok(())
        };
        let delegate = async |payer: &Keypair, wallet: &Wallet, validator| -> Result<()> {
            let ix = wallet.delegate(payer.pubkey(), validator);
            base.submit_and_confirm(payer, &[ix]).await?;
            owner_is(&wallet.eata, dlp::dlp_id()).await
        };
        let delegate_with_transfer = async |amount: u64| -> Result<Pubkey> {
            let account = base.funded_payer(PAYER_LAMPORTS).await?;
            let action = wallets[0].transfer_to(&wallets[1], amount);
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
        let record = async |expected: &Balances, delegated: bool| -> Result<()> {
            let observed = balances(&base, er, &wallets, &vault).await?;
            ensure!(&observed == expected);
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
                base.airdrop(&owner, PAYER_LAMPORTS).await?;
                let mut instructions =
                    vec![wallet.create(funder), wallet.mint_to(funder, INITIAL[i])];
                if i != 2 {
                    instructions.push(wallet.initialize(funder));
                }
                base.submit_and_confirm(&payer, &instructions).await?;
            }
            base.submit_and_confirm(&wallets[3].owner, &[wallets[3].deposit(40)]).await?;
            delegate(&payer, &wallets[3], foreign_validator).await?;
            for control in [2, 3] {
                let first = er
                    .api()
                    .await_accounts(
                        &[wallets[control].ata],
                        &format!("the er clones the {} ATA", NAMES[control]),
                        || amount(er, &wallets[control].ata),
                    )
                    .await?;
                ensure!(first == expected.projected[control]);
            }
            await_projection(er, &wallets, &expected.projected).await?;
            record(&expected, false).await
        })
        .await?;

        crate::phase("deposit", async {
            for (wallet, initial) in wallets[..2].iter().zip(INITIAL) {
                base.submit_and_confirm(
                    &wallet.owner,
                    &[spl::deposit_spl_tokens(&wallet.owner.pubkey(), &mint_key, initial)],
                )
                .await?;
            }
            expected.base_atas = [0, 0, 70, 5];
            expected.eatas = [Some(200), Some(100), None, Some(40)];
            expected.projected = expected.base_atas;
            expected.vault[0] = 340;
            await_projection(er, &wallets, &expected.projected).await?;
            record(&expected, false).await
        })
        .await?;

        crate::phase("delegation", async {
            for wallet in &wallets[..2] {
                delegate(&payer, wallet, identity).await?;
            }
            expected.projected = [200, 100, 70, 5];
            await_projection(er, &wallets, &expected.projected).await?;
            check_controls(er, &wallets, 1).await?;
            record(&expected, true).await
        })
        .await?;

        crate::phase("post-delegation action", async {
            let delegated = delegate_with_transfer(ACTION_AMOUNT).await?;
            expected.projected = [100, 200, 70, 5];
            await_projection(er, &wallets, &expected.projected).await?;
            owner_is(&delegated, dlp::dlp_id()).await?;
            record(&expected, true).await
        })
        .await?;

        // failing post-delegation action — an action that cannot execute
        // must route the freshly delegated account into scheduled
        // undelegation (ownership returns to system on base).
        crate::phase("failing post-delegation action", async {
            let failing = delegate_with_transfer(FAILING_AMOUNT).await?;
            base.api()
                .await_accounts(
                    &[failing],
                    "the failing action returns base ownership to the system \
                 program",
                    async || {
                        ensure!(matches!(
                            base.account(&failing).await,
                            Ok(Some(account)) if account.owner == system::system_id()
                        ));
                        Ok(())
                    },
                )
                .await?;
            record(&expected, true).await
        })
        .await?;

        crate::phase("er transfer", async {
            wallets[0].transfer(er, &wallets[1], 37).await?;
            expected.projected = [63, 237, 70, 5];
            record(&expected, true).await
        })
        .await?;

        let (signature, held, confirmations) =
            crate::phase("commit with withheld confirmation", async {
                let confirmations = [
                    proxies.stall(Selector::method("getSignatureStatuses").http().response()),
                    proxies.stall(Selector::method("signatureNotification").ws().notification()),
                ];
                let submission = proxies.intercept(
                    Selector::method("sendTransaction").http().response().account(&eatas[0]),
                );
                let signature = commit(1).await?;
                let held = submission.wait(TIMEOUT).await?;
                let landed = held.operation.signature()?;
                let tx = base.api().await_transaction(&landed).await?;
                let meta = transaction_meta(&tx)?;
                ensure!(meta.err == None);
                expected.eatas[..2].copy_from_slice(&[Some(63), Some(237)]);
                record(&expected, true).await?;
                wallets[0].transfer(er, &wallets[1], 11).await?;
                expected.projected = [52, 248, 70, 5];
                record(&expected, true).await?;
                Ok((signature, held, confirmations))
            })
            .await?;

        crate::phase("confirmation recovery", async {
            held.discard();
            proxies.close_connections();
            for rule in confirmations {
                rule.remove();
            }
            settled(&base, er, &signature, &eatas, false).await?;
            check_controls(er, &wallets, 2).await?;
            record(&expected, true).await?;
            settled(&base, er, &commit(2).await?, &eatas, false).await?;
            expected.eatas[..2].copy_from_slice(&[Some(52), Some(248)]);
            record(&expected, true).await
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
                settled(&base, er, &signature, &[wallet.eata], true).await?;
                owner_is(&wallet.eata, spl::eata_program()).await?;
            }
            expected.projected = expected.base_atas;
            await_projection(er, &wallets, &expected.projected).await?;
            record(&expected, false).await
        })
        .await?;

        crate::phase("withdrawal", async {
            base.submit_and_confirm(
                &wallets[0].owner,
                &[spl::withdraw_spl_tokens(&wallets[0].owner.pubkey(), &mint_key, 20)],
            )
            .await?;
            expected.base_atas[0] = 20;
            expected.eatas[0] = Some(32);
            expected.projected[0] = 20;
            expected.vault[0] = 320;
            await_projection(er, &wallets, &expected.projected).await?;
            record(&expected, false).await
        })
        .await?;

        crate::phase("redelegation", async {
            for wallet in &wallets[..2] {
                delegate(&wallet.owner, wallet, identity).await?;
            }
            expected.projected = [32, 248, 70, 5];
            await_projection(er, &wallets, &expected.projected).await?;
            check_controls(er, &wallets, 3).await?;
            record(&expected, true).await?;
            wallets[0].transfer(er, &wallets[1], 7).await?;
            expected.projected = [25, 255, 70, 5];
            record(&expected, true).await?;
            settled(&base, er, &commit(3).await?, &eatas, false).await?;
            expected.eatas[..2].copy_from_slice(&[Some(25), Some(255)]);
            record(&expected, true).await
        })
        .await?;

        MagicAtas::run(&base, er, &payer, mint_key, &wallets, &vault).await?;

        crate::phase("teardown", async {
            proxies.finish()?;
            private.finish().await
        })
        .await
    }
}
