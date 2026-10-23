use std::{rc::Rc, time::Duration};

use anyhow::{ensure, Context};
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::transaction_meta,
    dlp::{self, delegate_with_actions, DelegateArgs},
    expect_error,
    netfault::{BaseProxies, Held, RuleHandle, Selector},
    programs::{SettlementMode, REDLINE},
    receipt, system, topology, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};
use sdk::spl::builders::UndelegateEphemeralAtaBuilder;
use signature::Signature;
use signer::Signer;

mod magic_atas;
use tracing::instrument;

use super::spl;
use crate::PAYER_LAMPORTS;
use magic_atas::MagicAtas;

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

pub enum Mode {
    Backing,
    Overlay,
}

/// Checks backing settlement or an independently prepared Magic ATA overlay lifecycle.
pub struct ProjectedTokenLifecycle {
    base: Rc<BaseCtx>,
    proxies: Rc<BaseProxies>,
    er: ErCtx,
    mode: Mode,
    validator: Keypair,
    payer: Keypair,
    mint: Keypair,
    wallets: [Wallet; 4],
    vault: [Pubkey; 2],
}

impl Scenario<(), Mode> for ProjectedTokenLifecycle {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, mode: Mode) -> Result<Self> {
        let proxies = er.http_proxy()?;
        let validator = topology::identity_keypair(&er.identity())?;
        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let mint = Keypair::new();
        let mint_key = mint.pubkey();
        let wallets = std::array::from_fn(|_| Wallet::new(&mint_key));
        let global_vault = spl::derive_global_vault(&mint_key);
        let vault = [
            spl::derive_ata(&global_vault, &mint_key),
            spl::derive_eata(&global_vault, &mint_key),
        ];
        Ok(Self {
            base,
            proxies,
            er,
            mode,
            validator,
            payer,
            mint,
            wallets,
            vault,
        })
    }

    async fn run(self) -> Result<()> {
        let er = &self.er;
        let mut expected = Balances {
            base_atas: [200, 100, 70, 5],
            eatas: [Some(0), Some(0), None, Some(40)],
            projected: [200, 100, 70, 5],
            vault: [40, 0],
        };
        self.create_tokens(&expected).await?;
        self.check_balances(&expected, false).await?;

        self.deposit().await?;
        expected.base_atas = [0, 0, 70, 5];
        expected.eatas = [Some(200), Some(100), None, Some(40)];
        expected.projected = expected.base_atas;
        expected.vault[0] = 340;
        self.await_projection(&expected.projected).await?;
        self.check_balances(&expected, false).await?;

        self.delegate_wallets().await?;
        expected.projected = [200, 100, 70, 5];
        self.await_projection(&expected.projected).await?;
        if matches!(self.mode, Mode::Overlay) {
            self.wallets[0].transfer(er, &self.wallets[1], 155).await?;
            MagicAtas::run(&self).await?;
            return self.finish().await;
        }
        self.check_controls(1).await?;
        self.check_balances(&expected, true).await?;

        let delegated = self.delegate_with_transfer(ACTION_AMOUNT).await?;
        expected.projected = [100, 200, 70, 5];
        self.await_projection(&expected.projected).await?;
        self.check_owner(&delegated, dlp::dlp_id()).await?;
        self.check_balances(&expected, true).await?;

        self.check_failing_action().await?;
        self.check_balances(&expected, true).await?;

        self.wallets[0].transfer(er, &self.wallets[1], 37).await?;
        expected.projected = [63, 237, 70, 5];
        self.check_balances(&expected, true).await?;

        let (signature, held, confirmations) = self.commit_withheld().await?;
        expected.eatas[..2].copy_from_slice(&[Some(63), Some(237)]);
        self.check_balances(&expected, true).await?;
        self.wallets[0].transfer(er, &self.wallets[1], 11).await?;
        expected.projected = [52, 248, 70, 5];
        self.check_balances(&expected, true).await?;

        self.recover_confirmation(signature, held, confirmations).await?;
        self.check_controls(2).await?;
        self.check_balances(&expected, true).await?;
        let eatas = [self.wallets[0].eata, self.wallets[1].eata];
        self.settled(&self.commit(2).await?, &eatas, false).await?;
        expected.eatas[..2].copy_from_slice(&[Some(52), Some(248)]);
        self.check_balances(&expected, true).await?;

        self.undelegate_wallets().await?;
        expected.projected = expected.base_atas;
        self.await_projection(&expected.projected).await?;
        self.check_balances(&expected, false).await?;

        self.withdraw().await?;
        expected.base_atas[0] = 20;
        expected.eatas[0] = Some(32);
        expected.projected[0] = 20;
        expected.vault[0] = 320;
        self.await_projection(&expected.projected).await?;
        self.check_balances(&expected, false).await?;

        self.redelegate().await?;
        expected.projected = [32, 248, 70, 5];
        self.await_projection(&expected.projected).await?;
        self.check_controls(3).await?;
        self.check_balances(&expected, true).await?;
        self.wallets[0].transfer(er, &self.wallets[1], 7).await?;
        expected.projected = [25, 255, 70, 5];
        self.check_balances(&expected, true).await?;
        let signature = self.commit(3).await?;
        self.settled(&signature, &eatas, false).await?;
        expected.eatas[..2].copy_from_slice(&[Some(25), Some(255)]);
        self.check_balances(&expected, true).await?;

        self.finish().await
    }
}

impl ProjectedTokenLifecycle {
    #[instrument(skip_all, err)]
    async fn create_tokens(&self, expected: &Balances) -> Result<()> {
        let er = &self.er;
        let foreign_validator = Keypair::new().pubkey();
        self.base
            .submit_and_confirm_with(
                &self.payer,
                &[&self.mint],
                &[
                    spl::allocate_mint(&self.payer.pubkey(), &self.mint.pubkey()),
                    spl::initialize_mint(&self.mint.pubkey(), &self.payer.pubkey()),
                    spl::initialize_global_vault(&self.payer.pubkey(), &self.mint.pubkey()),
                ],
            )
            .await?;
        for (i, wallet) in self.wallets.iter().enumerate() {
            let owner = wallet.owner.pubkey();
            self.base.airdrop(&owner, PAYER_LAMPORTS).await?;
            let mut instructions = vec![
                wallet.create(self.payer.pubkey()),
                wallet.mint_to(self.payer.pubkey(), INITIAL[i]),
            ];
            if i != 2 {
                instructions.push(wallet.initialize(self.payer.pubkey()));
            }
            self.base.submit_and_confirm(&self.payer, &instructions).await?;
        }
        self.base
            .submit_and_confirm(&self.wallets[3].owner, &[self.wallets[3].deposit(40)])
            .await?;
        self.delegate(&self.payer, &self.wallets[3], foreign_validator).await?;
        for control in [2, 3] {
            let first = er
                .api()
                .await_accounts(
                    &[self.wallets[control].ata],
                    &format!("the er clones the {} ATA", NAMES[control]),
                    |accounts| {
                        let account = accounts[0].as_ref().context("ATA missing")?;
                        spl::account_balance(account).context("token balance missing")
                    },
                )
                .await?;
            ensure!(first == expected.projected[control]);
        }
        self.await_projection(&expected.projected).await?;
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn deposit(&self) -> Result<()> {
        for (wallet, initial) in self.wallets[..2].iter().zip(INITIAL) {
            self.base.submit_and_confirm(&wallet.owner, &[wallet.deposit(initial)]).await?;
        }
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn delegate_wallets(&self) -> Result<()> {
        for wallet in &self.wallets[..2] {
            self.delegate(&self.payer, wallet, self.validator.pubkey()).await?;
        }

        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn check_failing_action(&self) -> Result<()> {
        // A failing post-delegation action schedules undelegation, returning
        // base ownership to the system program.
        let failing = self.delegate_with_transfer(FAILING_AMOUNT).await?;
        self.base
            .api()
            .await_accounts(
                &[failing],
                "the failing action returns base ownership to the system \
                 program",
                |accounts| {
                    ensure!(matches!(
                        accounts[0].as_ref(),
                        Some(account) if account.owner == system::system_id()
                    ));
                    Ok(())
                },
            )
            .await?;
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn commit_withheld(&self) -> Result<(Signature, Held, [RuleHandle; 2])> {
        let confirmations = [
            self.proxies.stall(Selector::method("getSignatureStatuses").http().response()),
            self.proxies
                .stall(Selector::method("signatureNotification").ws().notification()),
        ];
        let submission = self.proxies.intercept(
            Selector::method("sendTransaction")
                .http()
                .response()
                .account(&self.wallets[0].eata),
        );
        let signature = self.commit(1).await?;
        let held = submission.wait(TIMEOUT).await?;
        let landed = held.operation.signature()?;
        let tx = self.base.api().await_transaction(&landed).await?;
        let meta = transaction_meta(&tx)?;
        ensure!(meta.err == None);
        Ok((signature, held, confirmations))
    }

    #[instrument(skip_all, err)]
    async fn recover_confirmation(
        &self,
        signature: Signature,
        held: Held,
        confirmations: [RuleHandle; 2],
    ) -> Result<()> {
        held.discard();
        self.proxies.close_connections();
        for rule in confirmations {
            rule.remove();
        }
        self.settled(
            &signature,
            &[self.wallets[0].eata, self.wallets[1].eata],
            false,
        )
        .await
    }

    #[instrument(skip_all, err)]
    async fn undelegate_wallets(&self) -> Result<()> {
        for wallet in &self.wallets[..2] {
            let signature = self
                .er
                .submit_and_confirm(
                    &wallet.owner,
                    &[UndelegateEphemeralAtaBuilder {
                        payer: wallet.owner.pubkey(),
                        user: wallet.owner.pubkey(),
                        mint: self.mint.pubkey(),
                    }
                    .instruction()],
                )
                .await?;
            self.settled(&signature, &[wallet.eata], true).await?;
            self.check_owner(&wallet.eata, spl::eata_program()).await?;
        }
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn withdraw(&self) -> Result<()> {
        self.base
            .submit_and_confirm(
                &self.wallets[0].owner,
                &[spl::withdraw_spl_tokens(
                    &self.wallets[0].owner.pubkey(),
                    &self.mint.pubkey(),
                    20,
                )],
            )
            .await?;
        Ok(())
    }

    async fn commit(&self, id: u64) -> Result<Signature> {
        self.er
            .submit_and_confirm(
                &self.validator,
                &[REDLINE
                    .batch()
                    .nonce(id)
                    .settle(
                        self.validator.pubkey(),
                        &[self.wallets[0].ata, self.wallets[1].ata],
                        SettlementMode::Commit,
                    )
                    .build()],
            )
            .await
    }

    async fn check_owner(&self, address: &Pubkey, owner: Pubkey) -> Result<()> {
        let actual = self.base.account(address).await?.map(|a| a.owner);
        ensure!(actual == Some(owner));
        Ok(())
    }

    async fn delegate(&self, payer: &Keypair, wallet: &Wallet, validator: Pubkey) -> Result<()> {
        let ix = wallet.delegate(payer.pubkey(), validator);
        self.base.submit_and_confirm(payer, &[ix]).await?;
        self.check_owner(&wallet.eata, dlp::dlp_id()).await
    }

    #[instrument(skip_all, err)]
    async fn delegate_with_transfer(&self, amount: u64) -> Result<Pubkey> {
        let account = self.base.funded_payer(PAYER_LAMPORTS).await?;
        let action = self.wallets[0].transfer_to(&self.wallets[1], amount);
        // Publish the owner change with its record so discovery sees both.
        self.base
            .submit_and_confirm_with(
                &self.payer,
                &[&account, &self.wallets[0].owner],
                &[
                    system::assign(&account.pubkey(), &dlp::dlp_id()),
                    delegate_with_actions(
                        &self.payer.pubkey(),
                        &account.pubkey(),
                        None,
                        DelegateArgs {
                            commit_frequency_ms: u32::MAX,
                            seeds: vec![],
                            validator: Some(self.validator.pubkey()),
                        },
                        &[action],
                    ),
                ],
            )
            .await?;
        Ok(account.pubkey())
    }

    #[instrument(skip_all, err)]
    async fn check_balances(&self, expected: &Balances, _delegated: bool) -> Result<()> {
        let observed = self.balances().await?;
        ensure!(&observed == expected);
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn finish(self) -> Result<()> {
        self.proxies.reset().await
    }

    async fn redelegate(&self) -> Result<()> {
        for wallet in &self.wallets[..2] {
            self.delegate(&wallet.owner, wallet, self.validator.pubkey()).await?;
        }
        Ok(())
    }

    async fn balances(&self) -> Result<Balances> {
        let mut state = Balances::default();
        for (i, wallet) in self.wallets.iter().enumerate() {
            state.base_atas[i] = amount(self.base.as_ref(), &wallet.ata).await?;
            state.eatas[i] = spl::token_balance(self.base.as_ref(), &wallet.eata).await?;
            state.projected[i] = amount(&self.er, &wallet.ata).await?;
        }
        for (i, address) in self.vault.iter().enumerate() {
            state.vault[i] = amount(self.base.as_ref(), address).await?;
        }
        Ok(state)
    }

    async fn await_projection(&self, expected: &[u64; 4]) -> Result<()> {
        self.er
            .api()
            .await_accounts(
                &self.wallets.iter().map(|wallet| wallet.ata).collect::<Vec<_>>(),
                "projected balances converge",
                |accounts| {
                    for (account, expected) in accounts.iter().zip(expected) {
                        let actual = spl::account_balance(account.as_ref().context("ATA missing")?)
                            .context("token balance missing")?;
                        ensure!(actual == *expected);
                    }
                    Ok(())
                },
            )
            .await
    }

    async fn check_controls(&self, amount: u64) -> Result<()> {
        let atas = self.wallets.each_ref().map(|wallet| wallet.ata);
        let before = self.er.accounts(&atas).await?;
        for control in [2, 3] {
            for (from, to) in [(control, 0), (0, control)] {
                let attempt =
                    self.wallets[from].transfer(&self.er, &self.wallets[to], amount).await;
                let error = expect_error(attempt, &format!("unauthorized transfer {from}->{to}"))?;
                let error = format!("{error:#}");
                ensure!([
                    "InvalidWritableAccount",
                    "ExternalAccountDataModified",
                    "ReadonlyDataModified",
                    "Immutable",
                ]
                .iter()
                .any(|code| error.contains(code)));
                ensure!(self.er.accounts(&atas).await? == before);
            }
        }
        Ok(())
    }

    async fn settled(
        &self,
        signature: &Signature,
        expected: &[Pubkey],
        undelegate: bool,
    ) -> Result<()> {
        let receipt = receipt::fetch_commit_receipt(self.er.api(), signature).await?;
        receipt.check_success("projected-token commit")?;
        receipt.check_accounts(expected, undelegate)?;
        receipt.confirm(self.base.api()).await
    }
}
