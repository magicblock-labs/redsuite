use std::time::{Duration, Instant};

use account::Account;
use anyhow::{ensure, Context};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{check, dlp, system, BaseCtx, ChainCtx, DynError, ErCtx, Result};
use sdk::spl::{
    builders::{InitializeRentPdaBuilder, WithdrawThroughDelegatedShuttleWithMergeBuilder},
    find_rent_pda,
};
use signer::Signer;
use tokio::time;

use super::{amount, balances, spl, Wallet, SUPPLY, TIMEOUT};

const RECEIVE: u64 = 60;
const DEPOSIT: u64 = 45;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Case {
    Drain,
    Readonly,
    Projection,
    FullShuttle,
    TransparentShuttle,
}

impl Case {
    const ALL: [Self; 5] = [
        Self::Drain,
        Self::Readonly,
        Self::Projection,
        Self::FullShuttle,
        Self::TransparentShuttle,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Drain => "receive/drain/close",
            Self::Readonly => "readonly overlay",
            Self::Projection => "projection after explicit close",
            Self::FullShuttle => "full shuttle withdrawal",
            Self::TransparentShuttle => "transparent shuttle withdrawal",
        }
    }
}

pub(super) struct MagicAtas<'a> {
    base: &'a BaseCtx,
    er: &'a ErCtx,
    funder: &'a Keypair,
    mint: Pubkey,
    wallets: &'a [Wallet; 4],
    vault: &'a [Pubkey; 2],
    source_balance: u64,
    extra_base: u64,
    extra_claims: u64,
    supply: u64,
}

impl MagicAtas<'_> {
    pub(super) async fn run(
        base: &BaseCtx,
        er: &ErCtx,
        funder: &Keypair,
        mint: Pubkey,
        wallets: &[Wallet; 4],
        vault: &[Pubkey; 2],
    ) -> Result<()> {
        let mut suite = MagicAtas {
            base,
            er,
            funder,
            mint,
            wallets,
            vault,
            source_balance: amount(er, &wallets[1].ata).await?,
            extra_base: 0,
            extra_claims: 0,
            supply: SUPPLY,
        };
        let destinations: [Wallet; 5] = std::array::from_fn(|_| Wallet::new(&mint));
        suite.prepare().await?;
        crate::phase("empty Magic ATA creation", suite.empty()).await?;
        for (case, destination) in Case::ALL.into_iter().zip(&destinations) {
            crate::phase(case.label(), suite.run_case(destination, case)).await?;
        }
        Ok(())
    }

    async fn prepare(&self) -> Result<()> {
        let rent = find_rent_pda().0;
        if self.base.account(&rent).await?.is_none() {
            self.base
                .submit_and_confirm(
                    self.funder,
                    &[InitializeRentPdaBuilder { payer: self.funder.pubkey() }.instruction()],
                )
                .await?;
        }
        self.base
            .submit_and_confirm(
                self.funder,
                &[
                    system::transfer(&self.funder.pubkey(), &rent, 100_000_000),
                    dlp::top_up_ephemeral_balance(&self.funder.pubkey(), 10_000_000, 255),
                ],
            )
            .await?;
        Ok(())
    }

    async fn empty(&self) -> Result<()> {
        let empty = Wallet::new(&self.mint);
        self.er
            .submit_and_confirm(self.funder, &[empty.ensure(self.funder.pubkey())])
            .await?;
        let account = self.er.account(&empty.ata).await?.context("empty Magic ATA missing")?;
        ensure!(
            spl::is_magic_ata(&account.data),
            "empty creation retains the Magic ATA marker"
        );
        ensure!(amount(self.er, &empty.ata).await? == 0);
        self.close(&empty).await?;
        ensure!(
            self.er.account(&empty.ata).await?.is_none(),
            "explicit close removes the empty ATA"
        );
        Ok(())
    }

    async fn run_case(&mut self, destination: &Wallet, case: Case) -> Result<()> {
        let readonly = if case == Case::Readonly {
            Some(self.readonly(destination).await?)
        } else {
            None
        };
        self.receive(destination, readonly.as_ref().map(|_| DEPOSIT)).await?;
        match case {
            Case::FullShuttle | Case::TransparentShuttle => {
                self.withdraw(destination, case).await?
            }
            _ => {
                self.drain(destination).await?;
                if case == Case::Projection {
                    self.project(destination).await?;
                }
                self.close(destination).await?;
                if case == Case::Projection {
                    self.await_projection(destination).await?;
                } else {
                    self.await_restored(destination, readonly).await?;
                }
            }
        }
        self.check_conservation().await
    }

    async fn readonly(&mut self, destination: &Wallet) -> Result<Account> {
        let source = &self.wallets[1];
        self.base
            .submit_and_confirm(
                self.funder,
                &[
                    destination.create(self.funder.pubkey()),
                    destination.mint_to(self.funder.pubkey(), DEPOSIT),
                ],
            )
            .await?;
        self.supply += DEPOSIT;
        self.extra_base += DEPOSIT;
        let original = self.base.account(&destination.ata).await?.context("base ATA missing")?;
        ensure!(self.er.account(&destination.ata).await?.as_ref() == Some(&original));
        let attempt = self
            .er
            .submit_and_confirm_with(
                self.funder,
                &[&destination.owner, &source.owner],
                &[
                    destination.transfer_to(source, DEPOSIT),
                    destination.ensure(self.funder.pubkey()),
                    source.transfer_to(destination, RECEIVE),
                ],
            )
            .await;
        let error = check::expect_error(attempt, "readonly balance unspendable before overlay")?;
        crate::rejection_code(
            "readonly spend before overlay",
            &[
                "InvalidWritableAccount",
                "ExternalAccountDataModified",
                "ReadonlyDataModified",
                "Immutable",
            ],
            &format!("{error:#}"),
        )?;
        ensure!(self.er.account(&destination.ata).await?.as_ref() == Some(&original));
        ensure!(amount(self.er, &source.ata).await? == self.source_balance);
        Ok(original)
    }

    async fn receive(&self, destination: &Wallet, expected_base: Option<u64>) -> Result<()> {
        let source = &self.wallets[1];
        self.er
            .submit_and_confirm_with(
                self.funder,
                &[&source.owner],
                &[
                    destination.ensure(self.funder.pubkey()),
                    source.transfer_to(destination, RECEIVE),
                ],
            )
            .await?;
        ensure!(amount(self.er, &destination.ata).await? == RECEIVE);
        ensure!(amount(self.er, &source.ata).await? == (self.source_balance - RECEIVE));
        let account = self.er.account(&destination.ata).await?.context("Magic ATA missing")?;
        ensure!(spl::is_magic_ata(&account.data), "received ATA is marked");
        ensure!(spl::token_balance(self.base, &destination.ata).await? == expected_base);
        Ok(())
    }

    async fn drain(&self, destination: &Wallet) -> Result<()> {
        let source = &self.wallets[1];
        self.er
            .submit_and_confirm_with(
                self.funder,
                &[&destination.owner],
                &[destination.transfer_to(source, RECEIVE)],
            )
            .await?;
        ensure!(amount(self.er, &source.ata).await? == self.source_balance);
        Ok(())
    }

    async fn project(&mut self, destination: &Wallet) -> Result<()> {
        self.base
            .submit_and_confirm_with(
                self.funder,
                &[&destination.owner],
                &[
                    destination.create(self.funder.pubkey()),
                    destination.mint_to(self.funder.pubkey(), DEPOSIT),
                    destination.initialize(self.funder.pubkey()),
                    destination.deposit(DEPOSIT),
                    destination.delegate(self.funder.pubkey(), self.er.identity()),
                ],
            )
            .await?;
        self.supply += DEPOSIT;
        self.extra_claims += DEPOSIT;
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            let account =
                self.er.account(&destination.ata).await?.context("empty overlay disappeared")?;
            ensure!(
                spl::is_magic_ata(&account.data),
                "projection cannot replace an existing Magic ATA"
            );
            ensure!(amount(self.er, &destination.ata).await? == 0);
            if Instant::now() >= until {
                break;
            }
            time::sleep(Duration::from_millis(250)).await;
        }
        Ok(())
    }

    async fn close(&self, destination: &Wallet) -> Result<()> {
        self.er
            .submit_and_confirm_with(self.funder, &[&destination.owner], &[destination.close()])
            .await?;
        Ok(())
    }

    async fn await_restored(&self, destination: &Wallet, original: Option<Account>) -> Result<()> {
        check::poll_for(
            "close restores the base ATA or permits eATA projection",
            TIMEOUT,
            || async {
                ensure!(self.er.account(&destination.ata).await? == original);
                Ok::<_, DynError>(())
            },
        )
        .await?;
        Ok(())
    }

    async fn await_projection(&self, destination: &Wallet) -> Result<()> {
        let base_ata = self.base.account(&destination.ata).await?.context("base ATA missing")?;
        check::poll_for(
            "close restores the base ATA or permits eATA projection",
            TIMEOUT,
            || async {
                let account =
                    self.er.account(&destination.ata).await?.context("projection missing")?;
                ensure!(
                    !spl::is_magic_ata(&account.data),
                    "projected ATA has no Magic marker"
                );
                ensure!(account.owner == spl::token_program());
                ensure!(account.data.len() == spl::TOKEN_ACCOUNT_LEN);
                ensure!(account.lamports == base_ata.lamports);
                ensure!(amount(self.er, &destination.ata).await? == DEPOSIT);
                Ok::<_, DynError>(())
            },
        )
        .await?;
        Ok(())
    }

    async fn withdraw(&mut self, destination: &Wallet, case: Case) -> Result<()> {
        let shuttle = WithdrawThroughDelegatedShuttleWithMergeBuilder {
            payer: self.funder.pubkey(),
            owner: destination.owner.pubkey(),
            owner_ata: destination.ata,
            mint: self.mint,
            shuttle_id: if case == Case::FullShuttle { 3 } else { 4 },
            amount: RECEIVE,
            validator: Some(self.er.identity()),
        }
        .instruction();
        let shuttle_accounts: Vec<_> =
            shuttle.accounts[2..5].iter().map(|meta| meta.pubkey).collect();
        let mut instructions = vec![destination.create(self.funder.pubkey())];
        if case == Case::TransparentShuttle {
            instructions.extend([
                destination.initialize(self.funder.pubkey()),
                destination.delegate(self.funder.pubkey(), self.er.identity()),
            ]);
        }
        instructions.push(shuttle);
        let signature = self
            .base
            .submit_and_confirm_with(self.funder, &[&destination.owner], &instructions)
            .await?;
        check::poll_for(
            &format!("shuttle {signature} settles and cleans up"),
            TIMEOUT,
            || async {
                ensure!(amount(self.base, &destination.ata).await? == RECEIVE);
                if let Some(local) = self.er.account(&destination.ata).await? {
                    ensure!(
                        !spl::is_magic_ata(&local.data),
                        "withdrawal clears the Magic ATA marker"
                    );
                }
                for address in &shuttle_accounts {
                    ensure!(
                        self.base.account(address).await?.is_none(),
                        "shuttle account {address} is removed"
                    );
                }
                Ok::<_, DynError>(())
            },
        )
        .await?;
        if case == Case::TransparentShuttle {
            ensure!(
                self.base.account(&destination.eata).await?.map(|a| a.owner) == Some(dlp::dlp_id())
            );
        }
        self.source_balance -= RECEIVE;
        self.extra_base += RECEIVE;
        Ok(())
    }

    async fn check_conservation(&self) -> Result<()> {
        ensure!(amount(self.er, &self.wallets[1].ata).await? == self.source_balance);
        let state = balances(self.base, self.er, self.wallets, self.vault).await?;
        state.check_supply(
            state.projected[..2].iter().sum::<u64>() + self.extra_claims,
            self.extra_base,
            self.supply,
        )
    }
}
