use std::time::{Duration, Instant};

use account::Account;
use anyhow::{ensure, Context};
use redsuite_core::{check, dlp, system, ChainCtx, Result};
use sdk::spl::{
    builders::{InitializeRentPdaBuilder, WithdrawThroughDelegatedShuttleWithMergeBuilder},
    find_rent_pda,
};
use signer::Signer;
use tokio::time;
use tracing::instrument;

use super::{amount, spl, ProjectedTokenLifecycle, Wallet, SUPPLY};

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
    tokens: &'a ProjectedTokenLifecycle,
    source_balance: u64,
    extra_base: u64,
    extra_claims: u64,
    supply: u64,
}

impl MagicAtas<'_> {
    pub(super) async fn run(tokens: &ProjectedTokenLifecycle) -> Result<()> {
        let mut suite = MagicAtas {
            tokens,
            source_balance: amount(tokens.private.ctx(), &tokens.wallets[1].ata).await?,
            extra_base: 0,
            extra_claims: 0,
            supply: SUPPLY,
        };
        let destinations: [Wallet; 5] = std::array::from_fn(|_| Wallet::new(&tokens.mint.pubkey()));
        suite.prepare().await?;
        suite.empty().await?;
        for (case, destination) in Case::ALL.into_iter().zip(&destinations) {
            suite.run_case(destination, case).await?;
        }
        Ok(())
    }

    async fn prepare(&self) -> Result<()> {
        let tokens = self.tokens;
        let rent = find_rent_pda().0;
        if tokens.base.account(&rent).await?.is_none() {
            tokens
                .base
                .submit_and_confirm(
                    &tokens.payer,
                    &[InitializeRentPdaBuilder { payer: tokens.payer.pubkey() }.instruction()],
                )
                .await?;
        }
        tokens
            .base
            .submit_and_confirm(
                &tokens.payer,
                &[
                    system::transfer(&tokens.payer.pubkey(), &rent, 100_000_000),
                    dlp::top_up_ephemeral_balance(&tokens.payer.pubkey(), 10_000_000, 255),
                ],
            )
            .await?;
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn empty(&self) -> Result<()> {
        let tokens = self.tokens;
        let er = tokens.private.ctx();
        let empty = Wallet::new(&tokens.mint.pubkey());
        er.submit_and_confirm(&tokens.payer, &[empty.ensure(tokens.payer.pubkey())])
            .await?;
        let account = er.account(&empty.ata).await?.context("empty Magic ATA missing")?;
        ensure!(spl::is_magic_ata(&account.data));
        ensure!(amount(er, &empty.ata).await? == 0);
        self.close(&empty).await?;
        ensure!(er.account(&empty.ata).await?.is_none());
        Ok(())
    }

    #[instrument(skip_all, err, fields(case = case.label()))]
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
        let tokens = self.tokens;
        let er = tokens.private.ctx();
        let source = &tokens.wallets[1];
        tokens
            .base
            .submit_and_confirm(
                &tokens.payer,
                &[
                    destination.create(tokens.payer.pubkey()),
                    destination.mint_to(tokens.payer.pubkey(), DEPOSIT),
                ],
            )
            .await?;
        self.supply += DEPOSIT;
        self.extra_base += DEPOSIT;
        let original = tokens.base.account(&destination.ata).await?.context("base ATA missing")?;
        ensure!(er.account(&destination.ata).await?.as_ref() == Some(&original));
        let attempt = er
            .submit_and_confirm_with(
                &tokens.payer,
                &[&destination.owner, &source.owner],
                &[
                    destination.transfer_to(source, DEPOSIT),
                    destination.ensure(tokens.payer.pubkey()),
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
        ensure!(er.account(&destination.ata).await?.as_ref() == Some(&original));
        ensure!(amount(er, &source.ata).await? == self.source_balance);
        Ok(original)
    }

    async fn receive(&self, destination: &Wallet, expected_base: Option<u64>) -> Result<()> {
        let tokens = self.tokens;
        let er = tokens.private.ctx();
        let source = &tokens.wallets[1];
        er.submit_and_confirm_with(
            &tokens.payer,
            &[&source.owner],
            &[
                destination.ensure(tokens.payer.pubkey()),
                source.transfer_to(destination, RECEIVE),
            ],
        )
        .await?;
        ensure!(amount(er, &destination.ata).await? == RECEIVE);
        ensure!(amount(er, &source.ata).await? == (self.source_balance - RECEIVE));
        let account = er.account(&destination.ata).await?.context("Magic ATA missing")?;
        ensure!(spl::is_magic_ata(&account.data));
        ensure!(spl::token_balance(tokens.base.as_ref(), &destination.ata).await? == expected_base);
        Ok(())
    }

    async fn drain(&self, destination: &Wallet) -> Result<()> {
        let tokens = self.tokens;
        let er = tokens.private.ctx();
        let source = &tokens.wallets[1];
        er.submit_and_confirm_with(
            &tokens.payer,
            &[&destination.owner],
            &[destination.transfer_to(source, RECEIVE)],
        )
        .await?;
        ensure!(amount(er, &source.ata).await? == self.source_balance);
        Ok(())
    }

    async fn project(&mut self, destination: &Wallet) -> Result<()> {
        let tokens = self.tokens;
        let er = tokens.private.ctx();
        tokens
            .base
            .submit_and_confirm_with(
                &tokens.payer,
                &[&destination.owner],
                &[
                    destination.create(tokens.payer.pubkey()),
                    destination.mint_to(tokens.payer.pubkey(), DEPOSIT),
                    destination.initialize(tokens.payer.pubkey()),
                    destination.deposit(DEPOSIT),
                    destination.delegate(tokens.payer.pubkey(), er.identity()),
                ],
            )
            .await?;
        self.supply += DEPOSIT;
        self.extra_claims += DEPOSIT;
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            let account =
                er.account(&destination.ata).await?.context("empty overlay disappeared")?;
            ensure!(spl::is_magic_ata(&account.data));
            ensure!(amount(er, &destination.ata).await? == 0);
            if Instant::now() >= until {
                break;
            }
            time::sleep(Duration::from_millis(250)).await;
        }
        Ok(())
    }

    async fn close(&self, destination: &Wallet) -> Result<()> {
        let tokens = self.tokens;
        tokens
            .private
            .ctx()
            .submit_and_confirm_with(&tokens.payer, &[&destination.owner], &[destination.close()])
            .await?;
        Ok(())
    }

    async fn await_restored(&self, destination: &Wallet, original: Option<Account>) -> Result<()> {
        let er = self.tokens.private.ctx();
        er.api()
            .await_accounts(
                &[destination.ata],
                "close restores the base ATA or permits eATA projection",
                async || {
                    ensure!(er.account(&destination.ata).await? == original);
                    Ok(())
                },
            )
            .await?;
        Ok(())
    }

    async fn await_projection(&self, destination: &Wallet) -> Result<()> {
        let tokens = self.tokens;
        let er = tokens.private.ctx();
        let base_ata = tokens.base.account(&destination.ata).await?.context("base ATA missing")?;
        er.api()
            .await_accounts(
                &[destination.ata],
                "close restores the base ATA or permits eATA projection",
                async || {
                    let account =
                        er.account(&destination.ata).await?.context("projection missing")?;
                    ensure!(!spl::is_magic_ata(&account.data));
                    ensure!(account.owner == spl::token_program());
                    ensure!(account.data.len() == spl::TOKEN_ACCOUNT_LEN);
                    ensure!(account.lamports == base_ata.lamports);
                    ensure!(amount(er, &destination.ata).await? == DEPOSIT);
                    Ok(())
                },
            )
            .await?;
        Ok(())
    }

    async fn withdraw(&mut self, destination: &Wallet, case: Case) -> Result<()> {
        let tokens = self.tokens;
        let er = tokens.private.ctx();
        let shuttle = WithdrawThroughDelegatedShuttleWithMergeBuilder {
            payer: tokens.payer.pubkey(),
            owner: destination.owner.pubkey(),
            owner_ata: destination.ata,
            mint: tokens.mint.pubkey(),
            shuttle_id: if case == Case::FullShuttle { 3 } else { 4 },
            amount: RECEIVE,
            validator: Some(er.identity()),
        }
        .instruction();
        let shuttle_accounts: Vec<_> =
            shuttle.accounts[2..5].iter().map(|meta| meta.pubkey).collect();
        let mut instructions = vec![destination.create(tokens.payer.pubkey())];
        if case == Case::TransparentShuttle {
            instructions.extend([
                destination.initialize(tokens.payer.pubkey()),
                destination.delegate(tokens.payer.pubkey(), er.identity()),
            ]);
        }
        instructions.push(shuttle);
        tokens
            .base
            .submit_and_confirm_with(&tokens.payer, &[&destination.owner], &instructions)
            .await?;
        tokens
            .base
            .api()
            .await_query("shuttle settlement and cleanup", async || {
                ensure!(amount(tokens.base.as_ref(), &destination.ata).await? == RECEIVE);
                if let Some(local) = er.account(&destination.ata).await? {
                    ensure!(!spl::is_magic_ata(&local.data));
                }
                for address in &shuttle_accounts {
                    ensure!(tokens.base.account(address).await?.is_none());
                }
                Ok(())
            })
            .await?;
        if case == Case::TransparentShuttle {
            ensure!(
                tokens.base.account(&destination.eata).await?.map(|a| a.owner)
                    == Some(dlp::dlp_id())
            );
        }
        self.source_balance -= RECEIVE;
        self.extra_base += RECEIVE;
        Ok(())
    }

    async fn check_conservation(&self) -> Result<()> {
        let tokens = self.tokens;
        ensure!(amount(tokens.private.ctx(), &tokens.wallets[1].ata).await? == self.source_balance);
        let state = tokens.balances().await?;
        state.check_supply(
            state.projected[..2].iter().sum::<u64>() + self.extra_claims,
            self.extra_base,
            self.supply,
        )
    }
}
