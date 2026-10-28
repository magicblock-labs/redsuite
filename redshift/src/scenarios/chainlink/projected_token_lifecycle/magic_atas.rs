use account::Account;
use anyhow::{ensure, Context};
use redsuite_core::{dlp, expect_error, system, ChainCtx, Result};
use sdk::spl::{builders::WithdrawThroughDelegatedShuttleWithMergeBuilder, find_rent_pda};
use signer::Signer;
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
            Self::Projection => "automatic projection over empty overlay",
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
            source_balance: amount(&tokens.er, &tokens.wallets[1].ata).await?,
            extra_base: 0,
            extra_claims: 0,
            supply: SUPPLY,
        };
        let destinations: [_; Case::ALL.len()] =
            std::array::from_fn(|_| Wallet::new(&tokens.mint.pubkey()));
        suite.prepare().await?;
        for (case, destination) in Case::ALL.into_iter().zip(&destinations) {
            suite.run_case(destination, case).await?;
        }
        Ok(())
    }

    async fn prepare(&self) -> Result<()> {
        let tokens = self.tokens;
        let rent = find_rent_pda().0;
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

    #[instrument(skip_all, err, fields(case = case.label()))]
    async fn run_case(&mut self, destination: &Wallet, case: Case) -> Result<()> {
        let readonly = if case == Case::Readonly {
            Some(self.readonly(destination).await?)
        } else {
            None
        };
        self.receive(destination, readonly.as_ref().map(|_| DEPOSIT)).await?;
        match case {
            Case::Drain | Case::Readonly => {
                self.drain(destination).await?;
                self.close(destination, readonly.as_ref()).await?;
            }
            Case::Projection => {
                self.drain(destination).await?;
                self.project(destination).await?;
            }
            Case::FullShuttle | Case::TransparentShuttle => {
                self.withdraw(destination, case == Case::TransparentShuttle).await?;
            }
        }
        self.check_conservation().await
    }

    async fn readonly(&mut self, destination: &Wallet) -> Result<Account> {
        let tokens = self.tokens;
        let er = &tokens.er;
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
        let error = expect_error(attempt, "readonly balance unspendable before overlay")?;
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
        let er = &tokens.er;
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
        let er = &tokens.er;
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
        let er = &tokens.er;
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
        let base_ata = tokens.base.account(&destination.ata).await?.context("base ATA missing")?;
        er.api()
            .await_accounts(
                &[destination.ata],
                "delegation replaces the empty Magic ATA without closing it",
                |accounts| {
                    let account = accounts[0].as_ref().context("projection missing")?;
                    ensure!(!spl::is_magic_ata(&account.data));
                    ensure!(account.owner == spl::token_program());
                    ensure!(account.data.len() == spl::TOKEN_ACCOUNT_LEN);
                    ensure!(account.lamports == base_ata.lamports);
                    ensure!(spl::account_balance(account) == Some(DEPOSIT));
                    Ok(())
                },
            )
            .await?;
        Ok(())
    }

    async fn close(&self, destination: &Wallet, original: Option<&Account>) -> Result<()> {
        let tokens = self.tokens;
        let er = &tokens.er;
        let events = er.api().events().await?;
        let mut account = events.account(&destination.ata).await?;
        er.submit_and_confirm_with(&tokens.payer, &[&destination.owner], &[destination.close()])
            .await?;
        account
            .until(|update| {
                update.value.owner == system::system_id().to_string()
                    && update.value.data.decode().is_some_and(|data| data.is_empty())
            })
            .await?;
        drop(account);
        drop(events);
        er.api()
            .await_accounts(
                &[destination.ata],
                "close restores the base ATA",
                |accounts| {
                    ensure!(
                        accounts[0].as_ref() == original,
                        "expected restored ATA {original:?}, observed {:?}",
                        accounts[0]
                    );
                    Ok(())
                },
            )
            .await?;
        Ok(())
    }

    async fn withdraw(&mut self, destination: &Wallet, transparent: bool) -> Result<()> {
        let tokens = self.tokens;
        let er = &tokens.er;
        let shuttle = WithdrawThroughDelegatedShuttleWithMergeBuilder {
            payer: tokens.payer.pubkey(),
            owner: destination.owner.pubkey(),
            owner_ata: destination.ata,
            mint: tokens.mint.pubkey(),
            shuttle_id: if transparent { 4 } else { 3 },
            amount: RECEIVE,
            validator: Some(er.identity()),
        }
        .instruction();
        let shuttle_accounts: Vec<_> =
            shuttle.accounts[2..5].iter().map(|meta| meta.pubkey).collect();
        let mut instructions = vec![destination.create(tokens.payer.pubkey())];
        if transparent {
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
        if transparent {
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
        ensure!(amount(&tokens.er, &tokens.wallets[1].ata).await? == self.source_balance);
        let state = tokens.balances().await?;
        state.check_supply(
            state.projected[..2].iter().sum::<u64>() + self.extra_claims,
            self.extra_base,
            self.supply,
        )
    }
}
