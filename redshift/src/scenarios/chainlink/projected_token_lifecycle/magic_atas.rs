use std::{slice, time::Duration};

use account::Account;
use anyhow::{ensure, Context};
use pubkey::Pubkey;
use redsuite_core::{dlp, system, wait, ChainCtx, Result};
use sdk::spl::{builders::WithdrawThroughDelegatedShuttleWithMergeBuilder, find_rent_pda};
use signer::Signer;
use tokio::time;
use tracing::instrument;

use super::{amount, check_readonly, spl, ProjectedTokenLifecycle, Wallet, SUPPLY};

const RECEIVE: u64 = 60;
const DEPOSIT: u64 = 45;
const ALLOWANCE: u64 = 25;
const RETURNED: u64 = 10;
const DELEGATE: Pubkey = Pubkey::new_from_array([1; 32]);
const OTHER_DELEGATE: Pubkey = Pubkey::new_from_array([2; 32]);
const SETTLE: Duration = Duration::from_secs(2);

/// Distinct ways a temporary token overlay can drain, close, be replaced, or
/// settle through a shuttle.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Case {
    Drain,
    Readonly,
    Projection,
    Funded,
    Shared,
    Conflict,
    FrozenOverlay,
    FrozenBase,
    Overlap,
    FullShuttle,
    TransparentShuttle,
}

impl Case {
    const ALL: [Self; 11] = [
        Self::Drain,
        Self::Readonly,
        Self::Projection,
        Self::Funded,
        Self::Shared,
        Self::Conflict,
        Self::FrozenOverlay,
        Self::FrozenBase,
        Self::Overlap,
        Self::FullShuttle,
        Self::TransparentShuttle,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Drain => "receive/drain/close",
            Self::Readonly => "readonly overlay",
            Self::Projection => "automatic projection over empty overlay",
            Self::Funded => "funded takeover with a sole approval",
            Self::Shared => "takeover merging a shared approval",
            Self::Conflict => "takeover refused for conflicting approvals",
            Self::FrozenOverlay => "funded takeover refused for a frozen overlay",
            Self::FrozenBase => "funded takeover refused for a frozen base ATA",
            Self::Overlap => "transfer and revoke overlapping takeover",
            Self::FullShuttle => "full shuttle withdrawal",
            Self::TransparentShuttle => "transparent shuttle withdrawal",
        }
    }

    fn merged(self) -> Option<(u64, Option<(Pubkey, u64)>)> {
        let funded = RECEIVE + DEPOSIT;
        match self {
            Self::Projection => Some((DEPOSIT, None)),
            Self::Funded => Some((funded, Some((DELEGATE, ALLOWANCE)))),
            Self::Shared => Some((funded, Some((DELEGATE, ALLOWANCE + DEPOSIT)))),
            Self::Overlap => Some((funded - RETURNED, None)),
            _ => None,
        }
    }
}

/// Tracks overlay claims and additional base tokens separately from the shared fixture supply.
pub(super) struct MagicAtas<'a> {
    tokens: &'a ProjectedTokenLifecycle,
    source_balance: u64,
    extra_base: u64,
    extra_claims: u64,
    supply: u64,
}

impl MagicAtas<'_> {
    /// Gives each overlay case a fresh destination while reconciling conservation after every case.
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

    /// Receives into an overlay, then exercises its case-specific terminal path.
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
                self.drain(destination, RECEIVE).await?;
                self.close(destination, readonly.as_ref()).await?;
            }
            Case::FullShuttle | Case::TransparentShuttle => {
                self.withdraw(destination, case == Case::TransparentShuttle).await?;
            }
            _ => self.take_over(destination, case).await?,
        }
        self.check_conservation().await
    }

    /// Seeds a readonly base balance and proves it is unspendable before overlay creation.
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
        let error = er
            .try_submit_and_confirm_with(
                &tokens.payer,
                &[&destination.owner, &source.owner],
                &[
                    destination.transfer_to(source, DEPOSIT),
                    destination.ensure(tokens.payer.pubkey()),
                    source.transfer_to(destination, RECEIVE),
                ],
            )
            .await?
            .err()
            .context("readonly balance spent before overlay")?;
        check_readonly(&error)?;
        ensure!(er.account(&destination.ata).await?.as_ref() == Some(&original));
        ensure!(amount(er, &source.ata).await? == self.source_balance);
        Ok(original)
    }

    /// Creates an ER-local Magic ATA without changing any pre-existing physical base balance.
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

    async fn drain(&self, destination: &Wallet, drained: u64) -> Result<()> {
        let tokens = self.tokens;
        let er = &tokens.er;
        let source = &tokens.wallets[1];
        er.submit_and_confirm_with(
            &tokens.payer,
            &[&destination.owner],
            &[destination.transfer_to(source, drained)],
        )
        .await?;
        ensure!(amount(er, &source.ata).await? == self.source_balance);
        Ok(())
    }

    async fn take_over(&mut self, destination: &Wallet, case: Case) -> Result<()> {
        let tokens = self.tokens;
        let er = &tokens.er;
        let payer = tokens.payer.pubkey();
        let owner = [&destination.owner];
        if case == Case::Projection {
            self.drain(destination, RECEIVE).await?;
        } else {
            let mut local = vec![destination.approve(DELEGATE, ALLOWANCE)];
            local.extend((case == Case::FrozenOverlay).then(|| destination.freeze(payer)));
            er.submit_and_confirm_with(&tokens.payer, &owner, &local).await?;
        }
        let overlay = er.account(&destination.ata).await?;
        let mut backing = vec![destination.create(payer), destination.mint_to(payer, DEPOSIT)];
        backing.extend([destination.initialize(payer), destination.deposit(DEPOSIT)]);
        backing.extend(match case {
            Case::Shared => Some(destination.approve(DELEGATE, 2 * DEPOSIT)),
            Case::Conflict => Some(destination.approve(OTHER_DELEGATE, ALLOWANCE)),
            Case::FrozenBase => Some(destination.freeze(payer)),
            _ => None,
        });
        backing.push(destination.delegate(payer, er.identity()));
        let delegation = tokens.base.submit_and_confirm_with(&tokens.payer, &owner, &backing);
        if case == Case::Overlap {
            let returned = [destination.transfer_to(&tokens.wallets[1], RETURNED)];
            let transfer = er.submit_and_confirm_with(&tokens.payer, &owner, &returned);
            tokio::try_join!(delegation, transfer)?;
            er.submit_and_confirm_with(&tokens.payer, &owner, &[destination.revoke()])
                .await?;
        } else {
            delegation.await?;
        }
        self.supply += DEPOSIT;
        let Some((balance, approval)) = case.merged() else {
            time::sleep(SETTLE).await;
            ensure!(er.account(&destination.ata).await? == overlay);
            self.source_balance -= RECEIVE;
            self.extra_claims += RECEIVE + DEPOSIT;
            return Ok(());
        };
        let base_ata = tokens.base.account(&destination.ata).await?.context("base ATA missing")?;
        er.api()
            .await_accounts(
                &[destination.ata],
                "delegation takes over the Magic ATA without closing it",
                |accounts| {
                    let account = accounts[0].as_ref().context("projection missing")?;
                    ensure!(!spl::is_magic_ata(&account.data));
                    ensure!(account.owner == spl::token_program());
                    ensure!(account.data.len() == spl::TOKEN_ACCOUNT_LEN);
                    ensure!(account.lamports == base_ata.lamports);
                    ensure!(spl::account_balance(account) == Some(balance));
                    ensure!(spl::approval(&account.data) == approval);
                    Ok(())
                },
            )
            .await?;
        if case == Case::Funded {
            let signature = tokens.commit(1, slice::from_ref(destination)).await?;
            tokens.settled(&signature, &[destination.eata], false).await?;
            ensure!(amount(tokens.base.as_ref(), &destination.eata).await? == balance);
            time::sleep(SETTLE).await;
            ensure!(amount(er, &destination.ata).await? == balance);
        }
        self.source_balance += DEPOSIT;
        self.drain(destination, balance).await
    }

    /// Observes overlay closure before checking that the original base ATA becomes visible again.
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

    /// Settles tokens through a shuttle and waits for both overlay and shuttle-account cleanup.
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
                let balance = amount(tokens.base.as_ref(), &destination.ata).await?;
                let local = er.account(&destination.ata).await?;
                let shuttles = tokens.base.accounts(&shuttle_accounts).await?;
                let projected =
                    local.as_ref().is_some_and(|account| spl::is_magic_ata(&account.data));
                let cleaned = shuttles == vec![None; shuttle_accounts.len()];
                let ready = balance == RECEIVE && !projected && cleaned;
                Ok(wait::when(ready, (balance, projected, cleaned)))
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

    /// Reconciles all overlay cases with the shared physical-supply and backing-claim oracle.
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
