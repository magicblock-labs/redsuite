use std::time::Duration;

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq,
    dlp::{self, delegate_with_actions, DelegateArgs},
    prep, system, BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
};
use signer::Signer;

use super::spl;

const AIRDROP: u64 = 2_000_000_000;
const SOURCE_BALANCE: u64 = 200;
const DESTINATION_BALANCE: u64 = 100;
const TRANSFER_AMOUNT: u64 = 100;
const PLAIN_BALANCE: u64 = 70;
const FOREIGN_BALANCE: u64 = 40;
const FAILING_AMOUNT: u64 = 1_000_000;
const PROJECTION_TIMEOUT: Duration = Duration::from_secs(20);
const ACTION_TIMEOUT: Duration = Duration::from_secs(30);

pub struct PostDelegationTokenTransfer;

#[async_trait(?Send)]
impl Scenario for PostDelegationTokenTransfer {
    fn name(&self) -> &str {
        "redshift/post_delegation_token_transfer"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let fee_payer = prep::funded_payer(base, AIRDROP).await?;
        let delegated_account = Keypair::new();
        let source_authority = Keypair::new();
        let destination_authority = Keypair::new();
        let mint = Keypair::new();
        base.airdrop(&delegated_account.pubkey(), AIRDROP).await?;
        base.airdrop(&source_authority.pubkey(), AIRDROP).await?;

        let mint_key = mint.pubkey();
        let source = source_authority.pubkey();
        let destination = destination_authority.pubkey();
        let source_ata = spl::derive_ata(&source, &mint_key);
        let destination_ata = spl::derive_ata(&destination, &mint_key);

        base.submit_and_confirm_with(
            &fee_payer,
            &[&mint, &source_authority],
            &[
                system::create_account(
                    &fee_payer.pubkey(),
                    &mint_key,
                    spl::MINT_RENT,
                    spl::MINT_LEN,
                    &spl::token_program(),
                ),
                spl::initialize_mint(&mint_key, &source),
                spl::create_ata_idempotent(
                    &fee_payer.pubkey(),
                    &source,
                    &mint_key,
                ),
                spl::create_ata_idempotent(
                    &fee_payer.pubkey(),
                    &destination,
                    &mint_key,
                ),
                spl::mint_to(&mint_key, &source_ata, &source, SOURCE_BALANCE),
                spl::mint_to(
                    &mint_key,
                    &destination_ata,
                    &source,
                    DESTINATION_BALANCE,
                ),
            ],
        )
        .await?;

        base.submit_and_confirm(
            &fee_payer,
            &[
                spl::initialize_global_vault(&fee_payer.pubkey(), &mint_key),
                spl::initialize_eata(&fee_payer.pubkey(), &source, &mint_key),
                spl::initialize_eata(
                    &fee_payer.pubkey(),
                    &destination,
                    &mint_key,
                ),
            ],
        )
        .await?;

        base.submit_and_confirm_with(
            &fee_payer,
            &[&source_authority, &destination_authority],
            &[
                spl::deposit_spl_tokens(&source, &mint_key, SOURCE_BALANCE),
                spl::deposit_spl_tokens(
                    &destination,
                    &mint_key,
                    DESTINATION_BALANCE,
                ),
            ],
        )
        .await?;

        base.submit_and_confirm(
            &fee_payer,
            &[
                spl::delegate_eata(
                    &fee_payer.pubkey(),
                    &source,
                    &mint_key,
                    &er.identity(),
                ),
                spl::delegate_eata(
                    &fee_payer.pubkey(),
                    &destination,
                    &mint_key,
                    &er.identity(),
                ),
            ],
        )
        .await?;

        check::poll(
            "the er projects the deposited eATA balances onto both ATAs",
            PROJECTION_TIMEOUT,
            || async {
                token_balance(er, &source_ata).await == Some(SOURCE_BALANCE)
                    && token_balance(er, &destination_ata).await
                        == Some(DESTINATION_BALANCE)
            },
        )
        .await?;
        check_eq!(
            token_balance(base, &source_ata).await,
            Some(0),
            "the source ATA on chain must be drained into the vault"
        )?;
        check_eq!(
            token_balance(base, &destination_ata).await,
            Some(0),
            "the destination ATA on chain must be drained into the vault"
        )?;

        let transfer_action = spl::transfer(
            &source_ata,
            &destination_ata,
            &source,
            TRANSFER_AMOUNT,
        );
        let delegate_ix = delegate_with_actions(
            &fee_payer.pubkey(),
            &delegated_account.pubkey(),
            None,
            DelegateArgs {
                commit_frequency_ms: u32::MAX,
                seeds: vec![],
                validator: Some(er.identity()),
            },
            &[transfer_action],
        );

        base.submit_and_confirm_with(
            &fee_payer,
            &[&delegated_account],
            &[system::assign(&delegated_account.pubkey(), &dlp::dlp_id())],
        )
        .await?;
        base.submit_and_confirm_with(
            &fee_payer,
            &[&delegated_account, &source_authority],
            &[delegate_ix],
        )
        .await?;

        check::poll(
            "the post-delegation transfer action moves the tokens on the er",
            ACTION_TIMEOUT,
            || async {
                token_balance(er, &source_ata).await
                    == Some(SOURCE_BALANCE - TRANSFER_AMOUNT)
                    && token_balance(er, &destination_ata).await
                        == Some(DESTINATION_BALANCE + TRANSFER_AMOUNT)
            },
        )
        .await?;

        // negative: an ATA with NO eATA must clone as the plain chain
        // account, never a projection.
        let plain_owner = Keypair::new().pubkey();
        let plain_ata = spl::derive_ata(&plain_owner, &mint_key);
        base.submit_and_confirm_with(
            &fee_payer,
            &[&source_authority],
            &[
                spl::create_ata_idempotent(
                    &fee_payer.pubkey(),
                    &plain_owner,
                    &mint_key,
                ),
                spl::mint_to(&mint_key, &plain_ata, &source, PLAIN_BALANCE),
            ],
        )
        .await?;
        check::poll(
            "the er clones the eATA-less plain ATA",
            PROJECTION_TIMEOUT,
            || async { token_balance(er, &plain_ata).await.is_some() },
        )
        .await?;
        check_eq!(
            token_balance(er, &plain_ata).await,
            Some(PLAIN_BALANCE),
            "an ATA without an eATA must present its chain balance on the er"
        )?;

        // negative: an eATA delegated to a FOREIGN validator must not be
        // substituted — the er presents the drained chain ATA, not the eATA.
        let foreign_validator = Keypair::new().pubkey();
        let foreign_authority = Keypair::new();
        let foreign = foreign_authority.pubkey();
        let foreign_ata = spl::derive_ata(&foreign, &mint_key);
        base.submit_and_confirm_with(
            &fee_payer,
            &[&source_authority],
            &[
                spl::create_ata_idempotent(
                    &fee_payer.pubkey(),
                    &foreign,
                    &mint_key,
                ),
                spl::mint_to(&mint_key, &foreign_ata, &source, FOREIGN_BALANCE),
                spl::initialize_eata(&fee_payer.pubkey(), &foreign, &mint_key),
            ],
        )
        .await?;
        base.submit_and_confirm_with(
            &fee_payer,
            &[&foreign_authority],
            &[spl::deposit_spl_tokens(
                &foreign,
                &mint_key,
                FOREIGN_BALANCE,
            )],
        )
        .await?;
        base.submit_and_confirm(
            &fee_payer,
            &[spl::delegate_eata(
                &fee_payer.pubkey(),
                &foreign,
                &mint_key,
                &foreign_validator,
            )],
        )
        .await?;
        check_eq!(
            token_balance(base, &foreign_ata).await,
            Some(0),
            "the foreign user's chain ATA must be drained into the vault"
        )?;
        check::poll(
            "the er clones the foreign-delegated ATA",
            PROJECTION_TIMEOUT,
            || async { token_balance(er, &foreign_ata).await.is_some() },
        )
        .await?;
        check_eq!(
            token_balance(er, &foreign_ata).await,
            Some(0),
            "an eATA delegated to a foreign validator must not substitute — \
             the er must present the drained chain ATA"
        )?;

        // failing post-delegation action — an action that cannot execute
        // must route the freshly delegated account into scheduled
        // undelegation (ownership returns to system on base).
        let failing_account = Keypair::new();
        base.airdrop(&failing_account.pubkey(), AIRDROP).await?;
        let failing_action = spl::transfer(
            &source_ata,
            &destination_ata,
            &source,
            FAILING_AMOUNT,
        );
        let failing_delegate = delegate_with_actions(
            &fee_payer.pubkey(),
            &failing_account.pubkey(),
            None,
            DelegateArgs {
                commit_frequency_ms: u32::MAX,
                seeds: vec![],
                validator: Some(er.identity()),
            },
            &[failing_action],
        );
        base.submit_and_confirm_with(
            &fee_payer,
            &[&failing_account],
            &[system::assign(&failing_account.pubkey(), &dlp::dlp_id())],
        )
        .await?;
        base.submit_and_confirm_with(
            &fee_payer,
            &[&failing_account, &source_authority],
            &[failing_delegate],
        )
        .await?;
        check::poll(
            "the failing action returns base ownership to the system program",
            ACTION_TIMEOUT,
            || async {
                matches!(
                    base.account(&failing_account.pubkey()).await,
                    Ok(Some(account)) if account.owner == system::system_id()
                )
            },
        )
        .await?;
        let failing_on_base = base
            .account(&failing_account.pubkey())
            .await?
            .ok_or("the failing-action account vanished on base")?;
        check_eq!(
            failing_on_base.owner,
            system::system_id(),
            "a failing post-delegation action must undelegate the account \
             back to its system owner"
        )?;
        check_eq!(
            token_balance(er, &source_ata).await,
            Some(SOURCE_BALANCE - TRANSFER_AMOUNT),
            "the failing action must not move tokens"
        )?;

        Ok(ScenarioReport::ok(self.name())
            .setting("mint decimals", 0u64)
            .setting("transfer amount", TRANSFER_AMOUNT)
            .setting(
                "source after",
                token_balance(er, &source_ata).await.unwrap_or_default(),
            )
            .setting(
                "destination after",
                token_balance(er, &destination_ata)
                    .await
                    .unwrap_or_default(),
            )
            .setting("plain ata on er", PLAIN_BALANCE)
            .setting("foreign-delegated ata on er", 0u64))
    }
}

async fn token_balance(ctx: &impl ChainCtx, account: &Pubkey) -> Option<u64> {
    spl::token_balance(ctx, account).await.ok().flatten()
}
