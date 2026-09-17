use std::time::Duration;

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, dlp,
    netfault::{self, BaseProxies, Selector},
    prep, receipt, system, topology,
    topology::ErOptions,
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport,
};
use sdk::spl::builders::UndelegateEphemeralAtaBuilder;
use signature::Signature;
use signer::Signer;

use super::spl;
use crate::program::instruction::build;

const LABEL: &str = "projected-token-lifecycle";
const NAMES: [&str; 4] = ["source", "destination", "unbacked", "foreign"];
const INITIAL: [u64; 4] = [200, 100, 70, 45];
const SUPPLY: u64 = 415;
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
    check!(receipt.succeeded(), "commit failed: {receipt:?}")?;
    receipt.included.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    check_eq!(
        receipt.included,
        expected,
        "settlement targets eATAs, not base ATAs"
    )?;
    check!(receipt.excluded.is_empty(), "no eATA may be excluded")?;
    check!(!receipt.base_signatures.is_empty(), "missing base txs")?;
    check_eq!(
        receipt.requested_undelegation,
        undelegate,
        "receipt lifecycle flag"
    )?;
    receipt::confirm_base_signatures(base.api(), &receipt, TIMEOUT).await
}

#[async_trait(?Send)]
impl PrivateErScenario for ProjectedTokenLifecycle {
    fn name(&self) -> &str {
        "redshift/projected_token_lifecycle"
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let proxies = BaseProxies::spawn(base).await?;
        let private = topology::private_er(
            base,
            ErOptions {
                label: LABEL.to_owned(),
                env: Vec::new(),
                request_timeout: None,
                base_endpoints: Some(proxies.endpoints()),
            },
        )
        .await?;
        let er = private.ctx();
        let validator = topology::identity_for_label(LABEL)?;
        let identity = validator.pubkey();
        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let funder = payer.pubkey();
        let mint = Keypair::new();
        let mint_key = mint.pubkey();
        let wallets = std::array::from_fn(|_| Wallet::new(&mint_key));
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
        let owner_is = async |wallet: &Wallet, owner| -> Result<()> {
            let actual = base.account(&wallet.eata).await?.map(|a| a.owner);
            Ok(check_eq!(
                actual,
                Some(owner),
                "eATA {} owner",
                wallet.eata
            )?)
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
                owner_is(wallet, dlp::dlp_id()).await
            };
        let mut report = ScenarioReport::ok(self.name())
            .setting("mint", mint_key)
            .setting("balance order", NAMES.join(", "))
            .setting("vault ATA", vault[0])
            .setting("vault eATA", vault[1])
            .setting("commit authority", identity);

        base.submit_and_confirm_with(
            &payer,
            &[&mint],
            &[
                system::create_account(
                    &funder,
                    &mint_key,
                    spl::MINT_RENT,
                    spl::MINT_LEN,
                    &spl::token_program(),
                ),
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
            report = report
                .setting(format!("{} ATA", NAMES[i]), wallet.ata)
                .setting(format!("{} eATA", NAMES[i]), wallet.eata);
        }
        let mut record = async |phase: &str,
                                expected: &Balances,
                                delegated: bool|
               -> Result<()> {
            let observed = balances(base, er, &wallets, &vault).await?;
            eprintln!("[redsuite] {LABEL}: {phase}: {observed:?}");
            check_eq!(&observed, expected, "{phase}: token balances")?;
            observed.conserved(delegated)?;
            report
                .config
                .push((phase.to_owned(), format!("{observed:?}")));
            Ok(())
        };
        let mut expected = Balances {
            base_atas: INITIAL,
            eatas: [Some(0), Some(0), None, Some(0)],
            projected: INITIAL,
            vault: [0, 0],
        };
        await_projection(er, &wallets, &expected.projected).await?;
        record("funded", &expected, false).await?;

        for i in [0, 1, 3] {
            base.submit_and_confirm(
                &wallets[i].owner,
                &[spl::deposit_spl_tokens(
                    &wallets[i].owner.pubkey(),
                    &mint_key,
                    if i == 3 { 40 } else { INITIAL[i] },
                )],
            )
            .await?;
        }
        expected.base_atas = [0, 0, 70, 5];
        expected.eatas = [Some(200), Some(100), None, Some(40)];
        expected.projected = expected.base_atas;
        expected.vault[0] = 340;
        await_projection(er, &wallets, &expected.projected).await?;
        record("deposited", &expected, false).await?;

        let foreign_validator = Keypair::new().pubkey();
        for (i, target) in
            [(0, identity), (1, identity), (3, foreign_validator)]
        {
            delegate(&payer, &wallets[i], target).await?;
        }
        expected.projected = [200, 100, 70, 5];
        await_projection(er, &wallets, &expected.projected).await?;
        check_controls(er, &wallets, 1).await?;
        record("delegated", &expected, true).await?;

        wallets[0].transfer(er, &wallets[1], 37).await?;
        expected.projected = [163, 137, 70, 5];
        record("transferred", &expected, true).await?;

        let confirmations = [
            proxies.stall(
                Selector::method("getSignatureStatuses").http().response(),
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
        expected.eatas[..2].copy_from_slice(&[Some(163), Some(137)]);
        record("base executed, confirmation withheld", &expected, true).await?;

        wallets[0].transfer(er, &wallets[1], 11).await?;
        expected.projected = [152, 148, 70, 5];
        record("newer ER balance", &expected, true).await?;
        held.discard();
        proxies.close_connections();
        for rule in confirmations {
            rule.remove();
        }
        settled(base, er, &signature, &eatas, false).await?;
        check_controls(er, &wallets, 2).await?;
        record("recovered", &expected, true).await?;

        settled(base, er, &commit(2).await?, &eatas, false).await?;
        expected.eatas[..2].copy_from_slice(&[Some(152), Some(148)]);
        record("latest balances committed", &expected, true).await?;

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
            owner_is(wallet, spl::eata_program()).await?;
        }
        expected.projected = expected.base_atas;
        await_projection(er, &wallets, &expected.projected).await?;
        record("undelegated", &expected, false).await?;

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
        expected.eatas[0] = Some(132);
        expected.projected[0] = 20;
        expected.vault[0] = 320;
        await_projection(er, &wallets, &expected.projected).await?;
        record("withdrawn on base", &expected, false).await?;

        for wallet in &wallets[..2] {
            delegate(&wallet.owner, wallet, identity).await?;
        }
        expected.projected = [132, 148, 70, 5];
        await_projection(er, &wallets, &expected.projected).await?;
        check_controls(er, &wallets, 3).await?;
        record("redelegated", &expected, true).await?;

        wallets[0].transfer(er, &wallets[1], 7).await?;
        expected.projected = [125, 155, 70, 5];
        record("redelegated transfer", &expected, true).await?;
        settled(base, er, &commit(3).await?, &eatas, false).await?;
        expected.eatas[..2].copy_from_slice(&[Some(125), Some(155)]);
        record("final committed balances", &expected, true).await?;

        let events = proxies.finish()?;
        private.finish().await?;
        report = report.setting("intercepted base commit", landed)
            .setting("foreign validator", foreign_validator)
            .setting("mint supply", SUPPLY)
            .setting("recovery", "base executed; send response and confirmation channels lost; reconnect");
        Ok(netfault::report_events(report, &events))
    }
}
