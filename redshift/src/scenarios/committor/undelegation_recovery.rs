use std::time::{Duration, Instant};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq,
    netfault::{BaseProxies, Selector},
    prep, topology,
    topology::{ErOptions, PrivateEr, RestartConfig},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use signer::Signer;

use crate::program::{instruction::build, DELEGATION_PROGRAM_ID};

const LABEL: &str = "undelegation-recovery";
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(90);
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(60);
const REDELEGATE_TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(250);
const HIDDEN: &str = "notification hidden by redsuite";

pub struct UndelegationRecovery;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Recovery {
    Reconnect,
    Restart,
}

impl Recovery {
    fn label(self) -> &'static str {
        match self {
            Self::Reconnect => "reconnect",
            Self::Restart => "restart",
        }
    }
}

fn value(case: u64, step: u64) -> u64 {
    100 * case + step
}

async fn write(
    er: &ErCtx,
    payer: &Keypair,
    id: u64,
    account: &Pubkey,
) -> Result<Vec<u8>> {
    er.submit_and_confirm(payer, &[build::simple_byte_set(id, &[*account])])
        .await?;
    let data = er
        .account(account)
        .await?
        .ok_or("er copy vanished after the write")?
        .data;
    check_eq!(
        crate::written_id(&data),
        Some(id),
        "the er write must land before undelegation is requested"
    )?;
    Ok(data)
}

async fn rejected_write(
    er: &ErCtx,
    payer: &Keypair,
    id: u64,
    account: &Pubkey,
    snapshot: &[u8],
    when: &str,
) -> Result<()> {
    let attempt = er
        .submit_and_confirm(payer, &[build::simple_byte_set(id, &[*account])])
        .await;
    check!(
        attempt.is_err(),
        "an er write {when} must be rejected while undelegation is pending, \
         got {attempt:?}"
    )?;
    let error = format!("{:?}", attempt.unwrap_err());
    crate::rejection_code(
        &format!("the er write {when}"),
        &crate::LOCKOUT_REJECTIONS,
        &error,
    )?;
    let on_er = er
        .account(account)
        .await?
        .ok_or("er copy vanished during the lockout")?;
    check_eq!(
        on_er.data,
        snapshot,
        "the rejected write {when} must leave the er copy untouched"
    )?;
    Ok(())
}

async fn await_ownership_return(
    base: &BaseCtx,
    account: &Pubkey,
    owner: &Pubkey,
    snapshot: &[u8],
) -> Result<()> {
    let started = Instant::now();
    loop {
        let on_base = base
            .account(account)
            .await?
            .ok_or("the account vanished from base during undelegation")?;
        if on_base.owner == *owner {
            check_eq!(
                on_base.data,
                snapshot,
                "the latest committed value must reach base before ownership \
                 returns to the program"
            )?;
            return Ok(());
        }
        check_eq!(
            on_base.owner,
            DELEGATION_PROGRAM_ID,
            "an undelegating account must stay dlp-owned until the program \
             owns it again"
        )?;
        check!(
            started.elapsed() < COMPLETION_TIMEOUT,
            "base must complete the undelegation of {account} within \
             {COMPLETION_TIMEOUT:?}"
        )?;
        tokio::time::sleep(POLL).await;
    }
}

async fn await_er_value(
    er: &ErCtx,
    account: &Pubkey,
    id: u64,
    what: &str,
) -> Result<()> {
    check::poll_for(what, DISCOVERY_TIMEOUT, || async {
        match er.account(account).await {
            Ok(Some(acc)) if crate::written_id(&acc.data) == Some(id) => Ok(()),
            Ok(Some(acc)) => Err(format!(
                "owner {} written id {:?}",
                acc.owner,
                crate::written_id(&acc.data)
            )),
            Ok(None) => Err("absent".to_owned()),
            Err(error) => Err(format!("read failed: {error}")),
        }
    })
    .await
    .map_err(|error| error.expected(format!("written id {id}")))?;
    Ok(())
}

async fn redelegate_and_write(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
    account: &Pubkey,
    seed: u8,
    id: u64,
) -> Result<()> {
    base.submit_and_confirm(
        payer,
        &[build::delegate(
            payer.pubkey(),
            *account,
            payer.pubkey(),
            seed,
            er.identity(),
        )],
    )
    .await?;
    let on_base = base
        .account(account)
        .await?
        .ok_or("the account vanished from base during redelegation")?;
    check_eq!(
        on_base.owner,
        DELEGATION_PROGRAM_ID,
        "dlp must own the redelegated account on base"
    )?;
    check::poll(
        "the er accepts writes again once the account is redelegated",
        REDELEGATE_TIMEOUT,
        || async {
            er.submit_and_confirm(
                payer,
                &[build::simple_byte_set(id, &[*account])],
            )
            .await
            .is_ok()
        },
    )
    .await?;
    let on_er = er
        .account(account)
        .await?
        .ok_or("er copy vanished after the redelegated write")?;
    check_eq!(
        crate::written_id(&on_er.data),
        Some(id),
        "the er must hold the write made after redelegation"
    )?;
    Ok(())
}

async fn recover(
    base: &BaseCtx,
    private: &mut PrivateEr,
    proxies: &BaseProxies,
    payer: &Keypair,
    seed: u8,
    mode: Recovery,
) -> Result<()> {
    let case = u64::from(seed);
    let identity = private.identity();
    let account =
        crate::init_delegated_account(base, payer, seed, identity).await?;
    prep::await_clones(
        private.ctx(),
        &[account],
        crate::ACCOUNT_SPACE as usize,
        CLONE_TIMEOUT,
    )
    .await?;
    let snapshot =
        write(private.ctx(), payer, value(case, 1), &account).await?;

    let submission = proxies.intercept(
        Selector::method("sendTransaction")
            .http()
            .request()
            .account(&account),
    );
    private
        .ctx()
        .submit_and_confirm(
            payer,
            &[build::commit_and_undelegate_accounts(
                case,
                payer.pubkey(),
                &[account],
            )],
        )
        .await?;
    let held = submission.wait(INTERCEPT_TIMEOUT).await?;
    rejected_write(
        private.ctx(),
        payer,
        value(case, 2),
        &account,
        &snapshot,
        "while base has not seen the undelegation",
    )
    .await?;

    let resubmission = proxies.intercept(
        Selector::method("sendTransaction")
            .http()
            .request()
            .account(&account),
    );
    held.discard();
    let held = resubmission.wait(INTERCEPT_TIMEOUT).await?;
    rejected_write(
        private.ctx(),
        payer,
        value(case, 3),
        &account,
        &snapshot,
        "after a failed base submission",
    )
    .await?;
    let on_base = base.account(&account).await?.ok_or(
        "the account vanished from base while its submission was held",
    )?;
    check_eq!(
        on_base.owner,
        DELEGATION_PROGRAM_ID,
        "base must still hold the delegation while the submission is held"
    )?;

    let blind = proxies.reject(
        Selector::methods(&[]).ws().notification().account(&account),
        HIDDEN,
    );
    proxies.close_connections();
    held.release();
    await_ownership_return(base, &account, &crate::program::id(), &snapshot)
        .await?;

    match mode {
        Recovery::Reconnect => {
            blind.remove();
            proxies.close_connections();
        }
        Recovery::Restart => {
            let timing = private.restart(RestartConfig::default()).await?;
            check_eq!(
                timing.exit_code,
                Some(0),
                "the er must stop cleanly before the same-storage relaunch"
            )?;
            blind.remove();
        }
    }

    let er = private.ctx();
    let base_write = value(case, 5);
    base.submit_and_confirm(
        payer,
        &[build::simple_byte_set(base_write, &[account])],
    )
    .await?;
    await_er_value(
        er,
        &account,
        base_write,
        &format!(
            "the er discovers the completed undelegation after {} and shows \
             the base write",
            mode.label()
        ),
    )
    .await?;
    redelegate_and_write(base, er, payer, &account, seed, value(case, 6))
        .await?;
    Ok(())
}

#[async_trait(?Send)]
impl PrivateErScenario for UndelegationRecovery {
    fn name(&self) -> &str {
        "redshift/undelegation_recovery"
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        let proxies = BaseProxies::spawn(base).await?;
        let mut private = topology::private_er(
            base,
            ErOptions {
                label: LABEL.to_owned(),
                env: Vec::new(),
                request_timeout: None,
                base_endpoints: Some(proxies.endpoints()),
            },
        )
        .await?;
        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;

        recover(base, &mut private, &proxies, &payer, 1, Recovery::Reconnect)
            .await?;
        recover(base, &mut private, &proxies, &payer, 2, Recovery::Restart)
            .await?;

        proxies.finish()?;
        private.finish().await?;
        Ok(())
    }
}
