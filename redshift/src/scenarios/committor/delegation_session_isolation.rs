use std::time::{Duration, Instant};

use async_trait::async_trait;
use borsh::BorshDeserialize;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::schedulecommit::{
    build as sc, MainAccount, ScheduleCommitType,
};
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq, dlp,
    netfault::{self, BaseProxies, Selector},
    prep, receipt, topology,
    topology::{ErOptions, PrivateEr, RestartConfig, RestartTiming},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport,
};
use signer::Signer;

const LABEL_A: &str = "session-isolation-a";
const LABEL_B: &str = "session-isolation-b";
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(60);
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(60);
const STALE_WINDOW: Duration = Duration::from_secs(5);
const RECOVERY_WINDOW: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(250);
const BASE_REJECTION: &str = "redsuite: base rejects session-a work";

pub struct DelegationSessionIsolation;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Target {
    SameValidator,
    OtherValidator,
}

impl Target {
    fn label(self) -> &'static str {
        match self {
            Self::SameValidator => "redelegation",
            Self::OtherValidator => "reassignment",
        }
    }
}

struct Outcome {
    intent_failure: String,
    completion_s: f64,
    restart: RestartTiming,
    fresh_commit_s: f64,
    old_validator_rejection: Option<String>,
}

fn value(case: u64, step: u64) -> u64 {
    100 * case + step
}

fn count(data: &[u8]) -> Result<u64> {
    Ok(MainAccount::try_from_slice(data)?.count)
}

async fn await_clone_count(
    er: &ErCtx,
    account: &Pubkey,
    id: u64,
) -> Result<()> {
    check::poll_for(
        &format!(
            "the er {} clones the committee with count {id}",
            er.identity()
        ),
        CLONE_TIMEOUT,
        || async {
            match er.account(account).await {
                Ok(Some(acc)) if count(&acc.data).ok() == Some(id) => Ok(()),
                Ok(Some(acc)) => {
                    Err(format!("count {:?}", count(&acc.data).ok()))
                }
                Ok(None) => Err("absent".to_owned()),
                Err(error) => Err(format!("read failed: {error}")),
            }
        },
    )
    .await
    .map_err(|error| error.expected(format!("count {id}")))?;
    Ok(())
}

async fn write(
    ctx: &impl ChainCtx,
    payer: &Keypair,
    player: &Pubkey,
    account: &Pubkey,
    id: u64,
) -> Result<()> {
    ctx.submit_and_confirm(payer, &[sc::set_count(*player, id)])
        .await?;
    let data = ctx
        .account(account)
        .await?
        .ok_or("the committee vanished after the write")?
        .data;
    check_eq!(count(&data)?, id, "the write must land")?;
    Ok(())
}

async fn base_state(
    base: &BaseCtx,
    account: &Pubkey,
) -> Result<(Pubkey, Option<u64>)> {
    let on_base = base
        .account(account)
        .await?
        .ok_or("the committee vanished from base")?;
    Ok((on_base.owner, count(&on_base.data).ok()))
}

async fn await_base(
    base: &BaseCtx,
    account: &Pubkey,
    owner: Pubkey,
    id: u64,
    what: &str,
) -> Result<f64> {
    let started = Instant::now();
    check::poll_for(what, COMPLETION_TIMEOUT, || async {
        match base_state(base, account).await {
            Ok((current_owner, written))
                if current_owner == owner && written == Some(id) =>
            {
                Ok(())
            }
            Ok((current_owner, written)) => {
                Err(format!("owner {current_owner} count {written:?}"))
            }
            Err(error) => Err(format!("read failed: {error}")),
        }
    })
    .await
    .map_err(|error| error.expected(format!("owner {owner} count {id}")))?;
    Ok(started.elapsed().as_secs_f64())
}

struct SessionB<'a> {
    er: &'a ErCtx,
    account: Pubkey,
    base_id: u64,
    er_id: u64,
    nonce: u64,
}

async fn hold_steady(
    base: &BaseCtx,
    session: &SessionB<'_>,
    window: Duration,
    what: &str,
) -> Result<()> {
    let started = Instant::now();
    while started.elapsed() < window {
        let (owner, written) = base_state(base, &session.account).await?;
        check_eq!(
            (owner, written),
            (dlp::dlp_id(), Some(session.base_id)),
            "{what}: base must keep the session-b delegation and value"
        )?;
        check_eq!(
            crate::last_commit_id(base, &session.account).await?,
            session.nonce,
            "{what}: the session-b nonce must not move"
        )?;
        let on_er = session
            .er
            .account(&session.account)
            .await?
            .ok_or("the session-b er copy vanished")?;
        check_eq!(
            count(&on_er.data)?,
            session.er_id,
            "{what}: the session-b er state must stay"
        )?;
        tokio::time::sleep(POLL).await;
    }
    Ok(())
}

async fn schedule_commit(
    er: &ErCtx,
    payer: &Keypair,
    player: &Pubkey,
    commit_type: ScheduleCommitType,
) -> Result<signature::Signature> {
    let writable = matches!(
        commit_type,
        ScheduleCommitType::CommitAndUndelegate
            | ScheduleCommitType::CommitFinalizeAndUndelegate
    );
    er.submit_and_confirm(
        payer,
        &[sc::schedule_commit_cpi(
            payer.pubkey(),
            vec![*player],
            false,
            false,
            commit_type,
            writable,
        )],
    )
    .await
}

async fn fail_session_a_intent(
    base: &BaseCtx,
    er: &ErCtx,
    proxies: &BaseProxies,
    payer: &Keypair,
    player: &Pubkey,
    account: &Pubkey,
) -> Result<String> {
    let rejects = proxies.reject(
        Selector::method("sendTransaction")
            .http()
            .request()
            .account(account),
        BASE_REJECTION,
    );
    let signature =
        schedule_commit(er, payer, player, ScheduleCommitType::Commit).await?;
    let receipt =
        receipt::fetch_commit_receipt(er.api(), &signature, RECEIPT_TIMEOUT)
            .await?;
    rejects.remove();
    let failure = receipt.error_message.clone().ok_or_else(|| {
        format!(
            "the session-a commit intent must fail while base rejects its \
             submission, got base signatures {:?}",
            receipt.base_signatures
        )
    })?;
    let (owner, _) = base_state(base, account).await?;
    check_eq!(
        owner,
        dlp::dlp_id(),
        "the failed session-a intent must leave the account delegated"
    )?;
    Ok(failure)
}

async fn isolate(
    base: &BaseCtx,
    proxies: &BaseProxies,
    er_a: &mut PrivateEr,
    er_b: Option<&PrivateEr>,
    payer: &Keypair,
    target: Target,
) -> Result<Outcome> {
    let case = u64::from(target == Target::OtherValidator) + 1;
    let committee = prep::init_committees(base, payer, er_a.identity(), 1)
        .await?
        .pop()
        .ok_or("no committee was created")?;
    let player = committee.player.pubkey();
    let account = committee.pda;
    prep::await_committee_clones(er_a.ctx(), std::slice::from_ref(&committee))
        .await?;

    let staged = value(case, 1);
    write(er_a.ctx(), payer, &player, &account, staged).await?;
    let intent_failure = fail_session_a_intent(
        base,
        er_a.ctx(),
        proxies,
        payer,
        &player,
        &account,
    )
    .await?;

    let delayed = proxies
        .stall(Selector::methods(&[]).ws().notification().account(&account));
    let undelegate = schedule_commit(
        er_a.ctx(),
        payer,
        &player,
        ScheduleCommitType::CommitAndUndelegate,
    )
    .await?;
    let undelegated = receipt::fetch_commit_receipt(
        er_a.ctx().api(),
        &undelegate,
        RECEIPT_TIMEOUT,
    )
    .await?;
    check!(
        undelegated.succeeded() || undelegated.failure_is_duplicate_rejection(),
        "the session-a undelegation must complete, got {:?}",
        undelegated.error_message
    )?;
    let completion_s = await_base(
        base,
        &account,
        redshift_interface::id(),
        staged,
        "base returns the account to the program with the staged value",
    )
    .await?;

    let base_value = value(case, 2);
    write(base, payer, &player, &account, base_value).await?;
    let session_b = match target {
        Target::SameValidator => er_a.ctx(),
        Target::OtherValidator => {
            er_b.ok_or("the reassignment case needs a second er")?.ctx()
        }
    };
    base.submit_and_confirm(
        payer,
        &[sc::delegate_cpi(
            payer.pubkey(),
            player,
            prep::COMMIT_FREQUENCY_MS,
            Some(session_b.identity()),
        )],
    )
    .await?;
    await_base(
        base,
        &account,
        dlp::dlp_id(),
        base_value,
        "base delegates the account again with the base value",
    )
    .await?;
    await_clone_count(session_b, &account, base_value).await?;
    let er_value = value(case, 3);
    write(session_b, payer, &player, &account, er_value).await?;
    let nonce = crate::last_commit_id(base, &account).await?;

    delayed.remove();
    hold_steady(
        base,
        &SessionB {
            er: session_b,
            account,
            base_id: base_value,
            er_id: er_value,
            nonce,
        },
        STALE_WINDOW,
        &format!("{}: after releasing session-a observations", target.label()),
    )
    .await?;

    let restart = er_a.restart(RestartConfig::default()).await?;
    check_eq!(
        restart.exit_code,
        Some(0),
        "the session-a validator must stop cleanly before recovery"
    )?;
    let session_b = match target {
        Target::SameValidator => er_a.ctx(),
        Target::OtherValidator => {
            er_b.ok_or("the reassignment case needs a second er")?.ctx()
        }
    };
    hold_steady(
        base,
        &SessionB {
            er: session_b,
            account,
            base_id: base_value,
            er_id: er_value,
            nonce,
        },
        RECOVERY_WINDOW,
        &format!("{}: while the restarted validator recovers", target.label()),
    )
    .await?;

    let old_validator_rejection = match target {
        Target::SameValidator => None,
        Target::OtherValidator => {
            let stale_write = er_a
                .ctx()
                .submit_and_confirm(
                    payer,
                    &[sc::set_count(player, value(case, 4))],
                )
                .await;
            check!(
                stale_write.is_err(),
                "the old validator must reject writes to the reassigned \
                 account, got {stale_write:?}"
            )?;
            let stale_commit = schedule_commit(
                er_a.ctx(),
                payer,
                &player,
                ScheduleCommitType::Commit,
            )
            .await;
            let rejection = match stale_commit {
                Err(error) => error.to_string(),
                Ok(signature) => {
                    let receipt = receipt::fetch_commit_receipt(
                        er_a.ctx().api(),
                        &signature,
                        RECEIPT_TIMEOUT,
                    )
                    .await?;
                    receipt.error_message.ok_or(
                        "the old validator must not commit the reassigned \
                         account",
                    )?
                }
            };
            check_eq!(
                base_state(base, &account).await?,
                (dlp::dlp_id(), Some(base_value)),
                "rejected old work must leave base untouched"
            )?;
            Some(rejection)
        }
    };

    let fresh_started = Instant::now();
    let fresh =
        schedule_commit(session_b, payer, &player, ScheduleCommitType::Commit)
            .await?;
    let fresh_receipt =
        receipt::fetch_commit_receipt(session_b.api(), &fresh, RECEIPT_TIMEOUT)
            .await?;
    check!(
        fresh_receipt.succeeded()
            || fresh_receipt.failure_is_duplicate_rejection(),
        "a fresh session-b commit must complete, got {:?}",
        fresh_receipt.error_message
    )?;
    await_base(
        base,
        &account,
        dlp::dlp_id(),
        er_value,
        "the session-b commit reaches base",
    )
    .await?;
    let fresh_commit_s = fresh_started.elapsed().as_secs_f64();

    Ok(Outcome {
        intent_failure,
        completion_s,
        restart,
        fresh_commit_s,
        old_validator_rejection,
    })
}

#[async_trait(?Send)]
impl PrivateErScenario for DelegationSessionIsolation {
    fn name(&self) -> &str {
        "redshift/delegation_session_isolation"
    }

    async fn run(&self, base: &BaseCtx) -> Result<ScenarioReport> {
        let proxies = BaseProxies::spawn(base).await?;
        let mut er_a = topology::private_er(
            base,
            ErOptions {
                label: LABEL_A.to_owned(),
                env: Vec::new(),
                request_timeout: None,
                base_endpoints: Some(proxies.endpoints()),
            },
        )
        .await?;
        let er_b = topology::private_er(
            base,
            ErOptions {
                label: LABEL_B.to_owned(),
                env: Vec::new(),
                request_timeout: None,
                base_endpoints: None,
            },
        )
        .await?;
        let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;

        let same = isolate(
            base,
            &proxies,
            &mut er_a,
            None,
            &payer,
            Target::SameValidator,
        )
        .await?;
        let other = isolate(
            base,
            &proxies,
            &mut er_a,
            Some(&er_b),
            &payer,
            Target::OtherValidator,
        )
        .await?;

        let events = proxies.finish()?;
        er_b.finish().await?;
        er_a.finish().await?;

        let report = ScenarioReport::ok(self.name())
            .setting("session-a er", LABEL_A)
            .setting("session-b er", LABEL_B)
            .setting("redelegation intent failure", same.intent_failure)
            .setting("reassignment intent failure", other.intent_failure)
            .setting(
                "old validator rejection",
                other.old_validator_rejection.unwrap_or_default(),
            )
            .metric(
                "redelegation undelegation s",
                Unit::Seconds,
                same.completion_s,
            )
            .metric(
                "reassignment undelegation s",
                Unit::Seconds,
                other.completion_s,
            )
            .metric(
                "redelegation fresh commit s",
                Unit::Seconds,
                same.fresh_commit_s,
            )
            .metric(
                "reassignment fresh commit s",
                Unit::Seconds,
                other.fresh_commit_s,
            )
            .metric(
                "redelegation restart startup s",
                Unit::Seconds,
                same.restart.startup.as_secs_f64(),
            )
            .metric(
                "reassignment restart startup s",
                Unit::Seconds,
                other.restart.startup.as_secs_f64(),
            )
            .metric("fault events", Unit::Count, events.len() as f64);
        Ok(netfault::report_events(report, &events))
    }
}
