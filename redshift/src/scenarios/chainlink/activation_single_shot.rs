use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use account::Account;

use async_trait::async_trait;
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_interface::flexi::{build as flexi, FlexiCounter};
use redsuite_core::{
    check, check_eq,
    dlp::{self, delegate_with_actions, DelegateArgs},
    netfault::{Action, BaseProxies, Selector},
    prep, system, topology,
    topology::{ErOptions, RestartConfig},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use signature::Signature;
use signer::Signer;
use tokio::task::JoinHandle;

const LABEL: &str = "activation-single-shot";
const COUNTER_LABEL: &str = "single shot";
const AIRDROP: u64 = 2_000_000_000;
const DEPENDENCY_LAMPORTS: u64 = 1_000_000;
const ACTION_COUNT: u8 = 7;
const CONCURRENT_READS: usize = 16;
const CONCURRENT_SUBMISSIONS: usize = 8;
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(30);
const HOLD_WINDOW: Duration = Duration::from_millis(2_500);
const ACTIVATION_TIMEOUT: Duration = Duration::from_secs(30);
const RESCUE_TIMEOUT: Duration = Duration::from_secs(45);
const STEADY_WINDOW: Duration = Duration::from_secs(4);
const RECOVERY_WINDOW: Duration = Duration::from_secs(6);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(200);

pub struct ActivationSingleShot;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CounterState {
    count: u64,
    updates: u64,
}

struct Discovery {
    reads: Vec<JoinHandle<Result<Option<Account>>>>,
    submissions: Vec<JoinHandle<Result<Signature>>>,
}

fn counter_action(
    actor: &Pubkey,
    dependency: &Pubkey,
    count: u8,
    fail: bool,
) -> Instruction {
    let mut instruction = if fail {
        flexi::add_error(*actor, count)
    } else {
        flexi::add_unsigned(*actor, count)
    };
    instruction
        .accounts
        .push(AccountMeta::new_readonly(*dependency, false));
    instruction
}

async fn counter_state(er: &ErCtx, counter: &Pubkey) -> Result<CounterState> {
    let account = er
        .account(counter)
        .await?
        .ok_or("the counter vanished from the er")?;
    let decoded = FlexiCounter::try_decode(&account.data)?;
    Ok(CounterState {
        count: decoded.count,
        updates: decoded.updates,
    })
}

async fn await_counter(
    er: &ErCtx,
    counter: &Pubkey,
    expected: CounterState,
    what: &str,
    timeout: Duration,
) -> Result<()> {
    check::poll_for(what, timeout, || async {
        match counter_state(er, counter).await {
            Ok(state) if state == expected => Ok(()),
            Ok(state) => Err(format!("{state:?}")),
            Err(error) => Err(format!("read failed: {error}")),
        }
    })
    .await
    .map_err(|error| error.expected(format!("{expected:?}")))?;
    Ok(())
}

async fn hold_steady(
    er: &ErCtx,
    counter: &Pubkey,
    expected: CounterState,
    window: Duration,
    what: &str,
) -> Result<()> {
    let started = Instant::now();
    while started.elapsed() < window {
        check_eq!(
            counter_state(er, counter).await?,
            expected,
            "{what}: the counter must keep exactly one application of the \
             action"
        )?;
        tokio::time::sleep(POLL).await;
    }
    Ok(())
}

async fn fund_dependency(base: &BaseCtx) -> Result<Pubkey> {
    let dependency = Keypair::new().pubkey();
    base.airdrop(&dependency, DEPENDENCY_LAMPORTS).await?;
    Ok(dependency)
}

async fn delegate_trigger(
    base: &BaseCtx,
    funder: &Keypair,
    validator: Pubkey,
    action: Instruction,
) -> Result<Keypair> {
    let trigger = Keypair::new();
    base.airdrop(&trigger.pubkey(), AIRDROP).await?;
    let delegate = delegate_with_actions(
        &funder.pubkey(),
        &trigger.pubkey(),
        None,
        DelegateArgs {
            commit_frequency_ms: u32::MAX,
            seeds: vec![],
            validator: Some(validator),
        },
        &[action],
    );
    // Publish the owner change with its record so discovery sees both.
    base.submit_and_confirm_with(
        funder,
        &[&trigger],
        &[system::assign(&trigger.pubkey(), &dlp::dlp_id()), delegate],
    )
    .await?;
    Ok(trigger)
}

fn dependency_fetch(dependency: &Pubkey) -> Selector {
    Selector::methods(&["getMultipleAccounts", "getAccountInfo"])
        .http()
        .response()
        .account(dependency)
}

fn held_fetches(proxies: &BaseProxies, dependency: &Pubkey) -> usize {
    let dependency = dependency.to_string();
    proxies
        .events()
        .iter()
        .filter(|event| event.action == Action::Held)
        .filter(|event| {
            event
                .operation
                .as_ref()
                .is_some_and(|op| op.accounts.contains(&dependency))
        })
        .count()
}

async fn await_fetch_held(
    proxies: &BaseProxies,
    dependency: &Pubkey,
) -> Result<()> {
    check::poll(
        "the er fetches the action dependency from base",
        INTERCEPT_TIMEOUT,
        || async { held_fetches(proxies, dependency) > 0 },
    )
    .await?;
    Ok(())
}

async fn race_discovery(
    base: &BaseCtx,
    er: &ErCtx,
    funder: &Keypair,
    er_payer: &Rc<Keypair>,
    trigger: &Pubkey,
    counter: &Pubkey,
    before: CounterState,
) -> Result<Discovery> {
    let started = Instant::now();
    let sender = er.sender(er_payer.clone());
    let trigger = *trigger;
    let reads: Vec<_> = (0..CONCURRENT_READS)
        .map(|_| {
            let api = er.api().clone();
            tokio::task::spawn_local(
                async move { api.get_account(&trigger).await },
            )
        })
        .collect();
    let submissions: Vec<_> = (0..CONCURRENT_SUBMISSIONS)
        .map(|_| {
            let sender = sender.clone();
            tokio::task::spawn_local(async move {
                sender
                    .submit_fresh(&[system::transfer(
                        &sender.payer().pubkey(),
                        &trigger,
                        1,
                    )])
                    .await
            })
        })
        .collect();
    base.submit_and_confirm(
        funder,
        &[system::transfer(&funder.pubkey(), &trigger, 1)],
    )
    .await?;
    while started.elapsed() < HOLD_WINDOW {
        check_eq!(
            counter_state(er, counter).await?,
            before,
            "the action must not apply while its dependency fetch is held"
        )?;
        tokio::time::sleep(POLL).await;
    }
    Ok(Discovery { reads, submissions })
}

async fn settle_discovery(discovery: Discovery) {
    for read in discovery.reads {
        let _ = tokio::time::timeout(SETTLE_TIMEOUT, read).await;
    }
    for submission in discovery.submissions {
        let _ = tokio::time::timeout(SETTLE_TIMEOUT, submission).await;
    }
}

async fn run_action(
    base: &BaseCtx,
    proxies: &BaseProxies,
    er: &ErCtx,
    funder: &Keypair,
    er_payer: &Rc<Keypair>,
    actor: &Pubkey,
    fail: bool,
) -> Result<Pubkey> {
    let (counter, _) = FlexiCounter::pda_and_bump(actor);
    let counter = &counter;
    let before = counter_state(er, counter).await?;
    let dependency = fund_dependency(base).await?;
    let stall = proxies.stall(dependency_fetch(&dependency));
    let trigger = delegate_trigger(
        base,
        funder,
        er.identity(),
        counter_action(actor, &dependency, ACTION_COUNT, fail),
    )
    .await?;
    await_fetch_held(proxies, &dependency).await?;
    let discovery = race_discovery(
        base,
        er,
        funder,
        er_payer,
        &trigger.pubkey(),
        counter,
        before,
    )
    .await?;
    stall.remove();
    settle_discovery(discovery).await;
    if fail {
        check::poll_for(
            "the failed activation completes the rescue undelegation on base",
            RESCUE_TIMEOUT,
            || async {
                let owner = base
                    .account(&trigger.pubkey())
                    .await?
                    .map(|account| account.owner);
                check_eq!(
                    owner,
                    Some(system::system_id()),
                    "rescued trigger {} owner",
                    trigger.pubkey()
                )?;
                Ok::<_, redsuite_core::DynError>(())
            },
        )
        .await?;
        check_eq!(
            counter_state(er, counter).await?,
            before,
            "failed activation leaves counter {counter} unchanged"
        )?;
    } else {
        let expected = CounterState {
            count: before.count + u64::from(ACTION_COUNT),
            updates: before.updates + 1,
        };
        await_counter(
            er,
            counter,
            expected,
            "the released activation applies the counter action exactly once",
            ACTIVATION_TIMEOUT,
        )
        .await?;
        check!(
            er.account(&trigger.pubkey()).await?.is_some(),
            "activated trigger {} is present on the er",
            trigger.pubkey()
        )?;
    }
    Ok(trigger.pubkey())
}

#[async_trait(?Send)]
impl PrivateErScenario for ActivationSingleShot {
    fn name(&self) -> &str {
        "redshift/activation_single_shot"
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
        let er = private.ctx();
        let funder = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let actor = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
        let er_payer = Rc::new(
            prep::delegated_payer(
                base,
                &funder,
                er.identity(),
                crate::PAYER_LAMPORTS,
            )
            .await?,
        );

        let (counter, setup) = prep::flexi_counter(
            actor.pubkey(),
            COUNTER_LABEL,
            er.identity(),
            prep::COMMIT_FREQUENCY_MS,
        );
        for instruction in setup {
            base.submit_and_confirm(&actor, &[instruction]).await?;
        }
        let zero = CounterState {
            count: 0,
            updates: 0,
        };
        await_counter(
            er,
            &counter,
            zero,
            "the er clones the delegated counter",
            CLONE_TIMEOUT,
        )
        .await?;

        let trigger = run_action(
            base,
            &proxies,
            er,
            &funder,
            &er_payer,
            &actor.pubkey(),
            false,
        )
        .await?;
        let applied = CounterState {
            count: u64::from(ACTION_COUNT),
            updates: 1,
        };
        hold_steady(er, &counter, applied, STEADY_WINDOW, "after activation")
            .await?;

        base.submit_and_confirm(
            &funder,
            &[system::transfer(&funder.pubkey(), &trigger, 1)],
        )
        .await?;
        proxies.close_connections();
        base.submit_and_confirm(
            &funder,
            &[system::transfer(&funder.pubkey(), &trigger, 1)],
        )
        .await?;
        hold_steady(
            er,
            &counter,
            applied,
            STEADY_WINDOW,
            "after reconnecting the base subscriptions",
        )
        .await?;

        let restart = private.restart(RestartConfig::default()).await?;
        check_eq!(
            restart.exit_code,
            Some(0),
            "the er must stop cleanly before the checkpointed restart"
        )?;
        let er = private.ctx();
        await_counter(
            er,
            &counter,
            applied,
            "the restarted er serves the counter with the action applied",
            CLONE_TIMEOUT,
        )
        .await?;
        base.submit_and_confirm(
            &funder,
            &[system::transfer(&funder.pubkey(), &trigger, 1)],
        )
        .await?;
        hold_steady(
            er,
            &counter,
            applied,
            RECOVERY_WINDOW,
            "after the checkpointed restart",
        )
        .await?;

        run_action(
            base,
            &proxies,
            er,
            &funder,
            &er_payer,
            &actor.pubkey(),
            true,
        )
        .await?;
        hold_steady(er, &counter, applied, STEADY_WINDOW, "after the rescue")
            .await?;

        proxies.finish()?;
        private.finish().await?;
        Ok(())
    }
}
