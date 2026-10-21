use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use account::Account;
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use pubkey::Pubkey;

use redsuite_core::{
    check, check_eq,
    dlp::{self, delegate_with_actions, DelegateArgs},
    netfault::{Action, BaseProxies, Selector},
    prep,
    programs::{CellHeader, REDSHIFT},
    system,
    topology::{self, ErOptions, PrivateEr, RestartConfig},
    BaseCtx, ChainCtx, DynError, ErCtx, PrivateErScenario, Result,
};
use signature::Signature;
use signer::Signer;
use tokio::{
    task::{self, JoinHandle},
    time,
};

use crate::PAYER_LAMPORTS;

const LABEL: &str = "activation-single-shot";
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CellState {
    value: u64,
    revision: u64,
}

struct Discovery {
    reads: Vec<JoinHandle<Result<Option<Account>>>>,
    submissions: Vec<JoinHandle<Result<Signature>>>,
}

fn cell_action(actor: &Pubkey, dependency: &Pubkey, count: u8, fail: bool) -> Instruction {
    let mut instruction = if fail {
        REDSHIFT.batch().writable(&[REDSHIFT.primary(*actor)]).fail(0).build()
    } else {
        REDSHIFT.batch().add(&[REDSHIFT.primary(*actor)], i64::from(count)).build()
    };
    instruction.accounts.push(AccountMeta::new_readonly(*dependency, false));
    instruction
}

async fn cell_state(er: &ErCtx, cell: &Pubkey) -> Result<CellState> {
    let account = er.account(cell).await?.ok_or("the cell vanished from the er")?;
    let decoded = CellHeader::try_decode(&account.data)?;
    Ok(CellState {
        value: decoded.value,
        revision: decoded.revision,
    })
}

async fn await_cell(
    er: &ErCtx,
    cell: &Pubkey,
    expected: CellState,
    what: &str,
    timeout: Duration,
) -> Result<()> {
    check::poll_for(what, timeout, || async {
        match cell_state(er, cell).await {
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
    cell: &Pubkey,
    expected: CellState,
    window: Duration,
    what: &str,
) -> Result<()> {
    let started = Instant::now();
    while started.elapsed() < window {
        check_eq!(
            cell_state(er, cell).await?,
            expected,
            "{what}: the cell must keep exactly one application of the \
             action"
        )?;
        time::sleep(POLL).await;
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
            event.operation.as_ref().is_some_and(|op| op.accounts.contains(&dependency))
        })
        .count()
}

async fn await_fetch_held(proxies: &BaseProxies, dependency: &Pubkey) -> Result<()> {
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
    cell: &Pubkey,
    before: CellState,
) -> Result<Discovery> {
    let started = Instant::now();
    let sender = er.sender(er_payer.clone());
    let trigger = *trigger;
    let mut reads = Vec::with_capacity(CONCURRENT_READS);
    for _ in 0..CONCURRENT_READS {
        let api = er.api().clone();
        reads.push(task::spawn_local(
            async move { api.get_account(&trigger).await },
        ));
    }
    let mut submissions = Vec::with_capacity(CONCURRENT_SUBMISSIONS);
    for _ in 0..CONCURRENT_SUBMISSIONS {
        let sender = sender.clone();
        submissions.push(task::spawn_local(async move {
            sender
                .submit_fresh(&[system::transfer(&sender.payer().pubkey(), &trigger, 1)])
                .await
        }));
    }
    base.submit_and_confirm(funder, &[system::transfer(&funder.pubkey(), &trigger, 1)])
        .await?;
    while started.elapsed() < HOLD_WINDOW {
        check_eq!(
            cell_state(er, cell).await?,
            before,
            "the action must not apply while its dependency fetch is held"
        )?;
        time::sleep(POLL).await;
    }
    Ok(Discovery { reads, submissions })
}

async fn settle_discovery(discovery: Discovery) {
    for read in discovery.reads {
        let _ = time::timeout(SETTLE_TIMEOUT, read).await;
    }
    for submission in discovery.submissions {
        let _ = time::timeout(SETTLE_TIMEOUT, submission).await;
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
    let cell = REDSHIFT.primary(*actor);
    let cell = &cell;
    let before = cell_state(er, cell).await?;
    let dependency = fund_dependency(base).await?;
    let stall = proxies.stall(dependency_fetch(&dependency));
    let trigger = delegate_trigger(
        base,
        funder,
        er.identity(),
        cell_action(actor, &dependency, ACTION_COUNT, fail),
    )
    .await?;
    await_fetch_held(proxies, &dependency).await?;
    let discovery =
        race_discovery(base, er, funder, er_payer, &trigger.pubkey(), cell, before).await?;
    stall.remove();
    settle_discovery(discovery).await;
    if fail {
        check::poll_for(
            "the failed activation completes the rescue undelegation on base",
            RESCUE_TIMEOUT,
            || async {
                let owner = base.account(&trigger.pubkey()).await?.map(|account| account.owner);
                check_eq!(
                    owner,
                    Some(system::system_id()),
                    "rescued trigger {} owner",
                    trigger.pubkey()
                )?;
                Ok::<_, DynError>(())
            },
        )
        .await?;
        check_eq!(
            cell_state(er, cell).await?,
            before,
            "failed activation leaves cell {cell} unchanged"
        )?;
    } else {
        let expected = CellState {
            value: before.value + u64::from(ACTION_COUNT),
            revision: before.revision + 1,
        };
        await_cell(
            er,
            cell,
            expected,
            "the released activation applies the cell action exactly once",
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

pub struct ActivationSingleShot {
    base: Rc<BaseCtx>,
    proxies: BaseProxies,
    private: PrivateEr,
}

impl PrivateErScenario for ActivationSingleShot {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let proxies = BaseProxies::spawn(&base).await?;
        let private = topology::private_er(
            &base,
            ErOptions {
                label: LABEL.to_owned(),
                base_endpoints: Some(proxies.endpoints()),
                ..Default::default()
            },
        )
        .await?;
        Ok(Self { base, proxies, private })
    }

    async fn run(self) -> Result<()> {
        let Self { base, proxies, mut private } = self;
        let er = private.ctx();
        let funder = base.funded_payer(PAYER_LAMPORTS).await?;
        let actor = base.funded_payer(PAYER_LAMPORTS).await?;
        let er_payer = Rc::new(base.delegated_payer(&funder, er.identity(), PAYER_LAMPORTS).await?);

        let cell = base
            .cells(REDSHIFT, &actor)
            .create_primary(er.identity(), prep::COMMIT_FREQUENCY_MS)
            .await?;
        let zero = CellState { value: 0, revision: 0 };
        await_cell(
            er,
            &cell,
            zero,
            "the er clones the delegated cell",
            CLONE_TIMEOUT,
        )
        .await?;

        let trigger = run_action(
            &base,
            &proxies,
            er,
            &funder,
            &er_payer,
            &actor.pubkey(),
            false,
        )
        .await?;
        let applied = CellState {
            value: u64::from(ACTION_COUNT),
            revision: 1,
        };
        hold_steady(er, &cell, applied, STEADY_WINDOW, "after activation").await?;

        base.submit_and_confirm(&funder, &[system::transfer(&funder.pubkey(), &trigger, 1)])
            .await?;
        proxies.close_connections();
        base.submit_and_confirm(&funder, &[system::transfer(&funder.pubkey(), &trigger, 1)])
            .await?;
        hold_steady(
            er,
            &cell,
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
        await_cell(
            er,
            &cell,
            applied,
            "the restarted er serves the cell with the action applied",
            CLONE_TIMEOUT,
        )
        .await?;
        base.submit_and_confirm(&funder, &[system::transfer(&funder.pubkey(), &trigger, 1)])
            .await?;
        hold_steady(
            er,
            &cell,
            applied,
            RECOVERY_WINDOW,
            "after the checkpointed restart",
        )
        .await?;

        run_action(
            &base,
            &proxies,
            er,
            &funder,
            &er_payer,
            &actor.pubkey(),
            true,
        )
        .await?;
        hold_steady(er, &cell, applied, STEADY_WINDOW, "after the rescue").await?;

        proxies.finish()?;
        private.finish().await?;
        Ok(())
    }
}
