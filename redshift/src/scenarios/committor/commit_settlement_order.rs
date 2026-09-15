use std::time::{Duration, Instant};

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::report::Unit;
use redsuite_core::{
    check, check_eq,
    netfault::{self, BaseProxies, Selector},
    prep,
    receipt::{self, CommitReceipt},
    topology::{self, ErOptions},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result, ScenarioReport,
};
use signature::Signature;
use signer::Signer;

use crate::program::{instruction::build, DELEGATION_PROGRAM_ID};

const LABEL: &str = "commit-settlement-order";
const CLONE_TIMEOUT: Duration = Duration::from_secs(30);
const INTERCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);
const BUFFER_CLEANUP_TIMEOUT: Duration = Duration::from_secs(60);
const HOLD_WINDOW: Duration = Duration::from_secs(5);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
const HISTORY_LIMIT: usize = 32;
const SMALL_SPACE: u32 = crate::ACCOUNT_SPACE;
const LARGE_SPACE: u32 = 2048;

pub struct CommitSettlementOrder;

#[derive(Clone, Copy)]
struct Variant {
    name: &'static str,
    space: u32,
    seed_base: u8,
    value_base: u64,
    buffered: bool,
}

const VARIANTS: [Variant; 2] = [
    Variant {
        name: "inline",
        space: SMALL_SPACE,
        seed_base: 10,
        value_base: 100,
        buffered: false,
    },
    Variant {
        name: "buffered",
        space: LARGE_SPACE,
        seed_base: 20,
        value_base: 200,
        buffered: true,
    },
];

struct Accounts {
    a: Pubkey,
    b: Pubkey,
    c: Pubkey,
    d: Pubkey,
}

impl Accounts {
    fn all(&self) -> [Pubkey; 4] {
        [self.a, self.b, self.c, self.d]
    }
}

struct Bundle {
    receipt: CommitReceipt,
    slot: u64,
}

struct Outcome {
    variant: &'static str,
    d_settled_during_hold_s: f64,
    retry_gap_s: f64,
    first_slot: u64,
    second_slot: u64,
    buffers_during_hold: usize,
    base_signatures: [usize; 4],
    seconds: f64,
}

async fn buffer_accounts(base: &BaseCtx) -> Result<usize> {
    let committor: Pubkey = topology::COMMITTOR_ID.parse()?;
    Ok(base.api().get_program_accounts(&committor).await?.len())
}

async fn write(
    er: &ErCtx,
    payer: &Keypair,
    id: u64,
    account: &Pubkey,
) -> Result<Vec<u8>> {
    er.submit_and_confirm(payer, &[build::simple_byte_set(id, &[*account])])
        .await?;
    let snapshot = er
        .account(account)
        .await?
        .ok_or("er copy vanished after the write")?
        .data;
    check_eq!(
        crate::written_id(&snapshot),
        Some(id),
        "the er write must land before it is committed"
    )?;
    Ok(snapshot)
}

async fn schedule(
    er: &ErCtx,
    payer: &Keypair,
    commit_id: u64,
    accounts: &[Pubkey],
) -> Result<Signature> {
    er.submit_and_confirm(
        payer,
        &[build::commit_accounts(commit_id, payer.pubkey(), accounts)],
    )
    .await
}

async fn base_data(base: &BaseCtx, account: &Pubkey) -> Result<Vec<u8>> {
    Ok(base
        .account(account)
        .await?
        .ok_or("the delegated account is missing on base")?
        .data)
}

async fn await_base(
    base: &BaseCtx,
    account: &Pubkey,
    snapshot: &[u8],
    what: &str,
) -> Result<()> {
    check::poll(what, BASE_STATE_TIMEOUT, || async {
        matches!(base.account(account).await, Ok(Some(acc)) if acc.data == snapshot)
    })
    .await?;
    Ok(())
}

async fn settled(
    base: &BaseCtx,
    er: &ErCtx,
    commit_signature: &Signature,
    expected: &[Pubkey],
    phase: &str,
) -> Result<Bundle> {
    let receipt = receipt::fetch_commit_receipt(
        er.api(),
        commit_signature,
        RECEIPT_TIMEOUT,
    )
    .await?;
    check!(
        receipt.succeeded(),
        "{phase}: the bundle must settle, got {:?}",
        receipt.error_message
    )?;
    let mut included = receipt.included.clone();
    included.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    check_eq!(
        included,
        expected,
        "{phase}: the receipt must list exactly the bundled accounts"
    )?;
    check!(
        !receipt.base_signatures.is_empty(),
        "{phase}: a settled bundle must name its base transactions"
    )?;
    receipt::confirm_base_signatures(
        base.api(),
        &receipt,
        BASE_CONFIRM_TIMEOUT,
    )
    .await?;
    let last = receipt.base_signatures[receipt.base_signatures.len() - 1];
    let slot = base
        .api()
        .await_transaction(&last, BASE_CONFIRM_TIMEOUT)
        .await?
        .slot;
    Ok(Bundle { receipt, slot })
}

async fn history_position(
    base: &BaseCtx,
    account: &Pubkey,
    signature: &Signature,
) -> Result<usize> {
    let history = base
        .api()
        .get_signatures_for_address(account, HISTORY_LIMIT)
        .await?;
    history
        .iter()
        .position(|text| text == &signature.to_string())
        .ok_or_else(|| {
            format!("base history of {account} does not list {signature}")
                .into()
        })
}

async fn hold_nonces(
    base: &BaseCtx,
    accounts: &[Pubkey],
    expected: &[u64],
    window: Duration,
    phase: &str,
) -> Result<()> {
    let deadline = Instant::now() + window;
    loop {
        for (account, expected) in accounts.iter().zip(expected) {
            let nonce = crate::last_commit_id(base, account).await?;
            check_eq!(
                nonce,
                *expected,
                "{phase}: the nonce of {account} must not move while the \
                 first bundle is held"
            )?;
        }
        if Instant::now() >= deadline {
            return Ok(());
        }
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
}

async fn await_clone(er: &ErCtx, account: &Pubkey, space: u32) -> Result<()> {
    check::poll(
        &format!("the private er clones the delegated account {account}"),
        CLONE_TIMEOUT,
        || async {
            matches!(er.account(account).await, Ok(Some(acc)) if acc.data.len() == space as usize)
        },
    )
    .await?;
    Ok(())
}

async fn prepare_accounts(
    base: &BaseCtx,
    er: &ErCtx,
    payer: &Keypair,
    identity: Pubkey,
    variant: Variant,
) -> Result<Accounts> {
    let mut keys = Vec::with_capacity(4);
    for index in 0..4u8 {
        let account = crate::init_delegated_account_sized(
            base,
            payer,
            variant.seed_base + index,
            identity,
            variant.space,
        )
        .await?;
        let on_base = base.account(&account).await?.ok_or("pda not on base")?;
        check_eq!(
            on_base.owner,
            DELEGATION_PROGRAM_ID,
            "a delegated pda must be dlp-owned on base"
        )?;
        await_clone(er, &account, variant.space).await?;
        keys.push(account);
    }
    Ok(Accounts {
        a: keys[0],
        b: keys[1],
        c: keys[2],
        d: keys[3],
    })
}

async fn run_variant(
    proxies: &BaseProxies,
    base: &BaseCtx,
    er: &ErCtx,
    identity: Pubkey,
    variant: Variant,
) -> Result<Outcome> {
    let phase = variant.name;
    let started = Instant::now();
    let payer = prep::funded_payer(base, crate::PAYER_LAMPORTS).await?;
    let accounts =
        prepare_accounts(base, er, &payer, identity, variant).await?;
    let value = |offset: u64| variant.value_base + offset;
    let buffers_before = buffer_accounts(base).await?;

    let a1 = write(er, &payer, value(1), &accounts.a).await?;
    let b1 = write(er, &payer, value(2), &accounts.b).await?;
    let nonce_a = crate::last_commit_id(base, &accounts.a).await?;
    let nonce_b = crate::last_commit_id(base, &accounts.b).await?;
    let nonce_c = crate::last_commit_id(base, &accounts.c).await?;
    let nonce_d = crate::last_commit_id(base, &accounts.d).await?;
    let first_submission = proxies.intercept(
        Selector::method("sendTransaction")
            .http()
            .request()
            .account(&accounts.a),
    );
    let first = schedule(er, &payer, 1, &[accounts.a, accounts.b]).await?;
    let held_first = first_submission.wait(INTERCEPT_TIMEOUT).await?;

    let b2 = write(er, &payer, value(3), &accounts.b).await?;
    let c1 = write(er, &payer, value(4), &accounts.c).await?;
    let second_submission = proxies.intercept(
        Selector::method("sendTransaction")
            .http()
            .request()
            .account(&accounts.c),
    );
    let second = schedule(er, &payer, 2, &[accounts.b, accounts.c]).await?;
    let d1 = write(er, &payer, value(5), &accounts.d).await?;
    let third = schedule(er, &payer, 3, &[accounts.d]).await?;

    for (offset, account) in accounts.all().iter().enumerate() {
        write(er, &payer, value(11 + offset as u64), account).await?;
    }

    let hold_started = Instant::now();
    let third_bundle =
        settled(base, er, &third, &[accounts.d], &format!("{phase} {{D}}"))
            .await?;
    let d_settled_during_hold_s = hold_started.elapsed().as_secs_f64();
    await_base(
        base,
        &accounts.d,
        &d1,
        "the independent bundle lands its staged value while the first \
         bundle is held",
    )
    .await?;
    check_eq!(
        crate::last_commit_id(base, &accounts.d).await?,
        nonce_d + 1,
        "{phase}: the independent bundle must advance D's commit nonce \
         exactly once"
    )?;
    hold_nonces(
        base,
        &[accounts.a, accounts.b],
        &[nonce_a, nonce_b],
        HOLD_WINDOW,
        phase,
    )
    .await?;
    let buffers_during_hold =
        buffer_accounts(base).await?.saturating_sub(buffers_before);
    if variant.buffered {
        check!(
            buffers_during_hold > 0,
            "{phase}: large accounts must be staged in temporary commit \
             buffers on base while the first bundle is held"
        )?;
    } else {
        check_eq!(
            buffers_during_hold,
            0,
            "{phase}: small accounts must not need commit buffers"
        )?;
    }

    let retry_submission = proxies.intercept(
        Selector::method("sendTransaction")
            .http()
            .request()
            .account(&accounts.a),
    );
    let dropped_at = Instant::now();
    held_first.discard();
    let held_retry = retry_submission.wait(INTERCEPT_TIMEOUT).await?;
    let retry_gap_s = dropped_at.elapsed().as_secs_f64();
    let retry_held_at = held_retry.held_at;
    held_retry.release();

    let first_bundle = settled(
        base,
        er,
        &first,
        &[accounts.a, accounts.b],
        &format!("{phase} {{A,B}}"),
    )
    .await?;
    check_eq!(
        base_data(base, &accounts.a).await?,
        a1,
        "{phase}: A must carry the first bundle's staged value after its \
         retry settles"
    )?;
    check_eq!(
        base_data(base, &accounts.b).await?,
        b1,
        "{phase}: B must carry the first bundle's staged value before the \
         conflicting bundle is submitted"
    )?;
    check_eq!(
        crate::last_commit_id(base, &accounts.a).await?,
        nonce_a + 1,
        "{phase}: the retried bundle must advance A's commit nonce exactly \
         once"
    )?;
    check_eq!(
        crate::last_commit_id(base, &accounts.b).await?,
        nonce_b + 1,
        "{phase}: the retried bundle must advance B's commit nonce exactly \
         once before the conflicting bundle is submitted"
    )?;

    let held_second = second_submission.wait(INTERCEPT_TIMEOUT).await?;
    check!(
        held_second.held_at > retry_held_at,
        "{phase}: the conflicting bundle must not be submitted before the \
         delayed bundle retried (submitted at +{:.3}s, retry at +{:.3}s)",
        held_second.held_at.as_secs_f64(),
        retry_held_at.as_secs_f64()
    )?;
    held_second.release();
    let second_bundle = settled(
        base,
        er,
        &second,
        &[accounts.b, accounts.c],
        &format!("{phase} {{B,C}}"),
    )
    .await?;
    await_base(
        base,
        &accounts.b,
        &b2,
        "B carries the second bundle's staged value once it settles",
    )
    .await?;
    check_eq!(
        base_data(base, &accounts.c).await?,
        c1,
        "{phase}: C must carry the second bundle's staged value"
    )?;
    check_eq!(
        crate::last_commit_id(base, &accounts.b).await?,
        nonce_b + 2,
        "{phase}: the conflicting bundle must advance B's commit nonce \
         exactly once on top of the delayed bundle"
    )?;
    check_eq!(
        crate::last_commit_id(base, &accounts.c).await?,
        nonce_c + 1,
        "{phase}: the conflicting bundle must advance C's commit nonce \
         exactly once"
    )?;
    check!(
        first_bundle.slot <= second_bundle.slot,
        "{phase}: the delayed bundle must land no later than the \
         conflicting one (slots {} and {})",
        first_bundle.slot,
        second_bundle.slot
    )?;
    let first_position = history_position(
        base,
        &accounts.b,
        &first_bundle.receipt.base_signatures[0],
    )
    .await?;
    let second_position = history_position(
        base,
        &accounts.b,
        &second_bundle.receipt.base_signatures[0],
    )
    .await?;
    check!(
        first_position > second_position,
        "{phase}: B's base history must show the delayed bundle before the \
         conflicting one"
    )?;

    let mut finals = Vec::with_capacity(4);
    for (offset, account) in accounts.all().iter().enumerate() {
        finals
            .push(write(er, &payer, value(21 + offset as u64), account).await?);
    }
    let fourth = schedule(er, &payer, 4, &accounts.all()).await?;
    let fourth_bundle = settled(
        base,
        er,
        &fourth,
        &accounts.all(),
        &format!("{phase} {{A,B,C,D}}"),
    )
    .await?;
    for (account, snapshot) in accounts.all().iter().zip(&finals) {
        await_base(
            base,
            account,
            snapshot,
            "the follow-up bundle lands every account's final value",
        )
        .await?;
    }
    check::poll(
        "the temporary commit buffers are closed once every bundle settled",
        BUFFER_CLEANUP_TIMEOUT,
        || async { matches!(buffer_accounts(base).await, Ok(count) if count <= buffers_before) },
    )
    .await?;

    Ok(Outcome {
        variant: phase,
        d_settled_during_hold_s,
        retry_gap_s,
        first_slot: first_bundle.slot,
        second_slot: second_bundle.slot,
        buffers_during_hold,
        base_signatures: [
            first_bundle.receipt.base_signatures.len(),
            second_bundle.receipt.base_signatures.len(),
            third_bundle.receipt.base_signatures.len(),
            fourth_bundle.receipt.base_signatures.len(),
        ],
        seconds: started.elapsed().as_secs_f64(),
    })
}

fn report_outcome(report: ScenarioReport, outcome: &Outcome) -> ScenarioReport {
    let prefix = outcome.variant;
    report
        .setting(
            format!("{prefix} base slots {{A,B}} {{B,C}}"),
            format!("{} {}", outcome.first_slot, outcome.second_slot),
        )
        .setting(
            format!("{prefix} base sigs per bundle"),
            format!("{:?}", outcome.base_signatures),
        )
        .setting(
            format!("{prefix} buffers during hold"),
            outcome.buffers_during_hold,
        )
        .metric(
            format!("{prefix} d settled during hold s"),
            Unit::Seconds,
            outcome.d_settled_during_hold_s,
        )
        .metric(
            format!("{prefix} retry gap s"),
            Unit::Seconds,
            outcome.retry_gap_s,
        )
        .metric(
            format!("{prefix} variant s"),
            Unit::Seconds,
            outcome.seconds,
        )
}

#[async_trait(?Send)]
impl PrivateErScenario for CommitSettlementOrder {
    fn name(&self) -> &str {
        "redshift/commit_settlement_order"
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
        let identity = private.identity();

        let mut outcomes = Vec::with_capacity(VARIANTS.len());
        for variant in VARIANTS {
            outcomes.push(
                run_variant(&proxies, base, private.ctx(), identity, variant)
                    .await?,
            );
        }

        let events = proxies.finish()?;
        private.finish().await?;

        let mut report = ScenarioReport::ok(self.name())
            .setting("er", LABEL)
            .setting(
                "fault",
                "first bundle's sendTransaction held before base, then \
                 dropped to force a retry",
            )
            .setting("small account bytes", SMALL_SPACE)
            .setting("large account bytes", LARGE_SPACE);
        for outcome in &outcomes {
            report = report_outcome(report, outcome);
        }
        report =
            report.metric("fault events", Unit::Count, events.len() as f64);
        Ok(netfault::report_events(report, &events))
    }
}
