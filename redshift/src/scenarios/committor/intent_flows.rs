use std::time::Duration;

use async_trait::async_trait;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift_program::flexi::{
    build, FlexiCounter, FAIL_UNDELEGATION_LABEL, PRIZE,
};
use redsuite_core::{
    assert::poll_until, dlp, prep, receipt, system, BaseCtx, ChainCtx, ErCtx,
    Result, Scenario, ScenarioReport,
};
use signer::Signer;

const AIRDROP: u64 = 2_000_000_000;
const ESCROW: u64 = 500_000_000;
const ESCROW_INDEX: u8 = 1;
const COMPUTE_UNITS: u32 = 100_000;
const COMMIT_FREQUENCY_MS: u32 = 1_000_000_000;
const CLONE_TIMEOUT: Duration = Duration::from_secs(15);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);
const BASE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);
const STATE_TIMEOUT: Duration = Duration::from_secs(30);
const LABEL: &str = "redshift intent";
const TRANSFER_AMOUNT: u64 = 890_880;
const TRANSFER_FEES: u64 = 10_000;

pub struct IntentFlows;

struct Actor {
    payer: Keypair,
    counter: Pubkey,
}

impl Actor {
    fn pubkey(&self) -> Pubkey {
        self.payer.pubkey()
    }
}

fn dlp_id() -> Pubkey {
    "DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh"
        .parse()
        .expect("dlp id")
}

fn decode(data: &[u8]) -> Result<FlexiCounter> {
    Ok(FlexiCounter::try_decode(data)?)
}

// airdrop + escrow index 1 + init + delegate the counter, then wait for the
// ER clone.
async fn setup_actor(base: &BaseCtx, er: &ErCtx, label: &str) -> Result<Actor> {
    let payer = prep::funded_payer(base, AIRDROP).await?;
    base.send(
        &payer,
        &[dlp::top_up_ephemeral_balance(
            &payer.pubkey(),
            ESCROW,
            ESCROW_INDEX,
        )],
    )
    .await?;

    let (init, counter) = build::init_counter(payer.pubkey(), label);
    base.send(&payer, &[init]).await?;
    base.send(
        &payer,
        &[build::delegate_counter(
            payer.pubkey(),
            COMMIT_FREQUENCY_MS,
            Some(er.identity()),
        )],
    )
    .await?;
    let on_base = base
        .account(&counter)
        .await?
        .ok_or("the counter is not on base after delegate")?;
    assert_eq!(
        on_base.owner,
        dlp_id(),
        "dlp must own the delegated counter"
    );

    poll_until(CLONE_TIMEOUT, || async {
        matches!(
            er.account(&counter).await,
            Ok(Some(clone)) if !clone.data.is_empty()
        )
    })
    .await;
    Ok(Actor { payer, counter })
}

async fn add(er: &ErCtx, actor: &Actor, count: u8) -> Result<()> {
    let before = decode(
        // Drive a CreateIntent for the given actors, confirm the base commit, and
        // return the transfer destination so the caller can assert the PRIZE payout.
        &er.account(&actor.counter)
            .await?
            .ok_or("the counter clone is missing on the er")?
            .data,
    )?;
    er.send(&actor.payer, &[build::add(actor.pubkey(), count)])
        .await?;
    let after = decode(
        &er.account(&actor.counter)
            .await?
            .ok_or("the counter clone is missing on the er")?
            .data,
    )?;
    assert_eq!(
        after,
        FlexiCounter {
            count: before.count + count as u64,
            updates: before.updates + 1,
            label: before.label,
        },
        "the add must increase the count and the updates fields"
    );
    Ok(())
}

async fn er_count(er: &ErCtx, actor: &Actor) -> Result<u64> {
    let account = er
        .account(&actor.counter)
        .await?
        .ok_or("the counter clone is missing on the er")?;
    Ok(decode(&account.data)?.count)
}

async fn schedule_intent(
    base: &BaseCtx,
    er: &ErCtx,
    actors: &[&Actor],
    counter_diffs: Option<Vec<i64>>,
) -> Result<Pubkey> {
    let destination = Keypair::new().pubkey();
    let payers: Vec<_> = actors.iter().map(|actor| actor.pubkey()).collect();
    let cosigners: Vec<&Keypair> =
        actors.iter().skip(1).map(|actor| &actor.payer).collect();
    let instruction = build::create_intent(
        &payers,
        destination,
        counter_diffs.clone(),
        COMPUTE_UNITS,
    );
    let signature = er
        .send_with(&actors[0].payer, &cosigners, &[instruction])
        .await?;
    confirm_intent(base, er, &signature).await?;

    let undelegating = counter_diffs.is_some();
    let multiplier = if undelegating { 2 } else { 1 };
    let expected = multiplier * actors.len() as u64 * PRIZE;
    poll_until(STATE_TIMEOUT, || async {
        base.api().get_balance(&destination).await.unwrap_or(0) == expected
    })
    .await;
    let paid = base.api().get_balance(&destination).await?;
    assert_eq!(
        paid, expected,
        "the destination must receive {multiplier}x PRIZE per payer"
    );
    Ok(destination)
}

async fn confirm_intent(
    base: &BaseCtx,
    er: &ErCtx,
    signature: &signature::Signature,
) -> Result<receipt::CommitReceipt> {
    let commit_receipt =
        receipt::fetch_commit_receipt(er.api(), signature, RECEIPT_TIMEOUT)
            .await?;
    if let Some(message) = &commit_receipt.error_message {
        return Err(format!("the intent failed: {message}").into());
    }
    receipt::confirm_base_signatures(
        base.api(),
        &commit_receipt,
        BASE_CONFIRM_TIMEOUT,
    )
    .await?;
    Ok(commit_receipt)
}

async fn await_base_count(
    base: &BaseCtx,
    counter: &Pubkey,
    expected: u64,
) -> Result<()> {
    poll_until(STATE_TIMEOUT, || async {
        matches!(
            base.account(counter).await,
            Ok(Some(acc)) if decode(&acc.data).ok().map(|c| c.count) == Some(expected)
        )
    })
    .await;
    let on_base = base
        .account(counter)
        .await?
        .ok_or("the counter is not on base")?;
    assert_eq!(decode(&on_base.data)?.count, expected, "base counter");
    Ok(())
}

async fn await_undelegated_on_er(er: &ErCtx, counter: &Pubkey) -> Result<()> {
    poll_until(STATE_TIMEOUT, || async {
        matches!(
            er.account(counter).await,
            Ok(Some(acc)) if acc.owner != dlp_id()
        )
    })
    .await;
    Ok(())
}

async fn base_owner(base: &BaseCtx, counter: &Pubkey) -> Result<Pubkey> {
    Ok(base
        .account(counter)
        .await?
        .ok_or("the counter is not on base")?
        .owner)
}

async fn schedule_bundle(
    base: &BaseCtx,
    er: &ErCtx,
    commit_only: &[&Actor],
    undelegate: &[&Actor],
    counter_diffs: Vec<i64>,
) -> Result<()> {
    let destination = Keypair::new().pubkey();
    let commit_payers: Vec<_> =
        commit_only.iter().map(|actor| actor.pubkey()).collect();
    let undelegate_payers: Vec<_> =
        undelegate.iter().map(|actor| actor.pubkey()).collect();
    let all: Vec<&Actor> = commit_only
        .iter()
        .chain(undelegate.iter())
        .copied()
        .collect();
    let cosigners: Vec<&Keypair> =
        all.iter().skip(1).map(|actor| &actor.payer).collect();
    let instruction = build::create_intent_bundle(
        &commit_payers,
        &undelegate_payers,
        destination,
        counter_diffs,
        COMPUTE_UNITS,
    );
    let signature = er
        .send_with(&all[0].payer, &cosigners, &[instruction])
        .await?;
    confirm_intent(base, er, &signature).await?;

    let expected =
        (commit_only.len() as u64 + 2 * undelegate.len() as u64) * PRIZE;
    poll_until(STATE_TIMEOUT, || async {
        base.api().get_balance(&destination).await.unwrap_or(0) == expected
    })
    .await;
    assert_eq!(
        base.api().get_balance(&destination).await?,
        expected,
        "the bundle destination payout"
    );
    Ok(())
}

async fn transfer_intent_cell(
    base: &BaseCtx,
    er: &ErCtx,
    funder: &Keypair,
    fail: bool,
) -> Result<(u64, u64)> {
    let actor = setup_actor(base, er, LABEL).await?;
    let delegate_setup = [
        system::assign(&actor.pubkey(), &dlp::dlp_id()),
        dlp::delegate_account(
            &funder.pubkey(),
            &actor.pubkey(),
            &er.identity(),
        ),
    ];
    base.send_with(funder, &[&actor.payer], &delegate_setup)
        .await?;

    let vault = dlp::magic_fee_vault_pda(&er.identity());
    assert!(
        er.account(&vault).await?.is_some(),
        "the magic fee vault for the booted er identity must exist"
    );

    let destination = Keypair::new().pubkey();
    let before = er.api().get_balance(&actor.pubkey()).await?;
    let signature = er
        .send(
            &actor.payer,
            &[build::create_transfer_intent(
                actor.pubkey(),
                destination,
                vault,
                TRANSFER_AMOUNT,
                fail,
                COMPUTE_UNITS,
            )],
        )
        .await?;
    confirm_intent(base, er, &signature).await?;

    let expected_paid = if fail { 0 } else { TRANSFER_AMOUNT };
    let expected_charged = if fail {
        TRANSFER_FEES
    } else {
        TRANSFER_AMOUNT + TRANSFER_FEES
    };
    poll_until(STATE_TIMEOUT, || async {
        let paid = base
            .api()
            .get_balance(&destination)
            .await
            .unwrap_or(u64::MAX);
        let after = er
            .api()
            .get_balance(&actor.pubkey())
            .await
            .unwrap_or(u64::MAX);
        paid == expected_paid
            && before.checked_sub(after) == Some(expected_charged)
    })
    .await;
    let paid = base.api().get_balance(&destination).await?;
    let after = er.api().get_balance(&actor.pubkey()).await?;
    Ok((paid, before - after))
}

async fn flow_basic_and_repeat(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let actor = setup_actor(base, er, LABEL).await?;
    add(er, &actor, 101).await?;
    assert_eq!(
        er_count(er, &actor).await?,
        101,
        "ephem count before intent"
    );
    schedule_intent(base, er, &[&actor], None).await?;
    await_base_count(base, &actor.counter, 101).await?;

    add(er, &actor, 2).await?;
    schedule_intent(base, er, &[&actor], None).await?;
    await_base_count(base, &actor.counter, 103).await?;
    Ok(())
}

async fn flow_commit_and_undelegate(
    base: &BaseCtx,
    er: &ErCtx,
) -> Result<Pubkey> {
    let undelegated = setup_actor(base, er, LABEL).await?;
    add(er, &undelegated, 101).await?;
    schedule_intent(base, er, &[&undelegated], Some(vec![-100])).await?;
    await_base_count(base, &undelegated.counter, 1).await?;
    await_undelegated_on_er(er, &undelegated.counter).await?;
    base_owner(base, &undelegated.counter).await
}

async fn flow_single_undelegation(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let single = setup_actor(base, er, LABEL).await?;
    add(er, &single, 100).await?;
    schedule_intent(base, er, &[&single], Some(vec![-50])).await?;
    await_base_count(base, &single.counter, 50).await?;
    await_undelegated_on_er(er, &single.counter).await?;
    Ok(())
}

async fn flow_two_payer(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let first = setup_actor(base, er, LABEL).await?;
    let second = setup_actor(base, er, LABEL).await?;
    add(er, &first, 100).await?;
    add(er, &second, 200).await?;
    schedule_intent(base, er, &[&first, &second], Some(vec![-50, 25])).await?;
    await_base_count(base, &first.counter, 50).await?;
    await_base_count(base, &second.counter, 225).await?;
    await_undelegated_on_er(er, &first.counter).await?;
    await_undelegated_on_er(er, &second.counter).await?;
    Ok(())
}

async fn flow_bundle_mixed(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let bundle_a = setup_actor(base, er, LABEL).await?;
    let bundle_b = setup_actor(base, er, LABEL).await?;
    let bundle_c = setup_actor(base, er, LABEL).await?;
    add(er, &bundle_a, 50).await?;
    add(er, &bundle_b, 75).await?;
    add(er, &bundle_c, 100).await?;
    schedule_bundle(base, er, &[&bundle_a, &bundle_b], &[&bundle_c], vec![-10])
        .await?;
    await_base_count(base, &bundle_a.counter, 50).await?;
    await_base_count(base, &bundle_b.counter, 75).await?;
    await_base_count(base, &bundle_c.counter, 90).await?;
    assert_eq!(
        base_owner(base, &bundle_a.counter).await?,
        dlp_id(),
        "a commit-only bundle member stays delegated"
    );
    assert_eq!(
        base_owner(base, &bundle_b.counter).await?,
        dlp_id(),
        "a commit-only bundle member stays delegated"
    );
    await_undelegated_on_er(er, &bundle_c.counter).await?;
    Ok(())
}

async fn flow_bundle_commit_only(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let only_a = setup_actor(base, er, LABEL).await?;
    let only_b = setup_actor(base, er, LABEL).await?;
    add(er, &only_a, 42).await?;
    add(er, &only_b, 88).await?;
    schedule_bundle(base, er, &[&only_a, &only_b], &[], vec![]).await?;
    await_base_count(base, &only_a.counter, 42).await?;
    await_base_count(base, &only_b.counter, 88).await?;
    assert_eq!(
        base_owner(base, &only_a.counter).await?,
        dlp_id(),
        "a commit-only bundle member stays delegated"
    );
    assert_eq!(
        base_owner(base, &only_b.counter).await?,
        dlp_id(),
        "a commit-only bundle member stays delegated"
    );
    Ok(())
}

async fn flow_bundle_undelegate_only(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let und_a = setup_actor(base, er, LABEL).await?;
    let und_b = setup_actor(base, er, LABEL).await?;
    add(er, &und_a, 200).await?;
    add(er, &und_b, 250).await?;
    schedule_bundle(base, er, &[], &[&und_a, &und_b], vec![50, -100]).await?;
    await_base_count(base, &und_a.counter, 250).await?;
    await_base_count(base, &und_b.counter, 150).await?;
    await_undelegated_on_er(er, &und_a.counter).await?;
    await_undelegated_on_er(er, &und_b.counter).await?;
    Ok(())
}

async fn flow_bundle_commit_and_finalize(
    base: &BaseCtx,
    er: &ErCtx,
) -> Result<()> {
    let commit_actor = setup_actor(base, er, LABEL).await?;
    let finalize_actor = setup_actor(base, er, LABEL).await?;
    add(er, &commit_actor, 31).await?;
    add(er, &finalize_actor, 47).await?;
    let finalize_ix = build::create_intent_bundle_commit_and_finalize(
        commit_actor.pubkey(),
        &[commit_actor.pubkey()],
        &[finalize_actor.pubkey()],
    );
    let finalize_sig = er.send(&commit_actor.payer, &[finalize_ix]).await?;
    confirm_intent(base, er, &finalize_sig).await?;
    await_base_count(base, &commit_actor.counter, 31).await?;
    await_base_count(base, &finalize_actor.counter, 47).await?;
    assert_eq!(
        base_owner(base, &commit_actor.counter).await?,
        dlp_id(),
        "commit keeps the account delegated"
    );
    assert_eq!(
        base_owner(base, &finalize_actor.counter).await?,
        dlp_id(),
        "commit-finalize keeps the account delegated"
    );
    Ok(())
}

async fn flow_transfer_intents(base: &BaseCtx, er: &ErCtx) -> Result<()> {
    let funder = prep::funded_payer(base, AIRDROP).await?;
    let (success_paid, success_charged) =
        transfer_intent_cell(base, er, &funder, false).await?;
    assert_eq!(
        success_paid, TRANSFER_AMOUNT,
        "a successful transfer intent must pay the destination"
    );
    assert_eq!(
        success_charged,
        TRANSFER_AMOUNT + TRANSFER_FEES,
        "the payer must be charged the amount plus both fees"
    );

    let (fail_paid, fail_charged) =
        transfer_intent_cell(base, er, &funder, true).await?;
    assert_eq!(
        fail_paid, 0,
        "a failed transfer intent must not pay the destination"
    );
    assert_eq!(
        fail_charged, TRANSFER_FEES,
        "the callback must refund the amount, leaving only the fees"
    );
    Ok(())
}

#[async_trait(?Send)]
impl Scenario for IntentFlows {
    fn name(&self) -> &str {
        "redshift/intent_flows"
    }

    async fn run(&self, base: &BaseCtx, er: &ErCtx) -> Result<ScenarioReport> {
        let (_, undelegate_owner, _, _, _, _, _, _, _) = tokio::try_join!(
            flow_basic_and_repeat(base, er),
            flow_commit_and_undelegate(base, er),
            flow_single_undelegation(base, er),
            flow_two_payer(base, er),
            flow_bundle_mixed(base, er),
            flow_bundle_commit_only(base, er),
            flow_bundle_undelegate_only(base, er),
            flow_bundle_commit_and_finalize(base, er),
            flow_transfer_intents(base, er),
        )?;

        Ok(ScenarioReport::ok(self.name())
            .setting("prize lamports", PRIZE)
            .setting("escrow lamports", ESCROW)
            .setting("compute units", COMPUTE_UNITS)
            .setting("undelegate base owner", undelegate_owner)
            .setting("transfer amount", TRANSFER_AMOUNT)
            .setting("fail undelegation label", FAIL_UNDELEGATION_LABEL))
    }
}
