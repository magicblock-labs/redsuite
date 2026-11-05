use super::{
    channels::{account_image, Channels},
    finish, fixture, BaseCtx, ErCtx, Result, Scenario, TIMEOUT,
};
use anyhow::{ensure, Context};
use redsuite_core::transport::wsraw::RawWs;
use std::{
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};
use tokio::time::Instant;
use tracing::info;

const SUBSCRIPTIONS: usize = 256;
const MAX_WRITES: usize = 256;
const DISCONNECTS: &str =
    r#"engine_keeper_subscription_slow_consumer_disconnects{subscription="accounts"}"#;

signal_scenario!(SlowSubscriberSignals, slow_subscriber);

async fn slow_subscriber(base: Rc<BaseCtx>, er: ErCtx) -> Result<()> {
    let mut fixture = fixture(base, er, "slow_subscriber_signals").await?;
    let mut channels = Channels::open(&fixture).await?;
    channels.control(&mut fixture, 14_000, &[]).await?;
    let mut slow = RawWs::connect(fixture.er.ws_url()).await?;
    let mut subscriptions = BTreeSet::new();
    for _ in 0..SUBSCRIPTIONS {
        ensure!(
            subscriptions.insert(slow.account_subscribe(&fixture.keys[1]).await?),
            "duplicate subscription ID"
        );
    }
    channels.control(&mut fixture, 14_001, &[]).await?;
    let image = fixture.expected[1].as_ref().context("baseline image missing")?;
    let mut baseline_delivered = BTreeSet::new();
    let deadline = Instant::now() + TIMEOUT;
    while baseline_delivered.len() != subscriptions.len() {
        let (method, id, payload) = slow
            .next_notification(deadline.saturating_duration_since(Instant::now()))
            .await?
            .context("slow-subscriber baseline delivery missing")?;
        ensure!(method == "accountNotification" && subscriptions.contains(&id));
        ensure!(
            account_image(&payload["value"])? == *image,
            "slow baseline reported an uncommitted account image"
        );
        baseline_delivered.insert(id);
    }
    let initial = fixture.er.scrape_metrics().await?.get(DISCONNECTS).unwrap_or(0.0);
    let mut committed = Vec::new();
    for index in 0..MAX_WRITES {
        channels.pressure_control(&mut fixture, 14_100 + index as u64).await?;
        committed.push(fixture.expected[1].clone().context("pressure image missing")?);
        if fixture.er.scrape_metrics().await?.get(DISCONNECTS).unwrap_or(0.0) > initial {
            break;
        }
    }
    let after = fixture.er.scrape_metrics().await?;
    let disconnected = after.get(DISCONNECTS).unwrap_or(0.0) - initial;
    ensure!(
        disconnected >= 0.0 && disconnected <= SUBSCRIPTIONS as f64,
        "eviction metric is not isolated: {disconnected}"
    );
    let mut committed_by_data: BTreeMap<&[u8], Vec<usize>> = BTreeMap::new();
    for (index, image) in committed.iter().enumerate() {
        committed_by_data.entry(&image.data).or_default().push(index);
    }
    let mut delivered: BTreeMap<u64, BTreeSet<usize>> = BTreeMap::new();
    let mut explicit_limit = false;
    let mut received = 0;
    let expected = committed.len() * subscriptions.len();
    let drain = Instant::now() + TIMEOUT;
    while received < expected {
        let remaining = drain.saturating_duration_since(Instant::now());
        let frame = match slow.next_notification(remaining).await {
            Ok(frame) => frame,
            Err(error) => {
                let message = format!("{error:#}").to_ascii_lowercase();
                explicit_limit =
                    ["slow consumer", "overload", "queue full", "capacity", "subscription limit"]
                        .iter()
                        .any(|needle| message.contains(needle));
                info!(%error, explicit_limit, "slow subscription closed");
                break;
            }
        };
        let Some((method, id, payload)) = frame else { break };
        ensure!(method == "accountNotification" && subscriptions.contains(&id));
        let image = account_image(&payload["value"])?;
        let index = committed_by_data
            .get(image.data.as_slice())
            .and_then(|indices| indices.iter().copied().find(|index| committed[*index] == image))
            .context("slow subscriber reported an image that never committed (#261)")?;
        received += usize::from(delivered.entry(id).or_default().insert(index));
    }
    ensure!(
        slow.malformed_frames() == 0,
        "slow subscriber received malformed frames (#261)"
    );
    let missing = expected - received;
    ensure!(disconnected > 0.0 || missing > 0 || explicit_limit,
        "slow-subscriber overrun not reached after {} committed writes; no eviction or missing update witnessed", committed.len());
    let findings = if missing > 0 && !explicit_limit {
        vec![format!("validator#1874: slow subscriber/account: {missing} committed notifications omitted without a named limit")]
    } else {
        vec![]
    };
    info!(
        writes = committed.len(),
        received, missing, disconnected, explicit_limit, "slow subscriber pressure observed"
    );
    drop(slow);
    let mut recovered = RawWs::connect(fixture.er.ws_url()).await?;
    let id = recovered.account_subscribe(&fixture.keys[1]).await?;
    channels.control(&mut fixture, 14_999, &[]).await?;
    let (method, observed_id, payload) = recovered
        .next_notification(TIMEOUT)
        .await?
        .context("account channel did not recover after eviction")?;
    ensure!(method == "accountNotification" && observed_id == id);
    ensure!(account_image(&payload["value"])? == *fixture.expected[1].as_ref().unwrap());
    finish(findings)
}
