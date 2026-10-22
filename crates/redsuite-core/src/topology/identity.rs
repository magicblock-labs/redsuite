use std::{
    collections::BTreeMap,
    fs,
    fs::{File, OpenOptions},
    path::Path,
    time::Duration,
};

use anyhow::{anyhow, Context};
use base64::{engine::general_purpose::STANDARD, Engine};
use keypair::Keypair;
use pubkey::Pubkey;
use signer::Signer;
use tokio::{time, time::Instant};

use super::{config::DLP_ID, state};
use crate::{context::BaseCtx, ChainCtx, Result};

pub(super) const IDENTITY_POOL_SIZE: usize = 64;
const POOL_MAP_FILE: &str = "identity-pool.json";
const POOL_LOCK_FILE: &str = "identity-pool.lock";

const VAULT_SPACE: usize = 8;
const VAULT_LAMPORTS: u64 = 10_000_000;

const IDENTITY_FUNDING_LAMPORTS: u64 = 20 * 1_000_000_000;
const FUNDING_POLL: Duration = Duration::from_millis(250);

pub fn identity_keypair(identity: &Pubkey) -> Result<Keypair> {
    let state =
        state::read_state().context("no shared stack state — boot the shared stack first")?;
    state
        .er_identity_pool
        .iter()
        .filter_map(|bytes| Keypair::try_from(&bytes[..]).ok())
        .find(|keypair| keypair.pubkey() == *identity)
        .with_context(|| format!("{identity} is not one of this stack's ER identities"))
}

fn vault_dump_json(vault: &Pubkey) -> String {
    let data = STANDARD.encode([0u8; VAULT_SPACE]);
    format!(
        r#"{{"pubkey":"{vault}","account":{{"lamports":{VAULT_LAMPORTS},"data":["{data}","base64"],"owner":"{DLP_ID}","executable":false,"rentEpoch":0,"space":{VAULT_SPACE}}}}}"#
    )
}

pub(super) fn write_vault_dump(dir: &Path, identity: &Pubkey) -> Result<()> {
    let vault = crate::dlp::validator_fees_vault_pda(identity);
    fs::write(dir.join(format!("{vault}.json")), vault_dump_json(&vault))?;
    Ok(())
}

// A fresh base is a fresh generation: previous label assignments are void.
pub(super) fn reset_pool(dir: &Path) {
    let _ = fs::remove_file(dir.join(POOL_MAP_FILE));
    let _ = fs::remove_file(dir.join(POOL_LOCK_FILE));
}

pub fn identity_for_label(label: &str) -> Result<Keypair> {
    let state =
        state::read_state().context("no shared stack state — boot the shared stack first")?;
    if state.er_identity_pool.is_empty() {
        return Err(anyhow!(
            "this stack predates per-ER identities — run \
                    `redsuite stack down` and boot it again"
        ));
    }
    let dir = state::stack_dir();
    let _guard = PoolLock::acquire(&dir)?;
    let map_path = dir.join(POOL_MAP_FILE);
    let mut assignments: BTreeMap<String, usize> = fs::read_to_string(&map_path)
        .ok()
        .and_then(|raw| json::from_str(&raw).ok())
        .unwrap_or_default();

    let slot = match assignments.get(label) {
        Some(slot) => *slot,
        None => {
            let slot = assignments.len();
            if slot >= state.er_identity_pool.len() {
                return Err(anyhow!(
                    "the stack reserved {} private-ER identities and all are \
                     bound; raise IDENTITY_POOL_SIZE and boot a fresh stack",
                    state.er_identity_pool.len()
                ));
            }
            assignments.insert(label.to_owned(), slot);
            fs::write(&map_path, json::to_string(&assignments)?)?;
            slot
        }
    };
    Keypair::try_from(&state.er_identity_pool[slot][..])
        .with_context(|| format!("corrupt state.json: bad pool identity {slot}"))
}

struct PoolLock(#[allow(dead_code)] File);

impl PoolLock {
    fn acquire(dir: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(POOL_LOCK_FILE))?;
        file.lock()?;
        Ok(Self(file))
    }
}

// The identity SPENDS concurrently (vault init, mdp sync), so the confirm
// polls the ABSOLUTE funding target — an exact-increment poll can never
// land while the balance moves under it.
pub(super) async fn ensure_identity_funded(base: &BaseCtx, identity: &Pubkey) -> Result<()> {
    let funded = || async {
        base.api().get_balance(identity).await.unwrap_or(0) >= IDENTITY_FUNDING_LAMPORTS
    };
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut ticks = 0u32;
    loop {
        if funded().await {
            return Ok(());
        }
        // Re-request on a cadence — a busy freshly-booted base can drop the
        // faucet transaction, so one request is not enough.
        if ticks.is_multiple_of(20) {
            let _ = base.api().request_airdrop(identity, IDENTITY_FUNDING_LAMPORTS).await;
        }
        ticks += 1;
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "the identity {identity} did not reach the funding target"
            ));
        }
        time::sleep(FUNDING_POLL).await;
    }
}
