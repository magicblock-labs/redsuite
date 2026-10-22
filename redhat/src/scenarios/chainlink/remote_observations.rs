use std::{collections::HashMap, rc::Rc, time::Duration};

use account::Account;
use async_trait::async_trait;
use futures_util::future::join;
use instruction::{AccountMeta, Instruction};
use json::JsonValueTrait;
use keypair::Keypair;
use pubkey::Pubkey;
use redshift::program::{self, instruction::build};
use redshift::scenarios::chainlink::spl;
use redshift_interface::flexi::{build as flexi, FlexiCounter};
use redsuite_core::{
    check, check_eq, dlp,
    netfault::{Action, BaseProxies, Held, Selector},
    phase, prep,
    random::{self, Rng},
    receipt,
    redline::Accounts,
    system,
    topology::{self, ErOptions, PrivateEr, RestartConfig},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use signer::Signer;
use tracing::info;

const TIMEOUT: Duration = Duration::from_secs(30);
const FETCHES: &[&str] = &["getAccountInfo", "getMultipleAccounts"];
const ROLES: [&str; 6] =
    ["funder", "owner", "payer", "trigger", "mint", "other"];

pub enum RemoteObservations {
    State,
    Tokens,
}

struct Case {
    value: u64,
    poke: u64,
    action: u8,
    reverse: bool,
    keys: [Keypair; 6],
    pool: Vec<Pubkey>,
}

impl Case {
    fn inputs(&self) -> String {
        format!(
            "value={} poke={} action={} reverse={} pool={} fixtures={:?}",
            self.value,
            self.poke,
            self.action,
            self.reverse,
            self.pool.len(),
            self.keys.each_ref().map(Signer::pubkey)
        )
    }
}

fn key(seed: u64, scenario: &str, role: &str, index: u64) -> Keypair {
    Keypair::new_from_array(
        random::rng(seed, &[scenario, "fixtures", role], index).gen(),
    )
}

async fn local_in(
    er: &ErCtx,
    owners: &[Pubkey],
    keys: &[Pubkey],
) -> Result<Vec<Option<Account>>> {
    let mut accounts = HashMap::new();
    for owner in owners {
        accounts.extend(er.api().get_program_accounts(owner).await?);
    }
    Ok(keys.iter().map(|key| accounts.get(key).cloned()).collect())
}

async fn local(er: &ErCtx, keys: &[Pubkey]) -> Result<Vec<Option<Account>>> {
    let owners = [
        program::id(),
        redshift_interface::id(),
        spl::token_program(),
        dlp::dlp_id(),
        system::system_id(),
    ];
    local_in(er, &owners, keys).await
}

async fn unchanged(
    er: &ErCtx,
    keys: &[Pubkey],
    expected: &[Option<Account>],
) -> Result<()> {
    check_eq!(
        local(er, keys).await?,
        expected,
        "protected full account images"
    )?;
    Ok(())
}

async fn marker(er: &ErCtx, key: Pubkey, expected: u64) -> Result<()> {
    check::poll_for("local marker", TIMEOUT, || async {
        let state = local(er, &[key]).await?;
        check_eq!(
            state[0]
                .as_ref()
                .and_then(|a| redsuite_core::redline::written_id(&a.data)),
            Some(expected),
            "marker {key}"
        )?;
        Result::Ok(())
    })
    .await?;
    Ok(())
}

async fn count(
    er: &ErCtx,
    counter: Pubkey,
    expected: u64,
    updates: u64,
) -> Result<()> {
    check::poll_for("activation counter", TIMEOUT, || async {
        let state =
            local_in(er, &[redshift_interface::id()], &[counter]).await?;
        let account = state[0].as_ref().ok_or("local counter missing")?;
        let actual = FlexiCounter::try_decode(&account.data)?;
        check_eq!(
            (actual.count, actual.updates),
            (expected, updates),
            "action executes once"
        )?;
        Result::Ok(())
    })
    .await?;
    Ok(())
}

fn notification(key: &Pubkey) -> Selector {
    Selector::method("programNotification")
        .ws()
        .notification()
        .account(key)
}

fn mirrored(key: &Pubkey) -> Selector {
    Selector::method("accountNotification")
        .ws()
        .notification()
        .account(key)
}

fn record_fetch(record: &Pubkey) -> Selector {
    Selector::methods(FETCHES).http().response().account(record)
}

fn replay(held: Held, old: &str) -> Result<()> {
    let mut frame: json::Value = json::from_str(&held.body)?;
    let old: json::Value = json::from_str(old)?;
    if frame.get("params").is_some() {
        frame["params"]["result"] = old["params"]["result"].clone();
    } else {
        frame["result"] = old["result"].clone();
    }
    held.replace(json::to_string(&frame)?);
    Ok(())
}

fn missing_record(held: Held, record: Pubkey) -> Result<()> {
    let mut body: json::Value = json::from_str(&held.body)?;
    if body["result"]["value"].is_array() {
        let position = held
            .operation
            .accounts
            .iter()
            .position(|key| key == &record.to_string())
            .ok_or("record request missing")?;
        check!(
            body["result"]["value"].get(position).is_some(),
            "record response position"
        )?;
        body["result"]["value"][position] = json::Value::new();
    } else {
        body["result"]["value"] = json::Value::new();
    }
    held.replace(json::to_string(&body)?);
    Ok(())
}

fn delegate_with_action(
    funder: &Keypair,
    target: &Pubkey,
    actor: Pubkey,
    action: u8,
    validator: Pubkey,
) -> [Instruction; 2] {
    [
        system::assign(target, &dlp::dlp_id()),
        dlp::delegate_with_actions(
            &funder.pubkey(),
            target,
            None,
            dlp::DelegateArgs {
                commit_frequency_ms: u32::MAX,
                seeds: vec![],
                validator: Some(validator),
            },
            &[flexi::add_unsigned(actor, action)],
        ),
    ]
}

async fn holds(
    er: &ErCtx,
    keys: &[Pubkey],
    expected: &[Option<Account>],
    counter: Pubkey,
    action: u8,
) -> Result<()> {
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        unchanged(er, keys, expected).await?;
        count(er, counter, u64::from(action) * 2, 2).await?;
    }
    Ok(())
}

async fn refused(
    er: &ErCtx,
    signer: Rc<Keypair>,
    instructions: &[Instruction],
    error: json::Value,
    reason: &str,
) -> Result<()> {
    let signature = er.sender(signer).submit(instructions).await?;
    let rejected = er.api().await_transaction(&signature, TIMEOUT).await?;
    check_eq!(rejected.err, Some(error), "{reason}: {:?}", rejected.logs)?;
    Ok(())
}

async fn base_poke(
    base: &BaseCtx,
    proxies: &BaseProxies,
    funder: &Keypair,
    target: Pubkey,
    lamports: u64,
) -> Result<Held> {
    let update = proxies.intercept(notification(&target));
    base.submit_and_confirm(
        funder,
        &[system::transfer(&funder.pubkey(), &target, lamports)],
    )
    .await?;
    update.wait(TIMEOUT).await
}

async fn write(
    ctx: &impl ChainCtx,
    signer: &Keypair,
    value: u64,
    keys: &[Pubkey],
) -> Result<()> {
    ctx.submit_and_confirm(signer, &[build::simple_byte_set(value, keys)])
        .await?;
    Ok(())
}

async fn delegate_eata(
    base: &BaseCtx,
    payer: &Keypair,
    user: &Pubkey,
    mint: &Pubkey,
    validator: &Pubkey,
) -> Result<()> {
    base.submit_and_confirm(
        payer,
        &[spl::delegate_eata(&payer.pubkey(), user, mint, validator)],
    )
    .await?;
    Ok(())
}

async fn backs(
    base: &BaseCtx,
    eatas: &[Pubkey; 2],
    expected: [u64; 2],
    reason: &str,
) -> Result<()> {
    for (eata, amount) in eatas.iter().zip(expected) {
        let balance = spl::token_balance(base, eata).await?;
        check_eq!(balance, Some(amount), "{reason}")?;
    }
    Ok(())
}

async fn boot(base: &BaseCtx, label: &str) -> Result<(BaseProxies, PrivateEr)> {
    let proxies = BaseProxies::spawn(base).await?;
    let private = topology::private_er(
        base,
        ErOptions {
            label: label.into(),
            settings: Box::new(|params| {
                params.engine.accountsdb.lru_capacity = 64
            }),
            request_timeout: Some(TIMEOUT),
            base_endpoints: Some(proxies.endpoints()),
        },
    )
    .await?;
    Ok((proxies, private))
}

async fn settled(
    base: &BaseCtx,
    er: &ErCtx,
    signature: &signature::Signature,
    expected: &[Pubkey],
) -> Result<()> {
    let mut receipt =
        receipt::fetch_commit_receipt(er.api(), signature, TIMEOUT).await?;
    check!(
        receipt.succeeded() && !receipt.base_signatures.is_empty(),
        "strict settlement: {receipt:?}"
    )?;
    receipt.included.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    check_eq!(receipt.included, expected, "settlement targets")?;
    check!(
        receipt.excluded.is_empty(),
        "no excluded settlement targets"
    )?;
    receipt::confirm_base_signatures(base.api(), &receipt, TIMEOUT).await
}

async fn evictions(er: &ErCtx) -> Result<f64> {
    er.scrape_metrics()
        .await?
        .get("engine_keeper_account_cache_evictions")
        .ok_or_else(|| "eviction metric missing".into())
}

async fn pressure(
    base: &BaseCtx,
    er: &ErCtx,
    funder: &Keypair,
    pool: &[Pubkey],
) -> Result<()> {
    for chunk in pool.chunks(16) {
        let transfers: Vec<_> = chunk
            .iter()
            .map(|key| system::transfer(&funder.pubkey(), key, 1_000_000))
            .collect();
        base.submit_and_confirm(funder, &transfers).await?;
    }
    let before = evictions(er).await?;
    er.accounts(&pool[..32]).await?;
    let resident = local_in(er, &[system::system_id()], &pool[..32]).await?;
    check!(
        resident.iter().all(Option::is_some),
        "eligible control entries are initially resident"
    )?;
    for chunk in pool[32..].chunks(32) {
        er.accounts(chunk).await?;
    }
    check::poll_for("eligible entries evicted", TIMEOUT, || async {
        let after = evictions(er).await?;
        check!(after > before, "real cache eviction: {before} -> {after}")?;
        let cached = local_in(er, &[system::system_id()], &pool[..32]).await?;
        check!(
            cached.iter().any(Option::is_none),
            "old eligible entry leaves the local cache"
        )?;
        Result::Ok(())
    })
    .await?;
    Ok(())
}

#[async_trait(?Send)]
impl PrivateErScenario for RemoteObservations {
    fn name(&self) -> &str {
        match self {
            Self::State => "redhat/stale_remote_state",
            Self::Tokens => "redhat/stale_token_projection",
        }
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        let seed = base.config().seed;
        let cases: Vec<Case> = (0..3)
            .map(|index| {
                let mut rng =
                    random::rng(seed, &[self.name(), "payload"], index);
                let mut pool = random::rng(
                    seed,
                    &[self.name(), "fixtures", "pressure"],
                    index,
                );
                Case {
                    value: rng.gen_range(100..1000),
                    poke: rng.gen_range(1..17),
                    action: match self {
                        Self::State => rng.gen_range(1..8),
                        Self::Tokens => 0,
                    },
                    reverse: random::rng(
                        seed,
                        &[self.name(), "schedule"],
                        index,
                    )
                    .gen(),
                    keys: ROLES.map(|role| key(seed, self.name(), role, index)),
                    pool: (0..512)
                        .map(|_| Pubkey::new_from_array(pool.gen()))
                        .collect(),
                }
            })
            .collect();
        info!(
            seed,
            scenario = self.name(),
            revisions = random::revisions(),
            "case plan"
        );
        for (index, case) in cases.iter().enumerate() {
            info!(index, input = %case.inputs(), "case planned");
        }
        let mut failures = Vec::new();
        for (index, case) in cases.into_iter().enumerate() {
            let context = format!(
                "seed={seed} case={index} {} revisions={}",
                case.inputs(),
                random::revisions()
            );
            let label = format!("{}-{index}", self.name().replace('/', "-"));
            let result = phase(&context, async {
                match self {
                    Self::State => {
                        state(base, &label, index as u64, case).await
                    }
                    Self::Tokens => {
                        tokens(base, &label, index as u64, case).await
                    }
                }
            })
            .await;
            info!(seed, case = index, passed = result.is_ok(), "case finished");
            if let Err(error) = result {
                tracing::error!(seed, case = index, %error, "case failed");
                failures.push(error.to_string());
            }
        }
        check!(failures.is_empty(), "{}", failures.join("\n"))?;
        Ok(())
    }
}

async fn state(
    base: &BaseCtx,
    label: &str,
    index: u64,
    case: Case,
) -> Result<()> {
    let Case {
        value,
        poke,
        action,
        reverse,
        keys: [funder, actor, payer, trigger, magic_key, _],
        pool,
    } = case;
    let (proxies, mut private) = boot(base, label).await?;
    let er = private.ctx().clone();
    for key in [&funder, &actor, &payer, &trigger] {
        base.airdrop(&key.pubkey(), crate::PAYER_LAMPORTS).await?;
    }
    prep::delegate_payer(base, &funder, &payer, er.identity()).await?;
    let (counter, setup) =
        prep::flexi_counter(actor.pubkey(), label, er.identity(), u32::MAX);
    for ix in setup {
        base.submit_and_confirm(&actor, &[ix]).await?;
    }
    er.account(&counter).await?;
    count(&er, counter, 0, 0).await?;
    base.submit_and_confirm_with(
        &funder,
        &[&trigger],
        &delegate_with_action(
            &funder,
            &trigger.pubkey(),
            actor.pubkey(),
            action,
            er.identity(),
        ),
    )
    .await?;
    er.account(&trigger.pubkey()).await?;
    count(&er, counter, u64::from(action), 1).await?;
    let pending = Accounts::new(128, er.identity())
        .init(base, &funder, 0, true)
        .await?;
    let mirror = Accounts::new(128, er.identity())
        .init(base, &funder, 1, false)
        .await?;
    let magic = magic_key.pubkey();
    er.submit_and_confirm_with(&payer, &[&magic_key], &[
        redshift_interface::ephemeral_cpi(Instruction {
            program_id: magic_api::id(),
            accounts: vec![AccountMeta::new(payer.pubkey(), true), AccountMeta::new(magic, true), AccountMeta::new(sdk::consts::EPHEMERAL_VAULT_ID, false)],
            data: magic_api::instruction::MagicBlockInstruction::CreateEphemeralAccount { data_len: 32 }.try_to_vec()?,
        }, value as u8),
    ]).await?;
    base.airdrop(&magic, crate::PAYER_LAMPORTS).await?;
    let magic_update = proxies.intercept(notification(&magic));
    base.submit_and_confirm_with(
        &funder,
        &[&magic_key],
        &delegate_with_action(
            &funder,
            &magic,
            actor.pubkey(),
            action,
            er.identity(),
        ),
    )
    .await?;
    let magic_update = magic_update.wait(TIMEOUT).await?;
    er.accounts(&[pending, mirror, magic]).await?;
    let old =
        base_poke(base, &proxies, &funder, trigger.pubkey(), poke).await?;
    let replayed = old.body.clone();
    write(&er, &payer, value, &[pending]).await?;
    er.submit_and_confirm(&actor, &[flexi::add(actor.pubkey(), action)])
        .await?;
    let protected = [pending, magic, counter, trigger.pubkey()];
    let mut images = local(&er, &protected).await?;
    check!(
        images.iter().all(Option::is_some),
        "every protected image is present"
    )?;
    check_eq!(
        images[1].as_ref().unwrap().owner,
        redshift_interface::id(),
        "local Magic owner"
    )?;
    count(&er, counter, u64::from(action) * 2, 2).await?;
    if reverse {
        magic_update.release();
        old.release();
    } else {
        old.release();
        magic_update.release();
    }
    holds(&er, &protected, &images, counter, action).await?;
    let held = proxies.intercept(mirrored(&mirror));
    write(base, &funder, value + 1, &[mirror]).await?;
    let held = held.wait(TIMEOUT).await?;
    write(base, &funder, value + 2, &[mirror]).await?;
    marker(&er, mirror, value + 2).await?;
    held.release();
    marker(&er, mirror, value + 2).await?;
    if index == 1 {
        let timing = private
            .restart(RestartConfig {
                reset: false,
                hard_kill: false,
                ready_timeout: TIMEOUT,
            })
            .await?;
        check_eq!(timing.exit_code, Some(0), "supported graceful restart")?;
        unchanged(&er, &protected, &images).await?;
    }
    let settlement = proxies.stall(
        Selector::method("sendTransaction")
            .request()
            .account(&pending),
    );
    let signature = er
        .submit_and_confirm(
            &funder,
            &[build::commit_and_undelegate_accounts(
                1,
                funder.pubkey(),
                &[pending],
            )],
        )
        .await?;
    check::poll("undelegation submission held", TIMEOUT, || async {
        proxies.events().iter().any(|e| {
            e.action == Action::Held
                && e.operation
                    .as_ref()
                    .is_some_and(|op| op.method == "sendTransaction")
        })
    })
    .await?;
    let mut pending_image = images[0].clone().ok_or("pending image missing")?;
    pending_image.owner = dlp::dlp_id();
    check_eq!(
        local(&er, &[pending]).await?,
        [Some(pending_image.clone())],
        "intentional Undelegating transition"
    )?;
    images[0] = Some(pending_image);
    let base_before = base.accounts(&protected).await?;
    for target in if reverse {
        [magic, pending]
    } else {
        [pending, magic]
    } {
        base_poke(base, &proxies, &funder, target, poke)
            .await?
            .release();
    }
    let record = dlp::delegation_record_pda(&pending);
    let response = proxies.intercept(record_fetch(&record));
    let (read, fault) = join(
        async {
            let started = std::time::Instant::now();
            let result = er.account(&pending).await;
            (started.elapsed(), result)
        },
        async {
            let held = response.wait(TIMEOUT).await?;
            if index == 1 {
                tokio::time::sleep(Duration::from_secs(6)).await;
                held.release();
            } else if index == 2 {
                missing_record(held, record)?;
            } else {
                held.release();
            }
            Result::Ok(())
        },
    )
    .await;
    fault?;
    if index == 1 {
        check!(
            read.0 >= Duration::from_secs(5),
            "withheld companion reaches the transient refresh deadline"
        )?;
    }
    if let Ok(Some(account)) = read.1 {
        check_eq!(
            Some(account),
            images[0],
            "pending read cannot install a stale image"
        )?;
    }
    pressure(base, &er, &funder, &pool).await?;
    unchanged(&er, &protected, &images).await?;
    let duplicate =
        base_poke(base, &proxies, &funder, trigger.pubkey(), poke * 2).await?;
    replay(duplicate, &replayed)?;
    holds(&er, &protected, &images, counter, action).await?;
    let mut expected_base = base_before;
    for i in [0, 1] {
        expected_base[i].as_mut().unwrap().lamports += poke;
    }
    expected_base[3].as_mut().unwrap().lamports += poke * 2;
    check_eq!(
        base.accounts(&protected).await?,
        expected_base,
        "only explicit base pokes change base state"
    )?;
    settlement.remove();
    settled(base, &er, &signature, &[pending]).await?;
    let on_base = base
        .account(&pending)
        .await?
        .ok_or("settled account missing")?;
    check_eq!(on_base.owner, program::id(), "rightful ownership restored")?;
    check_eq!(
        &on_base.data,
        &images[0].as_ref().unwrap().data,
        "settlement preserves local marker"
    )?;
    proxies.restore();
    er.accounts(&[pending, mirror]).await?;
    write(base, &funder, value + 3, &[pending, mirror]).await?;
    er.account(&mirror).await?;
    marker(&er, pending, value + 3).await?;
    marker(&er, mirror, value + 3).await?;
    base.submit_and_confirm(
        &funder,
        &[build::delegate(
            funder.pubkey(),
            pending,
            funder.pubkey(),
            0,
            er.identity(),
        )],
    )
    .await?;
    er.account(&pending).await?;
    write(&er, &payer, value + 4, &[pending]).await?;
    marker(&er, pending, value + 4).await?;
    private.finish().await?;
    proxies.finish()
}

fn amount(account: Option<&Account>) -> Result<u64> {
    let data = &account.ok_or("local token account missing")?.data;
    Ok(spl::token_amount(data).ok_or("token amount missing")?)
}

async fn projected(
    er: &ErCtx,
    atas: &[Pubkey; 2],
    expected: [u64; 2],
) -> Result<()> {
    check::poll_for("local token projection", TIMEOUT, || async {
        let accounts = local_in(er, &[spl::token_program()], atas).await?;
        check_eq!(
            [amount(accounts[0].as_ref())?, amount(accounts[1].as_ref())?],
            expected,
            "projected token amounts"
        )?;
        Result::Ok(())
    })
    .await?;
    Ok(())
}

async fn generation(
    base: &BaseCtx,
    eata: Pubkey,
) -> Result<dlp_api::state::DelegationRecord> {
    let account = base
        .account(&dlp::delegation_record_pda(&eata))
        .await?
        .ok_or("delegation record missing")?;
    Ok(
        *dlp_api::state::DelegationRecord::try_from_bytes_with_discriminator(
            &account.data,
        )?,
    )
}

async fn tokens(
    base: &BaseCtx,
    label: &str,
    index: u64,
    case: Case,
) -> Result<()> {
    let Case {
        value,
        poke: delta,
        reverse,
        keys: [funder, owner, spender, _, mint, destination],
        pool,
        ..
    } = case;
    let (proxies, private) = boot(base, label).await?;
    let er = private.ctx().clone();
    let owner = Rc::new(owner);
    let validator = topology::identity_for_label(label)?;
    let owners = [owner.pubkey(), destination.pubkey()];
    let atas = owners.map(|owner| spl::derive_ata(&owner, &mint.pubkey()));
    let eatas = owners.map(|owner| spl::derive_eata(&owner, &mint.pubkey()));
    let vault = spl::derive_ata(
        &spl::derive_global_vault(&mint.pubkey()),
        &mint.pubkey(),
    );
    let wallets = [owner.as_ref(), &destination];
    let order = if reverse { [1, 0] } else { [0, 1] };
    let claims = [value % 31 + 40, value % 31 + 41];
    let physical = value + 500;
    for key in [&funder, owner.as_ref(), &destination, &spender] {
        base.airdrop(&key.pubkey(), crate::PAYER_LAMPORTS).await?;
    }
    base.submit_and_confirm_with(
        &funder,
        &[&mint],
        &[
            spl::allocate_mint(&funder.pubkey(), &mint.pubkey()),
            spl::initialize_mint(&mint.pubkey(), &funder.pubkey()),
            spl::initialize_global_vault(&funder.pubkey(), &mint.pubkey()),
        ],
    )
    .await?;
    for i in order {
        let wallet = wallets[i];
        base.submit_and_confirm(
            &funder,
            &[
                spl::create_ata_idempotent(
                    &funder.pubkey(),
                    &wallet.pubkey(),
                    &mint.pubkey(),
                ),
                spl::mint_to(
                    &mint.pubkey(),
                    &atas[i],
                    &funder.pubkey(),
                    physical + claims[i],
                ),
                spl::initialize_eata(
                    &funder.pubkey(),
                    &wallet.pubkey(),
                    &mint.pubkey(),
                ),
            ],
        )
        .await?;
        base.submit_and_confirm(
            wallet,
            &[spl::deposit_spl_tokens(
                &wallet.pubkey(),
                &mint.pubkey(),
                claims[i],
            )],
        )
        .await?;
    }
    base.submit_and_confirm(
        &owner,
        &[spl::approve(
            &atas[0],
            &spender.pubkey(),
            &owner.pubkey(),
            delta,
        )],
    )
    .await?;
    er.accounts(&atas).await?;
    er.account(&spender.pubkey()).await?;
    projected(&er, &atas, [physical, physical]).await?;
    let revoked = proxies.intercept(mirrored(&atas[0]));
    base.submit_and_confirm(&owner, &[spl::revoke(&atas[0], &owner.pubkey())])
        .await?;
    let revoked = revoked.wait(TIMEOUT).await?;
    delegate_eata(base, &funder, &owners[1], &mint.pubkey(), &er.identity())
        .await?;
    projected(&er, &atas, [physical, claims[1]]).await?;
    let stale_layout = local(&er, &atas).await?;
    let latest_layout =
        base.account(&atas[0]).await?.ok_or("base ATA missing")?;
    check!(
        stale_layout[0].as_ref().unwrap().data[72..108]
            != latest_layout.data[72..108],
        "stale approval fixture differs from revoked base approval"
    )?;
    let record = dlp::delegation_record_pda(&eatas[0]);
    let response = proxies.intercept(record_fetch(&record));
    delegate_eata(base, &funder, &owners[0], &mint.pubkey(), &er.identity())
        .await?;
    let held = response.wait(TIMEOUT).await?;
    if index == 1 {
        let denied =
            proxies.reject(record_fetch(&record), "seeded record unavailable");
        missing_record(held, record)?;
        let refused = er
            .submit_and_confirm(
                &owner,
                &[spl::transfer(&atas[0], &atas[1], &owners[0], delta + 1)],
            )
            .await;
        check!(
            refused.is_err(),
            "missing record cannot authorize a token transfer"
        )?;
        let error = refused.unwrap_err().to_string();
        check!(
            error.contains("InvalidWritableAccount")
                || error.contains("Immutable")
                || error.contains("Failed to resolve"),
            "record fault refusal: {error}"
        )?;
        unchanged(&er, &atas, &stale_layout).await?;
        er.submit_and_confirm(
            &destination,
            &[spl::transfer(&atas[1], &atas[1], &owners[1], 1)],
        )
        .await?;
        denied.remove();
    } else if index == 2 {
        let stalled = proxies.stall(record_fetch(&record));
        let (read, fault) = join(er.account(&eatas[0]), async {
            tokio::time::sleep(Duration::from_secs(6)).await;
            unchanged(&er, &atas, &stale_layout).await?;
            held.release();
            stalled.remove();
            Result::Ok(())
        })
        .await;
        fault?;
        if let Ok(Some(account)) = read {
            check_eq!(
                amount(Some(&account))?,
                claims[0],
                "held lookup preserves token backing"
            )?;
        }
    } else {
        held.release();
    }
    base.submit_and_confirm(
        &funder,
        &[system::transfer(&funder.pubkey(), &eatas[0], delta)],
    )
    .await?;
    er.accounts(&atas).await?;
    projected(&er, &atas, claims).await?;
    let initial_generations = [
        generation(base, eatas[0]).await?,
        generation(base, eatas[1]).await?,
    ];
    let spender = Rc::new(spender);
    let signature = er
        .sender(spender.clone())
        .submit(&[spl::transfer(&atas[0], &atas[1], &spender.pubkey(), delta)])
        .await?;
    let rejected = er.api().await_transaction(&signature, TIMEOUT).await?;
    let revoked_signature = signature;
    let revoked_transfer = rejected.err.is_none();
    check!(
        revoked_transfer
            || rejected.err
                == Some(json::json!({"InstructionError": [0, {"Custom": 4}]})),
        "revoked approval probe reached SPL authority check: {rejected:?}"
    )?;
    let claims = if revoked_transfer {
        [claims[0] - delta, claims[1] + delta]
    } else {
        claims
    };
    projected(&er, &atas, claims).await?;
    if revoked_transfer {
        tracing::error!(%signature, delta, "#237: revoked base approval authorized a projected transfer");
    }
    er.submit_and_confirm(
        &owner,
        &[spl::transfer(&atas[0], &atas[1], &owners[0], delta)],
    )
    .await?;
    let balances = [claims[0] - delta, claims[1] + delta];
    projected(&er, &atas, balances).await?;
    let protected = local(&er, &atas).await?;
    let base_keys = [
        atas[0],
        atas[1],
        eatas[0],
        eatas[1],
        vault,
        dlp::delegation_record_pda(&eatas[0]),
        dlp::delegation_record_pda(&eatas[1]),
    ];
    let mut base_images = base.accounts(&base_keys).await?;
    base_images[2].as_mut().ok_or("base eATA missing")?.lamports += delta * 5;
    revoked.release();
    let stale = base_poke(base, &proxies, &funder, eatas[0], delta * 2).await?;
    let old = stale.body.clone();
    stale.release();
    let duplicate =
        base_poke(base, &proxies, &funder, eatas[0], delta * 3).await?;
    replay(duplicate, &old)?;
    pressure(base, &er, &funder, &pool).await?;
    unchanged(&er, &atas, &protected).await?;
    check_eq!(
        base.accounts(&base_keys).await?,
        base_images,
        "stale delivery preserves full base token images"
    )?;
    for i in 0..2 {
        check_eq!(
            generation(base, eatas[i]).await?,
            initial_generations[i],
            "stale delivery cannot change the delegation generation"
        )?;
    }
    check_eq!(
        spl::token_balance(base, &vault).await?,
        Some(claims.iter().sum()),
        "vault backs all local claims"
    )?;
    let base_total = spl::token_balance(base, &atas[0])
        .await?
        .ok_or("base balance missing")?
        + spl::token_balance(base, &atas[1])
            .await?
            .ok_or("base balance missing")?;
    check_eq!(
        base_total + balances.iter().sum::<u64>(),
        physical * 2 + claims.iter().sum::<u64>(),
        "token supply conserved"
    )?;
    let signature = er
        .submit_and_confirm(
            &validator,
            &[build::commit_accounts(1, er.identity(), &atas)],
        )
        .await?;
    settled(base, &er, &signature, &eatas).await?;
    backs(base, &eatas, balances, "settled eATA backs its projection").await?;
    proxies.restore();
    let foreign_label = format!("{label}-foreign");
    let foreign_identity = topology::identity_for_label(&foreign_label)?;
    check!(
        foreign_identity.pubkey() != er.identity(),
        "distinct foreign validator"
    )?;
    for i in order {
        let wallet = wallets[i];
        let signature = er
            .submit_and_confirm(
                wallet,
                &[sdk::spl::builders::UndelegateEphemeralAtaBuilder {
                    payer: wallet.pubkey(),
                    user: wallet.pubkey(),
                    mint: mint.pubkey(),
                }
                .instruction()],
            )
            .await?;
        settled(base, &er, &signature, &eatas[i..i + 1]).await?;
        check_eq!(
            base.account(&eatas[i])
                .await?
                .ok_or("undelegated eATA missing")?
                .owner,
            spl::eata_program(),
            "rightful eATA ownership restored"
        )?;
        delegate_eata(
            base,
            wallet,
            &wallet.pubkey(),
            &mint.pubkey(),
            &foreign_identity.pubkey(),
        )
        .await?;
        let next = generation(base, eatas[i]).await?;
        check!(
            next.delegation_slot > initial_generations[i].delegation_slot,
            "handoff advances delegation generation"
        )?;
        check_eq!(
            next.authority.to_bytes(),
            foreign_identity.pubkey().to_bytes(),
            "foreign generation authority"
        )?;
    }
    er.accounts(&atas).await?;
    projected(&er, &atas, [physical, physical]).await?;
    let before = local(&er, &atas).await?;
    refused(
        &er,
        owner.clone(),
        &[spl::transfer(&atas[0], &atas[1], &owners[0], 1)],
        json::json!({"InstructionError": [0, "Immutable"]}),
        "foreign delegation cannot grant local write authority",
    )
    .await?;
    unchanged(&er, &atas, &before).await?;
    let base_before = base.accounts(&eatas).await?;
    refused(
        &er,
        Rc::new(validator),
        &[build::commit_accounts(3, er.identity(), &atas)],
        json::json!({"InstructionError": [0, "IllegalOwner"]}),
        "foreign delegation cannot grant settlement authority",
    )
    .await?;
    unchanged(&er, &atas, &before).await?;
    check_eq!(
        base.accounts(&eatas).await?,
        base_before,
        "refused foreign settlement preserves base images"
    )?;
    let (foreign_proxy, foreign) = boot(base, &foreign_label).await?;
    let other = foreign.ctx();
    other.accounts(&atas).await?;
    projected(other, &atas, balances).await?;
    other
        .submit_and_confirm(
            &owner,
            &[spl::transfer(&atas[0], &atas[1], &owners[0], 1)],
        )
        .await?;
    projected(other, &atas, [balances[0] - 1, balances[1] + 1]).await?;
    let signature = other
        .submit_and_confirm(
            &foreign_identity,
            &[build::commit_accounts(2, other.identity(), &atas)],
        )
        .await?;
    settled(base, other, &signature, &eatas).await?;
    backs(
        base,
        &eatas,
        [balances[0] - 1, balances[1] + 1],
        "rightful foreign generation settles",
    )
    .await?;
    foreign.finish().await?;
    foreign_proxy.finish()?;
    private.finish().await?;
    proxies.finish()?;
    check!(!revoked_transfer, "#237: revoked base approval authorized a projected transfer: signature={revoked_signature}, delta={delta}")?;
    Ok(())
}
