use std::{cell::Cell, fs, rc::Rc, slice, time::Duration};

use account::Account;
use futures_util::future::{try_join, try_join_all};
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    api::{custom_error_code, RpcError},
    catalog::Fixture,
    check, check_eq,
    netfault::{Action, BaseProxies, Selector, Stage},
    redline::Accounts,
    topology,
    topology::{ErOptions, PrivateEr},
    BaseCtx, ChainCtx, ErCtx, PrivateErScenario, Result,
};
use signer::Signer;
use solana_loader_v3_interface::{
    get_program_data_address, instruction as loader, state::UpgradeableLoaderState,
};
use tokio::time;

use crate::{program::layout, PAYER_LAMPORTS};

const TIMEOUT: Duration = Duration::from_secs(90);
const FETCH_FAILURE: &str = "upgrade probe cold fetch unavailable";
const PHASES: [&str; 4] = ["before upgrade", "held", "released", "converged"];

async fn buffer(base: &BaseCtx, payer: &Keypair, bytes: &[u8]) -> Result<Keypair> {
    let buffer = Keypair::new();
    base.submit_and_confirm_with(
        payer,
        &[&buffer],
        &loader::create_buffer(
            &payer.pubkey(),
            &buffer.pubkey(),
            &payer.pubkey(),
            1_000_000_000,
            bytes.len(),
        )?,
    )
    .await?;
    try_join_all(bytes.chunks(800).enumerate().map(|(index, chunk)| {
        let ix = loader::write(
            &buffer.pubkey(),
            &payer.pubkey(),
            (index * 800) as u32,
            chunk.to_vec(),
        );
        async move { base.submit_and_confirm(payer, &[ix]).await }
    }))
    .await?;
    Ok(buffer)
}

async fn local_program(er: &ErCtx, program: Pubkey) -> Result<Option<Account>> {
    Ok(er
        .api()
        .get_program_accounts(&sdk_ids::loader_v4::ID)
        .await?
        .into_iter()
        .find_map(|(key, account)| (key == program).then_some(account)))
}

pub struct ProgramUpgrade {
    base: Rc<BaseCtx>,
    a: Vec<u8>,
    b: Vec<u8>,
    funder: Keypair,
    authority: Pubkey,
    b_buffer: Keypair,
    program_id: Pubkey,
    programdata: Pubkey,
    proxies: BaseProxies,
    hot: PrivateEr,
    cold: PrivateEr,
}

impl PrivateErScenario for ProgramUpgrade {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        let mut a = fs::read(Fixture::UpgradeProbe.resolve()?)?;
        let mut b = fs::read(Fixture::UpgradeProbeUpgraded.resolve()?)?;
        let size = a.len().max(b.len());
        a.resize(size, 0);
        b.resize(size, 0);
        check!(a != b, "fixture versions differ")?;
        let funder = base.funded_payer(10_000_000_000).await?;
        let authority = funder.pubkey();
        let (a_buffer, b_buffer) =
            try_join(buffer(&base, &funder, &a), buffer(&base, &funder, &b)).await?;
        let program = Keypair::new();
        let program_id = program.pubkey();
        let programdata = get_program_data_address(&program_id);
        base.submit_and_confirm_with(
            &funder,
            &[&program],
            &loader::deploy_with_max_program_len(
                &authority,
                &program_id,
                &a_buffer.pubkey(),
                &authority,
                2_000_000,
                size,
            )?,
        )
        .await?;
        let proxies = BaseProxies::spawn(&base).await?;
        let options = |label: &str| ErOptions {
            label: label.into(),
            base_endpoints: Some(proxies.endpoints()),
            request_timeout: Some(TIMEOUT),
            ..ErOptions::default()
        };
        let (hot, cold) = try_join(
            topology::private_er(&base, options("upgrade-hot")),
            topology::private_er(&base, options("upgrade-cold")),
        )
        .await?;
        Ok(Self {
            base,
            a,
            b,
            funder,
            authority,
            b_buffer,
            program_id,
            programdata,
            proxies,
            hot,
            cold,
        })
    }

    async fn run(self) -> Result<()> {
        let Self {
            base,
            a,
            b,
            funder,
            authority,
            b_buffer,
            program_id,
            programdata,
            proxies,
            hot,
            cold,
        } = self;
        let er = hot.ctx();
        let payer = base.delegated_payer(&funder, er.identity(), PAYER_LAMPORTS).await?;
        let keys = Accounts::new(
            (layout::DATA_OFFSET + layout::ID_SIZE) as u32,
            er.identity(),
        )
        .init_batched(base.as_ref(), slice::from_ref(&funder), 2, true)
        .await?;
        let base_state = base.accounts(&keys).await?;
        let state = async || -> Result<Vec<Account>> {
            er.accounts(&keys)
                .await?
                .into_iter()
                .collect::<Option<_>>()
                .ok_or_else(|| "state account missing".into())
        };
        let mut expected = state().await?;
        check!(
            local_program(cold.ctx(), program_id).await?.is_none(),
            "cold program absent"
        )?;
        let sender = er.sender(Rc::new(payer));
        let phase = Cell::new(0);
        let counts = Cell::new([[0usize; 4]; 4]);
        let done = Cell::new(false);
        let progressed = async |phase: usize, count: usize| {
            check::poll("invocations progress", TIMEOUT, || async {
                counts.get()[phase].iter().sum::<usize>() >= count
            })
            .await
        };
        let held = async |stage, key: Pubkey| {
            check::poll("observation held", TIMEOUT, || async {
                proxies.events().iter().any(|event| {
                    event.action == Action::Held
                        && event.operation.as_ref().is_some_and(|op| {
                            op.stage == stage && op.accounts.contains(&key.to_string())
                        })
                })
            })
            .await
        };
        let materialized = async |er: &ErCtx, bytes: &[u8]| -> Result<()> {
            check!(
                local_program(er, program_id)
                    .await?
                    .is_some_and(|account| { account.executable && account.data == bytes }),
                "program {program_id} contains expected bytes"
            )?;
            Ok(())
        };
        let traffic = async {
            let mut loading_failure = Some(
                proxies.reject(
                    Selector::methods(&["getAccountInfo", "getMultipleAccounts"])
                        .http()
                        .response()
                        .account(&program_id),
                    FETCH_FAILURE,
                ),
            );
            let mut converged = false;
            let mut accounts: Vec<_> =
                keys.iter().map(|key| AccountMeta::new(*key, false)).collect();
            accounts.push(AccountMeta::new_readonly(crate::program::id(), false));
            let mut probe = Instruction {
                program_id,
                accounts,
                data: Vec::new(),
            };
            let mut id = 0u64;
            while !done.get() {
                id += 1;
                let fail = id.is_multiple_of(4);
                probe.data = upgrade_probe::instruction_data(id, fail).to_vec();
                let tx = sender.prepare(slice::from_ref(&probe)).await?;
                let signature = tx.signatures[0];
                let current = phase.get();
                let mut tally = counts.get();
                let injecting = loading_failure.is_some();
                match sender.submit_prepared(&tx).await {
                    Err(error) => {
                        let rpc = error.downcast_ref::<RpcError>().ok_or(error.to_string())?;
                        check!(
                            rpc.code == -32003
                                && if injecting {
                                    rpc.message.contains(FETCH_FAILURE)
                                } else {
                                    matches!(current, 1 | 2)
                                },
                            "loading failure: {rpc}"
                        )?;
                        tally[current][3] += 1;
                    }
                    Ok(_) => {
                        check!(!injecting, "injected cold fetch must fail")?;
                        let outcome = er.api().await_transaction(&signature, TIMEOUT).await?;
                        let versions: Vec<_> = outcome
                            .logs
                            .iter()
                            .map(String::as_str)
                            .filter(|log| log.starts_with("Program log: Upgrade: "))
                            .collect();
                        let version = match versions.as_slice() {
                            ["Program log: Upgrade: 1"] => Some(1),
                            ["Program log: Upgrade: 2"] => Some(2),
                            [] => None,
                            _ => {
                                return Err(
                                    format!("invalid version {signature}: {versions:?}").into()
                                );
                            }
                        };
                        if let Some(version) = version {
                            check!(
                                matches!((current, version), (0, 1) | (1 | 2, 1 | 2) | (3, 2)),
                                "version {version} during {} for {signature}",
                                PHASES[current]
                            )?;
                            if let Some(error) = &outcome.err {
                                check!(fail, "execution failure for {signature}: {error}")?;
                                check_eq!(
                                    custom_error_code(error),
                                    Some(upgrade_probe::FAILURE_CODE),
                                    "failure after writes for {signature}"
                                )?;
                                tally[current][2] += 1;
                            } else {
                                check!(!fail, "failure probe {signature} must fail")?;
                                let value = id * 10 + version;
                                for (account, value) in expected.iter_mut().zip([value, !value]) {
                                    account.data[layout::DATA_OFFSET..]
                                        .copy_from_slice(&value.to_le_bytes());
                                }
                                tally[current][version as usize - 1] += 1;
                            }
                        } else {
                            check!(
                                outcome.err.is_some() && matches!(current, 1 | 2),
                                "missing logs for {signature}: {:?}",
                                outcome.err
                            )?;
                            tally[current][3] += 1;
                        }
                        converged |= current == 3;
                    }
                }
                check_eq!(
                    state().await?,
                    expected,
                    "complete effects or rollback for {signature}"
                )?;
                if let Some(fault) = loading_failure.take() {
                    fault.remove();
                }
                counts.set(tally);
            }
            check!(converged, "missing convergence transaction")?;
            Result::Ok(())
        };
        let upgrade = async {
            progressed(0, 4).await?;
            materialized(er, &a).await?;
            let holds = [program_id, programdata].map(|key| {
                proxies.stall(
                    Selector::methods(&[
                        "getAccountInfo",
                        "getMultipleAccounts",
                        "accountNotification",
                        "programNotification",
                    ])
                    .response()
                    .notification()
                    .account(&key),
                )
            });
            phase.set(1);
            base.submit_and_confirm(
                &funder,
                &[loader::upgrade(&program_id, &b_buffer.pubkey(), &authority, &authority)],
            )
            .await?;
            let data =
                base.account(&programdata).await?.ok_or("missing upgraded programdata")?.data;
            let offset = UpgradeableLoaderState::size_of_programdata_metadata();
            check!(
                data.get(offset..) == Some(b.as_slice()),
                "base installed B bytes"
            )?;
            held(Stage::Notification, programdata).await?;
            let after_upgrade = counts.get()[1].iter().sum::<usize>();
            let (clones, ()) = try_join(
                try_join_all((0..3).map(|_| cold.ctx().account(&program_id))),
                async {
                    held(Stage::Response, program_id).await?;
                    progressed(1, after_upgrade + 8).await?;
                    for hold in holds {
                        hold.remove();
                    }
                    phase.set(2);
                    Result::Ok(())
                },
            )
            .await?;
            check!(
                clones.iter().all(|a| a.as_ref().is_some_and(|a| a.executable
                    && a.owner == sdk_ids::loader_v4::ID
                    && a.data == b)),
                "cold clones contain B"
            )?;
            check::poll_for("fresh B materialization", TIMEOUT, || materialized(er, &b)).await?;
            phase.set(3);
            progressed(3, 12).await?;
            done.set(true);
            Result::Ok(())
        };
        time::timeout(Duration::from_secs(180), try_join(traffic, upgrade)).await??;
        check_eq!(
            base.accounts(&keys).await?,
            base_state,
            "ER writes stay off base"
        )?;
        try_join(hot.finish(), cold.finish()).await?;
        proxies.finish()?;
        Ok(())
    }
}
