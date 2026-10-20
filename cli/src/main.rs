mod args;
mod catalog;

use std::{
    future::Future,
    process::{Command, ExitCode},
    rc::Rc,
    time::Instant,
};

use args::{Cli, Run};
use clap::Parser;
use futures_util::{
    future::{join, join_all},
    StreamExt,
};
use redsuite_core::{
    catalog::{Er, ErGroup, Family, ScenarioEntry},
    logging,
    netfault::BaseProxies,
    profile::{ExecutionConfig, LoopMode},
    report::{self, ScenarioReport, Unit},
    topology::{self, PrivateEr, ER_READY_TIMEOUT},
    BaseCtx, ErCtx, Result, RunRecord,
};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

const NO_GROUP: &str = "the catalog entry declares no ER group";

struct GroupEr {
    group: &'static ErGroup,
    er: Option<Rc<Mutex<PrivateEr>>>,
    proxies: Option<Rc<BaseProxies>>,
    failure: Option<String>,
    tainted: bool,
}

impl GroupEr {
    fn new(group: &'static ErGroup) -> Self {
        Self {
            group,
            er: None,
            proxies: None,
            failure: None,
            tainted: false,
        }
    }

    fn owns(&self, entry: &ScenarioEntry) -> bool {
        entry
            .er
            .group()
            .is_some_and(|group| group.label == self.group.label)
    }

    fn note(&mut self, records: &[RunRecord]) {
        self.tainted |= records.iter().any(|record| !record.passed());
    }

    async fn run(
        &mut self,
        entry: &ScenarioEntry,
        base: &Result<BaseCtx>,
        config: ExecutionConfig,
    ) -> RunRecord {
        let er = self.provide(base).await;
        let record = (entry.run)(config, er).await;
        self.note(std::slice::from_ref(&record));
        record
    }

    async fn provide(
        &mut self,
        base: &Result<BaseCtx>,
    ) -> std::result::Result<ErCtx, String> {
        let label = self.group.label;
        if let Some(er) = &self.er {
            let reason = if self.tainted {
                "a scenario failed on it"
            } else {
                let er = er.lock().await;
                if er.healthy().await {
                    return Ok(er.ctx().clone());
                }
                "it is no longer healthy"
            };
            warn!(label, reason, "replacing group ER");
            self.finish().await;
        } else if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        self.tainted = false;
        let booted = match base {
            Ok(base) => self.boot(base).await,
            Err(error) => Err(error.to_string().into()),
        };
        booted.map_err(|error| {
            let failure = format!("group ER `{label}` did not boot: {error}");
            self.failure = Some(failure.clone());
            failure
        })
    }

    async fn boot(&mut self, base: &BaseCtx) -> Result<ErCtx> {
        let mut options = self.group.options();
        let proxies = if self.group.proxied {
            let proxies = BaseProxies::spawn(base).await?;
            options.base_endpoints = Some(proxies.endpoints());
            Some(Rc::new(proxies))
        } else {
            None
        };
        let mut er = topology::private_er(base, options).await?;
        if let Some(proxies) = &proxies {
            er = er.with_proxies(proxies.clone());
        }
        er.wait_ready(ER_READY_TIMEOUT).await?;
        let er = er.shared();
        let ctx = er.lock().await.ctx().clone();
        self.er = Some(er);
        self.proxies = proxies;
        Ok(ctx)
    }

    async fn finish(&mut self) {
        let er = self
            .er
            .take()
            .and_then(|er| Rc::try_unwrap(er).ok())
            .map(Mutex::into_inner);
        if let Some(er) = er {
            if let Err(error) = er.finish().await {
                error!(self.group.label, %error, "group ER cleanup failed");
            }
        }
        let proxies = self
            .proxies
            .take()
            .and_then(|proxies| Rc::try_unwrap(proxies).ok());
        if let Some(Err(error)) = proxies.map(BaseProxies::finish) {
            error!(self.group.label, %error, "group ER proxy cleanup failed");
        }
    }
}

type Lane = (&'static str, f64, Vec<RunRecord>);

async fn timed(
    lane: &'static str,
    run: impl Future<Output = Vec<RunRecord>>,
) -> Lane {
    let started = Instant::now();
    let records = run.await;
    (lane, started.elapsed().as_secs_f64(), records)
}

async fn run_lane(
    scenarios: Vec<&'static ScenarioEntry>,
    limit: usize,
    config: ExecutionConfig,
    er: std::result::Result<ErCtx, String>,
) -> Vec<RunRecord> {
    futures_util::stream::iter(scenarios)
        .map(|entry| (entry.run)(config, er.clone()))
        .buffer_unordered(limit.max(1))
        .collect()
        .await
}

async fn run_group(
    group: &mut GroupEr,
    members: Vec<&'static ScenarioEntry>,
    base: &Result<BaseCtx>,
    config: ExecutionConfig,
) -> Vec<RunRecord> {
    let (shared, exclusive): (Vec<_>, Vec<_>) = members
        .into_iter()
        .partition(|entry| matches!(entry.er, Er::Shared(_)));
    let mut records = Vec::new();
    if !shared.is_empty() {
        let er = group.provide(base).await;
        let limit = shared.len();
        let mut done = run_lane(shared, limit, config, er).await;
        group.note(&done);
        records.append(&mut done);
    }
    for entry in exclusive {
        records.push(group.run(entry, base, config).await);
    }
    records
}

async fn run(
    family: Family,
    args: Run,
    config: ExecutionConfig,
    serial: bool,
    fresh_concurrency: usize,
) -> Result<()> {
    let suite_started = Instant::now();
    let selections = args.selections;
    let selection = if selections.is_empty() {
        family.prefix().to_owned()
    } else {
        selections.join(" ")
    };
    let scenarios = catalog::select(family, &selections)?;
    let redline = family == Family::Redline;

    let mut report = redline.then(|| {
        let mut names: Vec<_> =
            scenarios.iter().map(|entry| entry.name()).collect();
        names.sort();
        let hostname = std::env::var("HOSTNAME")
            .or_else(|_| std::fs::read_to_string("/proc/sys/kernel/hostname"))
            .ok()
            .or_else(|| {
                let output = Command::new("hostname").output().ok()?;
                output.status.success().then(|| {
                    String::from_utf8_lossy(&output.stdout).into_owned()
                })
            })
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| format!("unknown-{}", report::run_id()));
        ScenarioReport::ok(&format!("suite/{selection}"))
            .setting("loop", config.loop_mode.name())
            .setting(
                "host",
                format!(
                    "{} {} {} {} cpus",
                    hostname.trim(),
                    std::env::consts::OS,
                    std::env::consts::ARCH,
                    std::thread::available_parallelism().map_or(0, |n| n.get())
                ),
            )
            .setting("serial", serial)
            .setting("cleanup", args.cleanup)
            .setting("scenarios", names.join(","))
            .setting("redline profile", config.profile.name())
    });

    info!(selection, "suite started");
    let base = topology::base_only(config).await;
    let mut groups: Vec<GroupEr> = Vec::new();
    for group in scenarios.iter().filter_map(|entry| entry.er.group()) {
        if !groups.iter().any(|known| known.group.label == group.label) {
            groups.push(GroupEr::new(group));
        }
    }
    let sequential = serial || redline;
    let lanes = if sequential {
        let run = async {
            let mut records = Vec::new();
            let mut active: Option<usize> = None;
            for entry in scenarios {
                let next = groups.iter().position(|group| group.owns(entry));
                if active != next {
                    if let Some(index) = active {
                        groups[index].finish().await;
                    }
                    active = next;
                }
                let record = match active {
                    Some(index) => {
                        groups[index].run(entry, &base, config).await
                    }
                    None => (entry.run)(config, Err(NO_GROUP.to_owned())).await,
                };
                records.push(record);
            }
            if let Some(index) = active {
                groups[index].finish().await;
            }
            records
        };
        let lane = timed("serial", run).await;
        vec![lane]
    } else {
        let (grouped, fresh): (Vec<_>, Vec<_>) = scenarios
            .into_iter()
            .partition(|entry| entry.er.group().is_some());
        let group_lanes = join_all(groups.iter_mut().map(|group| {
            let mine = grouped
                .iter()
                .copied()
                .filter(|entry| group.owns(entry))
                .collect();
            let base = &base;
            async move {
                let lane = timed(
                    group.group.label,
                    run_group(group, mine, base, config),
                )
                .await;
                group.finish().await;
                lane
            }
        }));
        let fresh_lane = timed(
            "fresh",
            run_lane(
                fresh,
                fresh_concurrency,
                config,
                Err(NO_GROUP.to_owned()),
            ),
        );
        let (mut lanes, fresh_lane) = join(group_lanes, fresh_lane).await;
        lanes.push(fresh_lane);
        lanes
    };

    let mut records = Vec::new();
    for (lane, seconds, mut runs) in lanes {
        if !runs.is_empty() {
            report = report.map(|report| {
                report.metric(format!("{lane} seconds"), Unit::Seconds, seconds)
            });
        }
        records.append(&mut runs);
    }
    let reclaim_errors: Vec<String> = base
        .iter()
        .flat_map(|base| base.reclaim(&format!("suite/{selection}")))
        .map(|error| error.to_string())
        .collect();
    let outcome = summarize(&records).and(if reclaim_errors.is_empty() {
        Ok(())
    } else {
        Err(reclaim_errors.join("; ").into())
    });
    let cleanup = if args.cleanup {
        topology::down()
    } else {
        Ok(())
    };
    if let Err(error) = &cleanup {
        error!(%error, "suite cleanup failed");
    }
    let outcome = outcome.and(cleanup);
    let outcome = if let Some(report) = report {
        let report = report.metric(
            "wall seconds",
            Unit::Seconds,
            suite_started.elapsed().as_secs_f64(),
        );
        let persisted = report::persist_summary(report, &outcome);
        if let Err(error) = &persisted {
            error!(%error, "suite report failed");
        }
        outcome.and(persisted)
    } else {
        outcome
    };
    info!(
        selection,
        elapsed_secs = suite_started.elapsed().as_secs_f64(),
        passed = outcome.is_ok(),
        "suite finished",
    );
    outcome
}

fn summarize(records: &[RunRecord]) -> Result<()> {
    let total_scenarios = records.len();
    let failed = records.iter().filter(|record| !record.passed()).count();
    if failed != 0 {
        return Err(
            format!("{failed} of {total_scenarios} scenarios failed").into()
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(error) = logging::init() {
        eprintln!("cannot initialize logging: {error}");
        return ExitCode::FAILURE;
    }
    let outcome = tokio::task::LocalSet::new()
        .run_until(dispatch(cli.command))
        .await;

    match outcome {
        Ok(code) => code,
        Err(error) => {
            error!(%error, "command failed");
            ExitCode::FAILURE
        }
    }
}

fn list(family: Family, selections: &[String]) -> Result<()> {
    for entry in catalog::select(family, selections)? {
        println!("{}", entry.name());
    }
    Ok(())
}

async fn checks(family: Family, command: args::Checks) -> Result<()> {
    match command {
        args::Checks::List { selections } => list(family, &selections),
        args::Checks::Run(args) => {
            run(
                family,
                args.run,
                ExecutionConfig::default(),
                args.serial,
                args.fresh_concurrency.get(),
            )
            .await
        }
    }
}

async fn dispatch(command: args::Command) -> Result<ExitCode> {
    match command {
        args::Command::Redshift(command) => {
            checks(Family::Redshift, command).await?
        }
        args::Command::Redhat(command) => {
            checks(Family::Redhat, command).await?
        }
        args::Command::Redline(command) => match command {
            args::Benchmarks::List { selections } => {
                list(Family::Redline, &selections)?
            }
            args::Benchmarks::Run(args) => {
                let config = ExecutionConfig {
                    profile: args.profile,
                    loop_mode: args.loop_mode,
                };
                run(Family::Redline, args.run, config, true, 1).await?;
            }
            args::Benchmarks::Load(args) => return load(args).await,
        },
        args::Command::Shared(command) => command.run()?,
    }
    Ok(ExitCode::SUCCESS)
}

async fn load(args: args::Load) -> Result<ExitCode> {
    use redsuite_core::{catalog::Fixture, run_private_er_scenario};
    use tokio::{
        signal::unix::{signal, SignalKind},
        sync::watch,
    };

    let mut interrupts = signal(SignalKind::interrupt())?;
    let (stop, receiver) = watch::channel(false);
    let config = ExecutionConfig {
        profile: args.profile,
        loop_mode: LoopMode::Open,
    };
    let scenario = redline::load::Load {
        duration: args.duration,
        rate: args.rate,
        concurrency: args.concurrency,
        stop: receiver,
    };
    let work = run_private_er_scenario(
        scenario,
        &[Fixture::RedlineProgram],
        &[],
        config,
    );
    tokio::pin!(work);
    let record = tokio::select! {
        record = &mut work => record,
        Some(()) = interrupts.recv() => {
            stop.send_replace(true);
            info!("load interrupted; stopping admission and draining");
            // Finish the same future so admitted work and its report survive SIGINT.
            work.await
        }
    };
    let outcome = summarize(&[record]);
    let cleanup = if args.cleanup {
        topology::down()
    } else {
        Ok(())
    };
    if let Err(error) = &cleanup {
        error!(%error, "load cleanup failed");
    }
    if *stop.borrow() {
        if let Err(error) = outcome.and(cleanup) {
            error!(%error, "interrupted load failed");
        }
        return Ok(ExitCode::from(130));
    }
    outcome.and(cleanup)?;
    Ok(ExitCode::SUCCESS)
}
