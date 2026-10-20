mod catalog;

use std::{future::Future, process::Command, rc::Rc, time::Instant};

use futures_util::{future::join_all, StreamExt};
use redsuite_core::{
    catalog::{Er, ErGroup, Family, ScenarioEntry},
    frontend, logging,
    netfault::BaseProxies,
    profile::{ExecutionConfig, LoopMode, Profile},
    report::{self, ScenarioReport, Unit},
    topology::{self, PrivateEr, ER_READY_TIMEOUT},
    BaseCtx, ErCtx, Result, RunRecord,
};
use tokio::sync::Mutex;
use tracing::{error, warn};

const FAMILIES: &[&[ScenarioEntry]] = &[
    catalog::redline::SCENARIOS,
    catalog::redshift::SCENARIOS,
    catalog::redhat::SCENARIOS,
];
const FRESH_CONCURRENCY: usize = 2;
const NO_GROUP: &str = "the catalog entry declares no ER group";

fn entries() -> impl Iterator<Item = &'static ScenarioEntry> {
    FAMILIES.iter().flat_map(|scenarios| scenarios.iter())
}

const USAGE_HEAD: &str = "\
usage:
  redsuite list [selection]                               list scenarios (selection as for run)
  redsuite run <scenario|family|prefix|all> [opts]        run scenarios (benchmarks last, alone)
      --profile <lite|full>                               Redline workload; requires Redline in the selection (default lite)
      --loop <open|closed>                                S1 loop mode
      --serial                                            one scenario at a time, then stack down
      --keep-storage                                      with --serial: leave the stack and its storage up
";

fn usage() -> ! {
    eprint!(
        "{USAGE_HEAD}{}{}",
        frontend::usage("redsuite"),
        frontend::usage_env()
    );
    std::process::exit(2);
}

fn selected(target: &str) -> Vec<&'static ScenarioEntry> {
    if target == "all" {
        return entries().collect();
    }
    if entries().any(|entry| entry.family.prefix() == target) {
        return entries()
            .filter(|entry| entry.family.prefix() == target)
            .collect();
    }
    let (exact, prefixed): (Vec<_>, Vec<_>) = entries()
        .filter(|entry| {
            entry.name().starts_with(target)
                || entry.short_name.starts_with(target)
        })
        .partition(|entry| {
            entry.name() == target || entry.short_name == target
        });
    if exact.is_empty() {
        prefixed
    } else {
        exact
    }
}

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

type Lane = (String, f64, Vec<RunRecord>);

async fn timed(lane: &str, run: impl Future<Output = Vec<RunRecord>>) -> Lane {
    let started = Instant::now();
    let records = run.await;
    (lane.to_owned(), started.elapsed().as_secs_f64(), records)
}

async fn run_one(
    entry: &'static ScenarioEntry,
    config: ExecutionConfig,
    er: std::result::Result<ErCtx, String>,
) -> RunRecord {
    (entry.run)(config, er).await
}

async fn run_lane(
    scenarios: Vec<&'static ScenarioEntry>,
    limit: usize,
    config: ExecutionConfig,
    er: std::result::Result<ErCtx, String>,
) -> Vec<RunRecord> {
    futures_util::stream::iter(scenarios)
        .map(|entry| run_one(entry, config, er.clone()))
        .buffer_unordered(limit.max(1))
        .collect()
        .await
}

async fn run_group(
    group: &mut GroupEr,
    members: Vec<&'static ScenarioEntry>,
    base: &Result<BaseCtx>,
    config: ExecutionConfig,
    serial: bool,
) -> Vec<RunRecord> {
    let (shared, exclusive): (Vec<_>, Vec<_>) = members
        .into_iter()
        .partition(|entry| matches!(entry.er, Er::Shared(_)));
    let (batch, mut sequential) = if serial {
        (Vec::new(), shared)
    } else {
        (shared, Vec::new())
    };
    sequential.extend(exclusive);
    let mut records = Vec::new();
    if !batch.is_empty() {
        let er = group.provide(base).await;
        let limit = batch.len();
        let mut done = run_lane(batch, limit, config, er).await;
        group.note(&done);
        records.append(&mut done);
    }
    for entry in sequential {
        let er = group.provide(base).await;
        let record = run_one(entry, config, er).await;
        group.note(std::slice::from_ref(&record));
        records.push(record);
    }
    records
}

fn members(
    group: &GroupEr,
    pool: &[&'static ScenarioEntry],
) -> Vec<&'static ScenarioEntry> {
    pool.iter()
        .copied()
        .filter(|entry| group.owns(entry))
        .collect()
}

async fn run(args: &[String]) -> Result<()> {
    let suite_started = Instant::now();
    let Some(target) = args.first() else { usage() };

    let scenarios = selected(target);
    if scenarios.is_empty() {
        return Err(format!(
            "nothing matches `{target}` — `redsuite list` shows what exists"
        )
        .into());
    }
    let redline = scenarios
        .iter()
        .any(|entry| entry.family == Family::Redline);
    let mut config = ExecutionConfig::from_env(redline)?;
    let mut serial = false;
    let mut keep_storage = false;
    let mut options = args[1..].iter();
    while let Some(flag) = options.next() {
        match flag.as_str() {
            "--serial" => {
                serial = true;
                continue;
            }
            "--keep-storage" => {
                keep_storage = true;
                continue;
            }
            _ => {}
        }
        let value = options.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--profile" => {
                if !redline {
                    return Err(
                        "--profile requires a selection containing Redline"
                            .into(),
                    );
                }
                config.profile = Profile::parse(value).ok_or_else(|| {
                    format!("unknown profile `{value}` (expected lite|full)")
                })?
            }
            "--loop" => {
                config.loop_mode = LoopMode::parse(value).ok_or_else(|| {
                    format!(
                        "unknown loop mode `{value}` (expected open|closed)"
                    )
                })?
            }
            _ => usage(),
        }
    }

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
        ScenarioReport::ok(&format!("suite/{target}"))
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
            .setting("keep storage", keep_storage)
            .setting("scenarios", names.join(","))
            .setting("redline profile", config.profile.name())
    });

    let base = topology::base_only(config).await;
    let mut groups: Vec<GroupEr> = Vec::new();
    for group in scenarios.iter().filter_map(|entry| entry.er.group()) {
        if !groups.iter().any(|known| known.group.label == group.label) {
            groups.push(GroupEr::new(group));
        }
    }
    let (benchmarks, functional): (Vec<_>, Vec<_>) = scenarios
        .into_iter()
        .partition(|entry| entry.host_exclusive);
    let (grouped, fresh): (Vec<_>, Vec<_>) = functional
        .into_iter()
        .partition(|entry| entry.er.group().is_some());

    let mut lanes = Vec::new();
    if serial {
        for group in &mut groups {
            let mut mine = members(group, &grouped);
            mine.extend(members(group, &benchmarks));
            let lane = format!("{} serial", group.group.label);
            lanes.push(
                timed(&lane, run_group(group, mine, &base, config, true)).await,
            );
            group.finish().await;
        }
        let mut rest = fresh;
        rest.extend(
            benchmarks
                .iter()
                .copied()
                .filter(|entry| entry.er.group().is_none()),
        );
        lanes.push(
            timed(
                "fresh serial",
                run_lane(rest, 1, config, Err(NO_GROUP.to_owned())),
            )
            .await,
        );
    } else {
        let group_lanes = join_all(groups.iter_mut().map(|group| {
            let mine = members(group, &grouped);
            let keep = benchmarks.iter().any(|entry| group.owns(entry));
            let base = &base;
            async move {
                let lane = timed(
                    group.group.label,
                    run_group(group, mine, base, config, false),
                )
                .await;
                if !keep {
                    group.finish().await;
                }
                lane
            }
        }));
        let fresh_lane = timed(
            "fresh",
            run_lane(
                fresh,
                FRESH_CONCURRENCY,
                config,
                Err(NO_GROUP.to_owned()),
            ),
        );
        let (group_lanes, fresh_lane) =
            futures_util::future::join(group_lanes, fresh_lane).await;
        lanes.extend(group_lanes);
        lanes.push(fresh_lane);
        if !benchmarks.is_empty() {
            let benchmark_lane = async {
                let mut records = Vec::new();
                for entry in benchmarks {
                    let mut group =
                        groups.iter_mut().find(|group| group.owns(entry));
                    let er = match &mut group {
                        Some(group) => group.provide(&base).await,
                        None => Err(NO_GROUP.to_owned()),
                    };
                    let record = run_one(entry, config, er).await;
                    if let Some(group) = group {
                        group.note(std::slice::from_ref(&record));
                    }
                    records.push(record);
                }
                records
            };
            lanes.push(timed("benchmarks", benchmark_lane).await);
        }
        for group in &mut groups {
            group.finish().await;
        }
    }

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
        .flat_map(|base| base.reclaim(&format!("suite/{target}")))
        .map(|error| error.to_string())
        .collect();
    let outcome = summarize(&records).and(if reclaim_errors.is_empty() {
        Ok(())
    } else {
        Err(reclaim_errors.join("; ").into())
    });
    let cleanup = if serial && !keep_storage {
        topology::down()
    } else {
        Ok(())
    };
    if let Err(error) = &cleanup {
        error!(%error, "suite cleanup failed");
    }
    let outcome = outcome.and(cleanup);
    let Some(report) = report else {
        return outcome;
    };
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

fn list(selection: Option<&str>) {
    for entry in selection.map_or_else(|| entries().collect(), selected) {
        println!("{}", entry.name());
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = logging::init() {
        eprintln!("cannot initialize logging: {error}");
        std::process::exit(1);
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |index: usize| args.get(index).map(String::as_str);

    let local = tokio::task::LocalSet::new();
    let outcome = match arg(0) {
        Some("list") => {
            list(arg(1));
            Ok(())
        }
        Some("run") => local.run_until(run(&args[1..])).await,
        _ => match frontend::dispatch(&args) {
            Some(outcome) => outcome,
            None => usage(),
        },
    };

    if let Err(err) = outcome {
        error!(error = %err, "command failed");
        std::process::exit(1);
    }
}
