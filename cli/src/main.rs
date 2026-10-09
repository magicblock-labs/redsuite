mod catalog;

use std::{process::Command, time::Instant};

use futures_util::StreamExt;
use redsuite_core::{
    catalog::{Family, Lane, ScenarioEntry, Topology},
    console, frontend,
    profile::{ExecutionConfig, LoopMode, Profile},
    report::{self, ScenarioReport, Unit},
    topology, Result, RunRecord,
};

const FAMILIES: &[&[ScenarioEntry]] = &[
    catalog::redline::SCENARIOS,
    catalog::redshift::SCENARIOS,
    catalog::redhat::SCENARIOS,
];
const PRIVATE_ER_CONCURRENCY: usize = 2;

fn entries() -> impl Iterator<Item = &'static ScenarioEntry> {
    FAMILIES.iter().flat_map(|scenarios| scenarios.iter())
}

const USAGE_HEAD: &str = "\
usage:
  redsuite list [family]                                  list scenarios (family: redline|redshift|redhat)
  redsuite run <scenario|family|all> [opts]               run scenarios (benchmarks last, alone)
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
    entries()
        .filter(|entry| entry.name() == target || entry.short_name == target)
        .collect()
}

async fn run_lane(
    lane: &'static str,
    scenarios: Vec<&'static ScenarioEntry>,
    limit: usize,
    config: ExecutionConfig,
) -> (Vec<RunRecord>, (&'static str, f64)) {
    let started = Instant::now();
    let records = futures_util::stream::iter(scenarios)
        .map(|entry| async move {
            console::debug(format_args!("starting {}", entry.name()));
            (entry.run)(config).await
        })
        .buffer_unordered(limit.max(1))
        .collect()
        .await;
    (records, (lane, started.elapsed().as_secs_f64()))
}

async fn run(args: &[String]) -> Result<()> {
    let suite_started = Instant::now();
    let Some(target) = args.first() else { usage() };

    let scenarios = selected(target);
    if scenarios.is_empty() {
        return Err(format!(
            "unknown scenario `{target}` — `redsuite list` shows what exists"
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
    let (benchmarks, functional): (Vec<_>, Vec<_>) = scenarios
        .into_iter()
        .partition(|entry| entry.lane() == Lane::Exclusive);
    let (private_er, shared): (Vec<_>, Vec<_>) = functional
        .into_iter()
        .partition(|entry| entry.lane() == Lane::PrivateEr);

    let mut lanes = Vec::new();
    let mut stop = Ok(false);
    if serial {
        let (private_benchmarks, shared_benchmarks): (Vec<_>, Vec<_>) =
            benchmarks
                .into_iter()
                .partition(|entry| entry.topology == Topology::PrivateEr);
        let mut on_shared_er = shared;
        on_shared_er.extend(shared_benchmarks);
        let mut on_private_er = private_er;
        on_private_er.extend(private_benchmarks);
        console::debug(format_args!(
            "running {} shared-ER scenarios, then {} private-ER scenarios, \
             one at a time",
            on_shared_er.len(),
            on_private_er.len()
        ));
        lanes.push(run_lane("shared serial", on_shared_er, 1, config).await);
        if !on_private_er.is_empty() {
            stop = topology::stop_shared_er().await;
        }
        if matches!(stop, Ok(true)) {
            console::line(format_args!(
                "stopped the shared ER before the private-ER scenarios"
            ));
        }
        if stop.is_ok() {
            lanes.push(
                run_lane("private-er serial", on_private_er, 1, config).await,
            );
        }
    } else {
        console::debug(format_args!(
            "running {} shared-stack and {} private-ER scenarios in parallel",
            shared.len(),
            private_er.len()
        ));
        let shared_count = shared.len();
        let (shared, private) = futures_util::future::join(
            run_lane("shared", shared, shared_count, config),
            run_lane("private-er", private_er, PRIVATE_ER_CONCURRENCY, config),
        )
        .await;
        lanes.extend([shared, private]);
        if !benchmarks.is_empty() {
            console::debug(format_args!(
                "running {} benchmark scenarios sequentially",
                benchmarks.len()
            ));
            lanes.push(run_lane("benchmarks", benchmarks, 1, config).await);
        }
    }
    let mut records = Vec::new();
    for (mut runs, (lane, seconds)) in lanes {
        if !runs.is_empty() {
            report = report.map(|report| {
                report.metric(format!("{lane} seconds"), Unit::Seconds, seconds)
            });
        }
        records.append(&mut runs);
    }
    let outcome = stop
        .map_err(|error| format!("stopping shared ER: {error}").into())
        .and(summarize(&records));
    let cleanup = if serial && !keep_storage {
        topology::down()
    } else {
        Ok(())
    };
    if let Err(error) = &cleanup {
        console::line(format_args!("suite cleanup failed: {error}"));
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
        console::line(format_args!("suite report failed: {error}"));
    }
    outcome.and(persisted)
}

fn summarize(records: &[RunRecord]) -> Result<()> {
    let total_scenarios = records.len();
    let failed: Vec<&RunRecord> =
        records.iter().filter(|record| !record.passed()).collect();
    if !failed.is_empty() {
        for record in &failed {
            match record.failure() {
                Some(failure) => eprintln!("[redsuite] FAILED {failure}"),
                None => eprintln!("[redsuite] FAILED {}", record.name),
            }
        }
        return Err(format!(
            "{} of {total_scenarios} scenarios failed",
            failed.len()
        )
        .into());
    }
    if total_scenarios > 1 {
        eprintln!("[redsuite] {total_scenarios} scenarios passed");
    }
    Ok(())
}

fn list(family: Option<&str>) {
    for entry in entries() {
        if family.is_none_or(|want| entry.family.prefix() == want) {
            println!("{}", entry.name());
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |index: usize| args.get(index).map(String::as_str);

    let outcome = match arg(0) {
        Some("list") => {
            list(arg(1));
            Ok(())
        }
        Some("run") => run(&args[1..]).await,
        _ => match frontend::dispatch(&args) {
            Some(outcome) => outcome,
            None => usage(),
        },
    };

    if let Err(err) = outcome {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
