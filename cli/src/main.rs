mod catalog;

use futures_util::StreamExt;
use redsuite_core::{
    catalog::{Lane, ScenarioEntry},
    profile::{self, ExecutionConfig, LoopMode, Profile},
    report, topology, Result, RunRecord,
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

const USAGE: &str = "\
usage:
  redsuite list [family]                       list scenarios (family: redline|redshift|redhat)
  redsuite run <scenario|family|all> [opts]    run scenarios (benchmarks last, alone)
      --profile <lite|full|soak|deep>          scenario profile (default lite)
      --loop <open|closed>                     S1 loop mode
  redsuite stack status                        show the shared base+ER stack
  redsuite stack down                          stop the shared stack
  redsuite report list                         list persisted scenario reports
  redsuite report compare [scenario] [--strict] [--brief]  diff the latest two runs per scenario (--brief: changed metrics only)
  redsuite report bmf [--out <path>]           export reports as Bencher Metric Format

environment:
  MAGICBLOCK_VALIDATOR_BIN   the ER binary under test (else `magicblock-validator` on PATH)
  REDSUITE_ROOT              workspace root, when the binary runs outside the checkout
";

fn usage() -> ! {
    eprint!("{USAGE}");
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
    scenarios: Vec<&'static ScenarioEntry>,
    limit: usize,
    config: ExecutionConfig,
) -> Vec<RunRecord> {
    if scenarios.is_empty() {
        return Vec::new();
    }
    let limit = limit.clamp(1, scenarios.len());
    futures_util::stream::iter(scenarios)
        .map(|entry| async move {
            eprintln!("[redsuite] starting {}", entry.name());
            (entry.run)(config).await
        })
        .buffer_unordered(limit)
        .collect()
        .await
}

async fn run(args: &[String]) -> Result<()> {
    let Some(target) = args.first() else { usage() };

    let mut config = ExecutionConfig::from_env()?;
    let mut options = args[1..].iter();
    while let Some(flag) = options.next() {
        let value = options.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--profile" => {
                config.profile = Profile::parse(value).ok_or_else(|| {
                    format!(
                        "unknown profile `{value}` (expected {})",
                        profile::ALL.join("|")
                    )
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

    let scenarios = selected(target);
    if scenarios.is_empty() {
        return Err(format!(
            "unknown scenario `{target}` — `redsuite list` shows what exists"
        )
        .into());
    }

    let requested_profile = config.profile.name();
    let (scenarios, unsupported): (Vec<_>, Vec<_>) = scenarios
        .into_iter()
        .partition(|entry| entry.profiles.contains(requested_profile));
    for entry in unsupported {
        eprintln!(
            "[redsuite] skipping {}: profile {requested_profile} is not in \
             its profile set",
            entry.name()
        );
    }
    if scenarios.is_empty() {
        return Err(format!(
            "no selected scenario runs under profile `{requested_profile}`"
        )
        .into());
    }

    let total_scenarios = scenarios.len();
    let (benchmarks, functional): (Vec<_>, Vec<_>) = scenarios
        .into_iter()
        .partition(|entry| entry.lane() == Lane::Exclusive);
    let (private_er, shared): (Vec<_>, Vec<_>) = functional
        .into_iter()
        .partition(|entry| entry.lane() == Lane::PrivateEr);

    let mut records = Vec::new();
    if !shared.is_empty() || !private_er.is_empty() {
        eprintln!(
            "[redsuite] running {} shared-stack and {} private-ER scenarios \
             in parallel",
            shared.len(),
            private_er.len()
        );
        let shared_count = shared.len();
        let (mut from_shared, mut from_private) = futures_util::future::join(
            run_lane(shared, shared_count, config),
            run_lane(private_er, PRIVATE_ER_CONCURRENCY, config),
        )
        .await;
        records.append(&mut from_shared);
        records.append(&mut from_private);
    }

    // Benchmarks run last and alone
    if !benchmarks.is_empty() {
        eprintln!(
            "[redsuite] running {} benchmark scenarios sequentially",
            benchmarks.len()
        );
        records.append(&mut run_lane(benchmarks, 1, config).await);
    }

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
        Some("stack") => match arg(1) {
            Some("status") => topology::status(),
            Some("down") => topology::down(),
            _ => usage(),
        },
        Some("report") => match arg(1) {
            Some("list") => report::list(),
            Some("compare") => {
                let rest = &args[2..];
                let strict = rest.iter().any(|flag| flag == "--strict");
                let brief = rest.iter().any(|flag| flag == "--brief");
                let filter = rest.iter().find(|arg| !arg.starts_with("--"));
                report::compare(filter.map(String::as_str), strict, brief)
            }
            Some("bmf") => match (arg(2), arg(3)) {
                (Some("--out"), Some(path)) => report::bmf(Some(path)),
                (None, _) => report::bmf(None),
                _ => usage(),
            },
            _ => usage(),
        },
        _ => usage(),
    };

    if let Err(err) = outcome {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_sets_gate_by_membership() {
        use redsuite_core::catalog::ProfileSet;
        for known in profile::ALL {
            assert!(ProfileSet::ALL.contains(known));
        }
        let narrowed = ProfileSet(&["soak"]);
        assert!(narrowed.contains("soak"));
        assert!(!narrowed.contains("lite"));
    }

    #[test]
    fn scenario_names_are_unique() {
        let mut names: Vec<String> =
            entries().map(|entry| entry.name()).collect();
        let total = names.len();
        names.sort();
        names.dedup();
        assert_eq!(total, names.len(), "duplicate scenario names");
    }

    fn nextest_overrides() -> Vec<(String, String)> {
        let config = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../.config/nextest.toml"
        ))
        .expect("read .config/nextest.toml");
        let mut pairs = Vec::new();
        let mut pending_filter = None;
        for line in config.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("filter = \"") {
                pending_filter = Some(rest.trim_end_matches('"').to_owned());
            }
            if let Some(rest) = line.strip_prefix("test-group = \"") {
                if let Some(filter) = pending_filter.take() {
                    pairs.push((filter, rest.trim_end_matches('"').to_owned()));
                }
            }
        }
        pairs
    }

    fn filter_matches(filter: &str, entry: &ScenarioEntry) -> bool {
        let test_name =
            format!("catalog::{}::{}", entry.family.prefix(), entry.short_name);
        filter.split(" or ").any(|term| {
            let term = term.trim();
            if let Some(needle) = term
                .strip_prefix("test(")
                .and_then(|rest| rest.strip_suffix(')'))
            {
                return test_name.contains(needle);
            }
            false
        })
    }

    #[test]
    fn nextest_groups_match_the_catalog() {
        let overrides = nextest_overrides();
        assert!(
            !overrides.is_empty(),
            "no test-group overrides found in nextest.toml"
        );
        for entry in entries() {
            let configured = overrides
                .iter()
                .find(|(filter, _)| filter_matches(filter, entry))
                .map(|(_, group)| group.as_str());
            assert_eq!(
                configured,
                entry.nextest_group(),
                "scenario {} sits in the wrong nextest group",
                entry.name()
            );
        }
    }
}
