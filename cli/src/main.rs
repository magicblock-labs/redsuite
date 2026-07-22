use redsuite_core::{report, run_scenario, topology, Result, ScenarioReport};

macro_rules! registry {
    ($(($name:literal, $scenario:path)),* $(,)?) => {
        const NAMES: &[&str] = &[$($name),*];

        async fn run_one(name: &str) -> Option<ScenarioReport> {
            match name {
                $($name => Some(run_scenario($scenario).await),)*
                _ => None,
            }
        }
    };
}

registry![
    ("redline/simple_load", redline::scenarios::aperture::simple_load::SimpleLoad),
    ("redline/rpc_warm_ingress", redline::scenarios::aperture::rpc_warm_ingress::WarmIngress),
    ("redline/rpc_capacity_blast", redline::scenarios::aperture::rpc_capacity_blast::RpcCapacityBlast),
    ("redline/high_cu", redline::scenarios::aperture::high_cu::HighCu),
    ("redline/ws_fanout_threshold", redline::scenarios::aperture::ws_fanout_threshold::WsFanoutThreshold),
    ("redline/ws_conn_capacity", redline::scenarios::aperture::ws_conn_capacity::WsConnCapacity),
    ("redline/hot_account_cliff", redline::scenarios::scheduler::hot_account_cliff::HotAccountCliff),
    ("redline/executor_saturation", redline::scenarios::scheduler::executor_saturation::ExecutorSaturation),
    ("redline/clone_lru_churn", redline::scenarios::chainlink::clone_lru_churn::CloneLruChurn),
    ("redline/ensure_gate_stall", redline::scenarios::chainlink::ensure_gate_stall::EnsureGateStall),
    ("redline/cold_hydration_tail", redline::scenarios::chainlink::cold_hydration_tail::ColdHydrationTail),
    ("redline/commit_width_envelope", redline::scenarios::committor::commit_width_envelope::CommitWidthEnvelope),
    ("redline/commit_throughput_ceiling", redline::scenarios::committor::commit_throughput_ceiling::CommitThroughputCeiling),
    ("redline/storage_prodsize_sustain", redline::scenarios::storage::storage_prodsize_sustain::StorageProdsizeSustain),
    ("redline/restart_under_load", redline::scenarios::lifecycle::restart_under_load::RestartUnderLoad),
    ("redline/protocol_boundary_selftest", redline::scenarios::harness::protocol_boundary_selftest::ProtocolBoundarySelftest),
    ("redshift/example", redshift::scenarios::harness::example::Example),
    ("redshift/clone_on_access", redshift::scenarios::chainlink::clone_on_access::CloneOnAccess),
    ("redshift/commit_roundtrip", redshift::scenarios::committor::commit_roundtrip::CommitRoundtrip),
    ("redshift/api_invariants", redshift::scenarios::harness::api_invariants::ApiInvariants),
    ("redshift/claim_fees", redshift::scenarios::committor::claim_fees::ClaimFees),
    ("redshift/pubsub_contracts", redshift::scenarios::pubsub::pubsub_contracts::PubsubContracts),
    ("redhat/illegal_writable", redhat::scenarios::chainlink::illegal_writable::IllegalWritable),
    ("redhat/fee_payer_rules", redhat::scenarios::aperture::fee_payer_rules::FeePayerRules),
];

const USAGE: &str = "\
usage:
  redsuite list [family]                       list scenarios (family: redline|redshift|redhat)
  redsuite run <scenario|family|all> [opts]    run scenarios sequentially
      --profile <lite|full|soak|deep>          scenario profile (default lite)
      --loop <open|closed>                     S1 loop mode
  redsuite stack status                        show the shared base+ER stack
  redsuite stack down                          stop the shared stack
  redsuite report list                         list persisted scenario reports
  redsuite report compare [scenario] [--strict]  diff the latest two runs per scenario
  redsuite report bmf [--out <path>]           export reports as Bencher Metric Format

environment:
  MAGICBLOCK_VALIDATOR_BIN   the ER binary under test (else `magicblock-validator` on PATH)
  REDSUITE_ROOT              workspace root, when the binary runs outside the checkout
";

fn usage() -> ! {
    eprint!("{USAGE}");
    std::process::exit(2);
}

fn selected(target: &str) -> Vec<&'static str> {
    match target {
        "all" => NAMES.to_vec(),
        family
            if NAMES
                .iter()
                .any(|name| name.starts_with(&format!("{family}/"))) =>
        {
            NAMES
                .iter()
                .copied()
                .filter(|name| name.starts_with(&format!("{family}/")))
                .collect()
        }
        exact => NAMES
            .iter()
            .copied()
            .filter(|name| {
                *name == exact || name.ends_with(&format!("/{exact}"))
            })
            .collect(),
    }
}

async fn run(args: &[String]) -> Result<()> {
    let Some(target) = args.first() else { usage() };

    let mut options = args[1..].iter();
    while let Some(flag) = options.next() {
        let value = options.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--profile" => std::env::set_var("REDSUITE_PROFILE", value),
            "--loop" => std::env::set_var("REDSUITE_LOOP", value),
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

    // Scenarios drive one shared stack and record performance numbers: a
    // co-tenant run contends for the box and corrupts them. Never parallel.
    for name in &scenarios {
        eprintln!("[redsuite] running {name}");
        run_one(name).await.ok_or_else(|| {
            format!("scenario `{name}` vanished from the registry")
        })?;
    }
    if scenarios.len() > 1 {
        eprintln!("[redsuite] {} scenarios passed", scenarios.len());
    }
    Ok(())
}

fn list(family: Option<&str>) {
    for name in NAMES {
        if family.is_none_or(|want| name.starts_with(&format!("{want}/"))) {
            println!("{name}");
        }
    }
}

#[tokio::main(flavor = "current_thread")]
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
                let filter = rest.iter().find(|arg| !arg.starts_with("--"));
                report::compare(filter.map(String::as_str), strict)
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
