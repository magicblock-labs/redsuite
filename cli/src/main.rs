use futures_util::StreamExt;
use redsuite_core::{
    profile, report, run_scenario, topology, Result, ScenarioReport,
};

const PRIVATE_ER_MARKERS: &[&str] = &[
    "aml_gate",
    "config_gates",
    "ledger_restore",
    "task_scheduler",
];
const PRIVATE_ER_CONCURRENCY: usize = 2;

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
    ("redshift/commits", redshift::scenarios::committor::commits::Commits),
    ("redshift/commit_and_undelegate", redshift::scenarios::committor::commit_and_undelegate::CommitAndUndelegate),
    ("redshift/commit_limit_and_fees", redshift::scenarios::committor::commit_limit_and_fees::CommitLimitAndFees),
    ("redshift/intent_flows", redshift::scenarios::committor::intent_flows::IntentFlows),
    ("redshift/config_gates", redshift::scenarios::harness::config_gates::ConfigGates),
    ("redshift/task_scheduler", redshift::scenarios::scheduler::task_scheduler::TaskScheduler),
    ("redshift/ledger_restore_basics", redshift::scenarios::lifecycle::ledger_restore_basics::LedgerRestoreBasics),
    ("redshift/ledger_restore_chain", redshift::scenarios::lifecycle::ledger_restore_chain::LedgerRestoreChain),
    ("redshift/api_invariants", redshift::scenarios::harness::api_invariants::ApiInvariants),
    ("redshift/claim_fees", redshift::scenarios::committor::claim_fees::ClaimFees),
    ("redshift/pubsub_contracts", redshift::scenarios::pubsub::pubsub_contracts::PubsubContracts),
    ("redshift/account_info_semantics", redshift::scenarios::chainlink::account_info_semantics::AccountInfoSemantics),
    ("redshift/escrow_cloning", redshift::scenarios::chainlink::escrow_cloning::EscrowCloning),
    ("redshift/parallel_cloning", redshift::scenarios::chainlink::parallel_cloning::ParallelCloning),
    ("redshift/multi_program_clone", redshift::scenarios::chainlink::multi_program_clone::MultiProgramClone),
    ("redshift/loader_matrix", redshift::scenarios::chainlink::loader_matrix::LoaderMatrix),
    ("redshift/post_delegation_token_transfer", redshift::scenarios::chainlink::post_delegation_token_transfer::PostDelegationTokenTransfer),
    ("redshift/aml_gate", redshift::scenarios::chainlink::aml_gate::AmlGate),
    ("redshift/table_mania", redshift::scenarios::committor::table_mania::TableManiaScenario),
    ("redhat/illegal_writable", redhat::scenarios::chainlink::illegal_writable::IllegalWritable),
    ("redhat/fee_payer_rules", redhat::scenarios::aperture::fee_payer_rules::FeePayerRules),
];

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

fn is_private_er(name: &str) -> bool {
    PRIVATE_ER_MARKERS
        .iter()
        .any(|marker| name.contains(marker))
}

async fn run_lane(names: Vec<&'static str>, limit: usize) -> Result<()> {
    if names.is_empty() {
        return Ok(());
    }
    let limit = limit.clamp(1, names.len());
    let mut running = futures_util::stream::iter(names)
        .map(|name| async move {
            eprintln!("[redsuite] starting {name}");
            (name, run_one(name).await)
        })
        .buffer_unordered(limit);
    while let Some((name, report)) = running.next().await {
        report.ok_or_else(|| {
            format!("scenario `{name}` vanished from the registry")
        })?;
    }
    Ok(())
}

async fn run(args: &[String]) -> Result<()> {
    let Some(target) = args.first() else { usage() };

    let mut options = args[1..].iter();
    while let Some(flag) = options.next() {
        let value = options.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--profile" => {
                if !profile::is_known(value) {
                    return Err(format!(
                        "unknown profile `{value}` (expected {})",
                        profile::ALL.join("|")
                    )
                    .into());
                }
                std::env::set_var("REDSUITE_PROFILE", value)
            }
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

    let total_scenarios = scenarios.len();
    let (benchmarks, functional): (Vec<_>, Vec<_>) = scenarios
        .into_iter()
        .partition(|name| name.starts_with("redline/"));
    let (private_er, shared): (Vec<_>, Vec<_>) =
        functional.into_iter().partition(|name| is_private_er(name));

    if !shared.is_empty() || !private_er.is_empty() {
        eprintln!(
            "[redsuite] running {} shared-stack and {} private-ER scenarios \
             in parallel",
            shared.len(),
            private_er.len()
        );
        let shared_count = shared.len();
        let (shared_outcome, private_outcome) = futures_util::future::join(
            run_lane(shared, shared_count),
            run_lane(private_er, PRIVATE_ER_CONCURRENCY),
        )
        .await;
        shared_outcome?;
        private_outcome?;
    }

    // Benchmarks run last and alone
    if !benchmarks.is_empty() {
        eprintln!(
            "[redsuite] running {} benchmark scenarios sequentially",
            benchmarks.len()
        );
        run_lane(benchmarks, 1).await?;
    }

    if total_scenarios > 1 {
        eprintln!("[redsuite] {total_scenarios} scenarios passed");
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
