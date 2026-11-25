mod args;
mod catalog;
mod suite;

use std::{
    num::NonZeroUsize,
    process::ExitCode,
    rc::Rc,
    sync::{Arc, OnceLock},
    time::Instant,
};

use anyhow::Context;
use args::{Benchmarks, Checks, Cli, Command};
use clap::Parser;
use redsuite_core::config::{Environment, ErEndpoints};
use redsuite_core::{catalog::Family, config::ExecutionConfig, logging, Result};
use suite::Suite;
use tokio::{sync::watch, task::LocalSet};
use tracing::error;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(error) = logging::init() {
        eprintln!("cannot initialize logging: {error:#}");
        return ExitCode::FAILURE;
    }
    let outcome = LocalSet::new().run_until(dispatch(cli.command)).await;

    match outcome {
        Ok(code) => code,
        Err(error) => {
            error!(error = %format_args!("{error:#}"), "command failed");
            ExitCode::FAILURE
        }
    }
}

fn list(family: Family, selections: &[String]) -> Result<()> {
    let entries = if selections.is_empty() {
        catalog::entries(family)
    } else {
        catalog::select(family, selections)?
    };
    for entry in entries {
        println!(
            "{}{}",
            entry.name(family),
            if entry.explicit_only { " (explicit only)" } else { "" }
        );
    }
    Ok(())
}

async fn interruptible(
    run: impl AsyncFnOnce(watch::Receiver<bool>, Arc<OnceLock<Instant>>) -> Result<()>,
) -> Result<ExitCode> {
    let (stop, receiver) = watch::channel(false);
    let stop_requested_at = Arc::new(OnceLock::new());
    let interrupted_at = stop_requested_at.clone();
    let interrupt = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            interrupted_at.get_or_init(Instant::now);
            stop.send_replace(true);
        }
    });
    let outcome = run(receiver.clone(), stop_requested_at).await;
    interrupt.abort();
    if *receiver.borrow() {
        if let Err(error) = outcome {
            error!(error = %format_args!("{error:#}"), "interrupted workload failed");
        }
        return Ok(ExitCode::from(130));
    }
    outcome?;
    Ok(ExitCode::SUCCESS)
}

async fn checks(family: Family, command: Checks) -> Result<()> {
    match command {
        Checks::List { selections } => list(family, &selections),
        Checks::Run(args) => {
            let seed = args.seed.unwrap_or_else(rand::random);
            Suite::new(
                family,
                args.run,
                ExecutionConfig {
                    seed,
                    require_running_base: args.require_running_base,
                    ..ExecutionConfig::default()
                },
                args.serial,
                args.fresh_concurrency,
                args.shared_concurrency,
            )?
            .run()
            .await
        }
    }
}

async fn dispatch(command: Command) -> Result<ExitCode> {
    match command {
        Command::Redshift(command) => checks(Family::Redshift, command).await?,
        Command::Redhat(command) => checks(Family::Redhat, command).await?,
        Command::Redline(command) => match command {
            Benchmarks::List { selections } => list(Family::Redline, &selections)?,
            Benchmarks::Run(args) => {
                let environment = if let Some(base_rpc) = args.base_rpc {
                    let er = args
                        .er_rpc
                        .map(|rpc| -> Result<ErEndpoints> {
                            Ok(ErEndpoints {
                                rpc: rpc.parse()?,
                                ws: args.er_ws.context("missing --er-ws")?.parse()?,
                                metrics: args.metrics.context("missing --metrics")?.parse()?,
                            })
                        })
                        .transpose()?;
                    Some(Rc::new(Environment {
                        base_rpc: base_rpc.parse()?,
                        base_ws: args.base_ws.context("missing --base-ws")?.parse()?,
                        er,
                        payer: args.payer.context("missing --payer")?,
                    }))
                } else {
                    None
                };
                return interruptible(async move |stop, stop_requested_at| {
                    let config = ExecutionConfig {
                        duration: Some(args.duration),
                        stop: Some(stop),
                        stop_requested_at: Some(stop_requested_at),
                        environment,
                        profile: args.profile,
                        loop_mode: args.loop_mode,
                        ..ExecutionConfig::default()
                    };
                    Suite::new(
                        Family::Redline,
                        args.run,
                        config,
                        true,
                        NonZeroUsize::MIN,
                        NonZeroUsize::MIN,
                    )?
                    .run()
                    .await
                })
                .await;
            }
        },
        Command::Continuous(args) => {
            let seed = args.seed.unwrap_or_else(rand::random);
            return interruptible(async move |stop, stop_requested_at| {
                let config = ExecutionConfig {
                    seed,
                    duration: Some(args.duration),
                    stop: Some(stop),
                    stop_requested_at: Some(stop_requested_at),
                    profile: args.profile,
                    continuous: true,
                    ..ExecutionConfig::default()
                };
                suite::continuous::run(config, args.shared_concurrency.get()).await
            })
            .await;
        }
        Command::Shared(command) => command.run().await?,
    }
    Ok(ExitCode::SUCCESS)
}
