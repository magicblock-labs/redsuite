mod args;
mod catalog;
mod suite;

use std::process::ExitCode;

use anyhow::Context;
use args::{Benchmarks, Checks, Cli, Command};
use clap::Parser;
use redsuite_core::config::{Environment, ErEndpoints};
use redsuite_core::{catalog::Family, config::ExecutionConfig, logging, Result};
use std::rc::Rc;
use suite::Suite;
use tokio::task::LocalSet;
use tracing::{error, info};

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

async fn checks(family: Family, command: Checks) -> Result<()> {
    match command {
        Checks::List { selections } => list(family, &selections),
        Checks::Run(args) => {
            let seed = args.seed.unwrap_or_else(rand::random);
            let runner = redsuite_core::report::binary_hash(&std::env::current_exe()?)?;
            info!(attack_seed = seed, runner, "run seed");
            Suite::new(
                family,
                args.run,
                ExecutionConfig {
                    seed,
                    ..ExecutionConfig::default()
                },
                args.serial,
                args.fresh_concurrency.get(),
                args.shared_concurrency.get(),
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
                let (stop, receiver) = tokio::sync::watch::channel(false);
                let interrupt = tokio::spawn(async move {
                    if tokio::signal::ctrl_c().await.is_ok() {
                        stop.send_replace(true);
                    }
                });
                let config = ExecutionConfig {
                    duration: Some(args.duration),
                    stop: Some(receiver.clone()),
                    environment,
                    profile: args.profile,
                    loop_mode: args.loop_mode,
                    ..ExecutionConfig::default()
                };
                let outcome =
                    Suite::new(Family::Redline, args.run, config, true, 1, 1)?.run().await;
                interrupt.abort();
                if *receiver.borrow() {
                    if let Err(error) = outcome {
                        error!(error = %format_args!("{error:#}"), "interrupted workload failed");
                    }
                    return Ok(ExitCode::from(130));
                }
                outcome?;
            }
        },
        Command::Shared(command) => command.run()?,
    }
    Ok(ExitCode::SUCCESS)
}
