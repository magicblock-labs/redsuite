mod args;
mod catalog;
mod suite;

use std::process::ExitCode;

use args::Cli;
use clap::Parser;
use redsuite_core::{
    catalog::Family,
    logging,
    profile::{ExecutionConfig, LoopMode},
    Result,
};
use tracing::{error, info};

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
            suite::Suite::new(
                family,
                args.run,
                ExecutionConfig::default(),
                args.serial,
                args.fresh_concurrency.get(),
            )?
            .run()
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
                suite::Suite::new(Family::Redline, args.run, config, true, 1)?
                    .run()
                    .await?;
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
    let work =
        run_private_er_scenario(scenario, &[Fixture::RedlineProgram], config);
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
    let outcome = suite::finish(&[record], args.cleanup);
    if *stop.borrow() {
        if let Err(error) = outcome {
            error!(%error, "interrupted load failed");
        }
        return Ok(ExitCode::from(130));
    }
    outcome?;
    Ok(ExitCode::SUCCESS)
}
