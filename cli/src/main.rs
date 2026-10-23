mod args;
mod catalog;
mod suite;

use std::process::ExitCode;

use args::{Benchmarks, Checks, Cli, Command};
use clap::Parser;
use redline::load::Options;
use redsuite_core::{
    catalog::Family,
    logging,
    profile::{ExecutionConfig, LoopMode},
    PrivateErScenario, Result,
};
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
    for entry in catalog::select(family, selections)? {
        println!("{}", entry.name(family));
    }
    Ok(())
}

async fn checks(family: Family, command: Checks) -> Result<()> {
    match command {
        Checks::List { selections } => list(family, &selections),
        Checks::Run(args) => {
            let seed = args.seed.unwrap_or_else(rand::random);
            info!(seed, "run seed");
            Suite::new(
                family,
                args.run,
                ExecutionConfig {
                    seed,
                    ..ExecutionConfig::default()
                },
                args.serial,
                args.fresh_concurrency.get(),
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
                let config = ExecutionConfig {
                    profile: args.profile,
                    loop_mode: args.loop_mode,
                    ..ExecutionConfig::default()
                };
                Suite::new(Family::Redline, args.run, config, true, 1)?.run().await?;
            }
            Benchmarks::Load(args) => return load(args).await,
        },
        Command::Shared(command) => command.run()?,
    }
    Ok(ExitCode::SUCCESS)
}

async fn load(args: args::Load) -> Result<ExitCode> {
    use redsuite_core::catalog::Fixture;
    use tokio::{
        signal::unix::{signal, SignalKind},
        sync::watch,
    };

    let mut interrupts = signal(SignalKind::interrupt())?;
    let (stop, receiver) = watch::channel(false);
    let config = ExecutionConfig {
        profile: args.profile,
        loop_mode: LoopMode::Open,
        ..ExecutionConfig::default()
    };
    let options = Options {
        duration: args.duration,
        rate: args.rate,
        concurrency: args.concurrency,
        stop: receiver,
    };
    let work = redsuite_core::scenario!(
        redline::load::Load,
        fresh(&[Fixture::RedlineProgram], options)
    )
    .run(Family::Redline, config, None);
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
            error!(error = %format_args!("{error:#}"), "interrupted load failed");
        }
        return Ok(ExitCode::from(130));
    }
    outcome?;
    Ok(ExitCode::SUCCESS)
}
