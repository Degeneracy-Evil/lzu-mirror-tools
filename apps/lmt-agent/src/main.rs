use std::path::PathBuf;

use clap::{Parser, Subcommand};
use lmt_agent::{
    Agent, PublicationStatus, abandon_publication, clear_publication_fence,
    config::{Config, Logging, LoggingFormat},
    preflight_publication, publication_doctor, publication_status, reset_spool, retry_publication_durability,
};
use tokio::{fs, sync::watch};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "/etc/lmt/agent.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    Doctor,
    ResetSpool {
        #[arg(long)]
        acknowledge_control_plane_restore: bool,
    },
    Publication {
        #[command(subcommand)]
        command: PublicationCommand,
    },
}

#[derive(Subcommand)]
enum PublicationCommand {
    Preflight,
    Status {
        #[arg(long)]
        mirror: String,
    },
    RetryDurability(PublicationIdentity),
    Abandon {
        #[command(flatten)]
        identity: PublicationIdentity,
        #[arg(long)]
        acknowledge_visible_publication_risk: bool,
    },
    FenceClear(PublicationIdentity),
}

#[derive(clap::Args)]
struct PublicationIdentity {
    #[arg(long)]
    mirror: String,
    #[arg(long)]
    run: String,
    #[arg(long)]
    attempt: u32,
    #[arg(long)]
    spec_hash: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config: Config = toml::from_str(&fs::read_to_string(args.config).await?)?;
    initialize_logging(config.logging.as_ref())?;
    match args.command {
        Some(Command::Doctor) => {
            let report = publication_doctor(&config).await?;
            for check in &report.checks {
                println!(
                    "{} {}: {}",
                    if check.healthy { "ok" } else { "critical" },
                    check.id,
                    check.message
                );
            }
            if !report.healthy {
                anyhow::bail!("publication doctor found unhealthy conditions");
            }
            return Ok(());
        }
        Some(Command::ResetSpool {
            acknowledge_control_plane_restore,
        }) => {
            let removed = reset_spool(&config, acknowledge_control_plane_restore).await?;
            println!("removed {removed} Attempt spool artifacts; Agent installation identity preserved");
            return Ok(());
        }
        Some(Command::Publication { command }) => {
            run_publication_command(&config, command).await?;
            return Ok(());
        }
        None => {}
    }
    let (sender, receiver) = watch::channel(false);
    tracing::info!(component = "agent", version = env!("CARGO_PKG_VERSION"), node = %config.node.name, "starting LMT Agent");
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = sender.send(true);
    });
    let agent = Agent::new(config, receiver).await?;
    let reload_agent = agent.clone();
    tokio::spawn(async move {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).expect("SIGHUP");
        while signal.recv().await.is_some() {
            match reload_agent.reload_token().await {
                Ok(()) => tracing::info!(component = "agent", "Agent credential reloaded"),
                Err(error) => tracing::error!(component = "agent", %error, "Agent credential reload failed"),
            }
        }
    });
    agent.run().await
}

async fn run_publication_command(config: &Config, command: PublicationCommand) -> anyhow::Result<()> {
    match command {
        PublicationCommand::Preflight => {
            preflight_publication(config).await?;
            println!("Atomic publication filesystem preflight passed");
        }
        PublicationCommand::Status { mirror } => {
            let statuses = publication_status(config, &mirror).await?;
            if statuses.is_empty() {
                println!("no local publication spool records for Mirror {mirror}");
            } else {
                for status in statuses {
                    print_status(&status);
                }
            }
        }
        PublicationCommand::RetryDurability(identity) => {
            let status = retry_publication_durability(
                config,
                &identity.mirror,
                &identity.run,
                identity.attempt,
                &identity.spec_hash,
            )
            .await?;
            print_status(&status);
        }
        PublicationCommand::Abandon {
            identity,
            acknowledge_visible_publication_risk,
        } => {
            let status = abandon_publication(
                config,
                &identity.mirror,
                &identity.run,
                identity.attempt,
                &identity.spec_hash,
                acknowledge_visible_publication_risk,
            )
            .await?;
            print_status(&status);
            println!("durable local writer fence retained; restart Agent to reconcile terminal failure");
        }
        PublicationCommand::FenceClear(identity) => {
            clear_publication_fence(
                config,
                &identity.mirror,
                &identity.run,
                identity.attempt,
                &identity.spec_hash,
            )
            .await?;
            println!("cleared exact durable publication fence for Mirror {}", identity.mirror);
        }
    }
    Ok(())
}

fn print_status(status: &PublicationStatus) {
    println!(
        "mirror={} run={} attempt={} spec_hash={} phase={} state={:?} event_ack={}/{}",
        status.mirror,
        status.run_id,
        status.attempt,
        status.spec_hash,
        status.phase,
        status.attempt_state,
        status.acknowledged_sequence,
        status.event_sequence
    );
}

fn initialize_logging(config: Option<&Logging>) -> anyhow::Result<()> {
    let level = config.map_or("info", |logging| logging.level.as_str());
    let filter = tracing_subscriber::EnvFilter::try_new(level)?;
    match config.map_or(LoggingFormat::Json, |logging| logging.format) {
        LoggingFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .try_init()
            .map_err(|error| anyhow::anyhow!(error.to_string()))?,
        LoggingFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|error| anyhow::anyhow!(error.to_string()))?,
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async { tokio::signal::ctrl_c().await.expect("signal") };
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("signal")
            .recv()
            .await;
    };
    tokio::select! { () = ctrl_c => {}, () = terminate => {} }
}
