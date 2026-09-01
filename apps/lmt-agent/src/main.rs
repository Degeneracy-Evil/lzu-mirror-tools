use std::path::PathBuf;

use clap::{Parser, Subcommand};
use lmt_agent::{
    Agent,
    config::{Config, Logging, LoggingFormat},
    reset_spool,
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
    ResetSpool {
        #[arg(long)]
        acknowledge_control_plane_restore: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config: Config = toml::from_str(&fs::read_to_string(args.config).await?)?;
    initialize_logging(config.logging.as_ref())?;
    if let Some(Command::ResetSpool {
        acknowledge_control_plane_restore,
    }) = args.command
    {
        let removed = reset_spool(&config, acknowledge_control_plane_restore).await?;
        println!("removed {removed} Attempt spool artifacts; Agent installation identity preserved");
        return Ok(());
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
