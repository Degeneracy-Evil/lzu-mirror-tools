use std::path::PathBuf;

use clap::Parser;
use lmt_agent::{Agent, config::Config};
use tokio::{fs, sync::watch};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "/etc/lmt/agent.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().json().with_env_filter("info").init();
    let config: Config = toml::from_str(&fs::read_to_string(Args::parse().config).await?)?;
    let (sender, receiver) = watch::channel(false);
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
