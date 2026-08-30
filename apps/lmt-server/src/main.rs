use anyhow::Context;
use clap::Parser;
use lmt_server::{ServerConfig, build_router, initialize};
use std::path::PathBuf;
use tokio::{fs, net::TcpListener};
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "/etc/lmt/server.toml")]
    config: PathBuf,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().json().with_env_filter("info").init();
    let a = Args::parse();
    let source = fs::read_to_string(&a.config)
        .await
        .with_context(|| format!("read {}", a.config.display()))?;
    let config: ServerConfig = toml::from_str(&source)?;
    let state = initialize(&config).await?;
    let listener = TcpListener::bind(&config.bind).await?;
    axum::serve(listener, build_router(state))
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}
async fn shutdown() {
    let ctrl = async { tokio::signal::ctrl_c().await.expect("signal") };
    #[cfg(unix)]
    let term = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("signal")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { () = ctrl => {}, () = term => {} }
}
