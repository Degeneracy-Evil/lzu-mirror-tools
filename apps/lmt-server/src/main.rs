use anyhow::Context;
use clap::{Parser, Subcommand};
use lmt_server::{LoggingConfig, LoggingFormat, ServerConfig, acquire_server_lock, backup, build_router, initialize};
use std::{
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};
use tokio::{fs, net::TcpListener};
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "/etc/lmt/server.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    Backup {
        #[arg(long)]
        output: PathBuf,
    },
    Restore {
        #[arg(long = "from")]
        source: PathBuf,
        #[arg(long)]
        acknowledge_control_plane_restore: bool,
    },
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    let source = fs::read_to_string(&a.config)
        .await
        .with_context(|| format!("read {}", a.config.display()))?;
    let config: ServerConfig = toml::from_str(&source)?;
    initialize_logging(config.logging.as_ref())?;
    if let Some(command) = a.command {
        return maintenance(&config, command).await;
    }
    let _process_lock = acquire_server_lock(&config)?;
    tracing::info!(
        component = "server",
        version = env!("CARGO_PKG_VERSION"),
        "starting LMT Server"
    );
    let state = initialize(&config).await?;
    let reload_state = state.clone();
    tokio::spawn(async move {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).expect("SIGHUP");
        while signal.recv().await.is_some() {
            match reload_state.reload_operator_token().await {
                Ok(()) => tracing::info!(component = "server", "operator credential reloaded"),
                Err(error) => tracing::error!(component = "server", %error, "operator credential reload failed"),
            }
        }
    });
    let listener = TcpListener::bind(&config.bind).await?;
    axum::serve(listener, build_router(state))
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}

fn initialize_logging(config: Option<&LoggingConfig>) -> anyhow::Result<()> {
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

async fn maintenance(config: &ServerConfig, command: Command) -> anyhow::Result<()> {
    let _process_lock = acquire_server_lock(config)?;
    match command {
        Command::Backup { output } => {
            let database = config.database_path.clone();
            let manifest = tokio::task::spawn_blocking(move || backup::create_at(&database, &output)).await??;
            println!("{}", serde_json::to_string_pretty(&manifest)?);
        }
        Command::Restore {
            source,
            acknowledge_control_plane_restore,
        } => {
            if !acknowledge_control_plane_restore {
                anyhow::bail!("restore requires --acknowledge-control-plane-restore and stopped Agents");
            }
            let database = config.database_path.clone();
            tokio::task::spawn_blocking(move || restore(&database, &source)).await??;
        }
    }
    Ok(())
}

fn restore(database: &Path, source: &Path) -> anyhow::Result<()> {
    backup::verify_file(source)?;
    let parent = database
        .parent()
        .ok_or_else(|| anyhow::anyhow!("database path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".lmt-restore-{}.tmp", ulid::Ulid::new()));
    {
        let mut input = std::fs::File::open(source)?;
        let mut output = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        std::io::copy(&mut input, &mut output)?;
        output.flush()?;
        output.sync_all()?;
    }
    backup::verify_path(&temporary)?;
    let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    lmt_store::normalize_restored_database(&temporary, i64::try_from(now)?)?;
    std::fs::File::open(&temporary)?.sync_all()?;
    for suffix in ["-wal", "-shm"] {
        match std::fs::remove_file(format!("{}{suffix}", database.display())) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let archived = database.with_extension(format!("pre-restore-{}", ulid::Ulid::new()));
    let had_database = database.exists();
    if had_database {
        std::fs::rename(database, &archived)?;
    }
    if let Err(error) = std::fs::rename(&temporary, database) {
        if had_database {
            let _ = std::fs::rename(&archived, database);
        }
        return Err(error.into());
    }
    std::fs::File::open(parent)?.sync_all()?;
    println!(
        "restored {}; previous database: {}",
        database.display(),
        archived.display()
    );
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
