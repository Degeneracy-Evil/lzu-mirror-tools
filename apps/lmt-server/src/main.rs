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
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
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
    let archived = restore_with_installer(database, source, |from, to| std::fs::rename(from, to))?;
    println!(
        "restored {}; previous database: {}",
        database.display(),
        archived.map_or_else(|| "none".into(), |path| path.display().to_string())
    );
    Ok(())
}

fn sqlite_sidecar(database: &Path, suffix: &str) -> PathBuf {
    let mut path = database.as_os_str().to_owned();
    path.push(suffix);
    path.into()
}

fn remove_if_present(path: &Path) -> anyhow::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn checkpoint_current_database(database: &Path) -> anyhow::Result<()> {
    if !database.exists() {
        return Ok(());
    }
    let connection = rusqlite::Connection::open(database)?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    let (busy, frames, checkpointed): (u32, u32, u32) =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    if busy != 0 || checkpointed != frames {
        anyhow::bail!(
            "restore_requires_offline: current SQLite WAL checkpoint was incomplete ({checkpointed}/{frames}, busy={busy})"
        );
    }
    drop(connection);
    std::fs::File::open(database)?.sync_all()?;
    if let Some(parent) = database.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn restore_with_installer<F>(database: &Path, source: &Path, installer: F) -> anyhow::Result<Option<PathBuf>>
where
    F: FnOnce(&Path, &Path) -> std::io::Result<()>,
{
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
        remove_if_present(&sqlite_sidecar(&temporary, suffix))?;
    }
    checkpoint_current_database(database)?;
    let archived = database.with_extension(format!("pre-restore-{}", ulid::Ulid::new()));
    let had_database = database.exists();
    if had_database {
        std::fs::rename(database, &archived)?;
        std::fs::File::open(parent)?.sync_all()?;
    }
    let installation = (|| -> anyhow::Result<()> {
        for suffix in ["-wal", "-shm"] {
            remove_if_present(&sqlite_sidecar(database, suffix))?;
        }
        installer(&temporary, database)?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if let Err(error) = installation {
        if database.exists() {
            let failed = database.with_extension(format!("failed-restore-{}", ulid::Ulid::new()));
            std::fs::rename(database, failed)?;
        }
        if had_database {
            std::fs::rename(&archived, database)?;
        }
        std::fs::File::open(parent)?.sync_all()?;
        let _ = remove_if_present(&temporary);
        return Err(error);
    }
    Ok(had_database.then_some(archived))
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

#[cfg(test)]
mod tests {
    use super::*;
    use lmt_core::RunState;
    use lmt_store::Store;

    #[test]
    fn crash_wal_writer_helper() {
        let Ok(path) = std::env::var("LMT_TEST_CRASH_WAL_DB") else {
            return;
        };
        let connection = rusqlite::Connection::open(path).expect("crash writer database");
        connection.pragma_update(None, "journal_mode", "WAL").expect("WAL");
        connection
            .pragma_update(None, "wal_autocheckpoint", 0)
            .expect("disable checkpoint");
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS restore_probe(value TEXT NOT NULL) STRICT;
                 INSERT INTO restore_probe(value) VALUES('committed-in-old-wal');",
            )
            .expect("committed WAL state");
        assert!(
            sqlite_sidecar(
                Path::new(&std::env::var("LMT_TEST_CRASH_WAL_DB").expect("path")),
                "-wal"
            )
            .exists()
        );
        std::process::exit(0);
    }

    async fn seed_active_snapshot(path: &Path) {
        drop(Store::open(path).await.expect("snapshot schema"));
        rusqlite::Connection::open(path)
            .expect("snapshot seed")
            .execute_batch(
                "INSERT INTO config_revisions(revision,bundle_hash,applied_at_ms,summary_json) VALUES(1,'h',1,'{}');
                 INSERT INTO nodes(name,registered_at_ms,active_runs,capabilities_json) VALUES('node-a',1,1,'{}');
                 INSERT INTO mirrors(name,managed,enabled,owner_node,current_generation) VALUES('demo',1,1,'node-a',1);
                 INSERT INTO mirror_generations(mirror_name,generation,config_revision,owner_node,config_hash,config_toml,created_at_ms)
                   VALUES('demo',1,1,'node-a','h','x',1);
                 INSERT INTO runs(id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,started_at_ms,max_attempts,retry_delay_ms,attempt_count)
                   VALUES('run-active','demo',1,'node-a','manual','running',1,2,1,0,1);
                 INSERT INTO attempts(run_id,attempt_no,state,spec_hash,spec_json,created_at_ms,started_at_ms,dispatch_count)
                   VALUES('run-active',1,'running','hash','{}',1,2,1);",
            )
            .expect("active snapshot state");
    }

    fn test_config(database: PathBuf, log_dir: PathBuf) -> ServerConfig {
        ServerConfig {
            bind: "127.0.0.1:0".into(),
            database_path: database,
            log_dir,
            operator_token: Some("operator".into()),
            operator_token_file: None,
            offline_after_seconds: 90,
            agents: vec![],
            run_logs: None,
            backup: None,
            status: None,
            logging: None,
        }
    }

    #[tokio::test]
    async fn real_offline_restore_preserves_wal_rolls_back_failure_and_cannot_redeliver() {
        let directory = tempfile::tempdir().expect("tempdir");
        let source = directory.path().join("snapshot-source.db");
        seed_active_snapshot(&source).await;
        let backup_file = directory.path().join("restore.sqlite");
        backup::create_at(&source, &backup_file).expect("verified backup");

        let control = directory.path().join("control");
        std::fs::create_dir_all(&control).expect("control directory");
        let database = control.join("lmt.db");
        drop(Store::open(&database).await.expect("old database"));
        let child = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .arg("--exact")
            .arg("tests::crash_wal_writer_helper")
            .env("LMT_TEST_CRASH_WAL_DB", &database)
            .status()
            .expect("crash writer process");
        assert!(child.success());
        assert!(sqlite_sidecar(&database, "-wal").exists(), "crash WAL fixture missing");

        let injected = restore_with_installer(&database, &backup_file, |_, _| {
            Err(std::io::Error::other("injected restore installation failure"))
        });
        assert!(injected.is_err());
        assert_eq!(
            rusqlite::Connection::open(&database)
                .expect("rolled back database")
                .query_row("SELECT value FROM restore_probe", [], |row| row.get::<_, String>(0))
                .expect("WAL state survived rollback"),
            "committed-in-old-wal"
        );

        let config = test_config(database.clone(), control.join("logs"));
        let held = acquire_server_lock(&config).expect("held Server lock");
        assert!(
            maintenance(
                &config,
                Command::Restore {
                    source: backup_file.clone(),
                    acknowledge_control_plane_restore: true,
                },
            )
            .await
            .is_err()
        );
        drop(held);
        maintenance(
            &config,
            Command::Restore {
                source: backup_file,
                acknowledge_control_plane_restore: true,
            },
        )
        .await
        .expect("actual offline maintenance restore");
        assert!(
            !sqlite_sidecar(&database, "-wal").exists(),
            "stale WAL paired with restored main DB"
        );

        let restored = Store::open(&database).await.expect("restored Store");
        assert_eq!(
            restored
                .get_run("run-active")
                .await
                .expect("Run query")
                .expect("restored Run")
                .state,
            RunState::Failed
        );
        assert!(
            restored
                .poll_action("node-a", i64::MAX, |_| unreachable!("restored stale work compiled"))
                .await
                .expect("poll after restore")
                .is_none()
        );
        let archived = std::fs::read_dir(&control)
            .expect("control listing")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.to_string_lossy().contains("pre-restore"))
            .expect("archived previous database");
        assert_eq!(
            rusqlite::Connection::open(archived)
                .expect("archived database")
                .query_row("SELECT value FROM restore_probe", [], |row| row.get::<_, String>(0))
                .expect("archived WAL state"),
            "committed-in-old-wal"
        );
    }
}
