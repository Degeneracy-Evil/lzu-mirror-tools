mod config;
mod output;

use anyhow::Context;
use clap::{Parser, Subcommand};
use config::{ClientSettings, OutputMode};
use lmt_core::{BundleFile, RunTrigger};
use lmt_protocol::v1alpha1::{
    ApplyRequest, BindingReplaceRequest, BundleRequest, CredentialIssueRequest, CredentialIssueResponse,
    DoctorResponse, ManualRunRequest, PlanResponse,
};
use reqwest::{Client, Response};
use serde::Serialize;
use std::{
    fs,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

#[derive(Debug)]
enum CommandResult {
    Response(Response),
    Value(serde_json::Value),
    Printed,
}

#[derive(Debug)]
struct CliError {
    exit_code: i32,
    message: String,
}

impl CliError {
    fn local(message: impl Into<String>) -> Self {
        Self {
            exit_code: 2,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl From<anyhow::Error> for CliError {
    fn from(error: anyhow::Error) -> Self {
        Self::local(error.to_string())
    }
}
impl From<std::io::Error> for CliError {
    fn from(error: std::io::Error) -> Self {
        Self::local(error.to_string())
    }
}
impl From<reqwest::Error> for CliError {
    fn from(error: reqwest::Error) -> Self {
        Self {
            exit_code: 6,
            message: error.to_string(),
        }
    }
}
impl From<serde_json::Error> for CliError {
    fn from(error: serde_json::Error) -> Self {
        Self {
            exit_code: 7,
            message: error.to_string(),
        }
    }
}

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    server: Option<String>,
    #[arg(long)]
    token_file: Option<PathBuf>,
    #[arg(long, value_enum)]
    output: Option<OutputMode>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    Mirror {
        #[command(subcommand)]
        command: MirrorCommand,
    },
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
    Maintenance {
        #[command(subcommand)]
        command: MaintenanceCommand,
    },
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },
    Status,
    Doctor,
}
#[derive(Subcommand)]
enum BackupCommand {
    Create,
    List,
    Verify { id: String },
}
#[derive(Subcommand)]
enum MaintenanceCommand {
    Logs {
        #[command(subcommand)]
        command: LogMaintenanceCommand,
    },
}
#[derive(Subcommand)]
enum LogMaintenanceCommand {
    Plan,
    Run,
}
#[derive(Subcommand)]
enum ConfigCommand {
    Validate {
        dir: PathBuf,
    },
    Plan {
        dir: PathBuf,
    },
    Apply {
        dir: PathBuf,
        #[arg(long)]
        acknowledge_moves: bool,
    },
}
#[derive(Subcommand)]
enum MirrorCommand {
    List,
    Show { name: String },
    Sync { name: String },
}
#[derive(Subcommand)]
enum NodeCommand {
    List,
    Show {
        name: String,
    },
    Binding {
        #[command(subcommand)]
        command: BindingCommand,
    },
    Credential {
        #[command(subcommand)]
        command: CredentialCommand,
    },
}
#[derive(Subcommand)]
enum BindingCommand {
    Show {
        name: String,
    },
    Replace {
        name: String,
        #[arg(long)]
        agent_id: String,
        #[arg(long)]
        acknowledge_execution_risk: bool,
    },
}
#[derive(Subcommand)]
enum CredentialCommand {
    Issue {
        name: String,
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        token_file: PathBuf,
    },
    List {
        name: String,
    },
    Revoke {
        name: String,
        id: String,
    },
}
#[derive(Subcommand)]
enum RunCommand {
    List {
        #[arg(long)]
        mirror: Option<String>,
        #[arg(long)]
        node: Option<String>,
        #[arg(long)]
        state: Option<String>,
        #[arg(long)]
        trigger: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        before: Option<String>,
    },
    Show {
        id: String,
    },
    Logs {
        id: String,
        #[arg(long)]
        attempt: Option<u32>,
        #[arg(long)]
        follow: bool,
    },
    Cancel {
        id: String,
    },
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(error.exit_code);
    }
}

async fn run() -> Result<(), CliError> {
    let a = Args::parse();
    let settings = ClientSettings::resolve(a.config, a.server, a.token_file, a.output).map_err(CliError::local)?;
    let token = fs::read_to_string(&settings.token_file)?.trim().to_owned();
    let client = Client::new();
    let base = format!("{}/api/v1alpha1", settings.server.trim_end_matches('/'));
    let result = match a.command {
        Command::Config { command } => match command {
            ConfigCommand::Validate { dir } => post(
                &client,
                &token,
                format!("{base}/config/validate"),
                &BundleRequest { files: bundle(&dir)? },
            )
            .await?
            .into(),
            ConfigCommand::Plan { dir } => post(
                &client,
                &token,
                format!("{base}/config/plan"),
                &BundleRequest { files: bundle(&dir)? },
            )
            .await?
            .into(),
            ConfigCommand::Apply { dir, acknowledge_moves } => {
                let files = bundle(&dir)?;
                let plan: PlanResponse = checked(
                    post(
                        &client,
                        &token,
                        format!("{base}/config/plan"),
                        &BundleRequest { files: files.clone() },
                    )
                    .await?,
                )
                .await?
                .json()
                .await?;
                post(
                    &client,
                    &token,
                    format!("{base}/config/apply"),
                    &ApplyRequest {
                        files,
                        base_revision: plan.base_revision,
                        acknowledge_moves,
                    },
                )
                .await?
                .into()
            }
        },
        Command::Mirror { command } => match command {
            MirrorCommand::List => get(&client, &token, format!("{base}/mirrors")).await?.into(),
            MirrorCommand::Show { name } => get(&client, &token, format!("{base}/mirrors/{name}")).await?.into(),
            MirrorCommand::Sync { name } => post(
                &client,
                &token,
                format!("{base}/mirrors/{name}/runs"),
                &ManualRunRequest {
                    request_id: ulid::Ulid::new().to_string(),
                    trigger: RunTrigger::Manual,
                },
            )
            .await?
            .into(),
        },
        Command::Node { command } => execute_node(&client, &token, &base, command).await?,
        Command::Run { command } => execute_run(&client, &token, &base, command, settings.output).await?,
        Command::Maintenance {
            command: MaintenanceCommand::Logs { command },
        } => match command {
            LogMaintenanceCommand::Plan => get(&client, &token, format!("{base}/maintenance/logs/plan"))
                .await?
                .into(),
            LogMaintenanceCommand::Run => post_empty(&client, &token, format!("{base}/maintenance/logs/run"))
                .await?
                .into(),
        },
        Command::Backup { command } => execute_backup(&client, &token, &base, command).await?,
        Command::Status => get(&client, &token, format!("{base}/status")).await?.into(),
        Command::Doctor => execute_doctor(&client, &token, &base, settings.output).await?,
    };
    let bytes = match result {
        CommandResult::Response(response) => checked(response).await?.bytes().await?.to_vec(),
        CommandResult::Value(value) => serde_json::to_vec(&value)?,
        CommandResult::Printed => return Ok(()),
    };
    let rendered = output::render(&bytes, settings.output)?;
    if !rendered.is_empty() {
        println!("{rendered}");
    }
    Ok(())
}
impl From<Response> for CommandResult {
    fn from(response: Response) -> Self {
        Self::Response(response)
    }
}

fn doctor_exit_code(response: &DoctorResponse) -> i32 {
    if response.healthy { 0 } else { 8 }
}

async fn execute_backup(
    c: &Client,
    token: &str,
    base: &str,
    command: BackupCommand,
) -> Result<CommandResult, CliError> {
    match command {
        BackupCommand::Create => post_empty(c, token, format!("{base}/backups")).await.map(Into::into),
        BackupCommand::List => get(c, token, format!("{base}/backups")).await.map(Into::into),
        BackupCommand::Verify { id } => post_empty(c, token, format!("{base}/backups/{id}/verify"))
            .await
            .map(Into::into),
    }
}

async fn execute_doctor(
    client: &Client,
    token: &str,
    base: &str,
    output_mode: OutputMode,
) -> Result<CommandResult, CliError> {
    let bytes = checked(get(client, token, format!("{base}/doctor")).await?)
        .await?
        .bytes()
        .await?;
    let diagnostic: DoctorResponse = serde_json::from_slice(&bytes)?;
    let rendered = output::render(&bytes, output_mode)?;
    if !rendered.is_empty() {
        println!("{rendered}");
    }
    if doctor_exit_code(&diagnostic) != 0 {
        return Err(CliError {
            exit_code: doctor_exit_code(&diagnostic),
            message: "doctor found unhealthy conditions".into(),
        });
    }
    Ok(CommandResult::Printed)
}
async fn execute_run(
    c: &Client,
    token: &str,
    base: &str,
    command: RunCommand,
    output: OutputMode,
) -> Result<CommandResult, CliError> {
    match command {
        RunCommand::List {
            mirror,
            node,
            state,
            trigger,
            limit,
            before,
        } => {
            let mut url =
                reqwest::Url::parse(&format!("{base}/runs")).map_err(|error| CliError::local(error.to_string()))?;
            {
                let mut query = url.query_pairs_mut();
                for (key, value) in [
                    ("mirror", mirror),
                    ("node", node),
                    ("state", state),
                    ("trigger", trigger),
                    ("limit", limit.map(|value| value.to_string())),
                    ("before", before),
                ] {
                    if let Some(value) = value {
                        query.append_pair(key, &value);
                    }
                }
            }
            get(c, token, url.to_string()).await.map(Into::into)
        }
        RunCommand::Show { id } => get(c, token, format!("{base}/runs/{id}")).await.map(Into::into),
        RunCommand::Logs { id, attempt, follow } => {
            stream_logs(c, token, base, &id, attempt, follow, output).await?;
            Ok(CommandResult::Printed)
        }
        RunCommand::Cancel { id } => post_empty(c, token, format!("{base}/runs/{id}/cancel"))
            .await
            .map(Into::into),
    }
}

async fn stream_logs(
    client: &Client,
    token: &str,
    base: &str,
    id: &str,
    attempt: Option<u32>,
    follow: bool,
    output: OutputMode,
) -> Result<(), CliError> {
    read_log_chunks(
        client,
        token,
        base,
        id,
        attempt,
        follow,
        |offset, next, complete, bytes| {
            let mut stdout = std::io::stdout().lock();
            match output {
                OutputMode::Human => stdout.write_all(bytes)?,
                OutputMode::Json => {
                    serde_json::to_writer(
                        &mut stdout,
                        &serde_json::json!({
                            "offset": offset,
                            "next_offset": next,
                            "complete": complete,
                            "data": String::from_utf8_lossy(bytes),
                        }),
                    )?;
                    stdout.write_all(b"\n")?;
                }
            }
            stdout.flush()?;
            Ok(())
        },
    )
    .await
}

async fn read_log_chunks<F>(
    client: &Client,
    token: &str,
    base: &str,
    id: &str,
    attempt: Option<u32>,
    follow: bool,
    mut emit: F,
) -> Result<(), CliError>
where
    F: FnMut(u64, u64, bool, &[u8]) -> Result<(), CliError>,
{
    let mut offset = 0_u64;
    loop {
        let mut url = reqwest::Url::parse(&format!("{base}/runs/{id}/logs"))
            .map_err(|error| CliError::local(error.to_string()))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("offset", &offset.to_string());
            query.append_pair("limit", "65536");
            if follow {
                query.append_pair("wait", "20s");
            }
            if let Some(attempt) = attempt {
                query.append_pair("attempt", &attempt.to_string());
            }
        }
        let response = checked(get(client, token, url.to_string()).await?).await?;
        let complete = response
            .headers()
            .get("x-lmt-log-complete")
            .and_then(|value| value.to_str().ok())
            == Some("true");
        let next = response
            .headers()
            .get("x-lmt-log-next-offset")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| CliError::local("log response omitted a valid next offset"))?;
        let bytes = response.bytes().await?;
        if next != offset.saturating_add(bytes.len() as u64) {
            return Err(CliError::local("log response offset did not match its chunk length"));
        }
        emit(offset, next, complete, &bytes)?;
        offset = next;
        if complete {
            break;
        }
        if !follow && bytes.is_empty() {
            break;
        }
    }
    Ok(())
}
async fn execute_node(c: &Client, token: &str, base: &str, command: NodeCommand) -> Result<CommandResult, CliError> {
    match command {
        NodeCommand::List => get(c, token, format!("{base}/nodes")).await.map(Into::into),
        NodeCommand::Show { name }
        | NodeCommand::Binding {
            command: BindingCommand::Show { name },
        } => get(c, token, format!("{base}/nodes/{name}")).await.map(Into::into),
        NodeCommand::Binding {
            command:
                BindingCommand::Replace {
                    name,
                    agent_id,
                    acknowledge_execution_risk,
                },
        } => post(
            c,
            token,
            format!("{base}/nodes/{name}/binding"),
            &BindingReplaceRequest {
                agent_id,
                acknowledge_execution_risk,
            },
        )
        .await
        .map(Into::into),
        NodeCommand::Credential {
            command: CredentialCommand::List { name },
        } => get(c, token, format!("{base}/nodes/{name}/credentials"))
            .await
            .map(Into::into),
        NodeCommand::Credential {
            command: CredentialCommand::Revoke { name, id },
        } => post_empty(c, token, format!("{base}/nodes/{name}/credentials/{id}/revoke"))
            .await
            .map(Into::into),
        NodeCommand::Credential {
            command:
                CredentialCommand::Issue {
                    name,
                    label,
                    token_file,
                },
        } => {
            let pending_secret = PendingSecretFile::prepare(&token_file)?;
            let issued: CredentialIssueResponse = checked(
                post(
                    c,
                    token,
                    format!("{base}/nodes/{name}/credentials"),
                    &CredentialIssueRequest { label },
                )
                .await?,
            )
            .await?
            .json()
            .await?;
            publish_credential_or_revoke(c, token, base, &name, pending_secret, issued).await
        }
    }
}

struct PendingSecretFile {
    final_path: PathBuf,
    temporary_path: PathBuf,
    file: Option<fs::File>,
}

impl PendingSecretFile {
    fn prepare(path: &Path) -> anyhow::Result<Self> {
        if path.exists() {
            anyhow::bail!("refusing to overwrite existing token file {}", path.display());
        }
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("token file has no parent"))?;
        if !parent.is_dir() {
            anyhow::bail!(
                "token file parent does not exist or is not a directory: {}",
                parent.display()
            );
        }
        let temporary_path = parent.join(format!(".lmt-token-{}.tmp", ulid::Ulid::new()));
        let file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary_path)?;
        Ok(Self {
            final_path: path.to_owned(),
            temporary_path,
            file: Some(file),
        })
    }

    fn publish(mut self, token: &str) -> anyhow::Result<()> {
        let mut file = self.file.take().expect("pending secret owns file");
        writeln!(file, "{token}")?;
        file.sync_all()?;
        drop(file);
        fs::hard_link(&self.temporary_path, &self.final_path)?;
        fs::remove_file(&self.temporary_path)?;
        if let Some(parent) = self.final_path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}

impl Drop for PendingSecretFile {
    fn drop(&mut self) {
        let _ = self.file.take();
        let _ = fs::remove_file(&self.temporary_path);
    }
}

async fn publish_credential_or_revoke(
    client: &Client,
    operator_token: &str,
    base: &str,
    node: &str,
    pending: PendingSecretFile,
    issued: CredentialIssueResponse,
) -> Result<CommandResult, CliError> {
    let credential_id = issued.credential.id.clone();
    if let Err(local_error) = pending.publish(&issued.token) {
        let revoke_url = format!("{base}/nodes/{node}/credentials/{credential_id}/revoke");
        let revoked = match post_empty(client, operator_token, revoke_url).await {
            Ok(response) => checked(response).await.is_ok(),
            Err(_) => false,
        };
        let cleanup = if revoked {
            "the newly issued credential was revoked"
        } else {
            "cleanup revocation could not be confirmed; manually revoke this credential ID"
        };
        return Err(CliError::local(format!(
            "failed to publish credential {credential_id} locally: {local_error}; {cleanup}: {credential_id}"
        )));
    }
    Ok(CommandResult::Value(serde_json::to_value(issued.credential)?))
}

async fn get(c: &Client, t: &str, u: String) -> Result<Response, CliError> {
    Ok(c.get(u).bearer_auth(t).send().await?)
}
async fn post<T: Serialize + ?Sized>(c: &Client, t: &str, u: String, b: &T) -> Result<Response, CliError> {
    Ok(c.post(u).bearer_auth(t).json(b).send().await?)
}
async fn post_empty(c: &Client, t: &str, u: String) -> Result<Response, CliError> {
    Ok(c.post(u).bearer_auth(t).send().await?)
}
async fn checked(r: Response) -> Result<Response, CliError> {
    if r.status().is_success() {
        Ok(r)
    } else {
        let status = r.status();
        let body = r.text().await?;
        Err(CliError {
            exit_code: exit_code_for_status(status),
            message: format!("server returned {status}: {body}"),
        })
    }
}
fn exit_code_for_status(status: reqwest::StatusCode) -> i32 {
    match status.as_u16() {
        401 | 403 => 3,
        404 | 410 => 4,
        409 | 412 => 5,
        _ => 7,
    }
}
fn bundle(root: &Path) -> anyhow::Result<Vec<BundleFile>> {
    let mut files = vec![];
    walk(root, root, &mut files)?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}
fn walk(root: &Path, dir: &Path, out: &mut Vec<BundleFile>) -> anyhow::Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            walk(root, &path, out)?;
        } else if path.extension().and_then(|x| x.to_str()) == Some("toml") {
            out.push(BundleFile {
                path: path.strip_prefix(root)?.to_string_lossy().replace('\\', "/"),
                contents: fs::read_to_string(&path)?,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn documented_http_exit_categories_are_stable() {
        assert_eq!(exit_code_for_status(reqwest::StatusCode::UNAUTHORIZED), 3);
        assert_eq!(exit_code_for_status(reqwest::StatusCode::NOT_FOUND), 4);
        assert_eq!(exit_code_for_status(reqwest::StatusCode::CONFLICT), 5);
        assert_eq!(exit_code_for_status(reqwest::StatusCode::SERVICE_UNAVAILABLE), 7);
    }

    #[test]
    fn issued_token_file_is_created_atomically_with_restrictive_mode() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("agent.token");
        fs::write(path.with_extension("tmp"), "stale crash artifact").expect("stale temp");
        PendingSecretFile::prepare(&path)
            .expect("preflight")
            .publish("lmt_a_secret")
            .expect("write token");
        assert_eq!(fs::read_to_string(&path).expect("token"), "lmt_a_secret\n");
        assert_eq!(
            fs::metadata(&path).expect("metadata").permissions().mode() & 0o777,
            0o600
        );
        assert!(
            PendingSecretFile::prepare(&path).is_err(),
            "existing secret was overwritten"
        );
    }

    fn issued_response() -> CredentialIssueResponse {
        CredentialIssueResponse {
            credential: lmt_protocol::v1alpha1::CredentialView {
                id: "credential-a".into(),
                node: "node-a".into(),
                label: None,
                created_at: "2026-09-01T00:00:00Z".into(),
                last_used_at: None,
                revoked_at: None,
            },
            token: "lmt_a_raw_secret_must_not_be_printed".into(),
        }
    }

    async fn revoke_server(status: reqwest::StatusCode) -> (String, std::sync::Arc<std::sync::atomic::AtomicU64>) {
        use axum::{Router, http::StatusCode, routing::post};
        use std::sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        };

        let calls = Arc::new(AtomicU64::new(0));
        let observed = calls.clone();
        let app = Router::new().route(
            "/api/v1alpha1/nodes/node-a/credentials/credential-a/revoke",
            post(move || {
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    StatusCode::from_u16(status.as_u16()).expect("status")
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let base = format!("http://{}/api/v1alpha1", listener.local_addr().expect("address"));
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        (base, calls)
    }

    #[tokio::test]
    async fn failed_local_credential_publication_revokes_or_gives_manual_cleanup_id() {
        use std::sync::atomic::Ordering;

        for (status, cleanup_confirmed) in [
            (reqwest::StatusCode::OK, true),
            (reqwest::StatusCode::INTERNAL_SERVER_ERROR, false),
        ] {
            let directory = tempfile::tempdir().expect("tempdir");
            let path = directory.path().join("agent.token");
            let pending = PendingSecretFile::prepare(&path).expect("preflight");
            fs::write(&path, "racing existing file").expect("inject publication failure");
            let (base, calls) = revoke_server(status).await;
            let error =
                publish_credential_or_revoke(&Client::new(), "operator", &base, "node-a", pending, issued_response())
                    .await
                    .expect_err("local publication failure");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(error.message.contains("credential-a"));
            assert!(!error.message.contains("raw_secret"));
            assert_eq!(error.message.contains("manually revoke"), !cleanup_confirmed);
            assert_eq!(fs::read_to_string(path).expect("existing file"), "racing existing file");
        }
    }

    #[tokio::test]
    async fn completed_logs_stream_every_chunk_with_bounded_callback_memory() {
        use axum::{
            Router,
            body::Bytes,
            extract::{Query, State},
            http::{HeaderMap, HeaderValue},
            routing::get,
        };
        use serde::Deserialize;
        use std::sync::Arc;

        #[derive(Deserialize)]
        struct ChunkQuery {
            offset: u64,
            limit: usize,
        }
        async fn chunk(State(log): State<Arc<Vec<u8>>>, Query(query): Query<ChunkQuery>) -> (HeaderMap, Bytes) {
            let start = usize::try_from(query.offset).expect("offset").min(log.len());
            let end = start.saturating_add(query.limit).min(log.len());
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-lmt-log-next-offset",
                HeaderValue::from_str(&end.to_string()).expect("next offset"),
            );
            headers.insert(
                "x-lmt-log-complete",
                HeaderValue::from_static(if end == log.len() { "true" } else { "false" }),
            );
            (headers, Bytes::copy_from_slice(&log[start..end]))
        }

        let log = Arc::new(vec![b'x'; 1024 * 1024 + 17]);
        let expected = log.len();
        let app = Router::new()
            .route("/api/v1alpha1/runs/run-a/logs", get(chunk))
            .with_state(log);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let base = format!("http://{}/api/v1alpha1", listener.local_addr().expect("address"));
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        let mut total = 0_usize;
        let mut max_chunk = 0_usize;
        let mut chunks = 0_u32;
        read_log_chunks(
            &Client::new(),
            "operator",
            &base,
            "run-a",
            None,
            false,
            |_, _, _, bytes| {
                total += bytes.len();
                max_chunk = max_chunk.max(bytes.len());
                chunks += 1;
                Ok(())
            },
        )
        .await
        .expect("stream complete log");
        assert_eq!(total, expected);
        assert!(chunks > 1);
        assert!(max_chunk <= 65_536);
    }

    #[test]
    fn unhealthy_doctor_has_documented_exit_code() {
        assert_eq!(
            doctor_exit_code(&DoctorResponse {
                healthy: false,
                checks: vec![],
            }),
            8
        );
        assert_eq!(
            doctor_exit_code(&DoctorResponse {
                healthy: true,
                checks: vec![],
            }),
            0
        );
    }

    #[test]
    fn json_output_is_machine_readable_and_human_output_is_compact() {
        let input = br#"{"runs_pending":2,"healthy":true}"#;
        let json = output::render(input, OutputMode::Json).expect("JSON rendering");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&json).expect("machine JSON")["runs_pending"],
            2
        );
        let human = output::render(input, OutputMode::Human).expect("human rendering");
        assert!(human.contains("runs_pending\t2"));
        assert!(human.contains("healthy\ttrue"));
    }
}
