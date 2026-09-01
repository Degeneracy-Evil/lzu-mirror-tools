mod config;
mod output;

use anyhow::Context;
use clap::{Parser, Subcommand};
use config::{ClientSettings, OutputMode};
use lmt_core::{BundleFile, RunTrigger};
use lmt_protocol::v1alpha1::{
    ApplyRequest, BindingReplaceRequest, BundleRequest, CredentialIssueRequest, CredentialIssueResponse,
    ManualRunRequest, PlanResponse,
};
use reqwest::{Client, Response};
use serde::Serialize;
use std::{
    fs,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

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
            if follow {
                follow_logs(c, token, base, &id, attempt, output).await?;
                Ok(CommandResult::Printed)
            } else {
                let suffix = attempt.map_or_else(String::new, |attempt| format!("?attempt={attempt}"));
                get(c, token, format!("{base}/runs/{id}/logs{suffix}"))
                    .await
                    .map(Into::into)
            }
        }
        RunCommand::Cancel { id } => post_empty(c, token, format!("{base}/runs/{id}/cancel"))
            .await
            .map(Into::into),
    }
}

async fn follow_logs(
    client: &Client,
    token: &str,
    base: &str,
    id: &str,
    attempt: Option<u32>,
    output: OutputMode,
) -> Result<(), CliError> {
    let mut offset = 0_u64;
    let mut collected = Vec::new();
    loop {
        let mut url = reqwest::Url::parse(&format!("{base}/runs/{id}/logs"))
            .map_err(|error| CliError::local(error.to_string()))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("offset", &offset.to_string());
            query.append_pair("limit", "65536");
            query.append_pair("wait", "20s");
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
            .unwrap_or(offset);
        let bytes = response.bytes().await?;
        match output {
            OutputMode::Human => {
                print!("{}", String::from_utf8_lossy(&bytes));
                std::io::stdout().flush()?;
            }
            OutputMode::Json => collected.extend_from_slice(&bytes),
        }
        offset = next;
        if complete {
            break;
        }
    }
    if matches!(output, OutputMode::Json) {
        println!(
            "{}",
            serde_json::to_string_pretty(&String::from_utf8_lossy(&collected))?
        );
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
            write_secret(&token_file, &issued.token)?;
            Ok(CommandResult::Value(serde_json::to_value(issued.credential)?))
        }
    }
}

fn write_secret(path: &Path, token: &str) -> anyhow::Result<()> {
    if path.exists() {
        anyhow::bail!("refusing to overwrite existing token file {}", path.display());
    }
    let temporary = path.with_extension("tmp");
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)?;
    writeln!(file, "{token}")?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
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
        write_secret(&path, "lmt_a_secret").expect("write token");
        assert_eq!(fs::read_to_string(&path).expect("token"), "lmt_a_secret\n");
        assert_eq!(
            fs::metadata(&path).expect("metadata").permissions().mode() & 0o777,
            0o600
        );
        assert!(
            write_secret(&path, "replacement").is_err(),
            "existing secret was overwritten"
        );
    }
}
