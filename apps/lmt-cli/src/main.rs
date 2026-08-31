use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use lmt_core::{BundleFile, RunTrigger};
use lmt_protocol::v1alpha1::{ApplyRequest, BundleRequest, ManualRunRequest, PlanResponse};
use reqwest::{Client, Response};
use serde::Serialize;
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    server: String,
    #[arg(long)]
    token_file: PathBuf,
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
        command: ShowCommand,
    },
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
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
enum ShowCommand {
    List,
    Show { name: String },
}
#[derive(Subcommand)]
enum RunCommand {
    List,
    Show {
        id: String,
    },
    Logs {
        id: String,
        #[arg(long, default_value_t = 1)]
        attempt: u32,
    },
    Cancel {
        id: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    let token = fs::read_to_string(&a.token_file)?.trim().to_owned();
    let client = Client::new();
    let base = format!("{}/api/v1alpha1", a.server.trim_end_matches('/'));
    let response = match a.command {
        Command::Config { command } => match command {
            ConfigCommand::Validate { dir } => {
                post(
                    &client,
                    &token,
                    format!("{base}/config/validate"),
                    &BundleRequest { files: bundle(&dir)? },
                )
                .await?
            }
            ConfigCommand::Plan { dir } => {
                post(
                    &client,
                    &token,
                    format!("{base}/config/plan"),
                    &BundleRequest { files: bundle(&dir)? },
                )
                .await?
            }
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
            }
        },
        Command::Mirror { command } => match command {
            MirrorCommand::List => get(&client, &token, format!("{base}/mirrors")).await?,
            MirrorCommand::Show { name } => get(&client, &token, format!("{base}/mirrors/{name}")).await?,
            MirrorCommand::Sync { name } => {
                post(
                    &client,
                    &token,
                    format!("{base}/mirrors/{name}/runs"),
                    &ManualRunRequest {
                        request_id: ulid::Ulid::new().to_string(),
                        trigger: RunTrigger::Manual,
                    },
                )
                .await?
            }
        },
        Command::Node { command } => match command {
            ShowCommand::List => get(&client, &token, format!("{base}/nodes")).await?,
            ShowCommand::Show { name } => get(&client, &token, format!("{base}/nodes/{name}")).await?,
        },
        Command::Run { command } => match command {
            RunCommand::List => get(&client, &token, format!("{base}/runs")).await?,
            RunCommand::Show { id } => get(&client, &token, format!("{base}/runs/{id}")).await?,
            RunCommand::Logs { id, attempt } => {
                get(&client, &token, format!("{base}/runs/{id}/logs?attempt={attempt}")).await?
            }
            RunCommand::Cancel { id } => post_empty(&client, &token, format!("{base}/runs/{id}/cancel")).await?,
        },
    };
    let response = checked(response).await?;
    let bytes = response.bytes().await?;
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&bytes) {
        println!("{}", serde_json::to_string_pretty(&json)?);
    } else {
        print!("{}", String::from_utf8_lossy(&bytes));
    }
    Ok(())
}
async fn get(c: &Client, t: &str, u: String) -> anyhow::Result<Response> {
    Ok(c.get(u).bearer_auth(t).send().await?)
}
async fn post<T: Serialize + ?Sized>(c: &Client, t: &str, u: String, b: &T) -> anyhow::Result<Response> {
    Ok(c.post(u).bearer_auth(t).json(b).send().await?)
}
async fn post_empty(c: &Client, t: &str, u: String) -> anyhow::Result<Response> {
    Ok(c.post(u).bearer_auth(t).send().await?)
}
async fn checked(r: Response) -> anyhow::Result<Response> {
    if r.status().is_success() {
        Ok(r)
    } else {
        let status = r.status();
        let body = r.text().await?;
        bail!("server returned {status}: {body}")
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
