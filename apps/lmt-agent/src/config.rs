use std::path::PathBuf;

use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node: Node,
    pub server: Server,
    pub storage: Storage,
    pub execution: Execution,
    pub runner: Runner,
    #[serde(default)]
    pub logging: Option<Logging>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Logging {
    #[serde(default = "logging_level_default")]
    pub level: String,
    #[serde(default = "logging_format_default")]
    pub format: LoggingFormat,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoggingFormat {
    Json,
    Text,
}
fn logging_level_default() -> String {
    "info".into()
}
const fn logging_format_default() -> LoggingFormat {
    LoggingFormat::Json
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    pub name: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Server {
    pub url: String,
    pub token_file: PathBuf,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    pub mirror_root: PathBuf,
    pub spool_dir: PathBuf,
    #[serde(default)]
    pub publication_root: Option<PathBuf>,
    #[serde(default)]
    pub publication_max_private_generations: Option<u32>,
    #[serde(default)]
    pub publication_reserve_bytes: Option<u64>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Execution {
    pub max_concurrent_runs: u32,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Runner {
    pub process: ProcessPolicy,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessPolicy {
    pub enabled: bool,
}
