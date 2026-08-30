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
