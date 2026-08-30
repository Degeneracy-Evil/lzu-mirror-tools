use std::path::{Path, PathBuf};

use anyhow::Context;
use lmt_core::{AttemptState, FailureKind, ProcessRunSpec};
use serde::{Deserialize, Serialize};
use tokio::fs;

#[derive(Clone, Serialize, Deserialize)]
pub struct SpoolRecord {
    pub run_id: String,
    pub attempt: u32,
    pub spec_hash: String,
    pub spec: ProcessRunSpec,
    pub state: AttemptState,
    pub sequence: u64,
    pub acknowledged_sequence: u64,
    pub accepted_at: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub exit_code: Option<i32>,
    pub failure_kind: Option<FailureKind>,
    pub failure_message: Option<String>,
    pub log_offset: u64,
    pub log_complete_acknowledged: bool,
}

impl SpoolRecord {
    pub fn accepted(run_id: String, attempt: u32, spec_hash: String, spec: ProcessRunSpec, now: String) -> Self {
        Self {
            run_id,
            attempt,
            spec_hash,
            spec,
            state: AttemptState::Accepted,
            sequence: 1,
            acknowledged_sequence: 0,
            accepted_at: Some(now),
            started_at: None,
            finished_at: None,
            exit_code: None,
            failure_kind: None,
            failure_message: None,
            log_offset: 0,
            log_complete_acknowledged: false,
        }
    }

    pub fn terminal(
        &mut self,
        state: AttemptState,
        exit_code: Option<i32>,
        kind: Option<FailureKind>,
        message: Option<String>,
        now: String,
    ) {
        self.state = state;
        self.sequence += 1;
        self.finished_at = Some(now);
        self.exit_code = exit_code;
        self.failure_kind = kind;
        self.failure_message = message;
    }

    pub fn ready_for_cleanup(&self) -> bool {
        self.state.is_terminal() && self.acknowledged_sequence >= self.sequence && self.log_complete_acknowledged
    }
}

pub fn state_path(root: &Path, run_id: &str, attempt: u32) -> PathBuf {
    root.join(format!("{run_id}-{attempt}.json"))
}

pub fn log_path(state: &Path) -> PathBuf {
    state.with_extension("log")
}

pub async fn read(path: &Path) -> anyhow::Result<SpoolRecord> {
    Ok(serde_json::from_slice(&fs::read(path).await?)?)
}

pub async fn write(path: &Path, record: &SpoolRecord) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(record)?;
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let temporary = path.with_extension("tmp");
        let mut file = std::fs::File::create(&temporary)?;
        std::io::Write::write_all(&mut file, &bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &path)?;
        std::fs::File::open(path.parent().context("spool path has no parent")?)?.sync_all()?;
        Ok(())
    })
    .await??;
    Ok(())
}

pub async fn retire(path: &Path) -> anyhow::Result<()> {
    let retired = path.with_extension("retired");
    fs::rename(path, &retired).await?;
    if let Some(parent) = path.parent() {
        let parent = parent.to_owned();
        tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all()).await??;
    }
    match fs::remove_file(log_path(path)).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::remove_file(retired).await?;
    if let Some(parent) = path.parent() {
        let parent = parent.to_owned();
        tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all()).await??;
    }
    Ok(())
}
