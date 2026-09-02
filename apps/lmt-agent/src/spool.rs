use std::path::{Path, PathBuf};

use anyhow::Context;
use lmt_core::{AttemptState, FailureKind, ProcessRunSpec};
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::publication_fs::FileIdentity;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PublicationPhase {
    Executing,
    PreparingExchange,
    ReadyToCommit,
    PreVisibilityRecovery,
    VisiblePendingDurability,
    CommittedPendingReport,
    AbandonedFenced,
}

impl PublicationPhase {
    pub const fn is_protected(self) -> bool {
        !matches!(self, Self::Executing)
    }

    pub const fn requires_namespace_recovery(self) -> bool {
        matches!(
            self,
            Self::PreparingExchange
                | Self::ReadyToCommit
                | Self::PreVisibilityRecovery
                | Self::VisiblePendingDurability
                | Self::CommittedPendingReport
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PublicationState {
    pub phase: PublicationPhase,
    pub mirror: String,
    pub publication_root: String,
    pub published_dir: String,
    pub candidate_dir: String,
    pub basis_dir: String,
    pub exchange_dir: String,
    pub gc_dir: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_identity: Option<FileIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_identity: Option<FileIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stable_previous_identity: Option<FileIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exchange_identity: Option<FileIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotated_previous_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotated_previous_identity: Option<FileIdentity>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SpoolRecord {
    pub run_id: String,
    pub attempt: u32,
    pub spec_hash: String,
    pub spec: Option<ProcessRunSpec>,
    #[serde(default)]
    pub cancel_requested: bool,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication: Option<PublicationState>,
}

impl SpoolRecord {
    pub fn accepted(run_id: String, attempt: u32, spec_hash: String, spec: ProcessRunSpec, now: String) -> Self {
        let publication = spec.publication.as_ref().map(|publication| PublicationState {
            phase: PublicationPhase::Executing,
            mirror: publication.mirror.clone(),
            publication_root: publication.publication_root.clone(),
            published_dir: publication.published_dir.clone(),
            candidate_dir: publication.candidate_dir.clone(),
            basis_dir: publication.basis_dir.clone(),
            exchange_dir: publication.exchange_dir.clone(),
            gc_dir: publication.gc_dir.clone(),
            candidate_identity: None,
            published_identity: None,
            stable_previous_identity: None,
            exchange_identity: None,
            rotated_previous_path: None,
            rotated_previous_identity: None,
        });
        Self {
            run_id,
            attempt,
            spec_hash,
            spec: Some(spec),
            cancel_requested: false,
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
            publication,
        }
    }

    pub fn cancellation_tombstone(run_id: String, attempt: u32, spec_hash: String, now: String) -> Self {
        Self {
            run_id,
            attempt,
            spec_hash,
            spec: None,
            cancel_requested: true,
            state: AttemptState::Cancelled,
            sequence: 1,
            acknowledged_sequence: 0,
            accepted_at: None,
            started_at: None,
            finished_at: Some(now),
            exit_code: None,
            failure_kind: None,
            failure_message: None,
            log_offset: 0,
            log_complete_acknowledged: false,
            publication: None,
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
        !self.has_protected_publication_evidence()
            && self.state.is_terminal()
            && self.acknowledged_sequence >= self.sequence
            && self.log_complete_acknowledged
    }

    pub fn has_protected_publication_evidence(&self) -> bool {
        self.publication
            .as_ref()
            .is_some_and(|publication| publication.phase.is_protected())
    }

    pub fn requires_publication_recovery(&self) -> bool {
        self.publication
            .as_ref()
            .is_some_and(|publication| publication.phase.requires_namespace_recovery())
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

#[cfg(test)]
mod tests {
    use super::*;
    use lmt_core::AtomicPublicationSpec;

    fn direct_spec() -> ProcessRunSpec {
        ProcessRunSpec {
            runner: "process".into(),
            program: "/bin/true".into(),
            args: Vec::new(),
            cwd: None,
            timeout_seconds: 60,
            mirror_root: "/srv/mirrors".into(),
            target_dir: "/srv/mirrors/demo".into(),
            publication: None,
        }
    }

    fn atomic_spec() -> ProcessRunSpec {
        let mut spec = direct_spec();
        spec.target_dir = "/srv/publication/.lmt/candidates/demo/run-1-1".into();
        spec.publication = Some(Box::new(AtomicPublicationSpec {
            mirror: "demo".into(),
            publication_root: "/srv/publication".into(),
            published_dir: "/srv/publication/demo".into(),
            candidate_dir: spec.target_dir.clone(),
            basis_dir: "/srv/publication/.lmt/basis/demo".into(),
            exchange_dir: "/srv/publication/.lmt/exchange/demo".into(),
            gc_dir: "/srv/publication/.lmt/gc/demo".into(),
        }));
        spec
    }

    #[test]
    fn accepted_m3_record_remains_readable_and_reencodes_without_m4_field() {
        let record = SpoolRecord::accepted(
            "run-1".into(),
            1,
            "hash".into(),
            direct_spec(),
            "2026-09-03T00:00:00Z".into(),
        );
        let mut value = serde_json::to_value(&record).expect("serialize direct record");
        assert!(value.get("publication").is_none());
        value.as_object_mut().expect("record object").remove("publication");

        let decoded: SpoolRecord = serde_json::from_value(value).expect("decode M3 record");
        assert_eq!(decoded.publication, None);
        assert!(
            serde_json::to_value(decoded)
                .expect("re-encode M3 record")
                .get("publication")
                .is_none()
        );
    }

    #[test]
    fn atomic_acceptance_records_exact_private_namespace_and_protection_phases() {
        let record = SpoolRecord::accepted(
            "run-1".into(),
            1,
            "hash".into(),
            atomic_spec(),
            "2026-09-03T00:00:00Z".into(),
        );
        let publication = record.publication.as_ref().expect("publication state");
        assert_eq!(publication.phase, PublicationPhase::Executing);
        assert_eq!(publication.mirror, "demo");
        assert_eq!(
            publication.candidate_dir,
            "/srv/publication/.lmt/candidates/demo/run-1-1"
        );
        assert!(!record.has_protected_publication_evidence());
        assert!(!record.requires_publication_recovery());

        for phase in [
            PublicationPhase::PreparingExchange,
            PublicationPhase::ReadyToCommit,
            PublicationPhase::PreVisibilityRecovery,
            PublicationPhase::VisiblePendingDurability,
            PublicationPhase::CommittedPendingReport,
        ] {
            assert!(phase.is_protected(), "{phase:?}");
            assert!(phase.requires_namespace_recovery(), "{phase:?}");
        }
        assert!(PublicationPhase::AbandonedFenced.is_protected());
        assert!(!PublicationPhase::AbandonedFenced.requires_namespace_recovery());
    }

    #[tokio::test]
    async fn durable_write_round_trips_protected_phase_and_inode_evidence() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("run-1-1.json");
        let mut record = SpoolRecord::accepted(
            "run-1".into(),
            1,
            "hash".into(),
            atomic_spec(),
            "2026-09-03T00:00:00Z".into(),
        );
        let publication = record.publication.as_mut().expect("publication state");
        publication.phase = PublicationPhase::PreparingExchange;
        publication.candidate_identity = Some(FileIdentity { device: 7, inode: 11 });
        write(&path, &record).await.expect("durable write");

        let reopened = read(&path).await.expect("reopen durable state");
        assert!(reopened.has_protected_publication_evidence());
        assert!(reopened.requires_publication_recovery());
        assert_eq!(
            reopened.publication.expect("publication state").candidate_identity,
            Some(FileIdentity { device: 7, inode: 11 })
        );
    }
}
