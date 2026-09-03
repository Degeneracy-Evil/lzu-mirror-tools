use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex, Weak},
};

use anyhow::{Context, bail};
use lmt_core::{AttemptState, FailureKind};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::{
    now, publication_fs,
    spool::{PublicationPhase, PublicationState, SpoolRecord, write},
};

#[derive(Clone, Default)]
pub struct MirrorLocks {
    entries: Arc<StdMutex<HashMap<String, Weak<Mutex<()>>>>>,
}

impl MirrorLocks {
    pub async fn acquire(&self, mirror: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut entries = self.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            entries.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = entries.get(mirror).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                entries.insert(mirror.to_owned(), Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }
}

pub async fn prepare_candidate(record: &SpoolRecord, locks: &MirrorLocks) -> anyhow::Result<()> {
    let publication = record
        .publication
        .as_ref()
        .context("Atomic record has no publication state")?;
    let mirror = publication.mirror.clone();
    let candidate = PathBuf::from(&publication.candidate_dir);
    let basis = PathBuf::from(&publication.basis_dir);
    let published = PathBuf::from(&publication.published_dir);
    let _guard = locks.acquire(&mirror).await;
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        use std::os::unix::fs::symlink;

        if publication_fs::identity_if_exists(&candidate)?.is_some()
            || publication_fs::identity_if_exists(&basis)?.is_some()
        {
            bail!("fresh Atomic Attempt candidate or basis already exists");
        }
        let attempt = candidate.parent().context("candidate has no Attempt directory")?;
        if basis.parent() != Some(attempt) {
            bail!("candidate and basis do not share one Attempt directory");
        }
        std::fs::create_dir_all(attempt).context("create Atomic Attempt directory")?;
        std::fs::create_dir(&candidate).context("create fresh Atomic candidate")?;
        if publication_fs::identity_if_exists(&published)?.is_some() {
            publication_fs::validate_published_target(&published)?;
            symlink(&published, &basis).context("create immutable rsync basis reference")?;
        } else {
            std::fs::create_dir(&basis).context("create empty first-publication basis")?;
            publication_fs::fsync_directory(&basis)?;
        }
        publication_fs::fsync_directory(&candidate)?;
        publication_fs::fsync_directory(attempt)?;
        if let Some(parent) = attempt.parent() {
            publication_fs::fsync_directory(parent)?;
        }
        Ok(())
    })
    .await??;
    Ok(())
}

pub async fn commit(path: &Path, record: &mut SpoolRecord, locks: &MirrorLocks) -> anyhow::Result<()> {
    let mirror = record
        .publication
        .as_ref()
        .context("Atomic record has no publication state")?
        .mirror
        .clone();
    let _guard = locks.acquire(&mirror).await;
    prepare(path, record).await?;
    commit_ready(path, record).await
}

pub async fn recover(path: &Path, record: &mut SpoolRecord, locks: &MirrorLocks) -> anyhow::Result<()> {
    let mirror = record
        .publication
        .as_ref()
        .context("publication recovery record has no publication state")?
        .mirror
        .clone();
    let _guard = locks.acquire(&mirror).await;
    match record.publication.as_ref().map(|state| state.phase) {
        Some(PublicationPhase::PreparingExchange) => {
            if let Err(error) = prepare_namespace(path, record).await {
                return restore_then_terminal(
                    path,
                    record,
                    AttemptState::Failed,
                    Some(FailureKind::InvalidResult),
                    error.to_string(),
                )
                .await;
            }
            commit_ready(path, record).await
        }
        Some(PublicationPhase::ReadyToCommit) => recover_ready(path, record).await,
        Some(PublicationPhase::PreVisibilityRecovery) => {
            let publication = record.publication.as_ref().context("publication state missing")?;
            let terminal_state = publication
                .pre_visibility_terminal_state
                .unwrap_or(AttemptState::Failed);
            let failure_kind = publication
                .pre_visibility_failure_kind
                .or(Some(FailureKind::InvalidResult));
            let message = publication
                .pre_visibility_message
                .clone()
                .unwrap_or_else(|| "publication private namespace recovered after an earlier failure".into());
            restore_then_terminal(path, record, terminal_state, failure_kind, message).await
        }
        Some(PublicationPhase::VisiblePendingDurability) => finish_visible(path, record).await,
        Some(PublicationPhase::CommittedPendingReport) => ensure_committed_terminal(path, record).await,
        Some(PublicationPhase::Executing | PublicationPhase::AbandonedFenced) | None => Ok(()),
    }
}

pub async fn recover_before_control_plane(
    path: &Path,
    record: &mut SpoolRecord,
    locks: &MirrorLocks,
) -> anyhow::Result<()> {
    let mirror = record
        .publication
        .as_ref()
        .context("publication recovery record has no publication state")?
        .mirror
        .clone();
    let _guard = locks.acquire(&mirror).await;
    match record.publication.as_ref().map(|state| state.phase) {
        Some(PublicationPhase::PreparingExchange) => {
            if let Err(error) = prepare_namespace(path, record).await {
                return restore_then_terminal(
                    path,
                    record,
                    AttemptState::Failed,
                    Some(FailureKind::InvalidResult),
                    error.to_string(),
                )
                .await;
            }
            Ok(())
        }
        Some(PublicationPhase::PreVisibilityRecovery) => {
            let publication = record.publication.as_ref().context("publication state missing")?;
            restore_then_terminal(
                path,
                record,
                publication
                    .pre_visibility_terminal_state
                    .unwrap_or(AttemptState::Failed),
                publication
                    .pre_visibility_failure_kind
                    .or(Some(FailureKind::InvalidResult)),
                publication
                    .pre_visibility_message
                    .clone()
                    .unwrap_or_else(|| "publication private namespace recovered after an earlier failure".into()),
            )
            .await
        }
        Some(PublicationPhase::VisiblePendingDurability) => finish_visible(path, record).await,
        Some(PublicationPhase::CommittedPendingReport) => ensure_committed_terminal(path, record).await,
        Some(PublicationPhase::Executing | PublicationPhase::ReadyToCommit | PublicationPhase::AbandonedFenced)
        | None => Ok(()),
    }
}

async fn prepare(path: &Path, record: &mut SpoolRecord) -> anyhow::Result<()> {
    persist_preparing(path, record).await?;
    if let Err(error) = prepare_namespace(path, record).await {
        return restore_then_terminal(
            path,
            record,
            AttemptState::Failed,
            Some(FailureKind::InvalidResult),
            error.to_string(),
        )
        .await;
    }
    Ok(())
}

async fn persist_preparing(path: &Path, record: &mut SpoolRecord) -> anyhow::Result<()> {
    let current = record.publication.as_ref().context("publication state missing")?;
    if current.phase != PublicationPhase::Executing {
        bail!("publication preparation requires executing phase");
    }
    let candidate = Path::new(&current.candidate_dir);
    let published = Path::new(&current.published_dir);
    let exchange = Path::new(&current.exchange_dir);
    publication_fs::validate_private_directory(candidate)?;
    publication_fs::validate_published_target(published)?;
    let candidate_identity =
        publication_fs::identity(candidate).context("candidate does not exist after sync success")?;
    let public_parent_identity = publication_fs::identity(&parent(published)?)?;
    let private_parent_identity = publication_fs::identity(&parent(exchange)?)?;
    if candidate_identity.device != public_parent_identity.device
        || candidate_identity.device != private_parent_identity.device
    {
        bail!("candidate, exchange, and published parent are not on one filesystem");
    }
    let published_identity = publication_fs::identity_if_exists(published)?;
    let stable_previous_identity = publication_fs::identity_if_exists(exchange)?;
    if published_identity.is_none() && stable_previous_identity.is_some() {
        bail!("published target is absent while a stable previous generation exists");
    }
    let rotated_previous_path = stable_previous_identity.map(|_| {
        Path::new(&current.gc_dir)
            .join(format!("previous-{}-{}", record.run_id, record.attempt))
            .to_string_lossy()
            .into_owned()
    });

    let mut next = record.clone();
    let publication = next.publication.as_mut().context("publication state missing")?;
    publication.phase = PublicationPhase::PreparingExchange;
    publication.candidate_identity = Some(candidate_identity);
    publication.published_identity = published_identity;
    publication.stable_previous_identity = stable_previous_identity;
    publication.exchange_identity = stable_previous_identity;
    publication.rotated_previous_path = rotated_previous_path;
    publication.rotated_previous_identity = None;
    publication.pre_visibility_terminal_state = None;
    publication.pre_visibility_failure_kind = None;
    publication.pre_visibility_message = None;
    write(path, &next).await.context("persist preparing_exchange")?;
    *record = next;
    Ok(())
}

async fn prepare_namespace(path: &Path, record: &mut SpoolRecord) -> anyhow::Result<()> {
    let state = record
        .publication
        .as_ref()
        .context("publication state missing")?
        .clone();
    if state.phase != PublicationPhase::PreparingExchange {
        bail!("private namespace preparation requires preparing_exchange phase");
    }
    let candidate_identity = state
        .candidate_identity
        .context("preparing_exchange lacks candidate identity")?;
    let candidate = PathBuf::from(&state.candidate_dir);
    let exchange = PathBuf::from(&state.exchange_dir);
    let gc = PathBuf::from(&state.gc_dir);
    tokio::fs::create_dir_all(&gc)
        .await
        .context("create publication GC directory")?;

    if let Some(previous_identity) = state.stable_previous_identity {
        let rotated = PathBuf::from(
            state
                .rotated_previous_path
                .as_ref()
                .context("preparing_exchange lacks planned previous rotation path")?,
        );
        match (
            publication_fs::identity_if_exists(&exchange)?,
            publication_fs::identity_if_exists(&rotated)?,
        ) {
            (Some(current), None) if current == previous_identity => {
                rename_noreplace(exchange.clone(), rotated.clone()).await?;
            }
            (None, Some(current)) if current == previous_identity => {}
            (Some(current), _) if current == candidate_identity => {}
            states => bail!("cannot reconstruct stable previous rotation from identities: {states:?}"),
        }
    } else if let Some(current) = publication_fs::identity_if_exists(&exchange)?
        && current != candidate_identity
    {
        bail!("unexpected exchange entry appeared during first publication");
    }

    match (
        publication_fs::identity_if_exists(&candidate)?,
        publication_fs::identity_if_exists(&exchange)?,
    ) {
        (Some(current), None) if current == candidate_identity => {
            rename_noreplace(candidate.clone(), exchange.clone()).await?;
        }
        (None, Some(current)) if current == candidate_identity => {}
        states => bail!("cannot stage candidate into fixed exchange slot from identities: {states:?}"),
    }

    sync_namespace(&[parent(&candidate)?, parent(&exchange)?, gc.clone()]).await?;
    let mut next = record.clone();
    let publication = next.publication.as_mut().context("publication state missing")?;
    publication.phase = PublicationPhase::ReadyToCommit;
    publication.exchange_identity = Some(candidate_identity);
    publication.rotated_previous_identity = match publication.rotated_previous_path.as_ref() {
        Some(rotated) => publication_fs::identity_if_exists(Path::new(rotated))?,
        None => None,
    };
    write(path, &next).await.context("persist ready_to_commit")?;
    *record = next;
    Ok(())
}

async fn commit_ready(path: &Path, record: &mut SpoolRecord) -> anyhow::Result<()> {
    let state = record
        .publication
        .as_ref()
        .context("publication state missing")?
        .clone();
    if state.phase != PublicationPhase::ReadyToCommit {
        bail!("visibility commit requires ready_to_commit phase");
    }
    if record.cancel_requested {
        return restore_then_terminal(
            path,
            record,
            AttemptState::Cancelled,
            None,
            "cancelled before visibility".into(),
        )
        .await;
    }
    if let Err(error) = verify_ready(&state) {
        return restore_then_terminal(
            path,
            record,
            AttemptState::Failed,
            Some(FailureKind::InvalidResult),
            error.to_string(),
        )
        .await;
    }
    let exchange = PathBuf::from(&state.exchange_dir);
    let published = PathBuf::from(&state.published_dir);
    if state.published_identity.is_some() {
        exchange_paths(exchange.clone(), published.clone()).await?;
    } else {
        rename_noreplace(exchange.clone(), published.clone()).await?;
    }
    finish_visible(path, record).await
}

async fn recover_ready(path: &Path, record: &mut SpoolRecord) -> anyhow::Result<()> {
    let state = record
        .publication
        .as_ref()
        .context("publication state missing")?
        .clone();
    let candidate = state
        .candidate_identity
        .context("ready_to_commit lacks candidate identity")?;
    let published = publication_fs::identity_if_exists(Path::new(&state.published_dir))?;
    let exchange = publication_fs::identity_if_exists(Path::new(&state.exchange_dir))?;
    if published == Some(candidate) {
        return finish_visible(path, record).await;
    }
    if exchange == Some(candidate) && published == state.published_identity {
        return commit_ready(path, record).await;
    }
    restore_then_terminal(
        path,
        record,
        AttemptState::Failed,
        Some(FailureKind::InvalidResult),
        "ready_to_commit identities are inconsistent".into(),
    )
    .await
}

fn verify_ready(state: &PublicationState) -> anyhow::Result<()> {
    let candidate = state
        .candidate_identity
        .context("ready_to_commit lacks candidate identity")?;
    if publication_fs::identity_if_exists(Path::new(&state.exchange_dir))? != Some(candidate) {
        bail!("candidate identity is not in fixed exchange slot");
    }
    if publication_fs::identity_if_exists(Path::new(&state.published_dir))? != state.published_identity {
        bail!("published identity changed outside LMT");
    }
    if let Some(previous) = state.stable_previous_identity {
        let rotated = state
            .rotated_previous_path
            .as_ref()
            .context("ready_to_commit lacks previous rotation path")?;
        if publication_fs::identity_if_exists(Path::new(rotated))? != Some(previous) {
            bail!("stable previous identity is not at its protected rotation path");
        }
    }
    Ok(())
}

async fn finish_visible(path: &Path, record: &mut SpoolRecord) -> anyhow::Result<()> {
    let state = record
        .publication
        .as_ref()
        .context("publication state missing")?
        .clone();
    let candidate = state
        .candidate_identity
        .context("publication lacks candidate identity")?;
    if publication_fs::identity_if_exists(Path::new(&state.published_dir))? != Some(candidate) {
        bail!("candidate identity is not visible at published path; visibility outcome is unresolved");
    }
    let parents = [
        parent(Path::new(&state.published_dir))?,
        parent(Path::new(&state.exchange_dir))?,
    ];
    if let Err(error) = sync_namespace(&parents).await {
        let mut pending = record.clone();
        pending.publication.as_mut().context("publication state missing")?.phase =
            PublicationPhase::VisiblePendingDurability;
        if write(path, &pending).await.is_ok() {
            *record = pending;
        }
        return Err(error).context("publication visible but directory durability is pending");
    }
    let mut committed = record.clone();
    committed
        .publication
        .as_mut()
        .context("publication state missing")?
        .phase = PublicationPhase::CommittedPendingReport;
    committed.terminal(AttemptState::Succeeded, Some(0), None, None, now());
    write(path, &committed)
        .await
        .context("persist committed_pending_report before success")?;
    *record = committed;
    Ok(())
}

async fn ensure_committed_terminal(path: &Path, record: &mut SpoolRecord) -> anyhow::Result<()> {
    if record.state == AttemptState::Succeeded {
        return Ok(());
    }
    let mut committed = record.clone();
    committed.terminal(AttemptState::Succeeded, Some(0), None, None, now());
    write(path, &committed).await?;
    *record = committed;
    Ok(())
}

async fn restore_then_terminal(
    path: &Path,
    record: &mut SpoolRecord,
    state: AttemptState,
    failure_kind: Option<FailureKind>,
    message: String,
) -> anyhow::Result<()> {
    if record.publication.as_ref().context("publication state missing")?.phase
        != PublicationPhase::PreVisibilityRecovery
    {
        let mut recovery = record.clone();
        let publication = recovery.publication.as_mut().context("publication state missing")?;
        publication.phase = PublicationPhase::PreVisibilityRecovery;
        publication.pre_visibility_terminal_state = Some(state);
        publication.pre_visibility_failure_kind = failure_kind;
        publication.pre_visibility_message = Some(message.clone());
        write(path, &recovery)
            .await
            .context("persist pre-visibility recovery intent")?;
        *record = recovery;
    }
    let publication = record
        .publication
        .as_ref()
        .context("publication state missing")?
        .clone();
    let candidate_identity = publication
        .candidate_identity
        .context("publication lacks candidate identity")?;
    let candidate = PathBuf::from(&publication.candidate_dir);
    let exchange = PathBuf::from(&publication.exchange_dir);
    if publication_fs::identity_if_exists(&exchange)? == Some(candidate_identity) {
        if publication_fs::identity_if_exists(&candidate)?.is_some() {
            bail!("cannot restore candidate because its private path exists");
        }
        rename_noreplace(exchange.clone(), candidate.clone()).await?;
    }
    if let Some(previous) = publication.stable_previous_identity {
        let rotated = PathBuf::from(
            publication
                .rotated_previous_path
                .as_ref()
                .context("publication lacks previous rotation path")?,
        );
        match (
            publication_fs::identity_if_exists(&exchange)?,
            publication_fs::identity_if_exists(&rotated)?,
        ) {
            (None, Some(current)) if current == previous => {
                rename_noreplace(rotated, exchange.clone()).await?;
            }
            (Some(current), _) if current == previous => {}
            identities => bail!("cannot restore stable previous identity: {identities:?}"),
        }
    }
    sync_namespace(&[parent(&candidate)?, parent(&exchange)?]).await?;
    let mut terminal = record.clone();
    let publication = terminal.publication.as_mut().context("publication state missing")?;
    publication.phase = PublicationPhase::Executing;
    publication.pre_visibility_terminal_state = None;
    publication.pre_visibility_failure_kind = None;
    publication.pre_visibility_message = None;
    terminal.terminal(state, None, failure_kind, Some(message), now());
    write(path, &terminal).await?;
    *record = terminal;
    Ok(())
}

fn parent(path: &Path) -> anyhow::Result<PathBuf> {
    path.parent()
        .map(Path::to_owned)
        .with_context(|| format!("path {} has no parent", path.display()))
}

async fn rename_noreplace(source: PathBuf, destination: PathBuf) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || publication_fs::rename_noreplace(&source, &destination)).await??;
    Ok(())
}

async fn exchange_paths(left: PathBuf, right: PathBuf) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || publication_fs::exchange(&left, &right)).await??;
    Ok(())
}

async fn sync_namespace(paths: &[PathBuf]) -> anyhow::Result<()> {
    let mut paths = paths.to_vec();
    paths.sort();
    paths.dedup();
    tokio::task::spawn_blocking(move || {
        for path in paths {
            publication_fs::fsync_directory(&path)?;
        }
        anyhow::Ok(())
    })
    .await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lmt_core::{AtomicPublicationSpec, ProcessRunSpec};
    use std::sync::atomic::{AtomicBool, Ordering};

    async fn atomic_record(root: &Path, run_id: &str) -> (PathBuf, SpoolRecord) {
        let (path, record) = atomic_record_without_candidate(root, run_id).await;
        let candidate = PathBuf::from(&record.publication.as_ref().expect("publication state").candidate_dir);
        tokio::fs::create_dir_all(&candidate).await.expect("candidate");
        tokio::fs::write(candidate.join("generation"), run_id)
            .await
            .expect("candidate contents");
        (path, record)
    }

    async fn atomic_record_without_candidate(root: &Path, run_id: &str) -> (PathBuf, SpoolRecord) {
        let mirror_root = root.join("mirrors");
        let private_root = root.join("publication");
        let mirror_private = private_root.join("demo");
        let candidate = mirror_private.join(format!("attempts/{run_id}-1/root"));
        tokio::fs::create_dir_all(&mirror_root).await.expect("mirror root");
        let spool = root.join("spool");
        tokio::fs::create_dir_all(&spool).await.expect("spool");
        let spec = ProcessRunSpec {
            runner: "process".into(),
            program: "/bin/true".into(),
            args: Vec::new(),
            cwd: None,
            timeout_seconds: 60,
            mirror_root: mirror_root.to_string_lossy().into_owned(),
            target_dir: candidate.to_string_lossy().into_owned(),
            publication: Some(Box::new(AtomicPublicationSpec {
                mirror: "demo".into(),
                publication_root: private_root.to_string_lossy().into_owned(),
                published_dir: mirror_root.join("demo").to_string_lossy().into_owned(),
                candidate_dir: candidate.to_string_lossy().into_owned(),
                basis_dir: mirror_private
                    .join(format!("attempts/{run_id}-1/basis"))
                    .to_string_lossy()
                    .into_owned(),
                exchange_dir: mirror_private.join("exchange").to_string_lossy().into_owned(),
                gc_dir: mirror_private.join("gc").to_string_lossy().into_owned(),
            })),
        };
        let mut record = SpoolRecord::accepted(run_id.into(), 1, format!("hash-{run_id}"), spec, now());
        record.state = AttemptState::Running;
        record.sequence = 2;
        record.started_at = Some(now());
        let path = spool.join(format!("{run_id}-1.json"));
        write(&path, &record).await.expect("initial spool");
        (path, record)
    }

    #[tokio::test]
    async fn same_mirror_serializes_while_different_mirrors_progress() {
        let locks = MirrorLocks::default();
        let first = locks.acquire("mirror-a").await;
        let entered_same = Arc::new(AtomicBool::new(false));
        let same_flag = entered_same.clone();
        let same_locks = locks.clone();
        let same = tokio::spawn(async move {
            let _guard = same_locks.acquire("mirror-a").await;
            same_flag.store(true, Ordering::SeqCst);
        });
        tokio::task::yield_now().await;
        assert!(!entered_same.load(Ordering::SeqCst));

        let _different = locks.acquire("mirror-b").await;
        drop(first);
        same.await.expect("same-mirror waiter");
        assert!(entered_same.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn candidate_admission_is_fresh_and_basis_never_aliases_the_destination() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (_path, record) = atomic_record_without_candidate(directory.path(), "run-1").await;
        let publication = record.publication.as_ref().expect("publication state");
        prepare_candidate(&record, &MirrorLocks::default())
            .await
            .expect("first-publication candidate");
        assert!(Path::new(&publication.candidate_dir).is_dir());
        assert!(Path::new(&publication.basis_dir).is_dir());
        assert!(prepare_candidate(&record, &MirrorLocks::default()).await.is_err());

        tokio::fs::create_dir_all(&publication.published_dir)
            .await
            .expect("published generation");
        let (_path, next) = atomic_record_without_candidate(directory.path(), "run-2").await;
        prepare_candidate(&next, &MirrorLocks::default())
            .await
            .expect("update candidate");
        let basis = PathBuf::from(&next.publication.as_ref().expect("publication state").basis_dir);
        assert_eq!(
            tokio::fs::read_link(&basis).await.expect("basis symlink"),
            PathBuf::from(&publication.published_dir)
        );
        assert_ne!(
            publication_fs::identity(&basis).expect("basis link identity"),
            publication_fs::identity(Path::new(&next.publication.as_ref().expect("state").candidate_dir))
                .expect("candidate identity")
        );
    }

    #[tokio::test]
    async fn first_and_repeated_publications_preserve_current_and_previous_ownership() {
        let directory = tempfile::tempdir().expect("tempdir");
        let locks = MirrorLocks::default();
        let (first_path, mut first) = atomic_record(directory.path(), "run-1").await;
        commit(&first_path, &mut first, &locks)
            .await
            .expect("first publication");
        let first_state = first.publication.as_ref().expect("publication state");
        assert_eq!(first.state, AttemptState::Succeeded);
        assert_eq!(first_state.phase, PublicationPhase::CommittedPendingReport);
        assert_eq!(
            tokio::fs::read_to_string(Path::new(&first_state.published_dir).join("generation"))
                .await
                .expect("first visible generation"),
            "run-1"
        );
        assert!(!Path::new(&first_state.exchange_dir).exists());

        let (second_path, mut second) = atomic_record(directory.path(), "run-2").await;
        let old_published = publication_fs::identity(Path::new(
            &second.publication.as_ref().expect("publication state").published_dir,
        ))
        .expect("old published identity");
        commit(&second_path, &mut second, &locks)
            .await
            .expect("second publication");
        let second_state = second.publication.as_ref().expect("publication state");
        assert_eq!(
            tokio::fs::read_to_string(Path::new(&second_state.published_dir).join("generation"))
                .await
                .expect("second visible generation"),
            "run-2"
        );
        assert_eq!(
            publication_fs::identity(Path::new(&second_state.exchange_dir)).expect("previous identity"),
            old_published
        );

        let (third_path, mut third) = atomic_record(directory.path(), "run-3").await;
        let immediately_previous = publication_fs::identity(Path::new(
            &third.publication.as_ref().expect("publication state").published_dir,
        ))
        .expect("current identity");
        commit(&third_path, &mut third, &locks)
            .await
            .expect("third publication");
        let third_state = third.publication.as_ref().expect("publication state");
        assert_eq!(
            publication_fs::identity(Path::new(&third_state.exchange_dir)).expect("fixed previous slot"),
            immediately_previous
        );
        let rotated = third_state
            .rotated_previous_path
            .as_ref()
            .expect("rotated older generation");
        assert_eq!(
            publication_fs::identity(Path::new(rotated)).expect("protected older generation"),
            third_state.stable_previous_identity.expect("old stable identity")
        );
    }

    #[tokio::test]
    async fn ready_record_recovers_forward_when_visibility_happened_before_spool_update() {
        let directory = tempfile::tempdir().expect("tempdir");
        let locks = MirrorLocks::default();
        let (initial_path, mut initial) = atomic_record(directory.path(), "run-old").await;
        commit(&initial_path, &mut initial, &locks)
            .await
            .expect("initial publication");

        let (path, mut record) = atomic_record(directory.path(), "run-new").await;
        prepare(&path, &mut record).await.expect("durable ready state");
        let ready = record.publication.as_ref().expect("ready state").clone();
        assert_eq!(ready.phase, PublicationPhase::ReadyToCommit);
        exchange_paths(PathBuf::from(&ready.exchange_dir), PathBuf::from(&ready.published_dir))
            .await
            .expect("simulated visibility commit before crash");

        let mut reopened = crate::spool::read(&path).await.expect("reopen ready spool");
        assert_eq!(
            reopened.publication.as_ref().expect("publication state").phase,
            PublicationPhase::ReadyToCommit
        );
        recover(&path, &mut reopened, &locks).await.expect("recover forward");
        assert_eq!(reopened.state, AttemptState::Succeeded);
        assert_eq!(
            tokio::fs::read_to_string(
                Path::new(&reopened.publication.as_ref().expect("publication state").published_dir).join("generation")
            )
            .await
            .expect("recovered visible generation"),
            "run-new"
        );
    }

    #[tokio::test]
    async fn preparing_write_ahead_recovers_before_and_between_private_namespace_mutations() {
        let directory = tempfile::tempdir().expect("tempdir");
        let locks = MirrorLocks::default();
        let (initial_path, mut initial) = atomic_record(directory.path(), "run-1").await;
        commit(&initial_path, &mut initial, &locks)
            .await
            .expect("initial publication");

        let (second_path, mut second) = atomic_record(directory.path(), "run-2").await;
        persist_preparing(&second_path, &mut second)
            .await
            .expect("write-ahead before mutation");
        let durable = crate::spool::read(&second_path)
            .await
            .expect("durable preparing record");
        assert_eq!(
            durable.publication.as_ref().expect("publication state").phase,
            PublicationPhase::PreparingExchange
        );
        assert!(Path::new(&durable.publication.as_ref().expect("publication state").candidate_dir).exists());
        recover(&second_path, &mut second, &locks)
            .await
            .expect("recover before mutation");
        assert_eq!(second.state, AttemptState::Succeeded);

        let (third_path, mut third) = atomic_record(directory.path(), "run-3").await;
        persist_preparing(&third_path, &mut third)
            .await
            .expect("third write-ahead");
        let third_state = third.publication.as_ref().expect("publication state").clone();
        let rotated = PathBuf::from(third_state.rotated_previous_path.expect("planned rotation"));
        tokio::fs::create_dir_all(&third_state.gc_dir).await.expect("GC dir");
        rename_noreplace(PathBuf::from(&third_state.exchange_dir), rotated)
            .await
            .expect("crash after previous rotation");
        let mut reopened = crate::spool::read(&third_path).await.expect("reopen after rotation");
        recover(&third_path, &mut reopened, &locks)
            .await
            .expect("recover rotated previous");
        assert_eq!(reopened.state, AttemptState::Succeeded);
        assert_eq!(
            tokio::fs::read_to_string(
                Path::new(&reopened.publication.as_ref().expect("publication state").published_dir).join("generation")
            )
            .await
            .expect("third generation"),
            "run-3"
        );

        let (fourth_path, mut fourth) = atomic_record(directory.path(), "run-4").await;
        persist_preparing(&fourth_path, &mut fourth)
            .await
            .expect("fourth write-ahead");
        let fourth_state = fourth.publication.as_ref().expect("publication state").clone();
        let rotated = PathBuf::from(fourth_state.rotated_previous_path.expect("planned rotation"));
        tokio::fs::create_dir_all(&fourth_state.gc_dir).await.expect("GC dir");
        rename_noreplace(PathBuf::from(&fourth_state.exchange_dir), rotated)
            .await
            .expect("rotate previous");
        rename_noreplace(
            PathBuf::from(&fourth_state.candidate_dir),
            PathBuf::from(&fourth_state.exchange_dir),
        )
        .await
        .expect("crash after candidate staging");
        let mut reopened = crate::spool::read(&fourth_path).await.expect("reopen after staging");
        recover(&fourth_path, &mut reopened, &locks)
            .await
            .expect("recover staged candidate");
        assert_eq!(reopened.state, AttemptState::Succeeded);
        assert_eq!(
            tokio::fs::read_to_string(
                Path::new(&reopened.publication.as_ref().expect("publication state").published_dir).join("generation")
            )
            .await
            .expect("fourth generation"),
            "run-4"
        );
    }

    #[tokio::test]
    async fn cancellation_after_staging_restores_private_previous_before_terminal() {
        let directory = tempfile::tempdir().expect("tempdir");
        let locks = MirrorLocks::default();
        for run in ["run-1", "run-2"] {
            let (path, mut record) = atomic_record(directory.path(), run).await;
            commit(&path, &mut record, &locks).await.expect("seed publication");
        }
        let (path, mut cancelled) = atomic_record(directory.path(), "run-cancelled").await;
        let publication = cancelled.publication.as_ref().expect("publication state");
        let published_before = publication_fs::identity(Path::new(&publication.published_dir)).expect("published");
        let previous_before = publication_fs::identity(Path::new(&publication.exchange_dir)).expect("previous");
        let candidate_before = publication_fs::identity(Path::new(&publication.candidate_dir)).expect("candidate");
        cancelled.cancel_requested = true;
        write(&path, &cancelled).await.expect("durable cancellation");

        commit(&path, &mut cancelled, &locks)
            .await
            .expect("cancelled restoration");
        let publication = cancelled.publication.as_ref().expect("publication state");
        assert_eq!(cancelled.state, AttemptState::Cancelled);
        assert_eq!(publication.phase, PublicationPhase::Executing);
        assert_eq!(
            publication_fs::identity(Path::new(&publication.published_dir)).expect("unchanged published"),
            published_before
        );
        assert_eq!(
            publication_fs::identity(Path::new(&publication.exchange_dir)).expect("restored previous"),
            previous_before
        );
        assert_eq!(
            publication_fs::identity(Path::new(&publication.candidate_dir)).expect("restored candidate"),
            candidate_before
        );
    }

    #[tokio::test]
    async fn restart_ready_waits_for_control_plane_then_honors_delivered_cancellation() {
        let directory = tempfile::tempdir().expect("tempdir");
        let locks = MirrorLocks::default();
        let (initial_path, mut initial) = atomic_record(directory.path(), "run-old").await;
        commit(&initial_path, &mut initial, &locks)
            .await
            .expect("initial publication");
        let (path, mut record) = atomic_record(directory.path(), "run-new").await;
        prepare(&path, &mut record).await.expect("ready before restart");
        let publication = record.publication.as_ref().expect("publication state").clone();
        let published_before = publication_fs::identity(Path::new(&publication.published_dir)).expect("published");

        recover_before_control_plane(&path, &mut record, &locks)
            .await
            .expect("pre-poll recovery");
        assert_eq!(record.state, AttemptState::Running);
        assert_eq!(
            record.publication.as_ref().expect("publication state").phase,
            PublicationPhase::ReadyToCommit
        );
        assert_eq!(
            publication_fs::identity(Path::new(&publication.published_dir)).expect("still-old published"),
            published_before
        );

        record.cancel_requested = true;
        write(&path, &record).await.expect("Server-delivered cancellation");
        recover(&path, &mut record, &locks).await.expect("post-poll recovery");
        assert_eq!(record.state, AttemptState::Cancelled);
        assert_eq!(
            publication_fs::identity(Path::new(&publication.published_dir)).expect("unchanged published"),
            published_before
        );
    }
}
