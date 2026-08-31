use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use lmt_core::{AttemptState, FailureKind};
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use tokio::{
    fs,
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{Mutex, mpsc, watch},
};

use crate::{
    now,
    spool::{SpoolRecord, read, write},
};

enum Outcome {
    Process(std::io::Result<std::process::ExitStatus>),
    TimedOut,
    Shutdown,
    Cancelled,
}

pub async fn execute(
    path: &Path,
    record: &mut SpoolRecord,
    mut shutdown: watch::Receiver<bool>,
    mut cancel: watch::Receiver<bool>,
    spool_lock: Arc<Mutex<()>>,
) {
    let Some(spec) = record.spec.as_ref() else {
        return;
    };
    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    if let Some(cwd) = &spec.cwd {
        command.current_dir(cwd);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            record.terminal(
                AttemptState::Failed,
                None,
                Some(FailureKind::Process),
                Some(error.to_string()),
                now(),
            );
            let _guard = spool_lock.lock().await;
            let _ = write(path, record).await;
            return;
        }
    };
    let process_group = child
        .id()
        .map(|id| Pid::from_raw(i32::try_from(id).unwrap_or(i32::MAX)));
    let capture = tokio::spawn(capture(
        path.with_extension("log"),
        child.stdout.take(),
        child.stderr.take(),
    ));
    record.state = AttemptState::Running;
    record.sequence += 1;
    record.started_at = Some(now());
    {
        let _guard = spool_lock.lock().await;
        if let Ok(latest) = read(path).await {
            record.cancel_requested |= latest.cancel_requested;
        }
        let _ = write(path, record).await;
    }
    let timeout = tokio::time::sleep(Duration::from_secs(spec.timeout_seconds));
    tokio::pin!(timeout);
    let outcome = tokio::select! {
        result = child.wait() => Outcome::Process(result),
        () = &mut timeout => { terminate_group(process_group).await; let _ = child.wait().await; Outcome::TimedOut },
        changed = shutdown.changed() => { let _ = changed; terminate_group(process_group).await; let _ = child.wait().await; Outcome::Shutdown },
        changed = cancel.changed() => { let _ = changed; terminate_group(process_group).await; let _ = child.wait().await; Outcome::Cancelled },
    };
    let _ = capture.await;
    let _guard = spool_lock.lock().await;
    if let Ok(latest) = read(path).await {
        record.cancel_requested |= latest.cancel_requested;
    }
    match outcome {
        _ if record.cancel_requested => record.terminal(AttemptState::Cancelled, None, None, None, now()),
        Outcome::Process(Ok(status)) if status.success() => {
            record.terminal(AttemptState::Succeeded, status.code(), None, None, now())
        }
        Outcome::Process(Ok(status)) => record.terminal(
            AttemptState::Failed,
            status.code(),
            Some(FailureKind::Process),
            Some("process exited non-zero".into()),
            now(),
        ),
        Outcome::Process(Err(error)) => record.terminal(
            AttemptState::Interrupted,
            None,
            Some(FailureKind::Interrupted),
            Some(error.to_string()),
            now(),
        ),
        Outcome::Shutdown => record.terminal(
            AttemptState::Interrupted,
            None,
            Some(FailureKind::Interrupted),
            Some("agent shutdown".into()),
            now(),
        ),
        Outcome::TimedOut => record.terminal(
            AttemptState::TimedOut,
            None,
            Some(FailureKind::Timeout),
            Some("attempt timed out".into()),
            now(),
        ),
        Outcome::Cancelled => record.terminal(AttemptState::Cancelled, None, None, None, now()),
    }
    let _ = write(path, record).await;
}

async fn terminate_group(group: Option<Pid>) {
    if let Some(group) = group {
        let _ = killpg(group, Signal::SIGTERM);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = killpg(group, Signal::SIGKILL);
    }
}

enum Frame {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
}

async fn capture(
    path: PathBuf,
    stdout: Option<tokio::process::ChildStdout>,
    stderr: Option<tokio::process::ChildStderr>,
) -> std::io::Result<()> {
    let (sender, mut receiver) = mpsc::channel(16);
    if let Some(stdout) = stdout {
        tokio::spawn(pump(stdout, sender.clone(), true));
    }
    if let Some(stderr) = stderr {
        tokio::spawn(pump(stderr, sender.clone(), false));
    }
    drop(sender);
    let mut file = fs::File::create(path).await?;
    while let Some(frame) = receiver.recv().await {
        match frame {
            Frame::Stdout(bytes) => {
                file.write_all(b"[stdout] ").await?;
                file.write_all(&bytes).await?;
            }
            Frame::Stderr(bytes) => {
                file.write_all(b"[stderr] ").await?;
                file.write_all(&bytes).await?;
            }
        }
        file.sync_data().await?;
    }
    file.sync_all().await
}

async fn pump(mut stream: impl AsyncRead + Unpin, sender: mpsc::Sender<Frame>, stdout: bool) {
    let mut buffer = vec![0; 8192];
    loop {
        let read = match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        let frame = if stdout {
            Frame::Stdout(buffer[..read].to_vec())
        } else {
            Frame::Stderr(buffer[..read].to_vec())
        };
        if sender.send(frame).await.is_err() {
            break;
        }
    }
}
