use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt, path::Path};

use anyhow::{Context, bail};
use nix::fcntl::{Flock, FlockArg};

#[derive(Debug)]
pub struct ProcessLock {
    _lock: Flock<std::fs::File>,
}

impl ProcessLock {
    pub fn acquire(path: &Path) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("open process lock {}", path.display()))?;
        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => Ok(Self { _lock: lock }),
            Err((_file, error)) => bail!("state_lock_busy: cannot lock {}: {error}", path.display()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_server_lock_is_refused_until_release() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("server.lock");
        let first = ProcessLock::acquire(&path).expect("first lock");
        assert!(ProcessLock::acquire(&path).is_err());
        drop(first);
        ProcessLock::acquire(&path).expect("released lock");
    }

    #[test]
    fn production_unit_keeps_state_writable_and_reloadable_under_hardening() {
        let unit = include_str!("../../../packaging/systemd/lmt-server.service");
        for directive in [
            "StateDirectory=lmt",
            "StateDirectoryMode=0750",
            "RuntimeDirectory=lmt",
            "ExecReload=/bin/kill -HUP $MAINPID",
            "ProtectSystem=strict",
            "PrivateDevices=true",
            "ProtectKernelTunables=true",
            "ProtectKernelModules=true",
            "ProtectControlGroups=true",
            "RestrictSUIDSGID=true",
            "Restart=on-failure",
        ] {
            assert!(unit.contains(directive), "missing {directive}");
        }
    }
}
