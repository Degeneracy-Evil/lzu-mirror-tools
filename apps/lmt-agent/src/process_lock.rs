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
    #[test]
    fn production_unit_preserves_process_runner_compatibility_and_reload() {
        let unit = include_str!("../../../packaging/systemd/lmt-agent.service");
        for directive in [
            "StateDirectory=lmt-agent",
            "StateDirectoryMode=0700",
            "ExecReload=/bin/kill -HUP $MAINPID",
            "KillMode=control-group",
            "ProtectSystem=full",
            "Restart=on-failure",
        ] {
            assert!(unit.contains(directive), "missing {directive}");
        }
        for incompatible in ["SystemCallFilter=", "CapabilityBoundingSet=", "PrivateDevices=true"] {
            assert!(
                !unit.contains(incompatible),
                "Agent runner was over-restricted by {incompatible}"
            );
        }
    }
}
