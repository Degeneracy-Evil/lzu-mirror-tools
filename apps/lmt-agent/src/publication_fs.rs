use std::{
    fs::{self, File},
    os::unix::{ffi::OsStringExt, fs::MetadataExt},
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use nix::{
    errno::Errno,
    fcntl::{RenameFlags, renameat2},
    unistd::fsync,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileIdentity {
    pub device: u64,
    pub inode: u64,
}

pub fn identity(path: &Path) -> anyhow::Result<FileIdentity> {
    let metadata = fs::symlink_metadata(path).with_context(|| format!("stat {}", path.display()))?;
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

pub fn identity_if_exists(path: &Path) -> anyhow::Result<Option<FileIdentity>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("stat {}", path.display())),
    }
}

pub fn exchange(left: &Path, right: &Path) -> anyhow::Result<()> {
    rename(left, right, RenameFlags::RENAME_EXCHANGE).context("RENAME_EXCHANGE")
}

pub fn rename_noreplace(source: &Path, destination: &Path) -> anyhow::Result<()> {
    rename(source, destination, RenameFlags::RENAME_NOREPLACE).context("RENAME_NOREPLACE")
}

fn rename(source: &Path, destination: &Path, flags: RenameFlags) -> anyhow::Result<()> {
    let source_parent = source.parent().context("rename source has no parent")?;
    let destination_parent = destination.parent().context("rename destination has no parent")?;
    let source_name = source.file_name().context("rename source has no file name")?;
    let destination_name = destination.file_name().context("rename destination has no file name")?;
    let source_directory =
        File::open(source_parent).with_context(|| format!("open rename source parent {}", source_parent.display()))?;
    let destination_directory = File::open(destination_parent)
        .with_context(|| format!("open rename destination parent {}", destination_parent.display()))?;
    renameat2(
        &source_directory,
        Path::new(source_name),
        &destination_directory,
        Path::new(destination_name),
        flags,
    )?;
    Ok(())
}

pub fn fsync_directory(path: &Path) -> anyhow::Result<()> {
    let directory = File::open(path).with_context(|| format!("open directory {} for fsync", path.display()))?;
    fsync(&directory).with_context(|| format!("fsync directory {}", path.display()))
}

pub fn validate_published_target(path: &Path) -> anyhow::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("stat published target {}", path.display())),
    };
    if !metadata.file_type().is_dir() {
        bail!("published target {} is not an ordinary directory", path.display());
    }
    let parent = path.parent().context("published target has no parent")?;
    if metadata.dev() != fs::metadata(parent)?.dev() || listed_mount_point(path)? {
        bail!("published target {} is a mount point", path.display());
    }
    Ok(())
}

pub fn validate_private_directory(path: &Path) -> anyhow::Result<()> {
    let metadata = fs::symlink_metadata(path).with_context(|| format!("stat private directory {}", path.display()))?;
    if !metadata.file_type().is_dir() {
        bail!(
            "private publication path {} is not an ordinary directory",
            path.display()
        );
    }
    Ok(())
}

fn listed_mount_point(path: &Path) -> anyhow::Result<bool> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("canonicalize published target {}", path.display()))?;
    let mountinfo = fs::read("/proc/self/mountinfo").context("read /proc/self/mountinfo")?;
    Ok(mountinfo.split(|byte| *byte == b'\n').any(|line| {
        line.split(|byte| *byte == b' ')
            .nth(4)
            .is_some_and(|field| decode_mount_path(field) == canonical)
    }))
}

fn decode_mount_path(field: &[u8]) -> PathBuf {
    let mut decoded = Vec::with_capacity(field.len());
    let mut index = 0;
    while index < field.len() {
        if field[index] == b'\\'
            && index + 3 < field.len()
            && field[index + 1..=index + 3]
                .iter()
                .all(|byte| matches!(byte, b'0'..=b'7'))
        {
            let value = (field[index + 1] - b'0') * 64 + (field[index + 2] - b'0') * 8 + (field[index + 3] - b'0');
            decoded.push(value);
            index += 4;
        } else {
            decoded.push(field[index]);
            index += 1;
        }
    }
    PathBuf::from(std::ffi::OsString::from_vec(decoded))
}

pub fn preflight(mirror_root: &Path, publication_root: &Path) -> anyhow::Result<()> {
    let mirror_root = mirror_root
        .canonicalize()
        .with_context(|| format!("canonicalize mirror_root {}", mirror_root.display()))?;
    let publication_root = publication_root
        .canonicalize()
        .with_context(|| format!("canonicalize publication_root {}", publication_root.display()))?;
    if publication_root == mirror_root || publication_root.starts_with(&mirror_root) {
        bail!("publication_root must be outside mirror_root");
    }
    let mirror_metadata = fs::metadata(&mirror_root)?;
    let publication_metadata = fs::metadata(&publication_root)?;
    if !mirror_metadata.is_dir() || !publication_metadata.is_dir() {
        bail!("mirror_root and publication_root must be directories");
    }
    if mirror_metadata.dev() != publication_metadata.dev() {
        bail!("mirror_root and publication_root must be on the same mounted filesystem");
    }
    probe_writable(&mirror_root, "mirror-root")?;
    probe_publication_backend(&publication_root)
}

fn probe_writable(root: &Path, label: &str) -> anyhow::Result<()> {
    let probe = root.join(format!(".lmt-{label}-probe-{}", ulid::Ulid::new()));
    fs::create_dir(&probe).with_context(|| format!("create writable probe in {}", root.display()))?;
    fsync_directory(root)?;
    fs::remove_dir(&probe).with_context(|| format!("remove writable probe {}", probe.display()))?;
    fsync_directory(root)
}

fn probe_publication_backend(root: &Path) -> anyhow::Result<()> {
    let probe = root.join(format!(".lmt-publication-probe-{}", ulid::Ulid::new()));
    fs::create_dir(&probe).with_context(|| format!("create publication probe in {}", root.display()))?;
    let result = probe_rename_flags(&probe);
    let cleanup = fs::remove_dir_all(&probe).with_context(|| format!("remove publication probe {}", probe.display()));
    result?;
    cleanup?;
    fsync_directory(root)
}

fn probe_rename_flags(probe: &Path) -> anyhow::Result<()> {
    let left = probe.join("left");
    let right = probe.join("right");
    fs::create_dir(&left)?;
    fs::create_dir(&right)?;
    validate_published_target(&left)?;
    validate_published_target(&right)?;
    let left_identity = identity(&left)?;
    let right_identity = identity(&right)?;
    fs::write(left.join("identity"), b"left")?;
    fs::write(right.join("identity"), b"right")?;
    exchange(&left, &right)?;
    if identity(&left)? != right_identity
        || identity(&right)? != left_identity
        || fs::read(right.join("identity"))? != b"left"
        || fs::read(left.join("identity"))? != b"right"
    {
        bail!("RENAME_EXCHANGE probe did not exchange directory identities");
    }

    let source = probe.join("noreplace-source");
    let destination = probe.join("noreplace-destination");
    fs::create_dir(&source)?;
    fs::create_dir(&destination)?;
    let error = match rename_noreplace(&source, &destination) {
        Ok(()) => bail!("RENAME_NOREPLACE overwrote probe target"),
        Err(error) => error,
    };
    if error.downcast_ref::<Errno>() != Some(&Errno::EEXIST) {
        return Err(error).context("RENAME_NOREPLACE existing-target probe");
    }
    fs::remove_dir(&destination)?;
    rename_noreplace(&source, &destination)?;
    if source.exists() || !destination.is_dir() {
        bail!("RENAME_NOREPLACE probe did not move the source directory");
    }
    fsync_directory(probe)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_filesystem_preflight_probes_exchange_noreplace_and_cleanup() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mirror_root = directory.path().join("mirrors");
        let publication_root = directory.path().join("publication");
        fs::create_dir(&mirror_root).expect("mirror root");
        fs::create_dir(&publication_root).expect("publication root");

        preflight(&mirror_root, &publication_root).expect("preflight");
        assert!(fs::read_dir(&mirror_root).expect("mirror entries").next().is_none());
        assert!(
            fs::read_dir(&publication_root)
                .expect("publication entries")
                .next()
                .is_none()
        );
    }

    #[test]
    fn preflight_rejects_private_root_below_served_root_and_invalid_targets() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mirror_root = directory.path().join("mirrors");
        let nested = mirror_root.join("private");
        fs::create_dir_all(&nested).expect("roots");
        assert!(preflight(&mirror_root, &nested).is_err());

        let file_target = mirror_root.join("file-target");
        fs::write(&file_target, b"not a directory").expect("file target");
        assert!(validate_published_target(&file_target).is_err());
        assert!(validate_published_target(&mirror_root.join("absent")).is_ok());
        assert!(validate_published_target(Path::new("/")).is_err());
    }

    #[test]
    fn identity_tracks_namespace_exchange() {
        let directory = tempfile::tempdir().expect("tempdir");
        let left = directory.path().join("left");
        let right = directory.path().join("right");
        fs::create_dir(&left).expect("left");
        fs::create_dir(&right).expect("right");
        let left_identity = identity(&left).expect("left identity");
        let right_identity = identity(&right).expect("right identity");
        exchange(&left, &right).expect("exchange");
        assert_eq!(identity(&left).expect("new left"), right_identity);
        assert_eq!(identity(&right).expect("new right"), left_identity);
        fsync_directory(directory.path()).expect("directory durability");
    }
}
