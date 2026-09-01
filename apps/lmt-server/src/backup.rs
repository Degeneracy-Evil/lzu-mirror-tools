use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, bail};
use lmt_protocol::v1alpha1::BackupManifest;
use rusqlite::{Connection, backup::Backup};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

fn paths(directory: &Path, id: &str) -> (PathBuf, PathBuf) {
    (
        directory.join(format!("{id}.sqlite")),
        directory.join(format!("{id}.json")),
    )
}

fn validate_id(id: &str) -> anyhow::Result<()> {
    if id.len() != 26 || !id.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        bail!("backup_invalid: invalid backup ID");
    }
    Ok(())
}

fn integrity_metadata(path: &Path) -> anyhow::Result<(u32, u64)> {
    let connection = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        bail!("backup_invalid: SQLite integrity check failed: {integrity}");
    }
    let schema_version =
        connection.query_row("SELECT COALESCE(MAX(version), 0) FROM schema_migrations", [], |row| {
            row.get::<_, u32>(0)
        })?;
    let config_revision =
        connection.query_row("SELECT COALESCE(MAX(revision), 0) FROM config_revisions", [], |row| {
            row.get::<_, u64>(0)
        })?;
    Ok((schema_version, config_revision))
}

fn checksum(path: &Path) -> anyhow::Result<(String, u64)> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok((hex::encode(digest.finalize()), size))
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn publish_manifest(directory: &Path, manifest: &BackupManifest) -> anyhow::Result<()> {
    let (_, final_path) = paths(directory, &manifest.id);
    let temporary = directory.join(format!("{}.json.tmp", manifest.id));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)?;
    serde_json::to_writer_pretty(&mut file, manifest)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temporary, final_path)?;
    sync_directory(directory)
}

pub fn create(source: &Path, directory: &Path) -> anyhow::Result<BackupManifest> {
    fs::create_dir_all(directory)?;
    let id = ulid::Ulid::new().to_string();
    let (final_database, _) = paths(directory, &id);
    let temporary = directory.join(format!("{id}.sqlite.tmp"));
    {
        let source = Connection::open(source).with_context(|| format!("open {}", source.display()))?;
        let mut destination = Connection::open(&temporary)?;
        fs::set_permissions(&temporary, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
        let backup = Backup::new(&source, &mut destination)?;
        backup.run_to_completion(128, Duration::from_millis(10), None)?;
    }
    let (schema_version, config_revision) = integrity_metadata(&temporary)?;
    let (sha256, database_size) = checksum(&temporary)?;
    File::open(&temporary)?.sync_all()?;
    fs::rename(&temporary, &final_database)?;
    sync_directory(directory)?;
    let manifest = BackupManifest {
        id,
        created_at: OffsetDateTime::now_utc().format(&Rfc3339)?,
        lmt_version: env!("CARGO_PKG_VERSION").into(),
        schema_version,
        config_revision,
        database_size,
        sha256,
    };
    publish_manifest(directory, &manifest)?;
    Ok(manifest)
}

pub fn list(directory: &Path) -> anyhow::Result<Vec<BackupManifest>> {
    let mut manifests = Vec::new();
    match fs::read_dir(directory) {
        Ok(entries) => {
            for entry in entries {
                let path = entry?.path();
                if path.extension().and_then(|value| value.to_str()) != Some("json") {
                    continue;
                }
                let manifest: BackupManifest = match serde_json::from_reader(File::open(&path)?) {
                    Ok(manifest) => manifest,
                    Err(_) => continue,
                };
                let (database, _) = paths(directory, &manifest.id);
                if database.is_file() {
                    manifests.push(manifest);
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    manifests.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    Ok(manifests)
}

pub fn verify(directory: &Path, id: &str) -> anyhow::Result<BackupManifest> {
    validate_id(id)?;
    let (database, manifest_path) = paths(directory, id);
    let manifest: BackupManifest = serde_json::from_reader(
        File::open(&manifest_path).with_context(|| format!("backup_invalid: open {}", manifest_path.display()))?,
    )?;
    if manifest.id != id {
        bail!("backup_invalid: manifest ID mismatch");
    }
    let (actual, size) = checksum(&database).context("backup_invalid: checksum backup")?;
    if actual != manifest.sha256 || size != manifest.database_size {
        bail!("backup_invalid: checksum or size mismatch");
    }
    let (schema, revision) = integrity_metadata(&database)?;
    if schema != manifest.schema_version || revision != manifest.config_revision {
        bail!("backup_invalid: manifest metadata mismatch");
    }
    Ok(manifest)
}

pub fn verify_path(path: &Path) -> anyhow::Result<()> {
    integrity_metadata(path).map(|_| ())
}

pub fn create_at(source: &Path, output: &Path) -> anyhow::Result<BackupManifest> {
    if output.exists() || output.with_extension("json").exists() {
        bail!("backup output already exists");
    }
    let directory = output
        .parent()
        .ok_or_else(|| anyhow::anyhow!("backup output has no parent"))?;
    let manifest = create(source, directory)?;
    let (database, manifest_path) = paths(directory, &manifest.id);
    fs::rename(database, output)?;
    fs::rename(manifest_path, output.with_extension("json"))?;
    sync_directory(directory)?;
    Ok(manifest)
}

pub fn verify_file(path: &Path) -> anyhow::Result<()> {
    let manifest_path = path.with_extension("json");
    let manifest: BackupManifest = serde_json::from_reader(
        File::open(&manifest_path).with_context(|| format!("backup_invalid: open {}", manifest_path.display()))?,
    )?;
    let (actual, size) = checksum(path)?;
    if actual != manifest.sha256 || size != manifest.database_size {
        bail!("backup_invalid: checksum or size mismatch");
    }
    let (schema, revision) = integrity_metadata(path)?;
    if schema != manifest.schema_version || revision != manifest.config_revision {
        bail!("backup_invalid: manifest metadata mismatch");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Seek, SeekFrom},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    #[tokio::test]
    async fn online_backup_is_consistent_during_concurrent_writes() {
        let directory = tempfile::tempdir().expect("tempdir");
        let source = directory.path().join("live.db");
        let _store = lmt_store::Store::open(&source).await.expect("store");
        Connection::open(&source)
            .expect("writer setup")
            .execute_batch("CREATE TABLE backup_probe(value INTEGER NOT NULL) STRICT;")
            .expect("probe table");
        let stopping = Arc::new(AtomicBool::new(false));
        let writer_path = source.clone();
        let writer_stopping = stopping.clone();
        let writer = std::thread::spawn(move || {
            let connection = Connection::open(writer_path).expect("writer");
            let mut value = 0_i64;
            while !writer_stopping.load(Ordering::Relaxed) {
                connection
                    .execute("INSERT INTO backup_probe(value) VALUES(?1)", [value])
                    .expect("insert");
                value += 1;
            }
        });
        let backup_dir = directory.path().join("backups");
        let manifest = create(&source, &backup_dir).expect("online backup");
        stopping.store(true, Ordering::Relaxed);
        writer.join().expect("writer thread");
        verify(&backup_dir, &manifest.id).expect("valid consistent backup");
        let (database, _) = paths(&backup_dir, &manifest.id);
        let count: i64 = Connection::open(database)
            .expect("backup")
            .query_row("SELECT COUNT(*) FROM backup_probe", [], |row| row.get(0))
            .expect("probe count");
        assert!(count >= 0);
    }

    #[tokio::test]
    async fn verification_detects_corruption_and_listing_ignores_temporary_files() {
        let directory = tempfile::tempdir().expect("tempdir");
        let source = directory.path().join("live.db");
        let _store = lmt_store::Store::open(&source).await.expect("store");
        let backup_dir = directory.path().join("backups");
        fs::create_dir_all(&backup_dir).expect("backup dir");
        fs::write(backup_dir.join("incomplete.sqlite.tmp"), b"partial").expect("temporary");
        assert!(list(&backup_dir).expect("list").is_empty());
        let manifest = create(&source, &backup_dir).expect("backup");
        let (database, _) = paths(&backup_dir, &manifest.id);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(database)
            .expect("backup file");
        file.seek(SeekFrom::Start(128)).expect("seek");
        file.write_all(b"corrupt").expect("corrupt");
        file.sync_all().expect("sync corruption");
        assert!(verify(&backup_dir, &manifest.id).is_err());
    }
}
