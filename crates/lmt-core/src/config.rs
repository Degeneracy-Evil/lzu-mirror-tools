use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{AttemptNo, MirrorName, NodeName, ProcessRunSpec, RunId, ScheduleConfig};

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BundleFile {
    pub path: String,
    pub contents: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConfigBundle {
    pub files: Vec<BundleFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MirrorDocument {
    pub mirror: MirrorConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleConfig>,
    pub sync: SyncConfig,
    #[serde(default)]
    pub runner: ProcessRunner,
    #[serde(default)]
    pub run: RunPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MirrorConfig {
    pub name: MirrorName,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub description: String,
    pub target: String,
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SyncConfig {
    Command {
        program: String,
        #[serde(default)]
        args: Vec<String>,
        cwd: Option<String>,
    },
    Rsync {
        source: String,
        #[serde(default)]
        args: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProcessRunner {
    #[serde(rename = "type")]
    pub kind: ProcessRunnerKind,
}

impl Default for ProcessRunner {
    fn default() -> Self {
        Self {
            kind: ProcessRunnerKind::Process,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ProcessRunnerKind {
    Process,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RunPolicy {
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub retry_delay_seconds: u64,
}

const fn default_timeout_seconds() -> u64 {
    3600
}

const fn default_max_attempts() -> u32 {
    1
}

impl Default for RunPolicy {
    fn default() -> Self {
        Self {
            timeout_seconds: default_timeout_seconds(),
            max_attempts: 1,
            retry_delay_seconds: 0,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CanonicalMirror {
    pub owner_node: NodeName,
    pub document: MirrorDocument,
    pub canonical_toml: String,
    pub config_hash: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CanonicalBundle {
    pub mirrors: BTreeMap<MirrorName, CanonicalMirror>,
    pub bundle_hash: String,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ConfigError {
    #[error("invalid bundle path {path:?}: expected nodes/<node>/mirrors/<name>.toml")]
    InvalidPath { path: String },
    #[error("invalid TOML in {path}: {message}")]
    InvalidToml { path: String, message: String },
    #[error("mirror {name} is defined more than once")]
    DuplicateMirror { name: MirrorName },
    #[error("mirror file stem {stem:?} does not match mirror.name {name}")]
    NameMismatch { stem: String, name: MirrorName },
    #[error("mirror target must be a safe relative path: {0:?}")]
    UnsafeTarget(String),
    #[error("command program must not be empty")]
    EmptyProgram,
    #[error("timeout_seconds must be between 1 and 604800")]
    InvalidTimeout,
    #[error("max_attempts must be between 1 and 10")]
    InvalidMaxAttempts,
    #[error("retry_delay_seconds must not exceed 86400")]
    InvalidRetryDelay,
    #[error("rsync source must not be empty")]
    EmptyRsyncSource,
    #[error("invalid schedule: {0}")]
    InvalidSchedule(String),
    #[error("unsupported placeholder {0}")]
    UnsupportedPlaceholder(String),
}

#[derive(Debug, Clone)]
pub struct RunSpecContext<'a> {
    pub mirror_name: &'a MirrorName,
    pub run_id: RunId,
    pub attempt_no: AttemptNo,
    pub node_name: &'a NodeName,
    pub mirror_root: &'a Path,
}

pub fn compile_process_run_spec(document: &MirrorDocument, context: &RunSpecContext<'_>) -> ProcessRunSpec {
    let mirror_root = context.mirror_root.to_string_lossy();
    let target_dir = context
        .mirror_root
        .join(&document.mirror.target)
        .to_string_lossy()
        .into_owned();
    let resolve = |value: &str| {
        value
            .replace("{mirror_name}", context.mirror_name.as_str())
            .replace("{run_id}", &context.run_id.to_string())
            .replace("{attempt}", &context.attempt_no.get().to_string())
            .replace("{node_name}", context.node_name.as_str())
            .replace("{mirror_root}", &mirror_root)
            .replace("{target_dir}", &target_dir)
    };
    let (program, args, cwd) = match &document.sync {
        SyncConfig::Command { program, args, cwd } => (
            resolve(program),
            args.iter().map(|value| resolve(value)).collect(),
            cwd.as_deref().map(resolve),
        ),
        SyncConfig::Rsync { source, args } => {
            let mut compiled = args.clone();
            compiled.push("--".into());
            compiled.push(source.clone());
            compiled.push(format!("{}/", target_dir.trim_end_matches('/')));
            ("rsync".into(), compiled, None)
        }
    };
    ProcessRunSpec {
        runner: "process".into(),
        program,
        args,
        cwd,
        timeout_seconds: document.run.timeout_seconds,
        mirror_root: mirror_root.into_owned(),
        target_dir,
    }
}

pub fn canonicalize_bundle(bundle: &ConfigBundle) -> Result<CanonicalBundle, Vec<ConfigError>> {
    let mut errors = Vec::new();
    let mut mirrors = BTreeMap::new();
    for file in &bundle.files {
        match canonicalize_file(file) {
            Ok((name, mirror)) => {
                if mirrors.insert(name.clone(), mirror).is_some() {
                    errors.push(ConfigError::DuplicateMirror { name });
                }
            }
            Err(error) => errors.push(error),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    let mut hasher = Sha256::new();
    for (name, mirror) in &mirrors {
        hasher.update(name.as_str());
        hasher.update([0]);
        hasher.update(mirror.owner_node.as_str());
        hasher.update([0]);
        hasher.update(&mirror.canonical_toml);
        hasher.update([0]);
    }
    Ok(CanonicalBundle {
        mirrors,
        bundle_hash: format!("sha256:{}", hex::encode(hasher.finalize())),
    })
}

fn canonicalize_file(file: &BundleFile) -> Result<(MirrorName, CanonicalMirror), ConfigError> {
    let path = Path::new(&file.path);
    let parts: Vec<_> = path.components().collect();
    if parts.len() != 4
        || parts[0].as_os_str() != "nodes"
        || parts[2].as_os_str() != "mirrors"
        || path.extension().and_then(|value| value.to_str()) != Some("toml")
    {
        return Err(ConfigError::InvalidPath {
            path: file.path.clone(),
        });
    }
    let node =
        NodeName::new(parts[1].as_os_str().to_string_lossy().into_owned()).map_err(|_| ConfigError::InvalidPath {
            path: file.path.clone(),
        })?;
    let stem = path.file_stem().and_then(|value| value.to_str()).unwrap_or_default();
    let mut document: MirrorDocument = toml::from_str(&file.contents).map_err(|error| ConfigError::InvalidToml {
        path: file.path.clone(),
        message: error.to_string(),
    })?;
    if stem != document.mirror.name.as_str() {
        return Err(ConfigError::NameMismatch {
            stem: stem.to_owned(),
            name: document.mirror.name,
        });
    }
    validate_document(&document)?;
    document.schedule = document
        .schedule
        .take()
        .map(ScheduleConfig::canonicalized)
        .transpose()
        .map_err(ConfigError::InvalidSchedule)?;
    let canonical_toml = toml::to_string(&document).map_err(|error| ConfigError::InvalidToml {
        path: file.path.clone(),
        message: error.to_string(),
    })?;
    let config_hash = format!("sha256:{}", hex::encode(Sha256::digest(canonical_toml.as_bytes())));
    let name = document.mirror.name.clone();
    Ok((
        name,
        CanonicalMirror {
            owner_node: node,
            document,
            canonical_toml,
            config_hash,
        },
    ))
}

fn validate_document(document: &MirrorDocument) -> Result<(), ConfigError> {
    let target = Path::new(&document.mirror.target);
    if document.mirror.target.is_empty()
        || target.is_absolute()
        || target
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(ConfigError::UnsafeTarget(document.mirror.target.clone()));
    }
    match &document.sync {
        SyncConfig::Command { program, args, cwd } => {
            if program.is_empty() {
                return Err(ConfigError::EmptyProgram);
            }
            for value in std::iter::once(program).chain(args).chain(cwd.iter()) {
                validate_placeholders(value)?;
            }
        }
        SyncConfig::Rsync { source, .. } if source.is_empty() => return Err(ConfigError::EmptyRsyncSource),
        SyncConfig::Rsync { .. } => {}
    }
    if !(1..=604_800).contains(&document.run.timeout_seconds) {
        return Err(ConfigError::InvalidTimeout);
    }
    if !(1..=10).contains(&document.run.max_attempts) {
        return Err(ConfigError::InvalidMaxAttempts);
    }
    if document.run.retry_delay_seconds > 86_400 {
        return Err(ConfigError::InvalidRetryDelay);
    }
    if let Some(schedule) = &document.schedule {
        schedule.validate().map_err(ConfigError::InvalidSchedule)?;
    }
    Ok(())
}

fn validate_placeholders(value: &str) -> Result<(), ConfigError> {
    let mut rest = value;
    while let Some(open) = rest.find(['{', '}']) {
        if rest.as_bytes()[open] == b'}' {
            return Err(ConfigError::UnsupportedPlaceholder(rest[open..].to_owned()));
        }
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            return Err(ConfigError::UnsupportedPlaceholder(after.to_owned()));
        };
        let placeholder = &after[..close];
        if !matches!(
            placeholder,
            "mirror_name" | "run_id" | "attempt" | "node_name" | "mirror_root" | "target_dir"
        ) {
            return Err(ConfigError::UnsupportedPlaceholder(placeholder.to_owned()));
        }
        rest = &after[close + 1..];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bundle(contents: &str) -> ConfigBundle {
        ConfigBundle {
            files: vec![BundleFile {
                path: "nodes/node-a/mirrors/example.toml".into(),
                contents: contents.into(),
            }],
        }
    }

    #[test]
    fn canonicalization_ignores_formatting() {
        let a = canonicalize_bundle(&bundle(
            "[mirror]\nname='example'\ntarget='example'\n[sync]\ntype='command'\nprogram='/bin/true'\n",
        ))
        .expect("valid");
        let b = canonicalize_bundle(&bundle(
            "[mirror]\nname = \"example\"\ntarget = \"example\"\n\n[sync]\ntype = \"command\"\nprogram = \"/bin/true\"\n",
        ))
        .expect("valid");
        assert_eq!(a.bundle_hash, b.bundle_hash);
    }

    #[test]
    fn target_traversal_is_rejected() {
        let errors = canonicalize_bundle(&bundle(
            "[mirror]\nname='example'\ntarget='../outside'\n[sync]\ntype='command'\nprogram='/bin/true'\n",
        ))
        .expect_err("unsafe");
        assert!(matches!(errors[0], ConfigError::UnsafeTarget(_)));
    }

    #[test]
    fn empty_target_and_malformed_placeholders_are_rejected() {
        for contents in [
            "[mirror]\nname='example'\ntarget=''\n[sync]\ntype='command'\nprogram='/bin/true'\n",
            "[mirror]\nname='example'\ntarget='example'\n[sync]\ntype='command'\nprogram='/bin/{unknown}'\n",
            "[mirror]\nname='example'\ntarget='example'\n[sync]\ntype='command'\nprogram='/bin/true}'\n",
        ] {
            assert!(canonicalize_bundle(&bundle(contents)).is_err());
        }
    }

    #[test]
    fn schedule_and_retry_policy_are_strict_and_canonical() {
        let first = canonicalize_bundle(&bundle(
            "[mirror]\nname='example'\ntarget='example'\n[schedule]\ninterval='60m'\n[sync]\ntype='command'\nprogram='/bin/true'\n[run]\nmax_attempts=10\nretry_delay_seconds=86400\n",
        ))
        .expect("valid");
        let equivalent = canonicalize_bundle(&bundle(
            "[mirror]\nname='example'\ntarget='example'\n[schedule]\ninterval='1h'\n[sync]\ntype='command'\nprogram='/bin/true'\n[run]\nmax_attempts=10\nretry_delay_seconds=86400\n",
        ))
        .expect("valid");
        assert_eq!(first.bundle_hash, equivalent.bundle_hash);

        for run in ["max_attempts=0\n", "max_attempts=11\n", "retry_delay_seconds=86401\n"] {
            assert!(
                canonicalize_bundle(&bundle(&format!(
                    "[mirror]\nname='example'\ntarget='example'\n[sync]\ntype='command'\nprogram='/bin/true'\n[run]\n{run}"
                )))
                .is_err()
            );
        }
    }

    #[test]
    fn rsync_compiles_to_the_generic_process_runner() {
        let canonical = canonicalize_bundle(&bundle(
            "[mirror]\nname='example'\ntarget='example'\n[sync]\ntype='rsync'\nsource='local/source/'\nargs=['--archive','--delete']\n",
        ))
        .expect("valid");
        let document = &canonical.mirrors.values().next().expect("mirror").document;
        let mirror = MirrorName::new("example").expect("name");
        let node = NodeName::new("node-a").expect("node");
        let spec = compile_process_run_spec(
            document,
            &RunSpecContext {
                mirror_name: &mirror,
                run_id: RunId::new(),
                attempt_no: AttemptNo::new(1).expect("attempt"),
                node_name: &node,
                mirror_root: Path::new("/srv/mirrors"),
            },
        );
        assert_eq!(spec.program, "rsync");
        assert_eq!(
            spec.args,
            ["--archive", "--delete", "--", "local/source/", "/srv/mirrors/example/"]
        );
    }

    #[test]
    fn production_trial_mirror_examples_form_a_valid_bundle() {
        let files = [
            (
                "example.toml",
                include_str!("../../../config/nodes/mirror01/mirrors/example.toml"),
            ),
            (
                "rsync-simple.toml",
                include_str!("../../../config/nodes/mirror01/mirrors/rsync-simple.toml"),
            ),
            (
                "rsync-production.toml",
                include_str!("../../../config/nodes/mirror01/mirrors/rsync-production.toml"),
            ),
            (
                "command-hook.toml",
                include_str!("../../../config/nodes/mirror01/mirrors/command-hook.toml"),
            ),
        ]
        .into_iter()
        .map(|(name, contents)| BundleFile {
            path: format!("nodes/mirror01/mirrors/{name}"),
            contents: contents.into(),
        })
        .collect();
        let canonical = canonicalize_bundle(&ConfigBundle { files }).expect("valid examples");
        assert_eq!(canonical.mirrors.len(), 4);
    }
}
