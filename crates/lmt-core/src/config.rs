use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{AttemptNo, MirrorName, NodeName, ProcessRunSpec, RunId};

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
    pub sync: CommandSync,
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
#[serde(deny_unknown_fields)]
pub struct CommandSync {
    #[serde(rename = "type")]
    pub kind: CommandKind,
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CommandKind {
    Command,
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
    #[error("M1 supports exactly one attempt")]
    UnsupportedRetries,
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
    ProcessRunSpec {
        runner: "process".into(),
        program: resolve(&document.sync.program),
        args: document.sync.args.iter().map(|value| resolve(value)).collect(),
        cwd: document.sync.cwd.as_deref().map(resolve),
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
    let document: MirrorDocument = toml::from_str(&file.contents).map_err(|error| ConfigError::InvalidToml {
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
    if document.sync.program.is_empty() {
        return Err(ConfigError::EmptyProgram);
    }
    if !(1..=604_800).contains(&document.run.timeout_seconds) {
        return Err(ConfigError::InvalidTimeout);
    }
    if document.run.max_attempts != 1 {
        return Err(ConfigError::UnsupportedRetries);
    }
    for value in std::iter::once(&document.sync.program)
        .chain(document.sync.args.iter())
        .chain(document.sync.cwd.iter())
    {
        validate_placeholders(value)?;
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
}
