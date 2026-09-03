use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{AtomicPublicationSpec, AttemptNo, MirrorName, NodeName, ProcessRunSpec, RunId, ScheduleConfig};

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication: Option<PublicationConfig>,
    #[serde(default)]
    pub runner: ProcessRunner,
    #[serde(default)]
    pub run: RunPolicy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PublicationConfig {
    pub mode: PublicationMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PublicationMode {
    Direct,
    Atomic,
}

impl MirrorDocument {
    pub fn publication_mode(&self) -> PublicationMode {
        self.publication
            .as_ref()
            .map_or(PublicationMode::Direct, |publication| publication.mode)
    }
}

pub fn publication_mode_from_toml(source: &str) -> Result<PublicationMode, ConfigError> {
    toml::from_str::<MirrorDocument>(source)
        .map(|document| document.publication_mode())
        .map_err(|error| ConfigError::InvalidToml {
            path: "<stored mirror generation>".into(),
            message: error.to_string(),
        })
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
    #[error("atomic publication requires an Agent publication root")]
    MissingPublicationRoot,
    #[error("mirror targets overlap on Node {node}: {first} and {second}")]
    TargetOverlap {
        node: NodeName,
        first: MirrorName,
        second: MirrorName,
    },
    #[error("atomic rsync option {option:?} is invalid: {reason}")]
    AtomicRsyncOption { option: String, reason: &'static str },
}

#[derive(Debug, Clone)]
pub struct RunSpecContext<'a> {
    pub mirror_name: &'a MirrorName,
    pub run_id: RunId,
    pub attempt_no: AttemptNo,
    pub node_name: &'a NodeName,
    pub mirror_root: &'a Path,
    pub publication_root: Option<&'a Path>,
}

pub fn compile_process_run_spec(
    document: &MirrorDocument,
    context: &RunSpecContext<'_>,
) -> Result<ProcessRunSpec, ConfigError> {
    let mirror_root = context.mirror_root.to_string_lossy();
    let published_dir = context
        .mirror_root
        .join(&document.mirror.target)
        .to_string_lossy()
        .into_owned();
    let publication = match document.publication_mode() {
        PublicationMode::Direct => None,
        PublicationMode::Atomic => {
            let publication_root = context.publication_root.ok_or(ConfigError::MissingPublicationRoot)?;
            let mirror_private = publication_root.join(context.mirror_name.as_str());
            let attempt_private =
                mirror_private
                    .join("attempts")
                    .join(format!("{}-{}", context.run_id, context.attempt_no.get()));
            Some(Box::new(AtomicPublicationSpec {
                mirror: context.mirror_name.to_string(),
                publication_root: publication_root.to_string_lossy().into_owned(),
                published_dir: published_dir.clone(),
                candidate_dir: attempt_private.join("root").to_string_lossy().into_owned(),
                basis_dir: attempt_private.join("basis").to_string_lossy().into_owned(),
                exchange_dir: mirror_private.join("exchange").to_string_lossy().into_owned(),
                gc_dir: mirror_private.join("gc").to_string_lossy().into_owned(),
            }))
        }
    };
    let target_dir = publication.as_ref().map_or_else(
        || published_dir.clone(),
        |publication| publication.candidate_dir.clone(),
    );
    let resolve = |value: &str| {
        value
            .replace("{mirror_name}", context.mirror_name.as_str())
            .replace("{run_id}", &context.run_id.to_string())
            .replace("{attempt}", &context.attempt_no.get().to_string())
            .replace("{node_name}", context.node_name.as_str())
            .replace("{mirror_root}", &mirror_root)
            .replace("{target_dir}", &target_dir)
            .replace("{published_dir}", &published_dir)
    };
    let (program, args, cwd) = match &document.sync {
        SyncConfig::Command { program, args, cwd } => (
            resolve(program),
            args.iter().map(|value| resolve(value)).collect(),
            cwd.as_deref().map(resolve),
        ),
        SyncConfig::Rsync { source, args } => {
            let mut compiled = args.clone();
            if let Some(publication) = &publication {
                compiled.push(format!("--link-dest={}", publication.basis_dir));
            }
            compiled.push("--".into());
            compiled.push(source.clone());
            compiled.push(format!("{}/", target_dir.trim_end_matches('/')));
            ("rsync".into(), compiled, None)
        }
    };
    Ok(ProcessRunSpec {
        runner: "process".into(),
        program,
        args,
        cwd,
        timeout_seconds: document.run.timeout_seconds,
        mirror_root: mirror_root.into_owned(),
        target_dir,
        publication,
    })
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
    errors.extend(target_overlap_errors(&mirrors));
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
        SyncConfig::Rsync { args, .. } => {
            if document.publication_mode() == PublicationMode::Atomic {
                validate_atomic_rsync_args(args)?;
            }
        }
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

fn target_overlap_errors(mirrors: &BTreeMap<MirrorName, CanonicalMirror>) -> Vec<ConfigError> {
    let entries: Vec<_> = mirrors.iter().collect();
    let mut errors = Vec::new();
    for (index, (first_name, first)) in entries.iter().enumerate() {
        for (second_name, second) in entries.iter().skip(index + 1) {
            if first.owner_node != second.owner_node {
                continue;
            }
            let first_target = Path::new(&first.document.mirror.target);
            let second_target = Path::new(&second.document.mirror.target);
            if first_target.starts_with(second_target) || second_target.starts_with(first_target) {
                errors.push(ConfigError::TargetOverlap {
                    node: first.owner_node.clone(),
                    first: (*first_name).clone(),
                    second: (*second_name).clone(),
                });
            }
        }
    }
    errors
}

#[derive(Clone, Copy)]
enum AtomicRsyncOptionKind {
    Flag,
    Value,
    Rejected(&'static str),
}

fn atomic_rsync_long_option(name: &str) -> Option<AtomicRsyncOptionKind> {
    use AtomicRsyncOptionKind::{Flag, Rejected, Value};
    match name {
        "archive" | "recursive" | "links" | "perms" | "times" | "group" | "owner" | "devices" | "specials"
        | "hard-links" | "acls" | "xattrs" | "numeric-ids" | "prune-empty-dirs" | "compress" | "whole-file"
        | "checksum" | "size-only" | "ignore-times" | "protect-args" | "itemize-changes" | "stats"
        | "human-readable" | "verbose" | "quiet" | "progress" | "copy-links" | "safe-links" | "copy-unsafe-links" => {
            Some(Flag)
        }
        "include" | "exclude" | "filter" | "include-from" | "exclude-from" | "files-from" | "max-size" | "min-size"
        | "bwlimit" | "timeout" | "contimeout" | "block-size" | "checksum-choice" | "compress-choice" => Some(Value),
        "delete"
        | "delete-before"
        | "delete-during"
        | "delete-delay"
        | "delete-after"
        | "delete-excluded"
        | "max-delete"
        | "force"
        | "ignore-errors"
        | "existing"
        | "ignore-existing"
        | "ignore-non-existing"
        | "update" => Some(Rejected(
            "fresh-generation Atomic publication has no existing destination history",
        )),
        "inplace"
        | "append"
        | "append-verify"
        | "write-devices"
        | "link-dest"
        | "copy-dest"
        | "compare-dest"
        | "backup"
        | "backup-dir"
        | "suffix"
        | "partial"
        | "partial-dir"
        | "remove-source-files"
        | "remove-sent-files"
        | "dry-run"
        | "list-only" => Some(Rejected(
            "option conflicts with LMT-owned Atomic candidate materialization",
        )),
        _ => None,
    }
}

fn validate_atomic_rsync_args(args: &[String]) -> Result<(), ConfigError> {
    let mut index = 0;
    while index < args.len() {
        let argument = &args[index];
        if let Some(long) = argument.strip_prefix("--") {
            let (name, inline_value) = long
                .split_once('=')
                .map_or((long, None), |(name, value)| (name, Some(value)));
            match atomic_rsync_long_option(name) {
                Some(AtomicRsyncOptionKind::Flag) if inline_value.is_none() => {}
                Some(AtomicRsyncOptionKind::Flag) => {
                    return Err(ConfigError::AtomicRsyncOption {
                        option: argument.clone(),
                        reason: "flag does not accept a value",
                    });
                }
                Some(AtomicRsyncOptionKind::Value) if inline_value.is_some_and(|value| !value.is_empty()) => {}
                Some(AtomicRsyncOptionKind::Value)
                    if inline_value.is_none() && index + 1 < args.len() && !args[index + 1].starts_with('-') =>
                {
                    index += 1;
                }
                Some(AtomicRsyncOptionKind::Value) => {
                    return Err(ConfigError::AtomicRsyncOption {
                        option: argument.clone(),
                        reason: "option requires a value",
                    });
                }
                Some(AtomicRsyncOptionKind::Rejected(reason)) => {
                    return Err(ConfigError::AtomicRsyncOption {
                        option: argument.clone(),
                        reason,
                    });
                }
                None => {
                    return Err(ConfigError::AtomicRsyncOption {
                        option: argument.clone(),
                        reason: "option is not in the audited Atomic profile",
                    });
                }
            }
        } else if let Some(short) = argument.strip_prefix('-') {
            if short.is_empty()
                || short.chars().any(|option| {
                    !matches!(
                        option,
                        'a' | 'r' | 'l' | 'p' | 't' | 'g' | 'o' | 'D' | 'H' | 'A' | 'X' | 'z' | 's' | 'v' | 'q'
                    )
                })
            {
                return Err(ConfigError::AtomicRsyncOption {
                    option: argument.clone(),
                    reason: "short option is not in the audited Atomic profile",
                });
            }
        } else {
            return Err(ConfigError::AtomicRsyncOption {
                option: argument.clone(),
                reason: "positional arguments are owned by LMT",
            });
        }
        index += 1;
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
            "mirror_name" | "run_id" | "attempt" | "node_name" | "mirror_root" | "target_dir" | "published_dir"
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
                publication_root: None,
            },
        )
        .expect("compile direct spec");
        assert_eq!(spec.program, "rsync");
        assert_eq!(
            spec.args,
            ["--archive", "--delete", "--", "local/source/", "/srv/mirrors/example/"]
        );
        assert_eq!(spec.publication, None);
    }

    #[test]
    fn atomic_config_compiles_a_fresh_private_candidate_and_published_placeholder() {
        let canonical = canonicalize_bundle(&bundle(
            "[mirror]\nname='example'\ntarget='example'\n[sync]\ntype='command'\nprogram='/bin/sync'\nargs=['{target_dir}','{published_dir}']\n[publication]\nmode='atomic'\n",
        ))
        .expect("valid Atomic config");
        let document = &canonical.mirrors.values().next().expect("mirror").document;
        let mirror = MirrorName::new("example").expect("name");
        let node = NodeName::new("node-a").expect("node");
        let run_id = RunId::new();
        let spec = compile_process_run_spec(
            document,
            &RunSpecContext {
                mirror_name: &mirror,
                run_id,
                attempt_no: AttemptNo::new(2).expect("attempt"),
                node_name: &node,
                mirror_root: Path::new("/srv/mirrors"),
                publication_root: Some(Path::new("/srv/publication")),
            },
        )
        .expect("compile Atomic spec");
        let publication = spec.publication.as_ref().expect("publication extension");
        assert_eq!(publication.published_dir, "/srv/mirrors/example");
        assert_eq!(spec.target_dir, publication.candidate_dir);
        assert_eq!(spec.args, [publication.candidate_dir.as_str(), "/srv/mirrors/example"]);
        assert!(publication.candidate_dir.ends_with(&format!("{run_id}-2/root")));
        assert_eq!(publication.exchange_dir, "/srv/publication/example/exchange");
        assert_eq!(publication.gc_dir, "/srv/publication/example/gc");
    }

    #[test]
    fn same_node_targets_cannot_overlap_but_cross_node_targets_may_match() {
        for second_target in ["archive", "archive/pool"] {
            let overlapping = ConfigBundle {
                files: vec![
                    BundleFile {
                        path: "nodes/node-a/mirrors/first.toml".into(),
                        contents:
                            "[mirror]\nname='first'\ntarget='archive'\n[sync]\ntype='command'\nprogram='/bin/true'\n"
                                .into(),
                    },
                    BundleFile {
                        path: "nodes/node-a/mirrors/second.toml".into(),
                        contents: format!(
                            "[mirror]\nname='second'\ntarget='{second_target}'\n[sync]\ntype='command'\nprogram='/bin/true'\n"
                        ),
                    },
                ],
            };
            assert!(
                canonicalize_bundle(&overlapping)
                    .expect_err("overlap")
                    .iter()
                    .any(|error| matches!(error, ConfigError::TargetOverlap { .. }))
            );
        }

        let separate_nodes = ConfigBundle {
            files: vec![
                BundleFile {
                    path: "nodes/node-a/mirrors/first.toml".into(),
                    contents: "[mirror]\nname='first'\ntarget='archive'\n[sync]\ntype='command'\nprogram='/bin/true'\n"
                        .into(),
                },
                BundleFile {
                    path: "nodes/node-b/mirrors/second.toml".into(),
                    contents:
                        "[mirror]\nname='second'\ntarget='archive'\n[sync]\ntype='command'\nprogram='/bin/true'\n"
                            .into(),
                },
            ],
        };
        assert!(canonicalize_bundle(&separate_nodes).is_ok());
    }

    #[test]
    fn atomic_rsync_profile_is_allowlisted_and_rejects_history_or_owned_options() {
        let accepted = [
            "-aH",
            "--numeric-ids",
            "--exclude=tmp/***",
            "--files-from",
            "manifest.txt",
            "--bwlimit=10m",
            "--timeout",
            "60",
            "--checksum",
            "--stats",
        ]
        .map(str::to_owned);
        validate_atomic_rsync_args(&accepted).expect("audited profile");

        for option in [
            "--delete",
            "--inplace",
            "--link-dest=/old",
            "--mystery",
            "-an",
            "source/",
        ] {
            let error = validate_atomic_rsync_args(&[option.into()]).expect_err("rejected option");
            assert!(matches!(error, ConfigError::AtomicRsyncOption { .. }));
        }
        assert!(
            validate_atomic_rsync_args(&["--timeout".into(), "--delete".into()]).is_err(),
            "an option requiring a value consumed a rejected option"
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
                "rsync-atomic.toml",
                include_str!("../../../config/nodes/mirror01/mirrors/rsync-atomic.toml"),
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
        assert_eq!(canonical.mirrors.len(), 5);
    }
}
