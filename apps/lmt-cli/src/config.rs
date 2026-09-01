use std::{fs, path::PathBuf};

use clap::ValueEnum;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum OutputMode {
    Human,
    Json,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientToml {
    server: String,
    token_file: PathBuf,
    #[serde(default)]
    output: Option<OutputMode>,
}

pub struct ClientSettings {
    pub server: String,
    pub token_file: PathBuf,
    pub output: OutputMode,
}

impl ClientSettings {
    pub fn resolve(
        config: Option<PathBuf>,
        server: Option<String>,
        token_file: Option<PathBuf>,
        output: Option<OutputMode>,
    ) -> Result<Self, String> {
        let explicit_config = config.is_some();
        let path = config.or_else(default_path);
        let file = match path {
            Some(path) => match fs::read_to_string(&path) {
                Ok(source) => Some(toml::from_str::<ClientToml>(&source).map_err(|error| error.to_string())?),
                Err(error) if !explicit_config && error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(format!("read {}: {error}", path.display())),
            },
            None => None,
        };
        let server = server
            .or_else(|| file.as_ref().map(|config| config.server.clone()))
            .unwrap_or_else(|| "http://127.0.0.1:8080".into());
        let token_file = token_file
            .or_else(|| file.as_ref().map(|config| config.token_file.clone()))
            .ok_or_else(|| "token_file is required in client TOML or --token-file".to_owned())?;
        let output = output
            .or_else(|| file.as_ref().and_then(|config| config.output))
            .unwrap_or(OutputMode::Human);
        Ok(Self {
            server,
            token_file,
            output,
        })
    }
}

fn default_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config/lmt/client.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_override_visible_client_toml() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("client.toml");
        fs::write(&path, "server='http://file'\ntoken_file='/file-token'\noutput='json'\n").expect("config");
        let resolved = ClientSettings::resolve(
            Some(path),
            Some("http://flag".into()),
            Some("/flag-token".into()),
            Some(OutputMode::Human),
        )
        .expect("resolve");
        assert_eq!(resolved.server, "http://flag");
        assert_eq!(resolved.token_file, PathBuf::from("/flag-token"));
        assert!(matches!(resolved.output, OutputMode::Human));
    }
}
