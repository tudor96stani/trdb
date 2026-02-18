use serde::Deserialize;
use std::path::PathBuf;
use std::{num::NonZeroUsize, path::Path};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("IO Error")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Parse Error")]
    ParseToml {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("Invalid TOML error")]
    Invalid { message: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub logs_dir: PathBuf,
    pub buffer_pages: NonZeroUsize,
}
impl EngineConfig {
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref().to_path_buf();
        let text = std::fs::read_to_string(&path).map_err(|e| ConfigError::Io {
            path: path.clone(),
            source: e,
        })?;

        let cfg: EngineConfig = toml::from_str(&text).map_err(|e| ConfigError::ParseToml {
            path: path.clone(),
            source: e,
        })?;

        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        // buffer_pages is already NonZeroUsize, so "0" can't happen.
        // Validate data_dir not empty / etc.
        if self.storage.data_dir.as_os_str().is_empty() {
            return Err(ConfigError::Invalid {
                message: "storage.data_dir must not be empty".to_string(),
            });
        }
        Ok(())
    }
}
