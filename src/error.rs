use std::{io, path::PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read configuration file {path}: {source}")]
    ReadConfig { path: PathBuf, source: io::Error },

    #[error("invalid TOML configuration: {0}")]
    ParseConfig(#[from] toml::de::Error),

    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl ConfigError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}
