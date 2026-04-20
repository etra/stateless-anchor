use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

/// Top-level config. Built once at startup; component crates consume the
/// section they need.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub http: HttpConfig,
    pub iceberg: IcebergConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
        }
    }
}

fn default_bind() -> SocketAddr {
    // 0.0.0.0:8080 is the only baked-in fallback — everything else is
    // required in config.yaml so misconfiguration fails fast at startup.
    "0.0.0.0:8080".parse().expect("valid default bind address")
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergConfig {
    pub catalog_uri: String,
    pub warehouse: String,
    /// OAuth2 bearer token for the REST catalog, if the catalog requires
    /// one. For Cloudflare R2 Data Catalog this is the Cloudflare API
    /// token (value starting with `cfat_`). iceberg-rust reads it as the
    /// `token` catalog property.
    #[serde(default)]
    pub token: Option<String>,
    /// Pass-through properties to the REST catalog / FileIO, e.g.
    /// `s3.endpoint`, `s3.access-key-id`, `s3.secret-access-key`, `s3.region`.
    #[serde(default)]
    pub storage: HashMap<String, String>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_yaml_ng::Error,
    },
}

impl Config {
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|e| ConfigError::Read {
            path: path.to_path_buf(),
            source: e,
        })?;
        serde_yaml_ng::from_str(&text).map_err(|e| ConfigError::Parse {
            path: path.to_path_buf(),
            source: e,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_apply_when_http_omitted() {
        let yaml = r#"
iceberg:
  catalog_uri: http://localhost:8181
  warehouse: s3://warehouse
"#;
        let cfg: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.http.bind, default_bind());
        assert_eq!(cfg.iceberg.catalog_uri, "http://localhost:8181");
        assert!(cfg.iceberg.storage.is_empty());
    }

    #[test]
    fn storage_props_parse() {
        let yaml = r#"
iceberg:
  catalog_uri: http://localhost:8181
  warehouse: s3://warehouse
  storage:
    s3.endpoint: http://minio:9000
    s3.region: us-east-1
"#;
        let cfg: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(
            cfg.iceberg.storage.get("s3.endpoint").map(String::as_str),
            Some("http://minio:9000")
        );
    }

    #[test]
    fn missing_required_field_errors() {
        let yaml = "iceberg: { warehouse: s3://w }";
        let err = serde_yaml_ng::from_str::<Config>(yaml).unwrap_err();
        assert!(err.to_string().contains("catalog_uri"), "got: {err}");
    }

    #[test]
    fn token_is_optional_and_parses_when_set() {
        let yaml = r#"
iceberg:
  catalog_uri: https://example.com
  warehouse: demo
  token: cfat_example
"#;
        let cfg: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.iceberg.token.as_deref(), Some("cfat_example"));
    }
}
