//! Profile resolution for the Kosha CLI.
//!
//! Precedence (highest wins): CLI flags → env vars → selected profile → defaults.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigFile {
    #[serde(default)]
    pub default_profile: Option<String>,
    #[serde(default)]
    pub profiles: BTreeMap<String, Profile>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Profile {
    pub host: Option<String>,
    /// Literal API key stored in the config file.
    pub api_key: Option<String>,
    /// Env var name that holds the API key (preferred over plaintext).
    pub api_key_env: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    pub host: String,
    pub api_key: Option<String>,
    pub profile: Option<String>,
}

#[derive(Debug)]
pub struct ResolveError(pub String);

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ResolveError {}

pub fn default_config_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".kosha")
        .join("config.toml")
}

pub fn load_config(path: &Path) -> Result<ConfigFile, ResolveError> {
    if !path.exists() {
        return Ok(ConfigFile::default());
    }
    let text = fs::read_to_string(path)
        .map_err(|e| ResolveError(format!("failed to read {}: {e}", path.display())))?;
    toml::from_str(&text)
        .map_err(|e| ResolveError(format!("invalid config {}: {e}", path.display())))
}

pub fn save_config(path: &Path, config: &ConfigFile) -> Result<(), ResolveError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| ResolveError(format!("failed to create {}: {e}", parent.display())))?;
    }
    let text = toml::to_string_pretty(config)
        .map_err(|e| ResolveError(format!("failed to serialize config: {e}")))?;
    fs::write(path, text)
        .map_err(|e| ResolveError(format!("failed to write {}: {e}", path.display())))
}

/// Resolve host + API key from flags, env, and config.
///
/// Order:
/// 1. `--host` / `--api-key` flags
/// 2. `KOSHA_HOST` / `KOSHA_API_KEY` env
/// 3. Selected profile (`--profile` → `KOSHA_PROFILE` → `default_profile`)
/// 4. Host fallback `http://localhost:8080`
pub fn resolve_connection(
    config: &ConfigFile,
    profile_flag: Option<&str>,
    host_flag: Option<&str>,
    api_key_flag: Option<&str>,
) -> Result<Connection, ResolveError> {
    let profile_name = profile_flag
        .map(str::to_string)
        .or_else(|| std::env::var("KOSHA_PROFILE").ok())
        .or_else(|| config.default_profile.clone());

    let profile = match profile_name.as_deref() {
        Some(name) => Some(
            config
                .profiles
                .get(name)
                .cloned()
                .ok_or_else(|| ResolveError(format!("unknown profile {name:?}")))?,
        ),
        None => None,
    };

    let host = host_flag
        .map(str::to_string)
        .or_else(|| std::env::var("KOSHA_HOST").ok())
        .or_else(|| profile.as_ref().and_then(|p| p.host.clone()))
        .unwrap_or_else(|| "http://localhost:8080".into())
        .trim_end_matches('/')
        .to_string();

    let api_key = api_key_flag
        .map(str::to_string)
        .or_else(|| std::env::var("KOSHA_API_KEY").ok())
        .or_else(|| {
            profile.as_ref().and_then(|p| {
                if let Some(env_name) = &p.api_key_env {
                    return std::env::var(env_name).ok();
                }
                p.api_key.clone()
            })
        });

    Ok(Connection {
        host,
        api_key,
        profile: profile_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn clear_env() {
        std::env::remove_var("KOSHA_HOST");
        std::env::remove_var("KOSHA_API_KEY");
        std::env::remove_var("KOSHA_PROFILE");
        std::env::remove_var("KOSHA_STAGING_API_KEY");
    }

    #[test]
    fn defaults_to_localhost_without_config() {
        let _guard = env_lock().lock().unwrap();
        clear_env();
        let conn = resolve_connection(&ConfigFile::default(), None, None, None).unwrap();
        assert_eq!(conn.host, "http://localhost:8080");
        assert_eq!(conn.api_key, None);
        assert_eq!(conn.profile, None);
    }

    #[test]
    fn flags_beat_env_and_profile() {
        let _guard = env_lock().lock().unwrap();
        clear_env();
        std::env::set_var("KOSHA_HOST", "http://env:1");
        std::env::set_var("KOSHA_API_KEY", "env-key");

        let config = ConfigFile {
            default_profile: Some("local".into()),
            profiles: BTreeMap::from([(
                "local".into(),
                Profile {
                    host: Some("http://profile:1".into()),
                    api_key: Some("profile-key".into()),
                    api_key_env: None,
                },
            )]),
        };

        let conn =
            resolve_connection(&config, None, Some("http://flag:1/"), Some("flag-key")).unwrap();
        assert_eq!(conn.host, "http://flag:1");
        assert_eq!(conn.api_key.as_deref(), Some("flag-key"));
        clear_env();
    }

    #[test]
    fn env_beats_profile() {
        let _guard = env_lock().lock().unwrap();
        clear_env();
        std::env::set_var("KOSHA_HOST", "http://env:1");
        std::env::set_var("KOSHA_API_KEY", "env-key");

        let config = ConfigFile {
            default_profile: Some("local".into()),
            profiles: BTreeMap::from([(
                "local".into(),
                Profile {
                    host: Some("http://profile:1".into()),
                    api_key: Some("profile-key".into()),
                    api_key_env: None,
                },
            )]),
        };

        let conn = resolve_connection(&config, None, None, None).unwrap();
        assert_eq!(conn.host, "http://env:1");
        assert_eq!(conn.api_key.as_deref(), Some("env-key"));
        assert_eq!(conn.profile.as_deref(), Some("local"));
        clear_env();
    }

    #[test]
    fn profile_api_key_env_is_resolved() {
        let _guard = env_lock().lock().unwrap();
        clear_env();
        std::env::set_var("KOSHA_STAGING_API_KEY", "from-env-ref");

        let config = ConfigFile {
            default_profile: None,
            profiles: BTreeMap::from([(
                "staging".into(),
                Profile {
                    host: Some("https://kosha.example".into()),
                    api_key: Some("plaintext-ignored".into()),
                    api_key_env: Some("KOSHA_STAGING_API_KEY".into()),
                },
            )]),
        };

        let conn = resolve_connection(&config, Some("staging"), None, None).unwrap();
        assert_eq!(conn.host, "https://kosha.example");
        assert_eq!(conn.api_key.as_deref(), Some("from-env-ref"));
        clear_env();
    }

    #[test]
    fn unknown_profile_errors() {
        let _guard = env_lock().lock().unwrap();
        clear_env();
        let err =
            resolve_connection(&ConfigFile::default(), Some("missing"), None, None).unwrap_err();
        assert!(err.to_string().contains("unknown profile"));
    }

    #[test]
    fn round_trip_config_file() {
        let dir = tempfile_dir();
        let path = dir.join("config.toml");
        let config = ConfigFile {
            default_profile: Some("local".into()),
            profiles: BTreeMap::from([(
                "local".into(),
                Profile {
                    host: Some("http://localhost:8080".into()),
                    api_key: Some("sk-kosha-dev".into()),
                    api_key_env: None,
                },
            )]),
        };
        save_config(&path, &config).unwrap();
        let loaded = load_config(&path).unwrap();
        assert_eq!(loaded.default_profile.as_deref(), Some("local"));
        assert_eq!(
            loaded.profiles["local"].api_key.as_deref(),
            Some("sk-kosha-dev")
        );
    }

    fn tempfile_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kosha-cli-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }
}
