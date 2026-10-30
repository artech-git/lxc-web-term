use std::env::{self, VarError};
use std::fmt;

/// Runtime configuration, read from the process environment once at startup.
///
/// Every value that used to be baked into the binary at compile time via the
/// `dotenv!` macro now lives here and is read at runtime, so a single build can
/// be shipped to multiple hosts. `Config::from_env` validates the whole set and
/// fails fast with a descriptive error, instead of silently falling back to a
/// default the way the session timeout parsing used to.
#[derive(Debug, Clone)]
pub struct Config {
    /// Interface address the server binds to (e.g. `0.0.0.0`).
    pub server_interface_addr: String,
    /// TCP port the server binds to.
    pub term_server_port: u16,
    /// Session timeout, in seconds.
    pub ttyd_session_timeout: u64,
    /// Directory the ttyd unix domain sockets are created in. Always ends in `/`.
    pub unix_socket_folder: String,
    /// Identifier of the host this instance is reachable under.
    pub machine_id: String,
    /// Public serving name (domain) used to build session URLs.
    pub serving_name: String,
    /// Secret used to sign JWT access tokens.
    pub jwt_signing_secret: String,
    /// JWT token lifetime, in seconds.
    pub jwt_timeout: u64,
    /// Caddy binary to supervise (name on `$PATH` or an absolute path).
    pub caddy_bin: String,
    /// Path to the Caddyfile the supervised Caddy loads.
    pub caddy_config: String,
    /// Whether the daemon starts Caddy automatically on boot.
    pub caddy_autostart: bool,
}

/// Why loading [`Config`] from the environment failed.
#[derive(Debug)]
pub enum ConfigError {
    /// A required variable was not set.
    Missing(&'static str),
    /// A variable was set but its value could not be used.
    Invalid {
        key: &'static str,
        value: String,
        reason: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Missing(key) => {
                write!(f, "missing required environment variable `{key}`")
            }
            ConfigError::Invalid { key, value, reason } => {
                write!(f, "invalid value `{value}` for `{key}`: {reason}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// A source of configuration values keyed by variable name.
type EnvSource<'a> = dyn Fn(&'static str) -> Result<String, VarError> + 'a;

impl Config {
    /// Read and validate the configuration from the process environment.
    ///
    /// Call `dotenv::dotenv()` first if a `.env` file should be loaded.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::load(&|key| env::var(key))
    }

    /// Validate a configuration against an arbitrary value source. Kept separate
    /// from [`Config::from_env`] so it can be unit tested without mutating the
    /// process-global environment.
    fn load(get: &EnvSource) -> Result<Self, ConfigError> {
        let server_interface_addr = required(get, "SERVER_INTERFACE_ADDR")?;
        let term_server_port = required_parse::<u16>(get, "TERM_SERVER_PORT")?;
        let ttyd_session_timeout = required_parse::<u64>(get, "TTYD_SESSION_TIMEOUT")?;

        let mut unix_socket_folder = required(get, "UNIX_SOCKET_FOLDER")?;
        // The socket path is built as `{folder}{name}.sock`, so a trailing
        // separator is required for the resulting path to be well-formed.
        if !unix_socket_folder.ends_with('/') {
            unix_socket_folder.push('/');
        }

        let machine_id = required(get, "MACHINE_ID")?;
        let serving_name = required(get, "SERVING_NAME")?;
        let jwt_signing_secret = required(get, "MZ_TERM_JWT_SINGING_SECRET")?;
        let jwt_timeout = required_parse::<u64>(get, "MZ_TERM_JWT_TIMEOUT")?;

        // Caddy supervision settings are optional and fall back to sensible
        // defaults, so existing deployments keep working without new variables.
        let caddy_bin = optional(get, "CADDY_BIN").unwrap_or_else(|| "caddy".to_string());
        let caddy_config =
            optional(get, "CADDY_CONFIG").unwrap_or_else(|| "/etc/caddy/Caddyfile".to_string());
        let caddy_autostart = match optional(get, "CADDY_AUTOSTART") {
            Some(value) => parse_bool(&value).ok_or_else(|| ConfigError::Invalid {
                key: "CADDY_AUTOSTART",
                value: value.clone(),
                reason: "must be one of true/false/1/0/yes/no".to_string(),
            })?,
            None => false,
        };

        if ttyd_session_timeout == 0 {
            return Err(ConfigError::Invalid {
                key: "TTYD_SESSION_TIMEOUT",
                value: ttyd_session_timeout.to_string(),
                reason: "must be greater than zero".to_string(),
            });
        }
        if jwt_timeout == 0 {
            return Err(ConfigError::Invalid {
                key: "MZ_TERM_JWT_TIMEOUT",
                value: jwt_timeout.to_string(),
                reason: "must be greater than zero".to_string(),
            });
        }

        Ok(Config {
            server_interface_addr,
            term_server_port,
            ttyd_session_timeout,
            unix_socket_folder,
            machine_id,
            serving_name,
            jwt_signing_secret,
            jwt_timeout,
            caddy_bin,
            caddy_config,
            caddy_autostart,
        })
    }

    /// `host:port` string the server binds to.
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.server_interface_addr, self.term_server_port)
    }
}

/// Read a required, non-empty value from the source.
fn required(get: &EnvSource, key: &'static str) -> Result<String, ConfigError> {
    match get(key) {
        Ok(val) if !val.trim().is_empty() => Ok(val),
        Ok(_) => Err(ConfigError::Invalid {
            key,
            value: String::new(),
            reason: "must not be empty".to_string(),
        }),
        Err(VarError::NotPresent) => Err(ConfigError::Missing(key)),
        Err(VarError::NotUnicode(_)) => Err(ConfigError::Invalid {
            key,
            value: "<non-unicode>".to_string(),
            reason: "must be valid unicode".to_string(),
        }),
    }
}

/// Read an optional value from the source, treating an unset or empty variable
/// as absent. A non-unicode value is also treated as absent.
fn optional(get: &EnvSource, key: &'static str) -> Option<String> {
    match get(key) {
        Ok(val) if !val.trim().is_empty() => Some(val),
        _ => None,
    }
}

/// Parse a permissive boolean. Returns `None` for an unrecognised value.
fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Read a required value and parse it into `T`, failing fast on a malformed
/// value rather than falling back to a default.
fn required_parse<T>(get: &EnvSource, key: &'static str) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    let raw = required(get, key)?;
    raw.trim().parse::<T>().map_err(|e| ConfigError::Invalid {
        key,
        value: raw,
        reason: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Build an [`EnvSource`]-compatible closure backed by a fixed map, so tests
    /// never touch the real process environment.
    fn source(pairs: &[(&'static str, &'static str)]) -> impl Fn(&'static str) -> Result<String, VarError> {
        let map: HashMap<&'static str, String> =
            pairs.iter().map(|(k, v)| (*k, v.to_string())).collect();
        move |key| map.get(key).cloned().ok_or(VarError::NotPresent)
    }

    fn valid_pairs() -> Vec<(&'static str, &'static str)> {
        vec![
            ("SERVER_INTERFACE_ADDR", "0.0.0.0"),
            ("TERM_SERVER_PORT", "9091"),
            ("TTYD_SESSION_TIMEOUT", "1800"),
            ("UNIX_SOCKET_FOLDER", "/tmp/sessions/"),
            ("MACHINE_ID", "session"),
            ("SERVING_NAME", "example.test"),
            ("MZ_TERM_JWT_SINGING_SECRET", "secret"),
            ("MZ_TERM_JWT_TIMEOUT", "3600"),
        ]
    }

    #[test]
    fn parses_a_full_valid_environment() {
        let cfg = Config::load(&source(&valid_pairs())).expect("valid config");
        assert_eq!(cfg.term_server_port, 9091);
        assert_eq!(cfg.ttyd_session_timeout, 1800);
        assert_eq!(cfg.jwt_timeout, 3600);
        assert_eq!(cfg.bind_address(), "0.0.0.0:9091");
    }

    #[test]
    fn appends_trailing_slash_to_socket_folder() {
        let mut pairs = valid_pairs();
        pairs.retain(|(k, _)| *k != "UNIX_SOCKET_FOLDER");
        pairs.push(("UNIX_SOCKET_FOLDER", "/tmp/sessions"));
        let cfg = Config::load(&source(&pairs)).expect("valid config");
        assert_eq!(cfg.unix_socket_folder, "/tmp/sessions/");
    }

    #[test]
    fn fails_fast_on_missing_variable() {
        let mut pairs = valid_pairs();
        pairs.retain(|(k, _)| *k != "SERVING_NAME");
        assert!(matches!(
            Config::load(&source(&pairs)),
            Err(ConfigError::Missing("SERVING_NAME"))
        ));
    }

    #[test]
    fn rejects_a_non_numeric_port() {
        let mut pairs = valid_pairs();
        pairs.retain(|(k, _)| *k != "TERM_SERVER_PORT");
        pairs.push(("TERM_SERVER_PORT", "not-a-port"));
        assert!(matches!(
            Config::load(&source(&pairs)),
            Err(ConfigError::Invalid { key: "TERM_SERVER_PORT", .. })
        ));
    }

    #[test]
    fn rejects_a_zero_timeout_instead_of_defaulting() {
        let mut pairs = valid_pairs();
        pairs.retain(|(k, _)| *k != "TTYD_SESSION_TIMEOUT");
        pairs.push(("TTYD_SESSION_TIMEOUT", "0"));
        assert!(matches!(
            Config::load(&source(&pairs)),
            Err(ConfigError::Invalid { key: "TTYD_SESSION_TIMEOUT", .. })
        ));
    }
}
