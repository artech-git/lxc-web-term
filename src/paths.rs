//! Resolution of the daemon's runtime paths (control socket, pid file, log
//! file). Kept free of any [`crate::config::Config`] dependency so the CLI
//! client can locate a running daemon without loading the full environment.
//!
//! Resolution order, most specific first:
//!   1. an explicit value (CLI flag or dedicated env var)
//!   2. the systemd-provided `$RUNTIME_DIRECTORY` (first colon-separated entry)
//!   3. `$LWT_RUNTIME_DIR`
//!   4. a `/tmp/lwt` fallback for development

use std::env;

/// Base runtime directory for the daemon's files.
pub fn runtime_dir() -> String {
    if let Ok(value) = env::var("RUNTIME_DIRECTORY") {
        if let Some(first) = value.split(':').next() {
            if !first.trim().is_empty() {
                return first.trim_end_matches('/').to_string();
            }
        }
    }
    if let Ok(value) = env::var("LWT_RUNTIME_DIR") {
        if !value.trim().is_empty() {
            return value.trim_end_matches('/').to_string();
        }
    }
    "/tmp/lwt".to_string()
}

/// Path to the control socket. An explicit `--socket` flag wins, then
/// `$LWT_CONTROL_SOCKET`, then `<runtime_dir>/control.sock`.
pub fn control_socket(cli_override: Option<&str>) -> String {
    if let Some(path) = cli_override {
        if !path.trim().is_empty() {
            return path.to_string();
        }
    }
    if let Ok(value) = env::var("LWT_CONTROL_SOCKET") {
        if !value.trim().is_empty() {
            return value;
        }
    }
    format!("{}/control.sock", runtime_dir())
}

/// Path to the pid file.
pub fn pid_file() -> String {
    env::var("LWT_PID_FILE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("{}/lwt.pid", runtime_dir()))
}

/// Path to the log file the daemon redirects stdout/stderr to when backgrounded.
pub fn log_file() -> String {
    env::var("LWT_LOG_FILE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("{}/lwt.log", runtime_dir()))
}

/// Parent directory of a path (used to pre-create runtime dirs).
pub fn parent_of(path: &str) -> Option<String> {
    std::path::Path::new(path)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
}
