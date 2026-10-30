//! Wire protocol for the local control socket.
//!
//! The daemon listens on a Unix domain socket and the `lwt` CLI connects to it,
//! sends exactly one [`Request`] as a single JSON line, and reads one or more
//! newline-delimited [`Response`] lines back. Everything is newline-delimited
//! JSON so the socket can also be driven by hand with tools like `socat`.

use serde::{Deserialize, Serialize};

/// A command sent by the CLI to the running daemon.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
    /// Liveness check; the daemon replies immediately.
    Ping,
    /// Daemon health: pid, uptime, server address, caddy state, session count.
    Status,
    /// Ask the daemon to shut down gracefully.
    Shutdown,
    /// List active terminal sessions.
    SessionsList,
    /// Terminate a single session by its session id.
    SessionClose { id: String },
    /// Provision a new session for an LXC instance.
    SessionOpen { instance: String },
    /// Point-in-time metrics snapshot.
    Metrics,
    /// Start the supervised Caddy reverse proxy.
    CaddyStart,
    /// Stop the supervised Caddy reverse proxy.
    CaddyStop,
    /// Restart the supervised Caddy reverse proxy.
    CaddyRestart,
    /// Hot-reload the Caddy configuration without dropping connections.
    CaddyReload,
    /// Report the Caddy supervisor state.
    CaddyStatus,
    /// Print the effective (secret-redacted) runtime configuration.
    ConfigShow,
    /// Subscribe to the daemon's log stream.
    ///
    /// When `follow` is false the daemon sends an [`Response::EndOfStream`] and
    /// closes; when true it keeps streaming [`Response::Log`] lines until the
    /// client disconnects.
    LogsSubscribe {
        follow: bool,
        level: Option<String>,
        component: Option<String>,
    },
}

/// A reply from the daemon. One request yields one terminal response
/// (`Ok`/`Error`/`EndOfStream`), optionally preceded by streamed `Log` lines.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    /// Success, carrying an arbitrary JSON payload.
    Ok { data: serde_json::Value },
    /// Failure, carrying a human-readable message.
    Error { message: String },
    /// A single formatted log line (JSON), streamed for `LogsSubscribe`.
    Log { line: String },
    /// Marks the end of a non-following log subscription.
    EndOfStream,
}

impl Response {
    pub fn ok(data: serde_json::Value) -> Self {
        Response::Ok { data }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Response::Error {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trips_through_json() {
        let req = Request::SessionClose {
            id: "abc-123".to_string(),
        };
        let line = serde_json::to_string(&req).unwrap();
        assert!(line.contains("\"cmd\":\"session_close\""));
        let back: Request = serde_json::from_str(&line).unwrap();
        matches!(back, Request::SessionClose { .. });
    }

    #[test]
    fn response_round_trips_through_json() {
        let resp = Response::ok(serde_json::json!({ "pong": true }));
        let line = serde_json::to_string(&resp).unwrap();
        let back: Response = serde_json::from_str(&line).unwrap();
        matches!(back, Response::Ok { .. });
    }
}
