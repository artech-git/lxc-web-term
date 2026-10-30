//! Local control socket: a Unix domain socket the `lwt` CLI connects to.
//!
//! One request per connection, newline-delimited JSON in and out. Most commands
//! produce a single response; a log subscription streams until the client
//! disconnects.

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::obj::BindingStatus;
use crate::paths;
use crate::protocol::{Request, Response};
use crate::routes::{self, TerminateError};

use super::DaemonState;

/// Bind the control socket and return the accept-loop future (to be spawned).
/// The loop runs until `shutdown` resolves.
pub async fn serve<F>(
    state: Arc<DaemonState>,
    shutdown: F,
) -> std::io::Result<impl std::future::Future<Output = ()>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let path = state.control_socket.clone();
    if let Some(parent) = paths::parent_of(&path) {
        let _ = std::fs::create_dir_all(&parent);
    }
    // a stale socket file from a previous run would make bind fail
    let _ = std::fs::remove_file(&path);

    let listener = UnixListener::bind(&path)?;
    // restrict to owner + group; the control plane is local-only
    let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660));
    tracing::info!(target: "lwt::control", %path, "control socket listening");

    Ok(async move {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok((stream, _addr)) => {
                        let state = state.clone();
                        tokio::spawn(async move {
                            if let Err(err) = handle_conn(stream, state).await {
                                tracing::debug!(target: "lwt::control", %err, "control connection ended");
                            }
                        });
                    }
                    Err(err) => tracing::warn!(target: "lwt::control", %err, "control accept failed"),
                },
                _ = &mut shutdown => {
                    tracing::info!(target: "lwt::control", "control socket stopping");
                    break;
                }
            }
        }
        let _ = std::fs::remove_file(&path);
    })
}

async fn handle_conn(stream: UnixStream, state: Arc<DaemonState>) -> std::io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    if reader.read_line(&mut line).await? == 0 {
        return Ok(());
    }

    let request: Request = match serde_json::from_str(line.trim()) {
        Ok(request) => request,
        Err(err) => {
            write_response(
                &mut write_half,
                &Response::error(format!("invalid request: {err}")),
            )
            .await?;
            return Ok(());
        }
    };

    dispatch(request, state, &mut write_half).await
}

async fn dispatch<W>(request: Request, state: Arc<DaemonState>, out: &mut W) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    match request {
        Request::Ping => {
            write_response(out, &Response::ok(serde_json::json!({ "pong": true }))).await
        }
        Request::Status => {
            let caddy = state.caddy.status();
            let active = state.job_handle.len() as u64;
            let data = serde_json::json!({
                "pid": std::process::id(),
                "uptime_secs": state.started_at.elapsed().as_secs(),
                "server_addr": state.config.bind_address(),
                "control_socket": state.control_socket,
                "active_sessions": active,
                "metrics": state.metrics.snapshot(active),
                "caddy": caddy,
            });
            write_response(out, &Response::ok(data)).await
        }
        Request::Shutdown => {
            // acknowledge before tearing down so the client sees the reply
            write_response(
                out,
                &Response::ok(serde_json::json!({ "message": "daemon shutting down" })),
            )
            .await?;
            let _ = state.shutdown.send(true);
            Ok(())
        }
        Request::SessionsList => {
            let now = tokio::time::Instant::now();
            let sessions: Vec<_> = state
                .job_handle
                .iter()
                .map(|entry| {
                    let (instance_id, _tx, last_activity) = entry.value();
                    serde_json::json!({
                        "session_id": entry.key(),
                        "instance_id": instance_id,
                        "idle_secs": now.saturating_duration_since(*last_activity).as_secs(),
                    })
                })
                .collect();
            write_response(out, &Response::ok(serde_json::json!({ "sessions": sessions }))).await
        }
        Request::SessionClose { id } => {
            let result = routes::terminate_session(
                id,
                state.job_handle.clone(),
                state.instance_handle.clone(),
                state.config.clone(),
                state.metrics.clone(),
            )
            .await;
            let response = match result {
                Ok(message) => Response::ok(serde_json::json!({ "message": message })),
                Err(TerminateError::NotFound) => Response::error("unknown session"),
                Err(TerminateError::Failed(message)) => Response::error(message),
            };
            write_response(out, &response).await
        }
        Request::SessionOpen { instance } => {
            let result = routes::start_session(
                instance,
                state.job_handle.clone(),
                state.instance_handle.clone(),
                state.config.clone(),
                state.metrics.clone(),
            )
            .await;
            let response = match result {
                Ok(binding) => match serde_json::to_value(&binding) {
                    Ok(value) => Response::ok(value),
                    Err(err) => Response::error(format!("failed to serialize session: {err}")),
                },
                Err(status) => Response::error(binding_status_message(&status)),
            };
            write_response(out, &response).await
        }
        Request::Metrics => {
            let active = state.job_handle.len() as u64;
            let snapshot = state.metrics.snapshot(active);
            match serde_json::to_value(snapshot) {
                Ok(value) => write_response(out, &Response::ok(value)).await,
                Err(err) => {
                    write_response(out, &Response::error(format!("metrics error: {err}"))).await
                }
            }
        }
        Request::CaddyStart => reply_unit(out, state.caddy.start().await, "caddy started").await,
        Request::CaddyStop => reply_unit(out, state.caddy.stop().await, "caddy stopped").await,
        Request::CaddyRestart => {
            reply_unit(out, state.caddy.restart().await, "caddy restarted").await
        }
        Request::CaddyReload => reply_unit(out, state.caddy.reload().await, "caddy reloaded").await,
        Request::CaddyStatus => match serde_json::to_value(state.caddy.status()) {
            Ok(value) => write_response(out, &Response::ok(value)).await,
            Err(err) => write_response(out, &Response::error(format!("caddy status error: {err}"))).await,
        },
        Request::ConfigShow => {
            let config = &state.config;
            let data = serde_json::json!({
                "server_interface_addr": config.server_interface_addr,
                "term_server_port": config.term_server_port,
                "ttyd_session_timeout": config.ttyd_session_timeout,
                "unix_socket_folder": config.unix_socket_folder,
                "machine_id": config.machine_id,
                "serving_name": config.serving_name,
                "jwt_signing_secret": "<redacted>",
                "jwt_timeout": config.jwt_timeout,
                "caddy_bin": config.caddy_bin,
                "caddy_config": config.caddy_config,
                "caddy_autostart": config.caddy_autostart,
            });
            write_response(out, &Response::ok(data)).await
        }
        Request::LogsSubscribe {
            follow,
            level,
            component,
        } => stream_logs(state, out, follow, level, component).await,
    }
}

/// Write a success/error response for a `Result<(), String>` style command.
async fn reply_unit<W>(out: &mut W, result: Result<(), String>, ok_message: &str) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let response = match result {
        Ok(()) => Response::ok(serde_json::json!({ "message": ok_message })),
        Err(message) => Response::error(message),
    };
    write_response(out, &response).await
}

/// Stream recent (and, when following, live) log lines to the client.
async fn stream_logs<W>(
    state: Arc<DaemonState>,
    out: &mut W,
    follow: bool,
    level: Option<String>,
    component: Option<String>,
) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    // subscribe before draining the backlog so no line is missed in the gap
    let mut rx = state.log_tx.subscribe();

    let backlog: Vec<String> = {
        let recent = state.recent_logs.lock().expect("recent logs lock");
        recent.iter().cloned().collect()
    };
    for line in backlog {
        if line_matches(&line, level.as_deref(), component.as_deref()) {
            write_response(out, &Response::Log { line }).await?;
        }
    }

    if !follow {
        return write_response(out, &Response::EndOfStream).await;
    }

    loop {
        match rx.recv().await {
            Ok(line) => {
                if line_matches(&line, level.as_deref(), component.as_deref()) {
                    // a write error means the client disconnected; stop quietly
                    if write_response(out, &Response::Log { line }).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
        }
    }
}

async fn write_response<W>(out: &mut W, response: &Response) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let mut encoded = serde_json::to_vec(response).map_err(std::io::Error::other)?;
    encoded.push(b'\n');
    out.write_all(&encoded).await?;
    out.flush().await
}

/// Best-effort filter of a JSON log line by severity and component.
fn line_matches(line: &str, level: Option<&str>, component: Option<&str>) -> bool {
    if level.is_none() && component.is_none() {
        return true;
    }
    let parsed: serde_json::Value = match serde_json::from_str(line) {
        Ok(value) => value,
        Err(_) => return true, // don't hide lines we can't parse
    };

    if let Some(wanted) = level {
        let actual = parsed.get("level").and_then(|v| v.as_str()).unwrap_or("");
        if severity(actual) < severity(wanted) {
            return false;
        }
    }

    if let Some(component) = component {
        let target = parsed.get("target").and_then(|v| v.as_str()).unwrap_or("");
        if !component_matches(target, component) {
            return false;
        }
    }

    true
}

fn severity(level: &str) -> u8 {
    match level.to_ascii_uppercase().as_str() {
        "ERROR" => 4,
        "WARN" => 3,
        "INFO" => 2,
        "DEBUG" => 1,
        "TRACE" => 0,
        _ => 0,
    }
}

fn component_matches(target: &str, component: &str) -> bool {
    match component.to_ascii_lowercase().as_str() {
        "daemon" => target.starts_with("lwt::daemon") || target.starts_with("lwt::control"),
        "caddy" => target.starts_with("lwt::caddy"),
        "session" => {
            target.starts_with("lwt::routes")
                || target.starts_with("lwt::utils")
                || target.starts_with("lwt::session")
        }
        other => target.contains(other),
    }
}

fn binding_status_message(status: &BindingStatus) -> String {
    match status {
        BindingStatus::Failed(m)
        | BindingStatus::Error(m)
        | BindingStatus::ProcessReadError(m)
        | BindingStatus::SessionRunning(m)
        | BindingStatus::NotFound(m)
        | BindingStatus::FileCreationError(m) => m.clone(),
        BindingStatus::AtCapacity => "at capacity: too many active sessions".to_string(),
        BindingStatus::Binding => "binding".to_string(),
        BindingStatus::Live => "live".to_string(),
    }
}
