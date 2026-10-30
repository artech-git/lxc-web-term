//! The long-running daemon: the session HTTP server, the idle-timeout sweep,
//! the Caddy supervisor, and the local control socket, wired together under a
//! single graceful-shutdown signal.

pub mod caddy;
pub mod control;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::{routing::get, Router};
use dashmap::DashMap;
use tokio::sync::{broadcast, watch};
use tokio::time::Instant;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::utils::{HandlerState, InstanceState};
use crate::{logging, paths, routes, utils};

use self::caddy::CaddyHandle;

/// Shared state handed to the control-socket handlers. Everything here is an
/// `Arc`/handle, so it is cheap to clone per connection.
pub struct DaemonState {
    pub config: Arc<Config>,
    /// session_id -> (instance_id, shutdown sender, last-activity instant)
    pub job_handle: HandlerState,
    /// instance_id -> session_id
    pub instance_handle: InstanceState,
    pub metrics: Arc<Metrics>,
    pub caddy: CaddyHandle,
    pub log_tx: broadcast::Sender<String>,
    /// Bounded ring of recent formatted log lines, for non-following `lwt logs`.
    pub recent_logs: Arc<Mutex<VecDeque<String>>>,
    pub started_at: Instant,
    pub control_socket: String,
    /// Set to `true` to request a graceful shutdown.
    pub shutdown: watch::Sender<bool>,
}

/// Number of recent log lines retained in memory for `lwt logs`.
const RECENT_LOG_CAPACITY: usize = 500;

/// Resolve once and complete when a graceful shutdown has been requested.
async fn wait_for_shutdown(mut rx: watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    while rx.changed().await.is_ok() {
        if *rx.borrow() {
            return;
        }
    }
}

/// Run the daemon to completion. Returns when a shutdown has been requested and
/// the HTTP server has drained.
pub async fn run(config: Arc<Config>, with_caddy: bool, control_socket: String) {
    let log_tx = logging::channel();
    logging::init(&log_tx);

    tracing::info!(target: "lwt::daemon", address = %config.bind_address(), control = %control_socket, "daemon starting");

    // clear sockets left behind by a previous run, and make sure the session
    // socket directory exists before any session is spawned
    utils::sweep_stale_sockets(&config.unix_socket_folder);

    // best-effort check that ttyd is present; a mismatch is logged, not fatal
    match utils::get_program_version("ttyd").await {
        Ok(version) if version == b"ttyd version 1.7.4-68521f5" => {
            tracing::info!(target: "lwt::daemon", "ttyd present")
        }
        Ok(_) => tracing::warn!(target: "lwt::daemon", "ttyd version mismatch"),
        Err(err) => tracing::warn!(target: "lwt::daemon", error = %err, "ttyd program failed to run"),
    }

    // shared session state
    let job_handle: HandlerState = Arc::new(DashMap::new());
    let instance_handle: InstanceState = Arc::new(DashMap::new());
    let metrics: Arc<Metrics> = Arc::new(Metrics::default());

    // idle-timeout sweep for ttyd sessions
    tokio::spawn(utils::handle_ttyd_process_timeout(
        config.ttyd_session_timeout,
        job_handle.clone(),
        instance_handle.clone(),
        metrics.clone(),
        config.unix_socket_folder.clone(),
    ));

    // caddy supervisor
    let caddy = caddy::spawn(config.clone());
    if with_caddy || config.caddy_autostart {
        match caddy.start().await {
            Ok(()) => tracing::info!(target: "lwt::daemon", "caddy autostart requested"),
            Err(err) => tracing::error!(target: "lwt::daemon", %err, "caddy autostart failed"),
        }
    }

    let (shutdown_tx, _shutdown_rx) = watch::channel(false);

    // feed a bounded ring of recent log lines so `lwt logs` (without --follow)
    // can show context immediately, in both foreground and backgrounded modes
    let recent_logs = Arc::new(Mutex::new(VecDeque::with_capacity(RECENT_LOG_CAPACITY)));
    {
        let recent_logs = recent_logs.clone();
        let mut rx = log_tx.subscribe();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(line) => {
                        let mut ring = recent_logs.lock().expect("recent logs lock");
                        if ring.len() >= RECENT_LOG_CAPACITY {
                            ring.pop_front();
                        }
                        ring.push_back(line);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    let state = Arc::new(DaemonState {
        config: config.clone(),
        job_handle: job_handle.clone(),
        instance_handle: instance_handle.clone(),
        metrics: metrics.clone(),
        caddy: caddy.clone(),
        log_tx: log_tx.clone(),
        recent_logs,
        started_at: Instant::now(),
        control_socket: control_socket.clone(),
        shutdown: shutdown_tx.clone(),
    });

    // translate OS signals into a shutdown request
    {
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            utils::shutdown_signal().await;
            let _ = shutdown_tx.send(true);
        });
    }

    // control socket accept loop
    match control::serve(state.clone(), wait_for_shutdown(shutdown_tx.subscribe())).await {
        Ok(handle) => {
            tokio::spawn(handle);
        }
        Err(err) => {
            tracing::error!(target: "lwt::daemon", %err, "failed to bind control socket");
            let _ = shutdown_tx.send(true);
        }
    }

    // session HTTP server
    let app = build_router(
        job_handle.clone(),
        instance_handle.clone(),
        config.clone(),
        metrics.clone(),
    );

    match tokio::net::TcpListener::bind(config.bind_address()).await {
        Ok(listener) => {
            tracing::info!(target: "lwt::daemon", "port binding succeeded");
            let server = axum::serve(listener, app)
                .with_graceful_shutdown(wait_for_shutdown(shutdown_tx.subscribe()));
            if let Err(err) = server.await {
                tracing::error!(target: "lwt::daemon", error = %err, "http server error");
            }
        }
        Err(err) => {
            tracing::error!(target: "lwt::daemon", error = %err, "port binding failed");
            let _ = shutdown_tx.send(true);
        }
    }

    // teardown: stop caddy, remove the control socket
    tracing::info!(target: "lwt::daemon", "shutting down");
    if let Err(err) = caddy.stop().await {
        tracing::warn!(target: "lwt::daemon", %err, "failed to stop caddy during shutdown");
    }
    let _ = std::fs::remove_file(&control_socket);
    let _ = std::fs::remove_file(paths::pid_file());
}

/// Build the session HTTP router, mirroring the original route/state wiring.
fn build_router(
    job_handle: HandlerState,
    instance_handle: InstanceState,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
) -> Router {
    let session_state = (
        job_handle.clone(),
        instance_handle.clone(),
        config.clone(),
        metrics.clone(),
    );

    let open = Router::new()
        .route(
            "/start_web_session/{instance_uid}",
            get(routes::open_terminal),
        )
        .with_state(session_state.clone());

    let close = Router::new()
        .route("/terminate_web_session/{uid}", get(routes::close_terminal))
        .with_state(session_state);

    let extend = Router::new()
        .route(
            "/extend_web_session/{session_id}",
            get(routes::extend_terminal),
        )
        .with_state(job_handle.clone());

    let health = Router::new().route("/health", get(routes::health));
    let metrics_route = Router::new()
        .route("/metrics", get(routes::metrics))
        .with_state((job_handle, metrics));

    Router::new()
        .merge(open)
        .merge(close)
        .merge(extend)
        .merge(health)
        .merge(metrics_route)
}
