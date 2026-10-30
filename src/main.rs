use std::sync::Arc;

use axum::{routing::get, Router};
use dashmap::DashMap;
use utils::shutdown_signal;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::utils::{HandlerState, InstanceState};

mod config;
mod metrics;
mod obj;
mod routes;
mod utils;

// runtime variable settings

#[tokio::main]
async fn main() {
    // structured (JSON) logging via tracing; level controlled by RUST_LOG,
    // defaulting to `info`.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    dotenv::dotenv().ok();

    // Load configuration at runtime and fail fast on anything missing or
    // malformed, rather than baking values into the binary at compile time.
    let config = match Config::from_env() {
        Ok(cfg) => Arc::new(cfg),
        Err(err) => {
            tracing::error!(error = %err, "invalid configuration");
            std::process::exit(1);
        }
    };

    // remove any sockets left behind by a previous run and make sure the
    // (systemd runtime) socket directory exists before any session is spawned
    utils::sweep_stale_sockets(&config.unix_socket_folder);

    let port = config.bind_address();
    tracing::info!(address = %port, "server starting");

    // Insert checks for presence for bore & ttyd !
    let ttyd_util = "ttyd";

    let ttyd_version = b"ttyd version 1.7.4-68521f5";

    let ttyd_check = utils::get_program_version(&ttyd_util).await;

    match ttyd_check {
        Ok(ttyd) if ttyd == ttyd_version => {
            tracing::info!("ttyd present");
        }
        Err(ttyd_err) => {
            tracing::warn!(error = %ttyd_err, "ttyd program failed to run");
            // return; //exit program gracefully
        }
        _ => {
            tracing::warn!("ttyd version mismatch, program failed abruptly");
            // return;
        }
    };

    let listener = match tokio::net::TcpListener::bind(port).await {
        Ok(val) => {
            tracing::info!("port binding succeeded");
            val
        }
        Err(err) => {
            tracing::error!(error = %err, "port binding failed");
            return;
        }
    };

    // maps instances id  with session id
    let global_instance_state: InstanceState = Arc::new(DashMap::new());

    // maps session_id with instance task handle
    // key => session_id, value => (instance_id, Sender, last_activity)
    let global_handle_state: HandlerState = Arc::new(DashMap::new());

    // process-wide metrics shared with the handlers and the timeout sweep
    let metrics: Arc<Metrics> = Arc::new(Metrics::default());

    //clone rest of the handlers for passing to handlers
    let timeout_secs = config.ttyd_session_timeout;
    let global_instance_state_clone = global_instance_state.clone();
    let global_handle_state_clone = global_handle_state.clone();
    let metrics_clone = metrics.clone();
    let socket_folder = config.unix_socket_folder.clone();

    // spawn a task to handle idle ttyd session timeouts
    tokio::spawn(utils::handle_ttyd_process_timeout(
        timeout_secs,
        global_handle_state_clone,
        global_instance_state_clone,
        metrics_clone,
        socket_folder,
    ));

    let route_1 = Router::new()
        .route(
            "/start_web_session/{instance_uid}",
            get(routes::open_terminal),
        )
        .with_state((
            global_handle_state.clone(),
            global_instance_state.clone(),
            config.clone(),
            metrics.clone(),
        ));
    let route_2 = Router::new()
        .route("/terminate_web_session/{uid}", get(routes::close_terminal))
        .with_state((
            global_handle_state.clone(),
            global_instance_state.clone(),
            config.clone(),
            metrics.clone(),
        ));

    // heartbeat endpoint to extend an active session's idle deadline
    let route_3 = Router::new()
        .route(
            "/extend_web_session/{session_id}",
            get(routes::extend_terminal),
        )
        .with_state(global_handle_state.clone());

    // operational endpoints: liveness and metrics
    let route_health = Router::new().route("/health", get(routes::health));
    let route_metrics = Router::new()
        .route("/metrics", get(routes::metrics))
        .with_state((global_handle_state.clone(), metrics.clone()));

    let app = Router::new()
        .merge(route_1)
        .merge(route_2)
        .merge(route_3)
        .merge(route_health)
        .merge(route_metrics);

    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());

    // poll the server future, until the error occurs / server is stopped
    if let Err(err) = server.await {
        tracing::error!(error = %err, "error starting the application");
    }
}
