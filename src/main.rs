#[macro_use]
extern crate dotenv_codegen;

use std::sync::Arc;

use axum::{routing::get, Router};
use dashmap::DashMap;
use utils::shutdown_signal;

use crate::metrics::Metrics;
use crate::utils::{HandlerState, InstanceState};

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

    let port = format!(
        "{}:{}",
        dotenv!("SERVER_INTERFACE_ADDR"),
        dotenv!("TERM_SERVER_PORT")
    );
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
    // key => session_id, value => (instance_id, Sender, starting_time)
    let global_handle_state: HandlerState = Arc::new(DashMap::new());

    // process-wide metrics shared with the handlers and the timeout sweep
    let metrics: Arc<Metrics> = Arc::new(Metrics::default());

    //clone rest of the handlers for passing to handlers
    let duration = dotenv!("TTYD_SESSION_TIMEOUT").to_string();
    let global_instance_state_clone = global_instance_state.clone();
    let global_handle_state_clone = global_handle_state.clone();
    let metrics_clone = metrics.clone();

    // spawn a task to handle ttyd process timeouts
    tokio::spawn(utils::handle_ttyd_process_timeout(
            duration,
            global_handle_state_clone,
            global_instance_state_clone,
            metrics_clone,
        )
    );

    let route_1 = Router::new()
        .route(
            "/start_web_session/{instance_uid}",
            get(routes::open_terminal),
        )
        .with_state((
            global_handle_state.clone(),
            global_instance_state.clone(),
            metrics.clone(),
        ));
    let route_2 = Router::new()
        .route("/terminate_web_session/{uid}", get(routes::close_terminal))
        .with_state((
            global_handle_state.clone(),
            global_instance_state.clone(),
            metrics.clone(),
        ));

    // operational endpoints: liveness and metrics
    let route_health = Router::new().route("/health", get(routes::health));
    let route_metrics = Router::new()
        .route("/metrics", get(routes::metrics))
        .with_state((global_handle_state.clone(), metrics.clone()));

    let app = Router::new()
        .merge(route_1)
        .merge(route_2)
        .merge(route_health)
        .merge(route_metrics);

    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());

    // poll the server future, until the error occurs / server is stopped
    if let Err(err) = server.await {
        tracing::error!(error = %err, "error starting the application");
    }
}
