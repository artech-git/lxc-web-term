
use std::sync::Arc;

use crate::{
    config::Config,
    metrics::Metrics,
    obj::{BindingStatus, ContainerBindingResponse},
    utils::{child_process_handler, HandlerState, InstanceState},
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use dashmap::mapref::entry::Entry;

// Upper bound on concurrently attached sessions; past this, start returns 429.
const MAX_CONCURRENT_SESSIONS: usize = 100;

// Build the success response (JWT token + access URL) for a given session name.
// Shared by the fresh-start and idempotent paths so both return an identical url.
fn build_session_response(
    uds_session_name: String,
    config: &Config,
) -> Result<ContainerBindingResponse, BindingStatus> {
    let token = crate::utils::jwt_token_generator(config.jwt_timeout, &config.jwt_signing_secret)
        .map_err(|_| BindingStatus::Error("JWT token generation failed".to_string()))?;

    // generate the domain over which ttyd web terminal can be accessed
    let url = crate::utils::acquire_response_details(
        config.serving_name.clone(),
        config.machine_id.clone(),
        uds_session_name.clone(),
        token.clone(),
    );

    Ok(ContainerBindingResponse {
        terminal_session_name: uds_session_name,
        access_token: token,
        url,
        status: BindingStatus::Live,
    })
}

/*
    Route for accepting the terminal based upon instance_id which we get from the user endpoint
    State carries the global handlerState, instanceState, runtime Config and Metrics references
*/

//parametrize the function to accept diff. confg. as per future requirement
pub async fn open_terminal(
    Path(instance_id): Path<String>,
    State((job_handle, session_handle, config, metrics)): State<(
        HandlerState,
        InstanceState,
        Arc<Config>,
        Arc<Metrics>,
    )>,
) -> impl IntoResponse {
    start_session(instance_id, job_handle, session_handle, config, metrics).await
}

/// Provision (or idempotently return) a terminal session for an instance.
///
/// Extracted from the HTTP handler so the control socket (`lwt sessions open`)
/// can drive exactly the same path: validation, capacity guard, running-check,
/// atomic slot reservation, ttyd spawn and bookkeeping.
pub async fn start_session(
    instance_id: String,
    job_handle: HandlerState,
    session_handle: InstanceState,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
) -> Result<ContainerBindingResponse, BindingStatus> {
    // perform checks against the endpoint instance_id input
    if let Err(validate_err) = crate::utils::validate_instance(&instance_id).await {
        tracing::warn!(%instance_id, error = %validate_err, "instance validation failed");
        return Err(BindingStatus::Failed(validate_err));
    }

    // Idempotent start: if a session already exists for this instance, return the
    // existing session's URL (200) instead of erroring. This happens before the
    // capacity check so an already-attached instance is served even at capacity.
    if let Some(existing) = session_handle.get(&instance_id) {
        let uds_session_name = existing.clone();
        tracing::info!(%instance_id, "session already found, returning existing url");
        return build_session_response(uds_session_name, &config);
    }

    // Capacity guard: reject new sessions past the configured ceiling with 429.
    if session_handle.len() >= MAX_CONCURRENT_SESSIONS {
        tracing::warn!(%instance_id, "at capacity, rejecting start");
        return Err(BindingStatus::AtCapacity);
    }

    // ensure the container actually exists and is running before spawning ttyd
    if !crate::utils::container_is_running(&instance_id).await {
        tracing::warn!(%instance_id, "container not found or not running");
        return Err(BindingStatus::Failed(format!(
            "container `{instance_id}` not found or not running"
        )));
    }

    // Atomically check-and-reserve the instance slot via the DashMap entry API,
    // closing the check-then-insert race (two concurrent starts for the same
    // instance can no longer both spawn). An Occupied entry means another request
    // reserved the slot in the window above, so return that session idempotently.
    let uds_session_name = match session_handle.entry(instance_id.clone()) {
        Entry::Occupied(existing) => {
            tracing::info!(%instance_id, "session already found, returning existing url");
            return build_session_response(existing.get().clone(), &config);
        }
        Entry::Vacant(slot) => {
            let uds = uuid::Uuid::new_v4().as_hyphenated().to_string();
            slot.insert(uds.clone());
            uds
        }
    };

    let ttyd_session = crate::utils::start_ttyd_process(
        &instance_id,
        &uds_session_name,
        &config.unix_socket_folder,
    )
    .await;

    match ttyd_session {
        Ok(ttyd_process) => {
            tracing::info!(%uds_session_name, %instance_id, "listening on domain socket");

            // the watcher task records the session in both maps and owns cleanup
            // (maps + socket) when the session ends, for either exit path
            child_process_handler(
                ttyd_process,
                uds_session_name.clone(),
                instance_id.clone(),
                job_handle.clone(),
                session_handle.clone(),
                config.unix_socket_folder.clone(),
            )
            .await;

            metrics.record_session_started();
            build_session_response(uds_session_name, &config)
        }
        Err(binding_err) => {
            // roll back the reservation so a failed spawn doesn't wedge the instance
            session_handle.remove(&instance_id);
            metrics.record_spawn_failure();
            tracing::error!(%instance_id, error = ?binding_err, "ttyd process failed to start");
            Err(binding_err)
        }
    }
}

/*
    Route for extending (resetting the idle deadline of) a live terminal session.

    The idle-timeout sweep measures inactivity from the last-activity Instant
    stored for each session. A client keeps an actively used shell alive by
    calling this endpoint as a heartbeat; each call resets that Instant to now,
    so only genuinely idle sessions are reaped.
*/
pub async fn extend_terminal(
    Path(session_id): Path<String>,
    State(job_handle): State<HandlerState>,
) -> impl IntoResponse {
    // session id is the uds session name, i.e. the key of the job handle map
    match job_handle.get_mut(&session_id) {
        Some(mut entry) => {
            // refresh the last-activity timestamp, pushing the idle deadline out
            entry.2 = tokio::time::Instant::now();
            tracing::info!(session = %session_id, "extended session");
            (StatusCode::OK, format!("+ session extended: {session_id}"))
        }
        None => {
            tracing::warn!(session = %session_id, "extend failed, unknown session");
            (StatusCode::NOT_FOUND, format!("- unknown session: {session_id}"))
        }
    }
}

/*
    Route for closing the terminal session based upon session_id which we get from the user endpoint
    State carries the global handlerState, instanceState, Config and Metrics references

    acquire the respective shutdown channel signal sender based upon the session_id if present
*/

// close terminal session handler
pub async fn close_terminal(
    Path(session_id): Path<String>,
    State((job_handle, session_handle, config, metrics)): State<(
        HandlerState,
        InstanceState,
        Arc<Config>,
        Arc<Metrics>,
    )>,
) -> impl IntoResponse {
    match terminate_session(session_id, job_handle, session_handle, config, metrics).await {
        Ok(message) => Response::new(message),
        Err(TerminateError::NotFound) => {
            let mut resp = Response::new("- task not found for session key".to_string());
            *resp.status_mut() = StatusCode::NOT_FOUND;
            resp
        }
        Err(TerminateError::Failed(message)) => Response::new(message),
    }
}

/// Why terminating a session did not complete.
pub enum TerminateError {
    /// No session was registered under the given id.
    NotFound,
    /// The session existed but could not be cancelled cleanly.
    Failed(String),
}

/// Terminate a live session by its session id.
///
/// Shared by the HTTP close handler and the control socket (`lwt sessions
/// close`): it removes the job entry, signals the watcher task to kill ttyd,
/// drops the instance mapping, unlinks the socket and records the lifetime.
pub async fn terminate_session(
    session_id: String,
    job_handle: HandlerState,
    session_handle: InstanceState,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
) -> Result<String, TerminateError> {
    if !job_handle.contains_key(&session_id) {
        tracing::warn!(session = %session_id, "task not found for session key");
        return Err(TerminateError::NotFound);
    }

    // attempt to remove the job from the hashmap
    let Some((sess, handle)) = job_handle.remove(&session_id) else {
        tracing::warn!(session = %session_id, "failed to close session id");
        return Err(TerminateError::Failed(
            "- job not found for session_key or already removed".to_string(),
        ));
    };

    // session lifetime, measured from the stored start Instant
    let started_at = handle.2;

    // send the shutdown signal to the task handler
    if handle.1.send(()).is_ok() {
        tracing::info!(session = %sess, "dropped session (closed)");
        let instance_id = handle.0;
        // once the task is dropped, remove the session from the instance map
        session_handle.remove(&instance_id);
        // remove the socket file on session end
        crate::utils::remove_socket(&config.unix_socket_folder, &session_id);
        // record the completed session's lifetime
        let duration = tokio::time::Instant::now().saturating_duration_since(started_at);
        metrics.record_session_completed(duration.as_secs());
        Ok("+ successfully closed the terminal session".to_string())
    } else {
        // if sending the shutdown signal fails, preserve the session details and
        // let the timeout sweep handle the cleanup
        tracing::warn!(session = %sess, "failed to cancel the session");
        Err(TerminateError::Failed(
            "- failed to cancel the session".to_string(),
        ))
    }
}

/*
    Liveness endpoint. Returns 200 with a small JSON body; used by load
    balancers / orchestrators to check the service is up.
*/
pub async fn health() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("Content-Type", "application/json")],
        r#"{"status":"ok"}"#,
    )
}

/*
    Metrics endpoint. Exposes active session count (read live from the session
    map so it never drifts), total sessions started, ttyd spawn failures, and
    completed-session count / average duration.
*/
pub async fn metrics(
    State((job_handle, metrics)): State<(HandlerState, Arc<Metrics>)>,
) -> impl IntoResponse {
    let snapshot = metrics.snapshot(job_handle.len() as u64);
    match serde_json::to_string(&snapshot) {
        Ok(body) => (
            StatusCode::OK,
            [("Content-Type", "application/json")],
            body,
        )
            .into_response(),
        Err(err) => {
            tracing::error!(error = %err, "failed to serialize metrics");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "metrics serialization failed",
            )
                .into_response()
        }
    }
}
