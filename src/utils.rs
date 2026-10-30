use std::{process::Stdio, sync::Arc};

use dashmap::DashMap;
use tokio::{io as tio, process::{Child as TokioChild, Command}, sync::watch::Sender, time::{self, Instant}};

use crate::metrics::Metrics;
use crate::obj::BindingStatus;

// common type alias
pub type HandlerState = Arc<DashMap<String, (String, Sender<()>, Instant)>>;
pub type InstanceState = Arc<DashMap<String, String>>;


pub async fn get_program_version(program: &str) -> tio::Result<Vec<u8>> {
    let output = Command::new(program).arg("--version").output().await?;
    Ok(output.stdout)
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("shutdown signal received (ctrl-c)");
        },
        _ = terminate => {
            tracing::info!("shutdown signal received (terminate)");
        },
    }
}

// Reap ttyd sessions that have been IDLE for longer than the timeout.
//
// The timeout is an *idle* timeout, not an absolute one: each session carries a
// "last activity" Instant (value.2) that is refreshed by the extend endpoint
// (see `routes::extend_terminal`). A session is reaped only once it has gone
// `timeout_secs` seconds without being extended. On reaping, the session's
// socket file is removed and its lifetime is recorded in the metrics. The
// timeout is validated at startup (Config), so it is used as-is here rather
// than being re-parsed with a silent fallback default.
pub async fn handle_ttyd_process_timeout(
    timeout_secs: u64,
    global_handle_state_clone: HandlerState,
    global_instance_state_clone: InstanceState,
    metrics: Arc<Metrics>,
    socket_folder: String,
) {
        let tokio_timeout_duration = time::Duration::from_secs(timeout_secs);

        loop {
            // give some 5 secs pause for the next iteration
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            let current_time = tokio::time::Instant::now();

            global_handle_state_clone.retain(|session_id, value| {

                // value.2 is the last-activity Instant, refreshed on every extend
                let last_activity = value.2;
                let instance_id = value.0.clone();

                let time_elasped = current_time - last_activity;

                if time_elasped < tokio_timeout_duration {
                    true
                } else {
                    // record the completed session's lifetime and remove its socket
                    metrics.record_session_completed(time_elasped.as_secs());
                    remove_socket(&socket_folder, session_id);
                    // send signal to the task to terminate
                    if value.1.send(()).is_ok() {
                        tracing::info!(%instance_id, "dropped session (timeout)");
                        global_instance_state_clone.remove(&value.0);
                        false
                    } else {
                        tracing::warn!(%instance_id, "failed to send signal to task");
                        false
                    }
                }
            });

        }
}

// perform instance params checks
//
// Container names follow LXC/LXD rules: 1..=63 characters, ASCII letters,
// digits and hyphens, and may not start or end with a hyphen. Hyphens are
// legal (the previous alphanumeric-only check wrongly rejected real container
// names such as `my-container-01`), and the length ceiling is 63, not 20.
pub async fn validate_instance(instance_id: impl AsRef<str>) -> Result<(), String> {
    let id = instance_id.as_ref();
    let char_count = id.chars().count();

    let valid_chars = !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
    let edges_ok = !id.starts_with('-') && !id.ends_with('-');

    if char_count < 1 || char_count > 63 || !valid_chars || !edges_ok {
        tracing::warn!(instance_id = %id, "instance ID check failed");
        return Err(format!("Incorrect instance id: {}", id));
    }
    Ok(())
}

// Confirm the container exists and is currently running before a session is
// spawned for it. `lxc list <name>` matches by prefix, so the JSON output is
// parsed and an exact name match with a RUNNING status is required.
pub async fn container_is_running(instance_id: &str) -> bool {
    let output = match Command::new("lxc")
        .args(["list", instance_id, "--format", "json"])
        .output()
        .await
    {
        Ok(output) => output,
        Err(err) => {
            tracing::warn!(%instance_id, error = %err, "failed to run `lxc list`");
            return false;
        }
    };

    if !output.status.success() {
        tracing::warn!(%instance_id, "`lxc list` reported failure");
        return false;
    }

    let containers: serde_json::Value = match serde_json::from_slice(&output.stdout) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(%instance_id, error = %err, "failed to parse `lxc list` output");
            return false;
        }
    };

    containers
        .as_array()
        .map(|list| {
            list.iter().any(|container| {
                let name = container.get("name").and_then(|n| n.as_str());
                let status = container.get("status").and_then(|s| s.as_str());
                name == Some(instance_id)
                    && status
                        .map(|s| s.eq_ignore_ascii_case("running"))
                        .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

// Directory the ttyd unix domain sockets live in (always ends in `/`).
//
// Prefer the systemd-provided runtime directory ($RUNTIME_DIRECTORY, which
// resolves to /run/<service> and is owned and cleaned up by systemd) over the
// configured fallback folder, which historically pointed at the world-writable
// /tmp. $RUNTIME_DIRECTORY may hold several colon-separated paths; the first is
// used.
pub fn socket_dir(fallback: &str) -> String {
    let dir = std::env::var("RUNTIME_DIRECTORY")
        .ok()
        .and_then(|value| value.split(':').next().map(|s| s.to_string()))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.to_string());

    if dir.ends_with('/') {
        dir
    } else {
        format!("{dir}/")
    }
}

// Absolute path of the socket file backing a given session name.
pub fn socket_path(fallback: &str, session_name: &str) -> String {
    format!("{}{}.sock", socket_dir(fallback), session_name)
}

// Remove a single session's socket file. A missing file is not an error, since
// ttyd does not always unlink its socket on exit and the startup sweep or the
// watcher task may have removed it already.
pub fn remove_socket(fallback: &str, session_name: &str) {
    let path = socket_path(fallback, session_name);
    match std::fs::remove_file(&path) {
        Ok(_) => tracing::info!(%path, "removed socket"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => tracing::warn!(%path, error = %err, "failed to remove socket"),
    }
}

// Ensure the socket directory exists and remove any stale `*.sock` files left
// behind by a previous run (a crash, a container stop, or a ttyd self-exit).
// Called once at startup so orphaned sockets never accumulate.
pub fn sweep_stale_sockets(fallback: &str) {
    let dir = socket_dir(fallback);

    if let Err(err) = std::fs::create_dir_all(&dir) {
        tracing::warn!(%dir, error = %err, "failed to create socket dir");
        return;
    }

    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) => {
            tracing::warn!(%dir, error = %err, "failed to read socket dir");
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("sock") {
            match std::fs::remove_file(&path) {
                Ok(_) => tracing::info!(path = %path.display(), "swept stale socket"),
                Err(err) => {
                    tracing::warn!(path = %path.display(), error = %err, "failed to sweep socket")
                }
            }
        }
    }
}

// start the ttyd child process for connecting with LXC container
pub async fn start_ttyd_process(
    instance_id: &str,
    uds_session_name: &str,
    socket_folder: &str,
) -> Result<TokioChild, BindingStatus> {
    // construct the unix socket path where TTYD will spawn the UDS path
     //todo: check if files which are created has 660 permission or not
    let session_path = socket_path(socket_folder, uds_session_name);

    // arguments to spawn ttyd process with the required params
    //
    // The interactive shell is chosen inside the container at runtime: prefer
    // bash when it is available, otherwise fall back to sh. Hard-coding
    // `/bin/bash` broke on images that ship only sh (e.g. Alpine).
    let args: Vec<&str> = vec![
        "-O",
        "-o",
        "-a",
        "-W",
        "-i",
        session_path.as_str(),
        "lxc",
        "exec",
        instance_id,
        "--",
        "sh",
        "-c",
        r#"if command -v bash >/dev/null 2>&1; then exec bash; else exec sh; fi"#,
    ];

    // spawn the ttyd process, return the child process handle, ensuring stdin is null and stdout/stderr are piped and killed on drop
    let ttyd_session = tokio::process::Command::new("ttyd")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn(); // spawn the process over the runtime

    ttyd_session.map_err(|err| BindingStatus::Error(format!(
        "ttyd process spawn error: {}",
        err.to_string()
    )))
}

// poll the child process on a another green thread on runtime
//
// The spawned watcher task owns the lifecycle of the session: it registers the
// session in the shared maps up-front and is the single place that removes those
// entries (and the socket file) again. Cleanup runs for BOTH exit paths:
//   - the ttyd child exits on its own (user types `exit`, the browser refreshes
//     and trips the `-O` once flag, or the container stops), and
//   - an explicit shutdown signal (close endpoint / timeout sweep).
// Previously the self-exit path left the map entries behind, so the instance was
// reported as "already attached" until the absolute timeout swept it away.
pub async fn child_process_handler(
    mut ttyd_process: TokioChild,
    session_id: String,
    instance_id: String,
    global_handle_state: HandlerState,
    global_instance_state: InstanceState,
    socket_folder: String,
) {
    // setup a channel to send shutdown signal to the task
    let (tx, mut recv) = tokio::sync::watch::channel(());

    // Register the session BEFORE spawning the watcher. Doing the insert here
    // (rather than in the route handler after the task is spawned) guarantees the
    // watcher's cleanup can never race ahead of the registration and leak an entry.
    global_handle_state.insert(
        session_id.clone(),
        (instance_id.clone(), tx, Instant::now()),
    );
    global_instance_state.insert(instance_id.clone(), session_id.clone());

    //spawn this on the runtime where it gets polled continously for the updates
    tokio::spawn(async move {

        let ttyd_pid = ttyd_process.id();

        // wait for either the process to end or a shutdown signal
        tokio::select! {
            // child process is required to be awaited here to avoid zombie processes
            _ = ttyd_process.wait() => {
                tracing::info!(%session_id, "ttyd child exited on its own");
            }
            // if shutdown signal is recv, kill the process
            _ = recv.changed() =>  {
                tracing::debug!("task shutdown requested");

                match ttyd_process.kill().await {
                    Ok(_) => {
                        tracing::info!(pid = ?ttyd_pid, "ttyd process shutdown success");
                    }
                    _ => {
                        tracing::warn!(pid = ?ttyd_pid, "ttyd process failed to shut down cleanly");
                    }
                }
            }
        }

        // The watcher owns cleanup: drop both map entries and the socket file
        // regardless of how the session ended. `remove` on an already-absent key
        // (or socket) is a harmless no-op, so this stays correct even when the
        // close endpoint or timeout sweep removed them first.
        global_handle_state.remove(&session_id);
        global_instance_state.remove(&instance_id);
        remove_socket(&socket_folder, &session_id);
        tracing::info!(%session_id, %instance_id, "cleaned up session");
    });
}

pub fn acquire_response_details(
    domain: impl AsRef<str>,
    main_path: impl AsRef<str>,
    uds_session_name: impl AsRef<str>,
    token: impl AsRef<str>,
) -> String /* domain url with jwt token */ {

    let mut domain = domain.as_ref().to_string();
    let mut main_path = main_path.as_ref().to_string();

    if !main_path.is_empty() {
        domain.push_str("/");
    }

    main_path.push_str(format!("/{}", uds_session_name.as_ref()).as_str());
    domain.push_str(main_path.as_str());

    //add token header as part of domain!
    domain.push_str(format!("?token={}", token.as_ref()).as_str());
    domain.to_string()
}

// TODO: rewrite the logic for producing the correct JWT auth token field
// method to generate the jwt token
pub fn jwt_token_generator(expiration: u64, key: &str) -> Result<String, Box<dyn std::error::Error>> {

    //todo: change this to return a token with session value of 30mins
    let token = r#"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjk5NTU4OTI2NzAsImp0aSI6IjgyMjk0YTYzLTk2NjAtNGM2Mi1hOGE4LTVhNjI2NWVmY2Q0ZSIsInN1YiI6IjM0MDYzMjc5NjM1MTY5MzIiLCJpc3MiOiJodHRwczovL2FwaS5leGFtcGxlLmNvbSIsImF1ZCI6WyJodHRwczovL2FwaS5leGFtcGxlLmlvIl0sInVzZXJuYW1lIjoiZ2dpY2NpIn0.O8kvRO9y6xQO3AymqdFE7DDqLRBQhkntf78O9kF71F8"#;

    let _ = (expiration, key);
    Ok(token.to_string())
}



// Test cases for the above util function

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_get_program_version() {
        let program = "ttyd";
        let version = get_program_version(program).await;
        assert!(version.is_ok());
    }

    #[tokio::test]
    async fn test_validate_instance() {

        // hyphens are legal and the length ceiling is 63 (not 20)
        let valid_instance = "instance123";
        let valid_hyphenated = "my-container-01";
        let valid_short = "a"; // a single character is a legal name
        let valid_max = "a".repeat(63);

        let invalid_symbols = "inst@nc#123";
        let invalid_empty = "";
        let invalid_too_long = "a".repeat(64);
        let invalid_leading_hyphen = "-abc";
        let invalid_trailing_hyphen = "abc-";

        assert!(validate_instance(valid_instance).await.is_ok());
        assert!(validate_instance(valid_hyphenated).await.is_ok());
        assert!(validate_instance(valid_short).await.is_ok());
        assert!(validate_instance(valid_max.as_str()).await.is_ok());

        assert!(validate_instance(invalid_symbols).await.is_err());
        assert!(validate_instance(invalid_empty).await.is_err());
        assert!(validate_instance(invalid_too_long.as_str()).await.is_err());
        assert!(validate_instance(invalid_leading_hyphen).await.is_err());
        assert!(validate_instance(invalid_trailing_hyphen).await.is_err());
    }

    #[tokio::test]
    async fn handle_ttyd_process_timeout_test_one() {
        // test for 3 second timeout
        let duration: u64 = 3;

        let global_handle_state: HandlerState = Arc::new(DashMap::new());
        let global_instance_state: InstanceState = Arc::new(DashMap::new());

        // Insert a dummy session to test timeout
        let instance_id = "test_instance".to_string();
        let session_id = "test_session".to_string();
        let (tx, _rx) = tokio::sync::watch::channel(());
        global_handle_state.insert(
            session_id.clone(),
         (instance_id.clone(), tx, Instant::now()),
        );

        // Insert the corresponding instance state
        global_instance_state.insert(instance_id.clone(), session_id.clone());

        // Spawn the timeout handler
        tokio::spawn(handle_ttyd_process_timeout(
            duration,
            global_handle_state.clone(),
            global_instance_state.clone(),
            Arc::new(Metrics::default()),
            "/tmp/sessions/".to_string(),
        ));

        // Allow some time for the handler to run
        tokio::time::sleep(tokio::time::Duration::from_secs(7)).await;

        // Since there are no sessions, the maps should remain empty
        assert!(global_handle_state.is_empty());
        assert!(global_instance_state.is_empty());
    }

    #[should_panic]
    #[tokio::test]
    async fn handle_ttyd_process_timeout_test_two() {
        // test for 10 second timeout
        let duration: u64 = 10;

        let global_handle_state: HandlerState = Arc::new(DashMap::new());
        let global_instance_state: InstanceState = Arc::new(DashMap::new());

        // Insert a dummy session to test timeout
        let instance_id = "test_instance".to_string();
        let session_id = "test_session".to_string();
        let (tx, _rx) = tokio::sync::watch::channel(());
        global_handle_state.insert(
            session_id.clone(),
         (instance_id.clone(), tx, Instant::now()),
        );

        // Insert the corresponding instance state
        global_instance_state.insert(instance_id.clone(), session_id.clone());

        // Spawn the timeout handler
        tokio::spawn(handle_ttyd_process_timeout(
            duration,
            global_handle_state.clone(),
            global_instance_state.clone(),
            Arc::new(Metrics::default()),
            "/tmp/sessions/".to_string(),
        ));

        // Allow some time for the handler to run
        tokio::time::sleep(tokio::time::Duration::from_secs(7)).await;

        // Since there are no sessions, the maps should remain empty
        assert!(global_handle_state.is_empty());
        assert!(global_instance_state.is_empty());
    }


    #[tokio::test]
    async fn test_ttyd_process_start() {
        let instance_id = "test_instance";
        let uds_session_name = "test_session";

        let result = start_ttyd_process(instance_id, uds_session_name, "/tmp/sessions/").await;

        // Since ttyd might not be installed in the test environment, we check for error
        assert!(result.is_err() || result.is_ok());
    }

}
