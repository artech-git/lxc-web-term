//! Supervisor for the Caddy reverse-proxy child process.
//!
//! A single owner task holds the running child and processes commands
//! sequentially, which avoids holding a lock across an await point. The child
//! is watched by a short-lived task that reports its exit back through the same
//! command channel, so the owner never has to borrow the child while also
//! waiting on it. When Caddy is wanted (`desired`) but exits, it is respawned
//! after a bounded backoff.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};

use crate::config::Config;

/// Largest backoff between crash-triggered restarts.
const MAX_BACKOFF_SECS: u64 = 30;

/// Point-in-time view of the supervisor, cheap to clone for `caddy status`.
#[derive(Serialize, Clone, Debug, Default)]
pub struct CaddyStatus {
    pub running: bool,
    pub pid: Option<u32>,
    pub config_path: String,
    pub binary: String,
    pub restarts: u64,
    pub last_exit_code: Option<i32>,
    pub last_error: Option<String>,
}

/// Commands accepted by the supervisor task.
enum CaddyCmd {
    Start(oneshot::Sender<Result<(), String>>),
    Stop(oneshot::Sender<Result<(), String>>),
    Restart(oneshot::Sender<Result<(), String>>),
    Reload(oneshot::Sender<Result<(), String>>),
    /// Internal: the watched child exited with this status code.
    ChildExited { code: Option<i32> },
    /// Internal: time to respawn after a backoff.
    Respawn,
}

/// Handle used by the rest of the daemon to drive the supervisor.
#[derive(Clone)]
pub struct CaddyHandle {
    tx: mpsc::Sender<CaddyCmd>,
    status: Arc<Mutex<CaddyStatus>>,
}

impl CaddyHandle {
    pub async fn start(&self) -> Result<(), String> {
        self.request(CaddyCmd::Start).await
    }

    pub async fn stop(&self) -> Result<(), String> {
        self.request(CaddyCmd::Stop).await
    }

    pub async fn restart(&self) -> Result<(), String> {
        self.request(CaddyCmd::Restart).await
    }

    pub async fn reload(&self) -> Result<(), String> {
        self.request(CaddyCmd::Reload).await
    }

    /// Snapshot of the current supervisor state.
    pub fn status(&self) -> CaddyStatus {
        self.status.lock().expect("caddy status lock").clone()
    }

    async fn request<F>(&self, make: F) -> Result<(), String>
    where
        F: FnOnce(oneshot::Sender<Result<(), String>>) -> CaddyCmd,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(make(reply_tx))
            .await
            .map_err(|_| "caddy supervisor is not running".to_string())?;
        reply_rx
            .await
            .map_err(|_| "caddy supervisor dropped the request".to_string())?
    }
}

/// Start the supervisor task and return a handle to it. The task runs until the
/// command channel is closed (i.e. every handle is dropped).
pub fn spawn(config: Arc<Config>) -> CaddyHandle {
    let (tx, rx) = mpsc::channel(32);
    let status = Arc::new(Mutex::new(CaddyStatus {
        config_path: config.caddy_config.clone(),
        binary: config.caddy_bin.clone(),
        ..Default::default()
    }));

    let actor = Supervisor {
        config,
        rx,
        self_tx: tx.clone(),
        status: status.clone(),
        desired: false,
        current_pid: None,
    };
    tokio::spawn(actor.run());

    CaddyHandle { tx, status }
}

struct Supervisor {
    config: Arc<Config>,
    rx: mpsc::Receiver<CaddyCmd>,
    self_tx: mpsc::Sender<CaddyCmd>,
    status: Arc<Mutex<CaddyStatus>>,
    desired: bool,
    current_pid: Option<u32>,
}

impl Supervisor {
    async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                CaddyCmd::Start(reply) => {
                    let _ = reply.send(self.handle_start());
                }
                CaddyCmd::Stop(reply) => {
                    self.desired = false;
                    self.kill_current();
                    self.set_running(false, None);
                    let _ = reply.send(Ok(()));
                }
                CaddyCmd::Restart(reply) => {
                    self.desired = true;
                    if self.current_pid.is_some() {
                        // the child exit will trigger a respawn because `desired`
                        // is set; nothing else to do here
                        self.kill_current();
                        let _ = reply.send(Ok(()));
                    } else {
                        let _ = reply.send(self.handle_start());
                    }
                }
                CaddyCmd::Reload(reply) => {
                    let _ = reply.send(self.handle_reload().await);
                }
                CaddyCmd::ChildExited { code } => {
                    self.current_pid = None;
                    self.record_exit(code);
                    if self.desired {
                        self.schedule_respawn();
                    }
                }
                CaddyCmd::Respawn => {
                    if self.desired && self.current_pid.is_none() {
                        if let Err(err) = self.spawn_child() {
                            self.record_error(err);
                        }
                    }
                }
            }
        }
    }

    fn handle_start(&mut self) -> Result<(), String> {
        if self.current_pid.is_some() {
            return Ok(());
        }
        self.desired = true;
        self.spawn_child()
    }

    /// Spawn the Caddy child and a watcher task that reports its exit back.
    fn spawn_child(&mut self) -> Result<(), String> {
        let mut child = Command::new(&self.config.caddy_bin)
            .arg("run")
            .arg("--config")
            .arg(&self.config.caddy_config)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|err| format!("failed to start caddy: {err}"))?;

        let pid = child.id();
        self.current_pid = pid;
        self.set_running(true, pid);
        tracing::info!(target: "lwt::caddy", ?pid, config = %self.config.caddy_config, "caddy started");

        let reporter = self.self_tx.clone();
        tokio::spawn(async move {
            let code = match child.wait().await {
                Ok(status) => status.code(),
                Err(_) => None,
            };
            let _ = reporter.send(CaddyCmd::ChildExited { code }).await;
        });

        Ok(())
    }

    async fn handle_reload(&mut self) -> Result<(), String> {
        let output = Command::new(&self.config.caddy_bin)
            .arg("reload")
            .arg("--config")
            .arg(&self.config.caddy_config)
            .output()
            .await
            .map_err(|err| format!("failed to run caddy reload: {err}"))?;

        if output.status.success() {
            tracing::info!(target: "lwt::caddy", "caddy configuration reloaded");
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            Err(format!("caddy reload failed: {stderr}"))
        }
    }

    /// Send SIGTERM to the current child (if any). The watcher task observes the
    /// exit and reports it; `kill_on_drop` is the backstop.
    fn kill_current(&mut self) {
        if let Some(pid) = self.current_pid.take() {
            // SAFETY: a plain kill(2) with SIGTERM; an invalid pid is a no-op error.
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
            tracing::info!(target: "lwt::caddy", pid, "sent SIGTERM to caddy");
        }
    }

    fn schedule_respawn(&self) {
        let restarts = self.status.lock().expect("caddy status lock").restarts;
        let backoff = (1u64 << restarts.min(5)).min(MAX_BACKOFF_SECS);
        let reporter = self.self_tx.clone();
        tracing::warn!(target: "lwt::caddy", backoff_secs = backoff, "scheduling caddy respawn");
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            let _ = reporter.send(CaddyCmd::Respawn).await;
        });
    }

    fn set_running(&self, running: bool, pid: Option<u32>) {
        let mut status = self.status.lock().expect("caddy status lock");
        status.running = running;
        status.pid = pid;
        if running {
            status.last_error = None;
        }
    }

    fn record_exit(&self, code: Option<i32>) {
        let mut status = self.status.lock().expect("caddy status lock");
        status.running = false;
        status.pid = None;
        status.last_exit_code = code;
        if self.desired {
            status.restarts += 1;
        }
        tracing::warn!(target: "lwt::caddy", ?code, "caddy exited");
    }

    fn record_error(&self, err: String) {
        let mut status = self.status.lock().expect("caddy status lock");
        status.running = false;
        status.pid = None;
        status.last_error = Some(err.clone());
        tracing::error!(target: "lwt::caddy", %err, "caddy respawn failed");
    }
}
