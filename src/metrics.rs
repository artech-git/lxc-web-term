use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

/// Process-wide counters for the terminal service, shared behind an `Arc` and
/// updated from the request handlers and the timeout sweep. The active-session
/// gauge is not stored here; it is read directly from the session map at scrape
/// time so it never drifts.
#[derive(Default, Debug)]
pub struct Metrics {
    /// Sessions successfully started.
    sessions_started: AtomicU64,
    /// ttyd spawn attempts that failed.
    spawn_failures: AtomicU64,
    /// Sessions that have ended (closed or timed out).
    sessions_completed: AtomicU64,
    /// Summed lifetime of completed sessions, in seconds.
    total_session_duration_secs: AtomicU64,
}

impl Metrics {
    pub fn record_session_started(&self) {
        self.sessions_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_spawn_failure(&self) {
        self.spawn_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a session ending after `duration_secs` seconds of life.
    pub fn record_session_completed(&self, duration_secs: u64) {
        self.sessions_completed.fetch_add(1, Ordering::Relaxed);
        self.total_session_duration_secs
            .fetch_add(duration_secs, Ordering::Relaxed);
    }

    /// Point-in-time view for the `/metrics` endpoint. `active_sessions` is
    /// passed in by the caller (the live session-map length).
    pub fn snapshot(&self, active_sessions: u64) -> MetricsSnapshot {
        let completed = self.sessions_completed.load(Ordering::Relaxed);
        let total_duration = self.total_session_duration_secs.load(Ordering::Relaxed);

        MetricsSnapshot {
            active_sessions,
            sessions_started_total: self.sessions_started.load(Ordering::Relaxed),
            spawn_failures_total: self.spawn_failures.load(Ordering::Relaxed),
            sessions_completed_total: completed,
            avg_session_duration_secs: if completed > 0 {
                total_duration as f64 / completed as f64
            } else {
                0.0
            },
        }
    }
}

/// Serializable snapshot returned by the `/metrics` endpoint.
#[derive(Serialize, Debug)]
pub struct MetricsSnapshot {
    pub active_sessions: u64,
    pub sessions_started_total: u64,
    pub spawn_failures_total: u64,
    pub sessions_completed_total: u64,
    pub avg_session_duration_secs: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reflects_recorded_events() {
        let m = Metrics::default();
        m.record_session_started();
        m.record_session_started();
        m.record_spawn_failure();
        m.record_session_completed(10);
        m.record_session_completed(20);

        let snap = m.snapshot(3);
        assert_eq!(snap.active_sessions, 3);
        assert_eq!(snap.sessions_started_total, 2);
        assert_eq!(snap.spawn_failures_total, 1);
        assert_eq!(snap.sessions_completed_total, 2);
        assert_eq!(snap.avg_session_duration_secs, 15.0);
    }

    #[test]
    fn average_duration_is_zero_without_completions() {
        let m = Metrics::default();
        assert_eq!(m.snapshot(0).avg_session_duration_secs, 0.0);
    }
}
