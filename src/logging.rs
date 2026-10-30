//! Daemon logging setup.
//!
//! Two sinks share one `tracing` subscriber:
//!   * a JSON formatter on stdout (captured to the log file when backgrounded),
//!   * a JSON formatter writing into a broadcast channel so the control socket
//!     can stream live logs to `lwt logs --follow`.
//!
//! Both honour `RUST_LOG` (defaulting to `info`).

use std::io::{self, Write};

use tokio::sync::broadcast;
use tracing_subscriber::fmt::MakeWriter;

/// Capacity of the live-log broadcast channel. A slow `logs --follow` client
/// that falls behind simply skips the missed lines rather than stalling the
/// daemon.
const LOG_CHANNEL_CAPACITY: usize = 2048;

/// Build the shared broadcast sender for live log streaming.
pub fn channel() -> broadcast::Sender<String> {
    broadcast::channel(LOG_CHANNEL_CAPACITY).0
}

/// Install the global subscriber. Safe to call once; a second call is ignored.
pub fn init(log_tx: &broadcast::Sender<String>) {
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let stdout_layer = fmt::layer().json();
    let broadcast_layer = fmt::layer()
        .json()
        .with_ansi(false)
        .with_writer(BroadcastWriter {
            tx: log_tx.clone(),
        });

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(broadcast_layer)
        .try_init();
}

/// A `MakeWriter` that forwards each formatted log event (one JSON object per
/// event) into the broadcast channel.
#[derive(Clone)]
struct BroadcastWriter {
    tx: broadcast::Sender<String>,
}

impl<'a> MakeWriter<'a> for BroadcastWriter {
    type Writer = BroadcastGuard;

    fn make_writer(&'a self) -> Self::Writer {
        BroadcastGuard {
            tx: self.tx.clone(),
            buf: Vec::new(),
        }
    }
}

/// Accumulates the bytes of a single formatted event and ships the completed
/// line to the broadcast channel when the writer is dropped (the fmt layer uses
/// a fresh writer per event, so drop marks the end of the line).
struct BroadcastGuard {
    tx: broadcast::Sender<String>,
    buf: Vec<u8>,
}

impl Write for BroadcastGuard {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for BroadcastGuard {
    fn drop(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        if let Ok(line) = String::from_utf8(std::mem::take(&mut self.buf)) {
            // ignore the error when there are no subscribers
            let _ = self.tx.send(line.trim_end().to_string());
        }
    }
}
