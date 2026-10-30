//! The CLI client: connect to a running daemon's control socket, send one
//! request, and render the reply. Log subscriptions stream until the stream
//! ends or the user interrupts.

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::paths;
use crate::protocol::{Request, Response};

/// Run a single control request and return a process exit code.
pub fn run(request: Request, socket: Option<String>, json: bool) -> i32 {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("lwt: failed to start runtime: {err}");
            return 1;
        }
    };
    runtime.block_on(execute(request, socket, json))
}

async fn execute(request: Request, socket: Option<String>, json: bool) -> i32 {
    let path = paths::control_socket(socket.as_deref());

    let stream = match UnixStream::connect(&path).await {
        Ok(stream) => stream,
        Err(err) => {
            eprintln!("lwt: cannot reach the daemon at {path} ({err}).");
            eprintln!("     Is it running? Start it with `lwt start`.");
            return 1;
        }
    };

    let streaming = matches!(request, Request::LogsSubscribe { .. });

    let (read_half, mut write_half) = stream.into_split();

    // send the request as one JSON line
    let mut encoded = match serde_json::to_vec(&request) {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("lwt: failed to encode request: {err}");
            return 1;
        }
    };
    encoded.push(b'\n');
    if let Err(err) = write_half.write_all(&encoded).await {
        eprintln!("lwt: failed to send request: {err}");
        return 1;
    }
    let _ = write_half.flush().await;

    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    if streaming {
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => return 0, // daemon closed the stream
                Ok(_) => {}
                Err(err) => {
                    eprintln!("lwt: log stream error: {err}");
                    return 1;
                }
            }
            match serde_json::from_str::<Response>(line.trim()) {
                Ok(Response::Log { line }) => print_log_line(&line, json),
                Ok(Response::EndOfStream) => return 0,
                Ok(Response::Error { message }) => {
                    eprintln!("lwt: {message}");
                    return 1;
                }
                Ok(Response::Ok { .. }) => {}
                Err(err) => eprintln!("lwt: malformed log frame: {err}"),
            }
        }
    }

    // single-response commands
    match reader.read_line(&mut line).await {
        Ok(0) => {
            eprintln!("lwt: daemon closed the connection without a reply");
            1
        }
        Ok(_) => match serde_json::from_str::<Response>(line.trim()) {
            Ok(Response::Ok { data }) => {
                render_ok(&data, json);
                0
            }
            Ok(Response::Error { message }) => {
                eprintln!("lwt: {message}");
                1
            }
            Ok(other) => {
                eprintln!("lwt: unexpected reply: {other:?}");
                1
            }
            Err(err) => {
                eprintln!("lwt: malformed reply: {err}");
                1
            }
        },
        Err(err) => {
            eprintln!("lwt: failed to read reply: {err}");
            1
        }
    }
}

/// Render a successful payload for humans (or raw JSON with `--json`).
fn render_ok(data: &serde_json::Value, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(data).unwrap_or_else(|_| data.to_string())
        );
        return;
    }

    // a list of sessions renders as a small table
    if let Some(sessions) = data.get("sessions").and_then(|v| v.as_array()) {
        if sessions.is_empty() {
            println!("no active sessions");
            return;
        }
        println!("{:<38} {:<24} {:>8}", "SESSION", "INSTANCE", "IDLE(s)");
        for session in sessions {
            let id = session.get("session_id").and_then(|v| v.as_str()).unwrap_or("-");
            let instance = session.get("instance_id").and_then(|v| v.as_str()).unwrap_or("-");
            let idle = session.get("idle_secs").and_then(|v| v.as_u64()).unwrap_or(0);
            println!("{id:<38} {instance:<24} {idle:>8}");
        }
        return;
    }

    // a bare status message
    if let Some(message) = data.get("message").and_then(|v| v.as_str()) {
        println!("{message}");
        return;
    }

    if data.get("pong").is_some() {
        println!("pong");
        return;
    }

    // everything else (status, metrics, caddy, config): pretty JSON
    println!(
        "{}",
        serde_json::to_string_pretty(data).unwrap_or_else(|_| data.to_string())
    );
}

/// Print one streamed log line. Human mode reformats the JSON event into a
/// compact `LEVEL target: message` line; `--json` passes it through.
fn print_log_line(line: &str, json: bool) {
    if json {
        println!("{line}");
        return;
    }
    match serde_json::from_str::<serde_json::Value>(line) {
        Ok(value) => {
            let level = value.get("level").and_then(|v| v.as_str()).unwrap_or("INFO");
            let target = value.get("target").and_then(|v| v.as_str()).unwrap_or("");
            let message = value
                .get("fields")
                .and_then(|f| f.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("");
            println!("{level:<5} {target}: {message}");
        }
        Err(_) => println!("{line}"),
    }
}
