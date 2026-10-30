use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{CommandFactory, Parser};
use daemonize::Daemonize;

use crate::cli::{CaddyCmd, Cli, Commands, ConfigCmd, SessionCmd};
use crate::config::Config;
use crate::protocol::Request;

mod cli;
mod client;
mod config;
mod daemon;
mod logging;
mod metrics;
mod obj;
mod paths;
mod protocol;
mod routes;
mod utils;

fn main() {
    let cli = Cli::parse();
    let socket = cli.socket.clone();
    let json = cli.json;

    let code = match cli.command {
        Commands::Start {
            foreground,
            with_caddy,
        } => start_daemon(foreground, with_caddy, socket),

        Commands::Restart {
            foreground,
            with_caddy,
        } => restart_daemon(foreground, with_caddy, socket, json),

        Commands::Stop => client::run(Request::Shutdown, socket, json),
        Commands::Status => client::run(Request::Status, socket, json),
        Commands::Metrics => client::run(Request::Metrics, socket, json),

        Commands::Sessions { action } => {
            let request = match action {
                SessionCmd::List => Request::SessionsList,
                SessionCmd::Close { id } => Request::SessionClose { id },
                SessionCmd::Open { instance } => Request::SessionOpen { instance },
            };
            client::run(request, socket, json)
        }

        Commands::Caddy { action } => {
            let request = match action {
                CaddyCmd::Start => Request::CaddyStart,
                CaddyCmd::Stop => Request::CaddyStop,
                CaddyCmd::Restart => Request::CaddyRestart,
                CaddyCmd::Reload => Request::CaddyReload,
                CaddyCmd::Status => Request::CaddyStatus,
            };
            client::run(request, socket, json)
        }

        Commands::Logs {
            follow,
            level,
            component,
        } => client::run(
            Request::LogsSubscribe {
                follow,
                level,
                component,
            },
            socket,
            json,
        ),

        Commands::Config { action } => match action {
            ConfigCmd::Check => config_check(),
            ConfigCmd::Show => client::run(Request::ConfigShow, socket, json),
        },

        Commands::Completions { shell } => {
            let mut command = Cli::command();
            clap_complete::generate(shell, &mut command, "lwt", &mut std::io::stdout());
            0
        }
    };

    std::process::exit(code);
}

/// Load and validate configuration, or print the reason and signal failure.
fn load_config() -> Result<Arc<Config>, i32> {
    dotenv::dotenv().ok();
    match Config::from_env() {
        Ok(config) => Ok(Arc::new(config)),
        Err(err) => {
            eprintln!("lwt: invalid configuration: {err}");
            Err(1)
        }
    }
}

/// Start the daemon. Unless `--foreground`, this double-forks into the
/// background (via `daemonize`) *before* the tokio runtime is built — forking a
/// multi-threaded process is unsafe, so the runtime must come after.
fn start_daemon(foreground: bool, with_caddy: bool, socket_override: Option<String>) -> i32 {
    let config = match load_config() {
        Ok(config) => config,
        Err(code) => return code,
    };

    let control_socket = paths::control_socket(socket_override.as_deref());
    let pid_file = paths::pid_file();
    let log_file = paths::log_file();

    // make sure every runtime directory exists before we need it
    for path in [
        control_socket.as_str(),
        pid_file.as_str(),
        log_file.as_str(),
    ] {
        if let Some(parent) = paths::parent_of(path) {
            let _ = std::fs::create_dir_all(&parent);
        }
    }
    let _ = std::fs::create_dir_all(&config.unix_socket_folder);

    if foreground {
        // record the pid so `status`/`stop` still work in the foreground
        if let Ok(mut file) = File::create(&pid_file) {
            let _ = writeln!(file, "{}", std::process::id());
        }
    } else {
        let stdout = match File::create(&log_file) {
            Ok(file) => file,
            Err(err) => {
                eprintln!("lwt: cannot open log file {log_file}: {err}");
                return 1;
            }
        };
        let stderr = match stdout.try_clone() {
            Ok(file) => file,
            Err(err) => {
                eprintln!("lwt: cannot duplicate log handle: {err}");
                return 1;
            }
        };

        let daemonize = Daemonize::new()
            .pid_file(&pid_file)
            .chown_pid_file(false)
            .working_directory("/")
            .umask(0o027)
            .stdout(stdout)
            .stderr(stderr);

        if let Err(err) = daemonize.start() {
            eprintln!("lwt: failed to start daemon: {err}");
            eprintln!("     (is one already running? check {pid_file})");
            return 1;
        }
        // from here we are the backgrounded daemon process
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("lwt: failed to start runtime: {err}");
            return 1;
        }
    };

    runtime.block_on(daemon::run(config, with_caddy, control_socket));
    0
}

/// Stop a running daemon (if any), wait for it to release the socket, then
/// start a fresh one.
fn restart_daemon(
    foreground: bool,
    with_caddy: bool,
    socket_override: Option<String>,
    json: bool,
) -> i32 {
    let socket_path = paths::control_socket(socket_override.as_deref());

    let running = std::os::unix::net::UnixStream::connect(&socket_path).is_ok();
    if running {
        let _ = client::run(Request::Shutdown, socket_override.clone(), json);

        // wait (up to 10s) for the old daemon to tear down its socket
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if std::os::unix::net::UnixStream::connect(&socket_path).is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    start_daemon(foreground, with_caddy, socket_override)
}

/// Validate the environment and Caddy settings without starting anything.
fn config_check() -> i32 {
    let config = match load_config() {
        Ok(config) => config,
        Err(code) => return code,
    };

    println!("configuration: OK");
    println!("  bind address:        {}", config.bind_address());
    println!("  socket folder:       {}", config.unix_socket_folder);
    println!("  session timeout:     {}s", config.ttyd_session_timeout);
    println!("  caddy binary:        {}", config.caddy_bin);
    println!("  caddy config:        {}", config.caddy_config);
    println!("  caddy autostart:     {}", config.caddy_autostart);

    if !Path::new(&config.caddy_config).exists() {
        println!(
            "  warning: caddy config not found at {}",
            config.caddy_config
        );
    }

    0
}
