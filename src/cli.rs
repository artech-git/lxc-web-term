//! Command-line surface for `lwt`. The first subcommand decides the role:
//! `start` runs (or backgrounds) the daemon, everything else is a client that
//! talks to a running daemon over the control socket.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "lwt",
    version,
    about = "LWT — control plane for the LXC web terminal",
    propagate_version = true,
    disable_help_subcommand = true
)]
pub struct Cli {
    /// Control socket path (default: $RUNTIME_DIRECTORY/control.sock or /tmp/lwt/control.sock)
    #[arg(long, global = true)]
    pub socket: Option<String>,

    /// Emit machine-readable JSON instead of formatted text
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the daemon (session server + supervisor)
    Start {
        /// Run in the foreground instead of backgrounding
        #[arg(long)]
        foreground: bool,
        /// Also start the Caddy reverse proxy
        #[arg(long)]
        with_caddy: bool,
    },

    /// Gracefully stop the running daemon
    Stop,

    /// Restart the daemon
    Restart {
        /// Run the restarted daemon in the foreground
        #[arg(long)]
        foreground: bool,
        /// Also start the Caddy reverse proxy
        #[arg(long)]
        with_caddy: bool,
    },

    /// Show daemon health: pid, uptime, server, caddy, session count
    Status,

    /// Manage terminal sessions
    Sessions {
        #[command(subcommand)]
        action: SessionCmd,
    },

    /// Manage the Caddy reverse proxy
    Caddy {
        #[command(subcommand)]
        action: CaddyCmd,
    },

    /// Stream daemon logs
    Logs {
        /// Keep streaming new log lines
        #[arg(short, long)]
        follow: bool,
        /// Minimum level to show (trace|debug|info|warn|error)
        #[arg(long)]
        level: Option<String>,
        /// Restrict to a component (daemon|caddy|session)
        #[arg(long)]
        component: Option<String>,
    },

    /// Print the metrics snapshot
    Metrics,

    /// Inspect configuration
    Config {
        #[command(subcommand)]
        action: ConfigCmd,
    },

    /// Generate a shell completion script
    Completions {
        /// Target shell
        shell: clap_complete::Shell,
    },
}

#[derive(Subcommand, Debug)]
pub enum SessionCmd {
    /// List active terminal sessions
    List,
    /// Terminate a session by its id
    Close {
        /// Session id (the uds session name)
        id: String,
    },
    /// Provision a session for an LXC instance
    Open {
        /// LXC instance / container name
        instance: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum CaddyCmd {
    /// Start the reverse proxy
    Start,
    /// Stop the reverse proxy
    Stop,
    /// Restart the reverse proxy
    Restart,
    /// Hot-reload the Caddyfile without dropping connections
    Reload,
    /// Show the supervisor state
    Status,
}

#[derive(Subcommand, Debug)]
pub enum ConfigCmd {
    /// Print the effective runtime configuration (secrets redacted)
    Show,
    /// Validate the environment and Caddy settings without starting
    Check,
}
