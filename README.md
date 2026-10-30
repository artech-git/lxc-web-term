# LWT — LXC Web Terminal

LWT gives browser-based access to the shell of an LXC container. A long-running
daemon provisions terminal sessions over HTTP, supervises the Caddy reverse
proxy, and is driven entirely through a small command-line tool that talks to
the daemon over a local control socket.

## Features

- Launch a web terminal for a named LXC container
- Close a session over the control socket or the REST API
- Idle session timeout (configurable, default 30 minutes)
- One Unix domain socket per session for simple routing
- Caddy reverse proxy that maps an HTTP path to each session socket
- Background daemon with a single CLI to control everything:
  start/stop, live status, session management, Caddy control, and logs

## How it works

```
Browser ──> Caddy (reverse proxy) ──> per-session Unix socket ──> ttyd ──> lxc exec <container>
                                                    ^
                                            LWT daemon
                              (session HTTP API + Caddy supervisor + control socket)
```

The data path (browser to shell) runs through Caddy and ttyd. The LWT daemon is
the control plane: it spawns ttyd per session, tracks sessions, supervises
Caddy, and answers the `lwt` CLI on a local Unix socket.

## Dependencies

- `ttyd` — serves the web terminal for each container shell
- `caddy` — reverse proxy in front of the per-session sockets
- `lxc` — the containers LWT attaches to
- Write permission on the socket directory

## Build

```bash
cargo build --release
```

The binary is `target/release/lwt`.

## Usage

Everything is a subcommand of `lwt`. Run `lwt --help` for the full menu.

Daemon:

```bash
lwt start               # start the daemon in the background
lwt start --foreground  # run in the foreground (for systemd or debugging)
lwt start --with-caddy   # also start the reverse proxy
lwt status              # pid, uptime, server address, caddy state, sessions
lwt stop                # graceful shutdown
lwt restart             # stop then start
```

Sessions:

```bash
lwt sessions list              # active sessions
lwt sessions open <container>  # provision a session for a container
lwt sessions close <id>        # terminate a session
```

Caddy:

```bash
lwt caddy start
lwt caddy stop
lwt caddy restart
lwt caddy reload    # hot-reload the Caddyfile
lwt caddy status
```

Observability:

```bash
lwt logs                          # recent log lines
lwt logs --follow                 # live stream
lwt logs --level warn             # minimum level
lwt logs --component caddy        # daemon | caddy | session
lwt metrics                       # session counters
```

Configuration:

```bash
lwt config show     # effective config (secrets redacted)
lwt config check    # validate settings without starting
```

Add `--json` to any command for machine-readable output, and `--socket <path>`
to point at a non-default control socket.

Shell completion:

```bash
lwt completions bash > /etc/bash_completion.d/lwt
```

## Configuration

Settings are read from the environment (a `.env` file in the working directory
is loaded automatically). Required values:

| Variable | Meaning |
| --- | --- |
| `SERVER_INTERFACE_ADDR` | Interface the HTTP server binds to |
| `TERM_SERVER_PORT` | Port the HTTP server binds to |
| `TTYD_SESSION_TIMEOUT` | Idle session timeout, in seconds |
| `UNIX_SOCKET_FOLDER` | Directory for per-session sockets |
| `MACHINE_ID` | Host identifier used in session URLs |
| `SERVING_NAME` | Public domain used in session URLs |
| `MZ_TERM_JWT_SINGING_SECRET` | Secret for signing access tokens |
| `MZ_TERM_JWT_TIMEOUT` | Token lifetime, in seconds |

Optional values (with defaults):

| Variable | Default | Meaning |
| --- | --- | --- |
| `CADDY_BIN` | `caddy` | Caddy binary to supervise |
| `CADDY_CONFIG` | `/etc/caddy/Caddyfile` | Caddyfile the supervised Caddy loads |
| `CADDY_AUTOSTART` | `false` | Start Caddy automatically with the daemon |
| `LWT_CONTROL_SOCKET` | `<runtime>/control.sock` | Control socket path |
| `LWT_PID_FILE` | `<runtime>/lwt.pid` | Daemon pid file |
| `LWT_LOG_FILE` | `<runtime>/lwt.log` | Log file when backgrounded |

`<runtime>` is `$RUNTIME_DIRECTORY` (set by systemd) if present, otherwise
`/tmp/lwt`.

## Running under systemd

```ini
[Unit]
Description=LXC Web Terminal
After=network.target

[Service]
ExecStart=/usr/local/bin/lwt start --foreground --with-caddy
RuntimeDirectory=lwt
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

With `RuntimeDirectory=lwt`, the control socket, pid file, and log file live
under `/run/lwt`, owned and cleaned up by systemd.

## License

This project is licensed under the MIT License.
