# Unix Socket Transport + Daemon/Shim Architecture

**Date:** 2026-06-27
**Authors:** Brandon Sneed, Porter
**Branch:** dev

## Goal

Add Unix domain socket support to sml_mcps so that multiple MCP clients (e.g. Claude Code terminals) can share a single MCP server instance instead of each spawning their own.

The problem: MCP clients spawn a fresh server process per session. For servers with heavy state — local ML models, large indexes, agent registries — duplicating that per client is wasteful or broken. Multiple instances of an agent manager mean session A can't see agents spawned by session B. The daemon/shim pattern solves both: one long-lived daemon holds the expensive state, lightweight shims proxy MCP traffic from each client.

## Current State

sml_mcps has two transports:
- `StdioTransport` — stdin/stdout, used by MCP servers spawned by clients like Claude Code
- `HttpTransport` — Streamable HTTP via tiny_http (behind `http` feature flag, external dep)

And two server modes:
- `Server::start(transport, context)` — blocking loop, one client, reads until transport closes
- `HttpServer` — wraps tiny_http, accepts HTTP requests, creates fresh Server per request

There's no way to share a server instance across multiple client sessions.

## Changes

### 1. `UnixTransport` — new Transport impl

Reads/writes JSON-RPC messages over a `UnixStream`. No external deps — `std::os::unix::net` only. Guarded by `#[cfg(unix)]` at the module level, not a feature flag.

```rust
use std::os::unix::net::UnixStream;

pub struct UnixTransport {
    reader: BufReader<UnixStream>,
    writer: UnixStream, // clone of the stream for writing
}

impl UnixTransport {
    /// Connect to an existing daemon socket (shim/client side)
    pub fn connect(path: impl AsRef<Path>) -> Result<Self>;

    /// Wrap an already-accepted stream (daemon/server side)
    pub fn from_stream(stream: UnixStream) -> Self;
}

impl Transport for UnixTransport {
    fn read(&mut self) -> Result<JsonRpcMessage>;  // line-delimited JSON from socket
    fn write(&mut self, message: &JsonRpcMessage) -> Result<()>;
    fn close(&mut self) -> Result<()>;  // shutdown the stream
}
```

Wire format is the same as stdio — newline-delimited JSON-RPC. No framing changes needed.

### 2. `UnixServer<C>` — daemon listener

Mirrors `HttpServer<C>`. Binds a `UnixListener`, accepts connections, spawns a thread per connection, processes tool calls. Thread-per-connection is fine — connection count is single digits (handful of client sessions).

```rust
pub struct UnixServer<C> {
    config: ServerConfig,
    setup: Option<SetupFn<C>>,
    idle_timeout: Option<Duration>,
}

impl<C: Send + Sync + 'static> UnixServer<C> {
    pub fn new(config: ServerConfig) -> Self;

    /// Configure tools via a setup closure (same pattern as HttpServer)
    pub fn with_tools<F>(self, setup: F) -> Self
    where
        F: Fn(&mut Server<C>) -> Result<()> + Send + Sync + 'static;

    /// How long to stay alive after last connection disconnects.
    /// Default: None (run forever). Recommended: 5 minutes for model-heavy daemons.
    pub fn idle_timeout(self, duration: Duration) -> Self;

    /// Run in the foreground (for debugging/development).
    /// Binds socket, accepts connections, blocks.
    pub fn serve<F>(self, socket_path: impl AsRef<Path>, context_factory: F) -> Result<()>
    where
        F: Fn(&str) -> C + Send + Sync + 'static;

    /// Daemonize, then serve.
    /// Double-forks, detaches (setsid), writes PID file, binds socket.
    /// PID file written to same directory as socket (e.g. ~/.myapp/server.pid).
    pub fn serve_daemon<F>(self, socket_path: impl AsRef<Path>, context_factory: F) -> Result<()>
    where
        F: Fn(&str) -> C + Send + Sync + 'static;
}
```

Context factory receives `conn_id: &str` — a unique identifier per connection. Tools that need isolation (agents) scope by conn_id. Tools that don't (filesystem) ignore it.

#### Accept loop internals

```
listener.accept() loop (main thread)
  └─ spawn std::thread per connection
       ├─ generate conn_id (UUID or monotonic counter)
       ├─ context = context_factory(conn_id)
       ├─ transport = UnixTransport::from_stream(stream)
       ├─ server = Server::new(config) + setup tools
       ├─ server.start(transport, context)
       ├─ on disconnect: decrement active_connections
       └─ if active_connections == 0: start idle timer
```

#### Idle timeout behavior

When last connection closes, start a timer (default 5 min if set). If a new connection arrives before it fires, cancel the timer. If the timer fires, clean up socket and PID file, exit. This prevents reloading multi-GB models because someone closed a terminal and opened a new one seconds later.

### 3. `Bridge` — transparent MCP proxy

The shim itself. Takes two transports, pumps messages bidirectionally. No tool registration, no message inspection — just a pass-through. `initialize`, `tools/list`, `tools/call`, everything forwards to the daemon.

```rust
pub struct Bridge;

impl Bridge {
    /// Connect to a daemon, auto-starting it if needed.
    ///
    /// 1. Try connecting to socket_path
    /// 2. If refused/missing: check PID file
    ///    - PID file exists, process alive → wait and retry
    ///    - PID file exists, process dead → stale; nuke socket + PID, start daemon
    ///    - No PID file → start daemon
    /// 3. Start daemon: spawn `{daemon_bin} --daemon`, poll socket until ready
    /// 4. Connect
    pub fn auto_start(
        socket_path: impl AsRef<Path>,
        daemon_bin: &str,
        daemon_args: &[&str],
    ) -> Result<UnixTransport>;

    /// Pump messages between client and upstream until either side closes.
    pub fn run<A: Transport, B: Transport>(client: A, upstream: B) -> Result<()>;
}
```

Complete shim binary:

```rust
fn main() -> Result<()> {
    let upstream = Bridge::auto_start(
        "~/.myapp/server.sock",
        "myapp-server",
        &["--daemon"],
    )?;
    Bridge::run(StdioTransport::new(), upstream)
}
```

### 4. Stale socket / PID file handling

PID file lives alongside socket (e.g. `~/.myapp/server.pid`). Contains just the daemon PID as text.

Stale detection in `Bridge::auto_start`:
1. Socket exists but connection refused → check PID file
2. Read PID from file, call `kill(pid, 0)` (signal 0 = existence check)
3. Process alive → daemon is starting up or wedged. Wait briefly (2s), retry connect. If still failing, treat as dead.
4. Process dead → stale. Remove socket and PID file, start fresh daemon.
5. No PID file but socket exists → orphaned socket. Remove and start fresh.

Daemon writes PID file after fork but before binding socket. Removes both on clean shutdown.

### 5. Daemonize helper

`serve_daemon` handles the Unix daemon dance:
1. First `fork()` — parent gets child PID, prints it, exits
2. `setsid()` — new session, detach from terminal
3. Second `fork()` — prevent re-acquiring a terminal
4. Write PID file
5. Redirect stdin/stdout/stderr to /dev/null (or a log file)
6. Bind socket and enter accept loop

This is all `libc` calls via `std::os::unix` — no external deps.

## Test Plan

### Unit tests

- `UnixTransport`: message roundtrip through a `UnixStream::pair()` — write on one end, read on the other. Test newline-delimited framing, large messages, connection close detection.
- `Bridge::run`: create two `UnixStream::pair()`s, pump messages through, verify both directions work and clean shutdown when one side closes.
- Stale socket detection: create a socket file without a listener, verify `auto_start` detects and removes it.
- PID file lifecycle: verify written on daemon start, removed on clean shutdown.

### Integration tests

- Start a `UnixServer` with a simple echo tool on a temp socket, connect a `UnixTransport`, send `initialize` + `tools/list` + `tools/call`, verify responses.
- Multi-client: connect 3 clients to same `UnixServer`, each call tools concurrently, verify no cross-talk.
- `conn_id` isolation: connect 2 clients, have each create scoped data, verify each only sees its own via `conn_id`.
- Idle timeout: start server with 1s timeout, connect, disconnect, verify server exits after ~1s. Reconnect before timeout, verify it cancels.
- `auto_start`: point Bridge at a non-existent socket, verify it spawns the daemon binary and connects. (Needs a test daemon binary — an example or test helper.)

### NOT testing

- `serve_daemon` double-fork in CI (process management in automated tests is fragile). Verify the non-daemon `serve` path works, trust the fork logic via manual testing.

## Dependencies

None. Everything is `std::os::unix` and `libc`. No new crate dependencies. No feature flag — just `#[cfg(unix)]` module gating.

## Notes

- The `context_factory` signature changes from `Fn() -> C` (HttpServer without auth) to `Fn(&str) -> C` for UnixServer. This is intentional — conn_id is always available. If a tool doesn't need it, it ignores it.
- `HttpServer::serve` keeps its existing `Fn() -> C` signature. No breaking change.
- Socket and PID file paths are entirely up to the consumer. sml_mcps doesn't enforce any directory convention.
- The Bridge is deliberately dumb — no message inspection, no middleware hooks. If we need request transformation later (injecting session IDs, filtering tools), that's a separate concern and can be layered on top. KISS for now.
- Windows is explicitly unsupported. `#[cfg(unix)]` means this module doesn't compile on Windows. If Windows support becomes needed, named pipes would be the equivalent transport, but that's a future problem.
- `idle_timeout` should be generous for daemons with heavy startup costs (loading ML models, building indexes) and can be `None` (run forever) for lightweight ones. Let the consumer binary decide.
- Race condition on daemon startup: two shims both detect no daemon and both try to start one. The socket `bind()` is the mutex — first one to bind wins. Second fork's bind fails, it sees the socket is now live, connects instead. The PID file write is best-effort in this race; the socket bind is the source of truth.
