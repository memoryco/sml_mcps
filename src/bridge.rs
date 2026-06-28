//! Bridge - transparent MCP proxy (the shim)
//!
//! A [`Bridge`] pumps JSON-RPC messages between two transports without
//! inspecting them. The shim binary wires a [`StdioTransport`](crate::StdioTransport)
//! (talking to the MCP client) to a [`UnixTransport`](crate::UnixTransport)
//! (talking to the daemon):
//!
//! ```ignore
//! fn main() -> sml_mcps::Result<()> {
//!     let upstream = Bridge::auto_start(
//!         "/tmp/myapp/server.sock",
//!         "myapp-server",
//!         &["--daemon"],
//!     )?;
//!     Bridge::run(StdioTransport::new(), upstream)
//! }
//! ```
//!
//! [`Bridge::auto_start`] connects to an existing daemon, or spawns one and
//! waits for it to come up - including stale socket / dead-PID recovery.
//!
//! Unix-only: gated behind `#[cfg(unix)]`.

use crate::transport::{Transport, UnixTransport, pid_path_for};
use crate::types::{McpError, Result};
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

/// How long to wait for a possibly-wedged daemon (live PID, socket present but
/// not yet accepting) before treating it as dead and restarting.
const STARTUP_GRACE: Duration = Duration::from_secs(2);

/// How long to wait for a freshly-spawned daemon to start listening.
const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Transparent bidirectional MCP proxy.
pub struct Bridge;

impl Bridge {
    /// Connect to a daemon, auto-starting it if needed.
    ///
    /// 1. Try connecting to `socket_path` - if a daemon is up, done.
    /// 2. Otherwise reconcile socket + PID-file state:
    ///    - live PID, socket present -> daemon may be starting; wait briefly.
    ///    - dead PID -> stale; remove socket + PID file and restart.
    ///    - socket but no PID file -> orphaned; remove and restart.
    /// 3. Spawn `daemon_bin` with `daemon_args` (expected to daemonize).
    /// 4. Poll the socket until the daemon is listening, then connect.
    ///
    /// `daemon_bin` must double-fork (e.g. via
    /// [`UnixServer::serve_daemon`](crate::UnixServer::serve_daemon)) so the
    /// spawned process exits promptly once the detached daemon is running.
    pub fn auto_start(
        socket_path: impl AsRef<Path>,
        daemon_bin: &str,
        daemon_args: &[&str],
    ) -> Result<UnixTransport> {
        auto_start_inner(
            socket_path.as_ref(),
            daemon_bin,
            daemon_args,
            STARTUP_GRACE,
            READY_TIMEOUT,
        )
    }

    /// Pump messages between `client` and `upstream` until the upstream side
    /// closes.
    ///
    /// Full duplex: spawns one thread per direction. Each thread owns an
    /// independent read source and write sink (via
    /// [`Transport::try_clone_writer`]), so neither blocks the other.
    ///
    /// Teardown drains cleanly: when the client hangs up, the request pump
    /// half-closes the upstream ([`Transport::close_write`]) so the daemon sees
    /// EOF and flushes its remaining responses; the response pump relays those,
    /// then ends when the daemon closes. `run` returns when that response pump
    /// finishes - i.e. once the upstream connection is fully drained and closed.
    ///
    /// Both transports must support [`Transport::try_clone_writer`] (stdio and
    /// unix do); a transport that doesn't yields an error.
    pub fn run<A: Transport + 'static, B: Transport + 'static>(
        client: A,
        upstream: B,
    ) -> Result<()> {
        let mut client = client;
        let mut upstream = upstream;

        let mut client_writer = client.try_clone_writer().ok_or_else(|| {
            McpError::Internal("client transport cannot be split for proxying".into())
        })?;
        let upstream_writer = upstream.try_clone_writer().ok_or_else(|| {
            McpError::Internal("upstream transport cannot be split for proxying".into())
        })?;

        // client -> upstream (requests). Detached: ends when the client hits
        // EOF, at which point it half-closes the upstream so the daemon can
        // finish up. If the daemon vanished while the client is still talking,
        // this thread outlives `run` and dies when the process exits.
        let _c2u = thread::spawn(move || {
            let mut writer = upstream_writer;
            let _ = pump(&mut client, writer.as_mut());
            let _ = writer.close_write();
        });

        // upstream -> client (responses). Joined: its completion means the
        // upstream connection has drained and closed - the definitive
        // end-of-session signal.
        let result = pump(&mut upstream, client_writer.as_mut());
        let _ = client_writer.close_write();
        result
    }

    /// Bridge stdio (stdin/stdout) to a Unix transport.
    ///
    /// This is the complete shim binary in one call: connects CC's stdio
    /// transport to an upstream daemon.
    ///
    /// # Example
    /// ```ignore
    /// let upstream = Bridge::auto_start("server.sock", "my-daemon", &["--daemon"])?;
    /// Bridge::run_stdio(upstream)?;
    /// ```
    pub fn run_stdio(upstream: UnixTransport) -> Result<()> {
        Self::run(crate::StdioTransport::new(), upstream)
    }
}

/// Forward every message from `reader` to `writer` until end-of-stream.
fn pump(reader: &mut dyn Transport, writer: &mut dyn Transport) -> Result<()> {
    loop {
        match reader.read() {
            Ok(msg) => writer.write(&msg)?,
            Err(McpError::TransportClosed) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

/// Testable core of [`Bridge::auto_start`] with injectable timeouts.
fn auto_start_inner(
    socket_path: &Path,
    daemon_bin: &str,
    daemon_args: &[&str],
    startup_grace: Duration,
    ready_timeout: Duration,
) -> Result<UnixTransport> {
    // Fast path: a daemon is already accepting connections.
    if let Ok(t) = UnixTransport::connect(socket_path) {
        return Ok(t);
    }

    let pid_path = pid_path_for(socket_path);

    if socket_path.exists() {
        // Socket file present but the connect above failed.
        match read_pid_file(&pid_path) {
            Some(pid) if process_alive(pid) => {
                // Daemon may be mid-startup or wedged. Give it a moment.
                if let Some(t) = wait_for_socket(socket_path, startup_grace) {
                    return Ok(t);
                }
                // Still not accepting -> treat as dead. Clear and restart.
                remove_quietly(socket_path);
                remove_quietly(&pid_path);
            }
            Some(_) => {
                // PID file points at a dead process -> stale.
                remove_quietly(socket_path);
                remove_quietly(&pid_path);
            }
            None => {
                // Socket with no PID file -> orphaned.
                remove_quietly(socket_path);
            }
        }
    } else if let Some(pid) = read_pid_file(&pid_path) {
        // No socket yet, but a PID file exists.
        if process_alive(pid) {
            // Daemon may still be binding; wait before deciding to respawn.
            if let Some(t) = wait_for_socket(socket_path, startup_grace) {
                return Ok(t);
            }
        }
        // Dead, or alive-but-never-bound: clear the stale PID file.
        remove_quietly(&pid_path);
    }

    // Spawn the daemon and wait for it to come up.
    spawn_daemon(daemon_bin, daemon_args)?;

    wait_for_socket(socket_path, ready_timeout).ok_or_else(|| {
        McpError::Internal(format!(
            "daemon `{}` did not start listening on {} within {:?}",
            daemon_bin,
            socket_path.display(),
            ready_timeout
        ))
    })
}

/// Read a PID from a PID file (trimmed integer), or `None` if absent/garbage.
fn read_pid_file(path: &Path) -> Option<i32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// True if a process with `pid` exists (via `kill(pid, 0)`).
///
/// `kill` returns 0 if a signal could be delivered; `EPERM` means the process
/// exists but we lack permission to signal it (still alive); `ESRCH` means no
/// such process.
fn process_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    // SAFETY: kill with signal 0 performs only an existence/permission check.
    unsafe {
        if libc::kill(pid, 0) == 0 {
            return true;
        }
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Poll-connect to the socket until success or the timeout elapses.
fn wait_for_socket(path: &Path, timeout: Duration) -> Option<UnixTransport> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(t) = UnixTransport::connect(path) {
            return Some(t);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(25));
    }
}

/// Spawn the daemon process with stdio detached, then reap it.
///
/// The launched process is expected to double-fork and `_exit` once the
/// detached daemon is running, so `wait()` returns promptly. Stdio is routed
/// to `/dev/null` so the daemon's startup output can't corrupt the shim's own
/// stdout (which carries MCP traffic).
fn spawn_daemon(bin: &str, args: &[&str]) -> Result<()> {
    use std::process::{Command, Stdio};

    let mut child = Command::new(bin)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| McpError::Internal(format!("failed to spawn daemon `{}`: {}", bin, e)))?;

    let _ = child.wait();
    Ok(())
}

/// Remove a file, ignoring errors (it may already be gone).
fn remove_quietly(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{Server, ServerConfig, Tool, ToolEnv};
    use crate::transport::{UnixServer, UnixTransport};
    use crate::types::{CallToolResult, JsonRpcMessage, RequestId, Result as McpResult};
    use serde_json::Value;
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_path(suffix: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("sml_mcps_bridge_{}_{}_{}", pid, n, suffix))
    }

    // ---- process_alive / pid file ---------------------------------------

    #[test]
    fn test_process_alive_self() {
        let me = unsafe { libc::getpid() };
        assert!(process_alive(me));
    }

    #[test]
    fn test_process_alive_dead() {
        // PID 0 and absurdly-high PIDs should not be alive.
        assert!(!process_alive(0));
        assert!(!process_alive(-1));
        // 0x7fff_fffe is extremely unlikely to be a live PID.
        assert!(!process_alive(2_147_483_646));
    }

    #[test]
    fn test_read_pid_file_roundtrip() {
        let path = temp_path("pid");
        std::fs::write(&path, "12345\n").unwrap();
        assert_eq!(read_pid_file(&path), Some(12345));
        let _ = std::fs::remove_file(&path);

        // Missing file -> None.
        assert_eq!(read_pid_file(Path::new("/no/such/pid/file")), None);

        // Garbage -> None.
        let path2 = temp_path("pid2");
        std::fs::write(&path2, "not-a-number").unwrap();
        assert_eq!(read_pid_file(&path2), None);
        let _ = std::fs::remove_file(&path2);
    }

    // ---- Bridge::run -----------------------------------------------------

    #[test]
    fn test_run_proxies_both_directions() {
        // Two socket pairs: one stands in for the client connection, the other
        // for the upstream/daemon connection. The bridge sits in the middle.
        let (client_bridge, client_peer) = UnixStream::pair().unwrap();
        let (upstream_bridge, upstream_peer) = UnixStream::pair().unwrap();

        let bridge = thread::spawn(move || {
            Bridge::run(
                UnixTransport::from_stream(client_bridge),
                UnixTransport::from_stream(upstream_bridge),
            )
        });

        // Act as the client and the daemon via the peer ends.
        let mut client = UnixTransport::from_stream(client_peer);
        let mut daemon = UnixTransport::from_stream(upstream_peer);

        // client -> upstream
        let req = JsonRpcMessage::request(1i64, "tools/call", None);
        client.write(&req).unwrap();
        assert_eq!(daemon.read().unwrap(), req);

        // upstream -> client (response)
        let resp = JsonRpcMessage::response(1i64, serde_json::json!({ "ok": true }));
        daemon.write(&resp).unwrap();
        assert_eq!(client.read().unwrap(), resp);

        // A notification followed by another request, to confirm full duplex
        // isn't gated on alternation.
        let note = JsonRpcMessage::notification("notifications/message", None);
        daemon.write(&note).unwrap();
        assert_eq!(client.read().unwrap(), note);

        let req2 = JsonRpcMessage::request(2i64, "ping", None);
        client.write(&req2).unwrap();
        assert_eq!(daemon.read().unwrap(), req2);

        // Tear down both ends: the client hangs up (half-closing the upstream)
        // and the daemon closes, ending the response pump so the bridge returns.
        drop(client);
        drop(daemon);
        let joined = bridge.join().unwrap();
        assert!(joined.is_ok());
    }

    #[test]
    fn test_run_drains_responses_after_client_eof() {
        // Models the shim teardown: the client sends requests then hits EOF; the
        // daemon's already-queued responses must still reach the client before
        // the bridge exits. A full close (instead of half-close) would drop them.
        let (client_bridge, client_peer) = UnixStream::pair().unwrap();
        let (upstream_bridge, upstream_peer) = UnixStream::pair().unwrap();

        // Fake daemon: reply to each request; on read EOF, close (drop).
        let daemon = thread::spawn(move || {
            let mut d = UnixTransport::from_stream(upstream_peer);
            while let Ok(JsonRpcMessage::Request(req)) = d.read() {
                let resp =
                    JsonRpcMessage::response(req.id, serde_json::json!({ "echo": req.method }));
                d.write(&resp).unwrap();
            }
            // d drops here -> upstream closes -> bridge's response pump ends.
        });

        let bridge = thread::spawn(move || {
            Bridge::run(
                UnixTransport::from_stream(client_bridge),
                UnixTransport::from_stream(upstream_bridge),
            )
        });

        let mut client = UnixTransport::from_stream(client_peer);

        // Fire three requests, then signal stdin-EOF via a write half-close.
        for i in 1..=3i64 {
            client
                .write(&JsonRpcMessage::request(i, "ping", None))
                .unwrap();
        }
        client.close_write().unwrap();

        // All three responses must still arrive (drained, not dropped).
        for i in 1..=3i64 {
            match client.read().unwrap() {
                JsonRpcMessage::Response(r) => assert_eq!(r.id, RequestId::Number(i)),
                other => panic!("expected response, got {:?}", other),
            }
        }

        let joined = bridge.join().unwrap();
        assert!(joined.is_ok());
        daemon.join().unwrap();
    }

    #[test]
    fn test_run_shuts_down_when_upstream_closes() {
        let (client_bridge, _client_peer) = UnixStream::pair().unwrap();
        let (upstream_bridge, upstream_peer) = UnixStream::pair().unwrap();

        let bridge = thread::spawn(move || {
            Bridge::run(
                UnixTransport::from_stream(client_bridge),
                UnixTransport::from_stream(upstream_bridge),
            )
        });

        // Drop the daemon side -> upstream read hits EOF -> bridge returns.
        drop(upstream_peer);
        let joined = bridge.join().unwrap();
        assert!(joined.is_ok());
    }

    // ---- auto_start ------------------------------------------------------

    struct PingContext;
    struct PingTool;
    impl Tool<PingContext> for PingTool {
        fn name(&self) -> &str {
            "ping_tool"
        }
        fn description(&self) -> &str {
            "ping"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _a: Value,
            _c: &mut PingContext,
            _e: &ToolEnv,
        ) -> McpResult<CallToolResult> {
            Ok(CallToolResult::text("pong"))
        }
    }

    #[test]
    fn test_auto_start_connects_to_running_daemon() {
        let sock = temp_path("sock");
        let sock_for_server = sock.clone();

        // Stand up a real UnixServer (idle timeout so it eventually exits).
        let _server = thread::spawn(move || {
            UnixServer::new(ServerConfig::default())
                .idle_timeout(Duration::from_secs(10))
                .with_tools(|s: &mut Server<PingContext>| {
                    s.add_tool(PingTool)?;
                    Ok(())
                })
                .serve(&sock_for_server, |_conn_id| PingContext)
        });

        // auto_start should take the fast path and connect without spawning.
        // daemon_bin "false" would fail if spawned, proving we didn't.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut transport = None;
        while Instant::now() < deadline {
            if let Ok(t) = auto_start_inner(
                &sock,
                "false",
                &[],
                Duration::from_millis(50),
                Duration::from_millis(200),
            ) {
                transport = Some(t);
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        let mut transport = transport.expect("auto_start should connect to the running daemon");

        // Drive a request through to prove it's a live connection.
        let req = JsonRpcMessage::request(1i64, "ping", None);
        transport.write(&req).unwrap();
        let resp = transport.read().unwrap();
        if let JsonRpcMessage::Response(r) = resp {
            assert_eq!(r.id, RequestId::Number(1));
        } else {
            panic!("expected response");
        }

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_auto_start_removes_orphaned_socket() {
        let sock = temp_path("sock");
        // Orphaned socket file: present, nothing listening, no PID file.
        std::fs::write(&sock, b"orphan").unwrap();
        assert!(sock.exists());

        // Spawn "true" as the "daemon": it exits immediately without binding,
        // so auto_start removes the orphan, spawns, then times out waiting.
        let result = auto_start_inner(
            &sock,
            "true",
            &[],
            Duration::from_millis(50),
            Duration::from_millis(150),
        );

        // The orphaned socket must have been detected and removed (and "true"
        // didn't recreate it).
        assert!(!sock.exists(), "orphaned socket should be removed");
        // And since "true" never binds, the overall call times out.
        assert!(result.is_err());
    }

    #[test]
    fn test_auto_start_clears_stale_pid_without_socket() {
        let sock = temp_path("sock");
        let pid_path = pid_path_for(&sock);
        // Dead PID, no socket.
        std::fs::write(&pid_path, "2147483646\n").unwrap();
        assert!(pid_path.exists());

        let result = auto_start_inner(
            &sock,
            "true",
            &[],
            Duration::from_millis(50),
            Duration::from_millis(150),
        );

        // Stale PID file cleared; "true" doesn't bind so we still time out.
        assert!(!pid_path.exists(), "stale PID file should be removed");
        assert!(result.is_err());
        let _ = std::fs::remove_file(&sock);
        let _ = std::fs::remove_file(&pid_path);
    }

    #[test]
    fn test_auto_start_spawns_and_connects_via_helper_binary() {
        // End-to-end: spawn a helper that runs a UnixServer in the foreground,
        // and verify auto_start brings it up and connects. Uses `cargo` to run
        // a throwaway? No - instead use a real server started slightly late to
        // exercise the post-spawn wait path deterministically without a binary.
        let sock = temp_path("sock");
        let sock_for_server = sock.clone();

        // Start the server ~150ms after auto_start begins polling, so the
        // wait_for_socket loop has to spin at least once.
        let server = thread::spawn(move || {
            thread::sleep(Duration::from_millis(150));
            UnixServer::new(ServerConfig::default())
                .idle_timeout(Duration::from_secs(5))
                .with_tools(|s: &mut Server<PingContext>| {
                    s.add_tool(PingTool)?;
                    Ok(())
                })
                .serve(&sock_for_server, |_conn_id| PingContext)
        });

        // "true" is a no-op spawn; the real readiness comes from the server
        // thread above. This exercises spawn_daemon + wait_for_socket.
        let transport = auto_start_inner(
            &sock,
            "true",
            &[],
            Duration::from_millis(50),
            Duration::from_secs(5),
        );
        assert!(
            transport.is_ok(),
            "auto_start should connect once server is up"
        );

        drop(transport);
        let _ = server.join();
        let _ = std::fs::remove_file(&sock);
    }

    // ---- multi-bridge integration ----------------------------------------

    /// Context that tracks its connection identity.
    struct BridgeTestContext {
        conn_id: String,
    }

    /// Returns the conn_id so we can verify each bridge reaches its own
    /// daemon-side context.
    struct BridgeWhoamiTool;
    impl Tool<BridgeTestContext> for BridgeWhoamiTool {
        fn name(&self) -> &str {
            "whoami"
        }
        fn description(&self) -> &str {
            "Return connection id"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _a: Value,
            ctx: &mut BridgeTestContext,
            _e: &ToolEnv,
        ) -> McpResult<CallToolResult> {
            Ok(CallToolResult::text(ctx.conn_id.clone()))
        }
    }

    /// Helper: send initialize handshake through a raw UnixTransport.
    fn bridge_initialize(t: &mut UnixTransport, id: i64) {
        let req = JsonRpcMessage::request(
            id,
            "initialize",
            Some(serde_json::json!({
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "1.0" }
            })),
        );
        t.write(&req).unwrap();
        let resp = t.read().unwrap();
        assert!(matches!(resp, JsonRpcMessage::Response(_)));
    }

    /// Helper: call a tool through a raw UnixTransport and return the text.
    fn bridge_call_tool(t: &mut UnixTransport, id: i64, name: &str, args: Value) -> String {
        let req = JsonRpcMessage::request(
            id,
            "tools/call",
            Some(serde_json::json!({ "name": name, "arguments": args })),
        );
        t.write(&req).unwrap();
        let resp = t.read().unwrap();
        match resp {
            JsonRpcMessage::Response(r) => {
                let result = r.result.expect("expected result");
                result["content"][0]["text"]
                    .as_str()
                    .expect("text content")
                    .to_string()
            }
            other => panic!("expected response, got {:?}", other),
        }
    }

    #[test]
    fn test_multi_bridge_concurrent_no_crosstalk() {
        // End-to-end: 3 bridges (simulating 3 CC terminals with shims) all
        // connected to the same daemon, sending interleaved requests.
        // Verifies each bridge gets responses routed to the correct client.
        let sock = temp_path("sock");
        let sock_for_server = sock.clone();

        // Stand up the daemon.
        let _server = thread::spawn(move || {
            UnixServer::new(ServerConfig::default())
                .idle_timeout(Duration::from_secs(10))
                .with_tools(|s: &mut Server<BridgeTestContext>| {
                    s.add_tool(BridgeWhoamiTool)?;
                    Ok(())
                })
                .serve(&sock_for_server, |conn_id| BridgeTestContext {
                    conn_id: conn_id.to_string(),
                })
        });

        // Wait for the daemon to be ready.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if UnixTransport::connect(&sock).is_ok() {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        // Spin up 3 bridges. Each one gets a UnixStream::pair() for the
        // "client" side and a real connection to the daemon for the upstream.
        let mut clients = Vec::new();
        let mut bridge_handles = Vec::new();

        for _ in 0..3 {
            let (client_bridge, client_peer) = UnixStream::pair().unwrap();
            let upstream = UnixTransport::connect(&sock).unwrap();

            let handle = thread::spawn(move || {
                Bridge::run(
                    UnixTransport::from_stream(client_bridge),
                    upstream,
                )
            });

            clients.push(UnixTransport::from_stream(client_peer));
            bridge_handles.push(handle);
        }

        // Initialize all three through their bridges.
        for client in clients.iter_mut() {
            bridge_initialize(client, 1);
        }

        // Each bridge should get a unique conn_id from the daemon.
        let mut conn_ids: Vec<String> = Vec::new();
        for client in clients.iter_mut() {
            conn_ids.push(bridge_call_tool(client, 2, "whoami", serde_json::json!({})));
        }
        assert_ne!(conn_ids[0], conn_ids[1]);
        assert_ne!(conn_ids[1], conn_ids[2]);
        assert_ne!(conn_ids[0], conn_ids[2]);

        // Interleave requests in a different order and verify each bridge
        // still gets its own conn_id back — no cross-talk.
        for round in 3..=5i64 {
            // Reverse order each round to stress the routing.
            let order: Vec<usize> = if round % 2 == 0 {
                vec![0, 1, 2]
            } else {
                vec![2, 1, 0]
            };
            for &i in &order {
                let got = bridge_call_tool(
                    &mut clients[i],
                    round,
                    "whoami",
                    serde_json::json!({}),
                );
                assert_eq!(
                    got, conn_ids[i],
                    "bridge {} got conn_id {} but expected {} (round {})",
                    i, got, conn_ids[i], round
                );
            }
        }

        // Clean shutdown: drop all clients, bridges will see EOF and exit.
        drop(clients);
        for h in bridge_handles {
            let _ = h.join();
        }
        let _ = std::fs::remove_file(&sock);
    }
}
