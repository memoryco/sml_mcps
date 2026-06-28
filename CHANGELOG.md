## v0.5.0

### Features

- **Unix socket transport** — `UnixTransport` for reading/writing MCP over Unix domain sockets. Connect mode for clients, `from_stream` for servers.
- **`UnixServer`** — daemon-style MCP server over Unix sockets. Thread-per-connection, `idle_timeout` for auto-shutdown, `conn_id`-based session isolation. Supports both foreground (`serve`) and daemonized (`serve_daemon`) modes.
- **`Bridge`** — transparent bidirectional MCP proxy. `auto_start` handles daemon spawning, stale socket/PID detection, and connection. `run_stdio` convenience for one-line shims.
- **Signal handling** — SIGTERM/SIGINT trigger clean daemon shutdown with socket and PID file cleanup via self-pipe pattern.
- **Transport trait extensions** — `try_clone_writer` and `close_write` for deadlock-free full-duplex bridging (non-breaking, default impls provided).

### Why

MCP clients (e.g. Claude Code) spawn a fresh server process per session. For servers with heavy state — local ML models, agent registries — duplicating that per client is wasteful or broken. The daemon/shim pattern lets multiple clients share one server instance: a long-lived daemon holds the expensive state, lightweight shims proxy MCP traffic.

## v0.4.0

### Changes (feature)

- pagination support (723b63c)
- added tool annotations (6688323)
- local ci checks + fmt (0a10336)

## v0.3.0

### Changes (feature)

- Add Apache 2 licensing as reason for sml_mcps (956a8d4)
- refactor https stuff to reduce boilerplate. (a6e5d53)
- Clippy fixes (ffcb351)
- Readme updates (7b2a365)
- release flow updates (7175849)

## v0.2.0

### Changes

- Initial commit (b8427e9)
- sml_mcps: Sync MCP server with sovran-mcp style API, SSE support, 93% coverage (28e26e8)
- Add MIT license, GitHub Actions CI, and codecov integration (879cc96)
- updated readme. (c280e49)
- Fix: dtolnay/rust-toolchain action name (657cd5b)
- Fix clippy warnings and format, soften codecov failure (a7b59af)
- fixed format (7ac4589)
- Add release workflow and changelog (5121c4e)
- Fix edition and add crates.io metadata (66fde8e)
- Fix edition to 2024, shorten keywords (0b762bb)
- Fix release workflow YAML syntax (432ca75)

# Changelog

All notable changes to this project will be documented in this file.

## v0.1.0

Initial release.

### Features

- Sync MCP server implementation (no tokio/async)
- `Tool<C>` trait generic over context type (sovran-mcp style API)
- `ToolEnv` for notifications, progress reporting, and resource access
- Stdio transport for Claude Desktop
- HTTP transport with automatic SSE/JSON response switching (MCP 2025-03-26 spec)
- JWT authentication support for hosted deployments
- Full MCP protocol support: tools, resources, prompts
- 93% test coverage
