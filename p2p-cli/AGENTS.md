# p2p-cli — Agent Notes

`p2p-cli` is the clap-based command-line front end on top of `p2p-core`. It also routes the no-arg invocation into the GUI when built with the `gui` feature. Workspace-wide guidance lives in the root [AGENTS.md](../AGENTS.md).

## Entry-point flow

The binary crate (`../src/main.rs`) calls `p2p_cli::run_cli_sync()`. The reason this exists as a **sync** function:

1. Parse `Cli` (clap derive).
2. Initialize `tracing` based on `--verbosity`.
3. **Before** creating any Tokio runtime, check if the command is `None` or `Commands::Gui` and the `gui` feature is on → call `p2p_gui::run_gui()` and return. Iced owns its own Tokio runtime; nesting one inside `block_on` panics.
4. Otherwise build `tokio::runtime::Runtime::new()?.block_on(run_cli_async(cli))`.

If you add a new command, add it to the `Commands` enum in `cli.rs` and its match arm in `run_cli_async`. Don't run async work in `run_cli_sync` outside `block_on`.

## File-per-command layout

```
src/
├── lib.rs        # run_cli_sync, run_cli_async, init_logging
├── cli.rs        # clap definitions: Cli, Commands, SessionParams, TransferParams
├── send.rs       # handle_send
├── receive.rs    # handle_receive
├── discover.rs   # handle_discover
├── nat_test.rs   # handle_nat_test
├── resume.rs     # handle_resume
└── history.rs    # handle_history
```

Each command module exposes a single `handle_*` entry point taking the parsed args. Keep CLI translation (prompts, progress bars, formatting) in these files; push protocol/transfer logic into `p2p-core`.

## Shared arg groups

`cli.rs` factors two `#[derive(Args)]` groups that are `#[command(flatten)]`d into multiple subcommands. **Use them — don't duplicate flags per command.**

- `SessionParams` — how the session is established
  - `--role client|server` (Option; defaults differ per command — `send` defaults to client, `receive` defaults to server)
  - `--peer <ip:port>` (only meaningful for direct `client` role)
  - `--peer-fingerprint <64-hex>` (required with `--peer`; pulled from the LAN beacon for `--discover`)
  - `--port <u16>` (default `14567`)
  - `--discover` (use UDP discovery to find the peer, client role only)
  - `--rendezvous <host[:port]>` + `--code <ABC123>` for cross-NAT pairing through `rendezvousd`. When `--rendezvous` is set, `--peer` and `--discover` are ignored.
  - `--force-relay` — skip the punch attempt and head straight for the relay (useful for testing the relay path; normal pairing should leave this off and let symmetric-NAT detection decide).
  - Helpers: `get_role(default)`, `is_client(default)`, `is_server(default)`, `parsed_fingerprint() -> Option<[u8;32]>`.

- `TransferParams` — transfer behavior, independent of who initiates
  - `--compress` (default true), `--compress-level <-7..22>` (default 3), `--adaptive` (default true)
  - `--chunk-size <KB>` (default 64)
  - `--max-speed <0|512K|10M|1G|unlimited>` (parsed by `p2p_core::bandwidth::parse_bandwidth`)

There is no `--window-size` flag — QUIC stream multiplexing replaced the sliding-window protocol in the Phase 0 rewrite. There is no `--max-retries` flag on `TransferParams` either; reconnect tuning lives in `p2p_core::reconnect::ReconnectConfig` and is currently not exposed through the CLI.

When adding a new transfer flag, add it to `TransferParams` so every relevant subcommand picks it up uniformly.

## Naming conventions

- **`--verbosity` is the canonical logging flag**, not `--log-level`. It's a global flag (`global = true`) on the `Cli` struct.
- Roles are the strings `"client"` and `"server"` (validated by clap's `value_parser`).
- Conventional commit prefixes for changes: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `perf:`, `chore:`.

## Logging setup

`init_logging(verbosity)` in `lib.rs`:
- `RUST_LOG` env var takes precedence when set (allows fine-grained module filtering).
- Otherwise builds an `EnvFilter` with directives `p2p_core=<level>` and `p2p_cli=<level>`.
- Subscriber uses compact format with ANSI colors, no module names, level shown.

## Bidirectional sessions

After session establishment, **both peers are equal** (see `p2p_core::session`). `--role` only chooses which side connects vs. listens — it does **not** constrain who sends. The receiver runs an event loop and auto-accepts further transfers on the same session until disconnect; commands that initiate a session must not exit after the first transfer.

## `nat-test` modes

`nat-test` has two distinct behaviors keyed by whether `--rendezvous` is supplied:

- **Default (STUN-only):** queries two STUN servers on a single UDP socket and reports the local NAT type (`Cone` with the public mapping, or `Symmetric`). `--stun-server <host:port>` overrides the default pair.
- **Self-loop punch (`--rendezvous host[:port]`):** spawns two local peers, registers both at the live rendezvous with a fresh code, races a real QUIC handshake between them through the rendezvous (and the relay if either side ends up needing it), and prints `direct` / `relay` / `failed` plus the round-trip latency. This is the end-to-end check that your rendezvous (and optional relay) deployment actually works for real clients.

## Feature flags

```toml
[features]
gui = ["p2p-gui"]   # lets this crate launch the GUI via `Commands::Gui` or no command
```

When `gui` is off and the user runs the binary with no command, `run_cli_sync` prints a help message and exits with code 1 — see the `#[cfg(not(feature = "gui"))]` block.

## Testing & lint

```bash
cargo test -p p2p-cli                                # tests for this crate
cargo test -p p2p-cli <name>                         # single test
cargo clippy -p p2p-cli --all-targets -- -D warnings
```

End-to-end CLI behavior is exercised by the workspace-level `test_transfer.py` and `benchmark.py` (see root [AGENTS.md](../AGENTS.md)).
