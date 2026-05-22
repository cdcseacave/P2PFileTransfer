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
  - `--peer <ip:port>` (only meaningful for `client` role)
  - `--port <u16>` (default `14567`)
  - `--discover` (use UDP discovery to find the peer, client role only)
  - Helpers: `get_role(default)`, `is_client(default)`, `is_server(default)`

- `TransferParams` — transfer behavior, independent of who initiates
  - `--compress` (default true), `--compress-level <-7..22>` (default 3), `--adaptive` (default true)
  - `--chunk-size <KB>` (default 64), `--window-size <N>` (default 16; `1` = sequential)
  - `--max-speed <0|512K|10M|1G|unlimited>` (parsed by `p2p_core::bandwidth::parse_bandwidth`)
  - `--max-retries <N>` (default 5, `0` = unlimited)

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
