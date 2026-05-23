# P2P File Transfer — Agent Notes

## Project Overview

P2P File Transfer is a Rust workspace implementing a peer-to-peer file/folder transfer tool over **QUIC** (TLS 1.3, cert-pinned), with per-chunk unidirectional streams, chunk-level resume, adaptive Zstd compression, file-level SHA256 verification, UDP LAN discovery, STUN-based NAT diagnostic, and bandwidth throttling. It ships both a CLI and an Iced-based GUI from a single binary (`p2p-transfer`).

Running `p2p-transfer` with **no subcommand** launches the GUI when the binary was built with the `gui` feature; otherwise it prints a help message and exits.

## Build & Run

```bash
# Default: CLI only (~3 MB)
cargo build --release

# CLI + GUI (default UI is GUI)
cargo build --release --features full

# GUI only
cargo build --release --features gui --no-default-features

# Run
./target/release/p2p-transfer            # GUI if built with gui, else help
./target/release/p2p-transfer send <path> --peer <ip:port> --peer-fingerprint <hex>
./target/release/p2p-transfer receive --output ./downloads --port 14567 --auto-accept
./target/release/p2p-transfer discover
./target/release/p2p-transfer resume <transfer-id> --to <ip:port> --peer-fingerprint <hex> --path <orig-path>
./target/release/p2p-transfer nat-test
./target/release/p2p-transfer history
```

Feature flags (root `Cargo.toml`):
- `cli` (default) — enables `p2p-cli`
- `gui` — enables `p2p-gui` and turns on `p2p-cli/gui` so the CLI binary can launch the GUI
- `full` — both

Toolchain is pinned via `rust-toolchain.toml` (stable, with `rustfmt` + `clippy`). `rustfmt` uses `max_width = 100`.

## Test & Lint

```bash
cargo test --all                                   # unit + integration + doc tests
cargo test --test integration_test                 # integration tests only (tests/integration_test.rs)
cargo test -p p2p-core <name>                      # single test in a crate
cargo test -- --nocapture                          # show println!/tracing output

cargo clippy --all-targets --all-features -- -D warnings   # zero-warning policy
cargo fmt -- --check                               # formatting check
cargo doc --no-deps                                # build docs

# End-to-end Python harness (cross-platform):
python3 test_transfer.py --size 50                 # incompressible payload
python3 test_transfer.py --size 50 --compressible  # compressible payload
# IMPORTANT: delete ./test_file between runs when changing --size or --compressible
python3 benchmark.py --mode sender                 # localhost benchmark (auto-starts receiver)
```

## Workspace Layout

Cargo workspace with three member crates plus a thin binary:

```
.                        workspace root — binary crate `p2p-transfer` (src/main.rs delegates to p2p-cli or p2p-gui)
p2p-core/                core library: protocol, transfer engine, networking, session, identity, history
p2p-cli/                 clap-based CLI (also launches the GUI when --features gui is enabled)
p2p-gui/                 Iced 0.12 GUI (tabs: Connection, Send, Receive, Settings, History, Console)
tests/integration_test.rs   workspace-level QUIC handshake smoke test
```

`src/main.rs` dispatches by feature: `cli` -> `p2p_cli::run_cli_sync()` (which itself routes the no-arg case to `p2p_gui::run_gui` when the `gui` feature is on); `gui` without `cli` -> direct `run_gui()`. **The GUI is started outside the async runtime** because Iced owns its own Tokio runtime — re-entering Tokio would panic. The CLI builds a `tokio::runtime::Runtime` and calls `block_on` for the async subcommands.

## Architecture (the parts you can't infer from one file)

### Layered design in `p2p-core`

1. **Identity & TLS** — `identity.rs` (Ed25519 keypair + self-signed cert via `rcgen`, persisted to `<config_dir>/p2p-transfer/identity.{key,cert}`), `tls.rs` (rustls 0.23 `ServerConfig`/`ClientConfig` + `FingerprintVerifier`), `known_peers.rs` (TOFU fingerprint store).
2. **Transport** — `network/quic.rs` is the **only** transport: `QuicEndpoint` wraps `quinn::Endpoint` (one UDP socket per endpoint, acts as both client and server), `QuicConnection` holds the `quinn::Connection` + the bidi control stream. `network/framing.rs` is MessagePack length-prefixed framing with the `P2PF` magic, used over the QUIC control stream. `network/udp.rs` is the UDP LAN beacon (port 14566).
3. **Handshake** — `handshake.rs` (`HandshakeClient`/`HandshakeServer`) over the bidi control stream: HELLO/HELLO_ACK with cert-fingerprint cross-check, then CONFIG/CONFIG_ACK. Produces `HandshakeResult { peer_device_id, peer_fingerprint, agreed_capabilities, config }`.
4. **Session** — `session.rs` (`P2PSession`). **After the handshake the connection is fully symmetric and bidirectional.** The `ConnectionRole` (`Initiator`/`Responder`) is retained only for `reconnect` (only the initiator knows where to reconnect to). Either side may call `send_path()` or `receive_to()` repeatedly on the same connection.
5. **Transfer engine** — `transfer_file.rs` (`FileTransferSession`, single file — opens one unidirectional QUIC stream per chunk with `[u64 LE index | u8 flags | payload]`) and `transfer_folder.rs` (`FolderTransferSession`, walks a directory tree and runs one `FileTransferSession` per file, aggregating `TransferStats`).
6. **Cross-cutting**: `compression.rs` (adaptive Zstd — samples first 3 chunks, disables if ratio < 1.05x), `verification.rs` (file-level SHA256 only — per-chunk CRC is gone, TLS AEAD authenticates every byte), `bandwidth.rs` (token bucket, parses `K`/`M`/`G` suffixes), `reconnect.rs` (exponential backoff retry loop), `state.rs` (chunk bitmap persisted as `transfer_<uuid>.json` for resume), `history.rs` (transfer log in a user data dir), `discovery.rs` + UDP beacons on port `14566`, `traversal/stun.rs` (async STUN on a borrowed `tokio::net::UdpSocket` — same socket type quinn owns), `progress.rs` (shared `ProgressState`).

Default ports and constants live in `p2p-core/src/lib.rs`: `DEFAULT_DISCOVERY_PORT = 14566`, `DEFAULT_TRANSFER_PORT = 14567`, `DEFAULT_RENDEZVOUS_PORT = 14570`, `DEFAULT_CHUNK_SIZE = 65536`, `PROTOCOL_VERSION = 2`, `PROTOCOL_MAGIC = b"P2PF"`, `ALPN_PROTOCOL = b"p2pf/2"`.

### CLI structure (`p2p-cli`)

Subcommands live in their own files (`send.rs`, `receive.rs`, `discover.rs`, `nat_test.rs`, `resume.rs`, `history.rs`). `cli.rs` factors **two shared `Args` groups** that are `#[command(flatten)]`d into multiple subcommands:
- `SessionParams` — `--role`, `--peer`, `--peer-fingerprint`, `--port`, `--discover` (governs how the QUIC session is established; `--peer-fingerprint` is required for `--peer` mode and pulled from the beacon for `--discover`)
- `TransferParams` — `--compress`, `--compress-level`, `--adaptive`, `--chunk-size`, `--max-speed`

When adding a new transfer-related flag, add it to `TransferParams` so every command picks it up consistently; don't duplicate it per subcommand. `--verbosity` is a global flag and the canonical name — do **not** rename it to `--log-level`.

`run_cli_sync` intercepts the `None`/`Gui` command **before** entering the async runtime (Iced runs blocking with its own runtime).

### GUI structure (`p2p-gui`)

Standard Iced 0.12 Elm-architecture split:
- `app.rs` — `Application` impl, tabs row + active view + console at bottom
- `state.rs` — `AppState`, per-tab state structs, `Tab` enum, `ConsoleIcon`
- `message.rs` — all `Message` variants
- `operations.rs` — `handle_message(state, msg) -> Command<Message>`; this is where async operations are spawned (file dialogs via `rfd`, transfer sessions wrapped in `Arc<Mutex<P2PSession>>`)
- `views/` — one file per tab plus `console.rs`
- `styles.rs`, `utils.rs` — theme and formatting helpers

The GUI holds the active `P2PSession` in shared state so transfer tabs can drive sends/receives against the same connection.

## Conventions

- **Logging**: use `tracing` macros (`error!`, `warn!`, `info!`, `debug!`, `trace!`). The CLI `--verbosity` flag maps to `EnvFilter` directives on `p2p_core` and `p2p_cli` targets; `RUST_LOG` overrides it.
- **Errors**: `p2p-core` returns its own `Error`/`Result` from `error.rs`; CLI layer uses `anyhow::Context` to add user-facing context. Don't `panic!` in library code.
- **Async**: all I/O is `tokio` async. Don't block the runtime; use `tokio::select!` for timeouts/cancellation.
- **Hot path**: the per-chunk loop in `transfer_file.rs` — avoid per-chunk allocations, prefer buffer reuse and references over cloning.
- **Documentation policy** (from `.github/copilot-instructions.md`): keep all docs in the four canonical files — `README.md`, `DESIGN.md`, `TODO.md`, `CHANGELOG.md`. Do **not** create per-feature markdown files. When a feature ships: remove its entry from `TODO.md`, document usage in `README.md`, document architecture in `DESIGN.md`, add a dated `CHANGELOG.md` entry.
- **Branches**: `main` stable, `develop` integration (default), `feature/*`, `bugfix/*`, `hotfix/*`. Conventional commit prefixes (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `perf:`, `chore:`).

## Gotchas

- **Don't nest Tokio runtimes.** Anything that calls `Iced::run` must be reached *outside* `block_on`; that's why `run_cli_sync` returns early for the GUI cases.
- **The QUIC bidi control stream only materialises on the responder once the initiator writes to it.** Real handshake code does this immediately; tests that don't exchange messages must either send a marker first or use the same `oneshot` "hold the connection" pattern the existing tests use.
- **Adaptive compression accounting**: track uncompressed size from `chunk_data.len()` *before* compression, not from the compressed payload, otherwise stats and SHA256 boundaries break.
- **Resume state files** are written as `transfer_<uuid>.json` in the working directory at the time of the transfer. Resume requires the original `--path`, `--to`, and `--peer-fingerprint` because the file doesn't store any of them.
- **Receiver event loop**: the receiver stays alive after a transfer finishes and accepts further transfers on the same connection until the peer disconnects — don't add logic that exits after the first transfer.
- **Both peers behind NAT** is not yet automated. `nat-test` reports the public endpoint and classifies the NAT (Cone vs Symmetric) via STUN; rendezvous-mediated hole punching is on the roadmap (see `TODO.md`).
