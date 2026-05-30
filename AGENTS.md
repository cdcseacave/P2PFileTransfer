# P2P File Transfer — Agent Notes

## Project Overview

P2P File Transfer is a Rust workspace implementing a peer-to-peer file/folder transfer tool over **QUIC** (TLS 1.3 with **mutual auth** — both peers present certs pinned by SHA-256 fingerprint), with per-chunk unidirectional streams, chunk-level resume, adaptive Zstd compression, file-level SHA-256 verification, UDP LAN discovery, **rendezvous-mediated UDP hole punching** (`p2p-rendezvous` crate + `rendezvousd` binary), **QUIC relay fallback** for symmetric NAT, STUN-based NAT classification, and bandwidth throttling. It ships both a CLI and an Iced-based GUI from a single binary (`p2p-transfer`), with a separate `rendezvousd` binary for self-hosted matchmaking.

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
./target/release/p2p-transfer send <path> --rendezvous host:14570 --code ABC123
./target/release/p2p-transfer send <path> --rendezvous host:14570 --code ABC123 --force-relay
./target/release/p2p-transfer receive --output ./downloads --port 14567 --auto-accept
./target/release/p2p-transfer receive --output ./downloads --rendezvous host:14570 --code ABC123
./target/release/p2p-transfer discover
./target/release/p2p-transfer resume <transfer-id> --path <orig-path> --peer <ip:port> --peer-fingerprint <hex>
./target/release/p2p-transfer resume <transfer-id> --path <orig-path> --rendezvous host:14570 --code ABC123
./target/release/p2p-transfer nat-test
./target/release/p2p-transfer nat-test --rendezvous host:14570        # self-loop punch test
./target/release/p2p-transfer history

# Rendezvous server (separate binary from p2p-rendezvous crate)
./target/release/rendezvousd --bind 0.0.0.0:14570
./target/release/rendezvousd --bind 0.0.0.0:14570 --relay-bind 0.0.0.0:14571 --max-relay-mbps 50
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

Cargo workspace with four member crates plus a thin binary:

```
.                                 workspace root — binary crate `p2p-transfer` (src/main.rs delegates to p2p-cli or p2p-gui)
p2p-core/                         core library: protocol, transfer engine, transport, session, identity, traversal
p2p-cli/                          clap-based CLI (also launches the GUI when --features gui is enabled)
p2p-gui/                          Iced 0.12 GUI (tabs: Connection, Send, Receive, Settings, History; bottom console)
p2p-rendezvous/                   pairing-by-code rendezvous server + relay; provides the `rendezvousd` binary
tests/integration_test.rs                       workspace-level QUIC handshake smoke test
tests/traversal_loopback_test.rs                rendezvous + race-connect-and-accept punch
tests/relay_loopback_test.rs                    rendezvous + UDP relay + QUIC-over-relay end-to-end
tests/rendezvous_disconnect_resume_test.rs      rendezvous re-pair after sender disconnect + resume-over-rendezvous end-to-end
scripts/deploy.py                               idempotent installer for `rendezvousd` on Ubuntu 24+ (install / uninstall / clean-build)
```

`src/main.rs` dispatches by feature: `cli` -> `p2p_cli::run_cli_sync()` (which itself routes the no-arg case to `p2p_gui::run_gui` when the `gui` feature is on); `gui` without `cli` -> direct `run_gui()`. **The GUI is started outside the async runtime** because Iced owns its own Tokio runtime — re-entering Tokio would panic. The CLI builds a `tokio::runtime::Runtime` and calls `block_on` for the async subcommands.

## Architecture (the parts you can't infer from one file)

### Layered design in `p2p-core`

1. **Identity & TLS** — `identity.rs` (Ed25519 keypair + self-signed cert via `rcgen`, persisted to `<config_dir>/p2p-transfer/identity.{key,cert}`). `tls.rs` builds rustls 0.23 configs for **mutual TLS**: server uses `with_client_cert_verifier(AcceptAnyClientCert)` so the client cert is required but its identity is checked at the handshake layer; client uses `with_client_auth_cert(...)` to present its own cert and `FingerprintVerifier` to pin the server cert. `known_peers.rs` is the TOFU fingerprint store.
2. **Transport** — `network/quic.rs` is the **only** transport: `QuicEndpoint` wraps `quinn::Endpoint` (one UDP socket per endpoint, acts as both client and server), `QuicConnection` holds the `quinn::Connection` + the bidi control stream and exposes `peer_fingerprint()` (now `Some` on **both** sides thanks to mTLS). `network/framing.rs` is MessagePack length-prefixed framing with the `P2PF` magic; clean EOF on the first read maps to `Error::Disconnected`, truncation inside a frame to `Error::Protocol`. `network/udp.rs` is the UDP LAN beacon (port 14566).
3. **Handshake** — `handshake.rs` (`HandshakeClient`/`HandshakeServer`) over the bidi control stream: HELLO/HELLO_ACK with cert-fingerprint cross-check (mismatch *or* missing observation = fatal `Error::FingerprintMismatch`), then CONFIG/CONFIG_ACK. Produces `HandshakeResult { peer_device_id, peer_fingerprint, agreed_capabilities, config }`.
4. **Session** — `session.rs` (`P2PSession`). **After the handshake the connection is fully symmetric and bidirectional.** The `ConnectionRole` (`Initiator`/`Responder`) is retained only for `reconnect` (only the initiator knows where to reconnect to). Either side may call `send_path()` or `receive_to()` repeatedly on the same connection. `P2PSession::from_rendezvous` is the cross-NAT entry point.
5. **Transfer engine** — `transfer_file.rs` (`FileTransferSession`, single file — opens one unidirectional QUIC stream per chunk with `[u64 LE index | u8 flags | payload]`; `send_chunk_stream` awaits `stream.stopped()` so the last chunk isn't lost on close; the receiver bounds-checks `chunk_index < total_chunks`) and `transfer_folder.rs` (`FolderTransferSession`, walks a directory tree, runs one `FileTransferSession` per file, aggregates `TransferStats`, and routes every wire-supplied path through `sanitize_relative_path` — rejects absolute paths, `..`, `.`, drive/root components).
6. **NAT traversal** — `traversal/stun.rs` (async STUN on a borrowed `tokio::net::UdpSocket`, validates response transaction id matches the request), `traversal/punch.rs` (`race_connect_and_accept`: runs `connect` and an address-validating `accept_from` in parallel; the larger-device-id peer staggers its `connect` by 50 ms to avoid Initial-packet collisions), `traversal/mod.rs` orchestrator (`establish_via_rendezvous`: bind socket → STUN classify → register → punch or join relay).
7. **Cross-cutting**: `compression.rs` (adaptive Zstd — samples first 3 chunks, disables if ratio < 1.05x), `verification.rs` (file-level SHA-256 — sender checks pre-send, receiver mismatch is a hard `Error::Verification`), `bandwidth.rs` (token bucket, parses `K`/`M`/`G` suffixes), `reconnect.rs` (exponential backoff retry loop), `state.rs` (chunk bitmap persisted as `transfer_<uuid>.json` for resume), `history.rs` (transfer log in a user data dir), `discovery.rs` + UDP beacons on port `14566`, `progress.rs` (shared `ProgressState`).

Default ports and constants live in `p2p-core/src/lib.rs`: `DEFAULT_DISCOVERY_PORT = 14566`, `DEFAULT_TRANSFER_PORT = 14567`, `DEFAULT_RENDEZVOUS_PORT = 14570`, `DEFAULT_CHUNK_SIZE = 1 MiB`, `PROTOCOL_VERSION = 2`, `PROTOCOL_MAGIC = b"P2PF"`, `ALPN_PROTOCOL = b"p2pf/2"`. Single source of truth — `ConfigMessage::default`, `TransferConfig::default`, and the GUI's `AppSettings::default` all derive from `DEFAULT_CHUNK_SIZE`; do not hardcode 65536 or 64 KB anywhere. Chunk indices on the wire and in memory are `u64` end-to-end — there is no `u32` narrowing anywhere on the chunk path, so files larger than `2^32` chunks transfer correctly.

### `p2p-rendezvous` crate

Standalone matchmaking + relay. See `p2p-rendezvous/AGENTS.md` for the crate-specific notes. Quick summary:
- `server.rs` — TCP listener, MessagePack frames, pairs peers by short code. **Concurrency cap** via `tokio::sync::Semaphore` (`Server::bind_with`, default 1024) applies backpressure on the listener. Each registration's IP is rewritten to the TCP peer's IP (the user-supplied UDP port is kept) so a peer can't aim the punch at a third-party victim.
- `relay.rs` — UDP packet forwarder. Slot binding is fingerprint-keyed lookup; `reserve_session` refuses identical fingerprints on both slots. Idle eviction runs in a 30 s background task (off the per-packet hot path). Recv buffer up to 65 KiB; warns on full-buffer reads.
- `protocol.rs` — `Message::{Register, Match, RelayMatch, Expired, Rejected}`, `RegisterRequest` with `want_relay` bit.
- `bin/rendezvousd.rs` — the `rendezvousd` binary.

### CLI structure (`p2p-cli`)

Subcommands live in their own files (`send.rs`, `receive.rs`, `discover.rs`, `nat_test.rs`, `resume.rs`, `history.rs`). `cli.rs` factors **two shared `Args` groups** that are `#[command(flatten)]`d into multiple subcommands:
- `SessionParams` — `--role`, `--peer`, `--peer-fingerprint`, `--port`, `--discover`, `--rendezvous <host:port>`, `--code <ABC123>`, `--force-relay`. When `--rendezvous` is set, `--peer` and `--discover` are ignored and pairing goes through the rendezvous server.
- `TransferParams` — `--compress`, `--compress-level`, `--adaptive`, `--chunk-size`, `--max-speed`.

When adding a new transfer-related flag, add it to `TransferParams` so every command picks it up consistently; don't duplicate it per subcommand. `--verbosity` is a global flag and the canonical name — do **not** rename it to `--log-level`.

`nat-test` has two modes: STUN-only classification (default — reports `Cone` vs `Symmetric`), and self-loop punch (`--rendezvous host:port` — spawns two local peers, registers both with a fresh code, races a real QUIC handshake, reports `direct` / `relay` / `failed` with latency).

`run_cli_sync` intercepts the `None`/`Gui` command **before** entering the async runtime (Iced runs blocking with its own runtime).

### GUI structure (`p2p-gui`)

Standard Iced 0.12 Elm-architecture split:
- `app.rs` — `Application` impl, tabs row + active view + console at bottom
- `state.rs` — `AppState`, per-tab state structs, `Tab` enum, `ConsoleIcon`
- `message.rs` — all `Message` variants
- `operations.rs` — `handle_message(state, msg) -> Command<Message>`; this is where async operations are spawned (file dialogs via `rfd`, transfer sessions wrapped in `Arc<tokio::Mutex<P2PSession>>`)
- `views/` — one file per tab plus `console.rs`
- `styles.rs`, `utils.rs` — theme and formatting helpers

The GUI holds the active `P2PSession` in shared state so transfer tabs can drive sends/receives against the same connection. The Connection tab has three modes — `Listen`, `Connect` (with `--peer-fingerprint`), and **`Pair with code (cross-NAT)`** (rendezvous + shared code with a Generate button). Session establishment runs **inside `Command::perform`** (off the iced thread) and only the resulting `P2PSession` is wrapped in `Arc<tokio::Mutex<...>>` via `ConnectionEstablishedWithSession` — so the UI stays responsive during multi-second rendezvous waits.

## Conventions

- **Logging**: use `tracing` macros (`error!`, `warn!`, `info!`, `debug!`, `trace!`). The CLI `--verbosity` flag maps to `EnvFilter` directives on `p2p_core` and `p2p_cli` targets; `RUST_LOG` overrides it.
- **Errors**: `p2p-core` returns its own `Error`/`Result` from `error.rs`; CLI layer uses `anyhow::Context` to add user-facing context. Don't `panic!` in library code.
- **Async**: all I/O is `tokio` async. Don't block the runtime; use `tokio::select!` for timeouts/cancellation.
- **Hot path**: the per-chunk loop in `transfer_file.rs` — avoid per-chunk allocations, prefer buffer reuse and references over cloning.
- **Documentation policy**: keep all docs in the four canonical files — `README.md`, `DESIGN.md`, `TODO.md`, `CHANGELOG.md`. Do **not** create per-feature markdown files (e.g. `FEATURE_NAME.md`, `IMPLEMENTATION_SUMMARY.md`, `QUICK_REFERENCE.md`). When a feature ships: remove its entry from `TODO.md`, document usage in `README.md`, document architecture in `DESIGN.md`, add a dated `CHANGELOG.md` entry. Rationale: keep documentation centralized so it doesn't fragment.
- **Module docs**: every module needs a `//!` header; every public item needs `///` docstrings.
- **Branches**: `main` stable, `develop` integration (default), `feature/*`, `bugfix/*`, `hotfix/*`. Conventional commit prefixes (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `perf:`, `chore:`).

## Workflow

### Before committing

Run the full pipeline locally — every step must be green:

```bash
cargo build --release                                     # compiles cleanly
cargo test --all                                          # unit + integration + doc
cargo clippy --all-targets --all-features -- -D warnings  # zero-warning policy
cargo fmt -- --check                                      # rustfmt clean
cargo doc --no-deps                                       # docs build without warnings
```

The end-to-end Python harness is the last gate when you've touched the wire protocol or transfer engine:

```bash
rm -f test_file
python3 test_transfer.py --size 50                 # incompressible
python3 test_transfer.py --size 50 --compressible  # ratio > 100×
```

### When refactoring

1. Run the full pipeline above.
2. Never remove a field or method without grepping every caller first.
3. Per the "no compat shim" rule, when a wire format changes, bump `PROTOCOL_VERSION` and update the call sites in place — don't leave deprecated paths.

### When adding a feature

1. Add unit tests in the module's `#[cfg(test)] mod tests`.
2. Follow the existing async/error/callback patterns.
3. Update `TODO.md` (remove the entry when fully shipped), `README.md` (usage), `DESIGN.md` (architecture), and `CHANGELOG.md` (dated entry).

### When fixing a bug

1. Write a failing test that reproduces the bug.
2. Fix it; the test goes green.
3. Run the full pipeline.
4. Add a dated `CHANGELOG.md` entry referencing the finding/symptom.

## Gotchas

- **Don't nest Tokio runtimes.** Anything that calls `Iced::run` must be reached *outside* `block_on`; that's why `run_cli_sync` returns early for the GUI cases.
- **The QUIC bidi control stream only materialises on the responder once the initiator writes to it.** Real handshake code does this immediately; tests that don't exchange messages must either send a marker first or use the same `oneshot` "hold the connection" pattern the existing tests use.
- **Adaptive compression accounting**: track uncompressed size from `chunk_data.len()` *before* compression, not from the compressed payload, otherwise stats and SHA-256 boundaries break.
- **Resume state files** are written as `transfer_<uuid>.json` in the working directory at the time of the transfer. Resume requires the original `--path`, `--peer`, and `--peer-fingerprint` because the file doesn't store any of them.
- **Receiver event loop**: the receiver stays alive after a transfer finishes and accepts further transfers on the same connection until the peer disconnects — don't add logic that exits after the first transfer.
- **Chunk indices are `u64` end-to-end**. `ChunkReader::total_chunks`, `read_chunk`, `fold_chunk`, `ChunkWriter::write_chunk` and the wire format all use `u64`. Do not narrow back to `u32` anywhere on the chunk path — that's what previously truncated large files at `2^32` chunks.
- **Sanitize before joining paths.** Anything written under the output directory goes through `transfer_folder::sanitize_relative_path` first — adding a new write site means routing it through the same sanitizer.
- **Mutual TLS, no compat shim.** Both sides present certs now; `cross_check_fingerprint` rejects a `None` observation, so any new transport layer that bypasses the standard `tls::server_config` / `client_config_pinning` builders has to keep client-cert presentation intact.
- **Source-address validation on accept.** `traversal::punch::accept_from` drops connections whose remote address doesn't match the rendezvous-supplied peer. If you add a new entry point that does its own `endpoint.accept()` outside a controlled test, wrap it the same way.
- **`PUBLIC_ENDPOINT` is server-rewritten.** A peer's `RegisterRequest.public_endpoint` IP is *replaced* by the TCP source IP at the rendezvous. The port is kept (because the UDP punch socket is a different transport from the TCP control channel) but the IP is forgeable for traffic reflection and the TCP source is the source of truth.
- **No backwards compatibility.** Per the project's "no compat on redesigns" rule, wire formats and call sites change in place; do not add shims or deprecated paths for the QUIC/rendezvous/relay flows.
