# p2p-core — Agent Notes

`p2p-core` is the protocol + transfer-engine library. No CLI parsing, no UI — everything user-facing lives in `p2p-cli` or `p2p-gui`. Public surface is re-exported through `lib.rs`.

Workspace-wide guidance lives in the root [AGENTS.md](../AGENTS.md); this file covers what you need to know to work productively *inside* this crate.

## Module map

The crate is layered. Higher layers depend on lower layers, not the other way around:

| Layer | Modules | Role |
|---|---|---|
| Constants | `lib.rs` | `PROTOCOL_VERSION = 2`, `DEFAULT_CHUNK_SIZE = 1 MiB`, `DEFAULT_DISCOVERY_PORT = 14566`, `DEFAULT_TRANSFER_PORT = 14567`, `DEFAULT_RENDEZVOUS_PORT = 14570`, `PROTOCOL_MAGIC = b"P2PF"`, `ALPN_PROTOCOL = b"p2pf/2"`. Single source of truth — `ConfigMessage::default`, `TransferConfig::default`, and the GUI's `AppSettings::default` derive from it. |
| Errors | `error.rs` | `Error`/`Result` — every fallible API in this crate returns these (`Quic`, `Tls`, `Rendezvous`, `HolePunchFailed`, `FingerprintMismatch`, `Verification`, `Disconnected`, ...) |
| Identity & TLS | `identity.rs`, `tls.rs`, `known_peers.rs` | `Identity` = persistent Ed25519 keypair + self-signed cert (rcgen). `tls::server_config` requires a client cert via `AcceptAnyClientCert` (mutual TLS — peer identity is pinned at the handshake layer, TLS just guarantees the cert is presented). `tls::client_config_pinning` presents our cert and pins the server cert via `FingerprintVerifier`. `KnownPeers` = TOFU store at `<config_dir>/p2p-transfer/known_peers.json`. |
| Protocol | `protocol.rs`, `config.rs` | `Message` enum (control-plane only — chunks ride raw on per-chunk uni streams), `HelloMessage`, `ConfigMessage`, `TransferInfo`, `FileMetadata`, `Capabilities` |
| Transport | `network/quic.rs`, `network/framing.rs`, `network/udp.rs` | `QuicEndpoint` + `QuicConnection` (the only transport — one UDP socket per endpoint, acts as both client and server; `peer_fingerprint()` is `Some` on both sides thanks to mTLS); `framing::read_message` maps clean EOF on the magic read to `Error::Disconnected`, frame-interior short reads to `Error::Protocol`; UDP helpers for LAN beacons |
| Crypto/check | `verification.rs`, `compression.rs` | File-level SHA-256 only (per-chunk CRC is gone — TLS AEAD authenticates every byte); receiver mismatch is a hard `Error::Verification` — never a warn. `AdaptiveCompressor` (Zstd levels -7..22, auto-disables under 1.05× ratio after sampling 3 chunks). |
| Throttle | `bandwidth.rs` | Token-bucket with `K`/`M`/`G` suffix parser; applied before each `open_uni().write` |
| Discovery / NAT | `discovery.rs`, `traversal/stun.rs`, `traversal/punch.rs`, `traversal/mod.rs` | UDP beacon-based `DiscoveryManager` carrying `cert_fingerprint`. `traversal::stun` runs async STUN on a borrowed `tokio::net::UdpSocket` and validates the response transaction id; `classify_nat` reports Cone vs Symmetric. `traversal::punch::race_connect_and_accept` runs `connect` and an address-validating `accept_from` in parallel (50 ms stagger by larger device id) — first success wins; mismatched source addresses are dropped and the accept loop continues. `traversal::mod` orchestrates STUN → register at rendezvous → punch-or-relay. |
| Handshake | `handshake.rs` | `HandshakeClient`/`HandshakeServer` over the bidi control stream — HELLO/HELLO_ACK with `cross_check_fingerprint` (fatal on mismatch *and* on missing observation, closing the responder TOFU bypass) + CONFIG/CONFIG_ACK, produces `HandshakeResult { peer_device_id, peer_fingerprint, agreed_capabilities, config }` |
| Transfer engine | `transfer_file.rs`, `transfer_folder.rs` | `FileTransferSession`: one unidirectional QUIC stream per chunk with `[u64 LE index \| u8 flags \| payload]`; `send_chunk_stream` awaits `stream.stopped()` so the last chunk isn't lost on close. Receiver bounds-checks `chunk_index < total_chunks` before writing. Chunk indices are `u64` end-to-end (`ChunkReader::total_chunks`, `read_chunk`, `fold_chunk`, `ChunkWriter::write_chunk`). `FolderTransferSession` walks the tree, runs per-file sessions, aggregates `TransferStats`, and exposes `sanitize_relative_path` (rejects absolute, `..`, `.`, drive/root, empty) which is applied to both incoming `FileMetadata.path` *and* outgoing `scan_folder` paths. |
| Session | `session.rs` | `P2PSession` — bidirectional, symmetric facade combining QUIC endpoint + handshake + transfer. `connect`, `accept`, and `from_rendezvous` are the three entry points. The event loop ends on `Error::Disconnected` / `Error::Quic` / `Error::Network` (no string matching). |
| Cross-cutting | `state.rs`, `history.rs`, `progress.rs`, `reconnect.rs` | Resume-state JSON (chunk bitmap); transfer-history log; shared `ProgressState` consumed by CLI bars and GUI updates; exponential-backoff reconnect loop |

## Design points you can't see from one file

### `P2PSession` is symmetric

After `connect()`/`accept()` complete, the connection is fully bidirectional. `ConnectionRole::{Initiator, Responder}` is retained only so the initiator side knows where to reconnect to — every operation (`send_path`, `receive_to`, multiple in sequence, interleaved) works from either side. Don't reintroduce client/server asymmetry into the session layer; the asymmetry is confined to establishment.

### One UDP socket, one transport

`QuicEndpoint::bind` (or `::from_socket`) takes ownership of a UDP socket and uses it for **both** outbound `connect` and inbound `accept`. The bidi control stream and per-chunk uni streams all multiplex over this single socket. This is also the socket that STUN + (future) hole-punching will run on; the order is: bind socket → STUN on the socket → hand it to `QuicEndpoint::from_socket`.

### Chunks bypass `Message`

`Message` is the control plane only. Chunk data rides raw on per-chunk unidirectional QUIC streams with the wire layout `[u64 LE chunk_index | u8 flags | payload bytes (zstd if flags&1)]`. There is no per-chunk ACK / retry / CRC — QUIC's per-stream flow control and packet retransmission cover loss recovery, and TLS 1.3 AEAD authenticates every byte. A finalized `SendStream` is end-to-end acked by QUIC itself.

### Transfer engine composition

`FolderTransferSession` does **not** reimplement chunk logic — it walks the directory tree and runs a `FileTransferSession` per file, reusing the same `QuicConnection`, then aggregates results. When adding folder-level behavior, decide whether it belongs:
- per-file (compression, file-level SHA256, per-chunk stream wire format) → `transfer_file.rs`
- per-folder (file enumeration, structure preservation, aggregate stats, state saves between files) → `transfer_folder.rs`

State is persisted **after each file completes** (not mid-file), so resume granularity is "skip completed files, start partial files from their last completed chunk." The chunk-level resume within a file is handled by `FileTransferSession` checking the chunk bitmap and only opening uni streams for missing indices.

### Identity persistence

`Identity::load_or_generate(dir: Option<&Path>)` reads PEM-encoded PKCS#8 key + PEM cert from `<dir>/identity.{key,cert}` (or `<config_dir>/p2p-transfer/identity.{key,cert}` when `dir` is `None`); created on first run with mode 0600 on Unix. The SHA-256 of the cert DER is the stable per-device fingerprint and is what peers pin. The cert is persisted alongside the key so the fingerprint stays stable across restarts — TOFU pinning in `known_peers.json` depends on it. The CLI exposes the override as `--identity-dir <PATH>`; the GUI always passes `None`.

### Mutual TLS, but pinning lives at the handshake layer

Both the server and the client now present certs (rustls's `with_client_cert_verifier(AcceptAnyClientCert)` on the server, `with_client_auth_cert(...)` on the client). `AcceptAnyClientCert` doesn't validate the client cert against any CA — it just lets the cert through so `QuicConnection::peer_fingerprint()` returns `Some(...)` on both sides. The actual identity check lives in `handshake.rs::cross_check_fingerprint`, which compares the cert TLS observed against the value HELLO claimed and fails if they disagree *or* if the observation is `None`. Don't reintroduce `with_no_client_auth()` on the server — that's the responder TOFU bypass the audit closed.

### Path sanitization

Any path that came in over the wire goes through `transfer_folder::sanitize_relative_path` before being joined under the output directory. It rejects absolute paths, `..` and `.` components, Windows drive prefixes, UNC roots, and empty paths. The sender also runs it on `scan_folder` output so weird local names fail fast instead of silently producing a wire payload the receiver will reject. When adding any new code path that writes to a receiver-controlled location, route the relative path through this function first.

### NAT traversal correctness

`traversal::punch::race_connect_and_accept` launches `connect` *and* `accept_from` in parallel on both peers — both `connect`s send their outbound QUIC `Initial` packets which open the NAT mappings on both sides. The smaller `device_id` peer fires its `connect` immediately; the larger one delays by `SECONDARY_CONNECT_DELAY` (50 ms) so the two flights don't collide in a way some NATs treat as garbage. `accept_from` loops on `endpoint.accept()` until the remote source matches the rendezvous-supplied peer address — that drops third-party connections that ride our open mapping.

`stun::query` validates the response transaction id (`data[8..20]` ≡ request tx) so a spoofed STUN-shaped packet from another source can't bind a fake mapping.

### Adaptive compression accounting

`AdaptiveCompressor` decides after the first 3 chunks whether to keep compressing. **Track uncompressed length from `chunk_data.len()` before compression** — using the compressed payload length to advance file offsets or update SHA256 will silently corrupt resume state and verification. This has caused incidents before; the comment in `compression.rs` exists for a reason.

### Protocol versioning

`PROTOCOL_VERSION = 2`, `MIN_PROTOCOL_VERSION = 2` (in `lib.rs`). Equality check only — no v1 compat code. v1 used TCP and a different protocol; v1 peers can't even reach a v2 endpoint (which is UDP/QUIC), so the failure is clean. Bump both constants together for any future hard break.

## Tests

```bash
# All tests in this crate
cargo test -p p2p-core

# Single test by name (substring match)
cargo test -p p2p-core <name>

# Single module
cargo test -p p2p-core compression::

# With logs
cargo test -p p2p-core -- --nocapture

# Doc tests
cargo test -p p2p-core --doc
```

Unit tests are `#[cfg(test)] mod tests { ... }` inline in each module. Cross-module workflow tests (full QUIC handshake end-to-end) live in the workspace `tests/integration_test.rs`, not in this crate.

`dev-dependencies` available here: `tokio-test`, `tempfile`.

### Test gotcha — keep the connection alive

The QUIC bidi control stream is only materialised on the responder when the initiator writes to it. Tests that exchange handshake messages naturally satisfy this; tests that *don't* (e.g. the artificial uni-stream test in `network/quic.rs`) must send a marker first. Likewise, when a server task finishes a handshake and immediately drops its `QuicConnection`, the connection close races the client's last `recv_message` — use the `oneshot` "hold the connection until the client signals done" pattern from `handshake::tests::handshake_round_trip_over_quic` for any new test that exchanges messages.

## Conventions specific to this crate

- **No CLI/UI concerns.** No `clap`, no `indicatif`, no `iced`. Progress is surfaced via `progress::ProgressState` callbacks; UI layers translate them.
- **All I/O is async (`tokio`).** Never block; use `tokio::select!` for timeouts/cancellation.
- **Hot path** = the per-chunk loop in `transfer_file.rs`. Avoid per-chunk allocations; reuse buffers; prefer `&[u8]` over `Vec<u8>` where possible.
- **Logging via `tracing`.** Targets default to `p2p_core`; the CLI's `EnvFilter` keys off this prefix.
- **Errors**: return `crate::Result<T>` (= `Result<T, crate::Error>`); don't sprinkle `anyhow` here — that's the user-facing layer's job.
- **Public items are documented** with `///`; modules have `//!` headers.
