# p2p-core — Agent Notes

`p2p-core` is the protocol + transfer-engine library. No CLI parsing, no UI — everything user-facing lives in `p2p-cli` or `p2p-gui`. Public surface is re-exported through `lib.rs`.

Workspace-wide guidance lives in the root [AGENTS.md](../AGENTS.md); this file covers what you need to know to work productively *inside* this crate.

## Module map

The crate is layered. Higher layers depend on lower layers, not the other way around:

| Layer | Modules | Role |
|---|---|---|
| Constants | `lib.rs` | `PROTOCOL_VERSION`, `DEFAULT_CHUNK_SIZE = 65536`, `DEFAULT_DISCOVERY_PORT = 14566`, `DEFAULT_TRANSFER_PORT = 14567`, `PROTOCOL_MAGIC = b"P2PF"` |
| Errors | `error.rs` | `Error`/`Result` — every fallible API in this crate returns these |
| Protocol | `protocol.rs`, `config.rs` | `Message` enum, `HandshakeMessage`, `ChunkMessage`, `ChunkAck`, `CompleteMessage`, `TransferInfo`, `FileMetadata`, `Capabilities`, `ConfigMessage` |
| Transport | `network/framing.rs`, `network/tcp.rs`, `network/udp.rs` | MessagePack length-prefixed framing with magic bytes; `TcpConnection`/`TcpServer` (TCP_NODELAY + keepalive); UDP socket helpers |
| Crypto/check | `verification.rs`, `compression.rs` | CRC32 (per-chunk), streaming SHA256 (per-file); `AdaptiveCompressor` (Zstd levels -7..22, auto-disables under 1.05x ratio after sampling 3 chunks) |
| Flow control | `window.rs`, `bandwidth.rs` | `SlidingWindow`, `InFlightChunk`, `WindowConfig`; token-bucket throttle with `K`/`M`/`G` suffix parser |
| Discovery / NAT | `discovery.rs`, `nat.rs` | UDP beacon-based `DiscoveryManager`; STUN RFC 5389 client |
| Handshake | `handshake.rs` | `HandshakeClient`/`HandshakeServer`, produce `HandshakeResult { config, capabilities, peer_id }` |
| Transfer engine | `transfer_file.rs`, `transfer_folder.rs`, `transfer.rs` | `FileTransferSession` (single file, sequential or windowed), `FolderTransferSession` (walks tree, orchestrates per-file sessions, aggregates `TransferStats`) |
| Session | `session.rs` | `P2PSession` — bidirectional, symmetric facade combining handshake + transfer; the GUI and CLI both drive this |
| Cross-cutting | `state.rs`, `history.rs`, `progress.rs`, `reconnect.rs` | Resume-state JSON; transfer-history log; shared `ProgressState` consumed by CLI bars and GUI updates; exponential-backoff reconnect (2→4→8→16→32→60s) |

## Design points you can't see from one file

### `P2PSession` is symmetric

After `connect()`/`accept()` complete, the connection is fully bidirectional. `ConnectionRole::{Initiator, Responder}` is retained for **logging only** — every operation (`send_path`, `receive_to`, multiple in sequence, interleaved) works from either side. Don't reintroduce client/server asymmetry into the session layer; the asymmetry is confined to establishment.

### Transfer engine composition

`FolderTransferSession` does **not** reimplement chunk logic — it walks the directory tree and runs a `FileTransferSession` per file, then aggregates results. When adding folder-level behavior, decide whether it belongs:
- per-file (compression, verification, windowing) → `transfer_file.rs`
- per-folder (file enumeration, structure preservation, aggregate stats, state saves between files) → `transfer_folder.rs`

State is persisted **after each file completes** (not mid-file), so resume granularity is "skip completed files, start partial files from their last completed chunk." The chunk-level resume within a file is handled by `FileTransferSession` checking `state.completed_chunks` (bitvec) against the file on disk.

### Windowed vs sequential mode

Single switch: `WindowConfig::window_size`. `1` = sequential (one chunk, wait for ACK, next chunk), `>=2` = windowed. The sliding window:
- keeps up to N chunks in flight
- handles out-of-order ACKs (ACKs carry the chunk index)
- per-chunk timeout (10s) with exponential backoff on retry
- memory ≈ `window_size * chunk_size`

### Adaptive compression accounting

`AdaptiveCompressor` decides after the first 3 chunks whether to keep compressing. **Track uncompressed length from `chunk_data.len()` before compression** — using the compressed payload length to advance file offsets or update SHA256 will silently corrupt resume state and verification. This has caused incidents before; the comment in `compression.rs` exists for a reason.

### Protocol versioning

`PROTOCOL_VERSION = 1`, `MIN_PROTOCOL_VERSION = 1` (in `lib.rs`). Bump `PROTOCOL_VERSION` when adding fields to messages; bump `MIN_PROTOCOL_VERSION` only on a hard break. The handshake refuses peers below `MIN_PROTOCOL_VERSION`.

`ChunkMessage` checksums use a custom hex-string serde (`checksum_hex` in `protocol.rs`) — this is on purpose for human-readable wire dumps; the old array format is rejected explicitly.

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

Unit tests are `#[cfg(test)] mod tests { ... }` inline in each module. Cross-module workflow tests (handshake + TCP + discovery end-to-end) live in the workspace `tests/integration_test.rs`, not in this crate.

`dev-dependencies` available here: `tokio-test`, `tempfile`.

## Conventions specific to this crate

- **No CLI/UI concerns.** No `clap`, no `indicatif`, no `iced`. Progress is surfaced via `progress::ProgressState` callbacks; UI layers translate them.
- **All I/O is async (`tokio`).** Never block; use `tokio::select!` for timeouts/cancellation.
- **Hot paths** = the chunk loops in `window.rs` and `transfer_file.rs`. Avoid per-chunk allocations; reuse buffers; prefer `&[u8]` over `Vec<u8>` where possible.
- **Logging via `tracing`.** Targets default to `p2p_core`; the CLI's `EnvFilter` keys off this prefix.
- **Errors**: return `crate::Result<T>` (= `Result<T, crate::Error>`); don't sprinkle `anyhow` here — that's the user-facing layer's job.
- **Public items are documented** with `///`; modules have `//!` headers.
