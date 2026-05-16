# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```powershell
# CLI only (default, ~3 MB)
cargo build --release

# CLI + GUI (~7 MB)
cargo build --release --features full

# GUI only (~6 MB)
cargo build --release --features gui --no-default-features

# Debug build (faster compile)
cargo build
```

## Testing

```powershell
# All tests
cargo test

# Single test
cargo test test_full_connection_flow

# Integration tests only
cargo test --test integration_test

# With logging
RUST_LOG=debug cargo test -- --nocapture
```

## Workspace Structure

Four crates:
- **root (`p2p-transfer`)** — binary entry point (`src/main.rs`), feature flags wire together CLI/GUI
- **`p2p-core`** — library; all networking, protocol, transfer logic. No CLI/GUI deps.
- **`p2p-cli`** — clap-based CLI; thin wrappers around `p2p-core` operations
- **`p2p-gui`** — iced-based GUI; async operations via `p2p-core`

## Architecture

### Session model
`p2p-core/src/session.rs` is the central abstraction. A `P2PSession` is established via `connect()` (client) or `accept()` (server) + handshake. After handshake, both peers are symmetric — either can call `send_path()` or `receive_to()` on the same session. Role (client/server) is only relevant during TCP setup.

### Protocol / framing
Messages are defined in `p2p-core/src/protocol.rs` as a single `Message` enum, serialized with MessagePack (`rmp-serde`). Wire framing in `p2p-core/src/network/framing.rs` uses the magic bytes `P2PF` + length prefix. Transport in `network/tcp.rs` (reliable) and `network/udp.rs` (discovery only).

### Transfer pipeline
1. **Handshake** (`handshake.rs`) — version + capability negotiation
2. **Config exchange** — compression level, window size, chunk size
3. **File transfer** (`transfer_file.rs`) — chunked (default 64 KB), windowed sliding-window protocol (`window.rs`) with parallel in-flight chunks
4. **Folder transfer** (`transfer_folder.rs`) — orchestrates multiple `transfer_file` calls; exposes `scan_folder_for_parallel` / `split_files_for_parallel` for the parallel system (current branch `feat/parallel-sys`)
5. **Verification** (`verification.rs`) — streaming CRC32 per-chunk + SHA256 per-file
6. **State persistence** (`state.rs`) — JSON state file per transfer; enables chunk-level resume

### Key supporting modules
- `progress.rs` — `ProgressState` shared between CLI bars and GUI updates
- `compression.rs` — adaptive zstd; auto-disables for incompressible data
- `bandwidth.rs` — token bucket rate limiter
- `reconnect.rs` — exponential backoff (2s → 60s cap)
- `discovery.rs` / `nat.rs` — UDP broadcast peer discovery; STUN NAT traversal

### Feature flags
`gui` feature pulls in `iced` (large dep, ~60 MB binary). Default is `cli` only. The `full` feature builds both. Keep core logic in `p2p-core` with no GUI/CLI deps.

## Constants (p2p-core/src/lib.rs)
- Default chunk size: `65536` (64 KB)
- Default transfer port: `14567`
- Default discovery port: `14566`
- Protocol version: `1`

## Performance tuning reference
Window size × 1 MB ≈ memory usage. Recommended: LAN=8, WiFi=16 (default), Internet=32, Satellite=64.
