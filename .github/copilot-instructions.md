# GitHub Copilot Instructions for P2P File Transfer

## Project Overview

**P2P File Transfer** is a peer-to-peer file transfer system built in Rust. Peers connect over **QUIC** (TLS 1.3, cert-pinned) on a single UDP socket and stream files chunk-by-chunk over per-chunk unidirectional QUIC streams. Includes automatic LAN peer discovery, fault-tolerant resume, and an optional Iced GUI.

### Key Features
- **QUIC transport** (quinn 0.11): mandatory TLS 1.3, per-stream flow control replaces a sliding window
- **Cert-pinned identity**: per-device Ed25519 + self-signed cert, pinned by SHA-256 fingerprint
- **Per-chunk unidirectional streams**: `[u64 LE index | u8 flags | payload]`; no per-chunk ACKs/CRC (TLS AEAD authenticates every byte)
- **Automatic resume**: chunk-level bitmap with state persistence
- **Adaptive Zstd compression**: auto-disables on incompressible data
- **Bandwidth throttling**: token bucket
- **Session-based architecture**: bidirectional symmetric `P2PSession` reusable for many transfers

### Project Type
- **Primary**: Command-line tool (CLI)
- **Future**: GUI application framework (in progress)
- **Language**: Rust (stable channel)
- **Target**: Cross-platform (Windows, macOS, Linux)

---

## Tech Stack

### Core Technologies
- **Rust** (stable) - Primary implementation language
- **Cargo** - Build system and package manager
- **Tokio** (`1.47.1`) - Async runtime
- **MessagePack** (`rmp-serde 1.3.0`) - Binary serialization protocol

### Key Dependencies

#### Networking
- `tokio` - Async I/O, UDP
- `quinn` (`0.11`) - QUIC transport
- `rustls` (`0.23`) - TLS 1.3
- `rcgen` (`0.13`) - self-signed cert generation

#### Compression & Verification
- `zstd` (`0.13.3`) - Zstandard compression
- `sha2` - SHA256 hashing

#### CLI & UX
- `clap` (`4.5.48`) - Command-line argument parsing with derive macros
- `indicatif` (`0.17.11`) - Progress bars
- `console` (`0.15.11`) - Terminal styling and colors
- `dialoguer` (`0.11.0`) - Interactive prompts

#### Utilities
- `uuid` (`1.18.1`) - Transfer and session IDs
- `anyhow` (`1.0.100`) - Error handling
- `tracing` + `tracing-subscriber` - Structured logging
- `chrono` (`0.4.42`) - Timestamp handling
- `dirs` (`5.0`) - Platform-specific directories

#### GUI (Future)
- `iced` (`0.12.1`) - Cross-platform GUI framework (in development)

### Development Tools
- `rustfmt` - Code formatting
- `clippy` - Linting
- `cargo-test` - Unit and integration testing

---

## Coding Standards & Style

### Project-Specific Standards
- Follow official **Rust Style Guide** and **Rust API Guidelines**
- Use `rustfmt` with project configuration (see `clippy.toml`)
- Run `cargo clippy -- -D warnings` (zero warnings policy)

### Project-Specific Conventions

#### CLI Parameter Naming
- Use `--verbosity` (not `--log-level`) for logging configuration
- Global flag: `--verbosity`. Shared transfer flags (`--compress`, `--chunk-size`, `--max-speed`, ...) live in the `TransferParams` `Args` group; session-establishment flags (`--peer`, `--peer-fingerprint`, `--port`, `--discover`, `--role`) live in `SessionParams`.

#### Documentation Requirements
- **Each module must have documentation** describing its purpose and functionality
- **All public items require documentation comments** (`///`)
- Module-level docs (`//!`) for `lib.rs` and major modules
- Each time a new feature is implemented, update `CHANGELOG.md` with date and short description
- Once a feature is fully implemented and tested, remove it from `TODO.md`, and update `README.md` and `DESIGN.md` to describe its usage and implementation details

#### Logging Strategy
Use `tracing` macros for structured logging:
- `error!()` - Unrecoverable errors
- `warn!()` - Recoverable issues, unexpected conditions
- `info!()` - High-level operation progress
- `debug!()` - Detailed debugging info
- `trace!()` - Very verbose tracing

### Performance Guidelines
- **Avoid allocations in hot paths** - reuse buffers
- **Use `async` for I/O** - never block on network/disk
- **Prefer zero-copy when possible** - use references over cloning
- **Chunk size: 64KB** (optimal for network + disk)

---

## Project Structure

### Repository Layout
```
P2PFileTransfer/
├── .github/
│   └── copilot-instructions.md    # This file
├── p2p-core/                       # Core library (protocol, transport, transfer logic)
│   ├── src/
│   │   ├── lib.rs                  # Library entry point + constants
│   │   ├── error.rs                # Error types
│   │   ├── identity.rs             # Ed25519 keypair + self-signed cert (persistent)
│   │   ├── tls.rs                  # rustls configs + fingerprint-pinning verifier
│   │   ├── known_peers.rs          # TOFU fingerprint trust store
│   │   ├── protocol.rs             # Control-plane Message definitions
│   │   ├── handshake.rs            # HELLO/CONFIG over QUIC bidi control stream
│   │   ├── session.rs              # P2PSession (symmetric, bidirectional)
│   │   ├── transfer_file.rs        # Single-file transfer (one uni stream per chunk)
│   │   ├── transfer_folder.rs      # Folder transfer orchestration
│   │   ├── compression.rs          # Adaptive Zstd compression
│   │   ├── verification.rs         # File-level SHA256
│   │   ├── bandwidth.rs            # Token bucket rate limiting
│   │   ├── reconnect.rs            # Exponential-backoff retry loop
│   │   ├── state.rs                # Chunk bitmap for resume
│   │   ├── history.rs              # Transfer history tracking
│   │   ├── config.rs               # Configuration types
│   │   ├── discovery.rs            # UDP peer discovery
│   │   ├── traversal/              # STUN + future hole-punch/rendezvous
│   │   │   ├── mod.rs
│   │   │   └── stun.rs             # Async STUN on a borrowed UdpSocket
│   │   └── network/
│   │       ├── mod.rs              # Re-exports
│   │       ├── quic.rs             # QuicEndpoint + QuicConnection (only transport)
│   │       ├── udp.rs              # LAN beacon socket helpers
│   │       └── framing.rs          # MessagePack framing
│   └── Cargo.toml
├── p2p-cli/                        # CLI wrapper
│   ├── src/
│   │   ├── lib.rs                  # CLI initialization
│   │   ├── cli.rs                  # Argument parsing with clap
│   │   ├── send.rs                 # Send command
│   │   ├── receive.rs              # Receive command
│   │   ├── discover.rs             # Discovery command
│   │   ├── resume.rs               # Resume command
│   │   ├── history.rs              # History command
│   │   └── nat_test.rs             # NAT test command
│   └── Cargo.toml
├── p2p-gui/                        # GUI (future, in development)
│   ├── src/
│   │   └── lib.rs                  # Iced GUI framework skeleton
│   └── Cargo.toml
├── src/
│   └── main.rs                     # Binary entry point
├── tests/
│   └── integration_test.rs         # Integration tests
├── Cargo.toml                      # Workspace root
├── Cargo.lock                      # Locked dependencies
├── clippy.toml                     # Clippy configuration
├── rust-toolchain.toml             # Rust toolchain version
├── test_transfer.py                # Python integration test script
├── benchmark.py                    # Performance benchmarking script
└── Documentation/
    ├── README.md                   # User-facing documentation
    ├── DESIGN.md                   # Architecture & design decisions
    ├── TODO.md                     # Planned features
    ├── CHANGELOG.md                # Version history and changes
    ├── CONTRIBUTING.md             # Contribution guidelines
    └── LICENSE                     # MIT License
```

### Key Files Explained

#### Core Library (`p2p-core/src/`)

**`protocol.rs`** - Control-plane message definitions (chunk data does NOT go through this enum)
- `HelloMessage` - Handshake hello (carries cert fingerprint)
- `ConfigMessage` - Transfer configuration negotiation
- `TransferInfo` - File/folder metadata + optional resume point
- `CompleteMessage` - Transfer completion summary
- `FileChecksumMessage` - Bidirectional file SHA256 exchange
- `ErrorMessage` - Error reporting

**`network/quic.rs`** - QUIC transport (the only transport)
- `QuicEndpoint` - wraps `quinn::Endpoint`; one UDP socket; acts as both client and server
- `QuicConnection` - wraps `quinn::Connection` + the bidi control stream; exposes `open_uni`/`accept_uni` for per-chunk streams

**`transfer_file.rs`** - File transfer engine
- `FileTransferSession` - opens one unidirectional QUIC stream per chunk
- Wire format: `[u64 LE chunk_index | u8 flags | payload]`
- Handles compression, file-level SHA256, progress tracking
- Resume support with chunk-level granularity (skip indices already in the bitmap)

**`transfer_folder.rs`** - Folder transfer orchestration
- `FolderTransferSession` - Multi-file transfers
- Preserves directory structure
- Sequential file processing (one file completes before next)
- Aggregates statistics across all files

**`session.rs`** - High-level session management
- `P2PSession` - Bidirectional connection abstraction
- Separates connection establishment from operations
- Enables multiple transfers on same connection
- Auto-receive event loop for server mode

**`compression.rs`** - Adaptive compression
- `AdaptiveCompressor` - Auto-detects incompressible data
- Samples first 3 chunks, disables if ratio < 1.05x
- Uses Zstd levels -7 to 22
- **Critical**: Must use `chunk_data.len()` for uncompressed size tracking

**`verification.rs`** - Data integrity
- File-level SHA256 only (per-chunk CRC removed — TLS 1.3 AEAD authenticates every byte)
- Sender computes SHA256 incrementally as chunks are read; receiver computes from the finalized file

#### CLI Layer (`p2p-cli/src/`)

**`cli.rs`** - Clap argument parsing
- Uses derive macros for clean definitions
- **Parameter naming**: Use `verbosity` (not `log-level`)
- Global flag: `--verbosity`. Shared `Args` groups: `SessionParams` (`--peer`, `--peer-fingerprint`, `--port`, `--discover`, `--role`) and `TransferParams` (`--compress`, `--compress-level`, `--adaptive`, `--chunk-size`, `--max-speed`).

**`send.rs`**, **`receive.rs`**, etc. - Command implementations
- Bridge between CLI args and core library
- Handle user interaction (prompts, progress)
- Error formatting for user-friendly messages

#### Documentation Files

**`README.md`** - User documentation
- Installation instructions
- Usage examples for all commands
- Performance tuning guidelines
- NAT traversal notes

**`DESIGN.md`** - Architecture documentation
- System architecture diagrams
- Protocol specifications
- Module responsibilities
- Design decisions and rationale
- Implementation details for major features

**`TODO.md`** - Development roadmap
- Organized by priority phases
- Time estimates for features
- Implementation notes for future complex features

**`CHANGELOG.md`** - Version history
- Semantic versioning
- Dated entries for all changes
- Categories: Added, Changed, Fixed, Removed

---

## Best Practices

### Architectural Patterns

#### 1. **Session-Based Architecture**
- Connection establishment separate from operations
- Enables bidirectional transfers
- Supports multiple operations per connection
- Future-proof for GUI applications

#### 2. **Separation of Concerns**
- **Protocol layer**: Message definitions (protocol.rs)
- **Network layer**: TCP/UDP transport (network/)
- **Transfer layer**: File/folder logic (transfer_*.rs)
- **Session layer**: Connection management (session.rs)
- **CLI layer**: User interaction (p2p-cli/)

#### 3. **Async/Await Pattern**
- All I/O operations are async
- Use `tokio::spawn` for concurrent tasks
- Use `tokio::select!` for timeouts and cancellation
- Never block the runtime

#### 4. **Progress Callbacks**
- Use callback pattern for progress reporting
- Callbacks are `Box<dyn Fn(Progress) + Send + Sync>`
- Enable CLI progress bars and future GUI updates

#### 5. **Error Context**
- Add contextual information to errors
- Use `anyhow::Context` trait
- Include file paths, chunk indices, etc.

### Testing Frameworks

#### Unit Tests
- Inline tests in each module (`#[cfg(test)]`)
- Test edge cases, error conditions
- Use helper functions for test data

#### Integration Tests
- Located in `tests/integration_test.rs`
- Test full workflows (handshake, transfer, discovery)
- Use async test macros

#### Python Integration Tests
- Script: `test_transfer.py`
- Tests real file transfers end-to-end
- Verifies statistics, compression, windowed mode, data integrity
- Remove `test_file` before running, when changing test size or compressibility

---

## Testing Procedure

### Complete Test Pipeline

Run tests in this order to ensure full validation:

#### 1. **Clean Build**
```bash
cargo clean
cargo build --release
```
**Expected**: Successful compilation in ~40-45 seconds

#### 2. **Unit Tests**
```bash
cargo test --all
```
**Expected**: 
- p2p-core: 50 tests pass
- Integration tests: 4 tests pass
- Doc tests: 8 tests pass
- **Total: 62 tests passed**

#### 3. **Clippy Linting**
```bash
cargo clippy --all-targets --all-features -- -D warnings
```
**Expected**: Zero warnings (strict mode)

#### 4. **Code Formatting Check**
```bash
cargo fmt -- --check
```
**Expected**: All files properly formatted

#### 5. **Documentation Build**
```bash
cargo doc --no-deps
```
**Expected**: Documentation generates without warnings

#### 6. **Python Integration Tests**

##### Test 1: Highly Compressible Data
```bash
rm -f test_file
python3 test_transfer.py --size 50 --compressible
```

**Validation Checklist:**
- ✅ Compression ratio > 100x (zeros compress extremely well)
- ✅ Network bytes << original bytes
- ✅ Throughput speed reflects actual data processed
- ✅ Files match after decompression
- ✅ No errors or warnings

##### Test 2: Incompressible Data
```bash
rm -f test_file
python3 test_transfer.py --size 50
```

**Validation Checklist:**
- ✅ Compression ratio = 1.00x (adaptive disabled)
- ✅ Network bytes ≈ original bytes (minimal overhead)
- ✅ Network speed ≈ throughput speed
- ✅ Files match perfectly (no compression applied)
- ✅ No errors or warnings

#### 7. **Binary Verification**
```bash
./target/release/p2p-transfer --help
```
**Expected**: Help text displays all commands

#### 8. **Performance Baseline**
```bash
python3 benchmark.py
```
**Expected**:
- Localhost transfers: > 70 MB/s
- Windowed mode: 5-15x faster than sequential on WAN
- Memory usage: Reasonable (window_size × 1MB)

---

## Important Notes for Copilot

### When Refactoring
1. **Always run the complete test pipeline** (see Testing Procedure above)
2. **Never remove fields without checking usage** across entire codebase
3. **Document why fields exist** if they appear unused (future extensibility)

### When Adding Features
1. **Add unit tests** for new functionality
2. **Update documentation** (code comments + markdown files)
3. **Follow existing patterns** (async/await, error handling, callbacks)
4. **Update in TODO.md** if it's a partial implementation
5. **Remove from TODO.md** when fully implemented and add it to README.md and DESIGN.md
6. **Update CHANGELOG.md** with date and short description

### When Fixing Bugs
1. **Write a failing test first** that reproduces the bug
2. **Fix the bug** and verify test passes
3. **Run full test suite** to ensure no regressions
4. **Update CHANGELOG.md** with the fix

### Documentation Policy
**CRITICAL**: Never create new markdown documentation files for feature summaries or implementation notes.

✅ **DO**:
- Update `README.md` with usage examples and user-facing documentation
- Update `DESIGN.md` with architecture and implementation details
- Update `TODO.md` to remove completed features or add notes for partial implementations
- Update `CHANGELOG.md` with dated entries for all changes
- Add inline code comments and module documentation

❌ **DON'T**:
- Create files like `FEATURE_NAME.md`, `IMPLEMENTATION_SUMMARY.md`, `QUICK_REFERENCE.md`, etc.
- Create separate documentation files for individual features
- Create temporary documentation files that duplicate existing docs

**Rationale**: Keep documentation centralized in the four main files (README, DESIGN, TODO, CHANGELOG) to avoid fragmentation and maintenance burden.
