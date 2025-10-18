# GitHub Copilot Instructions for P2P File Transfer

## Project Overview

**P2P File Transfer** is a high-performance, production-ready peer-to-peer file transfer system built in Rust. It enables direct device-to-device file and folder transfers on local networks with automatic peer discovery, fault-tolerant resume capability, and performance optimization through a sliding window protocol.

### Key Features
- **Windowed Transfer Protocol**: Parallel chunk transfers with sliding window (5-15x speedup on high-latency networks)
- **Automatic Resume**: Chunk-level resume support with state persistence
- **Smart Compression**: Adaptive Zstd compression with automatic incompressible data detection
- **Fault Tolerance**: Auto-reconnect, retry logic, and graceful interruption handling
- **Data Integrity**: Multi-layer verification (CRC32 per chunk + SHA256 per file)
- **Session-Based Architecture**: Connection reuse for multiple operations

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
- `tokio` - Async I/O, TCP/UDP
- `socket2` - Low-level socket configuration
- `mio` - Cross-platform I/O event notification

#### Compression & Verification
- `zstd` (`0.13.3`) - Zstandard compression
- `sha2` - SHA256 hashing
- `crc32fast` - CRC32 checksums

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
- Global flags: `--verbosity`, `--compress`, `--window-size`

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
├── p2p-core/                       # Core library (protocol, networking, transfer logic)
│   ├── src/
│   │   ├── lib.rs                  # Library entry point
│   │   ├── protocol.rs             # Message definitions
│   │   ├── window.rs               # Sliding window protocol
│   │   ├── transfer_file.rs        # File transfer engine
│   │   ├── transfer_folder.rs      # Folder transfer orchestration
│   │   ├── session.rs              # Session management
│   │   ├── handshake.rs            # Connection handshake
│   │   ├── compression.rs          # Adaptive Zstd compression
│   │   ├── verification.rs         # CRC32 + SHA256 verification
│   │   ├── bandwidth.rs            # Token bucket rate limiting
│   │   ├── reconnect.rs            # Auto-reconnect with backoff
│   │   ├── state.rs                # Transfer state persistence
│   │   ├── history.rs              # Transfer history tracking
│   │   ├── config.rs               # Configuration types
│   │   ├── error.rs                # Error types
│   │   ├── discovery.rs            # UDP peer discovery
│   │   ├── nat.rs                  # STUN NAT traversal
│   │   └── network/
│   │       ├── mod.rs              # Network module re-exports
│   │       ├── tcp.rs              # TCP connection
│   │       ├── udp.rs              # UDP socket
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

**`protocol.rs`** - Protocol message definitions
- `HandshakeMessage` - Capability negotiation
- `TransferInfo` - File/folder metadata
- `ChunkMessage` - Chunk data with checksum (7 fields after optimization)
- `ChunkAck` - Acknowledgment messages
- `CompleteMessage` - Transfer completion with SHA256
- `ErrorMessage` - Error reporting

**`window.rs`** - Sliding window flow control
- `SlidingWindow` - Manages parallel chunk transfers (7 fields)
- `InFlightChunk` - Tracks sent chunks (message + metadata)
- `WindowConfig` - Configuration (window size, timeout, retries)
- **Current Usage**: Single file at a time
- **Future**: Can be extended for connection pooling and concurrent transfers

**`transfer_file.rs`** - File transfer engine
- `FileTransferSession` - Single file transfer
- Supports both windowed and sequential modes
- Handles compression, verification, progress tracking
- Resume support with chunk-level granularity

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
- CRC32 per chunk (fast, catches corruption)
- SHA256 per file (cryptographic, final verification)
- Two-tier verification strategy

#### CLI Layer (`p2p-cli/src/`)

**`cli.rs`** - Clap argument parsing
- Uses derive macros for clean definitions
- **Parameter naming**: Use `verbosity` (not `log-level`)
- Global flags: `--verbosity`, `--compress`, `--window-size`

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
