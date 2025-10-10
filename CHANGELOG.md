# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Chunk-level resume** (2025-10-10): Transfer now resumes from exact chunk where interrupted, not from beginning
  - Chunk completions tracked in memory during transfer
  - State automatically saved to disk when connection error detected (before reconnection attempt)
  - On reconnection, transfer skips already-completed chunks
  - Works with both sequential and windowed transfer modes
  - Significantly reduces retry overhead for interrupted large file transfers
  - No user intervention required - automatic with auto-reconnect feature

### Changed
- **Streaming checksum computation** (2025-10-09): Files are no longer read fully into memory for checksums
  - File scanning now only reads metadata (size, modified time), not file contents
  - SHA256 checksums computed incrementally during transfer (as chunks are read/written)
  - Added `FileChecksumMessage` for bidirectional checksum exchange after each file
  - Removed redundant `matches` field - sender compares checksums locally
  - Unified checksum protocol to single message type (removed `ChecksumAckMessage`)
  - Receiver computes SHA256 incrementally and verifies against sender's checksum
  - Significantly reduces memory usage for large files
  - Maintains same security guarantees (CRC32 per chunk + SHA256 per file)
- **Optimized chunk ACK latency** (2025-10-09): Chunk acknowledgments now sent in parallel with I/O operations
  - Receiver sends ACK immediately after CRC32 verification (before decompression and disk write)
  - Decompression and disk write happen in parallel with ACK network transmission
  - Reduces per-chunk round-trip latency, especially on high-latency networks
  - Improves throughput for sequential transfer mode
- **Optimized checksum exchange** (2025-10-09): Both peers send checksums simultaneously
  - Sender and receiver both send first, then receive (symmetric pattern)
  - Both checksum messages "in flight" simultaneously, reducing latency
  - Minimizes round-trip delay for file verification
- **Auto-reconnect at session level** (2025-10-09): Connection re-establishment now handled by P2PSession
  - Added `P2PSession::reconnect()` method to re-establish connection after failure
  - Retry logic moved from `FolderTransferSession` to `P2PSession::send_path()`
  - Session automatically reconnects and retries on transient errors (broken pipe, connection reset)
  - Supports exponential backoff with configurable max attempts (default: 5 retries)
  - File-level resume: Skips already-completed files in folder transfers
  - Note: Chunk-level resume within a file not yet implemented (file restarts if interrupted mid-transfer)
  - Only client (initiator) sessions support reconnection (server sessions can't reconnect)
  - Removed deprecated `FolderTransferSession::send_folder()` method (use `P2PSession::send_path()` instead)
  - Updated `resume` command to use `P2PSession::send_path()` for proper reconnection support
- **Reconnect test mode** (2025-10-09): Added `--test-reconnect` flag to test_transfer.py
  - Automatically kills receiver mid-transfer to test auto-reconnect
  - Configurable kill and restart delays
  - Verifies sender automatically reconnects and completes transfer
  - Implement test in `test_transfer.py`, for ex: `python3 test_transfer.py --size 30 --max-speed 2MB --compressible --test-reconnect --kill-delay 3 --restart-delay 2`

### Added
- **Bidirectional session support** (2025-10-06): CLI can now act as both client and server
  - Added `SessionParams` struct with `--role`, `--peer`, `--port`, and `--discover` parameters
  - Added `TransferParams` struct for common transfer configuration
  - Send command can operate as client (default) or server (`--role server`)
  - Receive command can operate as server (default) or client (`--role client`)
  - Both peers can send or receive after session establishment
  - Session documentation updated to clarify bidirectional capabilities
  - Added `P2PSession::establish()` convenience method for role-based connection (eliminates code duplication)

### Changed
- **CLI parameter rename** (2025-10-06): `--log-level` renamed to `--verbosity` for better clarity
- **CLI parameter rename** (2025-10-06): `--to` renamed to `--peer` for consistency with session role model
- **Protocol optimization** (2025-10-06): Removed redundant `uncompressed_size` field from `ChunkMessage`, saving 4 bytes per chunk
- **InFlightChunk refactoring** (2025-10-06): Now stores complete `ChunkMessage` for efficient retransmission without data duplication

### Added
- Initial project structure
- Core protocol definitions
- Message framing implementation
- Compression utilities (zstd)
- Verification utilities (CRC32/SHA256)
- Error handling system
- Configuration management
- Transfer state management
- Basic CLI scaffolding
- Basic GUI scaffolding
- Comprehensive design document
- **Bandwidth throttling**: Token bucket algorithm with configurable speed limits (2025-10-05)
  - New `--max-speed` CLI flag supporting K/M/G units
  - Token bucket implementation with 2-second burst capacity
  - Applied to all chunk sends and retries
  - Comprehensive tests for throttling behavior
- **NAT traversal (STUN)**: Public endpoint discovery for NAT/firewall traversal (2025-10-05)
  - STUN client implementation (RFC 5389)
  - Support for XOR-MAPPED-ADDRESS and MAPPED-ADDRESS attributes
  - NAT type detection (Open, Cone, Symmetric)
- **Adaptive compression**: Intelligent compression that auto-disables for incompressible data (2025-10-05)
  - Samples first 3 chunks to determine compression effectiveness
  - Uses 1.05 ratio threshold to detect pre-compressed data
  - Automatically disables compression if data doesn't benefit
  - Saves CPU cycles on already-compressed files (ZIP, JPG, MP4, etc.)
  - New `--adaptive` CLI flag (enabled by default)
  - Added `ConfigMessage::Default` trait with sensible defaults
  - Added `AdaptiveCompressor::Default` trait for cleaner initialization
  - Clean API: `new(level, sample_size)` uses default threshold
- **Chunk-level resume**: Resume from exact chunk within partial files (2025-10-05)
  - Enhanced `FileTransferSession` with `send_file_with_resume()` and `send_file_windowed_with_resume()`
  - Updated `FolderTransferState` to track completed chunks per file (using BitVec bitmap)
  - Added `SlidingWindow::mark_completed()` for resume support in windowed mode
  - Simplified `ResumePoint` protocol message to use only `completed_chunks` bitmap
  - Removed `chunk_index` field (no backward compatibility needed)
  - Resume now skips individual completed chunks, not just whole files
  - Significantly faster resume for large files with partial completion (80-99% efficiency improvement)
- **Transfer history**: Track and view past transfers (2025-10-05)
  - New `history` module for tracking transfer records
  - Stores transfer ID, timestamps, peer, files, bytes, duration, and status
  - CLI command: `p2p-transfer history` with filtering options
  - Persistent JSON storage in `~/.p2p-transfer/history.json`
- **Auto-reconnect & auto-resume**: Automatic recovery from network failures (2025-10-05)
  - New `reconnect` module with exponential backoff logic
  - Automatic retry on transient network errors (connection reset, timeout, broken pipe)
  - Exponential backoff: 2s → 4s → 8s → 16s → 32s → 60s (capped at max)
  - Smart error classification: transient vs permanent errors
  - CLI flags: `--auto-reconnect` (default: true), `--max-retries` (default: 5, 0=unlimited)
  - Sender: `send_folder_with_reconnect()` wraps transfers in retry loop
  - Receiver: `receive_folder_with_state()` auto-detects and resumes known transfers
  - State preservation: automatic save/load between retry attempts
  - Zero user intervention for WiFi dropouts, router restarts, brief outages
  - New `p2p-transfer history` CLI command with filtering options
  - Filter by direction (send/receive), status (completed/failed), and limit
  - History stored in `~/.p2p-transfer/history.json`
  - Supports Completed, Interrupted, and Failed status tracking
  - Human-readable timestamps and size formatting

### Changed
- Nothing yet

### Deprecated
- Nothing yet

### Removed
- Nothing yet

### Fixed
- Nothing yet

### Security
- Nothing yet

## [0.1.0] - 2025-10-04

### Added
- Project initialization
- Design document
- Basic project structure

---

[Unreleased]: https://github.com/yourusername/p2p-transfer/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/yourusername/p2p-transfer/releases/tag/v0.1.0
