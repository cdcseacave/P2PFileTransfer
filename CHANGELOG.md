# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
