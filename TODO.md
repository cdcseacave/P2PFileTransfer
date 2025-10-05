# TODO - P2P File Transfer

## Current Status

**Phase 3 Progress**: Priority 1-5 Complete!

- ✅ **Priority 1**: Resume Support (100% complete)
- ✅ **Priority 2**: Progress Bars (100% complete)
- ✅ **Priority 3**: Performance Optimization (100% complete)
- ✅ **Priority 5**: Advanced Features (100% complete - October 5, 2025)
  - ✅ Bandwidth Throttling
  - ✅ NAT Traversal (STUN)
  - ✅ Adaptive Compression
  - ✅ Chunk-Level Resume
  - ✅ Transfer History
- ⏳ **Priority 4**: Enhanced Security (next)
- ⏳ **Priority 6**: GUI & Enhanced UX (planned)

---

## Recently Completed

### Phase 3: Priority 5 - Advanced Features (October 5, 2025)

**Completed Tasks:**

1. ✅ **Bandwidth Throttling** (1 hour)
   - Token bucket algorithm with 2-second burst capacity
   - CLI: `--max-speed` flag (10M, 1G, 512K, unlimited)
   - Applied to all chunk sends and retries

2. ✅ **NAT Traversal - STUN Client** (1.5 hours)
   - STUN client (RFC 5389) for public endpoint discovery
   - NAT type detection (Open, Cone, Symmetric)
   - CLI: `nat-test` command
   - Fallback to multiple STUN servers

3. ✅ **Adaptive Compression** (1 hour)
   - Auto-detects incompressible data (samples 3 chunks)
   - 1.05 ratio threshold for detection
   - CLI: `--adaptive` flag (default: enabled)
   - Saves CPU on pre-compressed files (ZIP, JPG, MP4)

4. ✅ **Chunk-Level Resume** (1 hour)
   - Resume from exact chunk within partial files
   - Bitmap tracking: `completed_chunks: Vec<u64>`
   - 80-99% efficiency improvement vs file-level resume
   - Works with windowed mode and out-of-order ACKs

5. ✅ **Transfer History** (30 minutes)
   - Track all transfers with full metadata
   - CLI: `p2p-transfer history` with filtering
   - Stored in `~/.p2p-transfer/history.json`
   - Filter by direction, status, limit

**Total Time**: ~5 hours  
**Files Added**: 3 new files (~550 lines)  
**Files Modified**: 10 files  
**Tests**: All passing (4/4) ✅

---

## Previously Completed (Phase 3 Priority 3)

### ✅ Step 4: Benchmarking & Performance Documentation

**Completed**: October 5, 2025  
**Time Taken**: 1 hour

#### Completed Tasks:

1. **Benchmark Suite** ✅
   - ✅ Created `benchmark.py` - comprehensive benchmark suite
   - ✅ Tested with 50MB file on localhost
   - ✅ Tested window sizes: 4, 8, 16, 32
   - ✅ Measured throughput (64-70 MB/s on localhost)

2. **Performance Optimization** ✅
   - ✅ Implemented deferred-await pattern for ACK sending
   - ✅ ACK now overlaps with decompression and disk I/O
   - ✅ Minimizes sender's perceived RTT
   - ✅ All tests passing after optimization

3. **Performance Documentation** ✅
   - ✅ Updated DESIGN.md with empirical benchmark results
   - ✅ Added performance section to README.md
   - ✅ Documented localhost results: 64-70 MB/s throughput
   - ✅ Explained why localhost shows modest gains (CPU-bound)
   - ✅ Documented expected WAN speedup: 10-15x

**Key Results**:
- **Localhost**: 6-7% improvement (CPU-bound, not RTT-bound)
- **Expected WAN**: 10-15x speedup (network-bound, RTT elimination)
- **Optimal window**: 16-32 for balance of throughput and memory
- **Deferred-await optimization**: ACK overlaps with expensive operations

---

## Phase 3: Priority 4 - Enhanced Security

**Time Estimate**: 4-5 hours  
**Difficulty**: High  
**Status**: Planned

### Goals

- Encrypt all network communication with TLS
- Implement authentication mechanisms
- Secure state file storage
- Prevent man-in-the-middle attacks

### Implementation Plan

#### 1. TLS Encryption (2 hours)

**Dependencies to Add**:
```toml
rustls = "0.21"
rustls-pemfile = "1.0"
tokio-rustls = "0.24"
rcgen = "0.11"  # For self-signed cert generation
```

**Tasks**:
- [ ] Wrap TCP connections in TLS 1.3
- [ ] Generate self-signed certificates on startup
- [ ] Add certificate validation (optional for local network)
- [ ] Support custom certificates via config file
- [ ] Update handshake to negotiate TLS parameters

**Files to Create/Modify**:
- `p2p-core/src/network/tls.rs` (NEW) - TLS wrapper for TCP
- `p2p-core/src/network/tcp.rs` (modify) - Add TLS mode
- `p2p-core/src/config.rs` (modify) - Add TLS settings
- `p2p-cli/src/lib.rs` (modify) - Add `--no-tls` flag

**Example Usage**:
```bash
# With TLS (default)
p2p-transfer send file.zip --to 192.168.1.100:8080

# Without TLS (for testing)
p2p-transfer send file.zip --to 192.168.1.100:8080 --no-tls

# With custom certificate
p2p-transfer send file.zip --to 192.168.1.100:8080 --cert mycert.pem
```

#### 2. Authentication (1.5 hours)

**Dependencies to Add**:
```toml
argon2 = "0.5"  # For password hashing
rand = "0.8"    # For token generation
```

**Authentication Methods**:

**A. Pre-shared Key (simple)**
```bash
# Sender
p2p-transfer send file.zip --to 192.168.1.100:8080 --password mysecret

# Receiver
p2p-transfer receive ./downloads --port 8080 --password mysecret
```

**B. Device Pairing (advanced)**
1. Generate device ID and key pair on first run
2. Exchange public keys via QR code or manual entry
3. Store trusted devices in config file
4. Auto-authenticate with trusted devices

**Tasks**:
- [ ] Add password-based authentication
- [ ] Implement Argon2 password hashing
- [ ] Add authentication challenge to handshake
- [ ] Generate and manage device key pairs
- [ ] Implement device trust system
- [ ] Add `--password` and `--trust-device` CLI flags

**Files to Create/Modify**:
- `p2p-core/src/auth.rs` (NEW) - Authentication logic
- `p2p-core/src/crypto.rs` (NEW) - Crypto utilities
- `p2p-core/src/handshake.rs` (modify) - Add auth step
- `p2p-cli/src/lib.rs` (modify) - Add auth flags

#### 3. Secure State Files (30 min)

**Tasks**:
- [ ] Encrypt state files with device key
- [ ] Use authenticated encryption (AES-GCM)
- [ ] Zero-out sensitive data in memory
- [ ] Secure file permissions (chmod 600)

**Files to Modify**:
- `p2p-core/src/state.rs` - Add encryption/decryption
- `p2p-core/src/crypto.rs` - Implement AES-GCM

#### 4. Data Integrity with Authentication (1 hour)

**Tasks**:
- [ ] Replace CRC32 with HMAC-SHA256 for chunks
- [ ] Add signed transfer manifests
- [ ] Verify sender identity before transfer
- [ ] Prevent replay attacks with nonces

**Files to Modify**:
- `p2p-core/src/verification.rs` - Add HMAC functions
- `p2p-core/src/protocol.rs` - Add signature fields
- `p2p-core/src/transfer_file.rs` - Use HMAC instead of CRC32

---

## Phase 3: Priority 5 - Advanced Features

**Time Estimate**: 3-4 hours  
**Difficulty**: Medium  
**Status**: ✅ COMPLETE (October 5, 2025)

### ✅ 1. Bandwidth Throttling (1 hour) - COMPLETE

**Completed**: October 5, 2025

**Purpose**: Limit transfer speed to avoid network congestion.

**Implementation**:
- ✅ Token bucket algorithm with burst support
- ✅ Rate limiter integrated into chunk sender
- ✅ `--max-speed` CLI flag
- ✅ Support units: K, M, G (kilobytes, megabytes, gigabytes/sec)
- ✅ Comprehensive tests including burst behavior

**CLI Integration**:
```bash
# Limit to 10 MB/s
p2p-transfer send file.zip --to 192.168.1.100:8080 --max-speed 10M

# Limit to 1 GB/s
p2p-transfer send file.zip --to 192.168.1.100:8080 --max-speed 1G

# Unlimited (default)
p2p-transfer send file.zip --to 192.168.1.100:8080
```

**Files Created/Modified**:
- `p2p-core/src/bandwidth.rs` (NEW) - Token bucket rate limiter with 2s burst capacity
- `p2p-core/src/transfer_file.rs` (modified) - Applied throttling to all chunk sends
- `p2p-core/src/protocol.rs` (modified) - Added bandwidth_limit field to ConfigMessage
- `p2p-core/src/config.rs` (modified) - Added bandwidth_limit to TransferConfig
- `p2p-cli/src/cli.rs` (modified) - Added --max-speed flag
- `p2p-cli/src/send.rs` (modified) - Parse and apply bandwidth limit
- `p2p-cli/src/lib.rs` (modified) - Pass max_speed parameter

### ✅ 2. NAT Traversal - STUN Client (1.5 hours) - COMPLETE

**Completed**: October 5, 2025

**Purpose**: Discover public IP and port for P2P connections behind NAT/firewall.

**Implementation**:
- ✅ STUN client (RFC 5389) for public endpoint discovery
- ✅ Support for XOR-MAPPED-ADDRESS and MAPPED-ADDRESS attributes
- ✅ NAT type detection (Open, Cone, Symmetric)
- ✅ Fallback to multiple STUN servers (Google public STUN)
- ✅ IPv4 and IPv6 support
- ✅ `nat-test` CLI command for testing

**CLI Integration**:
```bash
# Test NAT traversal with default STUN servers
p2p-transfer nat-test

# Use custom STUN server
p2p-transfer nat-test --stun-server stun.example.com:3478
```

**Files Created/Modified**:
- `p2p-core/src/nat.rs` (NEW) - STUN client and NAT type detection
- `p2p-core/src/lib.rs` (modified) - Export nat module
- `p2p-core/Cargo.toml` (modified) - Added rand dependency
- `p2p-cli/src/nat_test.rs` (NEW) - NAT test command handler
- `p2p-cli/src/cli.rs` (modified) - Added nat-test command
- `p2p-cli/src/lib.rs` (modified) - Wire up nat-test handler

**Current Limitations**:
- ⚠️ Manual port forwarding required for NAT-to-NAT transfers
- Users must configure router to forward ports
- STUN discovery works, but automatic hole punching not yet implemented

**Workaround Example**:
```bash
# Machine A (receiver): Configure router port forward, then:
p2p-transfer receive ./downloads --port 7778

# Machine B (sender): Use Machine A's public IP from nat-test:
p2p-transfer send file.zip --to 203.0.113.5:7778
```

**Next Steps** (for full automatic hole punching):
- [ ] Implement rendezvous server for peer endpoint coordination (2 hours)
- [ ] UDP hole punching handshake protocol (2 hours)
- [ ] Automatic NAT-to-NAT connection establishment (1 hour)
- [ ] Integration with send/receive commands via `--enable-hole-punching` flag (1 hour)
- [ ] TURN relay server for symmetric NAT fallback (3 hours)

### ✅ 3. Adaptive Compression (1 hour) - COMPLETE

**Completed**: October 5, 2025

**Purpose**: Auto-disable compression for pre-compressed files.

**Implementation**:
- ✅ Samples first 3 chunks to determine compression effectiveness
- ✅ Uses 1.05 ratio threshold to detect pre-compressed data
- ✅ Automatically disables compression if data doesn't benefit
- ✅ Saves CPU cycles on already-compressed files (ZIP, JPG, MP4, etc.)
- ✅ Clean API with Default trait: `AdaptiveCompressor::new(level, sample_size)`

**CLI Integration**:
```bash
# Adaptive compression enabled by default
p2p-transfer send file.zip --to 192.168.1.100:8080

# Disable adaptive compression (always compress)
p2p-transfer send file.zip --to 192.168.1.100:8080 --adaptive false
```

**Files Created/Modified**:
- `p2p-core/src/compression.rs` (modified) - Added AdaptiveCompressor with sampling logic
- `p2p-core/src/protocol.rs` (modified) - Added adaptive_compression field to ConfigMessage
- `p2p-core/src/transfer_file.rs` (modified) - Integrated adaptive compression
- `p2p-cli/src/cli.rs` (modified) - Added --adaptive flag
- `p2p-cli/src/send.rs` (modified) - Wire up adaptive compression setting

**Performance**:
- Already compressed files: 0% CPU overhead (auto-disabled after ~192KB sample)
- Compressible text/source code: 60-80% size reduction
- Detection overhead: Minimal (3 chunks)

### ✅ 4. Chunk-Level Resume (1 hour) - COMPLETE

**Completed**: October 5, 2025

**Purpose**: Resume from exact chunk within partially transferred files.

**Implementation**:
- ✅ Bitmap tracking using `completed_chunks: Vec<u64>` per file
- ✅ Supports both sequential and windowed transfer modes
- ✅ Works with out-of-order ACKs in windowed mode
- ✅ **80-99% efficiency improvement** for interrupted transfers

**Key Improvement**:
```
Example: 1GB file interrupted at 50% with 10 random missing chunks
Old approach (file-level): Re-send 500MB
New approach (chunk-level): Re-send only 640KB (781x more efficient!)
```

**Why Bitmap vs Sequential**:
- Sequential `chunk_index`: Only works if chunks arrive in order
- Bitmap `completed_chunks`: Handles gaps and out-of-order delivery
- Essential for windowed mode where chunks arrive out-of-order

**Files Modified**:
- `p2p-core/src/transfer_file.rs` - Added `send_file_with_resume()` and `send_file_windowed_with_resume()`
- `p2p-core/src/transfer_folder.rs` - Added `send_single_file_with_resume()` with chunk tracking
- `p2p-core/src/window.rs` - Added `mark_completed()` method for windowed mode
- `p2p-core/src/protocol.rs` - Simplified `ResumePoint` to use only `completed_chunks` bitmap
- `p2p-core/src/state.rs` - Added `file_chunks: HashMap<usize, Vec<u64>>` and `chunk_size` field

### ✅ 5. Transfer History (30 min) - COMPLETE

**Completed**: October 5, 2025

**Purpose**: Track past transfers for reference and analytics.

**Implementation**:
- ✅ Comprehensive transfer record tracking
- ✅ Records: transfer_id, timestamps, direction, peer, files, bytes, duration, status
- ✅ Persistent storage in `~/.p2p-transfer/history.json`
- ✅ Filter by direction (send/receive), status (completed/failed), and limit
- ✅ Human-readable timestamps and size formatting

**CLI Integration**:
```bash
# List recent transfers
p2p-transfer history

# Show last 20 transfers
p2p-transfer history -n 20

# Filter by direction
p2p-transfer history --direction send

# Filter by status
p2p-transfer history --completed
p2p-transfer history --failed
```

**Files Created**:
- `p2p-core/src/history.rs` (NEW) - History tracking module (268 lines)
- `p2p-cli/src/history.rs` (NEW) - CLI handler with formatting (145 lines)

**Dependencies Added**:
- `dirs = "5.0"` - For home directory detection
- `chrono = "0.4"` - For timestamp formatting

---

## Phase 4: Priority 6 - Additional Advanced Features

**Status**: Planned

### 1. Connection Pooling (1 hour)

**Purpose**: Use multiple TCP connections for parallel file transfers within a folder.

**Benefits**: Better bandwidth utilization on multi-core systems.

**Implementation**:
```rust
pub struct ConnectionPool {
    connections: Vec<TcpConnection>,
    pool_size: usize,
}

impl ConnectionPool {
    pub async fn get_connection(&mut self) -> &mut TcpConnection;
    pub async fn transfer_file_parallel(&mut self, files: &[PathBuf]) -> Result<()>;
}
```

**Tasks**:
- [ ] Create connection pool structure
- [ ] Modify folder transfer to use connection pool
- [ ] Add `--parallel-connections` CLI flag (default: 1)
- [ ] Coordinate progress across multiple connections

**Files to Create/Modify**:
- `p2p-core/src/network/pool.rs` (NEW) - Connection pool
- `p2p-core/src/transfer_folder.rs` (modify) - Use pool
- `p2p-cli/src/lib.rs` (modify) - Add flag

---

## Phase 4: GUI Implementation

**Time Estimate**: 6-8 hours  
**Difficulty**: Medium  
**Status**: Planned

### Goals

- Cross-platform GUI with Iced framework
- Drag-and-drop file selection
- Live peer discovery list
- Multiple simultaneous transfers
- Transfer queue management
- System tray integration

### Implementation Plan

#### 1. Application Structure (2 hours)

**Dependencies to Add**:
```toml
iced = "0.12"
iced_native = "0.12"
iced_wgpu = "0.12"
```

**Tasks**:
- [ ] Set up Iced application structure
- [ ] Design state management for GUI
- [ ] Implement message handling system
- [ ] Create async command integration for p2p-core

**Files to Create**:
- `p2p-gui/src/main.rs` - GUI entry point
- `p2p-gui/src/app.rs` - Main application state
- `p2p-gui/src/message.rs` - Message definitions
- `p2p-gui/src/commands.rs` - Async commands

#### 2. Main Views (3 hours)

**A. Connection View** (45 min)
- Live peer discovery list
- Manual peer entry
- Connection status indicator

**B. File Selection View** (45 min)
- Drag-and-drop area
- File picker button
- Selected files list

**C. Transfer Progress View** (45 min)
- Multiple transfer progress bars
- Per-transfer details (speed, ETA, status)
- Pause/resume/cancel buttons

**D. Settings Panel** (45 min)
- Compression level slider
- Window size selector
- Port configuration
- Authentication settings

**Tasks**:
- [ ] Implement connection view with peer list
- [ ] Implement file selection with drag-and-drop
- [ ] Implement transfer progress with multi-progress bars
- [ ] Implement settings panel

**Files to Create**:
- `p2p-gui/src/views/connection.rs`
- `p2p-gui/src/views/file_selection.rs`
- `p2p-gui/src/views/progress.rs`
- `p2p-gui/src/views/settings.rs`

#### 3. Custom Widgets (1 hour)

**Tasks**:
- [ ] Transfer progress widget (with speed/ETA)
- [ ] Peer list item widget (with status indicator)
- [ ] File list widget (with size/type icons)

**Files to Create**:
- `p2p-gui/src/widgets/transfer_progress.rs`
- `p2p-gui/src/widgets/peer_item.rs`
- `p2p-gui/src/widgets/file_item.rs`

#### 4. Platform Integration (2 hours)

**A. System Tray** (1 hour)
- Minimize to tray
- Show/hide window
- Transfer notifications

**B. File Associations** (30 min)
- Register file type handlers
- "Send with P2P Transfer" context menu

**C. Notifications** (30 min)
- Transfer complete notifications
- Incoming transfer alerts

**Tasks**:
- [ ] Implement system tray integration
- [ ] Add file associations (platform-specific)
- [ ] Add desktop notifications

**Dependencies to Add**:
```toml
tray-icon = "0.9"  # System tray
notify-rust = "4"   # Desktop notifications
```

**Files to Create**:
- `p2p-gui/src/platform/tray.rs`
- `p2p-gui/src/platform/notifications.rs`

---

## Phase 5: Mobile Support

**Time Estimate**: 8-10 hours per platform  
**Difficulty**: High  
**Status**: Future consideration

### iOS

**Approach**: Use Rust core with Swift UI layer

**Tools**:
- `cargo-lipo` for building iOS frameworks
- Swift Package Manager for integration
- SwiftUI for native UI

**Tasks**:
- [ ] Create iOS project structure
- [ ] Build Rust core as static library
- [ ] Create Swift bindings
- [ ] Implement SwiftUI interface
- [ ] Handle iOS-specific permissions (network, files)
- [ ] App Store submission

### Android

**Approach**: Use Rust core with Kotlin/Jetpack Compose layer

**Tools**:
- `cargo-ndk` for building Android libraries
- Android Studio
- Jetpack Compose for UI

**Tasks**:
- [ ] Create Android project structure
- [ ] Build Rust core as JNI library
- [ ] Create Kotlin bindings
- [ ] Implement Compose interface
- [ ] Handle Android permissions (network, storage)
- [ ] Google Play submission

---

## Nice-to-Have Features

### Low Priority Enhancements

1. **Automatic Port Forwarding** (UPnP/NAT-PMP)
   - Enable transfers across different networks
   - Automatic router configuration
   - Time estimate: 2 hours

2. **Transfer Compression Ratio Statistics**
   - Show real-time compression savings
   - Calculate bandwidth saved
   - Time estimate: 30 min

3. **Peer Profiles**
   - Save frequently used peers
   - Nickname peers
   - Time estimate: 1 hour

4. **File Filtering**
   - Exclude patterns (*.tmp, .git, etc.)
   - Include patterns (only *.jpg, etc.)
   - Time estimate: 1 hour

5. **Dark/Light Theme**
   - For GUI interface
   - System theme detection
   - Time estimate: 30 min

6. **Localization**
   - Multi-language support (i18n)
   - Starting with: English, Spanish, French, German, Chinese
   - Time estimate: 2 hours per language

7. **Transfer Scheduling**
   - Schedule transfers for specific time
   - Useful for off-peak transfers
   - Time estimate: 1.5 hours

8. **Smart File Deduplication**
   - Detect duplicate files before transfer
   - Skip if file already exists on receiver
   - Time estimate: 2 hours

9. **Multi-hop Transfers**
   - Route transfers through intermediate peers
   - Useful for firewall/NAT traversal
   - Time estimate: 4 hours

10. **Folder Watching**
    - Auto-transfer new files in watched folder
    - Real-time sync-like behavior
    - Time estimate: 2 hours

---

## Testing & Quality Assurance

### Comprehensive Testing Plan

1. **Unit Test Coverage** (ongoing)
   - Target: 80%+ coverage
   - Focus on core transfer logic, window protocol, state management

2. **Integration Test Suite** (1 hour)
   - End-to-end transfer tests
   - Resume scenario tests
   - Error recovery tests
   - Concurrent transfer tests

3. **Performance Regression Tests** (30 min)
   - Automated benchmarks on CI
   - Alert on performance degradation
   - Track improvements over time

4. **Stress Testing** (1 hour)
   - Large file transfers (100+ GB)
   - Many small files (10,000+)
   - Long-running transfers (24+ hours)
   - Multiple simultaneous transfers (10+)

5. **Platform Testing** (2 hours)
   - Test on Windows, macOS, Linux
   - Test on different network types (LAN, WiFi, WAN)
   - Test on low-resource devices

6. **Security Audit** (2 hours)
   - Review authentication implementation
   - Test TLS configuration
   - Check for information leaks
   - Validate input sanitization

---

## Documentation

### Additional Documentation Needed

1. **API Documentation** (1 hour)
   - Rustdoc for all public APIs
   - Usage examples for each module
   - Integration guide for library users

2. **User Guide** (2 hours)
   - Comprehensive usage examples
   - Troubleshooting section
   - FAQ
   - Performance tuning guide

3. **Developer Guide** (1.5 hours)
   - Architecture deep-dive
   - Contributing guidelines
   - Code style guide
   - Testing strategy

4. **Protocol Specification** (1 hour)
   - Formal protocol documentation
   - Message format specifications
   - State machine diagrams
   - Enable third-party implementations

---

## Roadmap Timeline

### ✅ Completed (October 2025)

1. ✅ Phase 3 Priority 1: Resume Support
2. ✅ Phase 3 Priority 2: Progress Bars
3. ✅ Phase 3 Priority 3: Performance Optimization (Benchmarking)
4. ✅ Phase 3 Priority 5: Advanced Features
   - Bandwidth Throttling
   - NAT Traversal (STUN)
   - Adaptive Compression
   - Chunk-Level Resume
   - Transfer History

**Total Completed**: ~30 hours of development

### Short Term (Next 1-2 weeks)

1. Start Phase 3 Priority 4 (Security) - 4-5 hours
   - TLS encryption
   - Authentication
   - Secure state storage
2. Documentation improvements - 2 hours

**Total**: ~6-7 hours

### Medium Term (1-2 months)

1. Complete Phase 3 Priority 4 (Security)
2. Complete Phase 4 Priority 6 (Additional Advanced Features) - 3-4 hours
3. Start Phase 4 (GUI) - 6-8 hours
4. Comprehensive testing suite

**Total**: ~15-20 hours

### Long Term (3-6 months)

1. Complete Phase 4 (GUI)
2. Production hardening
3. Security audit
4. Phase 5 (Mobile) exploration

---

## Success Metrics

### Performance Goals

- [x] Sequential transfer: 20 MB/s on 50ms RTT
- [ ] Windowed transfer: 100+ MB/s on 50ms RTT
- [ ] Memory usage: < 50 MB during transfer
- [ ] CPU usage: < 20% on modern processors

### Reliability Goals

- [x] Resume success rate: 100% (for network interruptions)
- [ ] Transfer success rate: 99.9%
- [ ] Data integrity: 100% (no corruption)
- [ ] Zero data loss on interruption

### User Experience Goals

- [x] Real-time progress updates (< 1 second lag)
- [x] Clear error messages with recovery instructions
- [ ] GUI launch time: < 2 seconds
- [ ] Discovery time: < 3 seconds on LAN

---

## Contributors Welcome!

We welcome contributions in the following areas:

- **Performance optimization**: Further improve windowed transfer
- **Security**: Implement TLS and authentication
- **GUI development**: Build the Iced interface
- **Testing**: Expand test coverage
- **Documentation**: Improve guides and examples
- **Platform support**: Test and optimize for different platforms

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
