# Design Document - P2P File Transfer

## Project Overview

P2P File Transfer is a high-performance, production-ready peer-to-peer file transfer system built in Rust. It provides direct device-to-device file and folder transfers on local networks with automatic peer discovery, fault-tolerant resume capability, real-time progress tracking, and performance optimization through a sliding window protocol.

### Design Principles

- **Performance First**: Windowed transfer protocol for parallel chunk processing
- **Fault Tolerance**: Automatic state management and seamless resume
- **User Experience**: Real-time feedback with two-tier progress bars
- **Reliability**: Multi-layer verification (CRC32 + SHA256)
- **Efficiency**: Smart compression with configurable levels
- **Simplicity**: Zero-configuration peer discovery and setup

### Scope

**Current Focus:**
- Local network P2P transfers (UDP broadcast discovery)
- Single file and folder transfers with structure preservation
- Resume support for interrupted transfers
- Performance optimization with sliding window protocol
- CLI interface with rich progress feedback

**Future Expansion:**
- Security layer (TLS encryption, authentication)
- Advanced features (bandwidth throttling, compression tuning)
- GUI interface with Iced framework
- Cross-platform mobile support

---

## Architecture

### High-Level System Design

```
┌─────────────────────────────────────────────────────────────┐
│                  Application Layer                          │
│         CLI (p2p-cli) / GUI (p2p-gui, future)              │
│  • Argument parsing • Progress display • User interaction   │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│              Core Transfer Engine (p2p-core)                │
│  ┌──────────────┬───────────────┬────────────────────────┐  │
│  │  Discovery   │  Handshake    │   Transfer Sessions    │  │
│  │  (UDP)       │   Protocol    │  (File/Folder/Window)  │  │
│  │              │               │                        │  │
│  │ • Beacons    │ • Capability  │ • FileTransferSession  │  │
│  │ • Peer list  │   negotiation │ • FolderTransferSession│  │
│  │ • Auto TTL   │ • Config      │ • SlidingWindow        │  │
│  │              │   exchange    │ • State management     │  │
│  └──────────────┴───────────────┴────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│              Network & Protocol Layer                       │
│  ┌──────────────────────┬──────────────────────────────┐    │
│  │   TCP Connection     │    UDP Discovery             │    │
│  │  • Keepalive         │   • Broadcast beacons        │    │
│  │  • Auto-reconnect    │   • Peer detection           │    │
│  │  • Message framing   │   • Protocol version check   │    │
│  │  • TCP_NODELAY       │                              │    │
│  └──────────────────────┴──────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│          Compression & Verification Layer                   │
│  ┌─────────────────────┬───────────────────────────────┐    │
│  │  Zstd Compression   │    Data Verification          │    │
│  │  • Levels 1-22      │   • CRC32 per chunk           │    │
│  │  • Stream support   │   • SHA256 per file           │    │
│  │  • Configurable     │   • Resume integrity          │    │
│  └─────────────────────┴───────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Crate Organization

```
p2p-transfer/                    # Cargo workspace root
├── Cargo.toml                   # Workspace definition
├── src/main.rs                  # Binary entry point (delegates to CLI)
├── p2p-core/                    # Core library (protocol + logic)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs               # Public API exports
│       ├── error.rs             # Error types and conversions
│       ├── protocol.rs          # Protocol message definitions
│       ├── config.rs            # Configuration structures
│       ├── state.rs             # Transfer state for resume
│       ├── compression.rs       # Zstd compression utilities
│       ├── verification.rs      # CRC32 and SHA256
│       ├── window.rs            # Sliding window protocol (360 lines)
│       ├── network/             # Networking abstractions
│       │   ├── mod.rs
│       │   ├── framing.rs       # Length-prefix framing
│       │   ├── tcp.rs           # TCP connections & server
│       │   └── udp.rs           # UDP discovery
│       ├── discovery.rs         # Peer discovery manager
│       ├── handshake.rs         # Connection handshake
│       ├── transfer.rs          # Transfer coordination
│       ├── transfer_file.rs     # Single file transfer logic (windowed + sequential)
│       └── transfer_folder.rs   # Folder transfer orchestration
├── p2p-cli/                     # CLI interface
│   ├── Cargo.toml
│   └── src/lib.rs               # Clap-based CLI implementation
├── p2p-gui/                     # GUI interface (future)
│   ├── Cargo.toml
│   └── src/lib.rs               # Iced-based GUI (placeholder)
└── tests/
    └── integration_test.rs      # Integration tests
```

---

## Core Components

### 1. Discovery System

**Purpose**: Automatic peer detection on local network using UDP broadcast.

**Implementation**: `p2p-core/src/discovery.rs` + `p2p-core/src/network/udp.rs`

#### Discovery Manager

```rust
pub struct DiscoveryManager {
    device_id: Uuid,
    device_name: String,
    listen_port: u16,
    peers: Arc<RwLock<HashMap<Uuid, PeerInfo>>>,
    broadcast_interval: Duration,    // Default: 2 seconds
    peer_ttl: Duration,               // Default: 10 seconds
}

pub struct PeerInfo {
    pub device_id: Uuid,
    pub device_name: String,
    pub addr: SocketAddr,
    pub protocol_version: u32,
    pub last_seen: Instant,
}

impl DiscoveryManager {
    pub async fn start(&self) -> Result<()>;
    pub async fn stop(&self) -> Result<()>;
    pub fn get_peers(&self) -> Vec<PeerInfo>;
    pub fn find_peer(&self, name_or_id: &str) -> Option<PeerInfo>;
}
```

#### Discovery Protocol Flow

```
Device A                           Device B
   |                                  |
   |--- Beacon (UDP broadcast) ------>|  Port 7777
   |    {id, name, addr, version}     |
   |                                  |
   |<---- Beacon (response) ----------|
   |      {id, name, addr, version}   |
   |                                  |
   |  (Both add each other to peer list)
   |                                  |
   | ... periodic beacons every 2s... |
   |                                  |
   | (Auto-cleanup removes stale peers after 10s)
```

**Beacon Structure**:
```rust
#[derive(Serialize, Deserialize)]
struct Beacon {
    device_id: Uuid,
    device_name: String,
    listen_addr: SocketAddr,
    protocol_version: u32,
}
```

**Thread Safety**: Uses `Arc<RwLock<HashMap>>` for concurrent peer list access.

---

### 2. Handshake Protocol

**Purpose**: Establish connection, negotiate capabilities, exchange configuration.

**Implementation**: `p2p-core/src/handshake.rs`

#### Handshake Flow

```
Client                         Server
  |                              |
  |------ HELLO ---------------->|
  |  {device_id, capabilities,   |
  |   protocol_version}          |
  |                              |
  |<----- HELLO_ACK -------------|
  |  {device_id, capabilities,   |
  |   protocol_version}          |
  |                              |
  |  [Version compatibility check]
  |                              |
  |------ CONFIG --------------->|
  |  {chunk_size, compress,      |
  |   compress_level, windowed,  |
  |   window_size}               |
  |                              |
  |<----- CONFIG_ACK ------------|
  |  {agreed configuration}      |
  |                              |
  |------ TRANSFER_INFO -------->|
  |  {transfer_id, file_list,    |
  |   metadata, resume_point}    |
  |                              |
  |<----- READY -----------------|
  |  {ready to receive}          |
  |                              |
  | >>> Begin data transfer >>>  |
```

#### Protocol Messages

```rust
#[derive(Serialize, Deserialize)]
pub enum ProtocolMessage {
    // Handshake messages
    Hello {
        device_id: Uuid,
        capabilities: Capabilities,
        version: u32,
    },
    HelloAck {
        device_id: Uuid,
        capabilities: Capabilities,
        version: u32,
    },
    
    // Configuration exchange
    Config {
        chunk_size: usize,
        compress: bool,
        compress_level: u8,
        windowed: bool,          // Use windowed protocol
        window_size: usize,      // Window size
    },
    ConfigAck {
        chunk_size: usize,
        compress: bool,
        compress_level: u8,
        windowed: bool,
        window_size: usize,
    },
    
    // Transfer coordination
    TransferInfo {
        transfer_id: Uuid,
        mode: TransferMode,      // File or Folder
        files: Vec<FileMetadata>,
        resume_point: Option<usize>,
    },
    Ready,
    
    // Data transfer
    Chunk {
        chunk_id: u64,
        data: Vec<u8>,
        crc32: u32,
        compressed: bool,
    },
    ChunkAck {
        chunk_id: u64,
    },
    
    // Completion
    Complete {
        total_chunks: u64,
        sha256: Option<[u8; 32]>,
    },
    
    // Error handling
    Error {
        code: ErrorCode,
        message: String,
    },
}
```

#### Capability Negotiation

```rust
bitflags! {
    pub struct Capabilities: u32 {
        const COMPRESSION  = 0b00000001;
        const RESUME       = 0b00000010;
        const FOLDER       = 0b00000100;
        const ENCRYPTION   = 0b00001000;  // Future
        const WINDOWED     = 0b00010000;  // Windowed protocol
    }
}
```

**Negotiation Logic:**
```rust
let agreed_capabilities = client_caps & server_caps;  // Bitwise AND
```

---

### 3. File Transfer System

**Purpose**: Transfer single files with chunking, compression, verification, and windowed protocol.

**Implementation**: `p2p-core/src/transfer_file.rs` + `p2p-core/src/window.rs`

#### File Transfer Session

```rust
pub struct FileTransferSession {
    connection: TcpConnection,
    config: ConfigMessage,
    transfer_id: Uuid,
}

impl FileTransferSession {
    // Sequential transfer (legacy, single chunk in-flight)
    pub async fn send_file(&mut self, path: &Path) -> Result<()>;
    pub async fn receive_file(&mut self, output_path: &Path) -> Result<()>;
    
    // Windowed transfer (multiple chunks in-flight)
    pub async fn send_file_windowed(&mut self, path: &Path) -> Result<()>;
}
```

#### Sequential Transfer Flow (Legacy)

```
Sender                                Receiver
  |                                      |
  |--- Chunk 0 ------------------------->|
  |                                      | (verify CRC32, write)
  |<-- ChunkAck 0 -----------------------|
  |                                      |
  |--- Chunk 1 ------------------------->|
  |                                      | (verify CRC32, write)
  |<-- ChunkAck 1 -----------------------|
  |                                      |
  | ... repeat for all chunks ...        |
  |                                      |
  |--- Complete (with SHA256) ---------->|
  |                                      | (verify SHA256)
  |<-- Final ACK ------------------------|
```

**Performance Limitation**: Round-trip time (RTT) bottleneck. On 50ms RTT:
- 1 chunk every 50ms = 20 chunks/sec
- At 1MB/chunk = 20 MB/s max (even on 1 Gbps network)

#### Windowed Transfer Flow (NEW)

```
Sender                                Receiver
  |                                      |
  |--- Chunk 0 ------------------------->|
  |--- Chunk 1 ------------------------->| (up to window_size chunks)
  |--- Chunk 2 ------------------------->| (no waiting for ACKs)
  |--- Chunk 3 ------------------------->|
  |  ...                                 |
  |--- Chunk 15 (window full) --------->|
  |                                      |
  |<-- ChunkAck 0 -----------------------| (ACKs arrive out-of-order)
  |<-- ChunkAck 2 -----------------------|
  |--- Chunk 16 (slide window) -------->|
  |<-- ChunkAck 1 -----------------------|
  |--- Chunk 17 ------------------------>|
  |<-- ChunkAck 3 -----------------------|
  |--- Chunk 18 ------------------------>|
  |                                      |
  | ... sliding window continues ...     |
  |                                      |
  | (Timeout detected for chunk 5)       |
  |--- Chunk 5 (retry) ----------------->|
  |<-- ChunkAck 5 -----------------------|
  |                                      |
  |--- Complete (with SHA256) ---------->|
  |<-- Final ACK ------------------------|
```

**Performance**: Multiple chunks in-flight eliminate RTT bottleneck. On 50ms RTT:
- 16 chunks in-flight
- Throughput limited by bandwidth, not RTT
- Expected 5-15x speedup depending on network conditions

#### Sliding Window Protocol

**Implementation**: `p2p-core/src/window.rs` (360 lines)

```rust
pub struct SlidingWindow {
    window_size: usize,                    // Max chunks in-flight (default 16)
    in_flight: HashMap<u64, InFlightChunk>,  // Chunks awaiting ACK
    next_chunk_id: u64,                    // Next chunk to send
    timeout: Duration,                     // Per-chunk timeout (10 seconds)
    max_retries: usize,                    // Max retry attempts (3)
}

pub struct InFlightChunk {
    pub chunk_id: u64,
    pub sent_at: Instant,
    pub retries: usize,
}

impl SlidingWindow {
    pub fn new(config: WindowConfig) -> Self;
    
    // Check if window has space for more chunks
    pub fn can_send(&self) -> bool {
        self.in_flight.len() < self.window_size
    }
    
    // Mark chunk as sent
    pub fn mark_sent(&mut self, chunk_id: u64);
    
    // Process acknowledgment (handle out-of-order ACKs)
    pub fn process_ack(&mut self, chunk_id: u64) -> bool;
    
    // Find timed-out chunks for retry
    pub fn check_timeouts(&mut self) -> Vec<u64>;
    
    // Check if all chunks acknowledged
    pub fn is_complete(&self) -> bool;
}
```

**Windowed Send Algorithm**:
```rust
// Simplified pseudocode
loop {
    // Phase 1: Fill window with new chunks
    while window.can_send() && has_more_chunks() {
        let chunk_id = next_chunk();
        send_chunk(chunk_id).await?;
        window.mark_sent(chunk_id);
    }
    
    // Phase 2: Receive ACKs (non-blocking, 50ms timeout)
    while let Ok(ack) = recv_ack_with_timeout(50ms).await {
        window.process_ack(ack.chunk_id);
    }
    
    // Phase 3: Check for timeouts and retry
    for timed_out_chunk_id in window.check_timeouts() {
        send_chunk(timed_out_chunk_id).await?;
        window.mark_sent(timed_out_chunk_id);
    }
    
    // Exit when all chunks acknowledged
    if window.is_complete() && no_more_chunks() {
        break;
    }
}
```

**Configuration**:
```rust
pub struct WindowConfig {
    pub window_size: usize,    // Default: 16 chunks
    pub timeout: Duration,     // Default: 10 seconds
    pub max_retries: usize,    // Default: 3 attempts
}
```

**Memory Usage**: `window_size × chunk_size`
- Window 16 × 1MB = 16MB
- Window 32 × 1MB = 32MB
- Window 64 × 1MB = 64MB

---

### 4. Folder Transfer System

**Purpose**: Orchestrate multi-file transfers with structure preservation.

**Implementation**: `p2p-core/src/transfer_folder.rs`

#### Folder Transfer Session

```rust
pub struct FolderTransferSession<'a> {
    connection: &'a mut TcpConnection,   // Borrows connection
    config: ConfigMessage,
    transfer_id: Uuid,
    progress_callback: Option<ProgressCallback>,
    state_callback: Option<StateCallback>,
}

pub type ProgressCallback = Box<dyn Fn(FolderProgress) + Send + Sync>;
pub type StateCallback = Box<dyn Fn(&FolderTransferState) + Send + Sync>;

impl<'a> FolderTransferSession<'a> {
    pub fn set_progress_callback(&mut self, callback: ProgressCallback);
    pub fn set_state_callback(&mut self, callback: StateCallback);
    
    pub async fn send_folder(&mut self, folder_path: &Path, base_name: &str) -> Result<()>;
    pub async fn receive_folder(&mut self, output_dir: &Path) -> Result<()>;
    pub async fn resume_send_folder(&mut self, folder_path: &Path, state: &FolderTransferState) -> Result<()>;
}
```

#### Folder Transfer Flow

```
Sender                                Receiver
  |                                      |
  | 1. Scan folder recursively           |
  |    - Collect all files               |
  |    - Calculate SHA256 for each       |
  |    - Build relative paths            |
  |                                      |
  |--- TransferInfo ------------------->|
  |    {file_list, metadata}            |
  |                                      | 2. Create directory structure
  |<-- Ready ----------------------------|
  |                                      |
  | 3. For each file in order:           |
  |                                      |
  |--- File 1 chunks ------------------>| 4. Receive, write, verify
  |<-- ACKs -----------------------------|
  |    [Progress: file 1 done]           | [SHA256 verification]
  |                                      |
  |--- File 2 chunks ------------------>|
  |<-- ACKs -----------------------------|
  |    [Progress: file 2 done]           | [SHA256 verification]
  |    [State callback: save state]      |
  |                                      |
  | ... repeat for all files ...         |
  |                                      |
  |--- Complete ----------------------->|
  |<-- Final ACK -----------------------|
  |    [Delete state file]               |
```

#### Progress Tracking

```rust
#[derive(Debug, Clone)]
pub struct FolderProgress {
    pub total_files: usize,
    pub completed_files: usize,
    pub current_file: Option<String>,
    pub current_file_progress: f64,    // 0.0 to 1.0
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub overall_progress: f64,         // 0.0 to 1.0
}
```

**Callback Usage**:
```rust
session.set_progress_callback(Box::new(|progress| {
    println!("[{}/{}] {} - {:.1}%",
        progress.completed_files,
        progress.total_files,
        progress.current_file.unwrap_or_default(),
        progress.current_file_progress * 100.0
    );
}));
```

---

### 5. Resume System

**Purpose**: Fault-tolerant transfers with automatic state persistence and recovery.

**Implementation**: `p2p-core/src/state.rs` + callbacks in `transfer_folder.rs`

#### Transfer State Structure

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderTransferState {
    pub transfer_id: Uuid,
    pub folder_name: String,
    pub files: Vec<PathBuf>,              // All files in transfer
    pub completed_files: HashSet<PathBuf>, // O(1) lookup
    pub current_file: Option<usize>,
    pub started_at: u64,                  // Unix timestamp
    pub last_updated: u64,                // Unix timestamp
}

impl FolderTransferState {
    pub async fn save_to_file(&self, path: &Path) -> Result<()>;
    pub async fn load_from_file(path: &Path) -> Result<Self>;
    pub fn mark_file_complete(&mut self, file_path: &PathBuf);
    pub fn next_file(&self) -> Option<usize>;
    pub fn progress_percentage(&self) -> f64;
    pub fn is_complete(&self) -> bool;
}
```

#### State File Management

**Naming Convention**: `transfer_{uuid}.json`

**Lifecycle**:
1. **Created** on transfer start (before first file)
2. **Updated** after each file completion (async, non-blocking)
3. **Preserved** on interruption (Ctrl+C saves automatically)
4. **Deleted** on successful completion

**Example State File**:
```json
{
  "transfer_id": "12345678-1234-5678-1234-567812345678",
  "folder_name": "my_project",
  "files": [
    "file1.txt",
    "file2.txt",
    "subfolder/file3.txt",
    "file4.txt"
  ],
  "completed_files": [
    "file1.txt",
    "file2.txt"
  ],
  "current_file": 2,
  "started_at": 1705234567,
  "last_updated": 1705234890
}
```

#### Auto-Save Mechanism

```rust
// Set state callback in CLI
session.set_state_callback(Box::new(move |state: &FolderTransferState| {
    let state_clone = state.clone();
    tokio::spawn(async move {
        let state_file = format!("transfer_{}.json", state_clone.transfer_id);
        if let Err(e) = state_clone.save_to_file(&state_file).await {
            eprintln!("⚠️  Failed to save state: {}", e);
        }
    });
}));
```

**Best-effort approach**: State saves are async and logged but don't fail the transfer.

#### Graceful Interruption

**Signal Handling** (in CLI):
```rust
tokio::select! {
    result = session.send_folder(&path, &folder_name) => {
        result?;
        println!("✅ Transfer complete!");
    }
    _ = tokio::signal::ctrl_c() => {
        println!("\n⚠️  Interrupted. State saved.");
        println!("  Resume with: p2p-transfer resume <transfer-id>");
        return Ok(());
    }
}
```

#### Resume Operation

**CLI Command**:
```bash
p2p-transfer resume <TRANSFER_ID> --to <ADDRESS> --path <FOLDER>
```

**Resume Flow**:
1. Load state from `transfer_{uuid}.json`
2. Reconnect to peer (fresh TCP connection)
3. Perform handshake with resume capability
4. Skip completed files (already on disk)
5. Resume from last incomplete file
6. Continue with full progress display
7. Update state during transfer
8. Delete state file on completion

---

### 6. Compression System

**Purpose**: Reduce transfer size using Zstd compression.

**Implementation**: `p2p-core/src/compression.rs`

```rust
pub fn compress(data: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::encode_all(data, level).map_err(|e| /* ... */)
}

pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data).map_err(|e| /* ... */)
}
```

**Compression Levels**: 1-22
- **1-3**: Fast, low compression (pre-compressed files)
- **3-9**: Balanced (default: 3)
- **10-19**: High compression (text/code)
- **20-22**: Maximum compression (archival)

**Per-Chunk Compression**:
- Each 1MB chunk compressed independently
- Receiver decompresses on-the-fly
- `compressed` flag in Chunk message

---

### 7. Verification System

**Purpose**: Ensure data integrity at chunk and file levels.

**Implementation**: `p2p-core/src/verification.rs`

#### Two-Layer Verification

**Chunk-level (CRC32)**:
```rust
pub fn calculate_crc32(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

pub fn verify_chunk(data: &[u8], expected_crc32: u32) -> bool {
    calculate_crc32(data) == expected_crc32
}
```

**File-level (SHA256)**:
```rust
pub fn calculate_sha256(path: &Path) -> Result<[u8; 32]> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    io::copy(&mut file, &mut hasher)?;
    Ok(hasher.finalize().into())
}
```

---

### 8. Network Layer

#### TCP Connection Management

**Implementation**: `p2p-core/src/network/tcp.rs`

```rust
pub struct TcpConnection {
    stream: TcpStream,
    addr: SocketAddr,
}

impl TcpConnection {
    pub async fn connect(addr: SocketAddr) -> Result<Self>;
    pub async fn send_message(&mut self, msg: &ProtocolMessage) -> Result<()>;
    pub async fn receive_message(&mut self) -> Result<ProtocolMessage>;
}
```

**Features**:
- TCP_NODELAY for low latency
- Keepalive: Ping/pong every 5 seconds
- Auto-reconnect: Exponential backoff (1s, 2s, 4s, 8s, 16s, 30s max)
- Timeouts: 10s connection, 30s receive

#### Message Framing

**Protocol**: Length-prefix framing
```
┌────────────────┬─────────────────────────┐
│  Length (u32)  │      Message Data       │
│   4 bytes      │    <length> bytes       │
└────────────────┴─────────────────────────┘
```

---

## Design Decisions

### 1. Async Architecture (Tokio)

**Rationale**: Non-blocking I/O essential for concurrent connections and responsive UI.

### 2. Callback-Based Progress

**Rationale**: Decouple core logic from UI concerns. Same callbacks work for CLI and GUI.

### 3. Borrowed Connection for Folders

**Rationale**: Folder transfer orchestrates multiple file transfers using same connection.
- `FileTransferSession` takes ownership (single files)
- `FolderTransferSession` borrows `&mut TcpConnection` (multi-file)

### 4. Best-Effort State Saving

**Rationale**: State saves should not fail the transfer. Async spawned tasks, errors logged.

### 5. Sliding Window Protocol

**Rationale**: Sequential transfer is RTT-bottlenecked on high-latency networks.

**Benefits**: 5-15x speedup on high-latency, maintains integrity, automatic retry.

**Trade-offs**: Increased memory, more complex logic, slight LAN overhead.

### 6. JSON for State Files

**Rationale**: Human-readable, easy to debug, forward-compatible.

---

## Performance Characteristics

### Theoretical Performance

#### Sequential Transfer

**Throughput**: `min(bandwidth, chunk_size / RTT)`

Example: 1MB chunks, 50ms RTT → Max 20 MB/s (even on 1 Gbps network)

#### Windowed Transfer

**Throughput**: `min(bandwidth, window_size × chunk_size / RTT)`

Example: 1MB chunks, 16 window, 50ms RTT → Max 320 MB/s (no longer RTT-bottlenecked)

**Speedup**: `≈ min(window_size, bandwidth × RTT / chunk_size)`

### Empirical Benchmarks

**Test Configuration:**
- Hardware: macOS ARM64 (Apple Silicon)
- Test file: 50MB random data
- Network: localhost (minimal RTT ~0.1ms)
- Compression: Enabled (zstd level 3)
- Chunk size: 1MB

**Results:**

| Transfer Mode | Window Size | Duration | Throughput | Speedup |
|--------------|-------------|----------|------------|---------|
| Sequential | N/A | 0.77s | 64.97 MB/s | 1.00x |
| Windowed | 4 | 0.73s | 68.89 MB/s | 1.06x |
| Windowed | 8 | 0.75s | 66.78 MB/s | 1.03x |
| Windowed | 16 (default) | 0.73s | 68.87 MB/s | 1.06x |
| Windowed | 32 | 0.72s | 69.33 MB/s | 1.07x |

**Key Findings:**

1. **Localhost Optimization**: On localhost with minimal RTT (~0.1ms), windowed protocol shows modest improvement (6-7%) because RTT is not the bottleneck
2. **CPU-Bound Performance**: Throughput is limited by compression/decompression (65-70 MB/s) rather than network
3. **Optimal Window Size**: Window size 16-32 provides best balance of throughput and memory usage
4. **Expected WAN Performance**: On networks with higher RTT (e.g., 50ms), windowed mode would show much larger speedups (10-20x) as predicted by theory

**Performance Optimization (Receiver):**

The receiver uses deferred-await pattern for maximum throughput:
```rust
// Verify checksum (fast: 1-2ms)
verification::verify_crc32(&chunk_msg.data, chunk_msg.checksum)?;

// Start sending ACK (creates future, network I/O begins)
let ack_future = self.send_ack(chunk_index, AckStatus::Success);

// Do expensive work while ACK is being sent (parallel execution)
let final_data = decompress(&chunk_msg.data)?;  // 10-50ms
writer.write_chunk(chunk_index, &final_data).await?;  // 5-20ms

// Ensure ACK completed (typically instant if already sent)
ack_future.await?;
```

This pattern allows ACK network I/O to overlap with CPU-intensive decompression and disk I/O, minimizing sender's perceived RTT.

**Benchmark Tool:**

A cross-platform Python benchmark script (`benchmark.py`) is provided for automated performance testing:

```bash
# Local mode (auto-starts receiver, tests on same machine)
python3 benchmark.py --mode sender

# Remote mode (tests between two machines on same network)
# On receiver machine:
python3 benchmark.py --mode receiver --port 7779

# On sender machine:
python3 benchmark.py --mode sender --receiver-ip 192.168.1.100 --port 7779
```

**Features:**
- Cross-platform (Windows, macOS, Linux)
- Dual mode: sender (runs tests) and receiver (accepts transfers)
- Automated test file creation (10MB, 50MB, 100MB, 500MB)
- Tests multiple window sizes (1, 4, 8, 16, 32)
- Comprehensive results with throughput calculations
- Saved results to `benchmark_results.txt`

### Memory Usage

| Component | Memory |
|-----------|--------|
| Window (16 chunks) | 16 MB |
| Compression buffer | 1-2 MB |
| Decompression buffer | 1-2 MB |
| **Total (typical)** | **20-25 MB** |

---

## Error Handling

### Error Categories

```rust
#[derive(Debug)]
pub enum P2PError {
    NetworkError(io::Error),
    ProtocolError(String),
    VerificationError { expected: u32, actual: u32 },
    CompressionError(String),
    Timeout,
    IncompatibleVersion { local: u32, remote: u32 },
    TransferAborted,
}
```

### Recovery Strategies

| Error Type | Recovery |
|------------|----------|
| Network timeout | Auto-reconnect with exponential backoff |
| Chunk CRC mismatch | Retransmit (up to 3 times) |
| File SHA256 mismatch | Abort, report corruption |
| Connection lost | Save state, allow resume |
| Incompatible version | Abort with clear message |

---

## Security Considerations

### Current State

**Network**: Unencrypted TCP (local network assumed trusted).

**Authentication**: None (UDP broadcast discovery).

**Integrity**: CRC32 + SHA256 (detects corruption, not tampering).

### Future Enhancements

1. **TLS Encryption**: Wrap TCP in TLS 1.3, self-signed certs for local network
2. **Authentication**: Pre-shared key, device pairing, token-based sessions
3. **Data Integrity with Auth**: HMAC instead of CRC32, signed manifests

---

## Testing Strategy

### Unit Tests

- Protocol serialization/deserialization
- Compression/decompression round-trips
- CRC32 and SHA256 calculations
- State management operations
- Sliding window operations

### Integration Tests

- Full connection flow (discovery → handshake → transfer)
- Concurrent connections (3+ simultaneous)
- Capability negotiation
- Resume after interruption

### Manual Testing

- Large file transfers (10+ GB)
- Folder transfers with many files (1000+)
- Resume after various interruption points
- Performance benchmarking

---

## Future Enhancements

See [TODO.md](TODO.md) for complete roadmap.

**Highlights**:
- Benchmarking suite for windowed vs sequential
- Security layer (TLS, authentication)
- Advanced features (bandwidth throttling, adaptive compression)
- GUI with Iced framework
- Mobile support (iOS, Android)

---

## References

- **Rust Async Book**: https://rust-lang.github.io/async-book/
- **Tokio Documentation**: https://tokio.rs/
- **Zstd Specification**: https://github.com/facebook/zstd
- **TCP Sliding Window**: RFC 793
