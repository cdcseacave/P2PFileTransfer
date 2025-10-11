# P2P File Transfer

A lightning-fast, resilient peer-to-peer file transfer system built in Rust with advanced features like resume support, real-time progress tracking, GUI interface, and windowed transfer protocol for optimal performance.

## Overview

P2P File Transfer is a production-ready application for transferring files and folders between devices on a local network. It features both a graphical user interface (default) and command-line interface, automatic peer discovery, fault-tolerant transfers with chunk-level resume capability, and optimized performance through parallel chunk transfers.

**Key Highlights:**
- 🖥️ **GUI Interface**: Modern graphical interface with tabbed navigation (default mode)
- ⚡ **Windowed Transfer Protocol**: Parallel chunk transfers for 5-15x speedup on high-latency networks
- 💾 **Chunk-Level Resume**: Resume from exact chunk within interrupted files, not just whole files
- 📊 **Real-time Progress**: Visual progress bars with speed, ETA, and transfer statistics
- 🗜️ **Smart Compression**: Adaptive Zstd compression auto-detects incompressible data
- 🔍 **Auto Discovery**: Find peers on local network via UDP broadcast
- ✅ **Streaming Verification**: Incremental SHA256 checksums (no memory overhead)
- 🚦 **Bandwidth Throttling**: Token bucket rate limiting with burst support
- 🔌 **NAT Traversal**: STUN-based public endpoint discovery for NAT/firewall traversal
- 🔄 **Auto-Reconnect**: Exponential backoff with seamless transfer continuation

## Features

### Core Capabilities
- ✅ **Single File & Folder Transfers**: Send individual files or entire directory trees
- ✅ **Structure Preservation**: Maintains folder hierarchy and file metadata
- ✅ **Chunked Streaming**: Efficient 64KB chunks with parallel processing
- ✅ **Adaptive Compression**: Auto-detects incompressible data (already compressed files)
- ✅ **Compression**: Zstd compression (levels -7 to 22) for bandwidth savings
- ✅ **Verification**: Multi-layer integrity checks (CRC32 + SHA256)
- ✅ **Cross-platform**: Runs on Windows, macOS, and Linux

### Performance Optimization
- ✅ **Windowed Transfer**: Sliding window protocol with configurable window size (default 16 chunks)
- ✅ **Out-of-order ACKs**: Handle responses in any order for maximum throughput
- ✅ **Automatic Retry**: Failed chunks are automatically retransmitted
- ✅ **Timeout Management**: 10-second chunk timeout with exponential backoff
- ✅ **Configurable Window**: Tune for LAN (4-8), WiFi (16), or WAN (32-64)

### Fault Tolerance
- ✅ **Auto-save State**: Transfer state saved after each file completion
- ✅ **Graceful Interruption**: Ctrl+C saves state for later resume
- ✅ **Chunk-Level Resume**: Resume from exact chunk within partial files (not just whole files)
- ✅ **Smart Resume**: Skip completed chunks, resume from next incomplete chunk
- ✅ **Auto-reconnect**: Exponential backoff with configurable max attempts
- ✅ **Transfer History**: Track past transfers with timestamps, sizes, and completion status

### User Experience
- ✅ **Graphical Interface**: Modern GUI with tabbed navigation (Connection, Send, Receive, Settings, History)
- ✅ **Real-time Progress**: Visual progress bars with speed, percentage, and ETA
- ✅ **File Browsers**: Native file/folder pickers for easy selection
- ✅ **Transfer History**: View past transfers with statistics and completion status
- ✅ **CLI Progress Bars**: Overall progress (files) + current file progress (bytes) in terminal
- ✅ **Color-coded Output**: Easy-to-read status indicators
- ✅ **Elapsed Time**: Track transfer duration
- ✅ **Transfer Mode Display**: See whether using windowed or sequential mode
- ✅ **Verbose Logging**: Detailed diagnostics with `-v` flag

### Architecture
- ✅ **Modular Design**: Separate core library, CLI, and GUI crates for clean separation
- ✅ **Session-Based Design**: Connection establishment separated from transfer operations
- ✅ **Bidirectional Transfers**: Either peer can send or receive after session setup
- ✅ **Multiple Operations**: Perform multiple transfers on same connection without re-handshaking
- ✅ **Auto-Receive Mode**: Receiver automatically accepts incoming transfers in event loop
- ✅ **GUI Implementation**: Full-featured Iced-based interface with async/await support

### Networking
- ✅ **TCP with Keepalive**: Reliable connections with automatic ping/pong
- ✅ **UDP Discovery**: Automatic peer detection on local network
- ✅ **Handshake Protocol**: Version and capability negotiation
- ✅ **TCP_NODELAY**: Low-latency optimizations
- ✅ **Bandwidth Throttling**: Token bucket rate limiting with burst support
- ✅ **NAT Traversal**: STUN client (RFC 5389) for public IP/port discovery
- ✅ **NAT Type Detection**: Identify Open, Cone, or Symmetric NAT configurations

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/p2p-transfer.git
cd p2p-transfer

# Build release binary (default: CLI only, ~3 MB)
cargo build --release

# Build with GUI support (~7 MB, includes both CLI and GUI)
cargo build --release --features full

# Build GUI only (~6 MB)
cargo build --release --features gui --no-default-features

# Binary location
./target/release/p2p-transfer
```

### GUI Mode (Default)

Simply run the program to launch the graphical interface:

```bash
# Default: Launch GUI
p2p-transfer

# Or explicitly specify GUI mode
p2p-transfer gui
```

**GUI Features:**
- **Connection Tab**: Start listening or connect to peers with discovery support
- **Send Tab**: Browse and select files/folders to transfer
- **Receive Tab**: Set download folder and auto-accept preferences
- **Settings Tab**: Configure all transfer parameters (compression, window size, bandwidth, etc.)
- **History Tab**: View past transfers with statistics
- **Real-time Progress**: Visual progress bar with speed, ETA, and transfer statistics

### CLI Mode

For command-line usage and automation, use specific commands:

#### Basic Usage

#### Bidirectional Sessions
After a session is established, **both peers are equal** and can send or receive files. The `--role` parameter only determines who initiates the connection:
- **Client role** (default for send): Connects to a peer
- **Server role** (default for receive): Listens for incoming connections

#### Send a File
```bash
# Send as client (default) - connect to peer and send
p2p-transfer send myfile.zip --peer 192.168.1.100:8080

# Send as server - listen for peer to connect, then send
p2p-transfer send myfile.zip --role server --port 8080

# With auto-discovery (client mode)
p2p-transfer send myfile.zip --discover

# Sequential mode (one chunk at a time)
p2p-transfer send myfile.zip --peer 192.168.1.100:8080 --window-size 1
```

#### Send a Folder
```bash
# Transfer entire directory with structure
p2p-transfer send ./my_project --peer 192.168.1.100:8080

# With compression (adaptive by default)
p2p-transfer send ./documents --peer 192.168.1.100:8080 --compress --compress-level 5

# Adaptive compression auto-disables for incompressible data (default: enabled)
p2p-transfer send ./mixed_content --peer 192.168.1.100:8080 --adaptive true

# Force compression even for incompressible data
p2p-transfer send ./photos --peer 192.168.1.100:8080 --adaptive false
```

#### Receive Files/Folders
```bash
# Receive as server (default) - listen for peer to connect and receive
p2p-transfer receive --output ./downloads --port 8080

# Receive as client - connect to peer and receive files
p2p-transfer receive --output ./downloads --role client --peer 192.168.1.100:8080

# Auto-accept incoming transfers (no prompts)
p2p-transfer receive --output ./received --port 14567 --auto-accept

# Short form
p2p-transfer receive -o ./received -p 14567 -a
```

**Note**: The receiver now runs in an event loop that automatically handles incoming transfers. When a peer initiates a send, the receiver will automatically start receiving - no manual action needed. The session stays alive for multiple transfers until the connection is closed.

#### Discover Peers
```bash
# Find available peers (default 3 second timeout)
p2p-transfer discover

# Extended discovery
p2p-transfer discover --timeout 10
```

#### Test NAT Traversal
```bash
# Discover your public IP and port using STUN
p2p-transfer nat-test

# Use custom STUN server
p2p-transfer nat-test --stun-server stun.example.com:3478
```

**Example Output:**
```
🔌 Testing NAT traversal...
  Using default STUN servers (Google public STUN)
  Querying STUN server...

✅ Successfully discovered public endpoint:
  Public IP:   203.0.113.5
  Public Port: 51234
  NAT Type:    RestrictedCone

🔓 Cone NAT detected - hole punching should work!
   You can establish P2P connections with most peers.
```

**Current Usage - Both Machines Behind NAT:**

Currently, when both machines are behind NAT, you need to manually use the discovered public endpoints.

**Manual Workaround** (requires port forwarding on router):

1. **On Machine A (receiver)** - Set up port forwarding on your router:
   ```bash
   # First, discover your public IP
   p2p-transfer nat-test
   # Output: Public IP: 203.0.113.5
   
   # Configure router to forward port 14567 to Machine A's local IP
   # (Done via router web interface, e.g., 192.168.1.100 → Internet:14567)
   
   # Start receiver
   p2p-transfer receive ./downloads --port 14567
   ```

2. **On Machine B (sender)** - Connect using Machine A's public IP:
   ```bash
   # Send to Machine A's public IP and forwarded port
   p2p-transfer send myfile.zip --peer 203.0.113.5
   ```

#### Resume Interrupted Transfer
```bash
# Transfer gets interrupted (Ctrl+C)
p2p-transfer send ./large_folder --peer 192.168.1.100:8080
# State saved to: transfer_12345678-1234-5678-1234-567812345678.json

# Resume later (supports chunk-level resume)
p2p-transfer resume 12345678-1234-5678-1234-567812345678 \
    --peer 192.168.1.100:8080 \
    --path ./large_folder
```

#### View Transfer History
```bash
# Show recent transfers
p2p-transfer history

# Show last 20 transfers
p2p-transfer history -n 20

# Show only sent transfers
p2p-transfer history --direction send

# Show only completed transfers
p2p-transfer history --completed

# Show only failed transfers
p2p-transfer history --failed
```

### Performance Tuning

```bash
# LAN (low latency, < 5ms)
p2p-transfer send file.zip --peer 192.168.1.100:8080 --window-size 8

# WiFi (medium latency, 10-20ms) - DEFAULT
p2p-transfer send file.zip --peer 192.168.1.100:8080 --window-size 16

# Internet (high latency, 50-100ms)
p2p-transfer send file.zip --peer 192.168.1.100:8080 --window-size 32

# Satellite/VPN (very high latency, 500ms+)
p2p-transfer send file.zip --peer 192.168.1.100:8080 --window-size 64
```

**Memory Usage**: Window size × 1MB chunk size
- Window 16 = 16MB memory
- Window 32 = 32MB memory
- Window 64 = 64MB memory

### Bandwidth Throttling

```bash
# Limit to 10 MB/s (useful for shared networks)
p2p-transfer send largefile.zip --peer 192.168.1.100:8080 --max-speed 10M

# Limit to 1 GB/s (for very fast networks)
p2p-transfer send largefile.zip --peer 192.168.1.100:8080 --max-speed 1G

# Limit to 512 KB/s (for slow connections)
p2p-transfer send largefile.zip --peer 192.168.1.100:8080 --max-speed 512K

# Unlimited bandwidth (default)
p2p-transfer send largefile.zip --peer 192.168.1.100:8080
```

**How it works**:
- Token bucket algorithm with 2-second burst capacity
- Applied to all chunk sends and retries
- Allows burst traffic up to 2 seconds worth of data
- Smooths out to configured limit over time

### Auto-Reconnect & Auto-Resume

Transfers automatically recover from network failures with exponential backoff:

```bash
# Auto-reconnect is enabled by default
p2p-transfer send large_folder/ --peer 192.168.1.100:8080

# Disable auto-reconnect (manual resume only)
p2p-transfer send large_folder/ --peer 192.168.1.100:8080 --auto-reconnect false

# Unlimited retries (keeps trying until success or permanent error)
p2p-transfer send large_folder/ --peer 192.168.1.100:8080 --max-retries 0

# Custom retry limit
p2p-transfer send large_folder/ --peer 192.168.1.100:8080 --max-retries 10
```

**How it works**:
- Detects transient network errors (connection reset, timeout, broken pipe)
- Exponential backoff: 2s → 4s → 8s → 16s → 32s → 60s (capped)
- Automatically saves and loads state between attempts
- Resumes from last completed chunk (chunk-level resume)
- Fails immediately on permanent errors (disk full, permission denied, etc.)
- Receiver automatically detects and resumes known transfers

**Example scenario**:
```
Transfer starts: [✓✓✓✓✓✓✓✓] - Transferring chunks...
WiFi drops:      [✓✓✓✓✓✓✓✓✗] - Connection lost at chunk 8
Auto-reconnect:  Waiting 2 seconds...
Retry attempt 1: [✓✓✓✓✓✓✓✓✓✓✓✓] - Resumed from chunk 8, continuing...
WiFi drops:      [✓✓✓✓✓✓✓✓✓✓✓✓✗] - Connection lost at chunk 12
Auto-reconnect:  Waiting 4 seconds...
Retry attempt 2: [✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓] - Completed successfully!
```

**Benefits**:
- Zero user intervention for transient failures
- Works seamlessly with chunk-level resume
- Prevents wasted retries on permanent errors
- Configurable for different reliability requirements
- Allows short bursts while maintaining average rate
- Applied to all chunk sends including retries
- Supported units: K (KB/s), M (MB/s), G (GB/s)

## Example Sessions

### Successful Transfer
```
📤 Starting send operation
  Path: myfile.zip
  Mode: Windowed (window size: 16)
  Connecting to: 192.168.1.100:8080
  ✓ Connected
  Performing handshake...
  ✓ Handshake complete

📄 Sending file: myfile.zip
   Size: 104857600 bytes (100 MB)
   Using windowed transfer protocol

Progress: 10/100 chunks (10.0%, 16 in-flight)
Progress: 20/100 chunks (20.0%, 16 in-flight)
Progress: 50/100 chunks (50.0%, 16 in-flight)
Progress: 100/100 chunks (100.0%, complete)

✅ File transfer complete!
   Transferred: 100 MB
   Duration: 15.2 seconds
   Average speed: 6.6 MB/s
```

### Interrupted and Resumed Transfer
```
📁 Sending folder: my_project
  State file: transfer_abc12345-def6-7890-ghij-klmnopqrstuv.json
[00:00:45] ████████████████████░░░░░░░░░░ 8/10 files (80%)
  Current: file8.txt ████████░░░░░░░░░░░░ 45MB/60MB (75%)

^C
⚠️  Transfer interrupted by user. State has been saved.
  Use 'p2p-transfer resume abc12345-def6-7890-ghij-klmnopqrstuv' to continue

# Later...
$ p2p-transfer resume abc12345-def6-7890-ghij-klmnopqrstuv \
    --peer 192.168.1.100:8080 --path my_project

🔄 Resuming transfer
  Progress: 8/10 files (80.0%)
  Reconnecting...
  ✓ Connected

📁 Resuming folder transfer...
  Skipping 8 completed files...
[00:00:12] ████████████████████████████████ 10/10 files (100%)

✅ Transfer resumed and completed!
```

## Project Structure

```
p2p-transfer/
├── src/main.rs              # Binary entry point
├── p2p-core/                # Core library
│   └── src/
│       ├── lib.rs           # Public API exports
│       ├── error.rs         # Error types
│       ├── protocol.rs      # Protocol messages
│       ├── config.rs        # Configuration
│       ├── state.rs         # Transfer state persistence
│       ├── history.rs       # Transfer history tracking
│       ├── compression.rs   # Adaptive Zstd compression
│       ├── verification.rs  # Streaming CRC32/SHA256
│       ├── window.rs        # Sliding window protocol
│       ├── bandwidth.rs     # Token bucket rate limiting
│       ├── reconnect.rs     # Auto-reconnect with backoff
│       ├── network/         # Networking layer
│       │   ├── framing.rs   # MessagePack framing
│       │   ├── tcp.rs       # TCP connections
│       │   └── udp.rs       # UDP discovery
│       ├── discovery.rs     # Peer discovery
│       ├── nat.rs           # STUN NAT traversal
│       ├── handshake.rs     # Connection handshake
│       ├── session.rs       # P2P session management
│       ├── transfer_file.rs # File transfer logic
│       └── transfer_folder.rs # Folder transfer logic
├── p2p-cli/                 # CLI interface
│   └── src/
│       ├── lib.rs           # CLI entry point
│       ├── cli.rs           # Clap-based argument parsing
│       ├── send.rs          # Send command
│       ├── receive.rs       # Receive command
│       ├── discover.rs      # Discovery command
│       ├── nat_test.rs      # NAT test command
│       ├── resume.rs        # Resume command
│       └── history.rs       # History command
├── p2p-gui/                 # GUI interface
│   └── src/
│       ├── lib.rs           # GUI entry point
│       ├── app.rs           # Iced application
│       ├── state.rs         # GUI state
│       ├── message.rs       # Event messages
│       ├── operations.rs    # Async operations
│       ├── utils.rs         # Formatting utilities
│       ├── styles.rs        # Color palette
│       └── views/           # Tab views
│           ├── connection.rs
│           ├── send.rs
│           ├── receive.rs
│           ├── settings.rs
│           └── history.rs
└── tests/                   # Integration tests
    └── integration_test.rs
```

### Completed Features

- ✅ TCP/UDP networking with async I/O
- ✅ Handshake protocol with capability negotiation
- ✅ Single file transfers with chunking
- ✅ Folder transfers with recursive structure
- ✅ On-the-fly zstd compression
- ✅ CRC32 verification
- ✅ CLI interface with full functionality
- ✅ Progress bars with real-time updates
- ✅ Auto-save state and resume support

### In Progress

- 🚧 Performance optimizations (parallel transfers)
- 🚧 Enhanced security (encryption)
- 🚧 Hole Punching

## Performance

### Empirical Benchmarks (Localhost)

**Test Configuration:**
- Hardware: macOS ARM64 (Apple Silicon)
- Test File: 50MB random data
- Network: WiFi (RTT ~20ms)
- Compression: Enabled (zstd level 3)

| Transfer Mode | Window Size | Throughput | vs Sequential |
|--------------|-------------|------------|---------------|
| Sequential | N/A | 14.97 MB/s | 1.00x (baseline) |
| Windowed | 4 | 68.89 MB/s | 5.86x faster |
| Windowed | 16 (default) | 68.87 MB/s | 5.86x faster |
| Windowed | 32 | 69.33 MB/s | 5.87x faster |

**Run your own benchmarks:**
```bash
# Local benchmarking (same machine, auto-starts receiver)
python3 benchmark.py --mode sender

# Remote benchmarking (two machines on same network)
# On receiver machine:
python3 benchmark.py --mode receiver --port 14568

# On sender machine:
python3 benchmark.py --mode sender --receiver-ip 192.168.1.100 --port 14568
```

The Python benchmark script works on Windows, macOS, and Linux, and properly coordinates sender/receiver for accurate network testing.

### Windowed Transfer Speedup (WAN)

On networks with higher latency, windowed mode shows dramatic improvements:

| Network Type | RTT | Window Size | Expected Speedup |
|--------------|-----|-------------|------------------|
| LAN | < 5ms | 8 | 2-3x |
| WiFi | 10-20ms | 16 | 5-10x |
| Internet | 50ms | 32 | 10-15x |
| Satellite/VPN | 500ms+ | 64 | 15-20x |

**Why the difference?**
- **Localhost (0.1ms RTT)**: CPU-bound (compression/decompression), not network-bound → modest gains (6-7%)
- **WAN (50ms+ RTT)**: Network-bound → windowed mode eliminates RTT bottleneck → massive gains (10-15x)

*Performance depends on bandwidth, packet loss, CPU, and compression ratio.*

## Requirements

- **Rust**: 1.70+ (2021 edition)
- **Platform**: Windows, macOS, or Linux
- **Network**: Local network access for peer discovery

## Dependencies

### Core
- **tokio**: Async runtime (v1.47)
- **serde/rmp-serde**: MessagePack serialization
- **zstd**: Compression (v0.13)
- **crc32fast**: Fast CRC32 checksums
- **sha2**: SHA256 file verification
- **uuid**: Transfer and session IDs
- **anyhow**: Error handling

### CLI
- **clap**: CLI argument parsing (v4.5)
- **indicatif**: Progress bars (v0.17)
- **console**: Terminal styling (v0.15)
- **dialoguer**: Interactive prompts (v0.11)
- **tracing/tracing-subscriber**: Structured logging

### GUI
- **iced**: Cross-platform GUI framework (v0.12)
- **rfd**: Async file dialogs (v0.14)
- **chrono**: Timestamp handling (v0.4)
- **dirs**: Platform-specific directories (v5.0)

## Contributing

Contributions are welcome! Please read [Contributing](CONTRIBUTING.md) and [Design](DESIGN.md) documents for details on our code of conduct, development process, architecture and implementation details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
