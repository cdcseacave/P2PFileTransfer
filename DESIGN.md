# Design — P2P File Transfer

## Overview

A peer-to-peer file transfer tool. Two peers establish an authenticated
**QUIC** connection over a single UDP socket, exchange a small control
flow, and stream files chunk-by-chunk over per-chunk unidirectional QUIC
streams. TLS 1.3 is mandatory (QUIC requires it) and identity is pinned
by SHA-256 fingerprint of a long-lived self-signed certificate.

## Crate layout

```
Cargo workspace
├── src/main.rs              binary entry point (delegates to p2p-cli)
├── p2p-core/                core library: protocol + transport + transfer engine
├── p2p-cli/                 clap-based CLI
├── p2p-gui/                 Iced 0.12 GUI
├── p2p-rendezvous/          rendezvous library + `rendezvousd` binary
└── tests/                   workspace integration tests
    ├── integration_test.rs            QUIC handshake smoke test
    └── traversal_loopback_test.rs     rendezvous + race-connect-and-accept
```

`p2p-core` module map:

```
identity        Ed25519 keypair + self-signed cert (rcgen), SHA-256 fingerprint
tls             rustls 0.23 ServerConfig/ClientConfig + FingerprintVerifier
known_peers     TOFU fingerprint store at <config_dir>/p2p-transfer/known_peers.json
network/quic    QuicEndpoint + QuicConnection (the only transport)
network/framing length-prefixed MessagePack frames over any stream
network/udp     LAN broadcast beacons (port 14566)
discovery       Beacon manager — maintains peer table from UDP beacons
traversal/      STUN primitives (Phase 0); hole punch + rendezvous (Phase 1)
protocol        Control-plane Message enum + ConfigMessage + TransferInfo + ...
handshake       HELLO / HELLO_ACK / CONFIG / CONFIG_ACK over the QUIC control stream
session         P2PSession owns QuicEndpoint + QuicConnection + handshake result
transfer_file   Single-file send/receive: one uni-stream per chunk
transfer_folder Folder = sequence of single-file transfers reusing the connection
compression     zstd; adaptive disable for incompressible data
verification    file-level SHA-256 (per-chunk CRC removed — TLS AEAD covers bytes)
bandwidth       token-bucket throttle applied before each stream.write
state           chunk bitmap for resume
reconnect       exponential backoff retry loop for transient errors
history         JSON-backed transfer history (UX-only)
progress        ProgressState — observer callbacks, no I/O
```

## Connection model

**One UDP socket per endpoint.** A `QuicEndpoint` wraps `quinn::Endpoint`
and is bound to a UDP socket (ephemeral by default). Both initiating
outbound connections and accepting inbound ones happen on the same
socket — that's also the socket the (future) NAT hole-punch will use, so
the STUN-discovered public mapping refers to the right port.

`QuicConnection` holds the `quinn::Connection` plus one open bidirectional
control stream (carrying HELLO / CONFIG / TRANSFER_INFO / READY / COMPLETE
messages) and provides `open_uni` / `accept_uni` for chunk streams.

### Chunk wire format

```
[ chunk_index : u64 LE | flags : u8 | payload bytes (compressed iff flags&1) ]
```

The receiver `accept_uni()`s, parses the 9-byte header, decompresses if
the flag is set, and writes the payload at `chunk_index * chunk_size`
in the destination file. There are no per-chunk ACKs, retries, or CRCs:
QUIC retransmits dropped packets, per-stream flow control replaces the
sliding window, and TLS 1.3 AEAD authenticates every byte. A
finalized `SendStream` is end-to-end acknowledged by QUIC itself.

### Handshake

The handshake runs over the bidirectional control stream after the QUIC
TLS handshake completes:

```
initiator                    responder
  |--- HELLO ---------------->|
  |    {protocol_version,     |
  |     device_id,            |
  |     capabilities,         |
  |     cert_fingerprint}     |
  |<-- HELLO_ACK -------------|
  |    (cross-check fp        |
  |     against TLS cert)     |
  |--- CONFIG --------------->|
  |    {compress, level,      |
  |     adaptive, chunk_size, |
  |     bandwidth_limit}      |
  |<-- CONFIG_ACK ------------|
```

After handshake both peers are symmetric: either side can call
`send_path` / `receive_to` over the same connection.

### Identity & trust

* Per-device Ed25519 keypair + self-signed cert generated on first run
  and persisted to `<config_dir>/p2p-transfer/identity.{key,cert}`.
  The SHA-256 of the cert's DER encoding is the stable per-device
  fingerprint (`identity.fingerprint()` / `--peer-fingerprint`).
* The initiator pins the responder's cert by SHA-256 via
  `tls::FingerprintVerifier`. The fingerprint is delivered out of band:
  - LAN: in the discovery beacon (with TOFU into `known_peers.json` on
    first contact).
  - Direct (`--peer`): on the command line via `--peer-fingerprint`.
  - WAN (Phase 1): via the rendezvous server.
* On the responder side rustls accepts the connection without requesting
  a client cert; the application-layer HELLO cross-checks the claimed
  fingerprint against the cert TLS observed.

## Discovery (LAN)

UDP beacons on `255.255.255.255:14566` carrying
`{device_id, device_name, port, capabilities, cert_fingerprint}`. The
`DiscoveryManager` broadcasts every 2 s, expires peers after a TTL, and
exposes `get_peers()`. The CLI's `--discover` flag and the GUI's
discovery toggle use this to pick the first responding peer.

## Resume

Chunk-level resume uses `state::TransferState` (a `BitVec` of completed
chunk indices per file) persisted to JSON. `P2PSession::send_path` loops
on a recoverable error (network/timeout/QUIC), re-establishes the
connection via `reconnect()`, and re-runs the folder send — which skips
any chunk index already in the bitmap.

## Bandwidth

`bandwidth::BandwidthLimiter` is a single-token-bucket;
`transfer_file::send_file` calls `wait_for_tokens(payload.len())` before
each `open_uni().write_all`.

## NAT traversal (phased)

* **Phase 0 (shipped):** LAN discovery and direct `--peer` only.
  `traversal/stun.rs` exposes async `query(&UdpSocket, server)` and
  `classify_nat(&UdpSocket, a, b)` primitives the next phases use.
* **Phase 1 (shipped):** new crate `p2p-rendezvous` + `rendezvousd`
  binary; CLI flags `--rendezvous` + `--code`;
  `traversal::establish_via_rendezvous` orchestrator. Both peers bind a
  UDP socket, run STUN on it (the same socket quinn will later own),
  register at the rendezvous with a short shared code, and on match race
  `quinn::Endpoint::connect` against `accept` — QUIC `Initial` packets
  themselves serve as the hole-punch. Symmetric NAT is detected up
  front by comparing mapped ports across two STUN servers and surfaces
  `Error::HolePunchFailed`. The rendezvous server never sees user data
  — it only stores the (endpoint, fingerprint, device_id) tuple long
  enough to deliver each peer's address to the other.
* **Phase 2 (shipped):** `rendezvousd --relay-bind <addr>
  --max-relay-mbps <n>` runs a tiny UDP packet forwarder. Any rendezvous
  match where either peer set `want_relay` (auto-set when STUN spots
  symmetric NAT, or forced via the `--force-relay` CLI flag) returns a
  `RelayMatch` with a fresh 16-byte session token and the relay's UDP
  address. Each peer sends a `RelayHello` so the relay records its
  source address, then runs a normal QUIC handshake with the relay's
  address as the apparent peer endpoint. Because the relay just forwards
  UDP packets verbatim, QUIC TLS still terminates end-to-end between
  the two real peers — the relay sees ciphertext only.

## Protocol versioning

`PROTOCOL_VERSION = 2`, `MIN_PROTOCOL_VERSION = 2`. Equality check only —
no v1 compatibility code. Pre-rewrite peers used TCP; the QUIC TLS
handshake fails cleanly when they try to talk to a v2 endpoint.

## Conventions

* All I/O async via `tokio`. No blocking inside async tasks.
* `tracing` for logging; CLI's `--verbosity` sets the `p2p_core` /
  `p2p_cli` filter and `RUST_LOG` overrides it.
* `p2p-core::Result<T> = Result<T, p2p-core::Error>`; CLI layer adds
  `anyhow::Context`.
* Docs live in this file + `README.md` + `TODO.md` + `CHANGELOG.md`.
  Per-feature markdown files are not added.
