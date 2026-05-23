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
identity         Ed25519 keypair + self-signed cert (rcgen), SHA-256 fingerprint
tls              rustls 0.23 ServerConfig (mutual TLS) / ClientConfig + FingerprintVerifier + AcceptAnyClientCert
known_peers      TOFU fingerprint store at <config_dir>/p2p-transfer/known_peers.json
network/quic     QuicEndpoint + QuicConnection (the only transport)
network/framing  length-prefixed MessagePack frames over any stream; typed Disconnected on clean EOF
network/udp      LAN broadcast beacons (port 14566)
discovery        Beacon manager — maintains peer table from UDP beacons
traversal/stun   async STUN with response-tx-id validation, Cone/Symmetric classifier
traversal/punch  race_connect_and_accept (parallel connect + address-validating accept loop)
traversal/mod    establish_via_rendezvous orchestrator (STUN → register → punch-or-relay)
protocol         Control-plane Message enum + ConfigMessage + TransferInfo + ...
handshake        HELLO / HELLO_ACK / CONFIG / CONFIG_ACK over the QUIC control stream
session          P2PSession owns QuicEndpoint + QuicConnection + handshake result
transfer_file    Single-file send/receive: one uni-stream per chunk, u64 indices, stream.stopped() drain
transfer_folder  Folder = sequence of single-file transfers; sanitize_relative_path on every wire path
compression      zstd; adaptive disable for incompressible data
verification     file-level SHA-256 (per-chunk CRC removed — TLS AEAD covers bytes); receiver mismatch is fatal
bandwidth        token-bucket throttle applied before each stream.write
state            chunk bitmap for resume
reconnect        exponential backoff retry loop for transient errors
history          JSON-backed transfer history (UX-only)
progress         ProgressState — observer callbacks, no I/O
```

`p2p-rendezvous` module map:

```
protocol         Wire enum (Register / Match / RelayMatch / Expired / Rejected) + RegisterRequest
server           TCP listener; concurrency-capped via Semaphore; rewrites public_endpoint IP to TCP source
relay            UDP packet forwarder; fingerprint-bound slot lookup; off-hot-path idle eviction
client           register / register_full → MatchOutcome (Direct | Relay)
lib              4 KiB-capped MessagePack framing
bin/rendezvousd  the binary (clap CLI over Server + Relay)
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
* **Mutual TLS.** The initiator pins the responder via
  `tls::FingerprintVerifier`; the responder requires a client cert
  via `tls::AcceptAnyClientCert` (which lets any cert through at the
  TLS layer — pinning happens at the handshake layer). Both sides
  observe the peer's cert via `QuicConnection::peer_fingerprint()`.
* The application-layer HELLO carries a claimed fingerprint that
  `handshake::cross_check_fingerprint` compares against the TLS
  observation. A mismatch *or* a missing observation (which would
  mean the peer didn't present a cert) is fatal
  (`Error::FingerprintMismatch`).
* The fingerprint is delivered out of band:
  - LAN: in the discovery beacon (with TOFU into `known_peers.json`
    on first contact).
  - Direct (`--peer`): on the command line via `--peer-fingerprint`.
  - WAN: via the rendezvous server's `Match` / `RelayMatch`.

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
  `classify_nat(&UdpSocket, a, b)` primitives the next phases use. STUN
  responses are validated against the request's transaction id so a
  spoofed reply from another source can't bind a fake mapping.
* **Phase 1 (shipped):** new crate `p2p-rendezvous` + `rendezvousd`
  binary; CLI flags `--rendezvous` + `--code`;
  `traversal::establish_via_rendezvous` orchestrator. Both peers bind a
  UDP socket, run STUN on it (the same socket quinn will later own),
  register at the rendezvous with a short shared code, and on match
  drive `traversal::punch::race_connect_and_accept`: both peers fire
  `quinn::Endpoint::connect` *and* an address-validating
  `accept_from(peer_addr)` in parallel. The smaller-device-id peer
  starts `connect` immediately; the larger one delays by 50 ms so the
  two Initial flights don't collide on a strict NAT. `accept_from`
  loops on `endpoint.accept()` and drops connections whose source
  address doesn't match the rendezvous-supplied peer — preventing
  third parties from riding our open mapping. Symmetric NAT is
  detected up front by comparing mapped ports across two STUN servers.
  The rendezvous server never sees user data; it only stores the
  (endpoint, fingerprint, device_id) tuple long enough to deliver each
  peer's address to the other, and it rewrites the claimed endpoint
  IP to the TCP source IP so a peer can't aim the punch at a
  third-party victim.
* **Phase 2 (shipped):** `rendezvousd --relay-bind <addr>
  --max-relay-mbps <n>` runs a tiny UDP packet forwarder. Any rendezvous
  match where either peer set `want_relay` (auto-set when STUN spots
  symmetric NAT, or forced via the `--force-relay` CLI flag) returns a
  `RelayMatch` with a fresh 16-byte session token and the relay's UDP
  address. Each peer sends a `RelayHello` so the relay records its
  source address against the fingerprint-bound slot the rendezvous
  reserved; subsequent UDP packets are forwarded verbatim. The relay
  rejects sessions where both peers claim the same fingerprint
  (`RelayError::DuplicateFingerprint`), uses a 65 KiB recv buffer,
  and runs idle eviction in a 30 s background task (off the per-packet
  hot path). Because the relay just forwards UDP packets verbatim,
  QUIC TLS still terminates end-to-end between the two real peers —
  the relay sees ciphertext only.

## Security & robustness guarantees

The data path enforces several invariants that an external code review
flagged as load-bearing — keep them intact when changing the relevant
modules.

* **Chunk indices are `u64` end-to-end.** `ChunkReader::total_chunks`,
  `read_chunk`, `fold_chunk`, and `ChunkWriter::write_chunk` all take
  `u64`. There is no `as u32` narrowing on the chunk path, so files
  larger than `2^32` chunks transfer correctly.
* **Bounds-checked chunk_index.** `FileTransferSession::receive_file`
  rejects `chunk_index >= total_chunks` with `Error::Protocol`. The
  wire-supplied index cannot make the writer seek to a random offset.
* **Drained streams.** `send_chunk_stream` awaits
  `stream.stopped().await` after `finish()` so the last chunk isn't
  lost when the sender closes the connection.
* **Hard SHA-256 verification.** The receiver computes the file SHA-256
  from disk after the transfer and compares to the sender's value; a
  mismatch returns `Error::Verification` (never just a warn).
* **Path sanitization.** Every wire path on the receive side runs
  through `transfer_folder::sanitize_relative_path`, which rejects
  absolute paths, `..`, `.`, drive letters, UNC roots, and empty
  paths. The sender runs the same check on `scan_folder` output so
  weird local names fail fast.
* **Mutual TLS.** `tls::server_config` requires a client cert
  (`AcceptAnyClientCert`) so `QuicConnection::peer_fingerprint()` is
  `Some(...)` on the responder side too. The handshake's
  `cross_check_fingerprint` rejects `None` observations — a peer that
  somehow didn't present a cert cannot pass the handshake even if its
  HELLO claim looks plausible.
* **Accept-from-expected-peer.** `traversal::punch::accept_from` loops
  on `endpoint.accept()` and drops connections whose `peer_addr()`
  doesn't match the rendezvous-supplied address.
* **STUN tx-id validation.** `stun::query` checks the response's
  transaction id against the one in the request before parsing
  attributes.
* **Rendezvous concurrency cap.** `Server::bind_with(max_concurrent)`
  applies backpressure via a `tokio::sync::Semaphore` (default 1024).
  An attacker can't fan out unbounded connections.
* **TCP-sourced public IP.** The rendezvous rewrites
  `RegisterRequest.public_endpoint`'s IP to the TCP peer's IP
  (keeping the user-supplied UDP port) so a client can't direct the
  punch at a third-party victim.
* **Relay slot pre-binding.** `reserve_session` rejects sessions where
  both peers share a fingerprint. With distinct fingerprints, slot
  binding is a single equality lookup per slot — no ambiguity, no
  duplicate-slot race.
* **Typed disconnect.** `framing::read_message` maps `UnexpectedEof`
  on the magic read to `Error::Disconnected`; frame-interior short
  reads become `Error::Protocol("truncated frame ...")`. The session
  event loop uses `matches!(err, Disconnected | Quic | Network)`
  instead of substring-matching error messages.

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
